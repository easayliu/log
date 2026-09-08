//! ClickHouse 入库：HTTP 接口 + `JSONEachRow` 批量插入。
//!
//! 建表语句见 [`ClickhouseSink::create_table_ddl`]，字段与 [`LogEvent`] 一一对应。

use std::io::Write;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{FixedOffset, NaiveDateTime, Offset, TimeZone};
use chrono_tz::Tz;

use crate::error::{Error, Result};
use crate::event::{LogEvent, WithOffset};
use crate::sink::Sink;

/// 建表时固定带的列，与 [`LogEvent`] 的固定字段一一对应。`timestamp` 的类型跟着
/// `timezone` 走，不在这里。
const BASE_COLUMNS: [(&str, &str); 8] = [
    ("level", "LowCardinality(String)"),
    ("trace_id", "String"),
    ("span_id", "String"),
    ("thread", "String"),
    ("logger", "String"),
    ("message", "String"),
    ("file", "String"),
    ("host", "LowCardinality(String)"),
];

/// 按 trace id 查日志（Jaeger 里拿到一个 id 反查全部日志）不一定带时间范围，
/// 排序键里 trace_id 排在 timestamp 后面帮不上忙，全表扫 30 天。bloom filter
/// 让这种查询跳过绝大多数 granule，代价是每个 granule 几十字节。
const TRACE_ID_INDEX: &str = "`idx_trace_id` `trace_id` TYPE bloom_filter GRANULARITY 4";

pub struct ClickhouseSink {
    client: reqwest::Client,
    endpoint: String,
    database: String,
    table: String,
    cluster: Option<String>,
    timezone: Option<Tz>,
    /// 固定列之外还要有的列（容器元数据、配置里的静态字段），建表和启动校验都用。
    extra_columns: Vec<(String, String)>,
    user: Option<String>,
    password: Option<String>,
    timeout: Duration,
    async_insert: bool,
    compress: bool,
}

impl ClickhouseSink {
    /// `endpoint` 形如 `http://127.0.0.1:8123`。
    pub fn new(
        endpoint: impl Into<String>,
        database: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint: endpoint.into().trim_end_matches('/').to_owned(),
            database: database.into(),
            table: table.into(),
            cluster: None,
            timezone: None,
            extra_columns: Vec::new(),
            user: None,
            password: None,
            timeout: Duration::from_secs(30),
            async_insert: false,
            compress: true,
        }
    }

    /// ClickHouse 集群名（`system.clusters` 里的那个，不是 k8s 集群）。
    ///
    /// 配了之后建表语句变成两张表：`<table>_local` 是 `ReplicatedMergeTree`，带
    /// `ON CLUSTER` 一次性下发到所有节点；`<table>` 是它上面的 `Distributed`，
    /// 也就是 sink 实际写入的那张。写入路径本身不受影响 —— 还是往 `table` 里 INSERT。
    pub fn cluster(mut self, cluster: impl Into<String>) -> Self {
        self.cluster = Some(cluster.into());
        self
    }

    /// 日志时间戳所在的时区，比如 `Asia/Shanghai`。
    ///
    /// 我们存的是日志里的墙上时间。不知道时区的话 `11:04:08.914` 是哪个绝对时刻由列
    /// （或服务端）的时区决定 —— 服务端 UTC、日志北京时间就差 8 小时，单看日志表
    /// 无所谓，但 Jaeger / Grafana 给的是绝对时间，从 span 跳日志按 UTC 开窗口去查，
    /// 一条都查不到。配了之后做两件事：
    ///
    /// * INSERT 时时间戳带偏移：`2026-09-07 11:04:08.914+08:00`。存进去的绝对时刻
    ///   不再依赖表结构，老表没来得及 ALTER 也不会存错；
    /// * `--ddl` 把列建成 `DateTime64(3, 'Asia/Shanghai')`，查出来显示的还是
    ///   `11:04:08.914`。存的值不变，只是 ClickHouse 知道该怎么换算了。
    pub fn timezone(mut self, timezone: Tz) -> Self {
        self.timezone = Some(timezone);
        self
    }

    /// 固定列之外的列：容器元数据（`stream`/`namespace`/`pod`/`container`）和配置里
    /// 的静态字段。`--ddl` 建出来，healthcheck 时校验表里确实有。
    pub fn extra_columns(mut self, columns: Vec<(String, String)>) -> Self {
        self.extra_columns = columns;
        self
    }

    pub fn auth(mut self, user: impl Into<String>, password: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self.password = Some(password.into());
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// 打开后由 ClickHouse 服务端再攒一层批，适合多实例小批量写入的场景。
    pub fn async_insert(mut self, enabled: bool) -> Self {
        self.async_insert = enabled;
        self
    }

    /// 是否 gzip 压缩 INSERT 的请求体，默认开。
    ///
    /// 日志 JSON 压得动（实测约 6.5 倍），DaemonSet 场景下这是每个节点乘以节点数的
    /// 常驻带宽。关掉它一般只有一个理由：中间的代理/网关不能正确转发压缩过的 body。
    pub fn compress(mut self, enabled: bool) -> Self {
        self.compress = enabled;
        self
    }

    /// 建表 + 补表的语句，直接拿去执行即可，重复执行也没事。
    ///
    /// 第一段 `CREATE TABLE IF NOT EXISTS` 管新表；后面的 `ALTER TABLE` 全是
    /// `ADD COLUMN IF NOT EXISTS` / `ADD INDEX IF NOT EXISTS` / `MODIFY COLUMN`
    /// 这类幂等操作，管老表：配置里新加了 `fields`、后来配了 `timezone`，重跑一次就把
    /// 差异补齐，不用人手对着表结构写 ALTER。集群模式下 `_local` 表和 `Distributed`
    /// 表各补一遍，`Distributed` 不会自动跟着本地表变列，而且它不支持跳数索引。
    ///
    /// 不碰的：排序键、分区键改不了；TTL 能改但 `MODIFY TTL` 会触发重算；已有列的类型
    /// 变了（静态字段从整数改成字符串）`IF NOT EXISTS` 会跳过 —— 这几种本来就该人看
    /// 一眼再动。
    pub fn create_table_ddl(&self) -> String {
        self.create_table_ddl_with(&self.extra_columns)
    }

    /// 同上，额外追加几列而不用 [`Self::extra_columns`]。
    pub fn create_table_ddl_with(&self, extra: &[(String, String)]) -> String {
        let timestamp_type = match &self.timezone {
            Some(tz) => format!("DateTime64(3, '{}')", tz.name()),
            None => "DateTime64(3)".to_owned(),
        };
        // timestamp 单列出来：CREATE 里排第一，ALTER 里只有 MODIFY，不 ADD。
        let mut columns: Vec<(String, String)> = BASE_COLUMNS
            .iter()
            .map(|(name, ty)| ((*name).to_owned(), (*ty).to_owned()))
            .collect();
        for (name, ty) in extra {
            if name != "timestamp" && !columns.iter().any(|(existing, _)| existing == name) {
                columns.push((name.clone(), ty.clone()));
            }
        }

        let width = columns
            .iter()
            .map(|(name, _)| name.len())
            .max()
            .unwrap_or(0)
            .max("timestamp".len());
        let pad = |name: &str| " ".repeat(width - name.len());
        let mut body = vec![format!(
            "    `timestamp`{} {timestamp_type}",
            pad("timestamp")
        )];
        body.extend(
            columns
                .iter()
                .map(|(name, ty)| format!("    `{name}`{} {ty}", pad(name))),
        );
        body.push(format!("    INDEX {TRACE_ID_INDEX}"));
        let body = body.join(",\n");

        let layout = "PARTITION BY toYYYYMMDD(`timestamp`)\n\
             ORDER BY (`timestamp`, `level`, `trace_id`)\n\
             TTL toDateTime(`timestamp`) + INTERVAL 30 DAY";
        let db = &self.database;
        let table = &self.table;
        let modify_timestamp = self
            .timezone
            .map(|_| format!("MODIFY COLUMN `timestamp` {timestamp_type}"));

        let alter = |target: &str, on_cluster: &str, with_index: bool| {
            let mut actions: Vec<String> = columns
                .iter()
                .map(|(name, ty)| format!("ADD COLUMN IF NOT EXISTS `{name}` {ty}"))
                .collect();
            if with_index {
                actions.push(format!("ADD INDEX IF NOT EXISTS {TRACE_ID_INDEX}"));
            }
            actions.extend(modify_timestamp.clone());
            format!(
                "ALTER TABLE `{db}`.`{target}`{on_cluster}\n    {}",
                actions.join(",\n    ")
            )
        };

        let Some(cluster) = &self.cluster else {
            return format!(
                "CREATE TABLE IF NOT EXISTS `{db}`.`{table}`\n\
                 (\n{body}\n)\n\
                 ENGINE = MergeTree\n{layout};\n\n{}",
                alter(table, "", true)
            );
        };

        // 集群：本地表存数据，Distributed 表负责分发，全部 ON CLUSTER 一次下发。
        // `{shard}` / `{replica}` 是 ClickHouse 自己的宏，由各节点的 macros 配置展开，
        // 不是这里要替换的东西。补列先补本地表再补 Distributed 表：反过来的话中间
        // 那一瞬间往 Distributed 表插新列会因为本地表没有而失败。
        let local = self.local_table();
        let on_cluster = format!(" ON CLUSTER `{cluster}`");
        format!(
            "CREATE TABLE IF NOT EXISTS `{db}`.`{local}`{on_cluster}\n\
             (\n{body}\n)\n\
             ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{db}/{local}', '{{replica}}')\n\
             {layout};\n\n\
             CREATE TABLE IF NOT EXISTS `{db}`.`{table}`{on_cluster}\n\
             AS `{db}`.`{local}`\n\
             ENGINE = Distributed(`{cluster}`, `{db}`, `{local}`, rand());\n\n\
             {};\n\n{}",
            alter(&local, &on_cluster, true),
            alter(table, &on_cluster, false)
        )
    }

    /// 表里必须有的列名：固定列 + 额外列。
    fn expected_columns(&self) -> impl Iterator<Item = &str> {
        std::iter::once("timestamp")
            .chain(BASE_COLUMNS.iter().map(|(name, _)| *name))
            .chain(self.extra_columns.iter().map(|(name, _)| name.as_str()))
    }

    /// 日志墙上时间在配置时区里的偏移。夏令时回拨造成的重复时刻取前一个，
    /// 拨快造成的不存在时刻按 UTC 那一刻的偏移兜底 —— 都只影响那一小时的日志。
    fn offset_at(tz: Tz, ts: &NaiveDateTime) -> FixedOffset {
        tz.offset_from_local_datetime(ts)
            .earliest()
            .unwrap_or_else(|| tz.offset_from_utc_datetime(ts))
            .fix()
    }

    /// 集群模式下真正存数据的本地表名：`<table>_local`。
    pub fn local_table(&self) -> String {
        format!("{}_local", self.table)
    }

    /// 执行任意 SQL（建表、查询都可以）。
    pub async fn execute(&self, sql: &str) -> Result<String> {
        self.request(sql, Vec::new()).await
    }

    async fn request(&self, sql: &str, body: Vec<u8>) -> Result<String> {
        let mut settings: Vec<(&str, &str)> = vec![
            ("query", sql),
            // 时间戳按 `2026-09-07 03:04:08.914` 发送，开启宽松解析更稳。
            ("date_time_input_format", "best_effort"),
            // 事件里的自定义字段可能没有对应列，跳过而不是整批失败。
            ("input_format_skip_unknown_fields", "1"),
        ];
        if self.async_insert {
            settings.push(("async_insert", "1"));
            settings.push(("wait_for_async_insert", "1"));
        }

        // 空 body（`SELECT 1`、`EXISTS TABLE` 这些健康检查）不压：gzip 一个空串反而
        // 会多出十几个字节的头，而这里正是 411 那个坑所在，保持原样最稳。
        let compressed = self.compress && !body.is_empty();
        let body = if compressed { gzip(&body)? } else { body };

        // Content-Length 必须自己写。body 为空时（`SELECT 1`、`EXISTS TABLE` 这些
        // 健康检查）hyper 认为流已经结束，既不发 Content-Length 也不用 chunked，
        // 而 ClickHouse 见到这样的 POST 直接回 411 Length Required。
        let mut request = self
            .client
            .post(&self.endpoint)
            .query(&settings)
            .timeout(self.timeout)
            .header(reqwest::header::CONTENT_LENGTH, body.len())
            .body(body);

        if compressed {
            request = request.header(reqwest::header::CONTENT_ENCODING, "gzip");
        }

        if let (Some(user), Some(password)) = (&self.user, &self.password) {
            request = request
                .header("X-ClickHouse-User", user)
                .header("X-ClickHouse-Key", password);
        }

        let response = request.send().await.map_err(Error::sink)?;
        let status = response.status();
        let text = response.text().await.map_err(Error::sink)?;

        if !status.is_success() {
            return Err(Error::Sink(
                format!("ClickHouse 返回 {status}: {}", text.trim()).into(),
            ));
        }
        Ok(text)
    }
}

/// SQL 字符串字面量转义：库名表名来自配置，反引号标识符走的是另一套规则，这里只管
/// `WHERE database = '...'` 里的单引号串。
fn escape_literal(raw: &str) -> String {
    raw.replace('\\', "\\\\").replace('\'', "\\'")
}

/// 压缩请求体。ClickHouse 见到 `Content-Encoding: gzip` 会自己解开，服务端不用开
/// 任何设置 —— `enable_http_compression` 管的是响应方向，跟这里无关。
///
/// 压缩级别取最快的那一档。实测 8.7MiB 一批（每条都有独立的 trace_id 和业务 id）：
/// level 1 压到 1/6.5 花 16ms，level 6 压到 1/7.5 却要 108ms —— 多压 15% 体积，
/// CPU 翻 6.8 倍。DaemonSet 里 CPU 才是紧张的那个资源，这笔账不划算。
fn gzip(body: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(
        Vec::with_capacity(body.len() / 8),
        flate2::Compression::fast(),
    );
    encoder
        .write_all(body)
        .map_err(|err| Error::io("压缩 ClickHouse 请求体失败".to_owned(), err))?;
    encoder
        .finish()
        .map_err(|err| Error::io("压缩 ClickHouse 请求体失败".to_owned(), err))
}

#[async_trait]
impl Sink for ClickhouseSink {
    async fn write(&mut self, events: &[LogEvent]) -> Result<()> {
        // 直接写进整批的缓冲区：先 to_json_line() 拿到 String 再拷进来，
        // 等于每条事件多一次分配 + 一次拷贝。
        let mut body: Vec<u8> = Vec::with_capacity(events.len() * 256);
        for event in events {
            match self.timezone {
                Some(tz) => {
                    let offset = Self::offset_at(tz, &event.timestamp);
                    serde_json::to_writer(&mut body, &WithOffset { event, offset })?;
                }
                None => serde_json::to_writer(&mut body, event)?,
            }
            body.push(b'\n');
        }

        let sql = format!(
            "INSERT INTO `{}`.`{}` FORMAT JSONEachRow",
            self.database, self.table
        );
        self.request(&sql, body).await?;

        tracing::debug!(count = events.len(), table = %self.table, "已写入 ClickHouse");
        Ok(())
    }

    async fn healthcheck(&self) -> Result<()> {
        self.execute("SELECT 1").await?;

        let exists = self
            .execute(&format!(
                "EXISTS TABLE `{}`.`{}`",
                self.database, self.table
            ))
            .await?;
        if exists.trim() != "1" {
            return Err(Error::Sink(
                format!(
                    "表 {}.{} 不存在，执行 `logpipe --ddl` 输出的语句建表",
                    self.database, self.table
                )
                .into(),
            ));
        }

        // 列齐不齐也要查。INSERT 带着 input_format_skip_unknown_fields=1，表里没有的
        // 字段不报错、整批也不失败，只是那个字段悄悄没了 —— 配置里刚加的 fields
        // 查不到值，多半就是这个原因。启动时对一遍，缺了直接说清楚该怎么补。
        let present = self
            .execute(&format!(
                "SELECT name FROM system.columns WHERE database = '{}' AND table = '{}' FORMAT TSV",
                escape_literal(&self.database),
                escape_literal(&self.table)
            ))
            .await?;
        let present: std::collections::HashSet<&str> = present
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .collect();
        let missing: Vec<&str> = self
            .expected_columns()
            .filter(|name| !present.contains(name))
            .collect();
        if !missing.is_empty() {
            return Err(Error::Sink(
                format!(
                    "表 {}.{} 缺列 {}：表结构没跟上配置，这些字段插入时会被静默丢掉。\
                     重跑 `logpipe --ddl` 输出的语句（ddl Job）即可补齐",
                    self.database,
                    self.table,
                    missing.join(", ")
                )
                .into(),
            ));
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "clickhouse"
    }
}
