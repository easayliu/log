//! ClickHouse 入库：HTTP 接口 + `JSONEachRow` 批量插入。
//!
//! 建表语句见 [`ClickhouseSink::create_table_ddl`]，字段与 [`LogEvent`] 一一对应。

use std::io::Write;
use std::time::Duration;

use async_trait::async_trait;

use crate::error::{Error, Result};
use crate::event::LogEvent;
use crate::sink::Sink;

pub struct ClickhouseSink {
    client: reqwest::Client,
    endpoint: String,
    database: String,
    table: String,
    cluster: Option<String>,
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

    /// 与 [`LogEvent`] 对应的建表语句，直接拿去执行即可。
    pub fn create_table_ddl(&self) -> String {
        self.create_table_ddl_with(&[])
    }

    /// 同上，额外追加几列。用于容器元数据（`pod`/`namespace`/…）和配置里的静态字段。
    pub fn create_table_ddl_with(&self, extra: &[(String, String)]) -> String {
        let mut columns: Vec<(String, String)> = [
            ("timestamp", "DateTime64(3)"),
            ("level", "LowCardinality(String)"),
            ("trace_id", "String"),
            ("span_id", "String"),
            ("thread", "String"),
            ("logger", "String"),
            ("message", "String"),
            ("file", "String"),
            ("host", "LowCardinality(String)"),
        ]
        .iter()
        .map(|(name, ty)| ((*name).to_owned(), (*ty).to_owned()))
        .collect();

        for (name, ty) in extra {
            if !columns.iter().any(|(existing, _)| existing == name) {
                columns.push((name.clone(), ty.clone()));
            }
        }

        let width = columns
            .iter()
            .map(|(name, _)| name.len())
            .max()
            .unwrap_or(0);
        let body = columns
            .iter()
            .map(|(name, ty)| {
                format!(
                    "    `{name}`{pad} {ty}",
                    pad = " ".repeat(width - name.len())
                )
            })
            .collect::<Vec<_>>()
            .join(",\n");

        let layout = "PARTITION BY toYYYYMMDD(`timestamp`)\n\
             ORDER BY (`timestamp`, `level`, `trace_id`)\n\
             TTL toDateTime(`timestamp`) + INTERVAL 30 DAY";
        let db = &self.database;
        let table = &self.table;

        let Some(cluster) = &self.cluster else {
            return format!(
                "CREATE TABLE IF NOT EXISTS `{db}`.`{table}`\n\
                 (\n{body}\n)\n\
                 ENGINE = MergeTree\n{layout}"
            );
        };

        // 集群：本地表存数据，Distributed 表负责分发，两条语句都 ON CLUSTER 一次下发。
        // `{shard}` / `{replica}` 是 ClickHouse 自己的宏，由各节点的 macros 配置展开，
        // 不是这里要替换的东西。
        let local = self.local_table();
        format!(
            "CREATE TABLE IF NOT EXISTS `{db}`.`{local}` ON CLUSTER `{cluster}`\n\
             (\n{body}\n)\n\
             ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{db}/{local}', '{{replica}}')\n\
             {layout};\n\n\
             CREATE TABLE IF NOT EXISTS `{db}`.`{table}` ON CLUSTER `{cluster}`\n\
             AS `{db}`.`{local}`\n\
             ENGINE = Distributed(`{cluster}`, `{db}`, `{local}`, rand())"
        )
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
            serde_json::to_writer(&mut body, event)?;
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
                    "表 {}.{} 不存在，可用 ClickhouseSink::create_table_ddl() 建表",
                    self.database, self.table
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
