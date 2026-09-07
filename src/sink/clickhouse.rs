//! ClickHouse 入库：HTTP 接口 + `JSONEachRow` 批量插入。
//!
//! 建表语句见 [`ClickhouseSink::create_table_ddl`]，字段与 [`LogEvent`] 一一对应。

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
        self.request(sql, String::new()).await
    }

    async fn request(&self, sql: &str, body: String) -> Result<String> {
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

        let mut request = self
            .client
            .post(&self.endpoint)
            .query(&settings)
            .timeout(self.timeout)
            .body(body);

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

#[async_trait]
impl Sink for ClickhouseSink {
    async fn write(&mut self, events: &[LogEvent]) -> Result<()> {
        let mut body = String::with_capacity(events.len() * 256);
        for event in events {
            body.push_str(&event.to_json_line()?);
            body.push('\n');
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
