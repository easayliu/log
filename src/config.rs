//! YAML 配置：给可执行文件用。库使用者也可以直接用 `Pipeline::builder()` 拼装。

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_yaml_ng::{Mapping, Value};

use crate::batch::{BatchConfig, RetryConfig};
use crate::error::{Error, Result};
use crate::parser::{ChainParser, ContainerFormat, LogFormat};
use crate::pipeline::{OnError, Pipeline};
use crate::sink::console::Encoding;
use crate::sink::{ClickhouseSink, ConsoleSink};
use crate::source::checkpoint::Checkpointer;
use crate::source::k8s::PodSelector;
use crate::source::FileSource;

#[derive(Debug)]
pub struct Config {
    pub source: SourceConfig,
    pub parser: ParserConfig,
    pub sink: SinkConfig,
    pub batch: BatchSettings,
    pub retry: RetrySettings,
    pub pipeline: PipelineSettings,
    /// 给每条日志附加的静态字段，例如 `app = "order-service"`、`env = "prod"`。
    pub fields: BTreeMap<String, serde_json::Value>,
}

/// 采哪儿的日志。`type: kubernetes` 按 k8s 标准自动发现，`type: file` 自己写路径。
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceConfig {
    /// 采本机所有容器的 stdout/stderr（containerd / CRI-O）。
    ///
    /// 路径不用配：自动扫 `/var/log/pods`、剥掉运行时外壳、从路径解出
    /// namespace / pod / container。要采哪些就按 k8s 的名字声明，支持 glob。
    Kubernetes(KubernetesSourceConfig),
    /// 直接采文件（应用自己写日志文件的场景）。
    File(FileSourceConfig),
}

impl SourceConfig {
    /// 事件里是否会带上 namespace / pod / container / stream 这几列。
    pub fn emits_pod_metadata(&self) -> bool {
        match self {
            SourceConfig::Kubernetes(_) => true,
            SourceConfig::File(file) => file.container_format == ContainerFormat::Cri,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KubernetesSourceConfig {
    /// 只采这些 namespace；留空 = 全都采。
    #[serde(default)]
    pub namespaces: Vec<String>,
    /// 排除这些 namespace，优先于 `namespaces`。比如 `["kube-*"]`。
    #[serde(default)]
    pub exclude_namespaces: Vec<String>,
    #[serde(default)]
    pub pods: Vec<String>,
    #[serde(default)]
    pub exclude_pods: Vec<String>,
    #[serde(default)]
    pub containers: Vec<String>,
    /// 比如 `["istio-proxy"]`。
    #[serde(default)]
    pub exclude_containers: Vec<String>,
    /// 不采自己（默认开）。容器里的 hostname 就是 pod 名，不需要额外配置。
    #[serde(default = "yes")]
    pub exclude_self: bool,
    /// kubelet 的日志目录。标准路径是 `/var/log/pods`，一般不用改。
    #[serde(default = "default_pod_log_dir")]
    pub log_dir: String,
    /// 位点目录。不填则不落盘，重启后重新开始。
    pub data_dir: Option<String>,
    /// 新出现的容器从头读还是只读增量。默认只读增量，避免首次部署把历史日志全灌一遍。
    #[serde(default)]
    pub read_from_beginning: bool,
    /// 多久扫一次目录发现新容器，秒。
    #[serde(default = "ten")]
    pub glob_interval_secs: u64,
    /// 覆盖主机名（默认取本机 hostname，在容器里就是 pod 名）。
    pub host: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSourceConfig {
    /// 要采集的文件，支持 glob。
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
    /// 位点目录。不填则不落盘，重启后重新开始。
    pub data_dir: Option<String>,
    /// 没有位点的新文件从头读（true，默认）还是只读增量（false）。
    #[serde(default = "yes")]
    pub read_from_beginning: bool,
    /// 多久扫一次 glob 发现新文件，秒。
    #[serde(default = "ten")]
    pub glob_interval_secs: u64,
    /// 覆盖主机名。默认取本机 hostname。
    pub host: Option<String>,
    /// 容器日志格式：`raw`（默认，直接是应用日志）或 `cri`（自己指定容器日志路径时用）。
    #[serde(default)]
    pub container_format: ContainerFormat,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParserConfig {
    /// 这条 pipeline 上会遇到的日志格式，按顺序尝试，第一个匹配上的胜出。
    /// 可选 `logback`（默认）/ `gin` / `nginx`。
    ///
    /// 整机采集（`type: kubernetes`）时按节点上实际跑的语言栈配全，比如
    /// `[logback, gin, nginx]`。**漏配的那种格式不会报错**，只会被当成上一条日志的
    /// 堆栈续行粘上去。
    #[serde(default)]
    pub formats: Vec<LogFormat>,
    /// 自定义正则，命名捕获组：timestamp / level / trace_id / span_id / thread / logger / message。
    /// 只作用在 `logback` 这一档上。
    pub pattern: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SinkConfig {
    Clickhouse {
        /// 形如 `http://127.0.0.1:8123`。
        endpoint: String,
        database: String,
        table: String,
        /// ClickHouse 集群名（`system.clusters` 里的，不是 k8s 集群，也和 `fields` 里
        /// 叫 cluster 的静态字段无关）。配了之后 `--ddl` 生成 `ReplicatedMergeTree`
        /// 本地表 + `Distributed` 表，两条都带 `ON CLUSTER`。
        cluster: Option<String>,
        user: Option<String>,
        password: Option<String>,
        #[serde(default)]
        async_insert: bool,
        /// gzip 压缩 INSERT 请求体，默认开。只有中间代理不能正确转发压缩 body 时才关。
        #[serde(default = "yes")]
        compress: bool,
        #[serde(default = "thirty")]
        timeout_secs: u64,
    },
    Console {
        /// `json`（默认）或 `text`。
        #[serde(default)]
        encoding: ConsoleEncoding,
        #[serde(default)]
        stderr: bool,
    },
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleEncoding {
    #[default]
    Json,
    Text,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchSettings {
    #[serde(default = "default_max_events")]
    pub max_events: usize,
    #[serde(default = "default_max_bytes")]
    pub max_bytes: usize,
    #[serde(default = "one")]
    pub timeout_secs: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetrySettings {
    #[serde(default = "five")]
    pub max_attempts: usize,
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    #[serde(default = "thirty")]
    pub max_backoff_secs: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineSettings {
    /// source 与 sink 之间的队列深度（按批计）。
    #[serde(default = "default_buffer")]
    pub buffer: usize,
    /// healthcheck 不通过就不启动。
    #[serde(default)]
    pub require_healthy: bool,
    /// 重试耗尽后：`stop`（默认，位点不推进）或 `drop`（丢掉继续跑）。
    #[serde(default)]
    pub on_error: OnErrorSetting,
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnErrorSetting {
    #[default]
    Stop,
    Drop,
}

fn yes() -> bool {
    true
}
fn one() -> u64 {
    1
}
fn five() -> usize {
    5
}
fn ten() -> u64 {
    10
}
fn thirty() -> u64 {
    30
}
fn default_max_events() -> usize {
    BatchConfig::default().max_events
}
fn default_max_bytes() -> usize {
    BatchConfig::default().max_bytes
}
fn default_initial_backoff_ms() -> u64 {
    RetryConfig::default().initial_backoff.as_millis() as u64
}
fn default_buffer() -> usize {
    64
}
fn default_pod_log_dir() -> String {
    crate::source::k8s::DEFAULT_POD_LOG_DIR.to_owned()
}

impl Default for BatchSettings {
    fn default() -> Self {
        Self {
            max_events: default_max_events(),
            max_bytes: default_max_bytes(),
            timeout_secs: one(),
        }
    }
}

impl Default for RetrySettings {
    fn default() -> Self {
        Self {
            max_attempts: five(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_secs: thirty(),
        }
    }
}

impl Default for PipelineSettings {
    fn default() -> Self {
        Self {
            buffer: default_buffer(),
            require_healthy: false,
            on_error: OnErrorSetting::Stop,
        }
    }
}

fn required_section<T: DeserializeOwned>(mapping: &Mapping, name: &str) -> Result<T> {
    let value = mapping
        .get(name)
        .ok_or_else(|| Error::config(format!("缺少 `{name}` 配置")))?;
    section(value.clone(), name)
}

fn optional_section<T: DeserializeOwned + Default>(mapping: &Mapping, name: &str) -> Result<T> {
    match mapping.get(name) {
        // 整段留空（比如底下只有注释）就用默认值
        None | Some(Value::Null) => Ok(T::default()),
        Some(value) => section(value.clone(), name),
    }
}

fn section<T: DeserializeOwned>(value: Value, name: &str) -> Result<T> {
    serde_yaml_ng::from_value(value).map_err(|err| {
        let message = err.to_string();
        // source / sink 是按 type 分派的，字段填错多半是 type 和字段没对上
        let hint = if matches!(name, "source" | "sink") && message.contains("unknown field") {
            format!("；请检查 {name}.type 和下面的字段是否匹配")
        } else {
            String::new()
        };
        Error::config(format!("`{name}` 配置有问题: {message}{hint}"))
    })
}

/// 静态字段该建成什么列类型。
fn column_type_of(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Bool(_) => "UInt8".to_owned(),
        serde_json::Value::Number(n) if n.is_i64() || n.is_u64() => "Int64".to_owned(),
        serde_json::Value::Number(_) => "Float64".to_owned(),
        // 字符串多是 app / env 这类枚举值，低基数列更省
        _ => "LowCardinality(String)".to_owned(),
    }
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::config(format!("读取配置 {} 失败: {e}", path.display())))?;
        Self::parse(&text)
    }

    /// 逐段解析。
    ///
    /// 不用一次性 derive 是为了让报错能点名是哪一段出了问题 —— `source` / `sink`
    /// 是按 `type` 分派的枚举，serde 会把整段缓冲起来再解析，行号就丢了，
    /// 只报一句 "unknown field `endpoint`" 很难定位。
    pub fn parse(text: &str) -> Result<Self> {
        const SECTIONS: [&str; 7] = [
            "source", "parser", "sink", "batch", "retry", "pipeline", "fields",
        ];

        let root: Value = serde_yaml_ng::from_str(text)
            .map_err(|e| Error::config(format!("YAML 语法错误: {e}")))?;

        let mapping = match &root {
            Value::Mapping(mapping) => mapping,
            Value::Null => return Err(Error::config("配置是空的")),
            _ => return Err(Error::config("配置最外层应当是 key: value 形式")),
        };

        for key in mapping.keys() {
            let name = key.as_str().unwrap_or_default();
            if !SECTIONS.contains(&name) {
                return Err(Error::config(format!(
                    "未知的配置项 `{name}`；可用的有: {}",
                    SECTIONS.join(" / ")
                )));
            }
        }

        Ok(Self {
            source: required_section(mapping, "source")?,
            parser: optional_section(mapping, "parser")?,
            sink: required_section(mapping, "sink")?,
            batch: optional_section(mapping, "batch")?,
            retry: optional_section(mapping, "retry")?,
            pipeline: optional_section(mapping, "pipeline")?,
            fields: optional_section(mapping, "fields")?,
        })
    }

    /// ClickHouse sink 对应的建表语句，会带上容器元数据列和 `fields` 里的静态字段列。
    pub fn ddl(&self) -> Result<String> {
        match &self.sink {
            SinkConfig::Clickhouse { .. } => Ok(self
                .build_clickhouse()?
                .create_table_ddl_with(&self.extra_columns())),
            SinkConfig::Console { .. } => Err(Error::config("当前 sink 是 console，没有建表语句")),
        }
    }

    /// 除固定字段之外还会写入哪些列。
    fn extra_columns(&self) -> Vec<(String, String)> {
        let mut columns: Vec<(String, String)> = Vec::new();

        if self.source.emits_pod_metadata() {
            for (name, ty) in [
                ("stream", "LowCardinality(String)"),
                ("namespace", "LowCardinality(String)"),
                ("pod", "String"),
                ("container", "LowCardinality(String)"),
            ] {
                columns.push((name.to_owned(), ty.to_owned()));
            }
        }

        for (key, value) in &self.fields {
            columns.push((key.clone(), column_type_of(value)));
        }
        columns
    }

    fn build_clickhouse(&self) -> Result<ClickhouseSink> {
        let SinkConfig::Clickhouse {
            endpoint,
            database,
            table,
            cluster,
            user,
            password,
            async_insert,
            compress,
            timeout_secs,
        } = &self.sink
        else {
            return Err(Error::config("sink 不是 clickhouse"));
        };

        let mut sink = ClickhouseSink::new(endpoint, database, table)
            .timeout(Duration::from_secs(*timeout_secs))
            .async_insert(*async_insert)
            .compress(*compress);
        if let Some(cluster) = cluster {
            sink = sink.cluster(cluster);
        }
        if let Some(user) = user {
            sink = sink.auth(user, password.clone().unwrap_or_default());
        }
        Ok(sink)
    }

    /// 实际生效的格式列表。配置留空就是默认的 logback 一种。
    fn formats(&self) -> Vec<LogFormat> {
        if self.parser.formats.is_empty() {
            vec![LogFormat::Logback]
        } else {
            self.parser.formats.clone()
        }
    }

    fn build_parser(&self) -> Result<ChainParser> {
        let formats = self.formats();
        if self.parser.pattern.is_some() && !formats.contains(&LogFormat::Logback) {
            return Err(Error::config(
                "parser.pattern 是给 logback 格式用的，formats 里没有 logback 时它不会生效",
            ));
        }
        ChainParser::from_formats(&formats, self.parser.pattern.as_deref())
    }

    fn build_source(&self) -> Result<FileSource> {
        let parser = self.build_parser()?;

        let source = match &self.source {
            SourceConfig::Kubernetes(k8s) => {
                let mut selector = PodSelector::new()
                    .namespaces(&k8s.namespaces)?
                    .exclude_namespaces(&k8s.exclude_namespaces)?
                    .pods(&k8s.pods)?
                    .exclude_pods(&k8s.exclude_pods)?
                    .containers(&k8s.containers)?
                    .exclude_containers(&k8s.exclude_containers)?;
                if k8s.exclude_self {
                    selector = selector.exclude_self();
                }

                let mut source = FileSource::kubernetes_in(&k8s.log_dir)
                    .pod_selector(selector)
                    .parser(parser)
                    .read_from_beginning(k8s.read_from_beginning)
                    .glob_interval(Duration::from_secs(k8s.glob_interval_secs));
                if let Some(dir) = &k8s.data_dir {
                    source = source.data_dir(dir);
                }
                if let Some(host) = &k8s.host {
                    source = source.host(host);
                }
                source.fields(self.fields.clone())
            }
            SourceConfig::File(file) => {
                if file.include.is_empty() {
                    return Err(Error::config("source.include 不能为空"));
                }

                let mut source = FileSource::new(file.include.clone())
                    .exclude(file.exclude.clone())
                    .parser(parser)
                    .container_format(file.container_format)
                    .read_from_beginning(file.read_from_beginning)
                    .glob_interval(Duration::from_secs(file.glob_interval_secs));
                if let Some(dir) = &file.data_dir {
                    source = source.data_dir(dir);
                }
                if let Some(host) = &file.host {
                    source = source.host(host);
                }
                source.fields(self.fields.clone())
            }
        };

        Ok(source)
    }

    /// 启动前的静态校验：配置能不能组装出组件、位点目录能不能用。
    ///
    /// 位点目录会被真正创建 —— 与其等跑起来读了一段日志才发现存不下位点，
    /// 不如在 `--check` 阶段就失败。
    pub fn check(&self) -> Result<()> {
        self.build_source()?;
        if let SinkConfig::Clickhouse { .. } = &self.sink {
            self.build_clickhouse()?;
        }
        if let Some(dir) = self.data_dir() {
            Checkpointer::load(Some(Path::new(dir)))?;
        }
        Ok(())
    }

    /// 位点目录（如果配了）。
    pub fn data_dir(&self) -> Option<&str> {
        match &self.source {
            SourceConfig::Kubernetes(k8s) => k8s.data_dir.as_deref(),
            SourceConfig::File(file) => file.data_dir.as_deref(),
        }
    }

    /// 按配置组装出一条可运行的 pipeline。
    pub fn build(self) -> Result<Pipeline> {
        let source = self.build_source()?;

        let builder = Pipeline::builder()
            .source(source)
            .batch(
                BatchConfig::default()
                    .max_events(self.batch.max_events)
                    .max_bytes(self.batch.max_bytes)
                    .timeout(Duration::from_secs(self.batch.timeout_secs)),
            )
            .retry(RetryConfig {
                max_attempts: self.retry.max_attempts.max(1),
                initial_backoff: Duration::from_millis(self.retry.initial_backoff_ms),
                max_backoff: Duration::from_secs(self.retry.max_backoff_secs),
            })
            .buffer(self.pipeline.buffer)
            .require_healthy(self.pipeline.require_healthy)
            .on_error(match self.pipeline.on_error {
                OnErrorSetting::Stop => OnError::Stop,
                OnErrorSetting::Drop => OnError::Drop,
            });

        Ok(match &self.sink {
            SinkConfig::Clickhouse { .. } => builder.sink(self.build_clickhouse()?).build()?,
            SinkConfig::Console { encoding, stderr } => {
                let sink = ConsoleSink::new(match encoding {
                    ConsoleEncoding::Json => Encoding::Json,
                    ConsoleEncoding::Text => Encoding::Text,
                });
                let sink = if *stderr { sink.stderr() } else { sink };
                builder.sink(sink).build()?
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_file_config() {
        let config = Config::parse(
            r#"
source:
  type: file
  include:
    - /var/log/app/*.log
sink:
  type: console
"#,
        )
        .unwrap();

        assert!(!config.source.emits_pod_metadata());
        assert_eq!(config.batch.timeout_secs, 1);
        assert_eq!(config.pipeline.on_error, OnErrorSetting::Stop);
        config.build().unwrap();
    }

    #[test]
    fn kubernetes_source_needs_no_paths() {
        let config = Config::parse(
            r#"
source:
  type: kubernetes
sink:
  type: console
"#,
        )
        .unwrap();

        let SourceConfig::Kubernetes(k8s) = &config.source else {
            panic!("应当是 kubernetes source");
        };
        // 路径、格式、排除自己都有默认值，不用配
        assert_eq!(k8s.log_dir, "/var/log/pods");
        assert!(k8s.exclude_self);
        assert!(!k8s.read_from_beginning);
        assert!(k8s.namespaces.is_empty(), "留空表示全采");
        config.build().unwrap();
    }

    #[test]
    fn kubernetes_selector_is_declared_in_k8s_terms() {
        let config = Config::parse(
            r#"
source:
  type: kubernetes
  namespaces: [prod, staging]
  exclude_namespaces: ["kube-*"]
  pods: ["order-*"]
  exclude_containers: [istio-proxy]
  data_dir: /var/lib/logpipe

sink:
  type: clickhouse
  endpoint: http://clickhouse:8123
  database: logs
  table: app_log

fields:
  cluster: bj-prod
  replica: 3
"#,
        )
        .unwrap();

        let SourceConfig::Kubernetes(k8s) = &config.source else {
            panic!("应当是 kubernetes source");
        };
        assert_eq!(k8s.namespaces, ["prod", "staging"]);
        assert_eq!(k8s.exclude_containers, ["istio-proxy"]);

        // k8s 元数据列和静态字段列都自动进 DDL
        let ddl = config.ddl().unwrap();
        for column in [
            "`stream`",
            "`namespace`",
            "`pod`",
            "`container`",
            "`cluster`",
        ] {
            assert!(ddl.contains(column), "DDL 少了 {column}:\n{ddl}");
        }
        assert!(ddl.contains("`replica`   Int64"));
        config.build().unwrap();
    }

    #[test]
    fn cluster_ddl_is_replicated_plus_distributed() {
        let config = Config::parse(
            r#"
source:
  type: kubernetes
sink:
  type: clickhouse
  endpoint: http://ck-lb:8123
  database: logs
  table: app_log
  cluster: bj_ck
"#,
        )
        .unwrap();

        let ddl = config.ddl().unwrap();
        // 本地表存数据，Distributed 表是 sink 实际写入的那张
        assert!(
            ddl.contains("`logs`.`app_log_local` ON CLUSTER `bj_ck`"),
            "{ddl}"
        );
        assert!(ddl.contains("ENGINE = ReplicatedMergeTree"), "{ddl}");
        assert!(
            ddl.contains("Distributed(`bj_ck`, `logs`, `app_log_local`, rand())"),
            "{ddl}"
        );
        // 两条语句，中间要有分号，不然 --ddl 出来的脚本没法直接执行
        assert_eq!(ddl.matches("CREATE TABLE").count(), 2, "{ddl}");
        assert!(ddl.contains(";"), "{ddl}");
        // 宏留给 ClickHouse 自己展开
        assert!(
            ddl.contains("{shard}") && ddl.contains("{replica}"),
            "{ddl}"
        );

        // 不配 cluster 还是单机 MergeTree
        let single = Config::parse(
            r#"
source:
  type: kubernetes
sink:
  type: clickhouse
  endpoint: http://127.0.0.1:8123
  database: logs
  table: app_log
"#,
        )
        .unwrap()
        .ddl()
        .unwrap();
        assert!(single.contains("ENGINE = MergeTree"), "{single}");
        assert!(!single.contains("ON CLUSTER"), "{single}");
        assert!(!single.contains("app_log_local"), "{single}");
    }

    #[test]
    fn access_log_formats_do_not_change_the_schema() {
        let config = Config::parse(
            r#"
source:
  type: kubernetes
parser:
  formats: [logback, gin, nginx]
sink:
  type: clickhouse
  endpoint: http://clickhouse:8123
  database: logs
  table: app_log
"#,
        )
        .unwrap();

        assert_eq!(
            config.formats(),
            [LogFormat::Logback, LogFormat::Gin, LogFormat::Nginx]
        );

        // access log 走 level / logger / message 这几个现成的列，
        // 配了新格式不该要求线上表 ALTER
        let ddl = config.ddl().unwrap();
        for column in ["`status`", "`path`", "`latency_us`", "`user_agent`"] {
            assert!(!ddl.contains(column), "多出了 {column}:\n{ddl}");
        }
        config.build().unwrap();
    }

    #[test]
    fn rejects_pattern_without_logback_format() {
        let err = Config::parse(
            r#"
source:
  type: kubernetes
parser:
  formats: [gin]
  pattern: '^(?P<timestamp>\S+ \S+) (?P<level>[A-Z]+) (?P<message>.*)$'
sink:
  type: console
"#,
        )
        .unwrap()
        .check()
        .expect_err("pattern 配了却不生效应当报错");
        assert!(err.to_string().contains("logback"), "{err}");
    }

    #[test]
    fn rejects_unknown_format() {
        let err = Config::parse(
            r#"
source:
  type: kubernetes
parser:
  formats: [log4j]
sink:
  type: console
"#,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("`parser` 配置有问题"), "{err}");
    }

    #[test]
    fn check_rejects_unusable_data_dir() {
        let dir = tempfile::tempdir().unwrap();
        let occupied = dir.path().join("occupied");
        std::fs::write(&occupied, b"x").unwrap();

        let err = Config::parse(&format!(
            r#"
source:
  type: kubernetes
  data_dir: {}
sink:
  type: console
"#,
            occupied.display()
        ))
        .unwrap()
        .check()
        .expect_err("位点目录不可用应当报错");
        assert!(err.to_string().contains("创建位点目录"), "{err}");
    }

    #[test]
    fn rejects_bad_selector_pattern() {
        let err = Config::parse(
            r#"
source:
  type: kubernetes
  namespaces: ['prod[']
sink:
  type: console
"#,
        )
        .unwrap()
        .build()
        .err()
        .expect("非法匹配式应当报错");
        assert!(err.to_string().contains("名字匹配式非法"), "{err}");
    }

    #[test]
    fn rejects_unknown_keys() {
        let err = Config::parse(
            r#"
source:
  type: file
  include:
    - a.log
  typo_here: 1
sink:
  type: console
"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("`source` 配置有问题"), "{err}");
    }

    #[test]
    fn rejects_unknown_source_type() {
        let err = Config::parse(
            r#"
source:
  type: kafka
sink:
  type: console
"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("`source` 配置有问题"), "{err}");
    }

    #[test]
    fn names_the_section_and_hints_type_mismatch() {
        // console sink 配了 clickhouse 的字段
        let err = Config::parse(
            r#"
source:
  type: kubernetes
sink:
  type: console
  endpoint: http://127.0.0.1:8123
"#,
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("`sink` 配置有问题"), "{err}");
        assert!(err.contains("unknown field `endpoint`"), "{err}");
        assert!(err.contains("sink.type"), "{err}");
    }

    #[test]
    fn rejects_unknown_top_level_section() {
        let err = Config::parse(
            r#"
source:
  type: kubernetes
sink:
  type: console
sinks:
  type: console
"#,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("未知的配置项 `sinks`"), "{err}");
    }

    #[test]
    fn reports_missing_section() {
        let err = Config::parse("source:\n  type: kubernetes\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("缺少 `sink` 配置"), "{err}");
    }
}
