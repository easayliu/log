//! 采集本机所有容器的 stdout/stderr 写入 ClickHouse（k8s + containerd）。
//!
//! 路径、日志格式、k8s 元数据都不用配，只声明「采哪些 namespace」：
//!
//! ```bash
//! export CH_ENDPOINT=http://clickhouse:8123
//! cargo run --example kubernetes_to_clickhouse -- prod staging
//! ```

use std::time::Duration;

use logpipe::batch::BatchConfig;
use logpipe::sink::ClickhouseSink;
use logpipe::source::k8s::PodSelector;
use logpipe::source::FileSource;
use logpipe::Pipeline;

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

#[tokio::main]
async fn main() -> logpipe::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // 命令行给的就是要采的 namespace，不给则全采
    let namespaces: Vec<String> = std::env::args().skip(1).collect();

    let selector = PodSelector::new()
        .namespaces(&namespaces)?
        .exclude_namespaces(["kube-*"])?
        .exclude_containers(["istio-proxy"])?;

    Pipeline::builder()
        .source(
            FileSource::kubernetes()
                .pod_selector(selector)
                // service_name 取 pod 的 app label，要能访问 API server（RBAC 见 deploy/）
                .service_name_label("app")
                .data_dir("/var/lib/logpipe"),
        )
        .sink(ClickhouseSink::new(
            env("CH_ENDPOINT", "http://127.0.0.1:8123"),
            env("CH_DATABASE", "logs"),
            env("CH_TABLE", "app_log"),
        ))
        .batch(BatchConfig::default().timeout(Duration::from_secs(2)))
        .require_healthy(true)
        .build()?
        .run()
        .await
}
