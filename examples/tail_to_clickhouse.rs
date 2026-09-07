//! 采集日志文件写入 ClickHouse。
//!
//! ```bash
//! export CH_ENDPOINT=http://127.0.0.1:8123
//! export CH_DATABASE=logs
//! export CH_TABLE=app_log
//! export CH_USER=default CH_PASSWORD=
//! cargo run --example tail_to_clickhouse -- '/var/log/app/*.log'
//! ```
//!
//! 第一次跑之前先建表：程序会在 `--ddl` 参数下把建表语句打出来。

use std::time::Duration;

use logpipe::batch::{BatchConfig, RetryConfig};
use logpipe::sink::ClickhouseSink;
use logpipe::source::FileSource;
use logpipe::{LogEvent, Pipeline};

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

    let mut sink = ClickhouseSink::new(
        env("CH_ENDPOINT", "http://127.0.0.1:8123"),
        env("CH_DATABASE", "logs"),
        env("CH_TABLE", "app_log"),
    )
    .timeout(Duration::from_secs(30));

    if let (Ok(user), Ok(password)) = (std::env::var("CH_USER"), std::env::var("CH_PASSWORD")) {
        sink = sink.auth(user, password);
    }

    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "--ddl") {
        println!("{};", sink.create_table_ddl());
        return Ok(());
    }
    if args.is_empty() {
        eprintln!("用法: tail_to_clickhouse ['--ddl' | '<glob>' ...]");
        std::process::exit(2);
    }

    Pipeline::builder()
        .source(
            FileSource::new(args)
                // 位点落盘，重启后从上次成功入库的位置继续。
                .data_dir("/var/lib/logpipe")
                .exclude(["*.gz", "*.1"]),
        )
        // 给每条日志补一个应用名，方便多服务共用一张表。
        .transform(|mut event: LogEvent| {
            event.insert("app", env("APP_NAME", "unknown"));
            Some(event)
        })
        .sink(sink)
        .batch(
            BatchConfig::default()
                .max_events(20_000)
                .timeout(Duration::from_secs(2)),
        )
        .retry(RetryConfig::default())
        // ClickHouse 不可用时不启动，避免白读一段日志
        .require_healthy(true)
        .build()?
        .run()
        .await
}
