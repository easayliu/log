//! 采集日志打到控制台，不需要任何外部依赖，最快验证解析是否正确。
//!
//! ```bash
//! cargo run --example tail_to_console -- '/var/log/app/*.log'
//! ```

use std::time::Duration;

use logpipe::batch::BatchConfig;
use logpipe::sink::console::Encoding;
use logpipe::sink::ConsoleSink;
use logpipe::source::FileSource;
use logpipe::Pipeline;

#[tokio::main]
async fn main() -> logpipe::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let includes: Vec<String> = std::env::args().skip(1).collect();
    if includes.is_empty() {
        eprintln!("用法: tail_to_console '<glob>' [<glob> ...]");
        std::process::exit(2);
    }

    Pipeline::builder()
        .source(FileSource::new(includes).read_from_beginning(true))
        .sink(ConsoleSink::new(Encoding::Json))
        .batch(BatchConfig::default().timeout(Duration::from_millis(200)))
        .build()?
        .run()
        .await
}
