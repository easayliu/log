//! 可执行入口：读一份 YAML 配置，把日志采上来写进存储。
//!
//! ```bash
//! logpipe                     # 读当前目录的 logpipe.yaml
//! logpipe /etc/logpipe.yaml   # 指定配置
//! logpipe --check <config>    # 只校验配置
//! logpipe --ddl   <config>    # 打印 ClickHouse 建表语句
//! ```

use std::process::ExitCode;

use logpipe::config::Config;

const DEFAULT_CONFIG: &str = "logpipe.yaml";

const USAGE: &str = "\
logpipe —— 日志采集入库

用法:
    logpipe [配置文件]           启动采集（默认 ./logpipe.yaml）
    logpipe --check [配置文件]   只校验配置，不启动
    logpipe --ddl   [配置文件]   打印 ClickHouse 建表语句
    logpipe --help

日志级别用 RUST_LOG 控制，例如 RUST_LOG=debug。
";

// 解析全在 FileSource 的那一个 task 里串行跑，多起 worker 并不会更快；而默认是
// 按机器核数起线程，DaemonSet 跑在几十核的节点上就会有几十个线程去抢那点 cpu 配额。
#[tokio::main(worker_threads = 2)]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    match run().await {
        Ok(code) => code,
        Err(err) => {
            eprintln!("错误: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> logpipe::Result<ExitCode> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (mode, path) = match args.split_first() {
        None => (Mode::Run, DEFAULT_CONFIG.to_owned()),
        Some((first, rest)) => {
            let path = rest
                .first()
                .cloned()
                .unwrap_or_else(|| DEFAULT_CONFIG.to_owned());
            match first.as_str() {
                "--help" | "-h" => {
                    print!("{USAGE}");
                    return Ok(ExitCode::SUCCESS);
                }
                "--check" => (Mode::Check, path),
                "--ddl" => (Mode::Ddl, path),
                other if other.starts_with('-') => {
                    eprint!("未知参数 {other}\n\n{USAGE}");
                    return Ok(ExitCode::from(2));
                }
                config => (Mode::Run, config.to_owned()),
            }
        }
    };

    let config = Config::load(&path)?;

    match mode {
        Mode::Check => {
            // 会校验 glob、正则、选择器，并确认位点目录可用。
            config.check()?;
            println!("配置 {path} 校验通过");
            Ok(ExitCode::SUCCESS)
        }
        Mode::Ddl => {
            println!("{};", config.ddl()?);
            Ok(ExitCode::SUCCESS)
        }
        Mode::Run => {
            tracing::info!(config = %path, "启动 logpipe");
            config.build()?.run().await?;
            tracing::info!("已退出");
            Ok(ExitCode::SUCCESS)
        }
    }
}

enum Mode {
    Run,
    Check,
    Ddl,
}
