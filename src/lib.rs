//! logpipe —— 一个精简的日志采集入库框架。
//!
//! 数据流只有三段：
//!
//! ```text
//!   Source ──(Batch)──▶ Pipeline ──(攒批 / 重试)──▶ Sink
//!   采集日志            批处理与背压                入库
//! ```
//!
//! * [`Source`] 负责产出日志（tail 文件、读 stdin……），并在数据落库后收到 ack；
//! * [`Sink`] 负责把一批日志写进存储（ClickHouse、控制台……）；
//! * [`Pipeline`] 负责攒批、重试、优雅退出，把两者串起来。
//!
//! ```no_run
//! use logpipe::{Pipeline, sink::ClickhouseSink, source::FileSource};
//!
//! # async fn run() -> logpipe::Result<()> {
//! Pipeline::builder()
//!     .source(FileSource::new(vec!["/var/log/app/*.log"]).data_dir("/var/lib/logpipe"))
//!     .sink(ClickhouseSink::new("http://127.0.0.1:8123", "logs", "app_log"))
//!     .build()?
//!     .run()
//!     .await
//! # }
//! ```

pub mod batch;
pub mod config;
pub mod error;
pub mod event;
pub mod parser;
pub mod pipeline;
pub mod shutdown;
pub mod sink;
pub mod source;

pub use error::{Error, Result};
pub use event::LogEvent;
pub use parser::{Aggregator, ChainParser, LogFormat, LogbackParser, Parser, RegexParser};
pub use pipeline::{Pipeline, PipelineBuilder, RunningPipeline};
pub use shutdown::{Shutdown, ShutdownHandle};
pub use sink::Sink;
pub use source::{Batch, Source, SourceSender};
