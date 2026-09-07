//! 从标准输入读日志。调试用，也可以配合 `tail -F xxx | myapp` 使用。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::error::Result;
use crate::event::LogEvent;
use crate::parser::{Aggregator, Parser, RegexParser};
use crate::shutdown::Shutdown;
use crate::source::{Source, SourceSender};

pub struct StdinSource {
    parser: Arc<dyn Parser>,
    host: Arc<str>,
    file: Arc<str>,
    batch_lines: usize,
    flush_interval: Duration,
}

impl StdinSource {
    pub fn new() -> Self {
        Self {
            parser: Arc::new(RegexParser::new()),
            host: Arc::from(super::file::hostname()),
            file: Arc::from("stdin"),
            batch_lines: 500,
            flush_interval: Duration::from_millis(500),
        }
    }

    pub fn parser(mut self, parser: impl Parser) -> Self {
        self.parser = Arc::new(parser);
        self
    }

    pub fn batch_lines(mut self, batch_lines: usize) -> Self {
        self.batch_lines = batch_lines.max(1);
        self
    }
}

impl Default for StdinSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Source for StdinSource {
    async fn run(self: Box<Self>, out: SourceSender, shutdown: Shutdown) -> Result<()> {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        let mut aggregator = Aggregator::new(Arc::clone(&self.parser));
        let mut pending: Vec<LogEvent> = Vec::with_capacity(self.batch_lines);
        let mut ticker = tokio::time::interval(self.flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                line = lines.next_line() => match line? {
                    Some(line) => {
                        if let Some(event) = aggregator.push(&line) {
                            pending.push(self.decorate(event));
                        }
                        if pending.len() >= self.batch_lines {
                            out.send(std::mem::take(&mut pending)).await?;
                        }
                    }
                    None => break, // EOF
                },
                _ = ticker.tick() => {
                    // 交互式输入时，最后一条日志可能一直等不到下一行，定期收口。
                    if let Some(event) = aggregator.flush() {
                        pending.push(self.decorate(event));
                    }
                    if !pending.is_empty() {
                        out.send(std::mem::take(&mut pending)).await?;
                    }
                }
            }
        }

        if let Some(event) = aggregator.flush() {
            pending.push(self.decorate(event));
        }
        out.send(pending).await?;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "stdin"
    }
}

impl StdinSource {
    fn decorate(&self, mut event: LogEvent) -> LogEvent {
        event.file = Arc::clone(&self.file);
        event.host = Arc::clone(&self.host);
        event
    }
}
