//! 打到标准输出，调试用。

use async_trait::async_trait;
use tokio::io::{AsyncWriteExt, Stderr, Stdout};

use crate::error::Result;
use crate::event::LogEvent;
use crate::sink::Sink;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
    /// 一行一个 JSON，和落库格式一致。
    Json,
    /// 还原成原始日志样式。
    Text,
}

pub struct ConsoleSink {
    encoding: Encoding,
    target: Target,
}

enum Target {
    Stdout(Stdout),
    Stderr(Stderr),
}

impl ConsoleSink {
    pub fn new(encoding: Encoding) -> Self {
        Self {
            encoding,
            target: Target::Stdout(tokio::io::stdout()),
        }
    }

    pub fn stderr(mut self) -> Self {
        self.target = Target::Stderr(tokio::io::stderr());
        self
    }
}

impl Default for ConsoleSink {
    fn default() -> Self {
        Self::new(Encoding::Json)
    }
}

#[async_trait]
impl Sink for ConsoleSink {
    async fn write(&mut self, events: &[LogEvent]) -> Result<()> {
        let mut buf = String::with_capacity(events.len() * 256);
        for event in events {
            match self.encoding {
                Encoding::Json => buf.push_str(&event.to_json_line()?),
                Encoding::Text => buf.push_str(&event.to_text_line()),
            }
            buf.push('\n');
        }

        match &mut self.target {
            Target::Stdout(out) => {
                out.write_all(buf.as_bytes()).await?;
                out.flush().await?;
            }
            Target::Stderr(out) => {
                out.write_all(buf.as_bytes()).await?;
                out.flush().await?;
            }
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "console"
    }
}
