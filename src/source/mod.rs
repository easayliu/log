//! 采集端：产出日志批次，并在数据成功落库后收到 ack。

pub mod checkpoint;
pub mod file;
pub mod k8s;
pub mod stdin;

pub use file::FileSource;
pub use k8s::PodMeta;
pub use stdin::StdinSource;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use crate::error::{Error, Result};
use crate::event::LogEvent;
use crate::shutdown::Shutdown;

/// 一批日志，可选携带一个 ack 通道。
///
/// pipeline 会在这批数据**确实写进存储之后**才回 ack，
/// file source 靠它推进 checkpoint，从而保证「至少一次」投递。
pub struct Batch {
    pub events: Vec<LogEvent>,
    pub(crate) ack: Option<oneshot::Sender<()>>,
}

impl Batch {
    pub fn new(events: Vec<LogEvent>) -> Self {
        Self { events, ack: None }
    }

    pub fn with_ack(events: Vec<LogEvent>) -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                events,
                ack: Some(tx),
            },
            rx,
        )
    }
}

/// source 往下游发数据的入口。channel 有界，天然形成背压。
#[derive(Clone, Debug)]
pub struct SourceSender {
    tx: mpsc::Sender<Batch>,
}

impl SourceSender {
    pub(crate) fn new(tx: mpsc::Sender<Batch>) -> Self {
        Self { tx }
    }

    /// 发送一批日志，不关心是否落库。
    pub async fn send(&self, events: Vec<LogEvent>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        self.tx
            .send(Batch::new(events))
            .await
            .map_err(|_| Error::other("下游已关闭"))
    }

    /// 发送一批日志，返回的 receiver 会在落库成功后被唤醒。
    pub async fn send_with_ack(&self, events: Vec<LogEvent>) -> Result<oneshot::Receiver<()>> {
        let (batch, ack) = Batch::with_ack(events);
        self.tx
            .send(batch)
            .await
            .map_err(|_| Error::other("下游已关闭"))?;
        Ok(ack)
    }
}

#[async_trait]
pub trait Source: Send + 'static {
    /// 持续产出日志，直到数据读完或收到退出信号。
    async fn run(self: Box<Self>, out: SourceSender, shutdown: Shutdown) -> Result<()>;

    fn name(&self) -> &'static str {
        "source"
    }
}
