//! 采集端：产出日志批次，并在数据成功落库后收到 ack。

pub mod checkpoint;
pub mod file;
pub mod k8s;
pub mod stdin;

pub use file::FileSource;
pub use k8s::PodMeta;
pub use stdin::StdinSource;

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

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
    /// 这批数据占用的在途字节配额，见 [`SourceSender`]。批次写完（或被丢弃）时
    /// 随之释放，配额才回到 source 手里 —— 背压就是这么形成的。
    pub(crate) permit: Option<OwnedSemaphorePermit>,
}

impl Batch {
    pub fn new(events: Vec<LogEvent>) -> Self {
        Self {
            events,
            ack: None,
            permit: None,
        }
    }

    pub fn with_ack(events: Vec<LogEvent>) -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                events,
                ack: Some(tx),
                permit: None,
            },
            rx,
        )
    }
}

/// source 往下游发数据的入口。
///
/// 背压有两道闸：有界的 channel（按**批**计）和在途字节配额（按**字节**计）。
/// 只有前者的话上限是「批数 × 每批条数」，一条日志可以是 20 字节也可以是合并了
/// 堆栈的 256KiB，真实内存占用能差四个数量级 —— 存储抖一下，队列就把内存吃穿。
/// 配额按 [`LogEvent::estimated_size`] 计量，发之前先申请，批次落库后释放，
/// 于是「在途的日志」有了一个跟条数无关的硬上限。
///
/// 注意配额算的是 JSON 体积，不是 RSS：一条事件在内存里是结构体 + 六七次小分配，
/// malloc 向上取整之后大致是估算值的两倍。定上限时按这个折算。
#[derive(Clone, Debug)]
pub struct SourceSender {
    tx: mpsc::Sender<Batch>,
    quota: Arc<Semaphore>,
    /// 配额总量。单批比它还大时按总量收，否则会永远等不到许可。
    capacity: u32,
}

impl SourceSender {
    pub(crate) fn new(tx: mpsc::Sender<Batch>, quota: Arc<Semaphore>, capacity: u32) -> Self {
        Self {
            tx,
            quota,
            capacity,
        }
    }

    /// 发送一批日志，不关心是否落库。
    pub async fn send(&self, events: Vec<LogEvent>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let permit = self.reserve(&events).await?;
        let mut batch = Batch::new(events);
        batch.permit = Some(permit);
        self.tx
            .send(batch)
            .await
            .map_err(|_| Error::other("下游已关闭"))
    }

    /// 发送一批日志，返回的 receiver 会在落库成功后被唤醒。
    pub async fn send_with_ack(&self, events: Vec<LogEvent>) -> Result<oneshot::Receiver<()>> {
        let permit = self.reserve(&events).await?;
        let (mut batch, ack) = Batch::with_ack(events);
        batch.permit = Some(permit);
        self.tx
            .send(batch)
            .await
            .map_err(|_| Error::other("下游已关闭"))?;
        Ok(ack)
    }

    /// 申请这批数据的在途配额，配额不够就在这里等 —— source 的读取循环随之停住。
    async fn reserve(&self, events: &[LogEvent]) -> Result<OwnedSemaphorePermit> {
        let want: usize = events.iter().map(LogEvent::estimated_size).sum();
        let want = want.clamp(1, self.capacity as usize) as u32;
        Arc::clone(&self.quota)
            .acquire_many_owned(want)
            .await
            // 只有 pipeline 收尾时会 close：正在等配额的 source 立刻醒过来，
            // 不用干等到下游把队列排空。
            .map_err(|_| Error::other("下游已关闭"))
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
