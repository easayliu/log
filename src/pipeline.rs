//! 把 source 和 sink 串起来：攒批、重试、优雅退出。

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::batch::{BatchConfig, RetryConfig};
use crate::error::{Error, Result};
use crate::event::LogEvent;
use crate::shutdown::{self, Shutdown, ShutdownHandle};
use crate::sink::Sink;
use crate::source::{Source, SourceSender};

/// 逐条改写事件；返回 `None` 表示丢弃这条日志。
pub type Transform = Box<dyn FnMut(LogEvent) -> Option<LogEvent> + Send>;

/// 一批数据重试耗尽后怎么办。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnError {
    /// 停掉整条 pipeline。位点不会推进，重启后从上次成功处重读。默认。
    Stop,
    /// 丢掉这批继续跑。**会丢数据**，只适合可容忍丢失的场景。
    Drop,
}

pub struct Pipeline {
    source: Box<dyn Source>,
    sink: Box<dyn Sink>,
    transforms: Vec<Transform>,
    batch: BatchConfig,
    retry: RetryConfig,
    buffer: usize,
    require_healthy: bool,
    on_error: OnError,
}

impl Pipeline {
    pub fn builder() -> PipelineBuilder {
        PipelineBuilder::default()
    }

    /// 前台运行，直到日志读完、出错，或者收到 Ctrl-C。
    pub async fn run(self) -> Result<()> {
        let (handle, shutdown) = shutdown::channel();
        let mut task = tokio::spawn(self.run_inner(shutdown));

        tokio::select! {
            result = &mut task => flatten(result),
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("收到 Ctrl-C，开始收尾");
                handle.trigger();
                flatten(task.await)
            }
        }
    }

    /// 后台运行，返回的句柄可以随时停。
    pub fn spawn(self) -> RunningPipeline {
        let (handle, shutdown) = shutdown::channel();
        RunningPipeline {
            handle,
            task: tokio::spawn(self.run_inner(shutdown)),
        }
    }

    async fn run_inner(self, shutdown: Shutdown) -> Result<()> {
        let Self {
            source,
            mut sink,
            mut transforms,
            batch,
            retry,
            buffer,
            require_healthy,
            on_error,
        } = self;

        if let Err(err) = sink.healthcheck().await {
            if require_healthy {
                return Err(err);
            }
            tracing::warn!(sink = sink.name(), %err, "healthcheck 未通过，仍然继续启动");
        }

        let (tx, mut rx) = mpsc::channel(buffer);
        let source_name = source.name();

        // 内部信号：外部 Ctrl-C / stop() 会转发到这里，pipeline 自己出错时也用它
        // 叫停 source，否则 source 会在没人接收的情况下空转。
        let (stop_source, source_shutdown) = shutdown::channel();
        tokio::spawn({
            let stop_source = stop_source.clone();
            async move {
                shutdown.cancelled().await;
                stop_source.trigger();
            }
        });

        let mut source_task: JoinHandle<Result<()>> =
            tokio::spawn(source.run(SourceSender::new(tx), source_shutdown));

        let mut pending = Pending::default();
        let mut deadline: Option<Instant> = None;

        tracing::info!(source = source_name, sink = sink.name(), "pipeline 启动");

        let result: Result<()> = async {
            loop {
                let timer = async {
                    match deadline {
                        Some(at) => tokio::time::sleep_until(at).await,
                        None => std::future::pending().await,
                    }
                };

                tokio::select! {
                    incoming = rx.recv() => match incoming {
                        Some(mut incoming) => {
                            for event in incoming.events.drain(..) {
                                if let Some(event) = apply(&mut transforms, event) {
                                    pending.push(event);
                                }
                            }
                            if let Some(ack) = incoming.ack.take() {
                                pending.acks.push(ack);
                            }

                            if deadline.is_none() {
                                deadline = Some(Instant::now() + batch.timeout);
                            }
                            if pending.is_full(&batch) {
                                flush(&mut sink, &mut pending, &retry, on_error).await?;
                                deadline = None;
                            }
                        }
                        // source 结束（读完 / 退出信号 / 出错），冲刷剩余数据。
                        None => {
                            flush(&mut sink, &mut pending, &retry, on_error).await?;
                            break;
                        }
                    },
                    _ = timer => {
                        flush(&mut sink, &mut pending, &retry, on_error).await?;
                        deadline = None;
                    }
                }
            }
            Ok(())
        }
        .await;

        // 通知 source 收工，并关掉接收端：它下一次发送会立刻失败，不至于卡在背压上。
        stop_source.trigger();
        rx.close();
        drop(rx);
        // 释放还没回执的 ack：对应批次没能落库，source 会保留原位点。
        // 必须在等 source 退出之前丢掉，否则 source 会一直等这些回执。
        drop(pending);

        match result {
            Ok(()) => flatten(source_task.await),
            Err(err) => {
                let _ = (&mut source_task).await;
                Err(err)
            }
        }
    }
}

/// 后台运行中的 pipeline。
pub struct RunningPipeline {
    handle: ShutdownHandle,
    task: JoinHandle<Result<()>>,
}

impl RunningPipeline {
    /// 触发优雅退出并等待收尾（剩余数据会先写完）。
    pub async fn stop(self) -> Result<()> {
        self.handle.trigger();
        flatten(self.task.await)
    }

    /// 等它自己跑完（比如文件读到 EOF、stdin 关闭）。
    pub async fn wait(self) -> Result<()> {
        flatten(self.task.await)
    }

    pub fn shutdown_handle(&self) -> ShutdownHandle {
        self.handle.clone()
    }
}

#[derive(Default)]
struct Pending {
    events: Vec<LogEvent>,
    acks: Vec<oneshot::Sender<()>>,
    bytes: usize,
}

impl Pending {
    fn push(&mut self, event: LogEvent) {
        self.bytes += event.estimated_size();
        self.events.push(event);
    }

    fn is_full(&self, batch: &BatchConfig) -> bool {
        self.events.len() >= batch.max_events || self.bytes >= batch.max_bytes
    }

    fn clear(&mut self) {
        self.events.clear();
        self.acks.clear();
        self.bytes = 0;
    }
}

/// 顺序执行 transform，任一环节返回 `None` 就丢弃这条日志。
fn apply(transforms: &mut [Transform], event: LogEvent) -> Option<LogEvent> {
    let mut current = event;
    for transform in transforms.iter_mut() {
        current = transform(current)?;
    }
    Some(current)
}

async fn flush(
    sink: &mut Box<dyn Sink>,
    pending: &mut Pending,
    retry: &RetryConfig,
    on_error: OnError,
) -> Result<()> {
    if pending.events.is_empty() {
        // 没有数据但可能攒了 ack（整批都被 transform 丢掉了），照样回执。
        for ack in pending.acks.drain(..) {
            let _ = ack.send(());
        }
        return Ok(());
    }

    let mut attempt = 1;
    let outcome = loop {
        match sink.write(&pending.events).await {
            Ok(()) => break Ok(()),
            Err(err) if attempt < retry.max_attempts => {
                let backoff = retry.backoff(attempt);
                tracing::warn!(
                    sink = sink.name(),
                    attempt,
                    ?backoff,
                    %err,
                    "写入失败，稍后重试"
                );
                tokio::time::sleep(backoff).await;
                attempt += 1;
            }
            Err(err) => break Err(err),
        }
    };

    match outcome {
        Ok(()) => {
            tracing::debug!(count = pending.events.len(), "落库成功");
            // 先回 ack，source 据此推进位点。
            for ack in pending.acks.drain(..) {
                let _ = ack.send(());
            }
            pending.clear();
            Ok(())
        }
        Err(err) => {
            let count = pending.events.len();
            match on_error {
                OnError::Stop => {
                    tracing::error!(count, %err, "重试耗尽，停止 pipeline（位点不推进）");
                    Err(err)
                }
                OnError::Drop => {
                    tracing::error!(count, %err, "重试耗尽，丢弃这批数据");
                    // 不回 ack：位点留在原地，重启后这段会被重读。
                    pending.clear();
                    Ok(())
                }
            }
        }
    }
}

fn flatten(result: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    match result {
        Ok(inner) => inner,
        Err(err) => Err(Error::other(format!("任务异常退出: {err}"))),
    }
}

#[derive(Default)]
pub struct PipelineBuilder {
    source: Option<Box<dyn Source>>,
    sink: Option<Box<dyn Sink>>,
    transforms: Vec<Transform>,
    batch: Option<BatchConfig>,
    retry: Option<RetryConfig>,
    buffer: Option<usize>,
    require_healthy: bool,
    on_error: Option<OnError>,
}

impl PipelineBuilder {
    pub fn source(mut self, source: impl Source) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    pub fn sink(mut self, sink: impl Sink) -> Self {
        self.sink = Some(Box::new(sink));
        self
    }

    /// 逐条改写/过滤事件，可以叠加多个，按添加顺序执行。
    pub fn transform<F>(mut self, transform: F) -> Self
    where
        F: FnMut(LogEvent) -> Option<LogEvent> + Send + 'static,
    {
        self.transforms.push(Box::new(transform));
        self
    }

    pub fn batch(mut self, batch: BatchConfig) -> Self {
        self.batch = Some(batch);
        self
    }

    pub fn retry(mut self, retry: RetryConfig) -> Self {
        self.retry = Some(retry);
        self
    }

    /// source 与 sink 之间的队列深度（按批计）。队列满了 source 会被自然阻塞。
    pub fn buffer(mut self, buffer: usize) -> Self {
        self.buffer = Some(buffer.max(1));
        self
    }

    /// healthcheck 失败就不启动。
    pub fn require_healthy(mut self, require: bool) -> Self {
        self.require_healthy = require;
        self
    }

    pub fn on_error(mut self, on_error: OnError) -> Self {
        self.on_error = Some(on_error);
        self
    }

    pub fn build(self) -> Result<Pipeline> {
        Ok(Pipeline {
            source: self.source.ok_or_else(|| Error::config("缺少 source"))?,
            sink: self.sink.ok_or_else(|| Error::config("缺少 sink"))?,
            transforms: self.transforms,
            batch: self.batch.unwrap_or_default(),
            retry: self.retry.unwrap_or_default(),
            buffer: self.buffer.unwrap_or(64),
            require_healthy: self.require_healthy,
            on_error: self.on_error.unwrap_or(OnError::Stop),
        })
    }
}
