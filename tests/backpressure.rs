//! 背压：在途日志有一个和条数无关的字节上限。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use logpipe::batch::BatchConfig;
use logpipe::{LogEvent, Pipeline, Shutdown, Sink, Source, SourceSender};
use tokio::sync::Semaphore;

/// 一条 256KiB（合并了堆栈的日志就是这个量级），一批 10 条 ≈ 2.5MiB。
const EVENT_BYTES: usize = 256 * 1024;
const EVENTS_PER_BATCH: usize = 10;
const MAX_INFLIGHT: usize = 4 * 1024 * 1024;

/// 拼命往下游灌，并记账「发出去但还没落库」的字节数。
struct FloodSource {
    sent: Arc<AtomicUsize>,
}

#[async_trait]
impl Source for FloodSource {
    async fn run(self: Box<Self>, out: SourceSender, shutdown: Shutdown) -> logpipe::Result<()> {
        while !shutdown.is_triggered() {
            let events: Vec<LogEvent> = (0..EVENTS_PER_BATCH)
                .map(|_| LogEvent::new("x".repeat(EVENT_BYTES)))
                .collect();
            let bytes: usize = events.iter().map(LogEvent::estimated_size).sum();
            // 收尾时下游会关掉，这不是错误
            if out.send(events).await.is_err() {
                break;
            }
            self.sent.fetch_add(bytes, Ordering::SeqCst);
        }
        Ok(())
    }
}

/// 写入一直卡住，直到测试放行 —— 模拟 ClickHouse 变慢或挂掉。
struct GatedSink {
    gate: Arc<Semaphore>,
}

#[async_trait]
impl Sink for GatedSink {
    async fn write(&mut self, _events: &[LogEvent]) -> logpipe::Result<()> {
        let _ = self.gate.acquire().await;
        Ok(())
    }
}

/// 存储写不动的时候，采集必须停在字节上限那里等，而不是接着往内存里灌。
///
/// 没有字节配额的话，拦得住 source 的只有队列深度：8 批 × 每批 10 条 ≈ 25MiB 照收
/// 不误，条数一样、日志形态一变内存就翻几十倍 —— OOMKilled 就是这么来的。
#[tokio::test]
async fn inflight_bytes_are_capped_when_the_sink_stalls() {
    let sent = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));

    let running = Pipeline::builder()
        .source(FloodSource {
            sent: Arc::clone(&sent),
        })
        .sink(GatedSink {
            gate: Arc::clone(&gate),
        })
        .batch(BatchConfig::default().timeout(Duration::from_millis(20)))
        .max_inflight_bytes(MAX_INFLIGHT)
        .build()
        .unwrap()
        .spawn();

    // 足够 source 把配额灌满并撞上背压
    tokio::time::sleep(Duration::from_millis(300)).await;
    let stalled_at = sent.load(Ordering::SeqCst);

    // 放行，否则收尾会卡在这一批的写入上
    gate.add_permits(Semaphore::MAX_PERMITS);
    running.stop().await.unwrap();

    assert!(
        stalled_at <= MAX_INFLIGHT,
        "在途字节超过上限：{stalled_at} > {MAX_INFLIGHT}"
    );
    assert!(
        stalled_at >= MAX_INFLIGHT / 2,
        "背压过头了，配额还剩一大半就不发了：{stalled_at} < {}",
        MAX_INFLIGHT / 2
    );
}
