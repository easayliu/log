//! 端到端：文件采集 -> 攒批 -> 入库。

use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use logpipe::batch::{BatchConfig, RetryConfig};
use logpipe::parser::ContainerFormat;
use logpipe::pipeline::OnError;
use logpipe::sink::{MemorySink, Sink};
use logpipe::source::k8s::PodSelector;
use logpipe::source::FileSource;
use logpipe::{LogEvent, Pipeline};

const LINE_1: &str = "2026-09-07 11:04:08.914 [TID:e89a476882236ce0f1186d1522c8f59f] [SpanID:e8b0e73e2132f21c] [scheduledThreadPoolExecutor-1] INFO  c.a.c.service.DelayTaskService -redis延时任务触发检查,action数量0";
const LINE_2: &str = "2026-09-07 11:04:09.001 [TID:aaaa] [SpanID:bbbb] [http-nio-8080-exec-3] ERROR c.a.c.web.OrderController -下单失败";
const STACK_1: &str = "java.lang.IllegalStateException: order closed";
const STACK_2: &str = "\tat com.acme.OrderController.submit(OrderController.java:88)";

fn append(path: &Path, lines: &[&str]) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    for line in lines {
        writeln!(file, "{line}").unwrap();
    }
}

fn source(path: &Path, data_dir: &Path) -> FileSource {
    FileSource::new([path.display().to_string()])
        .data_dir(data_dir)
        .read_interval(Duration::from_millis(20))
        .glob_interval(Duration::from_millis(50))
        .idle_flush(Duration::from_millis(50))
}

/// 一条能被默认正则解析的日志，`message` 就是传进来的这个串。
fn logline(second: u32, message: &str) -> String {
    format!("2026-09-07 11:04:{second:02}.914 [TID:t] [SpanID:s] [main] INFO  c.a.Foo -{message}")
}

/// 套上 containerd 的外壳。
fn cri(line: &str) -> String {
    format!("2026-09-07T03:04:08.914293456Z stdout F {line}")
}

/// 按 k8s 的默认口径采（只收增量），但把各种间隔都调快。
fn k8s_source(pods: &Path, data_dir: &Path) -> FileSource {
    FileSource::kubernetes_in(pods)
        .data_dir(data_dir)
        .read_interval(Duration::from_millis(20))
        .glob_interval(Duration::from_millis(50))
        .idle_flush(Duration::from_millis(50))
        .checkpoint_interval(Duration::from_millis(50))
}

fn state_of(data_dir: &Path) -> String {
    std::fs::read_to_string(data_dir.join("checkpoints.json")).unwrap_or_default()
}

fn batch() -> BatchConfig {
    BatchConfig::default()
        .max_events(100)
        .timeout(Duration::from_millis(50))
}

async fn wait_for(mut ready: impl FnMut() -> bool, label: &str) {
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        if ready() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("等待超时: {label}");
}

#[tokio::test]
async fn collects_lines_and_merges_stack_traces() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.log");
    append(&path, &[LINE_1, LINE_2, STACK_1, STACK_2]);

    let sink = MemorySink::new();
    let events = sink.events();

    let running = Pipeline::builder()
        .source(source(&path, dir.path()))
        .sink(sink)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();

    wait_for(|| events.lock().unwrap().len() == 2, "两条日志").await;
    running.stop().await.unwrap();

    let events = events.lock().unwrap();
    assert_eq!(events[0].level, "INFO");
    assert_eq!(events[0].trace_id, "e89a476882236ce0f1186d1522c8f59f");
    assert_eq!(events[0].logger, "c.a.c.service.DelayTaskService");
    assert_eq!(events[0].message, "redis延时任务触发检查,action数量0");
    assert_eq!(&*events[0].file, path.display().to_string().as_str());
    assert!(!events[0].host.is_empty());

    // 异常堆栈应当并进上一条，而不是变成三条日志。
    assert_eq!(events[1].level, "ERROR");
    assert_eq!(events[1].message, format!("下单失败\n{STACK_1}\n{STACK_2}"));
}

#[tokio::test]
async fn resumes_from_checkpoint_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.log");
    append(&path, &[LINE_1, LINE_2]);

    let first = MemorySink::new();
    let first_events = first.events();
    let running = Pipeline::builder()
        .source(source(&path, dir.path()))
        .sink(first)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();
    wait_for(|| first_events.lock().unwrap().len() == 2, "首轮两条").await;
    running.stop().await.unwrap();

    // 重启后只应采集到新追加的那一行。
    append(&path, &[LINE_2]);

    let second = MemorySink::new();
    let second_events = second.events();
    let running = Pipeline::builder()
        .source(source(&path, dir.path()))
        .sink(second)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();
    wait_for(|| second_events.lock().unwrap().len() == 1, "续读一条").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    running.stop().await.unwrap();

    assert_eq!(second_events.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn truncated_file_is_read_from_the_start() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.log");
    append(&path, &[LINE_1]);

    let sink = MemorySink::new();
    let events = sink.events();
    let running = Pipeline::builder()
        .source(source(&path, dir.path()))
        .sink(sink)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();
    wait_for(|| events.lock().unwrap().len() == 1, "第一条").await;

    // 原地清空后重写，模拟 `> app.log` 式的轮转。
    std::fs::write(&path, format!("{LINE_2}\n")).unwrap();
    wait_for(|| events.lock().unwrap().len() == 2, "截断后重读").await;
    running.stop().await.unwrap();

    assert_eq!(events.lock().unwrap()[1].level, "ERROR");
}

#[tokio::test]
async fn transform_can_filter_events() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.log");
    append(&path, &[LINE_1, LINE_2]);

    let sink = MemorySink::new();
    let events = sink.events();
    let running = Pipeline::builder()
        .source(source(&path, dir.path()))
        .transform(|mut event: LogEvent| {
            (event.level == "ERROR").then(|| {
                event.insert("env", "prod");
                event
            })
        })
        .sink(sink)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();

    wait_for(|| events.lock().unwrap().len() == 1, "只留 ERROR").await;
    running.stop().await.unwrap();

    let events = events.lock().unwrap();
    assert_eq!(events[0].level, "ERROR");
    assert_eq!(events[0].get("env").unwrap(), "prod");
}

#[tokio::test]
async fn collects_container_stdout_in_cri_format() {
    let dir = tempfile::tempdir().unwrap();
    // 模拟 kubelet 的目录结构：/var/log/pods/<ns>_<pod>_<uid>/<container>/0.log
    let log_dir = dir
        .path()
        .join("pods")
        .join("prod_order-service-7d9f8b6c4-abcde_1f2e3d4c")
        .join("order-service");
    std::fs::create_dir_all(&log_dir).unwrap();
    let path = log_dir.join("0.log");

    append(
        &path,
        &[
            &format!("2026-09-07T03:04:08.914293456Z stdout F {LINE_1}"),
            &format!("2026-09-07T03:04:09.001000000Z stdout F {LINE_2}"),
            &format!("2026-09-07T03:04:09.002000000Z stderr F {STACK_1}"),
            &format!("2026-09-07T03:04:09.003000000Z stderr F {STACK_2}"),
        ],
    );

    let sink = MemorySink::new();
    let events = sink.events();
    let running = Pipeline::builder()
        .source(
            FileSource::new([format!("{}/pods/*/*/*.log", dir.path().display())])
                .container_format(ContainerFormat::Cri)
                .data_dir(dir.path())
                .read_interval(Duration::from_millis(20))
                .glob_interval(Duration::from_millis(50))
                .idle_flush(Duration::from_millis(50)),
        )
        .sink(sink)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();

    wait_for(|| events.lock().unwrap().len() == 2, "两条容器日志").await;
    running.stop().await.unwrap();

    let events = events.lock().unwrap();
    // CRI 外壳被剥掉，应用自己的日志格式正常解析
    assert_eq!(events[0].level, "INFO");
    assert_eq!(events[0].logger, "c.a.c.service.DelayTaskService");
    assert_eq!(events[0].message, "redis延时任务触发检查,action数量0");
    assert_eq!(events[0].get("stream").unwrap(), "stdout");
    // k8s 元数据从路径解出
    assert_eq!(events[0].get("namespace").unwrap(), "prod");
    assert_eq!(
        events[0].get("pod").unwrap(),
        "order-service-7d9f8b6c4-abcde"
    );
    assert_eq!(events[0].get("container").unwrap(), "order-service");
    // 写到 stderr 的堆栈仍然并进上一条
    assert_eq!(events[1].message, format!("下单失败\n{STACK_1}\n{STACK_2}"));
}

#[tokio::test]
async fn kubernetes_source_selects_by_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let pods = dir.path().join("pods");

    // 两个 namespace 的容器日志，各一条
    for (namespace, pod, line) in [
        ("prod", "order-service-7d9f8b6c4-abcde", LINE_1),
        ("kube-system", "coredns-abc", LINE_2),
    ] {
        let log_dir = pods.join(format!("{namespace}_{pod}_1f2e3d4c")).join("app");
        std::fs::create_dir_all(&log_dir).unwrap();
        append(
            &log_dir.join("0.log"),
            &[&format!("2026-09-07T03:04:08.914293456Z stdout F {line}")],
        );
    }

    let sink = MemorySink::new();
    let events = sink.events();
    let running = Pipeline::builder()
        .source(
            // 不用写任何路径 glob：给目录 + 按 k8s 名字声明要采谁
            FileSource::kubernetes_in(&pods)
                .pod_selector(PodSelector::new().namespaces(["prod"]).unwrap())
                .read_from_beginning(true)
                .data_dir(dir.path())
                .read_interval(Duration::from_millis(20))
                .glob_interval(Duration::from_millis(50))
                .idle_flush(Duration::from_millis(50)),
        )
        .sink(sink)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();

    wait_for(|| events.lock().unwrap().len() == 1, "只采 prod").await;
    // 再等一会儿，确认 kube-system 那条不会迟到
    tokio::time::sleep(Duration::from_millis(300)).await;
    running.stop().await.unwrap();

    let events = events.lock().unwrap();
    assert_eq!(events.len(), 1, "kube-system 的日志不该被采进来");
    assert_eq!(events[0].get("namespace").unwrap(), "prod");
    assert_eq!(
        events[0].get("pod").unwrap(),
        "order-service-7d9f8b6c4-abcde"
    );
    assert_eq!(events[0].get("container").unwrap(), "app");
    assert_eq!(events[0].level, "INFO");
}

/// kubelet 的轮转发生在进程不在的时候：轮转文件里没读完的那一段、以及轮转出的
/// 新文件里的内容，重启后都要补回来。
#[tokio::test]
async fn rotation_while_agent_is_down_is_not_lost() {
    let dir = tempfile::tempdir().unwrap();
    let pods = dir.path().join("pods");
    let log_dir = pods
        .join("prod_order-service-7d9f8b6c4-abcde_1f2e3d4c")
        .join("app");
    std::fs::create_dir_all(&log_dir).unwrap();
    let current = log_dir.join("0.log");
    std::fs::write(&current, "").unwrap();

    // 第一轮：默认只收增量，所以等采集起来之后再写。
    let first = MemorySink::new();
    let first_events = first.events();
    let running = Pipeline::builder()
        .source(k8s_source(&pods, dir.path()))
        .sink(first)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();
    tokio::time::sleep(Duration::from_millis(200)).await;
    append(&current, &[&cri(&logline(1, "启动后第一条"))]);
    wait_for(|| first_events.lock().unwrap().len() == 1, "第一条").await;
    running.stop().await.unwrap();

    // 进程不在的窗口里：先又写进来一条，然后 kubelet 把它转走，新文件里再写一条。
    append(&current, &[&cri(&logline(2, "轮转前"))]);
    let rotated = log_dir.join("0.log.20260907-030410");
    std::fs::rename(&current, &rotated).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await; // 让两个文件的 mtime 分开
    append(&current, &[&cri(&logline(3, "轮转后"))]);

    let second = MemorySink::new();
    let second_events = second.events();
    let running = Pipeline::builder()
        .source(k8s_source(&pods, dir.path()))
        .sink(second)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();
    wait_for(|| second_events.lock().unwrap().len() == 2, "补采两条").await;
    // 再等等，确认不会重复采到「启动后第一条」
    tokio::time::sleep(Duration::from_millis(300)).await;
    running.stop().await.unwrap();

    let events = second_events.lock().unwrap();
    assert_eq!(events.len(), 2, "既不该丢也不该重复");
    assert_eq!(
        events[0].message, "轮转前",
        "轮转文件里没读完的那一段要按 inode 续读补回来"
    );
    assert_eq!(
        events[1].message, "轮转后",
        "轮转出的新文件要从头读，而且排在轮转文件之后"
    );
}

/// 容器重启后 kubelet 会写 `1.log`。哪怕重启发生在进程不在的时候，
/// 这个新文件也必须从头读 —— 否则启动阶段的日志（往往正是崩的原因）全丢。
#[tokio::test]
async fn container_restart_log_is_read_from_the_start() {
    let dir = tempfile::tempdir().unwrap();
    let pods = dir.path().join("pods");
    let log_dir = pods
        .join("prod_order-service-7d9f8b6c4-abcde_1f2e3d4c")
        .join("app");
    std::fs::create_dir_all(&log_dir).unwrap();
    let first_log = log_dir.join("0.log");
    std::fs::write(&first_log, "").unwrap();

    let first = MemorySink::new();
    let first_events = first.events();
    let running = Pipeline::builder()
        .source(k8s_source(&pods, dir.path()))
        .sink(first)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();
    tokio::time::sleep(Duration::from_millis(200)).await;
    append(&first_log, &[&cri(&logline(1, "第一次运行"))]);
    wait_for(|| first_events.lock().unwrap().len() == 1, "第一次运行").await;
    running.stop().await.unwrap();

    // 进程不在的时候容器重启了，日志写进同目录下的 1.log
    tokio::time::sleep(Duration::from_millis(20)).await;
    append(
        &log_dir.join("1.log"),
        &[&cri(&logline(2, "重启后的启动日志"))],
    );

    let second = MemorySink::new();
    let second_events = second.events();
    let running = Pipeline::builder()
        .source(k8s_source(&pods, dir.path()))
        .sink(second)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();
    wait_for(|| second_events.lock().unwrap().len() == 1, "重启后的日志").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    running.stop().await.unwrap();

    let events = second_events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].message, "重启后的启动日志");
}

/// 文件被删掉后位点要跟着删掉：inode 会被复用，留着旧位点会让复用到同一个
/// inode 的新文件从中间开始读，静默跳过开头。
#[tokio::test]
async fn deleted_file_drops_its_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.log");
    append(&path, &[LINE_1]);

    let sink = MemorySink::new();
    let events = sink.events();
    let running = Pipeline::builder()
        .source(source(&path, dir.path()).checkpoint_interval(Duration::from_millis(50)))
        .sink(sink)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();
    wait_for(|| events.lock().unwrap().len() == 1, "第一条").await;
    wait_for(|| state_of(dir.path()).contains("app.log"), "位点落盘").await;

    std::fs::remove_file(&path).unwrap();
    wait_for(|| !state_of(dir.path()).contains("app.log"), "位点被清掉").await;
    running.stop().await.unwrap();
}

/// 上一次进程被杀，留下了已经不存在的文件的位点。启动时要清掉。
#[tokio::test]
async fn orphan_checkpoints_are_pruned_on_start() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.log");
    append(&path, &[LINE_1]);
    std::fs::write(
        dir.path().join("checkpoints.json"),
        r#"{"files":{"1-999999":{"path":"/var/log/gone-with-the-pod.log","offset":4096}}}"#,
    )
    .unwrap();

    let sink = MemorySink::new();
    let events = sink.events();
    let running = Pipeline::builder()
        .source(source(&path, dir.path()).checkpoint_interval(Duration::from_millis(50)))
        .sink(sink)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();
    wait_for(|| events.lock().unwrap().len() == 1, "第一条").await;
    wait_for(
        || !state_of(dir.path()).contains("gone-with-the-pod"),
        "孤儿位点被清掉",
    )
    .await;
    running.stop().await.unwrap();

    // 自己的位点不能被误删
    assert!(state_of(dir.path()).contains("app.log"));
}

struct FailingSink;

#[async_trait]
impl Sink for FailingSink {
    async fn write(&mut self, _events: &[LogEvent]) -> logpipe::Result<()> {
        Err(logpipe::Error::other("存储挂了"))
    }
}

#[tokio::test]
async fn write_failure_stops_pipeline_and_keeps_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.log");
    append(&path, &[LINE_1]);

    let result = Pipeline::builder()
        .source(source(&path, dir.path()))
        .sink(FailingSink)
        .batch(batch())
        .retry(RetryConfig {
            max_attempts: 2,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(20),
        })
        .on_error(OnError::Stop)
        .build()
        .unwrap()
        .spawn()
        .wait()
        .await;
    assert!(result.is_err(), "写入失败应当把 pipeline 停掉");

    // 位点没有推进，换一个正常的 sink 重跑，数据还在。
    let sink = MemorySink::new();
    let events = sink.events();
    let running = Pipeline::builder()
        .source(source(&path, dir.path()))
        .sink(sink)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();
    wait_for(|| events.lock().unwrap().len() == 1, "重跑仍能拿到数据").await;
    running.stop().await.unwrap();
}

/// 落库失败时要把 sink 自己的错误报上来。写入是在独立任务里做的，主循环先看到的
/// 只是「写入任务已退出」，不能让这层转述盖掉真正的原因。
#[tokio::test]
async fn sink_error_is_reported_not_masked() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.log");
    append(&path, &[LINE_1]);

    let err = Pipeline::builder()
        .source(source(&path, dir.path()))
        .sink(FailingSink)
        .batch(batch())
        .retry(RetryConfig {
            max_attempts: 1,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
        })
        .on_error(OnError::Stop)
        .build()
        .unwrap()
        .spawn()
        .wait()
        .await
        .expect_err("写入失败应当把 pipeline 停掉");

    assert!(
        err.to_string().contains("存储挂了"),
        "根因被转述盖掉了: {err}"
    );
}

struct SlowSink {
    writes: Arc<Mutex<usize>>,
    delay: Duration,
}

#[async_trait]
impl Sink for SlowSink {
    async fn write(&mut self, _events: &[LogEvent]) -> logpipe::Result<()> {
        tokio::time::sleep(self.delay).await;
        *self.writes.lock().unwrap() += 1;
        Ok(())
    }
}

/// 攒下一批要和写上一批重叠，而不是「攒满 -> 停下来写 -> 再从头攒」串成一条。
///
/// 攒批超时和写入耗时都取 120ms：重叠的话一批约 120ms，串行则是两段相加约 240ms，
/// 同样的时间窗口里批数差一倍。
#[tokio::test]
async fn accumulation_overlaps_with_slow_writes() {
    const STEP: Duration = Duration::from_millis(120);
    const WINDOW: Duration = Duration::from_millis(960);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.log");
    append(&path, &[LINE_1]);

    let writes = Arc::new(Mutex::new(0usize));
    let running = Pipeline::builder()
        .source(source(&path, dir.path()))
        .sink(SlowSink {
            writes: Arc::clone(&writes),
            delay: STEP,
        })
        // max_events 取得足够大，保证每一批都是被 timeout 触发的
        .batch(BatchConfig::default().max_events(1_000_000).timeout(STEP))
        .build()
        .unwrap()
        .spawn();

    // 持续追加，让 source 一直有东西可发
    let start = std::time::Instant::now();
    while start.elapsed() < WINDOW {
        append(&path, &[LINE_1]);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    running.stop().await.unwrap();

    let count = *writes.lock().unwrap();
    let serial = WINDOW.as_millis() / (STEP.as_millis() * 2); // 串行时的批数上限
    assert!(
        count as u128 > serial + 1,
        "攒批与写入没有重叠：{WINDOW:?} 内只写了 {count} 批，串行也能到 {serial} 批"
    );
}
