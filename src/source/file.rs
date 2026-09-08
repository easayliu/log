//! 文件采集：按 glob 发现日志文件、tail 追加内容、记录位点。
//!
//! 行为约定：
//! * 位点只在数据**成功落库**后推进，进程重启从上次位点继续（至少一次投递）；
//! * 文件被截断（`> app.log` 或原地轮转）会自动从头重读；
//! * 文件被改名轮转（`app.log -> app.log.1`）时，旧句柄读到 EOF 才关闭，不丢尾巴；
//! * 进程不在时发生的轮转也补得回来：轮转文件按 inode 认出来续读，新文件从头读；
//! * 异常堆栈等多行日志由 [`Aggregator`] 合并成一条。

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;

use crate::error::{Error, Result};
use crate::event::LogEvent;
use crate::parser::{Aggregator, ContainerFormat, LogbackParser, Parser};
use crate::shutdown::Shutdown;
use crate::source::checkpoint::Checkpointer;
use crate::source::k8s::{self, KubeClient, PodMeta, PodSelector};
use crate::source::{Source, SourceSender};

const READ_CHUNK: usize = 64 * 1024;
/// 退出时等待落库回执的上限。
const COMMIT_WAIT: Duration = Duration::from_secs(30);

pub fn hostname() -> String {
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "unknown".to_owned())
}

pub struct FileSource {
    includes: Vec<String>,
    excludes: Vec<String>,
    data_dir: Option<PathBuf>,
    parser: Arc<dyn Parser>,
    host: Arc<str>,
    container_format: ContainerFormat,
    pod_selector: Option<PodSelector>,
    /// `service_name` 取 pod 的哪个 label。设了就要访问 API server。
    service_name_label: Option<String>,
    /// 不设则在 `run` 时按集群内环境组装（[`KubeClient::in_cluster`]）。
    kube: Option<KubeClient>,
    read_from_beginning: bool,
    glob_interval: Duration,
    read_interval: Duration,
    checkpoint_interval: Duration,
    idle_flush: Duration,
    max_line_bytes: usize,
    batch_lines: usize,
    fields: BTreeMap<String, Value>,
}

impl FileSource {
    /// `includes` 支持 glob，例如 `/var/log/app/*.log`。
    pub fn new<I, S>(includes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            includes: includes.into_iter().map(Into::into).collect(),
            excludes: Vec::new(),
            data_dir: None,
            parser: Arc::new(LogbackParser),
            host: Arc::from(hostname()),
            container_format: ContainerFormat::Raw,
            pod_selector: None,
            service_name_label: None,
            kube: None,
            read_from_beginning: true,
            glob_interval: Duration::from_secs(10),
            read_interval: Duration::from_millis(500),
            checkpoint_interval: Duration::from_secs(5),
            idle_flush: Duration::from_secs(2),
            max_line_bytes: 1024 * 1024,
            batch_lines: 1000,
            fields: BTreeMap::new(),
        }
    }

    /// 按 k8s 标准采集本机所有容器的 stdout/stderr：
    /// 自动扫 `/var/log/pods`、剥 CRI 外壳、解出 namespace/pod/container、跳过自己。
    ///
    /// 默认只收增量（新出现的文件从末尾读），避免首次部署把节点上的历史日志全灌一遍。
    pub fn kubernetes() -> Self {
        Self::kubernetes_in(k8s::DEFAULT_POD_LOG_DIR)
    }

    /// 同上，但指定 kubelet 的日志目录（测试或非标准部署用）。
    pub fn kubernetes_in(log_dir: impl AsRef<Path>) -> Self {
        // `*.log*` 把 kubelet 轮转出的 `0.log.<ts>` 一起收进来 —— 进程不在的时候
        // 转走的那一段，只有这样才补得回来。已压缩的在 discover 里跳过。
        Self::new([format!("{}/*/*/*.log*", log_dir.as_ref().display())])
            .container_format(ContainerFormat::Cri)
            .pod_selector(PodSelector::new().exclude_self())
            .read_from_beginning(false)
    }

    /// 挑要采的容器（namespace / pod / container 名字，支持 glob）。
    pub fn pod_selector(mut self, selector: PodSelector) -> Self {
        self.pod_selector = Some(selector);
        self
    }

    /// 把 pod 的这个 label（比如 `app`）写进 `service_name`，和 Jaeger 里的 service 对齐。
    ///
    /// label 不在日志路径里，要问 API server：每个 pod 查一次，结果缓存；没配
    /// [`Self::kube_client`] 就在启动时按集群内环境组装，RBAC 只要 `pods` 的 `get`。
    /// 查不到（pod 没这个 label、pod 已经没了）时退回静态 `fields.service_name`，
    /// 那也没有就是空串。空串等于不设。
    pub fn service_name_label(mut self, label: impl Into<String>) -> Self {
        let label = label.into();
        self.service_name_label = (!label.is_empty()).then_some(label);
        self
    }

    /// 取 label 用的 API server 客户端。集群里不用设，测试和 `kubectl proxy` 场景用。
    pub fn kube_client(mut self, client: KubeClient) -> Self {
        self.kube = Some(client);
        self
    }

    pub fn exclude<I, S>(mut self, excludes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.excludes = excludes.into_iter().map(Into::into).collect();
        self
    }

    /// 位点存放目录。不设置则只在内存里记，重启会重新开始。
    pub fn data_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.data_dir = Some(dir.into());
        self
    }

    pub fn parser(mut self, parser: impl Parser) -> Self {
        self.parser = Arc::new(parser);
        self
    }

    /// 容器日志格式。设为 [`ContainerFormat::Cri`] 时会剥掉 containerd 的外壳，
    /// 并从路径解出 `namespace` / `pod` / `container` 字段。
    pub fn container_format(mut self, format: ContainerFormat) -> Self {
        self.container_format = format;
        self
    }

    /// 新文件（没有位点）从头读还是只读增量，默认从头读。
    pub fn read_from_beginning(mut self, yes: bool) -> Self {
        self.read_from_beginning = yes;
        self
    }

    /// 多久扫一次 glob 发现新文件。
    pub fn glob_interval(mut self, interval: Duration) -> Self {
        self.glob_interval = interval;
        self
    }

    /// 多久把位点写一次盘。
    pub fn checkpoint_interval(mut self, interval: Duration) -> Self {
        self.checkpoint_interval = interval;
        self
    }

    /// 没有新数据时的轮询间隔。
    pub fn read_interval(mut self, interval: Duration) -> Self {
        self.read_interval = interval;
        self
    }

    /// 读到文件末尾后，最后一条日志等多久没有续行就认为它已经完整。
    pub fn idle_flush(mut self, idle_flush: Duration) -> Self {
        self.idle_flush = idle_flush;
        self
    }

    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Arc::from(host.into());
        self
    }

    /// 附加到每条日志上的静态字段。和 k8s 元数据一起放进 [`LogEvent::shared`]，
    /// 每个文件建一份、所有事件共享，比用 transform 逐条 `insert` 省掉一串小分配。
    pub fn fields(mut self, fields: BTreeMap<String, Value>) -> Self {
        self.fields = fields;
        self
    }

    fn discover(
        &self,
        watchers: &mut HashMap<String, Watcher>,
        order: &mut Vec<String>,
        checkpointer: &Checkpointer,
        first_pass: bool,
        unresolved: &mut HashMap<String, u32>,
    ) -> Result<()> {
        let excludes = self
            .excludes
            .iter()
            .filter_map(|pattern| glob::Pattern::new(pattern).ok())
            .collect::<Vec<_>>();

        for watcher in watchers.values_mut() {
            watcher.seen = false;
        }

        // 只有首轮、且不从头读时才需要问「这个目录以前采过吗」。一次性收成集合，
        // 否则每个新文件都要线性扫一遍全部位点，文件多的节点启动是 O(n²)。
        let checkpoint_dirs = if first_pass && !self.read_from_beginning {
            checkpointer.dirs()
        } else {
            HashSet::new()
        };

        // 先把这一轮的文件收齐，再按 mtime 从旧到新处理：轮转出来的旧文件排在当前
        // 文件前面，读出来的顺序才和写进去的顺序一致。
        let mut found: Vec<(PathBuf, std::fs::Metadata)> = Vec::new();
        for include in &self.includes {
            let paths = glob::glob(include)
                .map_err(|e| Error::config(format!("include glob 非法 {include}: {e}")))?;

            for path in paths.flatten() {
                if excludes.iter().any(|pattern| pattern.matches_path(&path)) {
                    continue;
                }
                if should_skip(&path) {
                    tracing::debug!(?path, "跳过压缩文件 / 压缩中间文件");
                    continue;
                }
                let Ok(metadata) = std::fs::metadata(&path) else {
                    continue;
                };
                if !metadata.is_file() {
                    continue;
                }
                found.push((path, metadata));
            }
        }
        found.sort_by(|(left_path, left), (right_path, right)| {
            left.modified()
                .ok()
                .cmp(&right.modified().ok())
                .then_with(|| left_path.cmp(right_path))
        });

        for (path, metadata) in found {
            let key = fingerprint(&path, &metadata);
            if let Some(watcher) = watchers.get_mut(&key) {
                watcher.seen = true;
                // 轮转（改名）后要跟着换 file 字段，否则后面读出来的内容还挂着
                // 轮转前的路径。只在真的变了时才重新分配。
                if watcher.path != path {
                    watcher.file_field = Arc::from(path.display().to_string());
                    watcher.path = path;
                }
                continue;
            }

            let mut file = match File::open(&path) {
                Ok(file) => file,
                // 单个文件读不了不至于停掉整个采集，但权限问题往往是部署配错了，
                // 值得用 error 级别喊出来（k8s 下容器日志是 root 0600）。
                Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                    tracing::error!(
                        ?path,
                        %err,
                        "没有权限读取日志文件，已跳过（容器日志通常需要 root）"
                    );
                    continue;
                }
                Err(err) => {
                    tracing::warn!(?path, %err, "打开日志文件失败，已跳过");
                    continue;
                }
            };

            // 优先用位点续读；位点比文件还大说明文件被换过，从头开始。
            let len = metadata.len();
            let start = match checkpointer.get(&key) {
                Some(offset) if offset <= len => offset,
                Some(_) => {
                    checkpointer.reset(&key);
                    0
                }
                // 没有位点的新文件从哪读，取决于它是「历史」还是「新产生的」：
                // * 不是首轮扫描 —— 这文件是我们盯着的时候才出现的（新 Pod 的
                //   0.log、容器重启后的 1.log、轮转出来的新 0.log），必须从头读；
                // * 同目录下已经有位点 —— 之前采过这个容器，说明进程不在时轮转过，
                //   也要从头读，否则「轮转到重启」之间那一段就丢了。
                // 只有首次部署时节点上就已经存在的文件才按 read_from_beginning 走，
                // 免得把整个节点的历史日志灌一遍。
                None if self.read_from_beginning
                    || !first_pass
                    || path
                        .parent()
                        .is_some_and(|dir| checkpoint_dirs.contains(dir)) =>
                {
                    0
                }
                None => len,
            };
            file.seek(std::io::SeekFrom::Start(start)).map_err(|err| {
                Error::io(
                    format!("定位到 {} 的 {start} 字节处失败", path.display()),
                    err,
                )
            })?;

            // CRI 布局下从路径解出 k8s 元数据，并据此决定采不采。
            let mut shared = self.fields.clone();
            let mut pod = None;
            if self.container_format == ContainerFormat::Cri {
                match k8s::parse_pod_path(&path) {
                    Some(meta) => {
                        if let Some(selector) = &self.pod_selector {
                            if !selector.matches(&meta) {
                                continue;
                            }
                        }
                        shared.insert("namespace".to_owned(), Value::from(meta.namespace.clone()));
                        shared.insert("pod".to_owned(), Value::from(meta.pod.clone()));
                        shared.insert("container".to_owned(), Value::from(meta.container.clone()));
                        pod = Some(meta);
                    }
                    None => {
                        // 解不出元数据就无法判断该不该采，配了筛选条件时宁可不采。
                        if self.pod_selector.is_some() {
                            tracing::warn!(?path, "路径不像 k8s 容器日志，已跳过");
                            continue;
                        }
                        tracing::warn!(?path, "路径不像 k8s 容器日志，没有 pod 元数据");
                    }
                }
            }

            tracing::info!(?path, offset = start, "开始采集");
            // label 要问 API server，同步的 discover 里做不了，记下来交给 run 循环
            if pod.is_some() && self.service_name_label.is_some() {
                unresolved.insert(key.clone(), 0);
            }
            order.push(key.clone());
            watchers.insert(
                key.clone(),
                Watcher {
                    key,
                    pod,
                    file_field: Arc::from(path.display().to_string()),
                    path,
                    file,
                    offset: start,
                    safe_offset: start,
                    buf: Vec::new(),
                    aggregator: Aggregator::new(Arc::clone(&self.parser))
                        .decoder(self.container_format.decoder()),
                    shared: (!shared.is_empty()).then(|| Arc::new(shared)),
                    host: Arc::clone(&self.host),
                    pending_since: None,
                    at_eof: false,
                    seen: true,
                },
            );
        }
        Ok(())
    }
}

impl FileSource {
    /// 给刚发现的容器补 `service_name`：问 API server 要 pod 的 label。
    ///
    /// 同一个 pod 的几个文件（多容器、轮转、重启后的 `1.log`）只查一次，结果按
    /// `namespace/pod` 缓存。查询并发发出去 —— API server 挂了的话，每个超时 10 秒，
    /// 串行等一个节点上百个 pod 会把采集卡住几分钟。
    ///
    /// 有定论的（有 label / 没 label / pod 没了）从 `unresolved` 里拿掉；出错的留着，
    /// 下一轮扫描再试，日志照采不误，只是这段时间的 `service_name` 是退路值。
    async fn resolve_service_names(
        &self,
        kube: &KubeClient,
        label: &str,
        watchers: &mut HashMap<String, Watcher>,
        unresolved: &mut HashMap<String, u32>,
        cache: &mut HashMap<String, Option<String>>,
    ) {
        // 先用缓存兜掉，剩下的按 pod 去重后并发查
        let mut lookups: HashMap<String, PodMeta> = HashMap::new();
        for (key, watcher) in watchers.iter_mut() {
            let Some(attempts) = unresolved.get_mut(key) else {
                continue;
            };
            let Some(meta) = &watcher.pod else {
                unresolved.remove(key);
                continue;
            };
            let pod_key = format!("{}/{}", meta.namespace, meta.pod);
            if let Some(name) = cache.get(&pod_key) {
                watcher.set_service_name(name.as_deref());
                unresolved.remove(key);
                continue;
            }
            *attempts += 1;
            lookups.entry(pod_key).or_insert_with(|| meta.clone());
        }
        if lookups.is_empty() {
            return;
        }

        let mut tasks = tokio::task::JoinSet::new();
        for (pod_key, meta) in lookups {
            let kube = kube.clone();
            let label = label.to_owned();
            tasks.spawn(async move {
                let result = kube.pod_label(&meta.namespace, &meta.pod, &label).await;
                (pod_key, meta, result)
            });
        }
        while let Some(joined) = tasks.join_next().await {
            let Ok((pod_key, meta, result)) = joined else {
                continue;
            };
            let name = match result {
                Ok(name) => name,
                Err(err) => {
                    // 第一次用 warn 喊出来，之后每 10 秒一次的重试降到 debug，免得刷屏
                    let first = watchers.values().any(|watcher| {
                        watcher.same_pod(&meta)
                            && unresolved.get(&watcher.key).is_some_and(|n| *n <= 1)
                    });
                    if first {
                        tracing::warn!(namespace = %meta.namespace, pod = %meta.pod, %err,
                            "取 pod 的 label 失败，service_name 先用退路值，稍后重试");
                    } else {
                        tracing::debug!(namespace = %meta.namespace, pod = %meta.pod, %err,
                            "取 pod 的 label 仍然失败");
                    }
                    continue;
                }
            };
            if name.is_none() {
                tracing::warn!(namespace = %meta.namespace, pod = %meta.pod, label,
                    "pod 没有这个 label（或已删除），service_name 用退路值");
            }
            for watcher in watchers.values_mut() {
                if watcher.same_pod(&meta) {
                    watcher.set_service_name(name.as_deref());
                    unresolved.remove(&watcher.key);
                }
            }
            cache.insert(pod_key, name);
        }
    }
}

#[async_trait]
impl Source for FileSource {
    async fn run(self: Box<Self>, out: SourceSender, shutdown: Shutdown) -> Result<()> {
        if self.includes.is_empty() {
            return Err(Error::config("FileSource 至少需要一个 include 路径"));
        }

        // label 要访问 API server：没显式给客户端就按集群内环境组装，不在集群里直接报错，
        // 而不是每个 pod 都查失败一遍再说
        let kube = match (&self.service_name_label, &self.kube) {
            (Some(_), Some(client)) => Some(client.clone()),
            (Some(_), None) if self.container_format == ContainerFormat::Cri => {
                Some(KubeClient::in_cluster()?)
            }
            _ => None,
        };
        let mut unresolved: HashMap<String, u32> = HashMap::new();
        let mut label_cache: HashMap<String, Option<String>> = HashMap::new();

        let checkpointer = Arc::new(Checkpointer::load(self.data_dir.as_deref())?);
        let mut watchers: HashMap<String, Watcher> = HashMap::new();
        // watcher 的发现顺序。按它来读，同一个容器「先轮转文件、后当前文件」的
        // 顺序才稳定；HashMap 的迭代顺序是乱的。
        let mut order: Vec<String> = Vec::new();
        // 整个 source 共用一块读缓冲：原来每次 read() 都 `vec![0u8; 64KiB]`，
        // 既要分配也要清零；放在 watcher 上又会变成每个文件常驻 64KiB。
        let mut chunk = vec![0u8; READ_CHUNK];
        let mut last_glob: Option<Instant> = None;
        let mut last_save = Instant::now();
        // 已发出、等待落库回执的提交任务；退出前要等它们结束再存位点。
        let mut inflight: Vec<(String, tokio::task::JoinHandle<()>)> = Vec::new();
        // 文件没了、但可能还有回执在路上的位点：等回执落定再删，
        // 否则迟到的 ack 会把刚删掉的记录重新插回去。
        let mut pending_forget: HashSet<String> = HashSet::new();

        loop {
            if shutdown.is_triggered() {
                break;
            }

            if last_glob.is_none_or(|at| at.elapsed() >= self.glob_interval) {
                let first_pass = last_glob.is_none();
                self.discover(
                    &mut watchers,
                    &mut order,
                    &checkpointer,
                    first_pass,
                    &mut unresolved,
                )?;
                if let (Some(kube), Some(label)) = (&kube, &self.service_name_label) {
                    if !unresolved.is_empty() {
                        self.resolve_service_names(
                            kube,
                            label,
                            &mut watchers,
                            &mut unresolved,
                            &mut label_cache,
                        )
                        .await;
                    }
                }
                last_glob = Some(Instant::now());

                // 首轮扫完清理孤儿位点。放在扫描之后是有意的：扫到了文件才说明日志
                // 目录确实就绪，否则（目录还没挂上）会把所有位点误删。
                if first_pass && !watchers.is_empty() {
                    let removed =
                        checkpointer.prune(|key, path| watchers.contains_key(key) || path.exists());
                    if removed > 0 {
                        tracing::info!(removed, "清理了已消失文件的位点");
                    }
                }
            }

            let mut read_any = false;
            let mut finished = Vec::new();

            for key in &order {
                let Some(watcher) = watchers.get_mut(key) else {
                    continue;
                };
                let events = match watcher.read(
                    &mut chunk,
                    self.batch_lines,
                    self.max_line_bytes,
                    self.idle_flush,
                    &checkpointer,
                ) {
                    Ok(events) => events,
                    Err(err) => {
                        tracing::warn!(path = ?watcher.path, %err, "读取失败，稍后重试");
                        continue;
                    }
                };

                // 文件已经消失（被轮转/删除）且读到 EOF，收尾后关闭句柄。
                //
                // `seen` 只说明「上一轮 glob 没扫到」，不等于文件没了：目录瞬时不可达、
                // stat 偶发失败都会让整轮扑空。只凭它就回收的话，位点会被 forget 掉，
                // 下一轮重新发现时按「新文件」从 0 读，**整个文件重新入库一遍**。
                // 安静的文件常驻 EOF，最容易中招。所以这里再确认一次文件确实不在了。
                if !watcher.seen && watcher.at_eof && !still_present(&watcher.path, &watcher.key) {
                    finished.push(watcher.key.clone());
                }

                if events.is_empty() {
                    continue;
                }

                read_any = true;
                let ack = out.send_with_ack(events).await?;
                inflight.push((
                    watcher.key.clone(),
                    spawn_commit(
                        Arc::clone(&checkpointer),
                        watcher.key.clone(),
                        watcher.path.clone(),
                        watcher.safe_offset,
                        ack,
                    ),
                ));
            }

            for key in finished {
                if let Some(watcher) = watchers.remove(&key) {
                    tracing::info!(path = ?watcher.path, "文件已轮转或删除，停止采集");
                }
                order.retain(|watching| *watching != key);
                unresolved.remove(&key);
                pending_forget.insert(key);
            }

            inflight.retain(|(_, task)| !task.is_finished());
            // inode 会被复用，已消失文件的位点留着会让新文件从中间开始读。
            pending_forget.retain(|key| {
                if inflight.iter().any(|(waiting, _)| waiting == key) {
                    return true;
                }
                checkpointer.forget(key);
                false
            });

            if last_save.elapsed() >= self.checkpoint_interval {
                if let Err(err) = checkpointer.save() {
                    tracing::warn!(%err, "保存位点失败，下次再试");
                }
                last_save = Instant::now();
            }

            if !read_any {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(self.read_interval) => {}
                }
            }
        }

        // 退出前把还没闭合的日志送出去，并落一次位点。
        for watcher in watchers.values_mut() {
            if let Some(event) = watcher.flush_pending() {
                let ack = out.send_with_ack(vec![event]).await?;
                inflight.push((
                    watcher.key.clone(),
                    spawn_commit(
                        Arc::clone(&checkpointer),
                        watcher.key.clone(),
                        watcher.path.clone(),
                        watcher.safe_offset,
                        ack,
                    ),
                ));
            }
        }

        // 等最后几批数据落库（pipeline 最迟在一个攒批周期内会把它们写掉），
        // 否则位点会停在更早的位置，重启后重复入库。
        let deadline = tokio::time::Instant::now() + COMMIT_WAIT;
        for (_, task) in inflight {
            let _ = tokio::time::timeout_at(deadline, task).await;
        }
        // 回执都落定了，这些文件的位点可以安全删掉。
        for key in &pending_forget {
            checkpointer.forget(key);
        }
        // 退出时存不上位点意味着这段日志重启后会重读，必须让人看见。
        if let Err(err) = checkpointer.save() {
            tracing::error!(%err, "退出前保存位点失败，重启后可能重复入库");
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "file"
    }
}

fn spawn_commit(
    checkpointer: Arc<Checkpointer>,
    key: String,
    path: PathBuf,
    offset: u64,
    ack: tokio::sync::oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // ack 通道被丢弃 = 这批数据没落库，位点保持不动，重启后重读。
        if ack.await.is_ok() {
            checkpointer.advance(&key, &path, offset);
        }
    })
}

/// 不是日志正文、必须跳过的文件。
///
/// 压缩过的轮转文件（`.gz`）内容读不了。`.tmp` 是 kubelet 压缩过程中的中间文件，
/// 里面装的是还没改名的压缩流 —— 按文本读会解析出一堆二进制垃圾行入库。
fn should_skip(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("gz" | "zst" | "xz" | "bz2" | "zip" | "tmp")
    )
}

/// 这个 watcher 盯着的文件是否还在原路径上。
///
/// 比 `path.exists()` 严一点：路径还在、但已经是重建出来的另一个文件（inode 变了）
/// 时也算没了，否则会一直攥着一个已删除文件的句柄，位点也永远清不掉。
fn still_present(path: &Path, key: &str) -> bool {
    match std::fs::metadata(path) {
        Ok(metadata) => fingerprint(path, &metadata) == key,
        // 只有明确「不存在」才算没了。权限、IO 抖动一律当作文件还在 ——
        // 宁可多留一个 watcher，也不能误清位点，那代价是整个文件重新入库。
        Err(err) => err.kind() != std::io::ErrorKind::NotFound,
    }
}

/// 文件指纹。Unix 下用 device + inode，改名不影响，重建文件会得到新指纹。
fn fingerprint(path: &Path, metadata: &std::fs::Metadata) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let _ = path;
        format!("{}-{}", metadata.dev(), metadata.ino())
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        path.display().to_string()
    }
}

struct Watcher {
    key: String,
    /// CRI 布局下从路径解出的 pod，取 label 用。
    pod: Option<PodMeta>,
    path: PathBuf,
    file_field: Arc<str>,
    file: File,
    /// `buf` 首字节在文件中的偏移。
    offset: u64,
    /// 已经完整成条、可以安全提交的偏移。
    safe_offset: u64,
    buf: Vec<u8>,
    aggregator: Aggregator,
    /// 每条日志都要带上的固定字段（k8s 元数据、静态 fields），所有事件共享一份。
    shared: Option<Arc<BTreeMap<String, Value>>>,
    host: Arc<str>,
    pending_since: Option<Instant>,
    at_eof: bool,
    seen: bool,
}

impl Watcher {
    /// 同一个 pod（不管哪个容器、哪个轮转文件）。
    fn same_pod(&self, meta: &PodMeta) -> bool {
        self.pod
            .as_ref()
            .is_some_and(|p| p.namespace == meta.namespace && p.pod == meta.pod)
    }

    /// 把 API server 给的 label 写进 `shared`。`None`（pod 没这个 label）保留静态
    /// `fields.service_name`，那也没有就不写，落库时列取默认值空串。
    fn set_service_name(&mut self, name: Option<&str>) {
        let Some(name) = name else {
            return;
        };
        let mut shared = self.shared.as_deref().cloned().unwrap_or_default();
        shared.insert("service_name".to_owned(), Value::from(name));
        self.shared = Some(Arc::new(shared));
    }

    /// 同步读。日志在本地盘上，一次 64KiB 读只阻塞几十微秒；走 `tokio::fs` 的话每次
    /// `metadata()` / `read()` 都要经 spawn_blocking 跳两次线程，空转轮询一个文件
    /// 就要 13µs（同步 0.7µs），整机几百上千个文件按 500ms 轮询，这是常驻的 CPU 开销。
    fn read(
        &mut self,
        chunk: &mut [u8],
        max_lines: usize,
        max_line_bytes: usize,
        idle_flush: Duration,
        checkpointer: &Checkpointer,
    ) -> std::io::Result<Vec<LogEvent>> {
        let mut events = Vec::new();

        // 截断检测：文件变短说明被重写了，回到开头。
        let len = self.file.metadata()?.len();
        if len < self.offset + self.buf.len() as u64 {
            tracing::info!(path = ?self.path, "文件被截断，从头重读");
            // 位点同步归零，否则后续的 ack 会被“只增不减”规则挡住。
            checkpointer.reset(&self.key);
            self.file.seek(std::io::SeekFrom::Start(0))?;
            self.buf.clear();
            self.offset = 0;
            self.safe_offset = 0;
            if let Some(event) = self.aggregator.flush() {
                events.push(self.decorate(event));
            }
        }

        while events.len() < max_lines {
            let n = self.file.read(&mut *chunk)?;
            if n == 0 {
                self.at_eof = true;
                break;
            }
            self.at_eof = false;
            self.buf.extend_from_slice(&chunk[..n]);
            self.drain_lines(&mut events, max_line_bytes);
        }

        // 读到文件末尾时，最后一条日志可能还在等续行，等够时间就收口。
        if self.at_eof && self.aggregator.has_pending() {
            match self.pending_since {
                Some(since) if since.elapsed() >= idle_flush => {
                    if let Some(event) = self.flush_pending() {
                        events.push(event);
                    }
                }
                Some(_) => {}
                None => self.pending_since = Some(Instant::now()),
            }
        }

        Ok(events)
    }

    fn drain_lines(&mut self, events: &mut Vec<LogEvent>, max_line_bytes: usize) {
        let mut start = 0;
        // memchr 是 SIMD 的，比 `iter().position()` 逐字节比快 8 倍；每行直接借用缓冲区
        // 里的切片交给解析器，不再先拷一份 String（合法 UTF-8 时 from_utf8_lossy 不分配）。
        while let Some(index) = memchr::memchr(b'\n', &self.buf[start..]) {
            let end = start + index;
            let line_offset = self.offset + start as u64;
            let mut raw = &self.buf[start..end];
            if raw.last() == Some(&b'\r') {
                raw = &raw[..raw.len() - 1];
            }
            let line = String::from_utf8_lossy(raw);
            start = end + 1;

            if let Some(event) = self.aggregator.push(&line) {
                events.push(decorate(event, &self.file_field, &self.host, &self.shared));
                self.safe_offset = if self.aggregator.has_pending() {
                    // 交出去的是上一条，当前行成了新的一条、还没闭合，
                    // 位点只能提交到当前行开头。
                    line_offset
                } else {
                    // 没有 pending 说明交出去的就是当前行本身（解析不出时间戳的
                    // 兜底行）。提交到行首的话，重启后这一行会被再发一遍。
                    self.offset + start as u64
                };
            }
            self.pending_since = None;
        }

        self.buf.drain(..start);
        self.offset += start as u64;

        // 超长的“行”（没有换行符的二进制/异常内容）直接丢弃，避免无限膨胀。
        if self.buf.len() > max_line_bytes {
            tracing::warn!(
                path = ?self.path,
                bytes = self.buf.len(),
                "单行超过上限，丢弃这段内容"
            );
            self.offset += self.buf.len() as u64;
            self.safe_offset = self.offset;
            self.buf.clear();
        }
    }

    fn flush_pending(&mut self) -> Option<LogEvent> {
        let event = self.aggregator.flush()?;
        self.safe_offset = self.offset;
        self.pending_since = None;
        Some(self.decorate(event))
    }

    fn decorate(&self, event: LogEvent) -> LogEvent {
        decorate(event, &self.file_field, &self.host, &self.shared)
    }
}

/// 补上来源信息。三个都是引用计数，不为每条事件分配。
fn decorate(
    mut event: LogEvent,
    file: &Arc<str>,
    host: &Arc<str>,
    shared: &Option<Arc<BTreeMap<String, Value>>>,
) -> LogEvent {
    event.file = Arc::clone(file);
    event.host = Arc::clone(host);
    event.shared = shared.clone();
    event
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_compressed_and_in_flight_rotation_files() {
        assert!(!should_skip(Path::new(
            "/var/log/pods/ns_pod_uid/app/0.log"
        )));
        assert!(!should_skip(Path::new(
            "/var/log/pods/ns_pod_uid/app/0.log.20260907-123709"
        )));
        assert!(should_skip(Path::new(
            "/var/log/pods/ns_pod_uid/app/0.log.20260907-123709.gz"
        )));
        // kubelet 压缩过程中的中间文件：里面是压缩流，按文本读会入库一堆二进制垃圾
        assert!(should_skip(Path::new(
            "/var/log/pods/ns_pod_uid/app/5.log.20260907-155201.tmp"
        )));
    }

    #[test]
    fn presence_check_distinguishes_gone_from_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log");
        std::fs::write(&path, "x").unwrap();
        let key = fingerprint(&path, &std::fs::metadata(&path).unwrap());

        assert!(still_present(&path, &key));

        // 路径还在、但已经是重建出来的另一个文件（指纹对不上），等于原来那个没了。
        // 这里直接拿一个不存在的指纹比，不用「删掉再建」—— ext4 会立刻复用 inode，
        // 那样断言成不成立取决于文件系统。
        assert!(!still_present(&path, "0-0"));

        std::fs::remove_file(&path).unwrap();
        assert!(!still_present(&path, &key));
    }
}
