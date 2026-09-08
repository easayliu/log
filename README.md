# logpipe

一个精简的日志采集入库框架，思路参考 [vector](https://github.com/vectordotdev/vector)，但只保留我们需要的部分：
**把服务器上的日志文件采上来，解析成结构化记录，批量写进 ClickHouse。**

```text
  ┌────────────┐        ┌──────────────────────┐        ┌────────────┐
  │   Source   │ Batch  │       Pipeline       │ 批量写  │    Sink    │
  │ 文件 tail   ├───────▶│ 攒批 / 重试 / 背压     ├───────▶│ ClickHouse │
  │ stdin      │        │ 优雅退出              │  ack   │ Console    │
  └────────────┘        └──────────────────────┘◀───────└────────────┘
        ▲                                        位点在落库后才推进
        └── checkpoint（重启续读）
```

## 日志格式

默认按我们线上的 logback 格式解析：

```text
2026-09-07 11:04:08.914 [TID:e89a476882236ce0f1186d1522c8f59f] [SpanID:e8b0e73e2132f21c] [scheduledThreadPoolExecutor-1] INFO  c.a.c.service.DelayTaskService -redis延时任务触发检查,action数量0
```

解析成：

| 字段 | 值 |
| --- | --- |
| `timestamp` | `2026-09-07 11:04:08.914`（原样落库，不做时区换算） |
| `trace_id` | `e89a476882236ce0f1186d1522c8f59f` |
| `span_id` | `e8b0e73e2132f21c` |
| `thread` | `scheduledThreadPoolExecutor-1` |
| `level` | `INFO` |
| `logger` | `c.a.c.service.DelayTaskService` |
| `message` | `redis延时任务触发检查,action数量0` |
| `file` / `host` | 采集时自动补上 |

* `[TID:...]`、`[SpanID:...]`、`[thread]` 都是可选的，缺失时为空串；
* 毫秒用逗号（logback `ISO8601` 默认的 `15:20:43,633`）和用小数点都认，落库都是毫秒；
* 级别带方括号也认，比如 `2026-09-07 15:20:43,633 [DEBUG] o.s.w.s.m.m.a.RequestMappingHandlerMapping Returning handler method [...]`；
* **异常堆栈会自动合并**到上一条日志的 `message`，不会被拆成一堆碎片；
* 格式不同的项目可以换正则：`RegexParser::with_pattern(...)`，命名捕获组用
  `timestamp` `level` `trace_id` `span_id` `thread` `logger` `message`；
* 一个节点上不止一种格式时用 `parser.formats` 列全（见下面「access log」）；
* 时间戳**存的就是日志里的本地时间**：日志写 `11:04:08.914`，库里查出来还是 `11:04:08.914`，
  中间不做任何时区换算。想让列自带时区标注可以把 DDL 改成 `DateTime64(3, 'Asia/Shanghai')`；
  解析不出时间的行（比如没有时间戳的裸行）用采集时刻的本地时间兜底。

### access log

整机采集时节点上通常不只有 Java 日志。`parser.formats` 按顺序列出会遇到的格式，
每行按顺序试，第一个匹配上的胜出：

```yaml
parser:
  formats: [logback, gin, nginx]   # 默认只有 logback
```

| 格式 | 样例 |
| --- | --- |
| `gin` | `[GIN] 2026/09/07 - 10:07:01 \| 200 \|      40.601µs \|    172.16.250.3 \| POST     "/extra_text"` |
| `nginx` | `127.0.0.6 - - [07/Sep/2026:18:06:53 +0800] "GET / HTTP/1.1" 200 601 "-" "kube-probe/1.28+" "-"` |

这三种格式共用同一张表、同一套列，**配了新格式不用 ALTER**：

| 字段 | access log 里是什么 |
| --- | --- |
| `timestamp` | 日志自己的时间戳（不配的话这里是采集时刻，行序会错） |
| `level` | 由状态码折算：5xx = `ERROR`，4xx = `WARN`，其余 `INFO`，好让按 level 过滤/告警照样生效 |
| `logger` | `gin` / `nginx`，用来把 access log 和业务日志分开查 |
| `message` | 整行原文 |

所以「找出错的请求」是 `where logger = 'gin' and level = 'ERROR'`。
不把 `status` / `path` / `latency` 抽成独立列是有意的：这张表混着业务日志，为少数行
加一堆稀疏列，换来的只是「拿日志表做访问分析」这一个场景，而那个场景本来就该单独
建表。要精确到 path 就从 `message` 里抠。

时间用日志自己的时间戳：gin 打的是服务本地时间，原样取；nginx CLF 带 `+0800`
这类偏移，按 CRI 拆壳同样的口径换成本机墙上时间。

两个已知不覆盖的：`gin.ForceConsoleColor()` 的彩色输出（状态码被 ANSI 转义包着）、
启动时的 `[GIN-debug]` 路由表 —— 都当解析不出，整行进 `message`。

> **漏配一种格式不会报错**，只会被当成上一条日志的堆栈续行粘到 `message` 尾巴上
> （这正是异常堆栈能自动合并的同一套机制）。发现日志串行了，先看 `formats` 配全了没有。

## 启动

有两种用法：直接跑二进制（读 TOML 配置），或者当库用（拓扑写在代码里）。

```bash
cp logpipe.yaml /etc/logpipe.yaml       # 仓库根目录有带注释的示例配置
cargo run --release -- --ddl /etc/logpipe.yaml | clickhouse-client   # 建表
cargo run --release -- --check /etc/logpipe.yaml                     # 只校验配置
cargo run --release -- /etc/logpipe.yaml                             # 启动
RUST_LOG=debug cargo run -- /etc/logpipe.yaml                        # 看每批写入
```

不带参数时读当前目录的 `logpipe.yaml`。`Ctrl-C` 是优雅退出：剩下的数据先写完、位点存好再退。

最小配置（先用 console sink 看解析结果，不用连库）：

```yaml
# 采本机所有容器的 stdout（k8s）
source:
  type: kubernetes

sink:
  type: console
  encoding: json
```

```yaml
# 或者采日志文件
source:
  type: file
  include:
    - /var/log/app/*.log
  data_dir: /var/lib/logpipe

sink:
  type: console
  encoding: json
```

换成入库只要改 `sink`：

```yaml
sink:
  type: clickhouse
  endpoint: http://127.0.0.1:8123
  database: logs
  table: app_log
  user: default
  password: ""

fields:            # 附加到每条日志的静态字段
  app: order-service
  env: prod
```

完整可配项（`source` / `parser` / `sink` / `batch` / `retry` / `pipeline` / `fields`）
见 `logpipe.yaml` 里的注释；写错的键会在启动时直接报错，不会静默忽略。

## 当库用

```rust
use logpipe::source::k8s::PodSelector;
use logpipe::{Pipeline, sink::ClickhouseSink, source::FileSource};

#[tokio::main]
async fn main() -> logpipe::Result<()> {
    Pipeline::builder()
        .source(
            // 采本机所有容器：/var/log/pods、CRI 拆壳、k8s 元数据、跳过自己，都是默认行为
            FileSource::kubernetes()
                .pod_selector(PodSelector::new().exclude_namespaces(["kube-*"])?)
                .data_dir("/var/lib/logpipe"), // 位点目录，重启续读
        )
        .sink(ClickhouseSink::new("http://127.0.0.1:8123", "logs", "app_log"))
        .require_healthy(true)
        .build()?
        .run()          // 跑到 Ctrl-C；剩余数据会先写完再退出
        .await
}
```

采日志文件就换成 `FileSource::new(["/var/log/app/*.log"])`。

## 表结构

字段由程序定义（上面那 9 个，外加容器元数据列和 `fields` 里的静态字段），
**建表由你自己执行**，程序不会自动建表也不会 `ALTER` ——
分区键、排序键、TTL、引擎这些线上细节留在你手里。启动时只做校验：`require_healthy: true`
的情况下会 `SELECT 1` + `EXISTS TABLE`，表不存在就直接报错退出，不会白读一段日志。

```bash
cargo run -- --ddl logpipe.yaml | clickhouse-client
```

```sql
CREATE TABLE IF NOT EXISTS `logs`.`app_log`
(
    `timestamp` DateTime64(3),
    `level`     LowCardinality(String),
    `trace_id`  String,
    `span_id`   String,
    `thread`    String,
    `logger`    String,
    `message`   String,
    `file`      String,
    `host`      LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(`timestamp`)
ORDER BY (`timestamp`, `level`, `trace_id`)
TTL toDateTime(`timestamp`) + INTERVAL 30 DAY;
```

改分区/TTL/排序键直接改这份 SQL 就行，程序不关心。要注意三点：

* 配置里 `fields` 加的静态字段（`cluster`、`env`……）**`--ddl` 会自动带上对应的列**，
  类型按值推断：字符串 → `LowCardinality(String)`、整数 → `Int64`、小数 → `Float64`、
  布尔 → `UInt8`。采容器日志时（`type: kubernetes`）`stream` / `namespace` / `pod` /
  `container` 四列同样自动带上。
* **但已经建好的表不会自动改。**给线上配置新加一个 `fields` 之后，要自己
  `ALTER TABLE ... ADD COLUMN`（或者重跑 `--ddl` 对照着补）。插入时带了
  `input_format_skip_unknown_fields=1`，表里没这列**不会报错、整批也不会失败，
  这个字段会被静默丢掉** —— 加了字段却查不到值，先去看表结构。
* 列名必须和字段名一致，多余的列（有默认值或 Nullable）不影响插入。

### 接 ClickHouse 集群

sink 就是一次 HTTP `INSERT`，所以 `endpoint` 指到 chproxy / VIP / k8s Service 都行。
表结构上填一个集群名（`system.clusters` 里的那个，和 `fields` 里叫 `cluster`
的静态字段没关系）：

```yaml
sink:
  type: clickhouse
  endpoint: http://ck-lb:8123
  database: logs
  table: app_log
  cluster: bj_ck        # 留空 / 不写 = 单机 MergeTree
  async_insert: true    # 多副本小批量写，建议打开
  compress: true        # 默认就是 true，gzip 压 INSERT 请求体
```

INSERT 的请求体默认 gzip 压缩（实测约 6.5 倍，8.7MiB 一批压完花 16ms）。每个节点
一个 DaemonSet，省下的是乘以节点数的常驻带宽。只有中间的代理/网关不能正确转发压缩
过的 body 时才需要 `compress: false`。

`--ddl` 就变成两条语句，一次执行完：

```sql
CREATE TABLE IF NOT EXISTS `logs`.`app_log_local` ON CLUSTER `bj_ck`
( ... )
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs/app_log_local', '{replica}')
PARTITION BY toYYYYMMDD(`timestamp`)
ORDER BY (`timestamp`, `level`, `trace_id`)
TTL toDateTime(`timestamp`) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS `logs`.`app_log` ON CLUSTER `bj_ck`
AS `logs`.`app_log_local`
ENGINE = Distributed(`bj_ck`, `logs`, `app_log_local`, rand());
```

* 本地表叫 `<table>_local`，存数据；`<table>` 是它上面的 Distributed 表，
  **sink 写的还是 `table`**，写入路径和单机时一模一样。
* `{shard}` / `{replica}` 是 ClickHouse 自己的宏，由各节点的 `macros` 配置展开，
  不用替换。集群没配这两个宏的话，把 zk 路径改成你们的约定。
* 分片键是 `rand()`，分布最均匀。想让同一台机器的日志落同一个分片
  （压缩率更好、按 host 查不用跨分片）改成 `cityHash64(host)`。
* 建库也要 `ON CLUSTER`，否则只在被连上的那个节点建出来，别的节点建本地表时会报库不存在。
* 加 `fields` 之后补列要**补两张表**，本地表在前：
  `ALTER TABLE logs.app_log_local ON CLUSTER bj_ck ADD COLUMN ...`，
  再对 `logs.app_log` 来一遍 —— Distributed 表的结构是建表时拷过去的，不会跟着变。

还差的是**多入口**：`endpoint` 只能填一个地址（`src/config.rs`），没有多节点轮询和
故障转移 —— 节点挂了只会按 retry 策略重试同一个地址，重试耗尽后按 `on_error` 停机或丢。
所以集群前面得有 LB。

不想写配置文件的话，`examples/` 下有两个直接跑的例子：

```bash
cargo run --example tail_to_console -- '/var/log/app/*.log'
cargo run --example tail_to_clickhouse -- '/var/log/app/*.log'
```

## 投递语义

* **至少一次**。位点（checkpoint）只在数据**确实写进存储之后**才推进，进程被杀/重启会从上次成功处重读，可能重复但不会丢。
* 写入失败按指数退避重试（默认 5 次）；仍然失败时默认 `OnError::Stop` —— 停掉 pipeline、位点不动，重启后重来。想「丢了也无所谓、优先别堵」的场景可以设 `OnError::Drop`。
* 背压：source 与 sink 之间是有界队列（`.buffer(n)`），存储慢下来时采集会自然被压住，不会把内存吃光。

## 采集容器控制台日志（k8s + containerd）

应用日志打到容器 stdout 时，kubelet 落到宿主机
`/var/log/pods/<ns>_<pod>_<uid>/<container>/0.log`，每行被运行时套了一层壳：

```text
2026-09-07T03:04:08.914293456Z stdout F 2026-09-07 11:04:08.914 [TID:...] INFO  c.a.c.Foo -消息
└── 运行时时间戳 ────────────┘ └流┘ └┘ └── 应用自己的日志 ──────────────────────────┘
                                      F = 整行结束，P = 片段（超长行被切开）
```

**这些路径和格式都不用配。**`type: kubernetes` 只让你按 k8s 的口径声明「采哪些」：

```yaml
source:
  type: kubernetes
  exclude_namespaces: ["kube-*"]          # 留空 = 全采；下面几项都支持 glob
  exclude_containers: [istio-proxy]
  data_dir: /var/lib/logpipe
```

想按业务挑就写 `namespaces: [prod, staging]`、`pods: ["order-*"]`、`containers: [app]`，
每项都有对应的 `exclude_*`（exclude 优先）。完整示例见 `deploy/logpipe-k8s.yaml`。

默认行为（都不用写）：

| 项 | 默认 | 说明 |
| --- | --- | --- |
| `log_dir` | `/var/log/pods` | kubelet 标准路径 |
| `exclude_self` | `true` | 不采自己，避免自我循环。容器里 hostname 就是 pod 名，不需要额外配置 |
| `read_from_beginning` | `false` | 只收增量，首次部署不会把节点上的历史日志全灌一遍 |
| `glob_interval_secs` | `10` | 多久扫一次新容器 |
| 轮转文件 | 一起采 | kubelet 轮转出的 `0.log.20260907-...` 也读，见下面「文件轮转」 |

程序做的事：

* 剥掉运行时外壳，把里面的应用日志交给 logback 解析器；
* 被切成 `P` 片段的超长行**先拼回整行**再解析；
* 从路径解出 `namespace` / `pod` / `container`，连同 `stream`（stdout/stderr）写进去 ——
  **不访问 API server**，所以不需要 ServiceAccount / RBAC；
* 写到 stderr 的异常堆栈照样合并进上一条日志；
* `--ddl` 自动带上这几列，不用手改。

> 按 label / annotation 选容器做不到 —— 那些不在路径里，得访问 API server。
> 现在的选择维度是 namespace / pod 名 / container 名，覆盖了绝大多数场景
> （pod 名带 Deployment 前缀，`pods: ["order-*"]` 基本等价于按服务选）。

部署是 DaemonSet（每节点一个），`deploy/logpipe-daemonset.yaml` 可以直接 apply，
镜像用根目录的 `Dockerfile` 构建。

**顺序是先建表、再起 DaemonSet** —— 配了 `require_healthy: true`，表不存在时 healthcheck
直接失败退出，Pod 会 CrashLoopBackOff（也就 exec 不进去，别指望进容器里拿 DDL）：

```bash
# 1. namespace + ConfigMap + DaemonSet
kubectl apply -f deploy/logpipe-daemonset.yaml

# 2. 建库建表（挂的是同一个 ConfigMap，列不会和采集端对不上）
kubectl apply -f deploy/logpipe-ddl-job.yaml
kubectl -n logging wait --for=condition=complete job/logpipe-ddl --timeout=180s

# 3. 让第 1 步已经起来的 Pod 立刻重试，不用等 CrashLoop 退避
kubectl -n logging rollout restart daemonset/logpipe
```

Job 里 `apply-ddl` 容器的 `CH_HOST` / `CH_DATABASE` / `CH_CLUSTER` / `CH_USER` /
`CH_PASSWORD` 要和 ConfigMap 里 sink 的 `endpoint` / `database` / `cluster` / `user` /
`password` 对上（接集群见上面「接 ClickHouse 集群」，`CH_CLUSTER` 留空就是单机）。DDL 是
`CREATE TABLE IF NOT EXISTS`，重复跑无副作用，可以挂成 CI / helm 的 pre-install hook。
**但已存在的表它不会改**：加了 `fields` 之后新列要自己 `ALTER TABLE ... ADD COLUMN`，
详见上面「表结构」那节。

不想在集群里跑 Job 的话，`--ddl` 不连库，本地也能渲染出来：

```bash
kubectl -n logging get cm logpipe-config -o jsonpath='{.data.logpipe\.yaml}' > /tmp/logpipe.yaml
docker run --rm -v /tmp/logpipe.yaml:/etc/logpipe/logpipe.yaml:ro \
  ghcr.io/easayliu/log:v0.1.4 --ddl /etc/logpipe/logpipe.yaml
```

要点：容器日志文件是 root `0600`，所以 `runAsUser: 0`；位点目录挂 hostPath 才能在 Pod
重建后续读；`tolerations: operator: Exists` 保证污点节点也采。

### 发布

镜像由 CI 构建推送，不用本地 `docker build`。打 tag 就发版：

```bash
# 1. 先改 Cargo.toml 的 version，CI 会校验它和 tag 一致，不一致直接失败
git commit -am "release v0.1.1"
git push origin main

# 2. tag 必须单独推，不能和分支挤在同一条 git push 里
git tag v0.1.1
git push origin v0.1.1
```

> **别写 `git push origin main --tags`。**分支和 tag 在同一次 push 里上去时，GitHub 只按
> 分支 ref 记一个事件，tag 不产生自己的事件，Actions 永远收不到 `ref_type: tag` 的 push
> —— tag 在远端好好地待着，镜像却没人构建。踩过一次：`git ls-remote` 能看到 tag、
> workflow 是 active、也没有 startup_failure，但 events 里只有 `PushEvent refs/heads/main`。
>
> 已经这样推错了的话，重复推同一个 ref 是 no-op、不会补触发，得先删远端 tag 再单独推：
> `git push origin :refs/tags/v0.1.1 && git push origin v0.1.1`

产出 `ghcr.io/easayliu/log:v0.1.1`，同时把 `:latest` 指过去（`v0.1.1-rc1` 这类预发布
tag 不会动 `latest`）。`workflow_dispatch` 手动触发只推 `sha-<短 sha>`，用来验证流水线本身。

`.github/workflows/ci.yml` 在 push / PR 上跑 `fmt --check` + `clippy -D warnings` +
`cargo test`；`docker.yml` 构建前会复用它作为闸门，测试不过就不会有镜像。

Docker + json-file 驱动（`{"log":"...","stream":"stdout","time":"..."}`）还没做，
需要的话加一个 `container_format: docker` 就行，解码位置和 CRI 是同一处。

## 文件轮转

| 情况 | 行为 |
| --- | --- |
| 追加写入 | tail 增量读 |
| `> app.log` 原地清空 | 检测到文件变短，从头重读并重置位点 |
| `app.log → app.log.1` 改名 | 认的是 inode，旧句柄读到 EOF 才关闭，尾巴不丢 |
| 新文件出现 | 按 `glob_interval`（默认 10s）扫描发现，从头读 |
| **进程不在时发生轮转** | 轮转文件按 inode 认出来续读，轮转出的新文件从头读，中间那段不丢 |
| 容器重启（kubelet 写 `1.log`） | 同目录的新文件从头读，启动阶段的日志不会被跳过 |
| 单行超过 1MB 没有换行 | 丢弃这段并告警，避免内存膨胀 |

`read_from_beginning: false` 只对**首次部署时节点上就已经存在的文件**生效 —— 避免把整个
节点的历史日志灌一遍。之后新出现的文件（新 Pod、容器重启、轮转）一律从头读，
否则「从末尾开始」会把这些文件开头的日志静默丢掉。

> **已知缺口**：kubelet 轮转后会把旧文件压成 `.gz`（通常在轮转后十几秒内），
> 压缩过的文件现在读不了、会被跳过。所以「进程停了很久 + 期间轮转过」这种情况，
> 被压掉的那一段仍然采不到。要彻底补上需要支持读 gzip。

## 加自己的组件

只有两个 trait，都很短：

```rust
#[async_trait]
pub trait Source: Send + 'static {
    async fn run(self: Box<Self>, out: SourceSender, shutdown: Shutdown) -> Result<()>;
}

#[async_trait]
pub trait Sink: Send + Sync + 'static {
    async fn write(&mut self, events: &[LogEvent]) -> Result<()>;
    async fn healthcheck(&self) -> Result<()> { Ok(()) }
}
```

攒批、重试、ack、退出都在 pipeline 里，写 sink 只需要实现「把这批写进去」。
写入可能被重试，所以实现要能容忍重复。

内置组件：

| 类型 | 组件 | 说明 |
| --- | --- | --- |
| source | `FileSource` | glob 发现 + tail + 位点 + 多行合并 |
| source | `FileSource::kubernetes()` | 上面这些 + CRI 拆壳 + 按 namespace/pod/container 选容器 |
| source | `StdinSource` | 配合 `tail -F xxx \| app` 或调试用 |
| sink | `ClickhouseSink` | HTTP `JSONEachRow` 批量插入 |
| sink | `ConsoleSink` | JSON / 原始文本输出 |
| sink | `MemorySink` | 测试用 |

## 还没做

* Docker json-file 格式的容器日志（CRI 已支持）
* ClickHouse 多 endpoint 轮询 / 故障转移（现在只能填一个地址，靠外面的 LB）
* Kafka / Elasticsearch sink
* 磁盘缓冲（现在队列在内存里，进程被杀时未落库的数据靠位点重读补回）
* 采集指标（写入条数/失败数暂时只有 tracing 日志）
