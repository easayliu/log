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
* **异常堆栈会自动合并**到上一条日志的 `message`，不会被拆成一堆碎片；
* 格式不同的项目可以换正则：`RegexParser::with_pattern(...)`，命名捕获组用
  `timestamp` `level` `trace_id` `span_id` `thread` `logger` `message`；
* 时间戳**存的就是日志里的本地时间**：日志写 `11:04:08.914`，库里查出来还是 `11:04:08.914`，
  中间不做任何时区换算。想让列自带时区标注可以把 DDL 改成 `DateTime64(3, 'Asia/Shanghai')`；
  解析不出时间的行（比如没有时间戳的裸行）用采集时刻的本地时间兜底。

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

字段由程序定义（就是上面那 9 个），**建表由你自己执行**，程序不会自动建表也不会 `ALTER` ——
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

改分区/TTL/排序键直接改这份 SQL 就行，程序不关心。要注意两点：

* 配置里 `fields` 加的静态字段（`app`、`env`……）**需要你自己往 DDL 里加列**。
  插入时带了 `input_format_skip_unknown_fields=1`，表里没这列就静默跳过，不会整批失败。
* 列名必须和字段名一致，多余的列（有默认值或 Nullable）不影响插入。

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

* 剥掉运行时外壳，把里面的应用日志交给那套 logback 正则；
* 被切成 `P` 片段的超长行**先拼回整行**再解析；
* 从路径解出 `namespace` / `pod` / `container`，连同 `stream`（stdout/stderr）写进去 ——
  **不访问 API server**，所以不需要 ServiceAccount / RBAC；
* 写到 stderr 的异常堆栈照样合并进上一条日志；
* `--ddl` 自动带上这几列，不用手改。

> 按 label / annotation 选容器做不到 —— 那些不在路径里，得访问 API server。
> 现在的选择维度是 namespace / pod 名 / container 名，覆盖了绝大多数场景
> （pod 名带 Deployment 前缀，`pods: ["order-*"]` 基本等价于按服务选）。

部署是 DaemonSet（每节点一个），`deploy/logpipe-daemonset.yaml` 可以直接 apply，
镜像用根目录的 `Dockerfile` 构建：

```bash
kubectl apply -f deploy/logpipe-daemonset.yaml
```

要点：容器日志文件是 root `0600`，所以 `runAsUser: 0`；位点目录挂 hostPath 才能在 Pod
重建后续读；`tolerations: operator: Exists` 保证污点节点也采。

### 发布

镜像由 CI 构建推送，不用本地 `docker build`。打 tag 就发版：

```bash
# 1. 先改 Cargo.toml 的 version，CI 会校验它和 tag 一致，不一致直接失败
git commit -am "release v0.1.1"
# 2. 打 tag 推上去
git tag v0.1.1 && git push origin main --tags
```

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
* Kafka / Elasticsearch sink
* 磁盘缓冲（现在队列在内存里，进程被杀时未落库的数据靠位点重读补回）
* 采集指标（写入条数/失败数暂时只有 tracing 日志）
