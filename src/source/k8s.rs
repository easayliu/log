//! k8s 元数据。
//!
//! namespace / pod / container 从容器日志的**文件路径**里解出来，不访问 API server：
//!
//! ```text
//! /var/log/pods/<namespace>_<pod>_<uid>/<container>/0.log          kubelet 真实文件
//! /var/log/containers/<pod>_<namespace>_<container>-<id>.log       指向上面的软链
//! ```
//!
//! `service_name` 是 pod 的 label（默认 `app`），路径里没有，得问 API server
//! —— [`KubeClient`] 只干这一件事：`GET /api/v1/namespaces/{ns}/pods/{pod}`。

use std::path::{Path, PathBuf};
use std::time::Duration;

use glob::Pattern;

use crate::error::{Error, Result};

/// kubelet 落容器日志的标准目录。
pub const DEFAULT_POD_LOG_DIR: &str = "/var/log/pods";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodMeta {
    pub namespace: String,
    pub pod: String,
    pub container: String,
}

pub fn parse_pod_path(path: &Path) -> Option<PodMeta> {
    parse_pods_layout(path).or_else(|| parse_containers_layout(path))
}

/// `/var/log/pods/<ns>_<pod>_<uid>/<container>/0.log`
fn parse_pods_layout(path: &Path) -> Option<PodMeta> {
    let container_dir = path.parent()?;
    let container = container_dir.file_name()?.to_str()?;
    let pod_dir = container_dir.parent()?.file_name()?.to_str()?;

    // <ns>_<pod>_<uid>：namespace 和 pod 名都不含下划线，uid 在最后。
    let (namespace, rest) = pod_dir.split_once('_')?;
    let (pod, _uid) = rest.rsplit_once('_')?;
    if namespace.is_empty() || pod.is_empty() || container.is_empty() {
        return None;
    }

    Some(PodMeta {
        namespace: namespace.to_owned(),
        pod: pod.to_owned(),
        container: container.to_owned(),
    })
}

/// `/var/log/containers/<pod>_<ns>_<container>-<containerid>.log`
fn parse_containers_layout(path: &Path) -> Option<PodMeta> {
    let stem = path.file_name()?.to_str()?.strip_suffix(".log")?;
    let (pod, rest) = stem.split_once('_')?;
    let (namespace, rest) = rest.split_once('_')?;
    let (container, id) = rest.rsplit_once('-')?;
    if pod.is_empty() || namespace.is_empty() || container.is_empty() || id.is_empty() {
        return None;
    }

    Some(PodMeta {
        namespace: namespace.to_owned(),
        pod: pod.to_owned(),
        container: container.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_pods_layout() {
        let meta = parse_pod_path(Path::new(
            "/var/log/pods/prod_order-service-7d9f8b6c4-abcde_1f2e3d4c-5b6a/order-service/0.log",
        ))
        .unwrap();
        assert_eq!(meta.namespace, "prod");
        assert_eq!(meta.pod, "order-service-7d9f8b6c4-abcde");
        assert_eq!(meta.container, "order-service");
    }

    #[test]
    fn parses_containers_symlink_layout() {
        let meta = parse_pod_path(Path::new(
            "/var/log/containers/order-service-7d9f8b6c4-abcde_prod_order-service-9f8e7d6c5b4a3210.log",
        ))
        .unwrap();
        assert_eq!(meta.namespace, "prod");
        assert_eq!(meta.pod, "order-service-7d9f8b6c4-abcde");
        assert_eq!(meta.container, "order-service");
    }

    #[test]
    fn returns_none_for_plain_paths() {
        assert!(parse_pod_path(Path::new("/var/log/app/app.log")).is_none());
    }
}

/// 只读 pod 对象的 API server 客户端，用来取 pod 的 label 当 `service_name`。
///
/// 不引 `kube` 那一整套（几十个 crate、要生成的 API 类型），需要的只是一个 GET：
/// 复用 ClickHouse sink 已经在用的 reqwest。集群里用 [`KubeClient::in_cluster`]，
/// 从 ServiceAccount 的挂载读 token 和 CA；RBAC 只要 `pods` 的 `get`。
#[derive(Clone, Debug)]
pub struct KubeClient {
    client: reqwest::Client,
    base: String,
    token: Option<TokenSource>,
}

#[derive(Clone, Debug)]
enum TokenSource {
    /// ServiceAccount 的投影 token 会轮换（默认一小时），kubelet 原地改写文件，
    /// 所以每次请求重读，而不是启动时读一次。
    File(PathBuf),
    Static(String),
}

/// ServiceAccount 挂载的标准位置。
const SA_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

impl KubeClient {
    /// 直接给 API server 地址（形如 `http://127.0.0.1:8001`），不带认证。
    /// 给测试和 `kubectl proxy` 用；集群里用 [`Self::in_cluster`]。
    pub fn new(base: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("reqwest client"),
            base: base.into().trim_end_matches('/').to_owned(),
            token: None,
        }
    }

    /// 固定的 Bearer token。
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(TokenSource::Static(token.into()));
        self
    }

    /// 按 Pod 里的标准环境组装：`KUBERNETES_SERVICE_HOST` / `_PORT` 定位 API server，
    /// ServiceAccount 挂载目录里的 `token` 和 `ca.crt` 做认证。
    pub fn in_cluster() -> Result<Self> {
        let host = std::env::var("KUBERNETES_SERVICE_HOST").ok();
        let port = std::env::var("KUBERNETES_SERVICE_PORT").ok();
        let (Some(host), Some(port)) = (host, port) else {
            return Err(Error::config(
                "不在 k8s 集群里（没有 KUBERNETES_SERVICE_HOST），取不到 pod 的 label；\
                 不需要 service_name 的话把 source.service_name_label 设成空串",
            ));
        };
        let sa_dir = Path::new(SA_DIR);
        let token_path = sa_dir.join("token");
        if !token_path.is_file() {
            return Err(Error::config(format!(
                "{} 不存在：Pod 没挂 ServiceAccount token（automountServiceAccountToken 被关了？）",
                token_path.display()
            )));
        }
        let ca = std::fs::read(sa_dir.join("ca.crt"))
            .map_err(|err| Error::io("读取 API server 的 CA 证书失败", err))?;
        let ca = reqwest::Certificate::from_pem(&ca)
            .map_err(|err| Error::config(format!("API server 的 CA 证书不合法: {err}")))?;
        let client = reqwest::Client::builder()
            .add_root_certificate(ca)
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|err| Error::config(format!("初始化 API server 客户端失败: {err}")))?;
        // IPv6 的 service ip 要加方括号
        let host = if host.contains(':') && !host.starts_with('[') {
            format!("[{host}]")
        } else {
            host
        };
        Ok(Self {
            client,
            base: format!("https://{host}:{port}"),
            token: Some(TokenSource::File(token_path)),
        })
    }

    /// 取 pod 的一个 label。
    ///
    /// * `Ok(Some(v))`：有这个 label；
    /// * `Ok(None)`：pod 在但没这个 label，或者 pod 已经没了（404，日志目录会比 pod
    ///   多活一阵）—— 两种都是**定论**，不必再试；
    /// * `Err`：网络、超时、401/403 这类，调用方稍后重试。403 的报错里带 RBAC 提示。
    pub async fn pod_label(
        &self,
        namespace: &str,
        pod: &str,
        label: &str,
    ) -> Result<Option<String>> {
        let url = format!("{}/api/v1/namespaces/{namespace}/pods/{pod}", self.base);
        let mut request = self.client.get(&url).header("Accept", "application/json");
        if let Some(token) = &self.token {
            let token = match token {
                TokenSource::Static(token) => token.clone(),
                TokenSource::File(path) => std::fs::read_to_string(path)
                    .map_err(|err| Error::io("读取 ServiceAccount token 失败", err))?,
            };
            request = request.bearer_auth(token.trim());
        }
        let response = request
            .send()
            .await
            .map_err(|err| Error::source(format!("请求 API server 失败 {url}: {err}")))?;
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let body = response
            .text()
            .await
            .map_err(|err| Error::source(format!("读 API server 响应失败 {url}: {err}")))?;
        if status == reqwest::StatusCode::FORBIDDEN {
            return Err(Error::source(format!(
                "API server 拒绝 GET {url}：ServiceAccount 没有 pods 的 get 权限，\
                 见 deploy/logpipe-daemonset.yaml 里的 ClusterRole；{}",
                body.trim()
            )));
        }
        if !status.is_success() {
            return Err(Error::source(format!(
                "API server 返回 {status} {url}: {}",
                body.trim()
            )));
        }
        let object: serde_json::Value = serde_json::from_str(&body)
            .map_err(|err| Error::source(format!("API server 返回的不是 JSON {url}: {err}")))?;
        Ok(object
            .pointer("/metadata/labels")
            .and_then(|labels| labels.get(label))
            .and_then(|value| value.as_str())
            .map(str::to_owned))
    }
}

/// 按 k8s 的口径挑要采的容器：namespace / pod / container 名字，支持 glob。
///
/// 规则：include 为空表示「全都要」；exclude 优先于 include。
/// 这些名字全部来自日志文件路径，**不需要访问 API server**。
#[derive(Clone, Debug, Default)]
pub struct PodSelector {
    namespaces: Vec<Pattern>,
    exclude_namespaces: Vec<Pattern>,
    pods: Vec<Pattern>,
    exclude_pods: Vec<Pattern>,
    containers: Vec<Pattern>,
    exclude_containers: Vec<Pattern>,
}

fn patterns<I, S>(values: I) -> Result<Vec<Pattern>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    values
        .into_iter()
        .map(|value| {
            Pattern::new(value.as_ref())
                .map_err(|e| Error::config(format!("名字匹配式非法 {}: {e}", value.as_ref())))
        })
        .collect()
}

impl PodSelector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn namespaces<I, S>(mut self, values: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.namespaces = patterns(values)?;
        Ok(self)
    }

    pub fn exclude_namespaces<I, S>(mut self, values: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.exclude_namespaces = patterns(values)?;
        Ok(self)
    }

    pub fn pods<I, S>(mut self, values: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.pods = patterns(values)?;
        Ok(self)
    }

    pub fn exclude_pods<I, S>(mut self, values: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.exclude_pods = patterns(values)?;
        Ok(self)
    }

    pub fn containers<I, S>(mut self, values: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.containers = patterns(values)?;
        Ok(self)
    }

    pub fn exclude_containers<I, S>(mut self, values: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.exclude_containers = patterns(values)?;
        Ok(self)
    }

    /// 不采自己，避免「采到自己的日志 → 产生新日志」的循环。
    ///
    /// 容器里的 hostname 就是 pod 名，所以不需要任何配置；
    /// 用 Downward API 注入 `POD_NAME` 时优先取它。
    pub fn exclude_self(mut self) -> Self {
        let own = std::env::var("POD_NAME")
            .ok()
            .filter(|name| !name.is_empty())
            .or_else(|| {
                hostname::get()
                    .ok()
                    .and_then(|host| host.into_string().ok())
            });

        if let Some(own) = own {
            if let Ok(pattern) = Pattern::new(&own) {
                tracing::debug!(pod = %own, "跳过自己的日志");
                self.exclude_pods.push(pattern);
            }
        }
        self
    }

    pub fn matches(&self, meta: &PodMeta) -> bool {
        selected(&self.namespaces, &self.exclude_namespaces, &meta.namespace)
            && selected(&self.pods, &self.exclude_pods, &meta.pod)
            && selected(&self.containers, &self.exclude_containers, &meta.container)
    }
}

fn selected(include: &[Pattern], exclude: &[Pattern], value: &str) -> bool {
    if exclude.iter().any(|pattern| pattern.matches(value)) {
        return false;
    }
    include.is_empty() || include.iter().any(|pattern| pattern.matches(value))
}

#[cfg(test)]
mod selector_tests {
    use super::*;

    fn meta(namespace: &str, pod: &str, container: &str) -> PodMeta {
        PodMeta {
            namespace: namespace.to_owned(),
            pod: pod.to_owned(),
            container: container.to_owned(),
        }
    }

    #[test]
    fn empty_selector_takes_everything() {
        let selector = PodSelector::new();
        assert!(selector.matches(&meta("prod", "order-1", "app")));
        assert!(selector.matches(&meta("kube-system", "kube-proxy-x", "kube-proxy")));
    }

    #[test]
    fn filters_by_namespace() {
        let selector = PodSelector::new().namespaces(["prod", "staging"]).unwrap();
        assert!(selector.matches(&meta("prod", "order-1", "app")));
        assert!(!selector.matches(&meta("kube-system", "kube-proxy-x", "kube-proxy")));
    }

    #[test]
    fn exclude_wins_over_include() {
        let selector = PodSelector::new()
            .namespaces(["*"])
            .unwrap()
            .exclude_namespaces(["kube-*"])
            .unwrap();
        assert!(selector.matches(&meta("prod", "order-1", "app")));
        assert!(!selector.matches(&meta("kube-system", "coredns-x", "coredns")));
    }

    #[test]
    fn filters_by_pod_and_container_globs() {
        let selector = PodSelector::new()
            .pods(["order-*"])
            .unwrap()
            .exclude_containers(["istio-proxy"])
            .unwrap();
        assert!(selector.matches(&meta("prod", "order-abc", "app")));
        assert!(!selector.matches(&meta("prod", "cart-abc", "app")));
        assert!(!selector.matches(&meta("prod", "order-abc", "istio-proxy")));
    }
}
