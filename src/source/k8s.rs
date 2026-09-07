//! 从容器日志的**文件路径**里解出 k8s 元数据，不需要访问 API server。
//!
//! 支持两种常见布局：
//!
//! ```text
//! /var/log/pods/<namespace>_<pod>_<uid>/<container>/0.log          kubelet 真实文件
//! /var/log/containers/<pod>_<namespace>_<container>-<id>.log       指向上面的软链
//! ```

use std::path::Path;

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
