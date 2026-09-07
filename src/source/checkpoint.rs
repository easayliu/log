//! 采集位点。只有数据确认落库后才推进，进程重启时据此续读。

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

const FILE_NAME: &str = "checkpoints.json";

#[derive(Debug, Default, Serialize, Deserialize)]
struct State {
    #[serde(default)]
    files: HashMap<String, Record>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    path: String,
    offset: u64,
}

#[derive(Debug)]
pub struct Checkpointer {
    path: Option<PathBuf>,
    state: Mutex<State>,
    dirty: std::sync::atomic::AtomicBool,
}

impl Checkpointer {
    /// `data_dir` 为 `None` 时只在内存里记录（进程重启会从头/末尾重新开始）。
    pub fn load(data_dir: Option<&Path>) -> Result<Self> {
        let Some(dir) = data_dir else {
            return Ok(Self {
                path: None,
                state: Mutex::new(State::default()),
                dirty: false.into(),
            });
        };

        std::fs::create_dir_all(dir)
            .map_err(|err| Error::io(format!("创建位点目录 {} 失败", dir.display()), err))?;

        let path = dir.join(FILE_NAME);
        let state = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|err| {
                tracing::warn!(?path, %err, "位点文件损坏，忽略后重新开始");
                State::default()
            }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => State::default(),
            Err(err) => {
                return Err(Error::io(
                    format!("读取位点文件 {} 失败", path.display()),
                    err,
                ))
            }
        };

        Ok(Self {
            path: Some(path),
            state: Mutex::new(state),
            dirty: false.into(),
        })
    }

    pub fn get(&self, key: &str) -> Option<u64> {
        self.state
            .lock()
            .unwrap()
            .files
            .get(key)
            .map(|record| record.offset)
    }

    /// 位点只增不减：ack 可能乱序返回。
    pub fn advance(&self, key: &str, path: &Path, offset: u64) {
        let mut state = self.state.lock().unwrap();
        // 快路径：记录已存在时一个字符串都不分配。`entry(key.to_owned())` 加上
        // `or_insert(Record { path: ... })` 会无条件求值两个参数，而这是每批都走的路径。
        if let Some(record) = state.files.get_mut(key) {
            if offset > record.offset {
                record.offset = offset;
                // 轮转后路径才会变，没变就不用重新分配。
                if Path::new(&record.path) != path {
                    record.path = path.display().to_string();
                }
                self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            return;
        }

        state.files.insert(
            key.to_owned(),
            Record {
                path: path.display().to_string(),
                offset,
            },
        );
        if offset > 0 {
            self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// 文件被截断（日志轮转后原地重写）时回退位点。
    pub fn reset(&self, key: &str) {
        let mut state = self.state.lock().unwrap();
        if let Some(record) = state.files.get_mut(key) {
            record.offset = 0;
            self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn forget(&self, key: &str) {
        if self.state.lock().unwrap().files.remove(key).is_some() {
            self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// 这个目录下是否已经有位点记录 —— 说明我们之前采过这个容器。
    ///
    /// 用来区分「首次部署时节点上的历史文件」和「我们不在的时候轮转出来的新文件」：
    /// 后者必须从头读，否则中间那一段就丢了。
    pub fn has_dir(&self, dir: &Path) -> bool {
        self.state
            .lock()
            .unwrap()
            .files
            .values()
            .any(|record| Path::new(&record.path).parent() == Some(dir))
    }

    /// 位点里出现过的所有目录，一次性取走。
    ///
    /// 逐个文件调 [`Self::has_dir`] 是每次线性扫全表，文件多时退化成 O(n²)。
    pub fn dirs(&self) -> HashSet<PathBuf> {
        self.state
            .lock()
            .unwrap()
            .files
            .values()
            .filter_map(|record| Path::new(&record.path).parent().map(Path::to_path_buf))
            .collect()
    }

    /// 清掉不再需要的位点，返回清掉的条数。
    ///
    /// 不清理有两个后果：位点文件在长期运行的节点上只增不减；更要紧的是 **inode 会被复用**，
    /// 留着已删除文件的位点会让复用到同一个 inode 的新文件从中间开始读，静默跳过开头一段。
    pub fn prune(&self, keep: impl Fn(&str, &Path) -> bool) -> usize {
        let mut state = self.state.lock().unwrap();
        let before = state.files.len();
        state
            .files
            .retain(|key, record| keep(key, Path::new(&record.path)));
        let removed = before - state.files.len();
        if removed > 0 {
            self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        removed
    }

    /// 写盘。先写临时文件再 rename，避免进程被杀时留下半个文件。
    pub fn save(&self) -> Result<()> {
        let Some(path) = self.path.as_ref() else {
            return Ok(());
        };
        if !self.dirty.swap(false, std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }

        let bytes = {
            let state = self.state.lock().unwrap();
            serde_json::to_vec(&*state)?
        };
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, bytes)
            .map_err(|err| Error::io(format!("写入位点文件 {} 失败", tmp.display()), err))?;
        std::fs::rename(&tmp, path)
            .map_err(|err| Error::io(format!("更新位点文件 {} 失败", path.display()), err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn survives_reload() {
        let dir = tempfile::tempdir().unwrap();
        let key = "1-2";
        let path = Path::new("/var/log/a.log");

        let checkpointer = Checkpointer::load(Some(dir.path())).unwrap();
        checkpointer.advance(key, path, 100);
        checkpointer.advance(key, path, 40); // 乱序 ack 不应回退
        checkpointer.save().unwrap();

        let reloaded = Checkpointer::load(Some(dir.path())).unwrap();
        assert_eq!(reloaded.get(key), Some(100));
    }

    #[test]
    fn reports_the_path_when_data_dir_is_unusable() {
        // 用一个普通文件当目录，制造 IO 失败，检查报错里带路径
        let dir = tempfile::tempdir().unwrap();
        let not_a_dir = dir.path().join("occupied");
        std::fs::write(&not_a_dir, b"x").unwrap();

        let err = Checkpointer::load(Some(&not_a_dir))
            .expect_err("应当报错")
            .to_string();
        assert!(err.contains("创建位点目录"), "{err}");
        assert!(err.contains("occupied"), "{err}");
    }

    #[test]
    fn has_dir_sees_recorded_directories() {
        let checkpointer = Checkpointer::load(None).unwrap();
        checkpointer.advance("1-2", Path::new("/var/log/pods/ns_pod_uid/app/0.log"), 10);

        assert!(checkpointer.has_dir(Path::new("/var/log/pods/ns_pod_uid/app")));
        assert!(!checkpointer.has_dir(Path::new("/var/log/pods/ns_other_uid/app")));
        // 只看直接父目录，不做前缀匹配
        assert!(!checkpointer.has_dir(Path::new("/var/log/pods")));
    }

    #[test]
    fn prune_drops_records_the_caller_does_not_keep() {
        let checkpointer = Checkpointer::load(None).unwrap();
        checkpointer.advance("live", Path::new("/var/log/a.log"), 1);
        checkpointer.advance("gone", Path::new("/var/log/b.log"), 2);

        let removed = checkpointer.prune(|key, _| key == "live");
        assert_eq!(removed, 1);
        assert_eq!(checkpointer.get("live"), Some(1));
        assert_eq!(checkpointer.get("gone"), None, "已删除文件的位点应当被清掉");
    }

    #[test]
    fn in_memory_when_no_data_dir() {
        let checkpointer = Checkpointer::load(None).unwrap();
        checkpointer.advance("k", Path::new("/tmp/a"), 7);
        assert_eq!(checkpointer.get("k"), Some(7));
        checkpointer.save().unwrap();
    }
}
