//! 日志事件：按我们线上的 Java 日志格式定型的一条记录。
//!
//! ```text
//! 2026-09-07 11:04:08.914 [TID:e89a4768...] [SpanID:e8b0e73e...] [thread-1] INFO  c.a.c.service.DelayTaskService -消息内容
//! └── timestamp ────────┘ └── trace_id ──┘ └── span_id ──────┘ └ thread ┘ level └── logger ──────────────────┘ └ message ┘
//! ```

use std::collections::BTreeMap;

use chrono::{Local, NaiveDateTime};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// ClickHouse `DateTime64(3)` 默认接受的时间格式。
///
/// 时间戳保留日志里的**墙上时间**，不做任何时区换算：日志写
/// `11:04:08.914`，库里就是 `11:04:08.914`。
pub const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.3f";

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LogEvent {
    /// 日志时间，原样取自日志行（不带时区，也不换算）。
    #[serde(with = "timestamp_format")]
    pub timestamp: NaiveDateTime,
    /// INFO / WARN / ERROR ...
    pub level: String,
    /// `[TID:xxx]`
    pub trace_id: String,
    /// `[SpanID:xxx]`
    pub span_id: String,
    /// `[scheduledThreadPoolExecutor-1]`
    pub thread: String,
    /// `c.a.c.service.DelayTaskService`
    pub logger: String,
    /// 正文。多行日志（异常堆栈）会被合并到这里。
    pub message: String,
    /// 采集来源文件路径。
    pub file: String,
    /// 采集主机名。
    pub host: String,
    /// 额外字段，落库前由调用方自行追加。
    #[serde(flatten)]
    pub fields: BTreeMap<String, Value>,
}

impl LogEvent {
    /// 构造一条只有正文的事件（时间取当前时刻），用于解析失败兜底。
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            timestamp: Local::now().naive_local(),
            message: message.into(),
            ..Default::default()
        }
    }

    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<Value>) -> Option<Value> {
        self.fields.insert(key.into(), value.into())
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.fields.get(key)
    }

    /// 追加一行续行（异常堆栈）。
    pub fn append_line(&mut self, line: &str) {
        self.message.push('\n');
        self.message.push_str(line);
    }

    /// 估算编码成 JSON 后的字节数，用于按体积攒批。
    pub fn estimated_size(&self) -> usize {
        let fixed = self.level.len()
            + self.trace_id.len()
            + self.span_id.len()
            + self.thread.len()
            + self.logger.len()
            + self.message.len()
            + self.file.len()
            + self.host.len();
        let extra: usize = self
            .fields
            .iter()
            .map(|(k, v)| k.len() + estimated_value_size(v) + 4)
            .sum();
        fixed + extra + 160
    }

    /// 编码成一行 JSON（ClickHouse `JSONEachRow`）。
    pub fn to_json_line(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// 还原成便于人读的一行，用于 console sink。
    pub fn to_text_line(&self) -> String {
        format!(
            "{} [TID:{}] [SpanID:{}] [{}] {:<5} {} -{}",
            self.timestamp.format(TIMESTAMP_FORMAT),
            self.trace_id,
            self.span_id,
            self.thread,
            self.level,
            self.logger,
            self.message
        )
    }
}

fn estimated_value_size(value: &Value) -> usize {
    match value {
        Value::Null => 4,
        Value::Bool(_) => 5,
        Value::Number(_) => 8,
        Value::String(s) => s.len() + 2,
        Value::Array(a) => 2 + a.iter().map(estimated_value_size).sum::<usize>() + a.len(),
        Value::Object(o) => {
            2 + o
                .iter()
                .map(|(k, v)| k.len() + estimated_value_size(v) + 4)
                .sum::<usize>()
        }
    }
}

/// 用 ClickHouse 友好的 `2026-09-07 11:04:08.914` 形式序列化时间戳。
mod timestamp_format {
    use chrono::NaiveDateTime;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(ts: &NaiveDateTime, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&ts.format(super::TIMESTAMP_FORMAT).to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<NaiveDateTime, D::Error> {
        let raw = String::deserialize(d)?;
        NaiveDateTime::parse_from_str(&raw, super::TIMESTAMP_FORMAT)
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_flat_and_keeps_wall_clock_time() {
        let mut event = LogEvent::new("hi");
        event.timestamp =
            NaiveDateTime::parse_from_str("2026-09-07 11:04:08.914", TIMESTAMP_FORMAT).unwrap();
        event.level = "INFO".into();
        event.insert("app", "delay-task");

        let json: Value = serde_json::from_str(&event.to_json_line().unwrap()).unwrap();
        // 日志里写的 11:04，库里就是 11:04
        assert_eq!(json["timestamp"], "2026-09-07 11:04:08.914");
        assert_eq!(json["level"], "INFO");
        assert_eq!(json["app"], "delay-task");
    }

    #[test]
    fn appends_stack_trace_lines() {
        let mut event = LogEvent::new("boom");
        event.append_line("\tat com.foo.Bar.baz(Bar.java:1)");
        assert_eq!(event.message, "boom\n\tat com.foo.Bar.baz(Bar.java:1)");
    }
}
