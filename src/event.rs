//! 日志事件：按我们线上的 Java 日志格式定型的一条记录。
//!
//! ```text
//! 2026-09-07 11:04:08.914 [TID:e89a4768...] [SpanID:e8b0e73e...] [thread-1] INFO  c.a.c.service.DelayTaskService -消息内容
//! └── timestamp ────────┘ └── trace_id ──┘ └── span_id ──────┘ └ thread ┘ level └── logger ──────────────────┘ └ message ┘
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{Local, NaiveDateTime};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

/// ClickHouse `DateTime64(3)` 默认接受的时间格式。
///
/// 时间戳保留日志里的**墙上时间**，不做任何时区换算：日志写
/// `11:04:08.914`，库里就是 `11:04:08.914`。
pub const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.3f";

#[derive(Clone, Debug, Default, Deserialize)]
pub struct LogEvent {
    /// 日志时间，原样取自日志行（不带时区，也不换算）。
    #[serde(deserialize_with = "deserialize_timestamp")]
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
    /// 采集来源文件路径。同一个文件的所有事件共享一份，见 `FileSource::decorate`。
    pub file: Arc<str>,
    /// 采集主机名。
    pub host: Arc<str>,
    /// 额外字段，落库前由调用方自行追加。
    #[serde(flatten)]
    pub fields: BTreeMap<String, Value>,
    /// 来源级的固定字段（k8s 的 namespace/pod/container、配置里的静态 `fields`）。
    /// 同一来源的所有事件共享一份，逐条只是加一次引用计数；原来每条 `insert` 进
    /// `fields`，一条事件光这几个字段就要六七次小分配。序列化时和 `fields` 一样平铺进
    /// JSON，同名时 `fields` 里的优先。
    #[serde(skip)]
    pub shared: Option<Arc<BTreeMap<String, Value>>>,
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

    /// 先查 `fields`，再查 `shared`。
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.fields
            .get(key)
            .or_else(|| self.shared.as_ref().and_then(|shared| shared.get(key)))
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
        let entry = |(k, v): (&String, &Value)| k.len() + estimated_value_size(v) + 4;
        let extra: usize = self.fields.iter().map(entry).sum::<usize>()
            + self
                .shared
                .as_ref()
                .map_or(0, |shared| shared.iter().map(entry).sum());
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

/// 平铺成一层 JSON：固定字段、`shared`、`fields` 依次写出。
///
/// 手写而不是 derive，一是要把 `shared` 平铺进去，二是时间戳的格式化：chrono 的
/// `format(...).to_string()` 每次都要重新解析格式串再分配，实测 240ns，占一条事件
/// 序列化开销的四成；这里直接按位写进栈上的定长缓冲。
impl Serialize for LogEvent {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(None)?;
        match format_timestamp(&self.timestamp) {
            Some(buf) => map.serialize_entry("timestamp", from_ascii(&buf))?,
            None => map.serialize_entry(
                "timestamp",
                &self.timestamp.format(TIMESTAMP_FORMAT).to_string(),
            )?,
        }
        map.serialize_entry("level", &self.level)?;
        map.serialize_entry("trace_id", &self.trace_id)?;
        map.serialize_entry("span_id", &self.span_id)?;
        map.serialize_entry("thread", &self.thread)?;
        map.serialize_entry("logger", &self.logger)?;
        map.serialize_entry("message", &self.message)?;
        map.serialize_entry("file", &*self.file)?;
        map.serialize_entry("host", &*self.host)?;
        if let Some(shared) = &self.shared {
            for (key, value) in shared.iter() {
                if !self.fields.contains_key(key) {
                    map.serialize_entry(key, value)?;
                }
            }
        }
        for (key, value) in &self.fields {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

/// `2026-09-07 11:04:08.914` 按位写进定长缓冲，等价于 [`TIMESTAMP_FORMAT`]。
///
/// 年份不在四位数范围内、或者闰秒把毫秒推过 999 时返回 `None`，交给 chrono 走慢路径。
fn format_timestamp(ts: &NaiveDateTime) -> Option<[u8; 23]> {
    use chrono::{Datelike, Timelike};

    let year = ts.year();
    if !(0..=9999).contains(&year) {
        return None;
    }
    let millis = ts.nanosecond() / 1_000_000;
    if millis > 999 {
        return None;
    }

    fn put(slot: &mut [u8], mut value: u32) {
        for byte in slot.iter_mut().rev() {
            *byte = b'0' + (value % 10) as u8;
            value /= 10;
        }
    }

    let mut buf = *b"0000-00-00 00:00:00.000";
    put(&mut buf[0..4], year as u32);
    put(&mut buf[5..7], ts.month());
    put(&mut buf[8..10], ts.day());
    put(&mut buf[11..13], ts.hour());
    put(&mut buf[14..16], ts.minute());
    put(&mut buf[17..19], ts.second());
    put(&mut buf[20..23], millis);
    Some(buf)
}

fn from_ascii(buf: &[u8]) -> &str {
    std::str::from_utf8(buf).expect("format_timestamp 只写 ASCII")
}

fn deserialize_timestamp<'de, D: Deserializer<'de>>(d: D) -> Result<NaiveDateTime, D::Error> {
    let raw = String::deserialize(d)?;
    NaiveDateTime::parse_from_str(&raw, TIMESTAMP_FORMAT).map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, Timelike};

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
    fn shared_fields_are_flattened_and_overridden_by_fields() {
        let mut event = LogEvent::new("hi");
        event.shared = Some(Arc::new(
            [
                ("namespace".to_owned(), Value::from("prod")),
                ("app".to_owned(), Value::from("shared")),
            ]
            .into_iter()
            .collect(),
        ));
        event.insert("app", "own");

        let json: Value = serde_json::from_str(&event.to_json_line().unwrap()).unwrap();
        assert_eq!(json["namespace"], "prod");
        assert_eq!(json["app"], "own");
        assert_eq!(event.get("namespace"), Some(&Value::from("prod")));
        assert_eq!(event.get("app"), Some(&Value::from("own")));
        assert!(event.estimated_size() > LogEvent::new("hi").estimated_size());
    }

    #[test]
    fn fast_timestamp_matches_chrono() {
        let cases = [
            "2026-09-07 11:04:08.914",
            "0001-01-01 00:00:00.000",
            "9999-12-31 23:59:59.999",
            "2026-02-28 09:05:03.007",
        ];
        for raw in cases {
            let ts = NaiveDateTime::parse_from_str(raw, TIMESTAMP_FORMAT).unwrap();
            let fast = format_timestamp(&ts).unwrap();
            assert_eq!(from_ascii(&fast), ts.format(TIMESTAMP_FORMAT).to_string());
            assert_eq!(from_ascii(&fast), raw);
        }
        // 纳秒截断到毫秒，不四舍五入
        let ts = NaiveDateTime::parse_from_str("2026-09-07 11:04:08", "%Y-%m-%d %H:%M:%S")
            .unwrap()
            .with_nanosecond(999_999_999)
            .unwrap();
        assert_eq!(
            from_ascii(&format_timestamp(&ts).unwrap()),
            "2026-09-07 11:04:08.999"
        );
        // 四位数之外的年份走慢路径
        let ancient = NaiveDate::from_ymd_opt(-1, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        assert!(format_timestamp(&ancient).is_none());
        let json: Value = serde_json::from_str(
            &LogEvent {
                timestamp: ancient,
                ..Default::default()
            }
            .to_json_line()
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            json["timestamp"],
            ancient.format(TIMESTAMP_FORMAT).to_string()
        );
    }

    #[test]
    fn appends_stack_trace_lines() {
        let mut event = LogEvent::new("boom");
        event.append_line("\tat com.foo.Bar.baz(Bar.java:1)");
        assert_eq!(event.message, "boom\n\tat com.foo.Bar.baz(Bar.java:1)");
    }
}
