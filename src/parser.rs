//! 把一行文本解析成 [`LogEvent`]。
//!
//! 三步：先按容器日志格式**拆壳**（[`LineDecoder`]），再按日志格式**解析字段**
//! （[`Parser`]），最后把异常堆栈**合并**进上一条日志（[`Aggregator`]）。

use std::borrow::Cow;
use std::sync::Arc;

use chrono::{Local, NaiveDateTime};
use regex::Regex;

use crate::error::{Error, Result};
use crate::event::LogEvent;

/// 默认格式：
/// `2026-09-07 11:04:08.914 [TID:xxx] [SpanID:xxx] [thread] INFO  c.a.c.Foo -消息`
///
/// TID / SpanID / thread 都是可选的，缺失时对应字段为空串。
pub const DEFAULT_PATTERN: &str = concat!(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?)",
    r"\s+(?:\[TID:(?P<trace_id>[^\]]*)\]\s*)?",
    r"(?:\[SpanID:(?P<span_id>[^\]]*)\]\s*)?",
    r"(?:\[(?P<thread>[^\]]*)\]\s*)?",
    r"(?P<level>[A-Z]+)\s+",
    r"(?P<logger>\S+)",
    r"\s*-?\s?(?P<message>.*)$",
);

/// 容器运行时给每行日志套的壳。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContainerFormat {
    /// 直接就是应用日志，没有外壳（写文件的场景）。
    #[default]
    Raw,
    /// containerd / CRI-O 的 `/var/log/pods/**/*.log`：
    /// `2026-09-07T03:04:08.914293456Z stdout F 应用日志...`
    Cri,
}

impl ContainerFormat {
    pub fn decoder(self) -> LineDecoder {
        match self {
            ContainerFormat::Raw => LineDecoder::Raw,
            ContainerFormat::Cri => LineDecoder::Cri(CriDecoder::default()),
        }
    }
}

/// 拆壳后的一行。
#[derive(Debug)]
pub struct DecodedLine<'a> {
    /// 真正的应用日志内容。
    pub content: Cow<'a, str>,
    /// `stdout` / `stderr`，没有壳时为 `None`。
    pub stream: Option<&'static str>,
    /// 运行时打的时间戳（已换成本机墙上时间），只在应用日志本身解析不出时间时兜底。
    pub time: Option<NaiveDateTime>,
}

/// 有状态的拆壳器：CRI 会把超长行切成多个 `P` 片段，需要跨行拼接。
/// 状态是**每个文件一份**的，由 source 负责创建。
#[derive(Debug)]
pub enum LineDecoder {
    Raw,
    Cri(CriDecoder),
}

impl LineDecoder {
    /// 返回 `None` 表示这一行只是个片段，先攒着，等结束标记再交出去。
    pub fn decode<'a>(&'a mut self, line: &'a str) -> Option<DecodedLine<'a>> {
        match self {
            LineDecoder::Raw => Some(DecodedLine {
                content: Cow::Borrowed(line),
                stream: None,
                time: None,
            }),
            LineDecoder::Cri(cri) => cri.decode(line),
        }
    }

    /// 取出没等到结束标记的残片（文件写到一半就被截断/轮转时）。
    pub fn take_partial(&mut self) -> Option<String> {
        match self {
            LineDecoder::Raw => None,
            LineDecoder::Cri(cri) => cri.take_partial(),
        }
    }
}

/// CRI 行格式：`<RFC3339Nano> <stream> <tag> <content>`，
/// tag 为 `F`（整行结束）或 `P`（片段，后面还有）。
#[derive(Debug, Default)]
pub struct CriDecoder {
    partial: String,
}

impl CriDecoder {
    fn decode<'a>(&'a mut self, line: &'a str) -> Option<DecodedLine<'a>> {
        // 壳不符合预期时不丢数据：整行当内容交给下一层。
        let Some((raw_time, rest)) = line.split_once(' ') else {
            return Some(Self::raw(line));
        };
        let Some((stream, rest)) = rest.split_once(' ') else {
            return Some(Self::raw(line));
        };
        let stream = match stream {
            "stdout" => "stdout",
            "stderr" => "stderr",
            _ => return Some(Self::raw(line)),
        };
        let (tag, content) = rest.split_once(' ').unwrap_or((rest, ""));

        // tag 是冒号分隔的，第一段才是 P/F
        let partial = match tag.split(':').next().unwrap_or(tag) {
            "P" => true,
            "F" => false,
            _ => return Some(Self::raw(line)),
        };

        if partial {
            self.partial.push_str(content);
            return None;
        }

        let content = if self.partial.is_empty() {
            Cow::Borrowed(content)
        } else {
            let mut joined = std::mem::take(&mut self.partial);
            joined.push_str(content);
            Cow::Owned(joined)
        };

        Some(DecodedLine {
            content,
            stream: Some(stream),
            // 运行时时间戳是 UTC，换成本机墙上时间，和落库口径保持一致。
            time: chrono::DateTime::parse_from_rfc3339(raw_time)
                .ok()
                .map(|dt| dt.with_timezone(&Local).naive_local()),
        })
    }

    fn raw(line: &str) -> DecodedLine<'_> {
        DecodedLine {
            content: Cow::Borrowed(line),
            stream: None,
            time: None,
        }
    }

    fn take_partial(&mut self) -> Option<String> {
        (!self.partial.is_empty()).then(|| std::mem::take(&mut self.partial))
    }
}

pub trait Parser: Send + Sync + 'static {
    /// 解析一行日志。返回 `None` 表示这是上一条日志的续行（例如异常堆栈）。
    fn parse(&self, line: &str) -> Option<LogEvent>;
}

/// 基于正则的行解析器，默认匹配我们线上的 logback 格式。
pub struct RegexParser {
    regex: Regex,
}

impl RegexParser {
    pub fn new() -> Self {
        Self::with_pattern(DEFAULT_PATTERN).expect("默认正则应当合法")
    }

    /// 自定义格式。可用的命名捕获组：`timestamp` `level` `trace_id` `span_id`
    /// `thread` `logger` `message`，全部可选。
    pub fn with_pattern(pattern: &str) -> Result<Self> {
        Ok(Self {
            regex: Regex::new(pattern).map_err(|e| Error::config(format!("正则非法: {e}")))?,
        })
    }

    /// 解析时间戳。日志里没有时区信息，这里也不做换算：拿到的就是墙上时间。
    fn parse_timestamp(&self, raw: &str) -> Option<NaiveDateTime> {
        const FORMATS: [&str; 4] = [
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
        ];
        FORMATS
            .iter()
            .find_map(|f| NaiveDateTime::parse_from_str(raw, f).ok())
    }
}

impl Default for RegexParser {
    fn default() -> Self {
        Self::new()
    }
}

impl Parser for RegexParser {
    fn parse(&self, line: &str) -> Option<LogEvent> {
        let caps = self.regex.captures(line)?;
        let group = |name: &str| {
            caps.name(name)
                .map(|m| m.as_str().trim().to_owned())
                .unwrap_or_default()
        };

        let timestamp = caps
            .name("timestamp")
            .and_then(|m| self.parse_timestamp(m.as_str()))
            .unwrap_or_else(|| Local::now().naive_local());

        Some(LogEvent {
            timestamp,
            level: group("level"),
            trace_id: group("trace_id"),
            span_id: group("span_id"),
            thread: group("thread"),
            logger: group("logger"),
            message: caps
                .name("message")
                .map(|m| m.as_str().to_owned())
                .unwrap_or_default(),
            ..Default::default()
        })
    }
}

/// 行聚合器：把「一条日志跨多行」的情况（异常堆栈）拼成一个事件。
///
/// 用法是逐行 `push`，拿到 `Some(event)` 就是**上一条**日志已经完整了；
/// 读到文件末尾/退出前调用 `flush` 取出最后一条。
pub struct Aggregator {
    parser: Arc<dyn Parser>,
    decoder: LineDecoder,
    pending: Option<LogEvent>,
    pending_lines: usize,
    /// 单条日志最多合并多少行，防止畸形堆栈把内存吃光。
    pub max_lines: usize,
    /// 单条日志正文最大字节数。
    pub max_bytes: usize,
}

impl Aggregator {
    pub fn new(parser: Arc<dyn Parser>) -> Self {
        Self {
            parser,
            decoder: LineDecoder::Raw,
            pending: None,
            pending_lines: 0,
            max_lines: 500,
            max_bytes: 256 * 1024,
        }
    }

    /// 指定容器日志的拆壳方式，默认不拆。
    pub fn decoder(mut self, decoder: LineDecoder) -> Self {
        self.decoder = decoder;
        self
    }

    pub fn max_lines(mut self, max_lines: usize) -> Self {
        self.max_lines = max_lines;
        self
    }

    pub fn max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    pub fn push(&mut self, line: &str) -> Option<LogEvent> {
        // 先拆壳。CRI 的片段行（tag = P）在这里被攒起来，不产出事件。
        let DecodedLine {
            content,
            stream,
            time,
        } = self.decoder.decode(line)?;

        match self.parser.parse(&content) {
            // 新的一条日志：把上一条交出去。
            Some(mut event) => {
                if let Some(stream) = stream {
                    event.insert("stream", stream);
                }
                self.pending_lines = 1;
                self.pending.replace(event)
            }
            // 续行：并入上一条；没有上一条时（比如从文件中间开始读）单独成条。
            None => match self.pending.as_mut() {
                Some(pending) => {
                    if self.pending_lines < self.max_lines
                        && pending.message.len() + content.len() < self.max_bytes
                    {
                        pending.append_line(&content);
                        self.pending_lines += 1;
                    }
                    None
                }
                None => {
                    let mut event = LogEvent::new(content.into_owned());
                    // 应用日志里没有时间戳时，用运行时打的时间兜底。
                    if let Some(time) = time {
                        event.timestamp = time;
                    }
                    if let Some(stream) = stream {
                        event.insert("stream", stream);
                    }
                    Some(event)
                }
            },
        }
    }

    /// 取出尚未闭合的最后一条日志。
    pub fn flush(&mut self) -> Option<LogEvent> {
        // 没等到结束标记的 CRI 片段也不能丢。
        if let Some(rest) = self.decoder.take_partial() {
            match self.pending.as_mut() {
                Some(pending) => pending.append_line(&rest),
                None => self.pending = Some(LogEvent::new(rest)),
            }
        }
        self.pending_lines = 0;
        self.pending.take()
    }

    /// 是否有正在等待续行的日志。
    pub fn has_pending(&self) -> bool {
        self.pending.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LINE: &str = "2026-09-07 11:04:08.914 [TID:e89a476882236ce0f1186d1522c8f59f] [SpanID:e8b0e73e2132f21c] [scheduledThreadPoolExecutor-1] INFO  c.a.c.service.DelayTaskService -redis延时任务触发检查,action数量0";

    #[test]
    fn parses_our_format() {
        let event = RegexParser::new().parse(LINE).expect("应当匹配");
        assert_eq!(event.level, "INFO");
        assert_eq!(event.trace_id, "e89a476882236ce0f1186d1522c8f59f");
        assert_eq!(event.span_id, "e8b0e73e2132f21c");
        assert_eq!(event.thread, "scheduledThreadPoolExecutor-1");
        assert_eq!(event.logger, "c.a.c.service.DelayTaskService");
        assert_eq!(event.message, "redis延时任务触发检查,action数量0");
        // 时间原样保留，不做时区换算
        assert_eq!(
            event.timestamp.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
            "2026-09-07 11:04:08.914"
        );
    }

    #[test]
    fn parses_line_without_trace_ids() {
        let event = RegexParser::new()
            .parse("2026-09-07 11:04:08.914 [main] WARN  c.a.Foo -启动慢")
            .expect("应当匹配");
        assert_eq!(event.thread, "main");
        assert!(event.trace_id.is_empty());
        assert_eq!(event.message, "启动慢");
    }

    #[test]
    fn stack_trace_merges_into_previous_event() {
        let mut agg = Aggregator::new(Arc::new(RegexParser::new()));
        assert!(agg.push(LINE).is_none());
        assert!(agg.push("java.lang.NullPointerException: boom").is_none());
        assert!(agg.push("\tat com.foo.Bar.baz(Bar.java:42)").is_none());

        let event = agg.push(LINE).expect("下一条日志到来时上一条应当闭合");
        assert_eq!(
            event.message,
            "redis延时任务触发检查,action数量0\njava.lang.NullPointerException: boom\n\tat com.foo.Bar.baz(Bar.java:42)"
        );
        assert!(agg.flush().is_some());
        assert!(agg.flush().is_none());
    }

    #[test]
    fn cri_shell_is_stripped() {
        let mut agg =
            Aggregator::new(Arc::new(RegexParser::new())).decoder(ContainerFormat::Cri.decoder());

        assert!(agg
            .push(&format!("2026-09-07T03:04:08.914293456Z stdout F {LINE}"))
            .is_none());
        assert!(agg
            .push("2026-09-07T03:04:09.000000000Z stderr F java.lang.NullPointerException: boom")
            .is_none());

        let event = agg.flush().expect("应当有一条");
        assert_eq!(event.level, "INFO");
        assert_eq!(event.logger, "c.a.c.service.DelayTaskService");
        assert_eq!(event.get("stream").unwrap(), "stdout");
        // stderr 上的堆栈仍然并进同一条日志
        assert_eq!(
            event.message,
            "redis延时任务触发检查,action数量0\njava.lang.NullPointerException: boom"
        );
    }

    #[test]
    fn cri_partial_lines_are_joined() {
        let mut agg =
            Aggregator::new(Arc::new(RegexParser::new())).decoder(ContainerFormat::Cri.decoder());

        // 超长行被运行时切成 P 片段，最后一段是 F
        assert!(agg
            .push("2026-09-07T03:04:08.914293456Z stdout P 2026-09-07 11:04:08.914 [TID:t] [SpanID:s] [main] INFO  c.a.Foo -前半")
            .is_none());
        assert!(agg
            .push("2026-09-07T03:04:08.914293456Z stdout F 后半")
            .is_none());

        let event = agg.flush().expect("拼接后应当成条");
        assert_eq!(event.message, "前半后半");
        assert_eq!(event.level, "INFO");
    }

    #[test]
    fn malformed_cri_line_is_kept_as_content() {
        let mut agg =
            Aggregator::new(Arc::new(RegexParser::new())).decoder(ContainerFormat::Cri.decoder());
        let event = agg.push("这行没有 CRI 外壳").expect("兜底成条");
        assert_eq!(event.message, "这行没有 CRI 外壳");
    }

    #[test]
    fn unparseable_leading_line_is_kept() {
        let mut agg = Aggregator::new(Arc::new(RegexParser::new()));
        let event = agg.push("no timestamp here").expect("兜底成条");
        assert_eq!(event.message, "no timestamp here");
    }
}
