//! 把一行文本解析成 [`LogEvent`]。
//!
//! 三步：先按容器日志格式**拆壳**（[`LineDecoder`]），再按日志格式**解析字段**
//! （[`Parser`]），最后把异常堆栈**合并**进上一条日志（[`Aggregator`]）。

use std::borrow::Cow;
use std::sync::Arc;

use chrono::{FixedOffset, Local, NaiveDate, NaiveDateTime, TimeZone};
use regex::Regex;

use crate::error::{Error, Result};
use crate::event::LogEvent;

/// 默认格式：
/// `2026-09-07 11:04:08.914 [TID:xxx] [SpanID:xxx] [thread] INFO  c.a.c.Foo -消息`
///
/// TID / SpanID / thread 都是可选的，缺失时对应字段为空串。另外兼容 logback 的
/// 两个常见变体：毫秒用逗号分隔（`ISO8601` 默认的 `15:20:43,633`）、级别带方括号
/// （`[DEBUG]`，`%-5level` 之外的另一种写法）。
///
/// **正文不写成捕获组**：正则只匹配到「头部」为止，剩下的整段就是正文，按整条
/// 匹配的结束位置切片取。写成 `(?P<message>.*)$` 的话，捕获要跟着正文一路走完，
/// 解析耗时随正文长度线性上升 —— 实测 200B 正文慢 2 倍、2KB 慢 13 倍、20KB 慢
/// 295 倍（0.55ms 一条），生产上只要有服务打大 payload 就会拖垮整个节点的采集。
/// 自定义 pattern 里仍然可以写 `message` 组，[`RegexParser`] 会照旧从捕获里取。
pub const DEFAULT_PATTERN: &str = concat!(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d{1,9})?)",
    r"\s+(?:\[TID:(?P<trace_id>[^\]]*)\]\s*)?",
    r"(?:\[SpanID:(?P<span_id>[^\]]*)\]\s*)?",
    r"(?:\[(?P<thread>[^\]]*)\]\s*)?",
    r"\[?(?P<level>[A-Z]+)\]?\s+",
    r"(?P<logger>\S+)",
    r"\s*-?\s?",
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

/// 基于正则的行解析器，给自定义 `pattern` 用；默认格式走 [`LogbackParser`]（同样的
/// 语义，快一个数量级）。
pub struct RegexParser {
    regex: Regex,
    /// 自定义 pattern 里是否写了 `message` 组。默认 pattern 没有，正文按整条匹配
    /// 结束的位置切片取，见 [`DEFAULT_PATTERN`]。
    has_message_group: bool,
}

impl RegexParser {
    pub fn new() -> Self {
        Self::with_pattern(DEFAULT_PATTERN).expect("默认正则应当合法")
    }

    /// 自定义格式。可用的命名捕获组：`timestamp` `level` `trace_id` `span_id`
    /// `thread` `logger` `message`，全部可选。
    pub fn with_pattern(pattern: &str) -> Result<Self> {
        let regex = Regex::new(pattern).map_err(|e| Error::config(format!("正则非法: {e}")))?;
        let has_message_group = regex
            .capture_names()
            .flatten()
            .any(|name| name == "message");
        Ok(Self {
            regex,
            has_message_group,
        })
    }
}

/// 读一段定长的十进制数字，有一位不是数字就作废。
fn digits(bytes: &[u8]) -> Option<u32> {
    let mut value = 0u32;
    for &byte in bytes {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value * 10 + (byte - b'0') as u32;
    }
    Some(value)
}

/// 解析时间戳。日志里没有时区信息，这里也不做换算：拿到的就是墙上时间。
///
/// 格式是定长的 `YYYY-MM-DD[ T]HH:MM:SS`，后面跟可选的小数秒（logback 的 ISO8601
/// 用逗号分隔，所以 `.` 和 `,` 都收）。这里手写扫描而不是用 chrono 的
/// `parse_from_str` 挨个试格式：实测 158ns -> 8ns，而这是每条日志都要走的路径。
/// 把日志里的 trace id 整理成 Jaeger / OTel 认的写法。
///
/// 日志和 trace 的联动靠这一列**精确相等**：Grafana 从 span 跳日志是
/// `where trace_id = '<span 的 traceId>'`，从日志跳 Jaeger 是拿原值拼 URL。
/// 所以入库前把写法统一掉，而不是留给查询时 `lower()`：
///
/// * 全是 hex 的转小写 —— W3C `traceparent` 规定小写，Java agent 也都这么打，
///   但 `.NET`/自定义 MDC 有大写的；
/// * 16 位 hex（64 位老格式）左补零到 32 位 —— Jaeger 存储里就是这么存的，
///   两边不统一等值查询就对不上；
/// * SkyWalking 没有 trace 上下文时打的 `N/A` 置空，别让字面量进库；
/// * 其他写法原样保留：不是我们认得的格式，但丢了信息更糟。
///
/// 热路径上跑，正常的 32 位小写 hex 只做一次分配，和原来 `to_owned` 一样。
fn normalize_trace_id(raw: &str) -> String {
    normalize_id(raw, 32)
}

/// 同 [`normalize_trace_id`]，span id 是 64 位，16 位 hex。
fn normalize_span_id(raw: &str) -> String {
    normalize_id(raw, 16)
}

fn normalize_id(raw: &str, width: usize) -> String {
    let raw = raw.trim();
    if raw.is_empty() || raw.eq_ignore_ascii_case("N/A") {
        return String::new();
    }
    let bytes = raw.as_bytes();
    if !bytes.iter().all(u8::is_ascii_hexdigit) {
        return raw.to_owned();
    }
    // 只有 16 位的 trace id 才补零；span id 传进来 width 就是 16，等长不补。
    let pad = if bytes.len() == 16 && width == 32 {
        16
    } else {
        0
    };
    let mut out = String::with_capacity(pad + bytes.len());
    for _ in 0..pad {
        out.push('0');
    }
    if bytes.iter().any(u8::is_ascii_uppercase) {
        out.extend(raw.chars().map(|c| c.to_ascii_lowercase()));
    } else {
        out.push_str(raw);
    }
    out
}

fn parse_timestamp(raw: &str) -> Option<NaiveDateTime> {
    let bytes = raw.as_bytes();
    if bytes.len() < 19
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || (bytes[10] != b' ' && bytes[10] != b'T')
    {
        return None;
    }

    let year = digits(&bytes[0..4])? as i32;
    let (month, day) = (digits(&bytes[5..7])?, digits(&bytes[8..10])?);
    let (hour, minute, second) = (
        digits(&bytes[11..13])?,
        digits(&bytes[14..16])?,
        digits(&bytes[17..19])?,
    );

    // 小数秒最多 9 位，按位补齐到纳秒（`.914` -> 914_000_000）。
    let mut nano = 0u32;
    if bytes.len() > 19 {
        if bytes[19] != b'.' && bytes[19] != b',' {
            return None;
        }
        let fraction = &bytes[20..];
        if fraction.is_empty() || fraction.len() > 9 {
            return None;
        }
        let mut scale = 100_000_000u32;
        for &byte in fraction {
            if !byte.is_ascii_digit() {
                return None;
            }
            nano += (byte - b'0') as u32 * scale;
            scale /= 10;
        }
    }

    NaiveDate::from_ymd_opt(year, month, day)?.and_hms_nano_opt(hour, minute, second, nano)
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
            .and_then(|m| parse_timestamp(m.as_str()))
            .unwrap_or_else(|| Local::now().naive_local());

        // 头部匹配到哪里，正文就从哪里开始。见 [`DEFAULT_PATTERN`] 里为什么不用捕获组。
        let message = if self.has_message_group {
            caps.name("message")
                .map(|m| m.as_str().to_owned())
                .unwrap_or_default()
        } else {
            let head_end = caps.get(0).map_or(line.len(), |m| m.end());
            line[head_end..].to_owned()
        };

        let raw = |name: &str| caps.name(name).map_or("", |m| m.as_str());
        Some(LogEvent {
            timestamp,
            level: group("level"),
            trace_id: normalize_trace_id(raw("trace_id")),
            span_id: normalize_span_id(raw("span_id")),
            thread: group("thread"),
            logger: group("logger"),
            message,
            ..Default::default()
        })
    }
}

/// 默认格式的手写解析器：语义与 [`DEFAULT_PATTERN`] 一致，但不走正则引擎。
///
/// 正则版解析同一行要 2.3µs，其中 1.5µs 花在给几个可选的 `[...]` 组回填捕获位置上
/// （`find` 只要 0.3µs；这种带歧义的可选组 regex 做不了 one-pass，只能退到回溯引擎）。
/// 按格式顺序切一遍只要 0.2µs，而这是每条日志都要走的路径。自定义 `pattern`
/// 仍然走 [`RegexParser`]，两者的等价性由测试保证。
pub struct LogbackParser;

/// 一行 logback 日志的头部，全部是对原行的切片。
struct LogbackHead<'a> {
    timestamp: &'a str,
    trace_id: &'a str,
    span_id: &'a str,
    thread: &'a str,
    level: &'a str,
    logger: &'a str,
    message: &'a str,
}

/// `\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d{1,9})?` 的长度。
fn logback_timestamp_len(bytes: &[u8]) -> Option<usize> {
    if bytes.len() < 19 {
        return None;
    }
    let all_digits = |range: std::ops::Range<usize>| bytes[range].iter().all(u8::is_ascii_digit);
    let shape_ok = all_digits(0..4)
        && bytes[4] == b'-'
        && all_digits(5..7)
        && bytes[7] == b'-'
        && all_digits(8..10)
        && (bytes[10] == b' ' || bytes[10] == b'T')
        && all_digits(11..13)
        && bytes[13] == b':'
        && all_digits(14..16)
        && bytes[16] == b':'
        && all_digits(17..19);
    if !shape_ok {
        return None;
    }

    let mut end = 19;
    if end < bytes.len() && (bytes[end] == b'.' || bytes[end] == b',') {
        let fraction = bytes[end + 1..]
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        // 正则里小数是可选组：`.` 后面没有数字时组整体不匹配，紧跟着的 `\s+` 落在 `.`
        // 上就失败了；超过 9 位时组只吃 9 位，`\s+` 落在第 10 位数字上同样失败。
        if fraction == 0 || fraction > 9 {
            return None;
        }
        end += 1 + fraction;
    }
    Some(end)
}

/// 取开头的 `[...]`，返回括号内的内容和后面（去掉 `\s*`）的剩余部分。
fn logback_bracket(raw: &str) -> Option<(&str, &str)> {
    let inner = raw.strip_prefix('[')?;
    let end = inner.find(']')?;
    Some((&inner[..end], inner[end + 1..].trim_start()))
}

/// `\[?(?P<level>[A-Z]+)\]?\s+(?P<logger>\S+)\s*-?\s?`，返回 (level, logger, message)。
fn logback_level_and_rest(raw: &str) -> Option<(&str, &str, &str)> {
    let raw = raw.strip_prefix('[').unwrap_or(raw);
    let level_len = raw.bytes().take_while(u8::is_ascii_uppercase).count();
    if level_len == 0 {
        return None;
    }
    let level = &raw[..level_len];
    let after = raw[level_len..]
        .strip_prefix(']')
        .unwrap_or(&raw[level_len..]);

    // `\s+`：至少一个空白
    let logger_start = after.trim_start();
    if logger_start.len() == after.len() {
        return None;
    }
    let logger_len = logger_start
        .find(char::is_whitespace)
        .unwrap_or(logger_start.len());
    if logger_len == 0 {
        return None;
    }
    let logger = &logger_start[..logger_len];

    // `\s*-?\s?`
    let mut message = logger_start[logger_len..].trim_start();
    if let Some(rest) = message.strip_prefix('-') {
        message = rest;
    }
    if let Some(first) = message.chars().next().filter(|c| c.is_whitespace()) {
        message = &message[first.len_utf8()..];
    }
    Some((level, logger, message))
}

fn parse_logback_head(line: &str) -> Option<LogbackHead<'_>> {
    let timestamp_end = logback_timestamp_len(line.as_bytes())?;
    let timestamp = &line[..timestamp_end];

    // 时间戳后面的 `\s+`
    let after = &line[timestamp_end..];
    let mut rest = after.trim_start();
    if rest.len() == after.len() {
        return None;
    }

    let mut trace_id = "";
    let mut span_id = "";
    let mut thread = "";
    if let Some((body, next)) = logback_bracket(rest) {
        if let Some(value) = body.strip_prefix("TID:") {
            trace_id = value;
            rest = next;
        }
    }
    if let Some((body, next)) = logback_bracket(rest) {
        if let Some(value) = body.strip_prefix("SpanID:") {
            span_id = value;
            rest = next;
        }
    }
    // 接下来的方括号是 thread 还是 `[LEVEL]`：和正则一样先按 thread 试，后面接得上
    // level 才算；接不上就把这个方括号本身当 `[LEVEL]` 重新解析。
    let (level, logger, message) = match logback_bracket(rest) {
        Some((body, next)) => match logback_level_and_rest(next) {
            Some(tail) => {
                thread = body;
                tail
            }
            None => logback_level_and_rest(rest)?,
        },
        None => logback_level_and_rest(rest)?,
    };

    Some(LogbackHead {
        timestamp,
        trace_id,
        span_id,
        thread,
        level,
        logger,
        message,
    })
}

impl Parser for LogbackParser {
    fn parse(&self, line: &str) -> Option<LogEvent> {
        let head = parse_logback_head(line)?;
        Some(LogEvent {
            timestamp: parse_timestamp(head.timestamp)
                .unwrap_or_else(|| Local::now().naive_local()),
            level: head.level.trim().to_owned(),
            trace_id: normalize_trace_id(head.trace_id),
            span_id: normalize_span_id(head.span_id),
            thread: head.thread.trim().to_owned(),
            logger: head.logger.trim().to_owned(),
            message: head.message.to_owned(),
            ..Default::default()
        })
    }
}

/// 日志格式。同一个节点上不同语言栈的格式不一样（Java 打 logback、Go 服务顺手
/// 就把 gin 的 access log 打到 stdout、网关是 nginx），配置里按顺序列几个，
/// 由 [`ChainParser`] 依次尝试。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    /// 我们线上的 Java 格式，见 [`DEFAULT_PATTERN`]。
    #[default]
    Logback,
    /// gin 默认的 access log。
    Gin,
    /// nginx 的 combined access log（CLF）。
    Nginx,
}

impl LogFormat {
    /// 建出对应的解析器。`logback` 可以用自定义正则替掉默认 pattern。
    pub fn parser(self, pattern: Option<&str>) -> Result<Box<dyn Parser>> {
        Ok(match self {
            LogFormat::Logback => match pattern {
                Some(pattern) => Box::new(RegexParser::with_pattern(pattern)?),
                None => Box::new(LogbackParser),
            },
            LogFormat::Gin => Box::new(GinParser),
            LogFormat::Nginx => Box::new(NginxParser),
        })
    }
}

/// 按顺序试多个解析器，第一个匹配上的胜出。
///
/// 全都不匹配时返回 `None`，落到 [`Aggregator`] 的续行/兜底逻辑 —— 这也是为什么
/// 混合格式一定要配全：没配上的那种日志会被当成上一条的堆栈粘上去。
///
/// 把最常见的格式放在前面。判不出格式的开销很低（logback 的正则卡在行首的
/// `\d{4}-`，gin 比一下行首的 `[GIN]`，nginx 找不到 `[时间]` 就走），
/// 所以列表长一点也不心疼。
pub struct ChainParser {
    parsers: Vec<Box<dyn Parser>>,
}

impl ChainParser {
    pub fn new(parsers: Vec<Box<dyn Parser>>) -> Self {
        Self { parsers }
    }

    /// 按格式列表建链。`pattern` 只作用在 `logback` 那一档上。
    pub fn from_formats(formats: &[LogFormat], pattern: Option<&str>) -> Result<Self> {
        let mut parsers = Vec::with_capacity(formats.len());
        for format in formats {
            parsers.push(format.parser(pattern)?);
        }
        Ok(Self::new(parsers))
    }
}

impl Parser for ChainParser {
    fn parse(&self, line: &str) -> Option<LogEvent> {
        self.parsers.iter().find_map(|parser| parser.parse(line))
    }
}

/// access log 共用的成条逻辑：状态码折算 level，**整行留在 `message` 里**。
///
/// 不把 status / path / latency 抽成独立字段：这张表混着业务日志，为少数行加一堆
/// 稀疏列，换来的只是「拿日志表做访问分析」这一个场景，而那个场景本来就该单独
/// 建表。要按状态码筛，`level` 折算已经够用；要精确到 path，从 `message` 里抠。
fn access_event(timestamp: NaiveDateTime, logger: &str, status: u16, line: &str) -> LogEvent {
    LogEvent {
        timestamp,
        level: level_of_status(status).to_owned(),
        logger: logger.to_owned(),
        message: line.to_owned(),
        ..Default::default()
    }
}

/// access log 没有级别，用状态码折算一个，好让按 level 过滤/告警对它照样生效。
fn level_of_status(status: u16) -> &'static str {
    match status {
        500.. => "ERROR",
        400..=499 => "WARN",
        _ => "INFO",
    }
}

/// gin 默认的 access log：
/// `[GIN] 2026/09/07 - 10:07:01 | 200 |      40.601µs |    172.16.250.3 | POST     "/extra_text"`
///
/// 手写解析而不是上正则：行首 `[GIN]` 一比就能判掉不是这个格式的行，比让正则引擎
/// 去试要便宜，而这是每条日志都要走的路径。
///
/// 两个已知不管的情况：`gin.ForceConsoleColor()` 的彩色输出（状态码被 ANSI 转义
/// 包着，解析不出，整行进 `message`）、启动时的 `[GIN-debug]` 路由表（不是 access
/// log，本来也不该按这个折算 level）。
pub struct GinParser;

impl Parser for GinParser {
    fn parse(&self, line: &str) -> Option<LogEvent> {
        let rest = line.strip_prefix("[GIN]")?;
        // 只要前两段：时间和状态码。后面的耗时 / IP / 方法 / 路径留在 message 里。
        let mut parts = rest.split('|');
        let timestamp = parse_gin_timestamp(parts.next()?.trim())?;
        let status: u16 = parts.next()?.trim().parse().ok()?;
        Some(access_event(timestamp, "gin", status, line))
    }
}

/// nginx 的 combined access log：
/// `127.0.0.6 - - [07/Sep/2026:18:06:53 +0800] "GET / HTTP/1.1" 200 601 "-" "kube-probe/1.28+" "-"`
///
/// 判定只押在 `[时间]` 和状态码这两处严格解析上：combined 前后的 `$remote_user` /
/// `$http_x_forwarded_for` 各家改得五花八门，跳过就是了。
pub struct NginxParser;

impl Parser for NginxParser {
    fn parse(&self, line: &str) -> Option<LogEvent> {
        // `$remote_addr - $remote_user [$time_local] "$request" $status ...`
        let open = line.find(" [")?;
        let (raw_time, rest) = line[open + 2..].split_once(']')?;
        let timestamp = parse_clf_timestamp(raw_time)?;

        // 状态码在 `"$request"` 后面，先把带引号的请求行整段跳过
        let (_request, rest) = take_quoted(rest.trim_start())?;
        let status: u16 = rest.trim_start().split(' ').next()?.parse().ok()?;
        Some(access_event(timestamp, "nginx", status, line))
    }
}

/// 取出开头的 `"..."`，返回引号内的内容和后面剩下的部分。
///
/// 不处理转义：nginx 默认把正文里的 `"` 转成 `\x22`，不会有裸引号。
fn take_quoted(raw: &str) -> Option<(&str, &str)> {
    let rest = raw.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some((&rest[..end], &rest[end + 1..]))
}

/// gin 的 `2026/09/07 - 10:07:01`。gin 打的是服务本地时间，按项目口径原样取，
/// 不做换算。
fn parse_gin_timestamp(raw: &str) -> Option<NaiveDateTime> {
    let bytes = raw.as_bytes();
    if bytes.len() != 21
        || bytes[4] != b'/'
        || bytes[7] != b'/'
        || &bytes[10..13] != b" - "
        || bytes[15] != b':'
        || bytes[18] != b':'
    {
        return None;
    }

    let year = digits(&bytes[0..4])? as i32;
    let (month, day) = (digits(&bytes[5..7])?, digits(&bytes[8..10])?);
    let (hour, minute, second) = (
        digits(&bytes[13..15])?,
        digits(&bytes[16..18])?,
        digits(&bytes[19..21])?,
    );
    NaiveDate::from_ymd_opt(year, month, day)?.and_hms_opt(hour, minute, second)
}

/// CLF 的 `07/Sep/2026:18:06:53 +0800`。
///
/// 带偏移时按 CRI 拆壳那套口径换成本机墙上时间（容器跑 UTC、采集端 +0800 也能
/// 对上）；不带偏移就当本地时间。
fn parse_clf_timestamp(raw: &str) -> Option<NaiveDateTime> {
    let bytes = raw.as_bytes();
    if bytes.len() < 20
        || bytes[2] != b'/'
        || bytes[6] != b'/'
        || bytes[11] != b':'
        || bytes[14] != b':'
        || bytes[17] != b':'
    {
        return None;
    }

    let day = digits(&bytes[0..2])?;
    let month = month_of_abbrev(&bytes[3..6])?;
    let year = digits(&bytes[7..11])? as i32;
    let (hour, minute, second) = (
        digits(&bytes[12..14])?,
        digits(&bytes[15..17])?,
        digits(&bytes[18..20])?,
    );
    let naive = NaiveDate::from_ymd_opt(year, month, day)?.and_hms_opt(hour, minute, second)?;

    if bytes.len() == 20 {
        return Some(naive);
    }
    // ` +0800`
    if bytes.len() != 26 || bytes[20] != b' ' {
        return None;
    }
    let sign = match bytes[21] {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let seconds = (digits(&bytes[22..24])? * 3600 + digits(&bytes[24..26])? * 60) as i32;
    let offset = FixedOffset::east_opt(sign * seconds)?;
    Some(
        offset
            .from_local_datetime(&naive)
            .single()?
            .with_timezone(&Local)
            .naive_local(),
    )
}

fn month_of_abbrev(raw: &[u8]) -> Option<u32> {
    Some(match raw {
        b"Jan" => 1,
        b"Feb" => 2,
        b"Mar" => 3,
        b"Apr" => 4,
        b"May" => 5,
        b"Jun" => 6,
        b"Jul" => 7,
        b"Aug" => 8,
        b"Sep" => 9,
        b"Oct" => 10,
        b"Nov" => 11,
        b"Dec" => 12,
        _ => return None,
    })
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

    /// 手写解析器和正则在所有变体上给出同样的结果。
    #[test]
    fn hand_parser_matches_regex() {
        let regex = RegexParser::new();
        let hand = LogbackParser;
        let lines = [
            LINE,
            "2026-09-07 11:04:08.914 [main] WARN  c.a.Foo -启动慢",
            "2026-09-07 15:20:43,633 [DEBUG] o.s.w.Foo Returning handler method [public x]",
            "2026-09-07 15:20:43,633 [http-nio-8080-exec-1] [WARN] c.j.b.Foo -慢查询",
            "2026-09-07T15:20:43.633123456 ERROR c.j.b.Foo - 带空格的分隔",
            "2026-09-07 15:20:43 INFO c.j.b.Foo 没有横线",
            "2026-09-07 15:20:43 INFO c.j.b.Foo  -  正文前多一个空格",
            "2026-09-07 15:20:43 INFO c.j.b.Foo",
            "2026-09-07 15:20:43 INFO c.j.b.Foo -",
            "2026-09-07 15:20:43 [TID:][SpanID:][t]INFO c.j.b.Foo -紧挨着",
            "2026-09-07 15:20:43 [SpanID:s] [TID:t] INFO c.j.b.Foo -顺序反了",
            "2026-09-07 15:20:43 [TID: 带空格 ] INFO c.j.b.Foo -x",
            "2026-09-07 15:20:43 [INFO] [WARN] c.j.b.Foo -两个大写括号",
            "2026-09-07 15:20:43 [INFO logger -少个右括号",
            "2026-09-07 15:20:43 INFO] logger -少个左括号",
            "2026-09-07 15:20:43 [tid] [span] [thread] INFO logger -三个普通括号",
            // 以下都不该匹配
            "\tat com.foo.Bar.baz(Bar.java:1)",
            "2026-09-07 15:20:43. INFO c.j.b.Foo -小数点后没数字",
            "2026-09-07 15:20:43.1234567890 INFO c.j.b.Foo -小数十位",
            "2026-09-07 15:20:43INFO c.j.b.Foo -时间后没空白",
            "2026-09-07 15:20:43 info c.j.b.Foo -小写级别",
            "2026-09-07 15:20:43 INFOx c.j.b.Foo -级别后粘着小写",
            "2026-09-07 15:20:43 INFO",
            "2026-09-07 15:20:43 [t] INFO",
            "2026/09/07 15:20:43 INFO c.j.b.Foo -日期分隔符不对",
            "",
        ];
        let fields = |event: Option<LogEvent>| {
            event.map(|e| {
                (
                    e.timestamp,
                    e.level,
                    e.trace_id,
                    e.span_id,
                    e.thread,
                    e.logger,
                    e.message,
                )
            })
        };
        for line in lines {
            assert_eq!(
                fields(hand.parse(line)),
                fields(regex.parse(line)),
                "行: {line:?}"
            );
        }
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
    fn normalizes_trace_ids_for_jaeger() {
        // 大写转小写、64 位补零、N/A 置空 —— 两个解析器口径一致
        for parser in [
            Box::new(RegexParser::new()) as Box<dyn Parser>,
            Box::new(LogbackParser),
        ] {
            let event = parser
                .parse("2026-09-07 11:04:08.914 [TID:E89A476882236CE0F1186D1522C8F59F] [SpanID:E8B0E73E2132F21C] [main] INFO c.a.Foo -x")
                .expect("应当匹配");
            assert_eq!(event.trace_id, "e89a476882236ce0f1186d1522c8f59f");
            assert_eq!(event.span_id, "e8b0e73e2132f21c");

            let event = parser
                .parse("2026-09-07 11:04:08.914 [TID:e8b0e73e2132f21c] [SpanID:e8b0e73e2132f21c] [main] INFO c.a.Foo -x")
                .expect("应当匹配");
            assert_eq!(event.trace_id, "0000000000000000e8b0e73e2132f21c");
            assert_eq!(event.span_id, "e8b0e73e2132f21c", "span id 不补零");

            let event = parser
                .parse("2026-09-07 11:04:08.914 [TID:N/A] [main] INFO c.a.Foo -x")
                .expect("应当匹配");
            assert!(event.trace_id.is_empty(), "{:?}", event.trace_id);
            assert_eq!(event.thread, "main");

            // 认不出的格式原样保留（比如 SkyWalking 带点号的 id）
            let event = parser
                .parse("2026-09-07 11:04:08.914 [TID:a1b2c3d4e5f6.53.17257470000000001] [main] INFO c.a.Foo -x")
                .expect("应当匹配");
            assert_eq!(event.trace_id, "a1b2c3d4e5f6.53.17257470000000001");
        }
    }

    #[test]
    fn parses_bracketed_level_with_comma_millis() {
        // logback `%d{ISO8601} [%level] %logger %msg`：毫秒是逗号，级别带方括号，没有 thread
        let event = RegexParser::new()
            .parse("2026-09-07 15:20:43,633 [DEBUG] o.s.w.s.m.m.a.RequestMappingHandlerMapping Returning handler method [public com.jubotech.framework.domain.base.BaseResp com.jubotech.business.web.controller.UploadController.health() throws java.lang.Exception]")
            .expect("应当匹配");
        assert_eq!(event.level, "DEBUG");
        assert_eq!(event.logger, "o.s.w.s.m.m.a.RequestMappingHandlerMapping");
        assert!(event.thread.is_empty());
        assert_eq!(
            event.message,
            "Returning handler method [public com.jubotech.framework.domain.base.BaseResp com.jubotech.business.web.controller.UploadController.health() throws java.lang.Exception]"
        );
        // 逗号毫秒也要落到时间戳里，不能被丢掉
        assert_eq!(
            event.timestamp.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
            "2026-09-07 15:20:43.633"
        );
    }

    #[test]
    fn parses_bracketed_level_after_thread() {
        let event = RegexParser::new()
            .parse("2026-09-07 15:20:43,633 [http-nio-8080-exec-1] [WARN] c.j.b.Foo -慢查询")
            .expect("应当匹配");
        assert_eq!(event.thread, "http-nio-8080-exec-1");
        assert_eq!(event.level, "WARN");
        assert_eq!(event.logger, "c.j.b.Foo");
        assert_eq!(event.message, "慢查询");
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
    fn timestamp_variants_all_parse() {
        // 手写定长解析要覆盖 chrono 那四种格式 + logback 的逗号毫秒
        let cases = [
            ("2026-09-07 11:04:08.914", "2026-09-07 11:04:08.914"),
            ("2026-09-07T11:04:08.914", "2026-09-07 11:04:08.914"),
            ("2026-09-07 11:04:08", "2026-09-07 11:04:08.000"),
            ("2026-09-07T11:04:08", "2026-09-07 11:04:08.000"),
            ("2026-09-07 11:04:08,633", "2026-09-07 11:04:08.633"),
            ("2026-09-07 11:04:08.914293456", "2026-09-07 11:04:08.914"),
            ("2026-09-07 11:04:08.9", "2026-09-07 11:04:08.900"),
        ];
        for (raw, want) in cases {
            let got = parse_timestamp(raw).unwrap_or_else(|| panic!("应当解析 {raw}"));
            assert_eq!(
                got.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
                want,
                "{raw}"
            );
        }

        // 非法输入不能悄悄变成一个错误的时间
        for bad in [
            "",
            "2026-09-07",
            "2026/09/07 11:04:08",
            "2026-09-07x11:04:08",
            "2026-13-07 11:04:08",            // 月份越界
            "2026-09-07 25:04:08",            // 小时越界
            "2026-09-07 11:04:08.",           // 有分隔符没数字
            "2026-09-07 11:04:08.1234567890", // 小数秒超过 9 位
            "2026-09-07 11:04:0a",
        ] {
            assert!(parse_timestamp(bad).is_none(), "不应解析 {bad:?}");
        }
    }

    #[test]
    fn message_is_sliced_not_captured() {
        // 正文不再走捕获组，长正文也要原样取回来
        let long = "x".repeat(50_000);
        let event = RegexParser::new()
            .parse(&format!(
                "2026-09-07 11:04:08.914 [main] INFO  c.a.Foo -{long}"
            ))
            .expect("应当匹配");
        assert_eq!(event.message, long);
        assert_eq!(event.logger, "c.a.Foo");

        // 正文里带分隔符样式的字符也不能被吃掉
        let event = RegexParser::new()
            .parse("2026-09-07 11:04:08.914 [main] INFO  c.a.Foo -a - b -c")
            .expect("应当匹配");
        assert_eq!(event.message, "a - b -c");

        // 空正文
        let event = RegexParser::new()
            .parse("2026-09-07 11:04:08.914 [main] INFO  c.a.Foo -")
            .expect("应当匹配");
        assert_eq!(event.message, "");
    }

    #[test]
    fn custom_pattern_with_message_group_still_works() {
        // 自定义 pattern 写了 message 组时，仍然从捕获里取
        let parser = RegexParser::with_pattern(
            r"^(?P<timestamp>\S+ \S+) (?P<level>[A-Z]+) (?P<message>.*)$",
        )
        .unwrap();
        let event = parser
            .parse("2026-09-07 11:04:08.914 ERROR 出事了")
            .expect("应当匹配");
        assert_eq!(event.level, "ERROR");
        assert_eq!(event.message, "出事了");
    }

    const GIN_LINE: &str = r#"[GIN] 2026/09/07 - 10:07:01 | 200 |      40.601µs |    172.16.250.3 | POST     "/extra_text""#;
    const NGINX_LINE: &str = r#"127.0.0.6 - - [07/Sep/2026:18:06:53 +0800] "GET / HTTP/1.1" 200 601 "-" "kube-probe/1.28+" "-""#;

    #[test]
    fn parses_gin_access_log() {
        let event = GinParser.parse(GIN_LINE).expect("应当匹配");
        // 时间是日志自己的，不是采集时刻
        assert_eq!(
            event.timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
            "2026-09-07 10:07:01"
        );
        assert_eq!(event.logger, "gin");
        assert_eq!(event.level, "INFO");
        // 整行原样留着，不抽字段
        assert_eq!(event.message, GIN_LINE);
        assert!(event.fields.is_empty());
    }

    #[test]
    fn parses_gin_variants() {
        // 新版本 gin 的路径不带引号；5xx 折算成 ERROR
        let event = GinParser
            .parse(
                "[GIN] 2026/09/07 - 10:07:01 | 502 |     1.5ms |     10.0.0.1 | GET      /healthz",
            )
            .expect("应当匹配");
        assert_eq!(event.level, "ERROR");

        // 4xx 折算成 WARN，路径里带 `|` 也不影响
        let event = GinParser
            .parse(r#"[GIN] 2026/09/07 - 10:07:01 | 404 | 1ms | 10.0.0.1 | GET      "/a|b""#)
            .expect("应当匹配");
        assert_eq!(event.level, "WARN");

        // 启动时的路由表不是 access log
        assert!(GinParser
            .parse("[GIN-debug] POST   /extra_text  --> main.handler (3 handlers)")
            .is_none());
        // 彩色输出解析不出来，交给兜底
        assert!(GinParser
            .parse(
                "[GIN] 2026/09/07 - 10:07:01 |\u{1b}[97;42m 200 \u{1b}[0m| 1ms | 10.0.0.1 | GET /"
            )
            .is_none());
    }

    #[test]
    fn parses_nginx_combined_log() {
        let event = NginxParser.parse(NGINX_LINE).expect("应当匹配");
        assert_eq!(event.logger, "nginx");
        assert_eq!(event.level, "INFO");
        assert_eq!(event.message, NGINX_LINE);
        assert!(event.fields.is_empty());

        // +0800 的日志在 +0800 的机器上时间不变
        let want = chrono::FixedOffset::east_opt(8 * 3600)
            .unwrap()
            .with_ymd_and_hms(2026, 9, 7, 18, 6, 53)
            .unwrap()
            .with_timezone(&Local)
            .naive_local();
        assert_eq!(event.timestamp, want);
    }

    #[test]
    fn parses_nginx_variants() {
        // body_bytes_sent 为 `-`、remote_user 非空、请求行里带查询串
        let event = NginxParser
            .parse(r#"10.1.2.3 - admin [07/Sep/2026:18:06:53 +0000] "POST /api/v1/order?id=1 HTTP/2.0" 500 - "https://example.com/x" "curl/8.4.0""#)
            .expect("应当匹配");
        assert_eq!(event.level, "ERROR");

        // 不带时区偏移的 CLF
        let event = NginxParser
            .parse(r#"10.1.2.3 - - [07/Sep/2026:18:06:53] "GET / HTTP/1.1" 304 0 "-" "-""#)
            .expect("应当匹配");
        assert_eq!(
            event.timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
            "2026-09-07 18:06:53"
        );

        // Java 日志里带方括号的正文不能被误判成 nginx
        assert!(NginxParser
            .parse(
                "2026-09-07 15:20:43,633 [main] INFO c.a.Foo -handler [public void foo()] 200 ok"
            )
            .is_none());
    }

    #[test]
    fn chain_tries_formats_in_order() {
        let parser = ChainParser::from_formats(
            &[LogFormat::Logback, LogFormat::Gin, LogFormat::Nginx],
            None,
        )
        .unwrap();

        assert_eq!(
            parser.parse(LINE).unwrap().logger,
            "c.a.c.service.DelayTaskService"
        );
        assert_eq!(parser.parse(GIN_LINE).unwrap().logger, "gin");
        assert_eq!(parser.parse(NGINX_LINE).unwrap().logger, "nginx");
        // 都不匹配还是 None，走 Aggregator 的续行/兜底
        assert!(parser.parse("\tat com.foo.Bar.baz(Bar.java:42)").is_none());
    }

    #[test]
    fn access_log_no_longer_glues_onto_previous_event() {
        // 配全格式之前：gin 行被当成上一条的堆栈粘上去
        let mut agg = Aggregator::new(Arc::new(RegexParser::new()));
        assert!(agg.push(LINE).is_none());
        assert!(agg.push(GIN_LINE).is_none());
        assert!(
            agg.flush().unwrap().message.contains("[GIN]"),
            "旧行为：粘连"
        );

        // 配全之后：各自成条
        let parser =
            ChainParser::from_formats(&[LogFormat::Logback, LogFormat::Gin], None).unwrap();
        let mut agg = Aggregator::new(Arc::new(parser));
        assert!(agg.push(LINE).is_none());
        let java = agg.push(GIN_LINE).expect("gin 行到来时 Java 那条应当闭合");
        assert_eq!(java.logger, "c.a.c.service.DelayTaskService");
        assert!(!java.message.contains("[GIN]"));

        // gin 自己的 ErrorMessage 是紧跟在后面的一行，仍然要并进这条 access log
        assert!(agg.push("Error #01: broken pipe").is_none());
        let access = agg.flush().expect("应当有一条");
        assert_eq!(access.logger, "gin");
        assert!(access.message.ends_with("Error #01: broken pipe"));
    }

    #[test]
    fn access_log_timestamps_reject_garbage() {
        for bad in [
            "",
            "2026/09/07 10:07:01",   // 少了 ` - `
            "2026-09-07 - 10:07:01", // 日期分隔符不对
            "2026/09/07 - 10:07:0",  // 长度不够
            "2026/13/07 - 10:07:01", // 月份越界
        ] {
            assert!(parse_gin_timestamp(bad).is_none(), "不应解析 {bad:?}");
        }
        for bad in [
            "",
            "07/Sep/2026 18:06:53",       // 少了冒号
            "07/Sept/2026:18:06:53",      // 月份缩写不对
            "07/Sep/2026:18:06:53 0800",  // 缺正负号
            "07/Sep/2026:18:06:53 +08",   // 偏移长度不对
            "32/Sep/2026:18:06:53 +0800", // 日期越界
        ] {
            assert!(parse_clf_timestamp(bad).is_none(), "不应解析 {bad:?}");
        }
    }

    #[test]
    fn unparseable_leading_line_is_kept() {
        let mut agg = Aggregator::new(Arc::new(RegexParser::new()));
        let event = agg.push("no timestamp here").expect("兜底成条");
        assert_eq!(event.message, "no timestamp here");
    }
}
