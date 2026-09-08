//! ClickHouse 走的是 HTTP 接口，这里直接对着一个假服务端看发出去的原始请求。
//!
//! 起因：空 body 的 POST（`SELECT 1` 这种健康检查）如果既没有 Content-Length
//! 也不是 chunked，ClickHouse 会直接回 411 Length Required。

use std::io::Read;
use std::time::Duration;

use flate2::read::GzDecoder;
use logpipe::sink::{ClickhouseSink, Sink};
use logpipe::LogEvent;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// 收一个请求，把原始报文（请求头 + body）交出来，然后回一个 200。
async fn capture(body: &'static str) -> (String, tokio::task::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut raw = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = stream.read(&mut buf).await.unwrap();
            raw.extend_from_slice(&buf[..n]);
            // 请求头收完就够判断了，别等客户端关连接
            if n == 0 || raw.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        String::from_utf8_lossy(&raw).to_string()
    });

    (format!("http://{addr}"), handle)
}

#[tokio::test]
async fn empty_body_still_carries_content_length() {
    let (endpoint, server) = capture("1\n").await;
    let sink = ClickhouseSink::new(endpoint, "logs", "app_log").timeout(Duration::from_secs(5));

    sink.execute("SELECT 1").await.unwrap();

    let raw = server.await.unwrap().to_lowercase();
    assert!(raw.starts_with("post "), "{raw}");
    // 二者缺一，ClickHouse 回 411 Length Required
    assert!(
        raw.contains("content-length:") || raw.contains("transfer-encoding: chunked"),
        "空 body 的 POST 没有 Content-Length，ClickHouse 会回 411:\n{raw}"
    );
}

#[tokio::test]
async fn insert_body_carries_content_length() {
    let (endpoint, server) = capture("").await;
    let mut sink = ClickhouseSink::new(endpoint, "logs", "app_log").timeout(Duration::from_secs(5));

    sink.write(&[LogEvent::new("hello")]).await.unwrap();

    let raw = server.await.unwrap().to_lowercase();
    assert!(
        raw.contains("content-length:") || raw.contains("transfer-encoding: chunked"),
        "{raw}"
    );
}

/// 收一个**完整**请求（按 Content-Length 把 body 读全），交出请求头文本和 body 原始字节。
async fn capture_full(
    response_body: &'static str,
) -> (String, tokio::task::JoinHandle<(String, Vec<u8>)>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut raw = Vec::new();
        let mut buf = [0u8; 8192];

        let head_end = loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break raw.len();
            }
            raw.extend_from_slice(&buf[..n]);
            if let Some(at) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                break at + 4;
            }
        };
        let head = String::from_utf8_lossy(&raw[..head_end]).to_string();

        let len: usize = head
            .to_lowercase()
            .split("content-length:")
            .nth(1)
            .and_then(|rest| rest.split("\r\n").next())
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0);
        while raw.len() - head_end < len {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            raw.extend_from_slice(&buf[..n]);
        }
        let body = raw[head_end..].to_vec();

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{response_body}",
            response_body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        (head, body)
    });

    (format!("http://{addr}"), handle)
}

/// 声明了 gzip 就得真的是 gzip：解出来必须和原始 JSONEachRow 一模一样，
/// 否则 ClickHouse 那边只会看到一堆解不开的字节。
#[tokio::test]
async fn insert_body_is_gzipped_and_round_trips() {
    let (endpoint, server) = capture_full("").await;
    let mut sink = ClickhouseSink::new(endpoint, "logs", "app_log").timeout(Duration::from_secs(5));

    sink.write(&[LogEvent::new("压缩测试")]).await.unwrap();

    let (head, body) = server.await.unwrap();
    assert!(
        head.to_lowercase().contains("content-encoding: gzip"),
        "没有声明 gzip:\n{head}"
    );

    let mut plain = String::new();
    GzDecoder::new(&body[..])
        .read_to_string(&mut plain)
        .expect("body 应当是合法的 gzip");
    assert!(
        plain.contains("\"message\":\"压缩测试\""),
        "解压后对不上: {plain}"
    );
    assert!(
        plain.ends_with('\n'),
        "JSONEachRow 每行都要以换行结尾: {plain:?}"
    );
}

/// 空 body（健康检查）不压：gzip 一个空串反而更大，而这里正是 411 那个坑所在。
#[tokio::test]
async fn healthcheck_body_is_not_gzipped() {
    let (endpoint, server) = capture_full("1\n").await;
    let sink = ClickhouseSink::new(endpoint, "logs", "app_log").timeout(Duration::from_secs(5));

    sink.execute("SELECT 1").await.unwrap();

    let (head, _) = server.await.unwrap();
    let head = head.to_lowercase();
    assert!(
        !head.contains("content-encoding: gzip"),
        "空 body 不该压:\n{head}"
    );
    assert!(
        head.contains("content-length:"),
        "411 的坑不能又踩回去:\n{head}"
    );
}

/// 中间代理不认压缩 body 时的退路。
#[tokio::test]
async fn compression_can_be_turned_off() {
    let (endpoint, server) = capture_full("").await;
    let mut sink = ClickhouseSink::new(endpoint, "logs", "app_log")
        .timeout(Duration::from_secs(5))
        .compress(false);

    sink.write(&[LogEvent::new("明文")]).await.unwrap();

    let (head, body) = server.await.unwrap();
    assert!(
        !head.to_lowercase().contains("content-encoding: gzip"),
        "关掉了还在压:\n{head}"
    );
    assert!(String::from_utf8_lossy(&body).contains("\"message\":\"明文\""));
}

/// 配了 timezone，INSERT 里的时间戳要带偏移：存进去的绝对时刻不再依赖列有没有标时区。
#[tokio::test]
async fn timezone_puts_offset_on_timestamps() {
    let (endpoint, server) = capture_full("").await;
    let mut sink = ClickhouseSink::new(endpoint, "logs", "app_log")
        .timeout(Duration::from_secs(5))
        .compress(false)
        .timezone(chrono_tz::Asia::Shanghai);

    let mut event = LogEvent::new("带偏移");
    event.timestamp = chrono::NaiveDateTime::parse_from_str(
        "2026-09-07 11:04:08.914",
        logpipe::event::TIMESTAMP_FORMAT,
    )
    .unwrap();
    sink.write(&[event]).await.unwrap();

    let (_, body) = server.await.unwrap();
    let body = String::from_utf8_lossy(&body);
    assert!(
        body.contains("\"timestamp\":\"2026-09-07 11:04:08.914+08:00\""),
        "墙上时间原样、后面接 +08:00: {body}"
    );
}

/// 按顺序应答多个请求（同一条 keep-alive 连接或多条连接都行），交出每个请求的 query 参数。
async fn serve_sequence(
    responses: Vec<&'static str>,
) -> (String, tokio::task::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        let mut queries = Vec::new();
        let mut pending = responses.into_iter();
        'conn: while pending.len() > 0 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut raw: Vec<u8> = Vec::new();
            let mut buf = [0u8; 8192];
            for response_body in pending.by_ref() {
                // 读到一个完整请求（头 + Content-Length 指定的 body）
                let head_end = loop {
                    if let Some(at) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                        break at + 4;
                    }
                    let n = stream.read(&mut buf).await.unwrap();
                    if n == 0 {
                        // 客户端换了连接，把没用掉的应答留给下一条
                        continue 'conn;
                    }
                    raw.extend_from_slice(&buf[..n]);
                };
                let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
                let len: usize = head
                    .to_lowercase()
                    .split("content-length:")
                    .nth(1)
                    .and_then(|rest| rest.split("\r\n").next())
                    .and_then(|value| value.trim().parse().ok())
                    .unwrap_or(0);
                while raw.len() - head_end < len {
                    let n = stream.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    raw.extend_from_slice(&buf[..n]);
                }
                raw.drain(..head_end + len);

                // 请求行里 `?query=...` 那一段，URL 解码后就是 SQL
                let query = head
                    .split_whitespace()
                    .nth(1)
                    .and_then(|path| path.split("query=").nth(1))
                    .and_then(|rest| rest.split('&').next())
                    .map(percent_decode)
                    .unwrap_or_default();
                queries.push(query);

                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{response_body}",
                    response_body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.flush().await.unwrap();
            }
        }
        queries
    });

    (format!("http://{addr}"), handle)
}

fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(b) => {
                        out.push(b);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

/// 表存在但列没跟上配置：healthcheck 必须把缺的列点出来，而不是让字段被静默丢掉。
#[tokio::test]
async fn healthcheck_reports_missing_columns() {
    // SELECT 1 → 1；EXISTS TABLE → 1；system.columns → 少了 env 和 pod
    let (endpoint, server) = serve_sequence(vec![
        "1\n",
        "1\n",
        "timestamp\nlevel\ntrace_id\nspan_id\nthread\nlogger\nmessage\nfile\nhost\ncluster\n",
    ])
    .await;
    let sink = ClickhouseSink::new(endpoint, "logs", "app_log")
        .timeout(Duration::from_secs(5))
        .extra_columns(vec![
            ("cluster".to_owned(), "LowCardinality(String)".to_owned()),
            ("env".to_owned(), "LowCardinality(String)".to_owned()),
            ("pod".to_owned(), "String".to_owned()),
        ]);

    let err = sink.healthcheck().await.expect_err("缺列应当报错");
    let msg = err.to_string();
    assert!(msg.contains("缺列 env, pod"), "{msg}");
    assert!(msg.contains("--ddl"), "要告诉人怎么补: {msg}");

    let queries = server.await.unwrap();
    assert_eq!(queries.len(), 3, "{queries:?}");
    assert!(
        queries[2].contains("system.columns")
            && queries[2].contains("database = 'logs'")
            && queries[2].contains("table = 'app_log'"),
        "{}",
        queries[2]
    );
}

/// 列齐了就放行。
#[tokio::test]
async fn healthcheck_passes_when_columns_present() {
    let (endpoint, server) = serve_sequence(vec![
        "1\n",
        "1\n",
        "timestamp\nlevel\ntrace_id\nspan_id\nthread\nlogger\nmessage\nfile\nhost\nenv\nextra_col\n",
    ])
    .await;
    let sink = ClickhouseSink::new(endpoint, "logs", "app_log")
        .timeout(Duration::from_secs(5))
        .extra_columns(vec![(
            "env".to_owned(),
            "LowCardinality(String)".to_owned(),
        )]);

    sink.healthcheck().await.expect("列齐了不该报错");
    assert_eq!(server.await.unwrap().len(), 3);
}
