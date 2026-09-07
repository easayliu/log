//! ClickHouse 走的是 HTTP 接口，这里直接对着一个假服务端看发出去的原始请求。
//!
//! 起因：空 body 的 POST（`SELECT 1` 这种健康检查）如果既没有 Content-Length
//! 也不是 chunked，ClickHouse 会直接回 411 Length Required。

use std::time::Duration;

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
