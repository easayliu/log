//! 退出信号：k8s 终止 Pod 发的是 SIGTERM，不是 SIGINT。只认 Ctrl-C 的话，
//! 容器里那套收尾逻辑（冲刷剩余数据 + 落位点）一次都跑不到。
//!
//! 这里直接把真正的二进制拉起来发信号，因为要测的恰恰是进程级的信号处理。
#![cfg(unix)]

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn wait_until(mut ready: impl FnMut() -> bool, label: &str) {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(15) {
        if ready() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("等待超时: {label}");
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

/// 发 `signal`，返回（退出是否正常，进程输出，位点文件是否存在）。
fn run_and_signal(signal: &str) -> (bool, String, bool) {
    let dir = tempfile::tempdir().unwrap();
    let logs = dir.path().join("logs");
    std::fs::create_dir_all(&logs).unwrap();
    std::fs::write(
        logs.join("app.log"),
        "2026-09-07 11:04:08.914 [main] INFO  c.a.Foo -启动完成\n",
    )
    .unwrap();

    let data_dir = dir.path().join("ck");
    let config = dir.path().join("logpipe.yaml");
    std::fs::write(
        &config,
        format!(
            "source:\n  \
               type: file\n  \
               include: [\"{}/*.log\"]\n  \
               data_dir: {}\n  \
               read_from_beginning: true\n\
             sink:\n  \
               type: console\n  \
               encoding: json\n\
             batch:\n  \
               timeout_secs: 1\n",
            logs.display(),
            data_dir.display()
        ),
    )
    .unwrap();

    let out = dir.path().join("out.log");
    let mut child = Command::new(env!("CARGO_BIN_EXE_logpipe"))
        .arg(&config)
        .stdout(Stdio::from(std::fs::File::create(&out).unwrap()))
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    // 等日志确实被采上来，再发信号 —— 否则测的就不是收尾了
    wait_until(|| read(&out).contains("启动完成"), "采集到第一条日志");

    Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(child.id().to_string())
        .status()
        .unwrap();

    let status = child.wait().unwrap();
    (
        status.success(),
        read(&out),
        data_dir.join("checkpoints.json").exists(),
    )
}

#[test]
fn sigterm_triggers_graceful_shutdown() {
    let (ok, output, checkpoint) = run_and_signal("TERM");

    assert!(
        ok,
        "SIGTERM 之后应当正常退出（被信号打死会是 143）:\n{output}"
    );
    assert!(
        output.contains("收到退出信号"),
        "SIGTERM 没有走到收尾分支:\n{output}"
    );
    assert!(output.contains("已退出"), "收尾没跑完:\n{output}");
    assert!(checkpoint, "收尾了却没落位点，重启会重读这一段");
}

/// Ctrl-C 原来就是通的，别在加 SIGTERM 的时候把它弄丢。
#[test]
fn sigint_still_triggers_graceful_shutdown() {
    let (ok, output, checkpoint) = run_and_signal("INT");

    assert!(ok, "SIGINT 之后应当正常退出:\n{output}");
    assert!(output.contains("收到退出信号"), "{output}");
    assert!(checkpoint, "收尾了却没落位点");
}
