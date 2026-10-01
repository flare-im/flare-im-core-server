#![cfg(unix)]

use std::{
    fs,
    path::PathBuf,
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{process::Command, time::timeout};

fn isolated_dir() -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    // 同一进程里并行的测试可能拿到同一个时间戳，再加一个序号。目录名要短：macOS 的
    // 临时目录本身就长，健康检查 socket 的路径不能超过 104 字节。
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_micros();
    let sequence = NEXT.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "flare-supervise-{}-{sequence}-{nonce}",
        std::process::id()
    ));
    fs::create_dir(&dir).unwrap();
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

fn signal_term(pid: u32) {
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }
    assert_eq!(unsafe { kill(i32::try_from(pid).unwrap(), 15) }, 0);
}

#[tokio::test]
async fn prefixes_child_logs_and_gracefully_stops_on_sigterm() {
    let dir = isolated_dir();
    let manifest = dir.join("supervise.toml");
    let socket = dir.join("health.sock");
    fs::write(
        &manifest,
        r#"
version = 1
shared_env = ["PATH"]
[restart]
min_delay_ms = 10
max_delay_ms = 100
reset_after_secs = 1
stop_grace_secs = 2
[[services]]
name = "demo"
command = "/bin/sh"
args = ["-c", "echo child-ready; exec sleep 30"]
"#,
    )
    .unwrap();

    let exe = env!("CARGO_BIN_EXE_flare-supervise");
    let child = Command::new(exe)
        .args([
            "supervise",
            "--services=demo",
            "--manifest",
            manifest.to_str().unwrap(),
            "--socket",
            socket.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let pid = child.id().unwrap();

    timeout(Duration::from_secs(3), async {
        loop {
            if Command::new(exe)
                .args(["health", "--socket", socket.to_str().unwrap()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await
                .is_ok_and(|status| status.success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("child never became healthy");

    signal_term(pid);
    let output = timeout(Duration::from_secs(4), child.wait_with_output())
        .await
        .expect("supervisor did not stop after SIGTERM")
        .unwrap();
    assert!(
        output.status.success(),
        "supervisor exit: {:?}",
        output.status
    );
    let log = String::from_utf8_lossy(&output.stderr);
    assert!(
        log.contains("[demo][stdout] child-ready"),
        "missing prefixed child log: {log}"
    );
    assert!(
        log.contains("[supervisor] stopping children (SIGTERM)"),
        "missing stop log: {log}"
    );
    // 每个子进程的退出状态与停机用时都要留在日志里。
    let stopped = stop_line(&log, "[demo] stopped after ");
    assert!(
        stopped.ends_with("signal: 15 (SIGTERM)"),
        "unexpected exit status: {stopped}"
    );
    assert!(
        log.contains("[supervisor] all children stopped in "),
        "missing total stop time: {log}"
    );
    assert!(!socket.exists(), "health socket left behind");
    fs::remove_file(manifest).unwrap();
    fs::remove_dir(dir).unwrap();
}

/// `prefix` 开头的那一行，且其后紧跟一个毫秒数与 ` ms`。
fn stop_line<'a>(log: &'a str, prefix: &str) -> &'a str {
    let line = log
        .lines()
        .find(|line| line.starts_with(prefix))
        .unwrap_or_else(|| panic!("missing `{prefix}` line: {log}"));
    let rest = &line[prefix.len()..];
    let digits = rest.chars().take_while(char::is_ascii_digit).count();
    assert!(
        digits > 0 && rest[digits..].starts_with(" ms"),
        "no stop duration in: {line}"
    );
    line
}

async fn wait_healthy(exe: &str, socket: &std::path::Path) {
    timeout(Duration::from_secs(3), async {
        loop {
            if Command::new(exe)
                .args(["health", "--socket", socket.to_str().unwrap()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await
                .is_ok_and(|status| status.success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("child never became healthy");
}

#[tokio::test]
async fn records_the_kill_of_a_child_that_ignores_sigterm() {
    let dir = isolated_dir();
    let manifest = dir.join("supervise.toml");
    let socket = dir.join("health.sock");
    fs::write(
        &manifest,
        r#"
version = 1
shared_env = ["PATH"]
[restart]
min_delay_ms = 10
max_delay_ms = 100
reset_after_secs = 1
stop_grace_secs = 1
[[services]]
name = "stubborn"
command = "/bin/sh"
args = ["-c", "trap '' TERM; echo child-ready; while :; do sleep 0.1; done"]
"#,
    )
    .unwrap();

    let exe = env!("CARGO_BIN_EXE_flare-supervise");
    let child = Command::new(exe)
        .args([
            "supervise",
            "--services=stubborn",
            "--manifest",
            manifest.to_str().unwrap(),
            "--socket",
            socket.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let pid = child.id().unwrap();
    wait_healthy(exe, &socket).await;
    // 子进程先跑起来，trap 才生效。
    tokio::time::sleep(Duration::from_millis(200)).await;

    signal_term(pid);
    let output = timeout(Duration::from_secs(5), child.wait_with_output())
        .await
        .expect("supervisor did not stop after SIGTERM")
        .unwrap();
    assert!(
        output.status.success(),
        "supervisor exit: {:?}",
        output.status
    );
    let log = String::from_utf8_lossy(&output.stderr);
    let killed = stop_line(&log, "[stubborn] killed after ");
    assert!(
        killed.contains("(grace 1 s exceeded)") && killed.ends_with("signal: 9 (SIGKILL)"),
        "unexpected kill line: {killed}"
    );
    fs::remove_file(manifest).unwrap();
    fs::remove_dir(dir).unwrap();
}
