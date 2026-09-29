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
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("flare-supervise-{}-{nonce}", std::process::id()));
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
    assert!(!socket.exists(), "health socket left behind");
    fs::remove_file(manifest).unwrap();
    fs::remove_dir(dir).unwrap();
}
