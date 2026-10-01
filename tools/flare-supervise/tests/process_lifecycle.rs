#![cfg(unix)]

use std::{
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
    sync::watch,
    task::JoinHandle,
    time::timeout,
};

/// 等待外部进程（监管器启动、子进程就绪、停机结束）的上限，只用来防挂死，不是被测语义：
/// 子进程是否在宽限内被 SIGTERM 停下、是否被 SIGKILL，看监管器日志里的退出状态与
/// 停机用时。并行构建满载时起一个进程就可能要好几秒（原先 3 s 的就绪上限失败过），
/// 默认 60 s，可用 `FLARE_SUPERVISE_TEST_WAIT_SECS` 调整。
fn wait_budget() -> Duration {
    let seconds = std::env::var("FLARE_SUPERVISE_TEST_WAIT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .unwrap_or(60);
    Duration::from_secs(seconds)
}

/// 测试用的私有目录。成功路径自己逐个删除并以此确认没有残留；测试中途失败时由这里
/// 整个删掉，满载下失败的那几次也不会在临时目录里留下清单与 socket。
struct TestDir(PathBuf);

impl std::ops::Deref for TestDir {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn isolated_dir() -> TestDir {
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
    TestDir(dir)
}

/// SIGTERM 给 `pid`，返回是否送达。
fn send_term(pid: u32) -> bool {
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }
    i32::try_from(pid).is_ok_and(|pid| unsafe { kill(pid, 15) } == 0)
}

/// 测试启动的监管器。stderr 边产生边读进 `log`，就绪与否按日志里的事件判断，
/// 不靠固定的睡眠。
struct Supervisor {
    child: Child,
    pid: u32,
    log: watch::Receiver<String>,
    reader: JoinHandle<()>,
    socket: PathBuf,
    stopped: bool,
}

impl Supervisor {
    fn start(manifest: &Path, socket: &Path, services: &str) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_flare-supervise"))
            .args([
                "supervise",
                &format!("--services={services}"),
                "--manifest",
                manifest.to_str().unwrap(),
                "--socket",
                socket.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap();
        let (sender, log) = watch::channel(String::new());
        let mut lines = BufReader::new(child.stderr.take().unwrap()).lines();
        let reader = tokio::spawn(async move {
            while let Ok(Some(line)) = lines.next_line().await {
                sender.send_modify(|log| {
                    log.push_str(&line);
                    log.push('\n');
                });
            }
        });
        Self {
            child,
            pid,
            log,
            reader,
            socket: socket.to_path_buf(),
            stopped: false,
        }
    }

    /// 等到日志里出现 `needle`。
    async fn wait_for_log(&mut self, needle: &str) {
        let budget = wait_budget();
        let seen = matches!(
            timeout(budget, self.log.wait_for(|log| log.contains(needle))).await,
            Ok(Ok(_))
        );
        assert!(
            seen,
            "no `{needle}` within {budget:?}: {}",
            self.log.borrow().as_str()
        );
    }

    /// 等到 `flare-supervise health` 报告健康。
    async fn wait_healthy(&self) {
        let budget = wait_budget();
        let healthy = timeout(budget, async {
            loop {
                if Command::new(env!("CARGO_BIN_EXE_flare-supervise"))
                    .args(["health", "--socket", self.socket.to_str().unwrap()])
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
        .await;
        assert!(
            healthy.is_ok(),
            "child never became healthy within {budget:?}: {}",
            self.log.borrow().as_str()
        );
    }

    /// SIGTERM，等监管器退出（子进程宽限 `grace` 之外再给 `wait_budget`），返回其日志。
    async fn stop(mut self, grace: Duration) -> String {
        assert!(send_term(self.pid), "cannot signal the supervisor");
        self.stopped = true;
        let budget = grace + wait_budget();
        let status = timeout(budget, self.child.wait())
            .await
            .unwrap_or_else(|_| panic!("supervisor did not stop within {budget:?} after SIGTERM"))
            .unwrap();
        // 监管器退出后 stderr 关闭，读取任务随之结束。
        timeout(wait_budget(), &mut self.reader)
            .await
            .expect("supervisor stderr stayed open")
            .unwrap();
        let log = self.log.borrow().clone();
        assert!(status.success(), "supervisor exit: {status:?}\n{log}");
        log
    }
}

impl Drop for Supervisor {
    /// 测试中途失败时也让监管器按停机流程收掉子进程（含对忽略 SIGTERM 的那个补
    /// SIGKILL）。这里不能 await，同步等它退出；仍不退出的，随 `kill_on_drop` 被 SIGKILL。
    fn drop(&mut self) {
        if self.stopped || !send_term(self.pid) {
            return;
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while std::time::Instant::now() < deadline && matches!(self.child.try_wait(), Ok(None)) {
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

#[tokio::test]
async fn prefixes_child_logs_and_gracefully_stops_on_sigterm() {
    let dir = isolated_dir();
    let manifest = dir.join("supervise.toml");
    let socket = dir.join("health.sock");
    // 宽限给足，满载时 SIGCHLD 晚到也不会被当成超时；子进程实际在 SIGTERM 后立即退出。
    // 子进程自己不能先结束：满载时从启动到 SIGTERM 可能超过半分钟（`sleep 30` 曾在
    // SIGTERM 之前自然退出，日志成了 `exit status: 0`），所以睡足一小时。
    fs::write(
        &manifest,
        r#"
version = 1
shared_env = ["PATH"]
[restart]
min_delay_ms = 10
max_delay_ms = 100
reset_after_secs = 1
stop_grace_secs = 10
[[services]]
name = "demo"
command = "/bin/sh"
args = ["-c", "echo child-ready; exec sleep 3600"]
"#,
    )
    .unwrap();

    let mut supervisor = Supervisor::start(&manifest, &socket, "demo");
    // 子进程打出这一行之后才发 SIGTERM，否则 sh 可能还没执行到 echo 就被停掉。
    supervisor.wait_for_log("[demo][stdout] child-ready").await;
    supervisor.wait_healthy().await;
    let log = supervisor.stop(Duration::from_secs(10)).await;

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
    fs::remove_dir(&*dir).unwrap();
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

    let mut supervisor = Supervisor::start(&manifest, &socket, "stubborn");
    // 这一行在 trap 之后打印：看到它，子进程就已经忽略 SIGTERM 了。
    supervisor
        .wait_for_log("[stubborn][stdout] child-ready")
        .await;
    supervisor.wait_healthy().await;
    let log = supervisor.stop(Duration::from_secs(1)).await;

    let killed = stop_line(&log, "[stubborn] killed after ");
    assert!(
        killed.contains("(grace 1 s exceeded)") && killed.ends_with("signal: 9 (SIGKILL)"),
        "unexpected kill line: {killed}"
    );
    fs::remove_file(manifest).unwrap();
    fs::remove_dir(&*dir).unwrap();
}
