use std::{
    collections::BTreeMap, env, os::unix::process::CommandExt, path::Path, sync::Arc,
    time::Duration,
};

use crate::error::{Context, Result, ensure, fail};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, UnixListener, UnixStream},
    process::{Child, Command},
    sync::{RwLock, watch},
    task::{JoinHandle, JoinSet},
    time::{Instant, sleep, timeout},
};

use crate::manifest::{Manifest, Restart, Service};

const MAX_LOG_LINE: usize = 16 * 1024;
const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_millis(150);

#[derive(Clone, Debug)]
struct HealthEntry {
    running: bool,
    tcp_health: Option<String>,
}

type HealthState = Arc<RwLock<BTreeMap<String, HealthEntry>>>;

pub(crate) async fn supervise(
    manifest: Manifest,
    services: Vec<Service>,
    socket: &Path,
) -> Result<()> {
    let state: HealthState = Arc::new(RwLock::new(
        services
            .iter()
            .map(|service| {
                (
                    service.name.clone(),
                    HealthEntry {
                        running: false,
                        tcp_health: service.tcp_health.clone(),
                    },
                )
            })
            .collect(),
    ));
    let listener = bind_health_socket(socket).await?;
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut health_task = tokio::spawn(serve_health(listener, Arc::clone(&state), stop_rx.clone()));

    let mut workers = JoinSet::new();
    for service in services {
        let state = Arc::clone(&state);
        let manifest = manifest.clone();
        let stop = stop_rx.clone();
        workers.spawn(async move { run_service(manifest, service, state, stop).await });
    }

    #[cfg(unix)]
    let (reason, health_finished) = {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            _ = term.recv() => ("SIGTERM".to_owned(), false),
            _ = tokio::signal::ctrl_c() => ("SIGINT".to_owned(), false),
            result = workers.join_next() => {
                (format!("worker task unexpectedly ended: {result:?}"), false)
            },
            result = &mut health_task => (format!("health server unexpectedly ended: {result:?}"), true),
        }
    };
    eprintln!("[supervisor] stopping children ({reason})");
    let stopping = Instant::now();
    let _ = stop_tx.send(true);
    while let Some(result) = workers.join_next().await {
        if let Err(error) = result.context("join service supervisor task")? {
            eprintln!("[supervisor] service supervisor error: {error:#}");
        }
    }
    eprintln!(
        "[supervisor] all children stopped in {} ms",
        stopping.elapsed().as_millis()
    );
    if !health_finished {
        health_task.abort();
        let _ = health_task.await;
    }
    if socket.exists() {
        std::fs::remove_file(socket)
            .with_context(|| format!("remove health socket {}", socket.display()))?;
    }
    if reason.contains("unexpectedly ended") {
        fail!("{reason}");
    }
    Ok(())
}

async fn run_service(
    manifest: Manifest,
    service: Service,
    state: HealthState,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let mut failures = 0u32;
    while !*stop.borrow() {
        let mut command = Command::new(&service.command);
        command
            .args(&service.args)
            .env_clear()
            .kill_on_drop(true)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        // Each child is a process-group leader so graceful shutdown also reaches
        // any subprocess it launched. No shell is involved in the production path.
        command.as_std_mut().process_group(0);
        command.envs(manifest.child_environment(&service, env::vars_os()));
        let started = Instant::now();
        match command.spawn() {
            Ok(mut child) => {
                eprintln!(
                    "[{}] started pid={}",
                    service.name,
                    child.id().unwrap_or_default()
                );
                set_running(&state, &service.name, true).await;
                let stdout = child
                    .stdout
                    .take()
                    .map(|pipe| tokio::spawn(copy_prefixed(pipe, service.name.clone(), "stdout")));
                let stderr = child
                    .stderr
                    .take()
                    .map(|pipe| tokio::spawn(copy_prefixed(pipe, service.name.clone(), "stderr")));
                tokio::select! {
                    status = child.wait() => {
                        set_running(&state, &service.name, false).await;
                        eprintln!("[{}] exited: {:?}", service.name, status);
                    }
                    _ = stop.changed() => {
                        set_running(&state, &service.name, false).await;
                        terminate_child(&service.name, &mut child, Duration::from_secs(manifest.restart.stop_grace_secs)).await;
                    }
                }
                drain_log(stdout).await;
                drain_log(stderr).await;
            }
            Err(error) => {
                set_running(&state, &service.name, false).await;
                eprintln!("[{}] spawn failed: {error}", service.name);
            }
        }
        if *stop.borrow() {
            break;
        }
        failures = if started.elapsed() >= Duration::from_secs(manifest.restart.reset_after_secs) {
            0
        } else {
            failures.saturating_add(1)
        };
        let delay = restart_delay(&manifest.restart, failures);
        eprintln!("[{}] restarting in {}ms", service.name, delay.as_millis());
        tokio::select! {
            _ = sleep(delay) => {},
            _ = stop.changed() => break,
        }
    }
    set_running(&state, &service.name, false).await;
    Ok(())
}

fn restart_delay(config: &Restart, failures: u32) -> Duration {
    let shift = failures.saturating_sub(1).min(31);
    let millis = config
        .min_delay_ms
        .saturating_mul(1_u64 << shift)
        .min(config.max_delay_ms);
    Duration::from_millis(millis)
}

async fn set_running(state: &HealthState, name: &str, running: bool) {
    if let Some(entry) = state.write().await.get_mut(name) {
        entry.running = running;
    }
}

/// 停一个子进程：SIGTERM 整个进程组，宽限期内没退出再 SIGKILL。每个子进程记一行退出状态
/// 与从 SIGTERM 到退出的毫秒数（W6-S2 F7：此前停机路径丢弃 `wait` 的结果，只能在容器外轮询
/// `/proc` 补证）。
async fn terminate_child(name: &str, child: &mut Child, grace: Duration) {
    let started = Instant::now();
    if let Some(pid) = child.id() {
        eprintln!("[supervisor] SIGTERM process group {pid}");
        send_group_signal(pid, 15);
    }
    match timeout(grace, child.wait()).await {
        Ok(status) => eprintln!(
            "[{name}] stopped after {} ms: {}",
            started.elapsed().as_millis(),
            describe_exit(status)
        ),
        Err(_) => {
            if let Some(pid) = child.id() {
                eprintln!("[supervisor] SIGKILL process group {pid} after grace timeout");
                send_group_signal(pid, 9);
            }
            let _ = child.start_kill();
            let status = child.wait().await;
            eprintln!(
                "[{name}] killed after {} ms (grace {} s exceeded): {}",
                started.elapsed().as_millis(),
                grace.as_secs(),
                describe_exit(status)
            );
        }
    }
}

fn describe_exit(status: std::io::Result<std::process::ExitStatus>) -> String {
    match status {
        Ok(status) => status.to_string(),
        Err(error) => format!("wait failed: {error}"),
    }
}

fn send_group_signal(pid: u32, signal: i32) {
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }
    if let Ok(pid) = i32::try_from(pid) {
        // Negative PID addresses only this child-created process group.
        let result = unsafe { kill(-pid, signal) };
        if result != 0 {
            eprintln!(
                "[supervisor] signal group {pid} failed: {}",
                std::io::Error::last_os_error()
            );
        }
    }
}

async fn drain_log(task: Option<JoinHandle<Result<()>>>) {
    if let Some(mut task) = task
        && timeout(Duration::from_secs(1), &mut task).await.is_err()
    {
        task.abort();
        let _ = task.await;
    }
}

async fn copy_prefixed(
    mut input: impl AsyncRead + Unpin,
    name: String,
    stream: &'static str,
) -> Result<()> {
    let mut buffer = [0u8; 4096];
    let mut line = Vec::with_capacity(4096);
    loop {
        let count = input.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        for &byte in &buffer[..count] {
            if byte == b'\n' {
                emit_line(&name, stream, &line);
                line.clear();
            } else {
                line.push(byte);
                if line.len() >= MAX_LOG_LINE {
                    emit_line(&name, stream, &line);
                    line.clear();
                }
            }
        }
    }
    if !line.is_empty() {
        emit_line(&name, stream, &line);
    }
    Ok(())
}

fn emit_line(name: &str, stream: &str, line: &[u8]) {
    eprintln!("[{name}][{stream}] {}", String::from_utf8_lossy(line));
}

async fn bind_health_socket(path: &Path) -> Result<UnixListener> {
    ensure!(path.is_absolute(), "health socket path must be absolute");
    let parent = path.parent().context("health socket has no parent")?;
    use std::os::unix::fs::{FileTypeExt, PermissionsExt};
    if let Ok(meta) = std::fs::symlink_metadata(parent) {
        ensure!(
            meta.is_dir() && !meta.file_type().is_symlink(),
            "unsafe health socket directory"
        );
        ensure!(
            meta.permissions().mode() & 0o077 == 0,
            "health socket directory must not be accessible to group/others"
        );
    } else {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        ensure!(
            !std::fs::symlink_metadata(parent)?.file_type().is_symlink(),
            "unsafe health socket directory"
        );
        // Only chmod the directory we just created. Never chmod /tmp or an
        // arbitrary existing directory supplied via --socket.
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
    }
    if let Ok(meta) = std::fs::symlink_metadata(path) {
        ensure!(
            meta.file_type().is_socket(),
            "health socket path is not a socket"
        );
        ensure!(
            UnixStream::connect(path).await.is_err(),
            "another supervisor owns health socket"
        );
        std::fs::remove_file(path)
            .with_context(|| format!("remove stale socket {}", path.display()))?;
    }
    let listener = UnixListener::bind(path)
        .with_context(|| format!("bind health socket {}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

async fn serve_health(
    listener: UnixListener,
    state: HealthState,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            accepted = listener.accept() => {
                let (mut stream, _) = accepted?;
                let entries = state.read().await.clone();
                let healthy = !*stop.borrow() && !entries.is_empty() && entries.values().all(|entry| entry.running);
                let mut healthy = healthy;
                if healthy {
                    for entry in entries.values() {
                        if let Some(address) = &entry.tcp_health
                            && !matches!(timeout(HEALTH_CHECK_TIMEOUT, TcpStream::connect(address)).await, Ok(Ok(_)))
                        {
                            healthy = false;
                            break;
                        }
                    }
                }
                let response = if healthy { b"ok\n".as_slice() } else { b"unhealthy\n".as_slice() };
                let _ = stream.write_all(response).await;
            }
        }
    }
    Ok(())
}

pub(crate) async fn health(path: &Path) -> Result<()> {
    timeout(Duration::from_secs(2), async {
        let mut stream = UnixStream::connect(path).await?;
        let mut buffer = [0u8; 3];
        stream.read_exact(&mut buffer).await?;
        ensure!(&buffer == b"ok\n", "supervisor is unhealthy");
        Result::<()>::Ok(())
    })
    .await
    .context("supervisor health check timed out")?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_exponential_backoff() {
        let config = Restart {
            min_delay_ms: 10,
            max_delay_ms: 35,
            reset_after_secs: 1,
            stop_grace_secs: 1,
        };
        assert_eq!(restart_delay(&config, 1), Duration::from_millis(10));
        assert_eq!(restart_delay(&config, 2), Duration::from_millis(20));
        assert_eq!(restart_delay(&config, 3), Duration::from_millis(35));
        assert_eq!(restart_delay(&config, 100), Duration::from_millis(35));
    }

    #[tokio::test]
    async fn health_rejects_dead_child_and_bad_port() {
        let mut entries = BTreeMap::new();
        entries.insert(
            "api".into(),
            HealthEntry {
                running: false,
                tcp_health: None,
            },
        );
        let state = Arc::new(RwLock::new(entries));
        let dir =
            std::env::temp_dir().join(format!("flare-supervise-test-{}-{}", std::process::id(), 1));
        let socket = dir.join("health.sock");
        let listener = bind_health_socket(&socket).await.unwrap();
        let (tx, rx) = watch::channel(false);
        let task = tokio::spawn(serve_health(listener, Arc::clone(&state), rx));
        assert!(health(&socket).await.is_err());
        set_running(&state, "api", true).await;
        health(&socket).await.unwrap();
        state.write().await.get_mut("api").unwrap().tcp_health = Some("127.0.0.1:1".into());
        assert!(health(&socket).await.is_err());
        tx.send(true).unwrap();
        task.await.unwrap().unwrap();
        std::fs::remove_file(&socket).unwrap();
        std::fs::remove_dir(&dir).unwrap();
    }
}
