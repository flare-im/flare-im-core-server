//! 进程内共享的 Redis 连接：懒建立、有界建连、有界应答、有界重建，以及 Redis 错误的分类。
//!
//! Storage Writer 与 Reader 的缓存仓储以前每次操作都 `ConnectionManager::new` 新建连接，用的是
//! redis 0.32 的默认值：不设建连与应答超时，建连失败重试 6 次、两次之间的退避每次至多 60 s。
//! Redis 不可达时一次调用可阻塞数分钟（对关闭端口实测 60 s 仍未返回），而 Writer 在持久写之后、
//! 同一个消费处理之内写热缓存，Redis 故障就卡住 Writer 的消费者。
//!
//! 现在每个进程共享一个连接（[`ConnectionManager`] 本身多路复用、可克隆）：
//! - 第一次使用时建立；建立时每次连接尝试受 [`RedisConnectionLimits::connect_timeout`] 约束，
//!   失败后至多再试 [`RedisConnectionLimits::connect_retries`] 次；
//! - 建立失败后，在一个建连超时的时长内直接回暂时错误、不再发起连接，过后的下一次调用再建
//!   （并发调用者等的是同一次建立，不会各建一次，也不会排队各等一轮超时）；
//! - 建立之后每条命令受 [`RedisConnectionLimits::response_timeout`] 约束；连接断开由
//!   `ConnectionManager` 在后台重连，重连同样受建连超时与重试次数约束。
//!
//! 默认值见 [`RedisConnectionLimits::default`]：Redis 不可达时一次调用在几秒内失败。

use std::time::{Duration, Instant};

use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::{ErrorKind, RedisError, RedisResult};

/// 每次建连尝试（TCP 加握手）的默认上限。
pub const DEFAULT_REDIS_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// 每条命令（含流水线）等待应答的默认上限。
pub const DEFAULT_REDIS_RESPONSE_TIMEOUT: Duration = Duration::from_secs(3);
/// 建连失败后的默认重试次数（不含第一次）。
pub const DEFAULT_REDIS_CONNECT_RETRIES: usize = 1;

/// 超时取值的上限：再长就不算有界了。
const MAX_TIMEOUT_MS: u64 = 60_000;
/// 重试次数的上限：两次尝试之间至少等 1 s（redis 的退避起点，不可配），次数多了同样不再有界。
const MAX_CONNECT_RETRIES: usize = 5;
/// 重试退避的上限（毫秒）。redis 的第一次退避固定从 1 s 起（加至多一倍的抖动），之后按 2 倍增长，
/// 封顶在这里。
const MAX_RETRY_DELAY_MS: u64 = 1_000;

/// 建连、应答与重试的上限。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedisConnectionLimits {
    /// 每次建连尝试（TCP 加握手）的上限。也是建立失败后不再发起连接的时长。
    pub connect_timeout: Duration,
    /// 每条命令（含流水线）等待应答的上限。
    pub response_timeout: Duration,
    /// 建连失败后的重试次数（不含第一次）。
    pub connect_retries: usize,
}

impl Default for RedisConnectionLimits {
    fn default() -> Self {
        Self {
            connect_timeout: DEFAULT_REDIS_CONNECT_TIMEOUT,
            response_timeout: DEFAULT_REDIS_RESPONSE_TIMEOUT,
            connect_retries: DEFAULT_REDIS_CONNECT_RETRIES,
        }
    }
}

fn config_error(message: String) -> flare_server_core::error::FlareError {
    ErrorBuilder::new(ErrorCode::ConfigurationError, &message).build_error()
}

fn parse_timeout_ms(name: &str, value: Option<&str>, default: Duration) -> Result<Duration> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(default),
        Some(value) => match value.parse::<u64>() {
            Ok(ms) if (1..=MAX_TIMEOUT_MS).contains(&ms) => Ok(Duration::from_millis(ms)),
            _ => Err(config_error(format!(
                "{name} must be a whole number of milliseconds between 1 and {MAX_TIMEOUT_MS}"
            ))),
        },
    }
}

impl RedisConnectionLimits {
    /// 由三个环境变量的取值得出（变量名由调用方给出，按调用方自己的前缀读取并登记在
    /// `env_registry`）：未设或为空时取默认；超时不是 1..=60000 的整数毫秒、重试次数不是 0..=5
    /// 的整数时报配置错误，启动即失败，不静默回落到默认值。错误里只有变量名，不带取值。
    pub fn from_env_values(
        connect_timeout_ms: (&str, Option<&str>),
        response_timeout_ms: (&str, Option<&str>),
        connect_retries: (&str, Option<&str>),
    ) -> Result<Self> {
        let defaults = Self::default();
        let (retries_name, retries_value) = connect_retries;
        let connect_retries = match retries_value
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            None => defaults.connect_retries,
            Some(value) => match value.parse::<usize>() {
                Ok(retries) if retries <= MAX_CONNECT_RETRIES => retries,
                _ => {
                    return Err(config_error(format!(
                        "{retries_name} must be a whole number between 0 and {MAX_CONNECT_RETRIES}"
                    )));
                }
            },
        };
        Ok(Self {
            connect_timeout: parse_timeout_ms(
                connect_timeout_ms.0,
                connect_timeout_ms.1,
                defaults.connect_timeout,
            )?,
            response_timeout: parse_timeout_ms(
                response_timeout_ms.0,
                response_timeout_ms.1,
                defaults.response_timeout,
            )?,
            connect_retries,
        })
    }

    /// 交给 `ConnectionManager` 的配置：建立与后台重连都用它。
    fn manager_config(&self) -> ConnectionManagerConfig {
        ConnectionManagerConfig::new()
            .set_connection_timeout(self.connect_timeout)
            .set_response_timeout(self.response_timeout)
            .set_number_of_retries(self.connect_retries)
            .set_factor(2)
            .set_max_delay(MAX_RETRY_DELAY_MS)
    }

    /// 一次建立最长要多久：每次尝试一个建连超时，两次尝试之间至多 2 s（1 s 起点加至多一倍抖动，
    /// 封顶 [`MAX_RETRY_DELAY_MS`] 之后同样至多一倍抖动）。只用于说明与用例的上界。
    pub fn connect_budget(&self) -> Duration {
        let attempts = u32::try_from(self.connect_retries + 1).unwrap_or(u32::MAX);
        let retries = attempts.saturating_sub(1);
        self.connect_timeout
            .saturating_mul(attempts)
            .saturating_add(Duration::from_secs(2).saturating_mul(retries))
    }
}

enum ConnectionState {
    /// 还没有建立过。
    Unconnected,
    /// 已建立；断开后由 `ConnectionManager` 在后台重连。
    Connected(ConnectionManager),
    /// 上一次建立失败的时刻；一个建连超时之内不再发起连接。
    FailedAt(Instant),
}

/// 进程内共享的一个 Redis 连接（见模块说明）。一个进程对同一个 Redis 只建一份，各仓储共用。
pub struct SharedRedisConnection {
    client: redis::Client,
    limits: RedisConnectionLimits,
    state: tokio::sync::Mutex<ConnectionState>,
}

impl std::fmt::Debug for SharedRedisConnection {
    // 不打印客户端：连接信息里可能有口令。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedRedisConnection")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl SharedRedisConnection {
    pub fn new(client: redis::Client, limits: RedisConnectionLimits) -> Self {
        Self {
            client,
            limits,
            state: tokio::sync::Mutex::new(ConnectionState::Unconnected),
        }
    }

    pub fn limits(&self) -> RedisConnectionLimits {
        self.limits
    }

    /// 共享的连接（克隆即复用同一条多路复用连接）。还没建立就在这里建立；上一次建立失败后的
    /// 一个建连超时之内直接回暂时错误（`NotConnected` 的 IO 错误，归类为 `ServiceUnavailable`）。
    pub async fn get(&self) -> RedisResult<ConnectionManager> {
        let mut state = self.state.lock().await;
        match &*state {
            ConnectionState::Connected(connection) => return Ok(connection.clone()),
            ConnectionState::FailedAt(at) if at.elapsed() < self.limits.connect_timeout => {
                return Err(RedisError::from(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "Redis connection failed recently; not reconnecting yet",
                )));
            }
            ConnectionState::FailedAt(_) | ConnectionState::Unconnected => {}
        }
        match ConnectionManager::new_with_config(self.client.clone(), self.limits.manager_config())
            .await
        {
            Ok(connection) => {
                *state = ConnectionState::Connected(connection.clone());
                Ok(connection)
            }
            Err(err) => {
                *state = ConnectionState::FailedAt(Instant::now());
                Err(err)
            }
        }
    }
}

/// 认证或权限被拒、应答解析失败、客户端配置无效：等 Redis 恢复也不会好，要查配置（口令、ACL、
/// 地址是否指向了别的服务、TLS 设置）。redis 自己把认证失败与解析错误算作「要重连」，原先的分类
/// 据此把它们判成了暂时错误。
fn is_redis_configuration_error(err: &RedisError) -> bool {
    matches!(
        err.kind(),
        ErrorKind::AuthenticationFailed
            | ErrorKind::ParseError
            | ErrorKind::InvalidClientConfig
            | ErrorKind::RESP3NotSupported
    ) || matches!(err.code(), Some("NOAUTH" | "WRONGPASS" | "NOPERM"))
}

/// Redis 错误的分类，口径各服务一致：
/// - 认证或权限被拒（`AuthenticationFailed`、服务端的 `NOAUTH`/`WRONGPASS`/`NOPERM`）、应答解析失败、
///   客户端配置无效：`ConfigurationError`，不可重试；
/// - 超时：调用方给的 `timeout` 码；
/// - 连不上、连接断开、其他 IO 失败、需要重连的错误，以及 Redis 正在加载、集群不可用、主节点下线：
///   `ServiceUnavailable`，可重试；
/// - 其余（命令错误、类型不符等）：`InternalError`，不可重试。
pub fn redis_error_code(err: &RedisError, timeout: ErrorCode) -> ErrorCode {
    if is_redis_configuration_error(err) {
        ErrorCode::ConfigurationError
    } else if err.is_timeout() {
        timeout
    } else if err.is_io_error()
        || err.is_unrecoverable_error()
        || matches!(
            err.kind(),
            ErrorKind::BusyLoadingError
                | ErrorKind::TryAgain
                | ErrorKind::ClusterDown
                | ErrorKind::MasterDown
        )
    {
        ErrorCode::ServiceUnavailable
    } else {
        ErrorCode::InternalError
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn limits(connect_ms: u64, response_ms: u64, retries: usize) -> RedisConnectionLimits {
        RedisConnectionLimits {
            connect_timeout: Duration::from_millis(connect_ms),
            response_timeout: Duration::from_millis(response_ms),
            connect_retries: retries,
        }
    }

    /// `get` 应当失败（`ConnectionManager` 没有 `Debug`，不能用 `expect_err`）。
    fn refused(result: RedisResult<ConnectionManager>) -> RedisError {
        match result {
            Ok(_) => panic!("the connection must not be established"),
            Err(err) => err,
        }
    }

    fn client(address: std::net::SocketAddr) -> redis::Client {
        redis::Client::open(format!("redis://{address}/")).unwrap()
    }

    /// 一个刚释放的本机端口：连上去立即被拒。
    async fn closed_port() -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        address
    }

    /// 缓冲区开头一条完整的 RESP 数组命令占用的字节数与命令名；不完整时为 `None`。
    fn next_command(buffer: &[u8]) -> Option<(String, usize)> {
        fn line(buffer: &[u8], from: usize) -> Option<(&[u8], usize)> {
            let end = buffer.get(from..)?.windows(2).position(|w| w == b"\r\n")? + from;
            Some((&buffer[from..end], end + 2))
        }
        fn number(text: &[u8], prefix: u8) -> Option<usize> {
            std::str::from_utf8(text.strip_prefix(&[prefix])?)
                .ok()?
                .parse()
                .ok()
        }
        let (header, mut at) = line(buffer, 0)?;
        let mut name = String::new();
        for index in 0..number(header, b'*')? {
            let (length, start) = line(buffer, at)?;
            let end = start + number(length, b'$')?;
            if buffer.len() < end + 2 {
                return None;
            }
            if index == 0 {
                name = String::from_utf8_lossy(&buffer[start..end]).into_owned();
            }
            at = end + 2;
        }
        Some((name, at))
    }

    /// 本机假 Redis：前 `refused` 个连接一接受就关掉；之后的连接对 `CLIENT` 与 `PING` 回 `+OK`，
    /// 对其余命令按 `answer_commands` 回 `+OK` 或一直不回。返回地址与已接受的连接数。
    async fn fake_redis(
        refused: usize,
        answer_commands: bool,
    ) -> (std::net::SocketAddr, Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = accepted.clone();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                if counter.fetch_add(1, Ordering::SeqCst) < refused {
                    drop(socket);
                    continue;
                }
                tokio::spawn(async move {
                    let mut pending = Vec::new();
                    let mut chunk = [0u8; 4096];
                    loop {
                        let read = match socket.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(read) => read,
                        };
                        pending.extend_from_slice(&chunk[..read]);
                        while let Some((command, used)) = next_command(&pending) {
                            pending.drain(..used);
                            let handshake = command.eq_ignore_ascii_case("CLIENT")
                                || command.eq_ignore_ascii_case("PING");
                            if (handshake || answer_commands)
                                && socket.write_all(b"+OK\r\n").await.is_err()
                            {
                                return;
                            }
                        }
                    }
                });
            }
        });
        (address, accepted)
    }

    /// 对关闭端口的连接在有界时间内失败（默认上限）：修复前每次操作 `ConnectionManager::new` 用
    /// redis 的默认退避（6 次重试、每次至多 60 s），60 s 仍未返回。
    #[tokio::test]
    async fn a_closed_port_fails_within_the_default_bound() {
        let connection = SharedRedisConnection::new(
            client(closed_port().await),
            RedisConnectionLimits::default(),
        );
        let budget = RedisConnectionLimits::default().connect_budget();
        let started = Instant::now();
        let err = refused(
            tokio::time::timeout(budget + Duration::from_secs(2), connection.get())
                .await
                .expect("connecting to a closed port must fail within the bound"),
        );
        assert!(started.elapsed() <= budget + Duration::from_secs(2));
        assert_eq!(
            redis_error_code(&err, ErrorCode::NetworkTimeout),
            ErrorCode::ServiceUnavailable,
            "{err}"
        );
    }

    /// 接受连接却不应答握手的服务：每次尝试在建连超时处失败，整体不超过建连上限。修复前没有建连
    /// 超时，一直等下去。
    #[tokio::test]
    async fn a_silent_server_times_out_while_connecting() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let held = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let sockets = held.clone();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                sockets.lock().await.push(socket);
            }
        });
        let limits = limits(300, 300, 1);
        let connection = SharedRedisConnection::new(client(address), limits);
        let err = refused(
            tokio::time::timeout(
                limits.connect_budget() + Duration::from_secs(2),
                connection.get(),
            )
            .await
            .expect("the connect timeout bounds a silent server"),
        );
        assert!(err.is_timeout(), "{err}");
    }

    /// 建立之后，不应答的命令在应答超时处失败。修复前没有应答超时。
    #[tokio::test]
    async fn an_unanswered_command_times_out() {
        let (address, _) = fake_redis(0, false).await;
        let connection = SharedRedisConnection::new(client(address), limits(1_000, 300, 0));
        let mut conn = connection.get().await.expect("the handshake is answered");
        let started = Instant::now();
        let err = tokio::time::timeout(
            Duration::from_secs(5),
            redis::cmd("SET")
                .arg("k")
                .arg("v")
                .query_async::<()>(&mut conn),
        )
        .await
        .expect("the response timeout bounds the command")
        .expect_err("the server never answers SET");
        assert!(err.is_timeout(), "{err}");
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(
            redis_error_code(&err, ErrorCode::OperationTimeout),
            ErrorCode::OperationTimeout
        );
    }

    /// 一个进程共享一条连接；建立失败后一个建连超时之内不再发起连接，过后再建。
    #[tokio::test]
    async fn one_connection_is_shared_and_rebuilt_only_after_the_backoff() {
        // 前两个连接（第一次建立及其一次重试）被关掉，之后正常应答。
        let (address, accepted) = fake_redis(2, true).await;
        let limits = limits(400, 1_000, 1);
        let connection = Arc::new(SharedRedisConnection::new(client(address), limits));

        let err = refused(connection.get().await);
        assert_eq!(accepted.load(Ordering::SeqCst), 2, "{err}");

        // 退避期内：不发起连接，直接回暂时错误。
        let err = refused(connection.get().await);
        assert_eq!(
            redis_error_code(&err, ErrorCode::NetworkTimeout),
            ErrorCode::ServiceUnavailable
        );
        assert_eq!(accepted.load(Ordering::SeqCst), 2);

        tokio::time::sleep(limits.connect_timeout).await;
        let mut handles = Vec::new();
        for _ in 0..8 {
            let connection = connection.clone();
            handles.push(tokio::spawn(async move {
                let mut conn = connection.get().await.expect("rebuilt after the backoff");
                redis::cmd("SET")
                    .arg("k")
                    .arg("v")
                    .query_async::<()>(&mut conn)
                    .await
                    .expect("answered");
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            3,
            "concurrent callers share one rebuilt connection"
        );
    }

    /// 认证与权限被拒、应答解析失败、客户端配置无效是配置错误（不可重试）；修复前前两类经
    /// redis 的「要重连」判成了可重试的 ServiceUnavailable，服务端的 NOAUTH/WRONGPASS/NOPERM 是
    /// InternalError。其余分类不变。
    #[test]
    fn auth_permission_and_parse_failures_are_configuration_errors() {
        let io = |kind| RedisError::from(std::io::Error::from(kind));
        let server = |code: &str| {
            redis::make_extension_error(code.to_string(), Some("leak-marker-51".to_string()))
        };
        for (err, code) in [
            (
                RedisError::from((ErrorKind::AuthenticationFailed, "auth")),
                ErrorCode::ConfigurationError,
            ),
            (
                RedisError::from((ErrorKind::ParseError, "parse")),
                ErrorCode::ConfigurationError,
            ),
            (
                RedisError::from((ErrorKind::InvalidClientConfig, "url")),
                ErrorCode::ConfigurationError,
            ),
            (server("NOAUTH"), ErrorCode::ConfigurationError),
            (server("WRONGPASS"), ErrorCode::ConfigurationError),
            (server("NOPERM"), ErrorCode::ConfigurationError),
            (io(std::io::ErrorKind::TimedOut), ErrorCode::NetworkTimeout),
            (
                io(std::io::ErrorKind::ConnectionRefused),
                ErrorCode::ServiceUnavailable,
            ),
            (
                io(std::io::ErrorKind::ConnectionReset),
                ErrorCode::ServiceUnavailable,
            ),
            (
                RedisError::from((ErrorKind::BusyLoadingError, "loading")),
                ErrorCode::ServiceUnavailable,
            ),
            (
                RedisError::from((ErrorKind::TypeError, "WRONGTYPE")),
                ErrorCode::InternalError,
            ),
            (
                RedisError::from((ErrorKind::ResponseError, "ERR")),
                ErrorCode::InternalError,
            ),
        ] {
            assert_eq!(
                redis_error_code(&err, ErrorCode::NetworkTimeout),
                code,
                "{err}"
            );
        }
    }

    #[test]
    fn limits_come_from_the_environment_or_the_defaults() {
        let names = ("CONNECT_MS", "RESPONSE_MS", "RETRIES");
        let parse = |connect: Option<&str>, response: Option<&str>, retries: Option<&str>| {
            RedisConnectionLimits::from_env_values(
                (names.0, connect),
                (names.1, response),
                (names.2, retries),
            )
        };
        assert_eq!(
            parse(None, Some(" "), None).unwrap(),
            RedisConnectionLimits::default()
        );
        assert_eq!(
            parse(Some("500"), Some("750"), Some("0")).unwrap(),
            limits(500, 750, 0)
        );
        for (connect, response, retries, name) in [
            (Some("0"), None, None, names.0),
            (Some("60001"), None, None, names.0),
            (Some("2s"), None, None, names.0),
            (None, Some("-1"), None, names.1),
            (None, None, Some("6"), names.2),
            (None, None, Some("many"), names.2),
        ] {
            let err = parse(connect, response, retries).unwrap_err();
            assert_eq!(err.code(), Some(ErrorCode::ConfigurationError), "{err}");
            assert!(err.to_string().contains(name), "{err}");
        }
        // 默认上限：一次建立在几秒内结束。
        assert!(RedisConnectionLimits::default().connect_budget() <= Duration::from_secs(6));
    }
}
