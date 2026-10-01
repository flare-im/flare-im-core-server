//! 统一 gRPC 通道解析：`http(s)://` 静态懒连接 + `discovery://<service_name>` 注册中心发现。
//!
//! 基于 [`flare_core_transport`](flare_server_core::discovery) 的 [`ServiceClient`]（P2C + Channel 缓存）。

use std::sync::Arc;
use std::time::Duration;

use super::ServiceClient;
use dashmap::DashMap;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioIo;
use once_cell::sync::Lazy;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tonic::codegen::http::Uri;
use tonic::transport::{Channel, Endpoint};
use tower::Service;

use super::create_discover;
use crate::service_names::service_names::{
    ACCESS_GATEWAY, ADMIN_GATEWAY, API_GATEWAY, CONVERSATION, MEDIA, MESSAGE_INGEST, ORCHESTRATOR,
    SIGNALING_ONLINE, SIGNALING_ROUTE, STORAGE_READER, SYNC_ORCHESTRATOR,
};

/// 注册中心路由前缀，与 capability `PluginRouteBook` 一致。
pub const DISCOVERY_ROUTE_PREFIX: &str = "discovery://";

/// Same-Cell Access Gateway gRPC listener, not its public WS/QUIC edge port.
pub const STATIC_ACCESS_GATEWAY_GRPC_URI: &str = "http://127.0.0.1:60060";

/// 单 Cell 默认静态 gRPC 地址；Access Gateway 必须指向内部 gRPC 而非 WS 端口。
pub fn default_static_grpc_fallback(service_name: &str) -> &'static str {
    match service_name {
        CONVERSATION => "http://127.0.0.1:50090",
        SIGNALING_ONLINE => "http://127.0.0.1:50061",
        SIGNALING_ROUTE => "http://127.0.0.1:50062",
        MESSAGE_INGEST => "http://127.0.0.1:50182",
        ORCHESTRATOR => "http://127.0.0.1:50181",
        STORAGE_READER => "http://127.0.0.1:60083",
        SYNC_ORCHESTRATOR => "http://127.0.0.1:60084",
        MEDIA => "http://127.0.0.1:60081",
        ACCESS_GATEWAY => STATIC_ACCESS_GATEWAY_GRPC_URI,
        API_GATEWAY => "http://127.0.0.1:50050",
        ADMIN_GATEWAY => "http://127.0.0.1:50051",
        _ => "http://127.0.0.1:65535",
    }
}

/// 带超时的 `ServiceClient::get_channel`（避免对未注册下游无限等待）。
pub async fn get_discovered_channel_with_timeout(
    service_name: &str,
    client: &mut ServiceClient,
) -> Result<Channel, String> {
    discovery_channel_with_timeout(service_name, client).await
}

/// 发现解析超时（与 capability 插件调用对齐）。
pub const DISCOVERY_CHANNEL_TIMEOUT: Duration = Duration::from_secs(10);

static DISCOVERY_CLIENT_CACHE: Lazy<DashMap<String, Arc<Mutex<ServiceClient>>>> =
    Lazy::new(DashMap::new);
static STATIC_CHANNEL_CACHE: Lazy<DashMap<String, Channel>> = Lazy::new(DashMap::new);

/// `discovery://flare-social-hook` → `flare-social-hook`
pub fn discovery_route_authority(service_name: &str) -> String {
    format!("{DISCOVERY_ROUTE_PREFIX}{service_name}")
}

pub fn is_discovery_route_authority(authority: &str) -> bool {
    authority.starts_with(DISCOVERY_ROUTE_PREFIX)
}

pub fn service_name_from_discovery_route(authority: &str) -> Option<&str> {
    authority
        .strip_prefix(DISCOVERY_ROUTE_PREFIX)
        .filter(|s| !s.trim().is_empty())
}

fn normalize_static_authority(grpc_authority: &str) -> Result<String, String> {
    Ok(
        if grpc_authority.starts_with("http://") || grpc_authority.starts_with("https://") {
            grpc_authority.to_string()
        } else {
            format!("http://{grpc_authority}")
        },
    )
}

/// 静态地址通道的连接策略。
///
/// 静态地址背后的实例会换地址（容器重启后拿到新 IP，名字不变）。懒连接的通道在对端消失后
/// 重连：若这次建连落在已经不存在的旧地址上而又没有上限，通道会一直停在「连接中」，期间
/// 每个调用都只能等满调用方自己的超时（W6-S2：Social 重启后 PreSend Hook 卡了约 66 s）。
/// 因此一次建连（名字解析加 TCP 握手）有上限，失败后下一次调用重新解析名字；已建立的连接
/// 用 HTTP/2 ping 探活，对端不再应答时断开重连，而不是等 TCP 自己超时。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaticChannelPolicy {
    /// 一次建连的上限，覆盖名字解析与 TCP 握手。
    pub connect_timeout: Duration,
    /// HTTP/2 ping 间隔（空闲时也发）。
    pub keepalive_interval: Duration,
    /// ping 未被确认即判连接失效的时间。
    pub keepalive_timeout: Duration,
    /// TCP keepalive 空闲时间。
    pub tcp_keepalive: Duration,
}

impl StaticChannelPolicy {
    pub const DEFAULT: Self = Self {
        connect_timeout: Duration::from_secs(2),
        keepalive_interval: Duration::from_secs(10),
        keepalive_timeout: Duration::from_secs(5),
        tcp_keepalive: Duration::from_secs(30),
    };
}

impl Default for StaticChannelPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// 按 [`StaticChannelPolicy`] 配好的静态 [`Endpoint`]（`http://` 或 `https://`）。
pub fn static_grpc_endpoint(uri: &str, policy: StaticChannelPolicy) -> Result<Endpoint, String> {
    let endpoint = Endpoint::from_shared(uri.to_string())
        .map_err(|e| format!("invalid static endpoint {uri}: {e}"))?;
    Ok(endpoint
        .connect_timeout(policy.connect_timeout)
        .http2_keep_alive_interval(policy.keepalive_interval)
        .keep_alive_timeout(policy.keepalive_timeout)
        .keep_alive_while_idle(true)
        .tcp_nodelay(true)
        .tcp_keepalive(Some(policy.tcp_keepalive)))
}

/// 与 tonic 默认连接器相同的 TCP 设置。它整体交给 tonic 的建连超时包住，所以上限也覆盖
/// 名字解析（tonic 默认连接器的 `connect_timeout` 只管 TCP 握手，不管 DNS）。
fn static_http_connector(policy: StaticChannelPolicy) -> HttpConnector {
    let mut http = HttpConnector::new();
    http.enforce_http(false);
    http.set_nodelay(true);
    http.set_keepalive(Some(policy.tcp_keepalive));
    http.set_connect_timeout(Some(policy.connect_timeout));
    http
}

/// 懒连接的静态通道，按 [`StaticChannelPolicy::DEFAULT`]：构造时不连下游。
pub fn connect_static_channel_lazy(uri: &str) -> Result<Channel, String> {
    let policy = StaticChannelPolicy::DEFAULT;
    connect_static_channel_lazy_with(uri, policy, static_http_connector(policy))
}

/// 同 [`connect_static_channel_lazy`]，连接器与策略可换（测试用它模拟名字解析换地址）。
pub fn connect_static_channel_lazy_with<C>(
    uri: &str,
    policy: StaticChannelPolicy,
    connector: C,
) -> Result<Channel, String>
where
    C: Service<Uri, Response = TokioIo<TcpStream>> + Send + 'static,
    C::Future: Send,
    C::Error: std::error::Error + Send + Sync + 'static,
{
    Ok(static_grpc_endpoint(uri, policy)?.connect_with_connector_lazy(connector))
}

/// 立即建连的静态通道（策略同上），建连失败直接返回错误。
pub async fn connect_static_channel(uri: &str) -> Result<Channel, String> {
    let policy = StaticChannelPolicy::DEFAULT;
    static_grpc_endpoint(uri, policy)?
        .connect_with_connector(static_http_connector(policy))
        .await
        .map_err(|e| format!("connect {uri}: {e}"))
}

/// 调用失败是否出在传输层（连不上、连接被断、ping 超时），而不是对端回了一个 gRPC 状态。
///
/// tonic 把传输错误包成 `Status` 时会保留原始错误作为 source；对端回的状态没有 source。
pub fn is_transport_failure(status: &tonic::Status) -> bool {
    std::error::Error::source(status).is_some()
}

fn resolve_static_channel(grpc_authority: &str) -> Result<Channel, String> {
    if let Some(ch) = STATIC_CHANNEL_CACHE.get(grpc_authority) {
        return Ok(ch.clone());
    }
    let endpoint = normalize_static_authority(grpc_authority)?;
    let channel = connect_static_channel_lazy(&endpoint)?;
    STATIC_CHANNEL_CACHE.insert(grpc_authority.to_string(), channel.clone());
    Ok(channel)
}

/// 丢弃某个静态地址的缓存通道：下一次 [`resolve_grpc_channel`] 新建通道，重新解析名字、
/// 新建连接，不再沿用旧通道的重连状态。`discovery://` 路由不受影响。
pub fn invalidate_static_channel(grpc_authority: &str) {
    STATIC_CHANNEL_CACHE.remove(grpc_authority);
}

/// 调用 `grpc_authority` 失败后调用：失败出在传输层时丢弃该静态通道（见
/// [`invalidate_static_channel`]）；对端回的业务状态不动缓存。返回是否丢弃了。
pub fn invalidate_static_channel_on_transport_failure(
    grpc_authority: &str,
    status: &tonic::Status,
) -> bool {
    if is_discovery_route_authority(grpc_authority) || !is_transport_failure(status) {
        return false;
    }
    invalidate_static_channel(grpc_authority);
    true
}

async fn resolve_discovery_channel(service_name: &str) -> Result<Channel, String> {
    if let Some(client) = DISCOVERY_CLIENT_CACHE.get(service_name) {
        let mut guard = client.lock().await;
        return discovery_channel_with_timeout(service_name, &mut guard).await;
    }

    let discover = create_discover(service_name)
        .await
        .map_err(|e| format!("create discover for {service_name}: {e}"))?
        .ok_or_else(|| format!("service discovery not configured for {service_name}"))?;
    let client = Arc::new(Mutex::new(ServiceClient::new(discover)));
    DISCOVERY_CLIENT_CACHE.insert(service_name.to_string(), Arc::clone(&client));
    let mut guard = client.lock().await;
    discovery_channel_with_timeout(service_name, &mut guard).await
}

async fn discovery_channel_with_timeout(
    service_name: &str,
    client: &mut ServiceClient,
) -> Result<Channel, String> {
    match tokio::time::timeout(DISCOVERY_CHANNEL_TIMEOUT, client.get_channel()).await {
        Ok(Ok(channel)) => Ok(channel),
        Ok(Err(e)) => {
            DISCOVERY_CLIENT_CACHE.remove(service_name);
            Err(format!("discover {service_name}: {e}"))
        }
        Err(_) => {
            DISCOVERY_CLIENT_CACHE.remove(service_name);
            Err(format!(
                "discover {service_name}: timeout after {}s (not registered or unreachable)",
                DISCOVERY_CHANNEL_TIMEOUT.as_secs()
            ))
        }
    }
}

/// 按 endpoint / authority 解析 gRPC 通道（静态或 `discovery://`）。
pub async fn resolve_grpc_channel(grpc_authority: &str) -> Result<Channel, String> {
    if is_discovery_route_authority(grpc_authority) {
        let service_name = service_name_from_discovery_route(grpc_authority)
            .ok_or_else(|| format!("invalid discovery route: {grpc_authority}"))?;
        return resolve_discovery_channel(service_name).await;
    }
    resolve_static_channel(grpc_authority)
}

/// 按逻辑服务名发现通道（等价于 `resolve_grpc_channel(discovery://name)`）。
pub async fn resolve_discovered_service_channel(service_name: &str) -> Result<Channel, String> {
    resolve_discovery_channel(service_name).await
}

/// 清除指定逻辑服务的发现客户端缓存（Pod 漂移 / 滚动发布后下次 RPC 重新选实例）。
pub fn invalidate_discovered_service(service_name: &str) {
    DISCOVERY_CLIENT_CACHE.remove(service_name);
}

/// 有注册中心则发现，否则连接静态 URI（本地开发回退）。
pub async fn connect_grpc_channel_with_fallback(
    service_name: &str,
    static_fallback_uri: &str,
) -> Result<Channel, String> {
    if create_discover(service_name)
        .await
        .map_err(|e| format!("discover probe {service_name}: {e}"))?
        .is_some()
    {
        match resolve_discovery_channel(service_name).await {
            Ok(channel) => return Ok(channel),
            Err(e) => {
                tracing::warn!(
                    service_name,
                    error = %e,
                    fallback = static_fallback_uri,
                    "discovery failed, using static fallback"
                );
            }
        }
    }
    tokio::time::timeout(
        DISCOVERY_CHANNEL_TIMEOUT,
        connect_static_channel(static_fallback_uri),
    )
    .await
    .map_err(|_| {
        format!(
            "timeout connecting to static fallback {} ({}s)",
            static_fallback_uri,
            DISCOVERY_CHANNEL_TIMEOUT.as_secs()
        )
    })?
    .map_err(|e| format!("static fallback {static_fallback_uri}: {e}"))
}

#[cfg(test)]
mod static_route_tests {
    use super::*;

    #[test]
    fn access_gateway_fallback_is_its_grpc_listener_not_public_edge() {
        assert_eq!(
            default_static_grpc_fallback(ACCESS_GATEWAY),
            STATIC_ACCESS_GATEWAY_GRPC_URI
        );
        assert_eq!(STATIC_ACCESS_GATEWAY_GRPC_URI, "http://127.0.0.1:60060");
    }
}
