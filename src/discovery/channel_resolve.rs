//! 统一 gRPC 通道解析：`http(s)://` 静态懒连接 + `discovery://<service_name>` 注册中心发现。
//!
//! 基于 [`flare_core_transport`](flare_server_core::discovery) 的 [`ServiceClient`]（P2C + Channel 缓存）。

use std::sync::Arc;
use std::time::Duration;

use super::ServiceClient;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};

use super::create_discover;
use crate::service_names::service_names::{
    ACCESS_GATEWAY, CONVERSATION, CORE_GATEWAY, MEDIA, ORCHESTRATOR, SIGNALING_ONLINE,
    SIGNALING_ROUTE, STORAGE_READER, SYNC_ORCHESTRATOR,
};

/// 注册中心路由前缀，与 capability `PluginRouteBook` 一致。
pub const DISCOVERY_ROUTE_PREFIX: &str = "discovery://";

/// 本地开发默认静态 gRPC 地址（与 `config/services/*.toml` 默认端口对齐）。
pub fn default_static_grpc_fallback(service_name: &str) -> &'static str {
    match service_name {
        CONVERSATION => "http://127.0.0.1:50090",
        SIGNALING_ONLINE => "http://127.0.0.1:50061",
        SIGNALING_ROUTE => "http://127.0.0.1:50062",
        ORCHESTRATOR => "http://127.0.0.1:50181",
        STORAGE_READER => "http://127.0.0.1:60083",
        SYNC_ORCHESTRATOR => "http://127.0.0.1:60084",
        MEDIA => "http://127.0.0.1:60081",
        ACCESS_GATEWAY => "http://127.0.0.1:60051",
        CORE_GATEWAY => "http://127.0.0.1:50050",
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

fn resolve_static_channel(grpc_authority: &str) -> Result<Channel, String> {
    if let Some(ch) = STATIC_CHANNEL_CACHE.get(grpc_authority) {
        return Ok(ch.clone());
    }
    let endpoint = normalize_static_authority(grpc_authority)?;
    let channel = Channel::from_shared(endpoint.clone())
        .map_err(|e| format!("invalid static endpoint {endpoint}: {e}"))?
        .connect_lazy();
    STATIC_CHANNEL_CACHE.insert(grpc_authority.to_string(), channel.clone());
    Ok(channel)
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
    let endpoint = Endpoint::from_shared(static_fallback_uri.to_string())
        .map_err(|e| format!("invalid static fallback {static_fallback_uri}: {e}"))?;
    tokio::time::timeout(DISCOVERY_CHANNEL_TIMEOUT, endpoint.connect())
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
