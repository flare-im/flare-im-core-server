//! 插件 gRPC 通道解析：静态 `http(s)://` 与 `discovery://<service_name>` 统一入口。

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use flare_server_core::ServiceClient;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::infrastructure::config::capability_runtime::{
    is_discovery_route_authority, service_name_from_discovery_route,
};

static DISCOVERY_CLIENT_CACHE: Lazy<DashMap<String, Arc<Mutex<ServiceClient>>>> =
    Lazy::new(DashMap::new);
static STATIC_CHANNEL_CACHE: Lazy<DashMap<String, Channel>> = Lazy::new(DashMap::new);

pub(crate) const PLUGIN_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);

fn normalize_static_authority(grpc_authority: &str) -> Result<String, String> {
    Ok(if grpc_authority.starts_with("http://") || grpc_authority.starts_with("https://") {
        grpc_authority.to_string()
    } else {
        format!("http://{grpc_authority}")
    })
}

fn resolve_static_channel(grpc_authority: &str) -> Result<Channel, String> {
    if let Some(ch) = STATIC_CHANNEL_CACHE.get(grpc_authority) {
        return Ok(ch.clone());
    }
    let endpoint = normalize_static_authority(grpc_authority)?;
    let channel = Channel::from_shared(endpoint.clone())
        .map_err(|e| format!("invalid plugin endpoint {endpoint}: {e}"))?
        .connect_lazy();
    STATIC_CHANNEL_CACHE.insert(grpc_authority.to_string(), channel.clone());
    Ok(channel)
}

async fn resolve_discovery_channel(service_name: &str) -> Result<Channel, String> {
    if let Some(client) = DISCOVERY_CLIENT_CACHE.get(service_name) {
        let mut guard = client.lock().await;
        return discovery_channel_with_timeout(service_name, &mut guard).await;
    }

    let discover = flare_im_core::discovery::create_discover(service_name)
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
    match tokio::time::timeout(PLUGIN_DISCOVERY_TIMEOUT, client.get_channel()).await {
        Ok(Ok(channel)) => Ok(channel),
        Ok(Err(e)) => {
            DISCOVERY_CLIENT_CACHE.remove(service_name);
            Err(format!("discover {service_name}: {e}"))
        }
        Err(_) => {
            DISCOVERY_CLIENT_CACHE.remove(service_name);
            Err(format!(
                "discover {service_name}: timeout after {}s (plugin not registered or unreachable)",
                PLUGIN_DISCOVERY_TIMEOUT.as_secs()
            ))
        }
    }
}

/// 按 `RegisteredPluginInstance.grpc_authority` 解析 gRPC 通道。
pub async fn resolve_plugin_channel(grpc_authority: &str) -> Result<Channel, String> {
    if is_discovery_route_authority(grpc_authority) {
        let service_name = service_name_from_discovery_route(grpc_authority)
            .ok_or_else(|| format!("invalid discovery route: {grpc_authority}"))?;
        return resolve_discovery_channel(service_name).await;
    }
    resolve_static_channel(grpc_authority)
}
