//! 进程内共享 `ServiceClient`，避免重复 `create_discover` 启动多套 Consul 刷新任务。

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use flare_server_core::discovery::{ServiceClient, ServiceDiscover};
use once_cell::sync::Lazy;
use tokio::sync::Mutex;

static SERVICE_CLIENT_POOL: Lazy<DashMap<String, Arc<Mutex<ServiceClient>>>> =
    Lazy::new(DashMap::new);

fn pool_key(service_type: &str, tag_suffix: Option<&str>) -> String {
    match tag_suffix {
        Some(suffix) if !suffix.is_empty() => format!("{service_type}|{suffix}"),
        _ => service_type.to_string(),
    }
}

/// 按 service_type 复用 discovery client（每个 key 仅创建一次 ServiceDiscover 刷新任务）。
pub async fn shared_service_client(
    service_type: &str,
    tag_suffix: Option<&str>,
    create_discover: impl std::future::Future<
        Output = Result<Option<ServiceDiscover>, Box<dyn std::error::Error + Send + Sync>>,
    >,
) -> Result<Option<Arc<Mutex<ServiceClient>>>, Box<dyn std::error::Error + Send + Sync>> {
    let key = pool_key(service_type, tag_suffix);
    if let Some(existing) = SERVICE_CLIENT_POOL.get(&key) {
        return Ok(Some(existing.clone()));
    }

    let discover = match create_discover.await? {
        Some(discover) => discover,
        None => return Ok(None),
    };
    let client = Arc::new(Mutex::new(ServiceClient::new(discover)));
    SERVICE_CLIENT_POOL.insert(key, client.clone());
    Ok(Some(client))
}

/// 测试 / 进程内重置（非生产 API）。
#[cfg(test)]
pub fn clear_service_client_pool_for_tests() {
    SERVICE_CLIENT_POOL.clear();
}

/// 将 tag_filters 序列化为 pool key 后缀。
pub fn tag_filters_pool_suffix(filters: &HashMap<String, String>) -> String {
    let mut pairs: Vec<_> = filters.iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}
