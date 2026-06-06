//! 插件数据面 endpoint 登记（CQRS 写路径的进程内实现，开发期默认）。
//!
//! 后续可替换为 PostgreSQL / Redis 投影，领域端口见 `domain::capability::plugin_route`（如需要再抽）。

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use flare_grpc_proto::capability::RegisteredPluginInstance;

#[derive(Debug, Clone)]
pub struct PluginRouteSnapshot {
    pub tenant_id: String,
    pub instance: RegisteredPluginInstance,
}

#[derive(Debug, Clone)]
pub struct PluginHealthStatus {
    pub healthy: bool,
    pub last_checked_at: Option<Instant>,
    pub last_error: Option<String>,
}

impl Default for PluginHealthStatus {
    fn default() -> Self {
        Self {
            healthy: true,
            last_checked_at: None,
            last_error: None,
        }
    }
}

/// `(tenant_id, plugin_id)` -> 最近一次上报的实例描述。
#[derive(Clone, Default)]
pub struct PluginRouteBook {
    inner: Arc<DashMap<String, RegisteredPluginInstance>>,
    health: Arc<DashMap<String, PluginHealthStatus>>,
}

impl PluginRouteBook {
    pub fn new() -> Self {
        Self::default()
    }

    fn composite_key(tenant_id: &str, plugin_id: &str) -> String {
        format!("{tenant_id}\x1f{plugin_id}")
    }

    pub fn upsert(&self, tenant_id: &str, instance: RegisteredPluginInstance) {
        let k = Self::composite_key(tenant_id, &instance.plugin_id);
        self.inner.insert(k.clone(), instance);
        self.health.entry(k).or_default();
    }

    pub fn remove(&self, tenant_id: &str, plugin_id: &str) -> bool {
        let key = Self::composite_key(tenant_id, plugin_id);
        self.health.remove(&key);
        self.inner.remove(&key).is_some()
    }

    pub fn list_filtered(
        &self,
        tenant_id: &str,
        capability_id: &str,
    ) -> Vec<RegisteredPluginInstance> {
        let prefix = format!("{tenant_id}\x1f");
        self.inner
            .iter()
            .filter(|e| e.key().starts_with(&prefix))
            .map(|e| e.value().clone())
            .filter(|i| capability_id.is_empty() || i.capability_id.as_str() == capability_id)
            .collect()
    }

    pub fn list_snapshots(&self) -> Vec<PluginRouteSnapshot> {
        self.inner
            .iter()
            .filter_map(|e| {
                let key = e.key();
                key.split_once('\x1f')
                    .map(|(tenant_id, _)| PluginRouteSnapshot {
                        tenant_id: tenant_id.to_string(),
                        instance: e.value().clone(),
                    })
            })
            .collect()
    }

    pub fn mark_health(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        healthy: bool,
        error: Option<String>,
    ) {
        let key = Self::composite_key(tenant_id, plugin_id);
        self.health.insert(
            key,
            PluginHealthStatus {
                healthy,
                last_checked_at: Some(Instant::now()),
                last_error: error,
            },
        );
    }

    pub fn is_healthy(&self, tenant_id: &str, plugin_id: &str, max_stale: Duration) -> bool {
        let key = Self::composite_key(tenant_id, plugin_id);
        let Some(status) = self.health.get(&key) else {
            return true;
        };
        if !status.healthy {
            return false;
        }
        if let Some(last) = status.last_checked_at
            && last.elapsed() > max_stale
        {
            return false;
        }
        true
    }

    pub fn last_error(&self, tenant_id: &str, plugin_id: &str) -> Option<String> {
        let key = Self::composite_key(tenant_id, plugin_id);
        self.health.get(&key).and_then(|s| s.last_error.clone())
    }
}
