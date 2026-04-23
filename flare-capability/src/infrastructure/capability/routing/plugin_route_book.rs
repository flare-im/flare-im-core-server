//! 插件数据面 endpoint 登记（CQRS 写路径的进程内实现，开发期默认）。
//!
//! 后续可替换为 PostgreSQL / Redis 投影，领域端口见 `domain::capability::plugin_route`（如需要再抽）。

use std::sync::Arc;

use dashmap::DashMap;
use flare_grpc_proto::capability::RegisteredPluginInstance;

/// `(tenant_id, plugin_id)` -> 最近一次上报的实例描述。
#[derive(Clone, Default)]
pub struct PluginRouteBook {
    inner: Arc<DashMap<String, RegisteredPluginInstance>>,
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
        self.inner.insert(k, instance);
    }

    pub fn remove(&self, tenant_id: &str, plugin_id: &str) -> bool {
        self.inner
            .remove(&Self::composite_key(tenant_id, plugin_id))
            .is_some()
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
            .filter(|i| {
                capability_id.is_empty() || i.capability_id.as_str() == capability_id
            })
            .collect()
    }
}
