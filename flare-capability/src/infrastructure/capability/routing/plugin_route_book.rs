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

    /// 这个实例是否承接某个能力。
    ///
    /// **声明即路由面**：verified 插件（有 `declared_operations`）按声明匹配，
    /// 而不是按注册记录上的 `capability_id`。注册粒度是 `(tenant, plugin)` ——
    /// 一个插件进程只有一条记录，`capability_id` 只是这条记录的标签。
    ///
    /// 只按 `capability_id` 匹配曾让声明了 13 个 op 的插件**只有 1 个能被路由到**，
    /// 其余 12 个报「未注册」。这个错误的信号是误导性的：插件明明注册成功、
    /// 健康检查也绿，看着像没部署。
    ///
    /// 匹配是「注册键**或**声明」而不是只看声明：注册键那一路要留着，
    /// 否则「注册了 X 却没声明 X」这种不一致会退化成「未注册」，
    /// 而它真正的原因是声明对不上。让它进候选、再由分发前的声明闸门拒掉，
    /// 报出来的才是「声明拒绝」—— 这两种症状的排查方向完全不同。
    ///
    /// unverified 实例（服务发现来的、没有声明）只剩注册键可依 ——
    /// 这正是「没有声明就没有边界」的代价，也是要求新插件声明的理由。
    fn serves(instance: &RegisteredPluginInstance, capability_id: &str) -> bool {
        if capability_id.is_empty() {
            return true;
        }
        if instance.capability_id.as_str() == capability_id {
            return true;
        }
        instance
            .declared_operations
            .iter()
            .any(|op| op.as_str() == capability_id)
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
            .filter(|e| Self::serves(e.value(), capability_id))
            .map(|e| e.value().clone())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn instance(
        plugin_id: &str,
        capability_id: &str,
        declared: &[&str],
    ) -> RegisteredPluginInstance {
        RegisteredPluginInstance {
            plugin_id: plugin_id.to_string(),
            capability_id: capability_id.to_string(),
            declared_operations: declared.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    /// 生产形态：插件只注册一条记录，却声明多个 op。
    ///
    /// 这条曾经是红的 —— 声明 13 个 op 的插件只有注册键那一个能被路由到，
    /// 其余报「未注册」。套件当时是绿的，因为它给每个 op 都重注册了一次，
    /// 而那不是任何插件的真实部署形态。
    #[test]
    fn a_single_registration_serves_every_declared_operation() {
        let book = PluginRouteBook::new();
        book.upsert(
            "t1",
            instance("p1", "x.feed", &["x.feed", "x.like", "x.publish"]),
        );

        for op in ["x.feed", "x.like", "x.publish"] {
            assert_eq!(
                book.list_filtered("t1", op).len(),
                1,
                "声明过的 {op} 应当路由得到"
            );
        }
    }

    /// 声明之外一律不路由 —— 边界就是这条匹配规则本身。
    #[test]
    fn undeclared_operations_are_not_routed() {
        let book = PluginRouteBook::new();
        book.upsert("t1", instance("p1", "x.feed", &["x.feed"]));
        assert!(book.list_filtered("t1", "x.delete").is_empty());
    }

    /// unverified（服务发现来的、无声明）退回注册键匹配：
    /// 旧插件不因为核心升级而被打死，代价是对它无法强制边界。
    #[test]
    fn unverified_instances_fall_back_to_the_registration_key() {
        let book = PluginRouteBook::new();
        book.upsert("t1", instance("p1", "x.feed", &[]));
        assert_eq!(book.list_filtered("t1", "x.feed").len(), 1);
        assert!(book.list_filtered("t1", "x.like").is_empty());
    }

    /// 租户隔离不受新匹配规则影响。
    #[test]
    fn declared_matching_does_not_cross_tenants() {
        let book = PluginRouteBook::new();
        book.upsert("t1", instance("p1", "x.feed", &["x.feed", "x.like"]));
        assert!(book.list_filtered("t2", "x.like").is_empty());
    }

    /// 空 capability 表示「列出该租户全部实例」，语义不变。
    #[test]
    fn empty_capability_lists_all_instances_of_the_tenant() {
        let book = PluginRouteBook::new();
        book.upsert("t1", instance("p1", "x.feed", &["x.feed"]));
        book.upsert("t1", instance("p2", "y.run", &["y.run"]));
        assert_eq!(book.list_filtered("t1", "").len(), 2);
    }

    /// 两个插件声明同一个 op → 都是候选，由健康状态决定先后。
    #[test]
    fn overlapping_declarations_yield_multiple_candidates() {
        let book = PluginRouteBook::new();
        book.upsert("t1", instance("p1", "x.feed", &["x.feed"]));
        book.upsert("t1", instance("p2", "y.feed", &["y.feed", "x.feed"]));
        assert_eq!(book.list_filtered("t1", "x.feed").len(), 2);
    }
}
