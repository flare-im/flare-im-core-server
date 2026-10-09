//! 插件数据面 endpoint 登记的进程内读模型。
//!
//! 两种来源：
//!
//! - **配置发现**（`plugin_discovery_endpoints`、`FLARE_MEDIA_CONTROL_GRPC_ENDPOINT`）：
//!   [`PluginRouteBook::upsert`]，只在本进程，不入库，从不按 TTL 清理。
//! - **登记**（`RegisterPluginEndpoint`）：[`PluginRouteBook::register`]。有库时
//!   （[`PluginRouteBook::with_store`]）先写 `capability_plugin_endpoints` 再进内存，写库失败即登记失败；
//!   启动时 [`PluginRouteBook::reload`] 整表载入，之后按周期重读，多实例由此收敛、重启不丢；
//!   健康检查通过的登记批量记 `last_healthy_at`，超过 TTL 不健康的登记由
//!   [`PluginRouteBook::cleanup_stale`] 删除。没有库时登记只在本进程（启动日志说明一次）。
//!
//! 同一 `(tenant, plugin)` 先由配置发现、后被登记时，登记覆盖之（来源变为登记）。

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use flare_grpc_proto::capability::RegisteredPluginInstance;
use flare_server_core::error::Result;

use crate::domain::model::{PluginEndpoint, PluginEndpointKey};
use crate::domain::repository::PluginEndpointRepository;

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

/// 一条路由从哪来：决定它是否入库、是否会被重读替换或按 TTL 清理。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteOrigin {
    /// 配置发现：进程内，不入库，不清理。
    Config,
    /// `RegisterPluginEndpoint` 登记：有库时入库，按表收敛。
    Registered,
}

#[derive(Debug, Clone)]
struct RouteEntry {
    instance: RegisteredPluginInstance,
    origin: RouteOrigin,
    /// 本进程最后一次写这条路由的时刻：比某次重读的快照更新的本地写不会被那次快照覆盖或删掉。
    touched: Instant,
}

/// `(tenant_id, plugin_id)` -> 最近一次上报的实例描述。
#[derive(Clone, Default)]
pub struct PluginRouteBook {
    inner: Arc<DashMap<String, RouteEntry>>,
    health: Arc<DashMap<String, PluginHealthStatus>>,
    /// 本进程注销的时刻：注销前开始的重读不得把它们再加回来。
    removed: Arc<DashMap<String, Instant>>,
    store: Option<Arc<dyn PluginEndpointRepository>>,
}

impl PluginRouteBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// 登记写进 `store`（`capability_plugin_endpoints`）的读模型。
    pub fn with_store(store: Arc<dyn PluginEndpointRepository>) -> Self {
        Self {
            store: Some(store),
            ..Self::default()
        }
    }

    /// 登记是否持久化（有库）。
    pub fn is_persistent(&self) -> bool {
        self.store.is_some()
    }

    fn composite_key(tenant_id: &str, plugin_id: &str) -> String {
        format!("{tenant_id}\x1f{plugin_id}")
    }

    fn split_key(key: &str) -> Option<(&str, &str)> {
        key.split_once('\x1f')
    }

    fn insert(&self, tenant_id: &str, instance: RegisteredPluginInstance, origin: RouteOrigin) {
        let key = Self::composite_key(tenant_id, &instance.plugin_id);
        self.removed.remove(&key);
        self.inner.insert(
            key.clone(),
            RouteEntry {
                instance,
                origin,
                touched: Instant::now(),
            },
        );
        self.health.entry(key).or_default();
    }

    /// 配置发现的端点：只进本进程内存。
    pub fn upsert(&self, tenant_id: &str, instance: RegisteredPluginInstance) {
        self.insert(tenant_id, instance, RouteOrigin::Config);
    }

    /// 从内存移除（不碰库）；返回之前是否在。
    pub fn remove(&self, tenant_id: &str, plugin_id: &str) -> bool {
        let key = Self::composite_key(tenant_id, plugin_id);
        self.health.remove(&key);
        if self.store.is_some() {
            self.removed.insert(key.clone(), Instant::now());
        }
        self.inner.remove(&key).is_some()
    }

    /// 这条路由的来源；不在簿里时为 `None`。
    pub fn origin(&self, tenant_id: &str, plugin_id: &str) -> Option<RouteOrigin> {
        self.inner
            .get(&Self::composite_key(tenant_id, plugin_id))
            .map(|entry| entry.origin)
    }

    /// `RegisterPluginEndpoint`：有库时先写库，写成功才进内存；写库失败即登记失败。
    pub async fn register(
        &self,
        tenant_id: &str,
        instance: RegisteredPluginInstance,
    ) -> Result<()> {
        if let Some(store) = &self.store {
            store
                .upsert(&PluginEndpoint {
                    tenant_id: tenant_id.to_string(),
                    instance: instance.clone(),
                })
                .await?;
        }
        self.insert(tenant_id, instance, RouteOrigin::Registered);
        Ok(())
    }

    /// `DeregisterPluginEndpoint`：有库时先删库，再从内存移除；返回库里或内存里是否有过这条。
    pub async fn deregister(&self, tenant_id: &str, plugin_id: &str) -> Result<bool> {
        let stored = match &self.store {
            Some(store) => store.delete(tenant_id, plugin_id).await?,
            None => false,
        };
        let local = self.remove(tenant_id, plugin_id);
        Ok(stored || local)
    }

    /// 从库重读全部登记：库里有的写进内存（比快照新的本地写除外），库里已没有的登记从内存删掉
    /// （配置发现的端点不动）。没有库时什么也不做。返回库里的登记数。
    pub async fn reload(&self) -> Result<usize> {
        let Some(store) = &self.store else {
            return Ok(0);
        };
        let started = Instant::now();
        let endpoints = store.list_all().await?;
        let count = endpoints.len();
        let mut seen = HashSet::with_capacity(count);
        for endpoint in endpoints {
            let key = Self::composite_key(&endpoint.tenant_id, &endpoint.instance.plugin_id);
            seen.insert(key.clone());
            if self
                .removed
                .get(&key)
                .is_some_and(|removed_at| *removed_at > started)
            {
                continue;
            }
            if self
                .inner
                .get(&key)
                .is_some_and(|entry| entry.touched > started)
            {
                continue;
            }
            self.inner.insert(
                key.clone(),
                RouteEntry {
                    instance: endpoint.instance,
                    origin: RouteOrigin::Registered,
                    touched: started,
                },
            );
            self.health.entry(key).or_default();
        }
        let gone: Vec<String> = self
            .inner
            .iter()
            .filter(|entry| {
                entry.origin == RouteOrigin::Registered
                    && entry.touched <= started
                    && !seen.contains(entry.key())
            })
            .map(|entry| entry.key().clone())
            .collect();
        for key in gone {
            self.inner
                .remove_if(&key, |_, entry| entry.touched <= started);
            if !self.inner.contains_key(&key) {
                self.health.remove(&key);
            }
        }
        // Every later snapshot already reflects these deletions.
        self.removed.retain(|_, removed_at| *removed_at > started);
        Ok(count)
    }

    /// 这些登记刚通过健康检查：批量记 `last_healthy_at`（配置发现的端点不入库，跳过）。
    pub async fn persist_health(&self, healthy: &[(String, String)]) -> Result<u64> {
        let Some(store) = &self.store else {
            return Ok(0);
        };
        let keys: Vec<PluginEndpointKey> = healthy
            .iter()
            .filter(|(tenant_id, plugin_id)| {
                self.origin(tenant_id, plugin_id) == Some(RouteOrigin::Registered)
            })
            .map(|(tenant_id, plugin_id)| PluginEndpointKey::new(tenant_id, plugin_id))
            .collect();
        if keys.is_empty() {
            return Ok(0);
        }
        store.record_healthy(&keys).await
    }

    /// 删除超过 `ttl` 既没重新登记、也没通过健康检查的登记（库与本进程内存）；其他实例在下次
    /// 重读时去掉。配置发现的端点不在库里，不受影响。返回删除的登记数。
    pub async fn cleanup_stale(&self, ttl: Duration) -> Result<usize> {
        let Some(store) = &self.store else {
            return Ok(0);
        };
        let started = Instant::now();
        // The configured TTL is at most 30 days; anything that does not convert is "never".
        let ttl =
            chrono::Duration::from_std(ttl).unwrap_or_else(|_| chrono::Duration::days(36_500));
        let cutoff = chrono::Utc::now() - ttl;
        let deleted = store.delete_stale(cutoff).await?;
        for key in &deleted {
            let key = Self::composite_key(&key.tenant_id, &key.plugin_id);
            self.inner.remove_if(&key, |_, entry| {
                entry.origin == RouteOrigin::Registered && entry.touched <= started
            });
            if !self.inner.contains_key(&key) {
                self.health.remove(&key);
            }
        }
        Ok(deleted.len())
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
        let mut instances: Vec<RegisteredPluginInstance> = self
            .inner
            .iter()
            .filter(|e| e.key().starts_with(&prefix))
            .filter(|e| Self::serves(&e.value().instance, capability_id))
            .map(|e| e.value().instance.clone())
            .collect();
        instances.sort_by(|a, b| a.plugin_id.cmp(&b.plugin_id));
        instances
    }

    pub fn list_snapshots(&self) -> Vec<PluginRouteSnapshot> {
        self.inner
            .iter()
            .filter_map(|e| {
                Self::split_key(e.key()).map(|(tenant_id, _)| PluginRouteSnapshot {
                    tenant_id: tenant_id.to_string(),
                    instance: e.value().instance.clone(),
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

    /// 内存里的登记表；`list_all` 可以先截快照、再等放行，用来复现与重读并发的写。
    #[derive(Default)]
    struct MemoryStore {
        rows: std::sync::Mutex<std::collections::BTreeMap<PluginEndpointKey, PluginEndpoint>>,
        fail: std::sync::atomic::AtomicBool,
        gate: std::sync::Mutex<Option<Arc<tokio::sync::Notify>>>,
        snapshot_taken: tokio::sync::Notify,
        healthy: std::sync::Mutex<Vec<PluginEndpointKey>>,
    }

    impl MemoryStore {
        fn check(&self) -> Result<()> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(flare_server_core::error::FlareError::system("store down"));
            }
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl PluginEndpointRepository for MemoryStore {
        async fn upsert(&self, endpoint: &PluginEndpoint) -> Result<()> {
            self.check()?;
            self.rows
                .lock()
                .unwrap()
                .insert(endpoint.key(), endpoint.clone());
            Ok(())
        }

        async fn delete(&self, tenant_id: &str, plugin_id: &str) -> Result<bool> {
            self.check()?;
            Ok(self
                .rows
                .lock()
                .unwrap()
                .remove(&PluginEndpointKey::new(tenant_id, plugin_id))
                .is_some())
        }

        async fn list_all(&self) -> Result<Vec<PluginEndpoint>> {
            self.check()?;
            let snapshot: Vec<_> = self.rows.lock().unwrap().values().cloned().collect();
            let gate = self.gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                self.snapshot_taken.notify_one();
                gate.notified().await;
            }
            Ok(snapshot)
        }

        async fn record_healthy(&self, keys: &[PluginEndpointKey]) -> Result<u64> {
            self.check()?;
            self.healthy.lock().unwrap().extend_from_slice(keys);
            Ok(keys.len() as u64)
        }

        async fn delete_stale(
            &self,
            _cutoff: chrono::DateTime<chrono::Utc>,
        ) -> Result<Vec<PluginEndpointKey>> {
            self.check()?;
            // Everything not recorded healthy counts as stale here.
            let healthy = self.healthy.lock().unwrap().clone();
            let mut rows = self.rows.lock().unwrap();
            let stale: Vec<_> = rows
                .keys()
                .filter(|key| !healthy.contains(key))
                .cloned()
                .collect();
            for key in &stale {
                rows.remove(key);
            }
            Ok(stale)
        }
    }

    fn ids(book: &PluginRouteBook, tenant: &str) -> Vec<String> {
        book.list_filtered(tenant, "")
            .into_iter()
            .map(|i| i.plugin_id)
            .collect()
    }

    #[tokio::test]
    async fn registrations_write_through_and_a_failed_write_changes_nothing() {
        let store = Arc::new(MemoryStore::default());
        let book = PluginRouteBook::with_store(store.clone());
        assert!(book.is_persistent());
        book.register("t1", instance("p1", "x", &["x"]))
            .await
            .unwrap();
        assert_eq!(store.rows.lock().unwrap().len(), 1);
        assert_eq!(book.origin("t1", "p1"), Some(RouteOrigin::Registered));

        store.fail.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(
            book.register("t1", instance("p2", "x", &["x"]))
                .await
                .is_err()
        );
        assert_eq!(ids(&book, "t1"), ["p1"]);
        assert!(book.deregister("t1", "p1").await.is_err());
        assert_eq!(ids(&book, "t1"), ["p1"]);
        assert!(book.reload().await.is_err());
        assert_eq!(ids(&book, "t1"), ["p1"]);

        store.fail.store(false, std::sync::atomic::Ordering::SeqCst);
        assert!(book.deregister("t1", "p1").await.unwrap());
        assert!(store.rows.lock().unwrap().is_empty());
        assert!(ids(&book, "t1").is_empty());
    }

    #[tokio::test]
    async fn a_reload_replaces_registrations_but_keeps_config_endpoints() {
        let store = Arc::new(MemoryStore::default());
        let book = PluginRouteBook::with_store(store.clone());
        book.upsert("t1", instance("config", "x", &[]));
        book.register("t1", instance("gone", "x", &["x"]))
            .await
            .unwrap();
        // Another instance deregistered "gone" and registered "new".
        store.rows.lock().unwrap().clear();
        store
            .upsert(&PluginEndpoint {
                tenant_id: "t1".into(),
                instance: instance("new", "x", &["x"]),
            })
            .await
            .unwrap();
        assert_eq!(book.reload().await.unwrap(), 1);
        assert_eq!(ids(&book, "t1"), ["config", "new"]);
        assert_eq!(book.origin("t1", "config"), Some(RouteOrigin::Config));
        assert_eq!(book.origin("t1", "new"), Some(RouteOrigin::Registered));
    }

    #[tokio::test]
    async fn writes_during_a_reload_are_not_undone_by_its_older_snapshot() {
        let store = Arc::new(MemoryStore::default());
        let book = Arc::new(PluginRouteBook::with_store(store.clone()));
        book.register("t1", instance("doomed", "x", &["x"]))
            .await
            .unwrap();
        let gate = Arc::new(tokio::sync::Notify::new());
        *store.gate.lock().unwrap() = Some(Arc::clone(&gate));

        // The reload snapshots ["doomed"], then waits; meanwhile "fresh" is registered and
        // "doomed" deregistered.
        let reload = tokio::spawn({
            let book = Arc::clone(&book);
            async move { book.reload().await }
        });
        store.snapshot_taken.notified().await;
        *store.gate.lock().unwrap() = None;
        book.register("t1", instance("fresh", "x", &["x"]))
            .await
            .unwrap();
        assert!(book.deregister("t1", "doomed").await.unwrap());
        gate.notify_one();
        reload.await.unwrap().unwrap();

        assert_eq!(ids(&book, "t1"), ["fresh"]);
        // The next reload sees the table as it is now.
        book.reload().await.unwrap();
        assert_eq!(ids(&book, "t1"), ["fresh"]);
    }

    #[tokio::test]
    async fn health_and_cleanup_touch_only_registrations() {
        let store = Arc::new(MemoryStore::default());
        let book = PluginRouteBook::with_store(store.clone());
        book.upsert("t1", instance("config", "x", &[]));
        book.register("t1", instance("ok", "x", &["x"]))
            .await
            .unwrap();
        book.register("t1", instance("down", "x", &["x"]))
            .await
            .unwrap();
        let recorded = book
            .persist_health(&[("t1".into(), "ok".into()), ("t1".into(), "config".into())])
            .await
            .unwrap();
        assert_eq!(recorded, 1);
        assert_eq!(
            book.cleanup_stale(Duration::from_secs(300)).await.unwrap(),
            1
        );
        assert_eq!(ids(&book, "t1"), ["config", "ok"]);

        // Without a store nothing is persisted, reloaded or cleaned.
        let local = PluginRouteBook::new();
        local.register("t1", instance("p", "x", &[])).await.unwrap();
        assert_eq!(local.reload().await.unwrap(), 0);
        assert_eq!(
            local
                .persist_health(&[("t1".into(), "p".into())])
                .await
                .unwrap(),
            0
        );
        assert_eq!(local.cleanup_stale(Duration::ZERO).await.unwrap(), 0);
        assert_eq!(ids(&local, "t1"), ["p"]);
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
