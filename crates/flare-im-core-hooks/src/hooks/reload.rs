//! 数据库配置热加载：约每秒查一次有没有范围变了，只取变了的范围的行，换指针。
//!
//! 启动时读不到库或有坏行就报错，服务不就绪：装了一半的必需 Hook 比没启动更危险。运行中读不到库就
//! 继续用上一份并告警；某个范围有坏行时只有这个范围留在上一份。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use flare_server_core::error::Result;
use tokio::sync::oneshot;

use super::config::HookFactory;
use super::registry::{HookRegistry, HookScope, PreSendPlan};
use super::stored::StoredHook;

/// 查版本号的间隔。
pub const HOOK_CONFIG_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// 持续读不到库时告警的最小间隔，期间的失败次数在下一条里补记。
const FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// 一个范围变到的版本。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookScopeChange {
    pub scope: HookScope,
    pub version: i64,
}

/// 配置库。
#[async_trait]
pub trait HookConfigStore: Send + Sync {
    /// 当前最新版本（没写过为 0）。
    async fn current_version(&self) -> Result<i64>;

    /// 版本晚于 `after` 的范围。
    async fn changed_scopes(&self, after: i64) -> Result<Vec<HookScopeChange>>;

    /// 启用的行；`scopes` 为 `None` 时取全部。
    async fn load(&self, scopes: Option<&[HookScope]>) -> Result<Vec<StoredHook>>;
}

#[derive(Default)]
struct FailureLog {
    last: Option<Instant>,
    suppressed: u64,
}

impl FailureLog {
    fn failed(&mut self, error: &dyn std::fmt::Display) {
        let now = Instant::now();
        if self
            .last
            .is_some_and(|last| now.duration_since(last) < FAILURE_LOG_INTERVAL)
        {
            self.suppressed += 1;
            return;
        }
        tracing::warn!(
            %error,
            suppressed_failures = self.suppressed,
            "hook configuration unavailable; keeping the last good configuration"
        );
        self.last = Some(now);
        self.suppressed = 0;
    }

    fn recovered(&mut self) {
        if self.last.take().is_some() {
            tracing::info!("hook configuration readable again");
        }
        self.suppressed = 0;
    }
}

/// 把库里的 Hook 物化进注册表。
pub struct HookConfigReloader {
    store: Arc<dyn HookConfigStore>,
    registry: Arc<HookRegistry>,
    factory: Arc<dyn HookFactory>,
    /// `FLARE_REGISTRY_MODE=static`：`discovery://` 目标解析不了，算坏行。
    static_routes_only: bool,
    version: i64,
    failures: FailureLog,
}

impl HookConfigReloader {
    pub fn new(
        store: Arc<dyn HookConfigStore>,
        registry: Arc<HookRegistry>,
        factory: Arc<dyn HookFactory>,
        static_routes_only: bool,
    ) -> Self {
        Self {
            store,
            registry,
            factory,
            static_routes_only,
            version: 0,
            failures: FailureLog::default(),
        }
    }

    /// 已加载到的版本。
    pub fn version(&self) -> i64 {
        self.version
    }

    fn plan(&self, row: &StoredHook) -> Result<PreSendPlan> {
        let definition = row.definition()?;
        if self.static_routes_only {
            definition.validate_static_route()?;
        }
        let mut plan = definition.plan(row.source(), self.factory.as_ref())?;
        plan.metadata.config_id = Some(row.id);
        Ok(plan)
    }

    /// 全量加载。先读版本再读行：其间的写入下一轮还会读到，重读一个范围没有副作用。
    pub async fn load_initial(&mut self) -> Result<()> {
        let version = self.store.current_version().await?;
        let rows = self.store.load(None).await?;
        let mut stored: HashMap<HookScope, Vec<PreSendPlan>> = HashMap::new();
        for row in &rows {
            stored.entry(row.scope()).or_default().push(self.plan(row)?);
        }
        self.registry.replace_stored(stored);
        self.version = version;
        tracing::info!(version, hooks = rows.len(), "database hooks loaded");
        Ok(())
    }

    /// 查一次版本，有变化就换指针。
    pub async fn refresh(&mut self) {
        let changes = match self.store.changed_scopes(self.version).await {
            Ok(changes) => changes,
            Err(error) => return self.failures.failed(&error),
        };
        if changes.is_empty() {
            self.failures.recovered();
            return;
        }
        let scopes: Vec<HookScope> = changes
            .iter()
            .map(|change| change.scope.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let rows = match self.store.load(Some(&scopes)).await {
            Ok(rows) => rows,
            Err(error) => return self.failures.failed(&error),
        };
        self.failures.recovered();

        let mut replaced: HashMap<HookScope, Vec<PreSendPlan>> = scopes
            .iter()
            .map(|scope| (scope.clone(), Vec::new()))
            .collect();
        let mut invalid = HashSet::new();
        for row in &rows {
            let scope = row.scope();
            match self.plan(row) {
                Ok(plan) => replaced.entry(scope).or_default().push(plan),
                Err(error) => {
                    tracing::warn!(
                        %error,
                        row_id = row.id,
                        "invalid hook row; this scope keeps its last good hooks"
                    );
                    invalid.insert(scope);
                }
            }
        }
        replaced.retain(|scope, _| !invalid.contains(scope));
        let reloaded = replaced.len();
        self.registry.replace_scopes(replaced);
        // 坏行不重试：它要等下一次写入才会变，写入带来新版本。
        self.version = changes
            .iter()
            .map(|change| change.version)
            .max()
            .unwrap_or(self.version);
        tracing::info!(
            version = self.version,
            reloaded,
            "hook configuration reloaded"
        );
    }

    /// 轮询到停机。
    pub async fn run(mut self, mut shutdown: oneshot::Receiver<()>) {
        let mut ticker = tokio::time::interval(HOOK_CONFIG_POLL_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = &mut shutdown => return,
                _ = ticker.tick() => self.refresh().await,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use flare_server_core::context::Context;
    use flare_server_core::error::{ErrorBuilder, ErrorCode};
    use serde_json::{Value, json};

    use super::*;
    use crate::Ctx;
    use crate::hooks::adapters::DefaultHookFactory;

    #[derive(Default)]
    struct FakeStore {
        version: Mutex<i64>,
        changes: Mutex<Vec<HookScopeChange>>,
        rows: Mutex<Vec<StoredHook>>,
        down: AtomicBool,
    }

    impl FakeStore {
        fn write(&self, scope: HookScope, rows: Vec<StoredHook>) {
            let mut version = self.version.lock().unwrap();
            *version += 1;
            self.changes.lock().unwrap().push(HookScopeChange {
                scope: scope.clone(),
                version: *version,
            });
            let mut stored = self.rows.lock().unwrap();
            stored.retain(|row| row.scope() != scope);
            stored.extend(rows);
        }

        fn check(&self) -> Result<()> {
            if self.down.load(Ordering::Relaxed) {
                return Err(ErrorBuilder::new(ErrorCode::DatabaseError, "down").build_error());
            }
            Ok(())
        }
    }

    #[async_trait]
    impl HookConfigStore for FakeStore {
        async fn current_version(&self) -> Result<i64> {
            self.check()?;
            Ok(*self.version.lock().unwrap())
        }

        async fn changed_scopes(&self, after: i64) -> Result<Vec<HookScopeChange>> {
            self.check()?;
            Ok(self
                .changes
                .lock()
                .unwrap()
                .iter()
                .filter(|change| change.version > after)
                .cloned()
                .collect())
        }

        async fn load(&self, scopes: Option<&[HookScope]>) -> Result<Vec<StoredHook>> {
            self.check()?;
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .filter(|row| scopes.is_none_or(|scopes| scopes.contains(&row.scope())))
                .cloned()
                .collect())
        }
    }

    fn webhook() -> Value {
        json!({"type": "webhook", "endpoint": "http://127.0.0.1:9/hook"})
    }

    fn row(id: i64, tenant: Option<&str>, name: &str, transport: Value) -> StoredHook {
        StoredHook {
            id,
            tenant_id: tenant.map(str::to_string),
            name: name.into(),
            description: None,
            order: 0,
            timeout_ms: 100,
            require_success: true,
            selector: json!({}),
            transport,
            metadata: None,
        }
    }

    fn reloader(store: &Arc<FakeStore>) -> (HookConfigReloader, Arc<HookRegistry>) {
        let registry = HookRegistry::new();
        let reloader = HookConfigReloader::new(
            Arc::clone(store) as Arc<dyn HookConfigStore>,
            Arc::clone(&registry),
            Arc::new(DefaultHookFactory::new().unwrap()),
            false,
        );
        (reloader, registry)
    }

    fn names(registry: &HookRegistry, tenant: &str) -> Vec<String> {
        let ctx: Ctx = Arc::new(Context::with_request_id("req").with_tenant_id(tenant));
        registry
            .plan_pre_send(&ctx)
            .iter()
            .map(|plan| plan.metadata().name.to_string())
            .collect()
    }

    /// 全量加载后，写入只换变了的范围；删光的范围回到共享链。
    #[tokio::test]
    async fn changed_scopes_are_swapped_without_touching_the_others() {
        let store = Arc::new(FakeStore::default());
        store.write(
            HookScope::Platform,
            vec![row(1, None, "platform", webhook())],
        );
        store.write(
            HookScope::Application("a".into()),
            vec![row(2, Some("a"), "app-a", webhook())],
        );
        let (mut reloader, registry) = reloader(&store);
        reloader.load_initial().await.unwrap();
        assert_eq!(reloader.version(), 2);
        assert_eq!(names(&registry, "a"), vec!["app-a", "platform"]);
        assert_eq!(names(&registry, "b"), vec!["platform"]);

        store.write(
            HookScope::Application("b".into()),
            vec![row(3, Some("b"), "app-b", webhook())],
        );
        reloader.refresh().await;
        assert_eq!(reloader.version(), 3);
        assert_eq!(names(&registry, "b"), vec!["app-b", "platform"]);
        assert_eq!(names(&registry, "a"), vec!["app-a", "platform"]);

        store.write(HookScope::Application("a".into()), Vec::new());
        reloader.refresh().await;
        assert_eq!(names(&registry, "a"), vec!["platform"]);
    }

    /// 启动时读不到库或有坏行：报错，不装半份配置。
    #[tokio::test]
    async fn startup_fails_when_the_database_or_a_row_cannot_be_read() {
        let store = Arc::new(FakeStore::default());
        store.down.store(true, Ordering::Relaxed);
        let (mut reloader_down, _) = reloader(&store);
        assert!(reloader_down.load_initial().await.is_err());
        store.down.store(false, Ordering::Relaxed);

        store.write(
            HookScope::Application("a".into()),
            vec![row(1, Some("a"), "bad", json!({"type": "grpc"}))],
        );
        let (mut reloader_bad, registry) = reloader(&store);
        assert!(reloader_bad.load_initial().await.is_err());
        assert!(names(&registry, "a").is_empty());
    }

    /// 运行中读不到库：继续用上一份，恢复后补上。
    #[tokio::test]
    async fn a_database_outage_keeps_the_last_good_configuration() {
        let store = Arc::new(FakeStore::default());
        store.write(
            HookScope::Application("a".into()),
            vec![row(1, Some("a"), "first", webhook())],
        );
        let (mut reloader, registry) = reloader(&store);
        reloader.load_initial().await.unwrap();

        store.write(
            HookScope::Application("a".into()),
            vec![row(2, Some("a"), "second", webhook())],
        );
        store.down.store(true, Ordering::Relaxed);
        reloader.refresh().await;
        assert_eq!(names(&registry, "a"), vec!["first"]);
        assert_eq!(reloader.version(), 1, "retried after the outage");

        store.down.store(false, Ordering::Relaxed);
        reloader.refresh().await;
        assert_eq!(names(&registry, "a"), vec!["second"]);
    }

    /// 运行中出现坏行：只有这个范围留在上一份，别的范围照常换。
    #[tokio::test]
    async fn a_bad_row_keeps_only_its_own_scope_on_the_last_good_hooks() {
        let store = Arc::new(FakeStore::default());
        store.write(
            HookScope::Application("a".into()),
            vec![row(1, Some("a"), "good-a", webhook())],
        );
        let (mut reloader, registry) = reloader(&store);
        reloader.load_initial().await.unwrap();

        store.write(
            HookScope::Application("a".into()),
            vec![row(2, Some("a"), "bad-a", json!({"type": "grpc"}))],
        );
        store.write(
            HookScope::Application("b".into()),
            vec![row(3, Some("b"), "good-b", webhook())],
        );
        reloader.refresh().await;
        assert_eq!(names(&registry, "a"), vec!["good-a"]);
        assert_eq!(names(&registry, "b"), vec!["good-b"]);
        assert_eq!(reloader.version(), 3);
    }

    /// 静态注册模式下 `discovery://` 目标解析不了，算坏行。
    #[tokio::test]
    async fn static_registry_mode_rejects_discovery_rows() {
        let store = Arc::new(FakeStore::default());
        store.write(
            HookScope::Platform,
            vec![row(
                1,
                None,
                "by-service",
                json!({"type": "grpc", "service_name": "risk-hook"}),
            )],
        );
        let registry = HookRegistry::new();
        let mut reloader = HookConfigReloader::new(
            Arc::clone(&store) as Arc<dyn HookConfigStore>,
            registry,
            Arc::new(DefaultHookFactory::new().unwrap()),
            true,
        );
        assert!(reloader.load_initial().await.is_err());
    }
}
