//! 租户运行时缓存：网关 / ingest / sync 读取控制面投影（`tenants` 表）的唯一入口。
//!
//! - 进程内、按租户缓存 `{status, quota, version}` 快照；每租户每个刷新周期（默认 1s）最多
//!   回源一次，同一租户的并发未命中合并为一次查询。
//! - 有界（默认 1024 个租户），超出按 LRU 淘汰；未知租户与回源失败同样按周期缓存，
//!   避免热路径反复打库。
//! - 回源是一张单行小表的点查，读整行与「只查版本」代价相同，所以直接以整行替代版本轮询。
//! - 没有配置数据库时为 `disabled`：所有查询返回 [`TenantLookup::Unknown`]，由调用方按
//!   [`TenantAccessPolicy`] 决定放行或拒绝；限流回落全局配置。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use flare_im_contracts::domain::tenant::{
    TenantCoreQuota, TenantQuota, TenantRuntimeSnapshot, TenantStatus,
};
use flare_im_contracts::utils::normalize_tenant_id;
use moka::future::Cache;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

/// 未知租户（本地没有投影）时的接入策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TenantAccessPolicy {
    /// 放行：单租户 / 开发部署无需先推投影。
    #[default]
    Lenient,
    /// 拒绝：生产环境要求每个租户都先经控制面投影；回源失败同样拒绝（fail-closed）。
    Strict,
}

impl TenantAccessPolicy {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "lenient" => Some(TenantAccessPolicy::Lenient),
            "strict" => Some(TenantAccessPolicy::Strict),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            TenantAccessPolicy::Lenient => "lenient",
            TenantAccessPolicy::Strict => "strict",
        }
    }

    /// 从环境变量读取；未设置返回 `None`，设置了但非法返回 `Err`。
    pub fn from_env(key: &str) -> Result<Option<Self>, String> {
        match std::env::var(key) {
            Ok(raw) if !raw.trim().is_empty() => Self::parse(&raw)
                .map(Some)
                .ok_or_else(|| format!("{key} must be `lenient` or `strict`, got `{raw}`")),
            _ => Ok(None),
        }
    }
}

/// 一次查询的结果。
#[derive(Debug, Clone)]
pub enum TenantLookup {
    /// 本地有投影。
    Known(Arc<TenantRuntimeSnapshot>),
    /// 本地没有这个租户的投影（或缓存未启用）。
    Unknown,
    /// 回源失败（数据库不可达等）；调用方按策略处理：宽松放行，严格拒绝。
    Unavailable,
}

impl TenantLookup {
    pub fn snapshot(&self) -> Option<&TenantRuntimeSnapshot> {
        match self {
            TenantLookup::Known(snapshot) => Some(snapshot),
            _ => None,
        }
    }

    /// 是否允许建立连接：已知租户看状态，未知 / 不可用按策略。
    pub fn admits_connection(&self, policy: TenantAccessPolicy) -> bool {
        match self {
            TenantLookup::Known(snapshot) => snapshot.status.admits_connections(),
            TenantLookup::Unknown | TenantLookup::Unavailable => {
                matches!(policy, TenantAccessPolicy::Lenient)
            }
        }
    }

    /// 核侧运行时配额；未知 / 不可用时全部为 0（回落全局配置）。
    pub fn core_quota(&self) -> TenantCoreQuota {
        self.snapshot()
            .map(TenantRuntimeSnapshot::core_quota)
            .unwrap_or_default()
    }
}

/// 回源接口：默认实现读 PostgreSQL `tenants` 表；测试可注入内存实现。
#[async_trait]
pub trait TenantRuntimeSource: Send + Sync + 'static {
    async fn load(&self, tenant_id: &str) -> Result<Option<TenantRuntimeSnapshot>, String>;
}

/// PostgreSQL 回源：`SELECT status, quota, version FROM tenants WHERE tenant_id = $1`。
pub struct PgTenantRuntimeSource {
    pool: PgPool,
}

impl PgTenantRuntimeSource {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TenantRuntimeSource for PgTenantRuntimeSource {
    async fn load(&self, tenant_id: &str) -> Result<Option<TenantRuntimeSnapshot>, String> {
        let row = sqlx::query(
            "SELECT status, quota::text AS quota, version FROM tenants WHERE tenant_id = $1",
        )
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("tenants lookup failed: {e}"))?;

        let Some(row) = row else {
            return Ok(None);
        };
        let status_raw: String = row
            .try_get("status")
            .map_err(|e| format!("tenants.status: {e}"))?;
        let quota_raw: Option<String> = row
            .try_get("quota")
            .map_err(|e| format!("tenants.quota: {e}"))?;
        let version: i64 = row
            .try_get("version")
            .map_err(|e| format!("tenants.version: {e}"))?;

        // 状态列受 CHECK 约束；万一出现未知值按「不可接入」处理，绝不因为解析失败而放行。
        let status = TenantStatus::parse(&status_raw).unwrap_or_else(|| {
            tracing::warn!(tenant_id, status = %status_raw, "unknown tenants.status; treating as suspended");
            TenantStatus::Suspended
        });
        let quota = quota_raw
            .as_deref()
            .filter(|raw| !raw.trim().is_empty())
            .map(serde_json::from_str::<TenantQuota>)
            .transpose()
            .unwrap_or_else(|e| {
                tracing::warn!(tenant_id, error = %e, "tenants.quota is not the expected JSON shape; ignoring quota");
                None
            })
            .unwrap_or_default();

        Ok(Some(TenantRuntimeSnapshot {
            tenant_id: tenant_id.to_string(),
            status,
            quota,
            version: u64::try_from(version).unwrap_or(0),
        }))
    }
}

/// 缓存参数。
#[derive(Debug, Clone, Copy)]
pub struct TenantRuntimeCacheOptions {
    /// 每租户回源的最小间隔（快照存活期）。
    pub refresh_interval: Duration,
    /// 缓存的租户数上限。
    pub capacity: u64,
}

impl Default for TenantRuntimeCacheOptions {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(1),
            capacity: 1024,
        }
    }
}

struct Inner {
    source: Arc<dyn TenantRuntimeSource>,
    cache: Cache<String, TenantLookup>,
}

/// 进程内租户运行时缓存（`Clone` 共享同一份状态）。
#[derive(Clone)]
pub struct TenantRuntimeCache {
    inner: Option<Arc<Inner>>,
}

impl TenantRuntimeCache {
    /// 未配置数据库：永远返回 [`TenantLookup::Unknown`]。
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self::with_source(
            Arc::new(PgTenantRuntimeSource::new(pool)),
            TenantRuntimeCacheOptions::default(),
        )
    }

    pub fn with_source(
        source: Arc<dyn TenantRuntimeSource>,
        options: TenantRuntimeCacheOptions,
    ) -> Self {
        let cache = Cache::builder()
            .max_capacity(options.capacity.max(1))
            .time_to_live(options.refresh_interval.max(Duration::from_millis(1)))
            .build();
        Self {
            inner: Some(Arc::new(Inner { source, cache })),
        }
    }

    /// 用连接串建一个小连接池（缓存每租户每秒最多一次点查，2 个连接足够）。
    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self, String> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections.max(1))
            .min_connections(0)
            .acquire_timeout(Duration::from_secs(5))
            .idle_timeout(Duration::from_secs(300))
            .connect(database_url)
            .await
            .map_err(|e| format!("tenant runtime postgres connect failed: {e}"))?;
        Ok(Self::from_pool(pool))
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// 查询租户快照；`tenant_id` 先按 [`normalize_tenant_id`] 归一（`""` / `"default"` → `"0"`）。
    pub async fn lookup(&self, tenant_id: &str) -> TenantLookup {
        let Some(inner) = self.inner.as_ref() else {
            return TenantLookup::Unknown;
        };
        let key = normalize_tenant_id(tenant_id);
        let source = Arc::clone(&inner.source);
        let load_key = key.clone();
        inner
            .cache
            .get_with(key, async move {
                match source.load(&load_key).await {
                    Ok(Some(snapshot)) => TenantLookup::Known(Arc::new(snapshot)),
                    Ok(None) => TenantLookup::Unknown,
                    Err(error) => {
                        tracing::warn!(tenant_id = %load_key, %error, "tenant runtime lookup failed");
                        TenantLookup::Unavailable
                    }
                }
            })
            .await
    }

    /// 核侧运行时配额（未知 / 不可用 → 全 0）。
    pub async fn core_quota(&self, tenant_id: &str) -> TenantCoreQuota {
        self.lookup(tenant_id).await.core_quota()
    }

    /// 让淘汰 / 过期立即生效（测试与诊断用）。
    pub async fn run_pending_tasks(&self) {
        if let Some(inner) = self.inner.as_ref() {
            inner.cache.run_pending_tasks().await;
        }
    }

    /// 当前缓存的租户数（含未知 / 不可用条目）。
    pub fn entry_count(&self) -> u64 {
        self.inner
            .as_ref()
            .map(|inner| inner.cache.entry_count())
            .unwrap_or(0)
    }
}

impl std::fmt::Debug for TenantRuntimeCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantRuntimeCache")
            .field("enabled", &self.is_enabled())
            .field("entries", &self.entry_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MemorySource {
        rows: Mutex<HashMap<String, TenantRuntimeSnapshot>>,
        loads: AtomicUsize,
        fail: std::sync::atomic::AtomicBool,
    }

    impl MemorySource {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                rows: Mutex::new(HashMap::new()),
                loads: AtomicUsize::new(0),
                fail: std::sync::atomic::AtomicBool::new(false),
            })
        }

        fn put(&self, snapshot: TenantRuntimeSnapshot) {
            self.rows
                .lock()
                .unwrap()
                .insert(snapshot.tenant_id.clone(), snapshot);
        }

        fn loads(&self) -> usize {
            self.loads.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl TenantRuntimeSource for MemorySource {
        async fn load(&self, tenant_id: &str) -> Result<Option<TenantRuntimeSnapshot>, String> {
            self.loads.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                return Err("db down".to_string());
            }
            Ok(self.rows.lock().unwrap().get(tenant_id).cloned())
        }
    }

    fn snapshot(tenant_id: &str, status: TenantStatus, send_qps: u32) -> TenantRuntimeSnapshot {
        TenantRuntimeSnapshot {
            tenant_id: tenant_id.to_string(),
            status,
            quota: TenantQuota {
                core: TenantCoreQuota {
                    send_qps,
                    ..Default::default()
                },
                ..Default::default()
            },
            version: 1,
        }
    }

    fn cache(source: &Arc<MemorySource>, ttl: Duration, capacity: u64) -> TenantRuntimeCache {
        TenantRuntimeCache::with_source(
            source.clone(),
            TenantRuntimeCacheOptions {
                refresh_interval: ttl,
                capacity,
            },
        )
    }

    #[tokio::test]
    async fn at_most_one_load_per_refresh_interval_per_tenant() {
        let source = MemorySource::new();
        source.put(snapshot("t1", TenantStatus::Active, 7));
        let cache = cache(&source, Duration::from_millis(80), 1024);

        for _ in 0..5 {
            let lookup = cache.lookup("t1").await;
            assert_eq!(lookup.core_quota().send_qps, 7);
        }
        assert_eq!(
            source.loads(),
            1,
            "repeated lookups inside the interval hit memory"
        );

        source.put(snapshot("t1", TenantStatus::Suspended, 9));
        assert!(
            cache
                .lookup("t1")
                .await
                .admits_connection(TenantAccessPolicy::Strict)
        );
        tokio::time::sleep(Duration::from_millis(120)).await;
        let refreshed = cache.lookup("t1").await;
        assert_eq!(
            source.loads(),
            2,
            "after the interval the row is read again"
        );
        assert!(!refreshed.admits_connection(TenantAccessPolicy::Lenient));
        assert_eq!(refreshed.core_quota().send_qps, 9);
    }

    #[tokio::test]
    async fn unknown_tenant_is_cached_and_short_circuits() {
        let source = MemorySource::new();
        let cache = cache(&source, Duration::from_secs(5), 1024);

        for _ in 0..3 {
            assert!(matches!(cache.lookup("ghost").await, TenantLookup::Unknown));
        }
        assert_eq!(source.loads(), 1);
        assert!(
            cache
                .lookup("ghost")
                .await
                .admits_connection(TenantAccessPolicy::Lenient)
        );
        assert!(
            !cache
                .lookup("ghost")
                .await
                .admits_connection(TenantAccessPolicy::Strict)
        );
        assert_eq!(
            cache.lookup("ghost").await.core_quota(),
            TenantCoreQuota::default()
        );
    }

    #[tokio::test]
    async fn source_failure_is_unavailable_and_fails_closed_under_strict() {
        let source = MemorySource::new();
        source.fail.store(true, Ordering::SeqCst);
        let cache = cache(&source, Duration::from_secs(5), 1024);

        let lookup = cache.lookup("t1").await;
        assert!(matches!(lookup, TenantLookup::Unavailable));
        assert!(lookup.admits_connection(TenantAccessPolicy::Lenient));
        assert!(!lookup.admits_connection(TenantAccessPolicy::Strict));
        let _ = cache.lookup("t1").await;
        assert_eq!(
            source.loads(),
            1,
            "failures are cached for the interval too"
        );
    }

    #[tokio::test]
    async fn tenant_id_is_normalized_before_lookup() {
        let source = MemorySource::new();
        source.put(snapshot("0", TenantStatus::Active, 3));
        let cache = cache(&source, Duration::from_secs(5), 1024);

        assert_eq!(cache.lookup("").await.core_quota().send_qps, 3);
        assert_eq!(cache.lookup("default").await.core_quota().send_qps, 3);
        assert_eq!(cache.lookup(" 0 ").await.core_quota().send_qps, 3);
        assert_eq!(source.loads(), 1);
    }

    #[tokio::test]
    async fn capacity_is_bounded() {
        let source = MemorySource::new();
        let cache = cache(&source, Duration::from_secs(60), 64);
        for i in 0..1000 {
            let _ = cache.lookup(&format!("t{i}")).await;
        }
        cache.run_pending_tasks().await;
        assert!(cache.entry_count() <= 64, "entries={}", cache.entry_count());
    }

    #[tokio::test]
    async fn disabled_cache_never_loads() {
        let cache = TenantRuntimeCache::disabled();
        assert!(!cache.is_enabled());
        assert!(matches!(cache.lookup("t1").await, TenantLookup::Unknown));
        assert_eq!(cache.core_quota("t1").await, TenantCoreQuota::default());
    }

    #[test]
    fn policy_parses_case_insensitively() {
        assert_eq!(
            TenantAccessPolicy::parse("Strict"),
            Some(TenantAccessPolicy::Strict)
        );
        assert_eq!(
            TenantAccessPolicy::parse(" lenient "),
            Some(TenantAccessPolicy::Lenient)
        );
        assert_eq!(TenantAccessPolicy::parse("open"), None);
    }
}
