//! # Hook配置监听器
//!
//! 监听配置变更并自动重新加载配置。当前配置是一份按租户分桶的快照
//! （[`HookConfigSnapshot`]）：全局配置对所有租户生效，各租户登记的配置只用于该租户的事件。

use std::sync::Arc;
use std::time::Duration;

use flare_server_core::error::Result;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info, warn};

use crate::domain::model::HookConfig;
use crate::infrastructure::config::loader::{ConfigLoaderItem, ConfigValidator, LoadedHookConfig};
use crate::infrastructure::config::snapshot::HookConfigSnapshot;

/// 配置监听器
///
/// 监听配置变更并自动重新加载配置
pub struct ConfigWatcher {
    state: Arc<WatcherState>,
    refresh_interval: Duration,
}

struct WatcherState {
    loaders: Vec<Arc<ConfigLoaderItem>>,
    /// 各配置源最近一次成功加载的结果（与 `loaders` 一一对应）。某个源加载失败时沿用它，
    /// 一次数据库或配置中心故障不会清掉该源登记的 Hook。锁同时串行化全量与按租户的重载，
    /// 避免较早开始的全量重载覆盖较晚完成的租户重载。
    loaded: Mutex<Vec<Option<LoadedHookConfig>>>,
    current: RwLock<Arc<HookConfigSnapshot>>,
}

impl ConfigWatcher {
    pub fn new(loaders: Vec<Arc<ConfigLoaderItem>>, refresh_interval: Duration) -> Self {
        let slots = vec![None; loaders.len()];
        Self {
            state: Arc::new(WatcherState {
                loaders,
                loaded: Mutex::new(slots),
                current: RwLock::new(Arc::new(HookConfigSnapshot::default())),
            }),
            refresh_interval,
        }
    }

    /// 测试用：没有加载器、全局配置固定为 `config` 的监听器。
    #[cfg(test)]
    pub(crate) fn with_config(config: HookConfig) -> Self {
        Self::with_snapshot(HookConfigSnapshot::new(config, Default::default()))
    }

    /// 测试用：没有加载器、当前快照固定为 `snapshot` 的监听器。
    #[cfg(test)]
    pub(crate) fn with_snapshot(snapshot: HookConfigSnapshot) -> Self {
        Self {
            state: Arc::new(WatcherState {
                loaders: Vec::new(),
                loaded: Mutex::new(Vec::new()),
                current: RwLock::new(Arc::new(snapshot)),
            }),
            refresh_interval: Duration::from_secs(3600),
        }
    }

    /// 当前快照。
    pub async fn snapshot(&self) -> Arc<HookConfigSnapshot> {
        self.state.current.read().await.clone()
    }

    /// `tenant_id` 的事件实际要执行的配置（见 [`HookConfigSnapshot::for_tenant`]）。
    pub async fn config_for_tenant(&self, tenant_id: &str) -> HookConfig {
        self.snapshot().await.for_tenant(tenant_id)
    }

    /// 启动配置监听
    pub async fn start(&self) -> Result<()> {
        // 初始加载
        self.reload().await?;

        // 启动定时刷新任务
        let state = Arc::clone(&self.state);
        let interval = self.refresh_interval;

        tokio::spawn(async move {
            let mut interval_timer =
                tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            loop {
                interval_timer.tick().await;
                match state.reload_all().await {
                    Ok(()) => info!("Hook config reloaded successfully"),
                    Err(e) => error!(error = %e, "Failed to reload hook config"),
                }
            }
        });

        Ok(())
    }

    /// 重新加载所有配置源
    pub async fn reload(&self) -> Result<()> {
        self.state.reload_all().await
    }

    /// 只重新加载一个租户自己登记的配置（`CapabilityService/Administer` 写入后调用）。
    ///
    /// 没有按租户分桶的配置源（例如设了 `TENANT_ID` 的单租户进程）时改为全量重载。该租户的
    /// 配置加载或校验失败时返回错误，快照不变。
    pub async fn reload_tenant(&self, tenant_id: &str) -> Result<()> {
        self.state.reload_tenant(tenant_id).await
    }
}

impl WatcherState {
    async fn reload_all(&self) -> Result<()> {
        let mut loaded = self.loaded.lock().await;
        let mut next = loaded.clone();
        for (slot, loader) in next.iter_mut().zip(&self.loaders) {
            match loader.load().await {
                Ok(mut fresh) => {
                    fresh.inherit_failed_tenants(slot.as_ref());
                    *slot = Some(fresh);
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        "Failed to load config from loader; keeping its last loaded hooks"
                    );
                }
            }
        }
        self.publish(&mut loaded, next).await
    }

    async fn reload_tenant(&self, tenant_id: &str) -> Result<()> {
        let mut loaded = self.loaded.lock().await;
        let mut next = loaded.clone();
        let mut scoped = false;
        for (slot, loader) in next.iter_mut().zip(&self.loaders) {
            let Some(result) = loader.load_tenant(tenant_id).await else {
                continue;
            };
            scoped = true;
            let config = result?;
            ConfigValidator::validate(&config)?;
            let source = slot.get_or_insert_with(LoadedHookConfig::default);
            source.failed_tenants.remove(tenant_id);
            if config.is_empty() {
                source.tenants.remove(tenant_id);
            } else {
                source.tenants.insert(tenant_id.to_owned(), config);
            }
        }
        if !scoped {
            drop(loaded);
            return self.reload_all().await;
        }
        self.publish(&mut loaded, next).await
    }

    /// 由 `next` 合成新快照并替换当前快照；合成失败（全局配置无效）时什么都不改。
    async fn publish(
        &self,
        loaded: &mut Vec<Option<LoadedHookConfig>>,
        next: Vec<Option<LoadedHookConfig>>,
    ) -> Result<()> {
        let previous = self.current.read().await.clone();
        let snapshot = HookConfigSnapshot::build(&next, &previous)?;
        *loaded = next;
        *self.current.write().await = Arc::new(snapshot);
        Ok(())
    }
}
