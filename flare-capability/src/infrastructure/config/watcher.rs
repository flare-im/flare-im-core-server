//! # Hook配置监听器
//!
//! 监听配置变更并自动重新加载配置。当前配置是一份按租户分桶的快照
//! （[`HookConfigSnapshot`]）：全局配置对所有租户生效，各租户登记的配置只用于该租户的事件。
//!
//! 启动时本进程自己的 Hook 文件加载失败，[`ConfigWatcher::start`] 返回错误、Capability 不启动；
//! 启动时从数据库或配置中心加载失败只记告警、等下一次刷新。运行中的重载（定时刷新、跨实例通知、
//! Administer 写入后的全量重载）不会因为哪个配置源出错而失败或退出：失败的源沿用它上一次加载
//! 成功的结果并记日志。

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
    /// 有配置源把租户的行与全局行分开加载（多租户进程）。只有这时全局配置才不含租户的行，
    /// 全局 Hook 的名字才为平台保留（见 [`ConfigWatcher::reserves_global_name`]）。
    buckets_by_tenant: bool,
    /// 各配置源最近一次成功加载的结果（与 `loaders` 一一对应）。某个源加载失败时沿用它，
    /// 一次数据库或配置中心故障不会清掉该源登记的 Hook。锁同时串行化全量与按租户的重载，
    /// 避免较早开始的全量重载覆盖较晚完成的租户重载。
    loaded: Mutex<Vec<Option<LoadedHookConfig>>>,
    current: RwLock<Arc<HookConfigSnapshot>>,
}

impl ConfigWatcher {
    pub fn new(loaders: Vec<Arc<ConfigLoaderItem>>, refresh_interval: Duration) -> Self {
        let slots = vec![None; loaders.len()];
        let buckets_by_tenant = loaders.iter().any(|loader| loader.buckets_by_tenant());
        Self {
            state: Arc::new(WatcherState {
                loaders,
                buckets_by_tenant,
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

    /// 测试用：没有加载器、当前快照固定为 `snapshot` 的监听器（按多租户进程对待）。
    #[cfg(test)]
    pub(crate) fn with_snapshot(snapshot: HookConfigSnapshot) -> Self {
        Self {
            state: Arc::new(WatcherState {
                loaders: Vec::new(),
                buckets_by_tenant: true,
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

    /// 租户不能登记 `hook_type` 类型、名为 `name` 的 Hook：多租户进程里该名字已被当前的
    /// 全局配置占用。固定了 `TENANT_ID` 的进程全局配置里含该租户自己的行，不做这项限制
    /// （行为不变）。
    pub async fn reserves_global_name(&self, hook_type: &str, name: &str) -> bool {
        self.state.buckets_by_tenant && self.snapshot().await.is_global_hook(hook_type, name)
    }

    /// 是否按租户分桶（多租户进程）。
    pub fn buckets_by_tenant(&self) -> bool {
        self.state.buckets_by_tenant
    }

    /// 初始加载并启动定时刷新。本进程自己的 Hook 文件加载失败（见 `FileConfigLoader`）或全局
    /// 配置校验不过时返回错误，Capability 拒绝启动。
    pub async fn start(&self) -> Result<()> {
        self.state.reload_all(LoadPhase::Startup).await?;

        // 启动定时刷新任务
        let state = Arc::clone(&self.state);
        let interval = self.refresh_interval;

        tokio::spawn(async move {
            let mut interval_timer =
                tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            loop {
                interval_timer.tick().await;
                state.refresh().await;
            }
        });

        Ok(())
    }

    /// 重新加载所有配置源。某个源加载失败时沿用它上一次的结果（记日志），不算失败；只有合成的
    /// 全局配置校验不过时返回错误，快照不变。
    pub async fn reload(&self) -> Result<()> {
        self.state.reload_all(LoadPhase::Refresh).await.map(drop)
    }

    /// 只重新加载一个租户自己登记的配置（`CapabilityService/Administer` 写入后调用）。
    ///
    /// 没有按租户分桶的配置源（例如设了 `TENANT_ID` 的单租户进程）时改为全量重载。该租户的
    /// 配置加载或校验失败时返回错误，快照不变。
    pub async fn reload_tenant(&self, tenant_id: &str) -> Result<()> {
        self.state.reload_tenant(tenant_id).await
    }
}

/// 一次全量加载发生在启动时还是运行中（见模块说明）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoadPhase {
    Startup,
    Refresh,
}

impl WatcherState {
    /// 加载全部配置源并发布新快照，返回这次加载失败、沿用上一次结果的源数。启动时
    /// 必需的源（本进程自己的 Hook 文件）失败则直接返回错误，什么都不发布。
    async fn reload_all(&self, phase: LoadPhase) -> Result<usize> {
        let mut loaded = self.loaded.lock().await;
        let mut next = loaded.clone();
        let mut failed_sources = 0;
        for (slot, loader) in next.iter_mut().zip(&self.loaders) {
            match loader.load().await {
                Ok(mut fresh) => {
                    fresh.inherit_failed_tenants(slot.as_ref());
                    *slot = Some(fresh);
                }
                Err(e) if phase == LoadPhase::Startup && loader.required_at_startup() => {
                    return Err(e);
                }
                Err(e) => {
                    failed_sources += 1;
                    if loader.required_at_startup() {
                        // 运行中改坏了本进程的 Hook 文件：服务照常运行，文件里的 Hook 停在上一次
                        // 加载成功的版本，直到文件改好。
                        error!(
                            source = loader.source_name(),
                            error = %e,
                            "Failed to reload hook config source; keeping its last loaded hooks"
                        );
                    } else {
                        warn!(
                            source = loader.source_name(),
                            error = %e,
                            "Failed to load config from loader; keeping its last loaded hooks"
                        );
                    }
                }
            }
        }
        self.publish(&mut loaded, next).await?;
        Ok(failed_sources)
    }

    /// 定时刷新一次：出错只记日志，当前快照中出错的部分不变，运行中的服务不因此退出。
    async fn refresh(&self) {
        match self.reload_all(LoadPhase::Refresh).await {
            Ok(0) => info!("Hook config reloaded successfully"),
            Ok(failed_sources) => warn!(
                failed_sources,
                "Hook config reloaded; failed sources keep their last loaded hooks"
            ),
            Err(e) => error!(error = %e, "Failed to reload hook config; keeping the current hooks"),
        }
    }

    async fn reload_tenant(&self, tenant_id: &str) -> Result<()> {
        let mut loaded = self.loaded.lock().await;
        let mut next = loaded.clone();
        let mut scoped = false;
        let mut hooks_count = 0;
        for (slot, loader) in next.iter_mut().zip(&self.loaders) {
            let Some(result) = loader.load_tenant(tenant_id).await else {
                continue;
            };
            scoped = true;
            let config = result?;
            ConfigValidator::validate(&config)?;
            hooks_count += config.hook_count();
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
            return self.reload_all(LoadPhase::Refresh).await.map(drop);
        }
        self.publish(&mut loaded, next).await?;
        // 只含该租户自己登记的、启用的 Hook（不含全局配置）。
        info!(
            tenant_id = %tenant_id,
            hooks_count,
            "Reloaded tenant hook config"
        );
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::config::loader::test_support::{
        SECRET, TempDir, broken_profiles, webhook_profile,
    };
    use crate::infrastructure::config::loader::{ConfigCenterLoader, FileConfigLoader};

    fn file_watcher(path: &std::path::Path) -> ConfigWatcher {
        ConfigWatcher::new(
            vec![Arc::new(ConfigLoaderItem::File(FileConfigLoader::new(
                path,
            )))],
            // 定时刷新不在用例里触发，刷新经 `reload` 与 `refresh` 直接调用。
            Duration::from_secs(3600),
        )
    }

    async fn file_hook_names(watcher: &ConfigWatcher) -> Vec<String> {
        watcher
            .snapshot()
            .await
            .global()
            .all_hooks()
            .map(|hook| hook.name.clone())
            .collect()
    }

    /// 启动时本进程自己的 Hook 文件坏了：`start` 报错（Capability 不启动），报错带文件路径、
    /// 不带文件内容；什么都不发布。修复前只记一条告警，按没有文件 Hook 启动。
    #[tokio::test]
    async fn a_broken_hook_file_stops_startup() {
        let dir = TempDir::new();
        for (content, expected) in broken_profiles() {
            let path = dir.write("hooks.toml", &content);
            let watcher = file_watcher(&path);
            let error = watcher.start().await.unwrap_err();
            let shown = error.to_string();
            assert!(shown.contains(&path.display().to_string()), "{shown}");
            assert!(shown.contains(expected), "{shown}");
            assert!(!format!("{error:?}").contains(SECRET), "{error:?}");
            assert!(watcher.snapshot().await.global().is_empty());
        }
    }

    /// 文件不存在照常启动、没有文件 Hook（行为同以前）；数据库与配置中心启动时加载失败也照旧
    /// 不挡启动。
    #[tokio::test]
    async fn a_missing_hook_file_or_a_failing_remote_source_does_not_stop_startup() {
        let dir = TempDir::new();
        let watcher = ConfigWatcher::new(
            vec![
                Arc::new(ConfigLoaderItem::File(FileConfigLoader::new(
                    dir.path("hooks.toml"),
                ))),
                // 不认识的端点格式：加载必然失败，且不发起任何连接。
                Arc::new(ConfigLoaderItem::ConfigCenter(ConfigCenterLoader::new(
                    "zk://127.0.0.1:1".into(),
                    None,
                ))),
            ],
            Duration::from_secs(3600),
        );
        watcher.start().await.unwrap();
        assert!(watcher.snapshot().await.global().is_empty());
    }

    /// 运行中文件改坏了（解析不了或校验不过）：重载不失败、服务不退出，文件里的 Hook 停在上一次
    /// 加载成功的版本，错误按 ERROR 记下（带路径、不带内容），定时刷新另记有几个源沿用了上一版。
    /// 文件改好后下一次重载生效；文件删掉则没有文件 Hook（行为同以前）。
    #[tokio::test]
    async fn a_hook_file_broken_at_runtime_keeps_the_last_good_hooks_and_is_reported() {
        let dir = TempDir::new();
        let path = dir.write("hooks.toml", webhook_profile("audit"));
        let watcher = file_watcher(&path);
        watcher.start().await.unwrap();
        assert_eq!(file_hook_names(&watcher).await, ["audit"]);

        for (content, expected) in broken_profiles() {
            std::fs::write(&path, &content).unwrap();
            let capture = crate::test_logs::LogCapture::start(tracing::Level::WARN);
            watcher.state.refresh().await;
            watcher.reload().await.unwrap();
            let output = capture.finish();

            assert_eq!(file_hook_names(&watcher).await, ["audit"], "{expected}");
            let errors: Vec<&str> = output
                .lines()
                .filter(|line| line.contains("ERROR"))
                .collect();
            assert_eq!(errors.len(), 2, "{output}");
            for line in errors {
                assert!(line.contains("keeping its last loaded hooks"), "{line}");
                assert!(line.contains("source=\"file\""), "{line}");
                assert!(line.contains(&path.display().to_string()), "{line}");
                assert!(line.contains(expected), "{line}");
            }
            assert!(output.contains("failed_sources=1"), "{output}");
            assert!(!output.contains(SECRET), "{output}");
        }

        std::fs::write(&path, webhook_profile("audit-v2")).unwrap();
        watcher.reload().await.unwrap();
        assert_eq!(file_hook_names(&watcher).await, ["audit-v2"]);

        std::fs::remove_file(&path).unwrap();
        watcher.reload().await.unwrap();
        assert!(file_hook_names(&watcher).await.is_empty());
    }
}
