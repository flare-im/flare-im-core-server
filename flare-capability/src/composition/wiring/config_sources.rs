//! **配置源装配**：文件 / 配置中心 / 数据库加载器与 `ConfigWatcher`（读模型侧基础设施）。

use std::collections::HashMap;
use std::sync::Arc;

use flare_server_core::error::{AnyhowContext, Result};
use serde_json::json;

use crate::infrastructure::config::loader::{
    ConfigCenterLoader, ConfigLoaderItem, DatabaseConfigLoader, FileConfigLoader,
};
use crate::infrastructure::config::{ConfigWatcher, HookConfigInvalidation, HookConfigListener};
use crate::infrastructure::persistence::postgres_config::PostgresHookConfigRepository;

use crate::composition::process_config::CapabilityServiceConfig;

use flare_server_core::discovery::{BackendType, DiscoveryConfig};
use flare_server_core::{KvBackend, KvStore};

/// Hook 配置持久化仓储（仅当配置了 `database_url`）。
pub(crate) type HookConfigRepository = Arc<PostgresHookConfigRepository>;

/// 配置子图：已启动的 `ConfigWatcher` + 可选 Postgres Hook 仓储及其跨实例失效通知。
pub(crate) struct ConfigSourcesReady {
    pub watcher: Arc<ConfigWatcher>,
    pub hook_config_repository: Option<HookConfigRepository>,
    /// 有库时：Administer 写入后通知其余实例。
    pub invalidation: Option<Arc<HookConfigInvalidation>>,
    /// 有库时：本实例已启动的监听任务（独立连接），须保存到停机时停掉。
    pub listener: Option<HookConfigListener>,
}

pub(crate) fn log_capability_db_target(database_url: &str) {
    let tail = database_url
        .strip_prefix("postgresql://")
        .or_else(|| database_url.strip_prefix("postgres://"))
        .and_then(|rest| rest.find('@').map(|i| &rest[i + 1..]));
    match tail {
        Some(host_db) => {
            tracing::info!(
                target = "flare_capability::db",
                postgres_after_at = %host_db,
                "flare-capability PostgreSQL target (credentials omitted); deploy/init.sql must be applied to this host/database"
            );
        }
        None => {
            tracing::warn!(
                target = "flare_capability::db",
                "could not parse database_url for logging"
            );
        }
    }
}

/// 按优先级组装加载器并启动 `ConfigWatcher`。
pub(crate) async fn prepare_config_sources(
    config: &CapabilityServiceConfig,
) -> Result<ConfigSourcesReady> {
    let mut loaders: Vec<Arc<ConfigLoaderItem>> = Vec::new();

    if let Some(ref path) = config.config_file {
        let file = FileConfigLoader::new(path.clone());
        // 本进程自己的 Hook 文件先于配置中心与数据库读：文件存在却读不了、解析或校验不过时直接
        // 拒绝启动，报错不被随后的数据库不可达之类的错误盖住。文件不存在仍按没有文件 Hook 处理。
        file.load_at_startup().await?;
        loaders.push(Arc::new(ConfigLoaderItem::File(file)));
    }

    if let Some(ref endpoint) = config.config_center_endpoint {
        let mut config_loader = ConfigCenterLoader::new(endpoint.clone(), config.tenant_id.clone());

        if endpoint.starts_with("consul://")
            && let Some(addr) = endpoint.strip_prefix("consul://")
        {
            let parts: Vec<&str> = addr.split(':').collect();
            if parts.len() == 2 {
                let host = parts[0];
                let port = parts[1];
                let mut backend_config = HashMap::new();
                backend_config.insert(
                    "url".to_string(),
                    json!(format!("http://{}:{}", host, port)),
                );
                let discovery_config = DiscoveryConfig {
                    backend: BackendType::Consul,
                    backend_config,
                    namespace: None,
                    version: None,
                    tag_filters: vec![],
                    load_balance: flare_server_core::LoadBalanceStrategy::RoundRobin,
                    health_check: None,
                    refresh_interval: Some(30),
                };
                if let Ok(consul) =
                    flare_server_core::discovery::backend::consul::ConsulBackend::new(
                        &discovery_config,
                    )
                    .await
                {
                    let kv_backend: Arc<dyn KvBackend> = Arc::new(consul);
                    let kv_store = Arc::new(KvStore::new(kv_backend));
                    config_loader = config_loader.with_kv_store(kv_store);
                }
            }
        }

        loaders.push(Arc::new(ConfigLoaderItem::ConfigCenter(config_loader)));
    }

    let hook_config_repository = if let Some(ref database_url) = config.database_url {
        let repository = Arc::new(
            PostgresHookConfigRepository::new_with_pool_profile(
                database_url,
                config.postgres_max_connections,
                config.postgres_min_connections,
                config.postgres_acquire_timeout_seconds,
                config.postgres_idle_timeout_seconds,
                config.postgres_max_lifetime_seconds,
            )
            .await
            .context("Failed to create database config repository")?,
        );

        log_capability_db_target(database_url);
        crate::infrastructure::persistence::PostgresCapabilityPolicy::assert_public_capability_schema(
            repository.connection_pool().as_ref(),
        )
        .await
        .context("Capability policy PostgreSQL schema (public.capability_*) missing or wrong database")?;
        crate::infrastructure::persistence::PostgresCapabilityPolicy::warn_if_user_grants_empty(
            repository.connection_pool().as_ref(),
        )
        .await
        .context("Capability policy probe capability_user_grants row count")?;

        let repository_clone = repository.clone();
        loaders.push(Arc::new(ConfigLoaderItem::Database(
            DatabaseConfigLoader::new(repository_clone, config.tenant_id.clone()),
        )));

        Some(repository)
    } else {
        None
    };

    let watcher = Arc::new(ConfigWatcher::new(
        loaders,
        std::time::Duration::from_secs(config.refresh_interval_secs),
    ));

    watcher
        .start()
        .await
        .context("Failed to start config watcher")?;

    // 多实例：其余实例经 Administer 写入的 Hook 配置不必等定时刷新。监听用独立连接，不占
    // 仓储连接池；句柄交给进程生命周期，停机时停掉。
    let (invalidation, listener) = match hook_config_repository.as_ref() {
        Some(repository) => {
            let invalidation = Arc::new(HookConfigInvalidation::new(repository.connection_pool()));
            let listener = invalidation.spawn_listener(Arc::clone(&watcher));
            (Some(invalidation), Some(listener))
        }
        None => (None, None),
    };

    Ok(ConfigSourcesReady {
        watcher,
        hook_config_repository,
        invalidation,
        listener,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::config::loader::test_support::{
        SECRET, TempDir, broken_profiles, webhook_profile,
    };

    /// 装配配置源（Capability 启动）时本进程自己的 Hook 文件坏了就报错、不往下装配：先于数据库，
    /// 库连不上也不会盖住文件的错误。文件不存在或正常时照常装配。
    #[tokio::test]
    async fn a_broken_hook_file_refuses_to_start_before_the_database_is_touched() {
        let dir = TempDir::new();
        for (content, expected) in broken_profiles() {
            let path = dir.write("hooks.toml", &content);
            let config = CapabilityServiceConfig {
                config_file: Some(path.clone()),
                // 端口 1 上没有 PostgreSQL：要是先连库，报的会是连库失败。
                database_url: Some("postgres://r75b@127.0.0.1:1/none".into()),
                postgres_acquire_timeout_seconds: 1,
                ..Default::default()
            };
            let error = prepare_config_sources(&config)
                .await
                .err()
                .expect("a broken hook file must stop startup");
            let shown = error.to_string();
            assert!(shown.contains(&path.display().to_string()), "{shown}");
            assert!(shown.contains(expected), "{shown}");
            assert!(!format!("{error:?}").contains(SECRET), "{error:?}");
        }

        for (name, hooks) in [("absent.toml", 0), ("hooks.toml", 1)] {
            if hooks > 0 {
                dir.write(name, webhook_profile("audit"));
            }
            let config = CapabilityServiceConfig {
                config_file: Some(dir.path(name)),
                ..Default::default()
            };
            let ready = prepare_config_sources(&config)
                .await
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(ready.watcher.snapshot().await.global().hook_count(), hooks);
            assert!(ready.hook_config_repository.is_none());
        }
    }
}
