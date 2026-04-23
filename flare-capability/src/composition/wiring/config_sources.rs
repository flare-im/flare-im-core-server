//! **配置源装配**：文件 / 配置中心 / 数据库加载器与 `ConfigWatcher`（读模型侧基础设施）。

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::json;

use crate::infrastructure::config::loader::{
    ConfigCenterLoader, ConfigLoaderItem, DatabaseConfigLoader, FileConfigLoader,
};
use crate::infrastructure::config::ConfigWatcher;
use crate::infrastructure::persistence::postgres_config::PostgresHookConfigRepository;

use crate::composition::process_config::CapabilityServiceConfig;

use flare_server_core::discovery::{BackendType, DiscoveryConfig};
use flare_server_core::{KvBackend, KvStore};

/// Hook 配置持久化仓储（仅当配置了 `database_url`）。
pub(crate) type HookConfigRepository = Arc<PostgresHookConfigRepository>;

/// 配置子图：已启动的 `ConfigWatcher` + 可选 Postgres Hook 仓储。
pub(crate) struct ConfigSourcesReady {
    pub watcher: Arc<ConfigWatcher>,
    pub hook_config_repository: Option<HookConfigRepository>,
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
                "flare-capability PostgreSQL target (credentials omitted); init_v2.sql must be applied to this host/database"
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
        loaders.push(Arc::new(ConfigLoaderItem::File(FileConfigLoader::new(
            path.clone(),
        ))));
    }

    if let Some(ref endpoint) = config.config_center_endpoint {
        let mut config_loader =
            ConfigCenterLoader::new(endpoint.clone(), config.tenant_id.clone());

        if endpoint.starts_with("consul://") {
            if let Some(addr) = endpoint.strip_prefix("consul://") {
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
        }

        loaders.push(Arc::new(ConfigLoaderItem::ConfigCenter(config_loader)));
    }

    let hook_config_repository = if let Some(ref database_url) = config.database_url {
        let repository = Arc::new(
            PostgresHookConfigRepository::new(database_url)
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

    Ok(ConfigSourcesReady {
        watcher,
        hook_config_repository,
    })
}
