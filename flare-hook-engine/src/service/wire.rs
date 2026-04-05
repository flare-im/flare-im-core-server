//! Wire 风格的依赖注入模块
//!
//! 类似 Go 的 Wire 框架，提供简单的依赖构建方法

use std::sync::Arc;

use anyhow::{Context, Result};

use crate::application::handlers::HookCommandHandler;
use crate::domain::service::HookOrchestrationService;
use crate::infrastructure::adapters::HookAdapterFactory;
use crate::infrastructure::config::ConfigWatcher;
use crate::infrastructure::config::loader::{
    ConfigCenterLoader, ConfigLoaderItem, DatabaseConfigLoader, FileConfigLoader,
};
use crate::infrastructure::monitoring::{ExecutionRecorder, MetricsCollector};
use crate::interface::grpc::{HookExtensionServer, HookServiceServer};
use crate::service::bootstrap::HookEngineConfig;
use crate::service::registry::CoreHookRegistry;

use flare_server_core::discovery::{BackendType, DiscoveryConfig};
use flare_server_core::{KvBackend, KvStore};
use serde_json::json;
use std::collections::HashMap;

/// 应用上下文 - 包含所有已初始化的服务
pub struct ApplicationContext {
    pub hook_extension_service: HookExtensionServer,
    pub hook_service: Option<HookServiceServer>,
}

/// 构建应用上下文
///
/// 类似 Go Wire 的 Initialize 函数，按照依赖顺序构建所有组件
///
/// # 参数
/// * `config` - Hook引擎配置
///
/// # 返回
/// * `ApplicationContext` - 构建好的应用上下文
pub async fn initialize(config: HookEngineConfig) -> Result<ApplicationContext> {
    // 1. 创建配置加载器（按优先级从低到高）
    let mut loaders: Vec<Arc<ConfigLoaderItem>> = Vec::new();

    // 配置文件（最低优先级）
    if let Some(ref path) = config.config_file {
        loaders.push(Arc::new(ConfigLoaderItem::File(FileConfigLoader::new(
            path.clone(),
        ))));
    }

    // 配置中心（中等优先级）
    if let Some(ref endpoint) = config.config_center_endpoint {
        let mut config_loader = ConfigCenterLoader::new(endpoint.clone(), config.tenant_id.clone());

        // 如果是Consul端点，创建KV存储
        if endpoint.starts_with("consul://") {
            // 解析endpoint
            if let Some(addr) = endpoint.strip_prefix("consul://") {
                let parts: Vec<&str> = addr.split(':').collect();
                if parts.len() == 2 {
                    let host = parts[0];
                    let port = parts[1];

                    // 创建Consul后端配置
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

                    // 创建Consul后端
                    // 直接创建ConsulBackend实例，这样我们可以同时获得DiscoveryBackend和KvBackend的实现
                    if flare_server_core::discovery::backend::consul::ConsulBackend::new(
                        &discovery_config,
                    )
                    .await
                    .is_ok()
                    {
                        // 由于ConsulBackend没有实现Clone trait，我们需要创建一个新的实例用于KvBackend
                        if let Ok(kv_backend) =
                            flare_server_core::discovery::backend::consul::ConsulBackend::new(
                                &discovery_config,
                            )
                            .await
                        {
                            let kv_backend: Arc<dyn KvBackend> = Arc::new(kv_backend);
                            let kv_store = Arc::new(KvStore::new(kv_backend));
                            config_loader = config_loader.with_kv_store(kv_store);
                        }
                    }
                }
            }
        }

        loaders.push(Arc::new(ConfigLoaderItem::ConfigCenter(config_loader)));
    }

    // 数据库配置（最高优先级）
    let config_repository = if let Some(ref database_url) = config.database_url {
        let repository = Arc::new(
            crate::infrastructure::persistence::postgres_config::PostgresHookConfigRepository::new(
                database_url,
            )
            .await
            .context("Failed to create database config repository")?,
        );

        let repository_clone = repository.clone();
        loaders.push(Arc::new(ConfigLoaderItem::Database(
            DatabaseConfigLoader::new(repository_clone, config.tenant_id.clone()),
        )));

        Some(repository)
    } else {
        None
    };

    // 2. 创建配置监听器
    let config_watcher = Arc::new(ConfigWatcher::new(
        loaders,
        std::time::Duration::from_secs(config.refresh_interval_secs),
    ));

    // 启动配置监听
    config_watcher
        .start()
        .await
        .context("Failed to start config watcher")?;

    // 3. 创建监控组件
    let metrics_collector = Arc::new(MetricsCollector::new());
    let execution_recorder = Arc::new(ExecutionRecorder::new());

    // 4. 创建适配器工厂
    let adapter_factory = Arc::new(HookAdapterFactory::new());

    // 5. 创建编排服务
    let orchestration_service = Arc::new(HookOrchestrationService);

    // 6. 创建命令和查询处理器
    let command_handler = Arc::new(HookCommandHandler::new(orchestration_service.clone()));

    // 7. 创建Hook注册表
    let registry = Arc::new(CoreHookRegistry::new(config_watcher.clone()));

    // 8. 构建 HookExtension 服务
    let hook_extension_service =
        HookExtensionServer::new(command_handler, registry.clone(), adapter_factory);

    // 9. 构建 HookService 服务（如果配置了数据库）
    let hook_service = if let Some(ref repository) = config_repository {
        Some(
            HookServiceServer::new(repository.clone(), registry.clone())
                .with_monitoring(metrics_collector.clone(), execution_recorder.clone()),
        )
    } else {
        tracing::warn!("Database repository not available, HookService will not be available");
        None
    };

    Ok(ApplicationContext {
        hook_extension_service,
        hook_service,
    })
}
