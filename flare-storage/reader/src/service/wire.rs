//! Wire 风格依赖注入：仅组装「消息与可见性读模型」相关组件

use std::sync::Arc;

use anyhow::{Context as AnyhowContext, Result};

use crate::application::handlers::MessageStorageQueryHandler;
use crate::config::StorageReaderConfig;
use crate::domain::repository::{MessageStorage, VisibilityStorage};
use crate::domain::service::{MessageStorageDomainConfig, MessageStorageDomainService};
use crate::infrastructure::persistence::optimized_postgres_store::OptimizedPostgresMessageStorageImpl;
use crate::infrastructure::persistence::postgres_base::PostgresBaseStorage;
use crate::infrastructure::persistence::visibility_storage_impl::PostgresVisibilityStorageImpl;
use crate::interface::grpc::handler::StorageReaderGrpcHandler;

pub struct ApplicationContext {
    pub handler: StorageReaderGrpcHandler,
}

/// 构建应用上下文：PostgreSQL 存储 + 可选 Redis 缓存 → 领域服务 → 查询 Handler → gRPC
pub async fn initialize(
    app_config: &flare_im_core::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    let config = Arc::new(
        StorageReaderConfig::from_app_config(app_config)
            .with_context(|| "Failed to load storage reader service configuration")?,
    );

    let postgres_base_storage = match PostgresBaseStorage::new(config.as_ref()).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            return Err(anyhow::anyhow!(
                "PostgreSQL URL not configured. Set POSTGRES_URL or STORAGE_POSTGRES_URL, or define postgres profile in config"
            ));
        }
        Err(e) => return Err(e).with_context(|| "Failed to create PostgreSQL base storage"),
    };

    let storage: Arc<dyn MessageStorage + Send + Sync> = Arc::new(
        OptimizedPostgresMessageStorageImpl::new(
            postgres_base_storage.clone(),
            postgres_base_storage.cache.clone(),
            None,
        ),
    );

    let visibility_storage: Option<Arc<dyn VisibilityStorage + Send + Sync>> =
        Some(Arc::new(PostgresVisibilityStorageImpl::new(postgres_base_storage)));

    let domain_config = MessageStorageDomainConfig {
        max_page_size: config.max_page_size,
        default_range_seconds: config.default_range_seconds,
    };

    let domain_service = Arc::new(MessageStorageDomainService::new(
        storage.clone(),
        visibility_storage,
        domain_config,
    ));

    let query_handler = Arc::new(MessageStorageQueryHandler::with_domain_service(
        storage,
        domain_service.clone(),
    ));

    let grpc_handler = StorageReaderGrpcHandler::new(query_handler).await?;

    Ok(ApplicationContext {
        handler: grpc_handler,
    })
}
