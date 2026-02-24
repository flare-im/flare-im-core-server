use std::sync::Arc;

use anyhow::{Context as AnyhowContext, Result};

use crate::application::handlers::{MessageStorageQueryHandler};
use crate::config::StorageReaderConfig;
use crate::domain::repository::{MessageStorage, VisibilityStorage};
use crate::domain::service::{MessageStorageDomainConfig, MessageStorageDomainService};
use crate::infrastructure::persistence::optimized_postgres_store::OptimizedPostgresMessageStorageImpl;
use crate::interface::grpc::handler::StorageReaderGrpcHandler;

/// 应用上下文 - 包含所有已初始化的服务
pub struct ApplicationContext {
    pub handler: StorageReaderGrpcHandler,
}

/// 构建应用上下文
///
/// 类似 Go Wire 的 Initialize 函数，按照依赖顺序构建所有组件
///
/// # 参数
/// * `app_config` - 应用配置
///
/// # 返回
/// * `ApplicationContext` - 构建好的应用上下文
pub async fn initialize(
    app_config: &flare_im_core::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    // 1. 加载存储读取器配置
    let config = Arc::new(
        StorageReaderConfig::from_app_config(app_config)
            .with_context(|| "Failed to load storage reader service configuration")?,
    );

    // 2. 创建基础存储组件
    let postgres_base_storage = match crate::infrastructure::persistence::postgres_base::PostgresBaseStorage::new(config.as_ref())
        .await
        .with_context(|| "Failed to create PostgreSQL base storage")?
    {
        Some(storage) => storage,
        None => {
            return Err(anyhow::anyhow!(
                "PostgreSQL URL not configured. Set POSTGRES_URL or STORAGE_POSTGRES_URL, or define postgres profile in config"
            ));
        }
    };
    
    // 3. 创建优化的消息存储实例（实现 MessageStorage trait）
    let optimized_storage = Arc::new(
        OptimizedPostgresMessageStorageImpl::new(postgres_base_storage.clone(), postgres_base_storage.cache.clone())
    );
    tracing::info!("Using PostgreSQL storage with optimizations");

    // 4. 创建可见性存储实例（实现 VisibilityStorage trait）
    let visibility_storage_impl = Arc::new(
        crate::infrastructure::persistence::visibility_storage_impl::PostgresVisibilityStorageImpl::new(postgres_base_storage)
    );

    // 5. 创建消息存储和可见性存储实例（分别实现不同的 trait）
    let storage: Arc<dyn MessageStorage + Send + Sync> = optimized_storage;
    let visibility_storage: Option<Arc<dyn VisibilityStorage + Send + Sync>> = Some(visibility_storage_impl);

    // 6. 消息状态仓储不再需要（功能已合并到 message_read_records 和 message_visibility 表）
    
    // 7. 构建领域配置
    let domain_config = MessageStorageDomainConfig {
        max_page_size: config.max_page_size,
        default_range_seconds: config.default_range_seconds,
    };

    // 8. 构建领域服务
    let domain_service = Arc::new(MessageStorageDomainService::new(
        storage.clone(),
        visibility_storage,
        domain_config,
    ));

    // 9. 构建查询处理器（对于基于 seq 的查询，需要使用领域服务）
    let query_handler = Arc::new(MessageStorageQueryHandler::with_domain_service(
        storage,
        domain_service.clone(),
    ));

    // 10. 构建 gRPC 处理器
    let grpc_handler = StorageReaderGrpcHandler::new(query_handler).await?;

    Ok(ApplicationContext {
        handler: grpc_handler,
    })
}