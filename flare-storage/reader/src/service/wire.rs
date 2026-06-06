use std::sync::Arc;

use flare_server_core::error::{AnyhowContext, Result};
use sqlx::ConnectOptions;
use sqlx::postgres::PgConnectOptions;
use sqlx::postgres::PgPoolOptions;

use crate::application::handlers::MessageStorageQueryHandler;
use crate::config::StorageReaderConfig;
use crate::domain::repository::MessageStorage;
use crate::infrastructure::persistence::optimized_postgres_store::OptimizedPostgresMessageStorageImpl;
use crate::infrastructure::persistence::postgres_base::PostgresBaseStorage;
use crate::infrastructure::persistence::redis_cache::RedisMessageCache;
use crate::interface::grpc::StorageReaderGrpcHandler;

// 类型别名，简化泛型参数
pub type MessageStorageType = OptimizedPostgresMessageStorageImpl;
type QueryHandlerType = MessageStorageQueryHandler<MessageStorageType>;
type GrpcHandlerType = StorageReaderGrpcHandler<MessageStorageType>;

/// 应用上下文 - 包含所有已初始化的服务
pub struct ApplicationContext<M>
where
    M: MessageStorage + Send + Sync + Clone + 'static,
{
    pub handler: StorageReaderGrpcHandler<M>,
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
) -> Result<ApplicationContext<MessageStorageType>> {
    // 1. 加载存储读取器配置
    let config = Arc::new(
        StorageReaderConfig::from_app_config(app_config)
            .with_context(|| "Failed to load storage reader service configuration")?,
    );

    // 2. 创建基础存储组件（在 wire 中建池并开启 SQL 日志便于调试）
    let postgres_url = config
        .postgres_url
        .as_ref()
        .ok_or_else(|| flare_server_core::error::FlareError::system("PostgreSQL URL not configured. Set POSTGRES_URL or STORAGE_POSTGRES_URL, or define postgres profile in config".to_string()))?;
    let connect_opts: PgConnectOptions = postgres_url
        .parse::<PgConnectOptions>()
        .with_context(|| "Invalid PostgreSQL URL")?
        .log_statements(log::LevelFilter::Trace);
    let pool = PgPoolOptions::new()
        .max_connections(config.postgres_max_connections)
        .min_connections(config.postgres_min_connections)
        .acquire_timeout(std::time::Duration::from_secs(
            config.postgres_acquire_timeout_seconds,
        ))
        .idle_timeout(Some(std::time::Duration::from_secs(
            config.postgres_idle_timeout_seconds,
        )))
        .max_lifetime(Some(std::time::Duration::from_secs(
            config.postgres_max_lifetime_seconds,
        )))
        .test_before_acquire(true)
        .connect_with(connect_opts)
        .await
        .with_context(|| "Failed to connect to PostgreSQL")?;
    let cache: Option<Arc<RedisMessageCache>> = match &config.redis_url {
        Some(redis_url) => {
            let client =
                redis::Client::open(redis_url.as_str()).with_context(|| "Invalid Redis URL")?;
            Some(Arc::new(RedisMessageCache::new(
                Arc::new(client),
                config.as_ref(),
            )))
        }
        None => None,
    };
    let postgres_base_storage = PostgresBaseStorage::from_pool_and_cache(pool, cache.clone())
        .await
        .with_context(|| "Failed to create PostgreSQL base storage")?;

    // 3. 创建优化的消息存储实例（实现 MessageStorage trait）
    let storage: Arc<MessageStorageType> = Arc::new(MessageStorageType::new(
        postgres_base_storage.clone(),
        cache,
        None,
    ));
    tracing::info!("Using PostgreSQL storage with optimizations");

    // 4. 构建查询处理器（直接使用存储层）
    let query_handler: Arc<QueryHandlerType> = Arc::new(QueryHandlerType::new(storage));

    // 5. 构建 gRPC 处理器
    let grpc_handler: GrpcHandlerType = GrpcHandlerType::new(query_handler.clone()).await?;
    Ok(ApplicationContext {
        handler: grpc_handler,
    })
}
