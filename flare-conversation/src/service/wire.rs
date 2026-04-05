//! Wire 风格的依赖注入模块
//!
//! 类似 Go 的 Wire 框架，提供简单的依赖构建方法

use std::sync::Arc;

use crate::error::{ErrorBuilder, ErrorCode, Result};

use crate::application::handlers::{ConversationCommandHandler, ConversationQueryHandler};
use crate::config::ConversationConfig;
use crate::domain::model::ConversationDomainConfig;
use crate::domain::service::{ConversationDomainService, DefaultConversationDomainService};
use crate::infrastructure::event_consumer::{
    ConversationEnsureEventConsumer, ReadReceiptEventConsumer,
};
use crate::infrastructure::persistence::redis_presence::RedisPresenceRepository;
use crate::infrastructure::rpc::StorageReaderClient;
use crate::interface::grpc::ConversationGrpcHandler;

/// 应用上下文 - 包含所有已初始化的服务
pub struct ApplicationContext {
    pub handler: ConversationGrpcHandler,
    /// 配置了 Kafka 且初始化成功时存在，由 bootstrap 后台 `run`（`ctx` 在 MQ 侧重建）
    pub read_receipt_consumer: Option<ReadReceiptEventConsumer>,
    pub conversation_ensure_consumer: Option<ConversationEnsureEventConsumer>,
}

/// 构建应用上下文
///
/// 类似 Go Wire 的 Initialize 函数，按照依赖顺序构建所有组件
///
/// 注意：由于 Rust 2024 原生 async fn 不支持 dyn 兼容性，
/// 我们使用泛型 + 具体类型的方式，在编译期确定实现
pub async fn initialize(
    app_config: &flare_im_core::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    // 1. 加载会话配置
    let conversation_config = Arc::new(
        ConversationConfig::from_app_config(app_config)
            .map_err(|e| ErrorBuilder::new(ErrorCode::InternalError, "Failed to load conversation service configuration")
                .details(e.to_string())
                .build_error())?,
    );

    // 2. 创建 Redis 客户端
    let redis_client = Arc::new(redis::Client::open(conversation_config.redis_url.clone())
        .map_err(|e| ErrorBuilder::new(ErrorCode::NetworkError, "Failed to create Redis client")
            .details(e.to_string())
            .build_error())?);

    // 3. 创建 PostgreSQL 连接池（可选）
    let postgres_pool = if let Some(ref postgres_url) = conversation_config.postgres_url {
        Arc::new(
            sqlx::PgPool::connect(postgres_url)
                .await
                .map_err(|e| ErrorBuilder::new(ErrorCode::DatabaseError, "Failed to connect to PostgreSQL")
                    .details(e.to_string())
                    .build_error())?,
        )
    } else {
        return Err(ErrorBuilder::new(
            ErrorCode::InvalidParameter,
            "postgres config is required for conversation service",
        )
        .build_error());
    };

    // 4. 创建会话仓储（硬切到 Postgres，会话元数据必须来自持久化读模型）
    let conversation_repo = Arc::new(
        crate::infrastructure::persistence::PostgresConversationRepository::new(
            postgres_pool,
            conversation_config.clone(),
        ),
    );

    // 5. 创建在线状态仓储
    let presence_repo: Arc<RedisPresenceRepository> = Arc::new(RedisPresenceRepository::new(
        redis_client.clone(),
        conversation_config.clone(),
    ));

    // 6. 创建消息提供者（可选）
    let message_provider: Option<Arc<StorageReaderClient>> = {
        use flare_im_core::service_names::{STORAGE_READER, get_service_name};
        let storage_reader_service = get_service_name(STORAGE_READER);

        // 创建 Storage Reader 服务发现
        let storage_discover = flare_im_core::discovery::create_discover(&storage_reader_service)
            .await
            .map_err(|e| {
                ErrorBuilder::new(
                    ErrorCode::NetworkError,
                    &format!("Failed to create storage reader service discover for {}: {}", storage_reader_service, e)
                ).build_error()
            })?;

        let provider = if let Some(discover) = storage_discover {
            let service_client = flare_server_core::discovery::ServiceClient::new(discover);
            StorageReaderClient::with_service_client(service_client)
        } else {
            // Fallback: construct provider with service name; provider will try env direct connect
            tracing::warn!("Storage Reader service discovery not configured, using env fallback");
            StorageReaderClient::new(storage_reader_service)
        };

        Some(Arc::new(provider))
    };

    // 7. 构建领域配置
    let domain_config = ConversationDomainConfig::new(conversation_config.recent_message_limit);

    // 8. 构建领域服务（使用泛型参数以获得更好的性能）
    let domain_service: Arc<DefaultConversationDomainService> =
        Arc::new(ConversationDomainService::new(
            conversation_repo.clone(),
            presence_repo.clone(),
            message_provider.clone(),
            domain_config,
        ));

    // 8. 构建命令处理器
    let command_handler = Arc::new(ConversationCommandHandler::new(domain_service.clone()));

    // 9. 构建查询处理器
    let query_handler = Arc::new(ConversationQueryHandler::new(
        conversation_repo,
        message_provider,
        domain_service.clone(),
    ));

    // 10. 构建 gRPC 处理器
    let grpc_handler = ConversationGrpcHandler::new(command_handler, query_handler);

    let read_receipt_consumer = if conversation_config.kafka_bootstrap.is_some() {
        match ReadReceiptEventConsumer::new(conversation_config.as_ref(), domain_service.clone())
            .await
        {
            Ok(c) => Some(c),
            Err(e) => {
                tracing::warn!(error = %e, "ReadReceipt Kafka consumer init failed, skipping");
                None
            }
        }
    } else {
        None
    };

    let conversation_ensure_consumer = if conversation_config.kafka_bootstrap.is_some() {
        match ConversationEnsureEventConsumer::new(
            conversation_config.as_ref(),
            domain_service.clone(),
        )
        .await
        {
            Ok(c) => Some(c),
            Err(e) => {
                tracing::warn!(error = %e, "ConversationEnsure Kafka consumer init failed, skipping");
                None
            }
        }
    } else {
        None
    };

    Ok(ApplicationContext {
        handler: grpc_handler,
        read_receipt_consumer,
        conversation_ensure_consumer,
    })
}
