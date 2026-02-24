//! Wire 风格的依赖注入模块
//!
//! 类似 Go 的 Wire 框架，提供简单的依赖构建方法

use std::sync::Arc;

use anyhow::{Context as AnyhowContext, Result};
use tracing::warn;

use crate::application::handlers::{MessageOperationCommandHandler, MessagePersistenceCommandHandler};
use crate::config::StorageWriterConfig;
use crate::domain::repository::{
    AckPublisher, ArchiveStoreRepository, HotCacheRepository, MediaAttachmentVerifier,
    MessageIdempotencyRepository, ConversationStateRepository, UserSyncCursorRepository,
    WalCleanupRepository,
};
use crate::domain::repository::ConversationUpdateRepository;
use crate::domain::service::{MessageOperationDomainService, MessagePersistenceDomainService};
use crate::infrastructure::external::media::MediaAttachmentClient;
use crate::infrastructure::messaging::ack_publisher::KafkaAckPublisher;
use crate::infrastructure::persistence::postgres_store::PostgresMessageStore;
use crate::infrastructure::persistence::redis_cache::RedisHotCacheRepository;
use crate::infrastructure::persistence::redis_idempotency::RedisIdempotencyRepository;
use crate::infrastructure::persistence::redis_wal_cleanup::RedisWalCleanupRepository;
use crate::infrastructure::persistence::conversation_repo::PostgresConversationRepository;
use crate::infrastructure::persistence::conversation_state::RedisConversationStateRepository;
use crate::infrastructure::persistence::user_cursor::RedisUserCursorRepository;
use crate::interface::messaging::normal_consumer::NormalMessageConsumer;
use crate::interface::messaging::operation_consumer::OperationMessageConsumer;

use flare_im_core::metrics::StorageWriterMetrics;
use flare_server_core::ServiceClient;
use flare_server_core::kafka::build_kafka_producer; // 添加ServiceClient导入

/// 应用上下文 - 包含所有已初始化的服务
pub struct ApplicationContext {
    pub normal_consumer: NormalMessageConsumer,
    pub operation_consumer: OperationMessageConsumer,
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
    // 1. 加载存储写入器配置
    let config = Arc::new(
        StorageWriterConfig::from_app_config(app_config)
            .with_context(|| "Failed to load storage writer service configuration")?,
    );

    // 2. 创建 ACK 发布者（可选）
    let ack_publisher = build_ack_publisher(&config)?;

    // 4. 创建 Redis 客户端（可选）
    let redis_client = build_redis_client(&config);

    // 5. 创建媒体附件验证器（可选）
    let media_verifier = config.media_service_endpoint.as_ref().map(|endpoint| {
        Arc::new(MediaAttachmentClient::new(endpoint.clone()))
            as Arc<dyn MediaAttachmentVerifier + Send + Sync>
    });

    // 6. 创建幂等性仓储（可选）
    let idempotency_repo = redis_client.as_ref().map(|client| {
        Arc::new(RedisIdempotencyRepository::new(client.clone(), &config))
            as Arc<dyn MessageIdempotencyRepository + Send + Sync>
    });

    // 7. 创建热缓存仓储（可选）
    let hot_cache_repo = redis_client.as_ref().map(|client| {
        Arc::new(RedisHotCacheRepository::new(client.clone(), &config))
            as Arc<dyn HotCacheRepository + Send + Sync>
    });

    // 8. 创建 WAL 清理仓储（可选）
    let wal_cleanup_repo = match (&redis_client, &config.wal_hash_key) {
        (Some(client), Some(key)) => Some(Arc::new(RedisWalCleanupRepository::new(
            client.clone(),
            key.clone(),
        ))
            as Arc<dyn WalCleanupRepository + Send + Sync>),
        _ => None,
    };

    // 9. 创建归档存储仓储（PostgreSQL，可选）
    let archive_repo: Option<Arc<dyn ArchiveStoreRepository + Send + Sync>> =
        match PostgresMessageStore::new(&config).await {
            Ok(Some(store)) => {
                Some(Arc::new(store) as Arc<dyn ArchiveStoreRepository + Send + Sync>)
            }
            Ok(None) => None,
            Err(err) => {
                warn!(error = ?err, "Failed to connect to PostgreSQL, skipping PostgreSQL storage");
                None
            }
        };

    // 11. 创建会话状态仓储（可选）
    let mut conversation_state_repo: Option<Arc<dyn ConversationStateRepository + Send + Sync>> =
        redis_client
            .as_ref()
            .map(|client| Arc::new(RedisConversationStateRepository::new(client.clone())) as Arc<_>);

    // 12. 创建用户游标仓储（可选）
    let user_cursor_repo: Option<Arc<dyn UserSyncCursorRepository + Send + Sync>> = redis_client
        .as_ref()
        .map(|client| Arc::new(RedisUserCursorRepository::new(client.clone())) as Arc<_>);

    // 13. 创建 Seq 生成器（可选）

    // 14. 创建 Session 更新仓储（可选，需要 PostgreSQL）
    let session_update_repo: Option<Arc<dyn ConversationUpdateRepository + Send + Sync>> =
        archive_repo.as_ref().and_then(|archive| {
            if let Some(pg_store) = archive.as_any().downcast_ref::<PostgresMessageStore>() {
                Some(Arc::new(PostgresConversationRepository::new(Arc::new(
                    pg_store.pool().clone(),
                )))
                    as Arc<dyn ConversationUpdateRepository + Send + Sync>)
            } else {
                None
            }
        });

    // 15. 初始化指标收集（应用层关注点）
    let metrics = Arc::new(StorageWriterMetrics::new());

    // 16. 创建 Session 服务客户端（用于获取会话参与者列表）
    let conversation_client: Option<Arc<tokio::sync::Mutex<ServiceClient>>> = {
        use flare_im_core::service_names::{CONVERSATION, get_service_name};
        let conversation_service = get_service_name(CONVERSATION);

        // 添加超时保护，避免服务发现阻塞整个启动过程
        let discover_result = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            flare_im_core::discovery::create_discover(&conversation_service),
        )
        .await;

        match discover_result {
            Ok(Ok(Some(discover))) => {
                let service_client = ServiceClient::new(discover);
                Some(Arc::new(tokio::sync::Mutex::new(service_client)))
            }
            Ok(Ok(None)) => {
                tracing::warn!("Session service discovery returned None");
                None
            }
            Ok(Err(e)) => {
                tracing::warn!(error = ?e, "Failed to create session service discovery");
                None
            }
            Err(_) => {
                tracing::warn!("Session service discovery timeout");
                None
            }
        }
    };

    // 17. 创建领域服务（不包含指标，符合 DDD 原则）
    // 注意：根据设计文档，只使用 PostgreSQL 作为归档存储，Redis 作为缓存
    let domain_service = Arc::new(MessagePersistenceDomainService::new(
        idempotency_repo,
        hot_cache_repo,
        None, // realtime_repo: 已移除 MongoDB 支持
        archive_repo.clone(),
        wal_cleanup_repo,
        ack_publisher,
        media_verifier,
        conversation_state_repo.clone(), // 先传入原始的conversation_state_repo
        user_cursor_repo,
        session_update_repo,
        conversation_client, // 添加conversation_client参数
    ));

    // 更新conversation_state_repo，注入domain_service
    if let Some(repo) = &mut conversation_state_repo {
        // 由于Rust的所有权机制，我们需要重新构建conversation_state_repo
        if let Some(client) = redis_client {
            *repo = Arc::new(
                RedisConversationStateRepository::new(client.clone())
                    .with_domain_service(Some(domain_service.clone())),
            );
        }
    }

    // 17. 创建操作消息领域服务
    let operation_service = Arc::new(MessageOperationDomainService::new(archive_repo.clone()));

    // 18. 创建命令处理器（应用层负责指标记录）
    let command_handler = Arc::new(MessagePersistenceCommandHandler::new(
        domain_service,
        operation_service.clone(),
        metrics.clone(),
    ));

    // 18. 创建操作命令处理器
    let operation_command_handler = Arc::new(MessageOperationCommandHandler::new(
        operation_service,
        metrics.clone(),
    ));

    // 16. 构建 Kafka 消费者（普通消息和操作消息分离）
    let normal_consumer = NormalMessageConsumer::new(
        config.clone(),
        command_handler.clone(),
        metrics.clone(),
    )
    .await
    .with_context(|| "Failed to create NormalMessageConsumer")?;

    let operation_consumer = OperationMessageConsumer::new(
        config.clone(),
        operation_command_handler,
        metrics.clone(),
    )
    .await
    .with_context(|| "Failed to create OperationMessageConsumer")?;

    Ok(ApplicationContext {
        normal_consumer,
        operation_consumer,
    })
}

/// 构建 ACK 发布者
fn build_ack_publisher(
    config: &Arc<StorageWriterConfig>,
) -> Result<Option<Arc<dyn AckPublisher + Send + Sync>>> {
    if let Some(topic) = &config.kafka_ack_topic {
        // 使用统一的 Kafka 生产者构建器（从 flare-server-core）
        let producer = build_kafka_producer(
            config.as_ref() as &dyn flare_server_core::kafka::KafkaProducerConfig
        )
        .with_context(|| "Failed to create Kafka producer for ACK")?;

        let producer = Arc::new(producer);
        let publisher: Arc<dyn AckPublisher + Send + Sync> = Arc::new(KafkaAckPublisher::new(
            producer,
            config.clone(),
            topic.clone(),
        ));
        Ok(Some(publisher))
    } else {
        Ok(None)
    }
}

/// 构建 Redis 客户端
fn build_redis_client(config: &Arc<StorageWriterConfig>) -> Option<Arc<redis::Client>> {
    config.redis_url.as_ref().and_then(|url| {
        match redis::Client::open(url.as_str()) {
            Ok(client) => Some(Arc::new(client)),
            Err(err) => {
                warn!(error = ?err, "Failed to initialize Redis client; Redis-backed features disabled");
                None
            }
        }
    })
}
