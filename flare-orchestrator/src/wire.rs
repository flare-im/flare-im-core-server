//! Wire 风格的依赖注入模块
//!
//! 类似 Go 的 Wire 框架，提供简单的依赖构建方法
//!
//! ## 依赖注入顺序
//! 1. 基础设施层（Infrastructure）：Redis、Kafka
//! 2. Repository 层：数据访问抽象
//! 3. Domain Service 层：业务逻辑
//! 4. Application Handler 层：用例编排
//! 5. Interface 层：gRPC/HTTP 控制器

use std::sync::Arc;

use anyhow::{Context, Result};
use flare_im_core::constants::topics::TOPIC_MESSAGE_MAIN;
use flare_im_core::hooks::{HookDispatcher, HookRegistry};
use flare_im_core::service_names::CONVERSATION;
use flare_server_core::mq::consumer::dispatcher::{Dispatcher, TopicDispatcher};
use flare_server_core::mq::consumer::{ConsumerConfig, MessageHandler as MqMessageHandler};
use flare_server_core::mq::kafka::producer::KafkaProducer;

use crate::application::handlers::{
    EventHandler, MessageActionHandler, MessageHandler as AppMessageHandler, StorageHandler,
};
use crate::config::MessageOrchestratorConfig;
use crate::domain::service::{
    ConversationEnsureService, EventDomainService, HookExecutionService, MessageDomainService,
    SequenceAllocator,
};
use crate::infrastructure::rpc::ConversationClient;
use crate::infrastructure::messaging::conversation_ensure_publisher::MqConversationEnsurePublisher;
use crate::infrastructure::messaging::push_repository::MqPushRepository;
use crate::infrastructure::persistence::redis_wal::RedisWalRepository;
use crate::infrastructure::persistence::recipient_repository::RecipientRepositoryImpl;
use crate::infrastructure::persistence::wal_repository_impl::WalRepositoryItem;
use crate::interface::grpc::{MessageActionGrpcHandler, MessageSendGrpcHandler};
use crate::interface::mq::StorageConsumerHandler;

/// 应用上下文 - 包含所有已初始化的服务
pub struct ApplicationContext {
    pub message_send_grpc: MessageSendGrpcHandler,
    pub message_action_grpc: MessageActionGrpcHandler,
    /// `TOPIC_MESSAGE_MAIN` 消费者逻辑（与 [ConsumerConfig]、[Dispatcher] 配套使用）
    pub storage_consumer_handler: Arc<StorageConsumerHandler>,
    pub consumer_config: ConsumerConfig,
    pub main_queue_dispatcher: Arc<dyn Dispatcher>,
    pub config: Arc<MessageOrchestratorConfig>,
}

/// 构建应用上下文
///
/// 类似 Go Wire 的 Initialize 函数，按照依赖顺序构建所有组件
pub async fn initialize(
    app_config: &flare_im_core::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    let config = Arc::new(MessageOrchestratorConfig::from_app_config(app_config));

    let redis_client = build_redis_client(&config).await?;

    let kafka_producer = build_kafka_producer(&config).await?;

    let push_repository = MqPushRepository::new(kafka_producer.clone());

    let conversation_service_type = config
        .conversation_service_type
        .as_deref()
        .unwrap_or(CONVERSATION);
    let conversation_channel = flare_im_core::discovery::connect_grpc_channel_from_app_config(
        app_config,
        conversation_service_type,
        "http://127.0.0.1:50090",
    )
    .await
    .map_err(|e| anyhow::anyhow!("connect conversation service ({conversation_service_type}) failed: {e}"))?;
    let conversation_repository = Arc::new(ConversationClient::new(conversation_channel));

    let recipient_repository: Arc<dyn crate::domain::repository::RecipientRepository> =
        Arc::new(RecipientRepositoryImpl::new(conversation_repository.clone()));

    let wal_repository: Arc<WalRepositoryItem> = Arc::new(WalRepositoryItem::Redis(
        Arc::new(RedisWalRepository::new(redis_client.clone(), config.clone())),
    ));

    let sequence_allocator = SequenceAllocator::new(redis_client.clone(), 100)
        .await
        .map_err(|e| anyhow::anyhow!("sequence allocator: {}", e))?;

    let message_domain_service = Arc::new(MessageDomainService::new(
        push_repository.clone(),
        recipient_repository.clone(),
        wal_repository,
        Arc::new(sequence_allocator.clone()),
        config.defaults(),
        None,
        None,
    ));

    let event_domain_service = Arc::new(EventDomainService::new(
        push_repository,
        recipient_repository,
        Arc::new(sequence_allocator),
        None,
    ));

    let hook_execution_service = Arc::new(HookExecutionService::new(
        Arc::new(HookDispatcher::new(HookRegistry::new())),
        config.default_tenant_id.clone(),
    ));

    let conversation_ensure_service = Arc::new(ConversationEnsureService::new(
        Some(conversation_repository.clone()),
        config.session_creation_mode,
        Some(MqConversationEnsurePublisher::new(kafka_producer)),
    ));

    let message_handler = Arc::new(AppMessageHandler::new(
        message_domain_service.clone(),
        hook_execution_service,
        conversation_ensure_service,
    ));

    let event_handler = Arc::new(EventHandler::new(event_domain_service.clone()));

    let message_action_handler = Arc::new(MessageActionHandler::new(event_handler));

    let message_send_grpc =
        MessageSendGrpcHandler::new(message_handler, event_domain_service.clone());

    let message_action_grpc = MessageActionGrpcHandler::new(message_action_handler);

    let storage_handler = Arc::new(StorageHandler::new(
        message_domain_service,
        event_domain_service,
    ));
    let storage_consumer_handler = Arc::new(StorageConsumerHandler::new(storage_handler));

    let mut topic_dispatcher = TopicDispatcher::new();
    let mq_handler: Arc<dyn MqMessageHandler> = storage_consumer_handler.clone();
    topic_dispatcher
        .register(TOPIC_MESSAGE_MAIN.to_string(), mq_handler)
        .map_err(|e| anyhow::anyhow!("register main queue dispatcher: {}", e))?;
    let main_queue_dispatcher: Arc<dyn Dispatcher> = Arc::new(topic_dispatcher);

    let consumer_config = ConsumerConfig::default().with_concurrency(32);

    Ok(ApplicationContext {
        message_send_grpc,
        message_action_grpc,
        storage_consumer_handler,
        consumer_config,
        main_queue_dispatcher,
        config,
    })
}

async fn build_redis_client(
    config: &MessageOrchestratorConfig,
) -> Result<Arc<redis::Client>> {
    let redis_url = config
        .redis_url
        .as_ref()
        .context("Redis URL not configured")?;

    let client = redis::Client::open(redis_url.as_str())
        .with_context(|| format!("failed to open Redis client for {}", redis_url))?;

    Ok(Arc::new(client))
}

async fn build_kafka_producer(
    config: &MessageOrchestratorConfig,
) -> Result<Arc<dyn flare_server_core::mq::producer::Producer>> {
    let producer = KafkaProducer::new(config)
        .map_err(|e| anyhow::anyhow!("failed to create Kafka producer: {}", e))?;

    Ok(Arc::new(producer))
}
