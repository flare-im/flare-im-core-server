//! Wire 风格的依赖注入模块
//!
//! 类似 Go 的 Wire 框架，提供简单的依赖构建方法
//!
//! 通话/媒体信令走 realtime/capability packet 路由，不进入 durable IM event path。
//!
//! ## 依赖注入顺序
//! 1. 基础设施层（Infrastructure）：Redis、JetStream
//! 2. Repository 层：数据访问抽象
//! 3. Domain Service 层：业务逻辑
//! 4. Application Handler 层：用例编排
//! 5. Interface 层：gRPC/HTTP 控制器

use std::sync::Arc;
use std::time::Duration;

use flare_im_contracts::constants::topics::TOPIC_MESSAGE_MAIN;
use flare_im_contracts::service_names::CONVERSATION;
use flare_im_message_pipeline::{ConversationClient, MqPushRepository, RecipientRepositoryImpl};
use flare_im_seq::SequenceAllocator;
use flare_server_core::error::{AnyhowContext, Result};
use flare_server_core::mq::consumer::dispatcher::{Dispatcher, TopicDispatcher};
use flare_server_core::mq::consumer::{ConsumerConfig, MessageHandler as MqMessageHandler};
use flare_server_core::mq::consumer::{
    ConsumerFailurePublishers, FailureTopic, ProducerDeadLetterPublisher, ProducerRetryPublisher,
    RetryForwarderHandler,
};
use flare_server_core::mq::kafka::KafkaProducer;
use flare_server_core::mq::nats::NatsProducer;

use crate::application::handlers::{EventHandler, MessageActionHandler, StorageHandler};
use crate::config::MessageOrchestratorConfig;
use crate::domain::service::{EventDomainService, MessageFanoutService};
use crate::interface::grpc::{MessageActionGrpcHandler, MessageEventExecuteGrpcHandler};
use crate::interface::mq::StorageConsumerHandler;

/// 应用上下文 - 包含所有已初始化的服务
pub struct ApplicationContext {
    pub message_action_grpc: MessageActionGrpcHandler,
    pub message_event_execute_grpc: MessageEventExecuteGrpcHandler,
    /// `TOPIC_MESSAGE_MAIN` 消费者逻辑（与 [ConsumerConfig]、[Dispatcher] 配套使用）
    pub storage_consumer_handler: Arc<StorageConsumerHandler>,
    pub consumer_config: ConsumerConfig,
    pub main_queue_dispatcher: Arc<dyn Dispatcher>,
    pub retry_forwarder_dispatcher: Option<Arc<dyn Dispatcher>>,
    pub failure_publishers: ConsumerFailurePublishers,
    pub config: Arc<MessageOrchestratorConfig>,
}

/// 构建应用上下文
///
/// 类似 Go Wire 的 Initialize 函数，按照依赖顺序构建所有组件
pub async fn initialize(
    app_config: &flare_im_service_kit::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    let config = Arc::new(MessageOrchestratorConfig::from_app_config(app_config));

    let redis_client = build_redis_client(&config).await?;

    let mq_producer = build_mq_producer(&config).await?;
    let failure_publishers = build_failure_publishers(&config, mq_producer.clone());
    let retry_forwarder_dispatcher =
        build_retry_forwarder_dispatcher(&config, mq_producer.clone())?;

    let push_repository = MqPushRepository::new(mq_producer.clone());

    let conversation_service_type = config
        .conversation_service_type
        .as_deref()
        .unwrap_or(CONVERSATION);
    // 启动期 lazy：不等待 conversation 在 Consul 注册，首包 RPC 再建连。
    let conversation_channel =
        flare_im_service_kit::discovery::connect_grpc_channel_lazy_from_app_config(
            app_config,
            conversation_service_type,
            flare_im_service_kit::discovery::default_static_grpc_fallback(
                conversation_service_type,
            ),
        )
        .map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "lazy conversation channel ({conversation_service_type}) failed: {e}"
            ))
        })?;
    let conversation_repository = Arc::new(ConversationClient::new(conversation_channel));

    let recipient_repository: Arc<dyn crate::domain::repository::RecipientRepository> = Arc::new(
        RecipientRepositoryImpl::new(conversation_repository.clone()),
    );

    let sequence_allocator = SequenceAllocator::new(redis_client.clone(), 100)
        .await
        .map_err(|e| {
            flare_server_core::error::FlareError::system(format!("sequence allocator: {}", e))
        })?;

    let message_fanout_service = Arc::new(MessageFanoutService::new(push_repository.clone()));

    let event_domain_service = Arc::new(EventDomainService::new(
        push_repository.clone(),
        recipient_repository.clone(),
        Arc::new(sequence_allocator.clone()),
        None,
    ));

    let event_handler = Arc::new(EventHandler::new(event_domain_service.clone()));

    let message_event_execute_grpc = MessageEventExecuteGrpcHandler::new(event_handler.clone());
    let message_action_handler = Arc::new(MessageActionHandler::new(event_handler));

    let message_action_grpc = MessageActionGrpcHandler::new(message_action_handler);

    let storage_handler = Arc::new(StorageHandler::new(
        message_fanout_service,
        event_domain_service,
    ));
    let storage_consumer_handler = Arc::new(StorageConsumerHandler::new(storage_handler));

    let mut topic_dispatcher = TopicDispatcher::new();
    let mq_handler: Arc<dyn MqMessageHandler> = storage_consumer_handler.clone();
    topic_dispatcher
        .register(TOPIC_MESSAGE_MAIN.to_string(), mq_handler)
        .map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "register main queue dispatcher: {}",
                e
            ))
        })?;
    let main_queue_dispatcher: Arc<dyn Dispatcher> = Arc::new(topic_dispatcher);

    let consumer_config = ConsumerConfig::default()
        .with_concurrency(32)
        .with_ordered(true);

    Ok(ApplicationContext {
        message_action_grpc,
        message_event_execute_grpc,
        storage_consumer_handler,
        consumer_config,
        main_queue_dispatcher,
        retry_forwarder_dispatcher,
        failure_publishers,
        config,
    })
}

async fn build_redis_client(config: &MessageOrchestratorConfig) -> Result<Arc<redis::Client>> {
    let redis_url = config
        .redis_url
        .as_ref()
        .context("Redis URL not configured")?;

    let client = redis::Client::open(redis_url.as_str())
        .with_context(|| format!("failed to open Redis client for {}", redis_url))?;

    Ok(Arc::new(client))
}

async fn build_mq_producer(
    config: &MessageOrchestratorConfig,
) -> Result<Arc<dyn flare_server_core::mq::producer::Producer>> {
    match config.mq_backend.as_str() {
        "kafka" => {
            let producer = KafkaProducer::new(config).map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "failed to create Kafka producer: {}",
                    e
                ))
            })?;
            Ok(Arc::new(producer))
        }
        "nats" | "jetstream" => {
            let producer = NatsProducer::new(config).await.map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "failed to create JetStream producer: {}",
                    e
                ))
            })?;
            Ok(Arc::new(producer))
        }
        other => Err(flare_server_core::error::FlareError::system(format!(
            "unsupported mq backend: {}",
            other
        ))),
    }
}

fn build_failure_publishers(
    config: &MessageOrchestratorConfig,
    producer: Arc<dyn flare_server_core::mq::producer::Producer>,
) -> ConsumerFailurePublishers {
    let dlq = Arc::new(ProducerDeadLetterPublisher::new(
        producer.clone(),
        FailureTopic::fixed(config.message_dlq_topic.clone()),
    ));

    match config.mq_backend.as_str() {
        "kafka" => ConsumerFailurePublishers::new()
            .with_retry(Arc::new(
                ProducerRetryPublisher::new(
                    producer,
                    FailureTopic::fixed(config.message_retry_topic.clone()),
                )
                .with_not_before_delay(Duration::from_millis(config.message_retry_delay_ms.max(1))),
            ))
            .with_dead_letter(dlq),
        "nats" | "jetstream" => ConsumerFailurePublishers::new().with_dead_letter(dlq),
        _ => ConsumerFailurePublishers::new(),
    }
}

fn build_retry_forwarder_dispatcher(
    config: &MessageOrchestratorConfig,
    producer: Arc<dyn flare_server_core::mq::producer::Producer>,
) -> Result<Option<Arc<dyn Dispatcher>>> {
    if config.mq_backend.as_str() != "kafka" {
        return Ok(None);
    }

    let handler: Arc<dyn MqMessageHandler> =
        Arc::new(RetryForwarderHandler::new(producer).with_name("orchestrator-retry-forwarder"));
    let mut dispatcher = TopicDispatcher::new();
    dispatcher
        .register(config.message_retry_topic.clone(), handler)
        .map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "register orchestrator retry-forwarder consumer {}: {}",
                config.message_retry_topic, e
            ))
        })?;
    Ok(Some(Arc::new(dispatcher)))
}
