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
use crate::domain::service::{
    EventDomainService, MessageFanoutService, UserSyncCompensationWorker,
};
use crate::infrastructure::persistence::{
    RedisReadWatermarkRepository, RedisUserSyncCompensationRepository, RedisUserSyncIndexRepository,
};
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
    pub user_sync_compensation_worker: Arc<UserSyncCompensationWorker>,
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

    let push_repository = MqPushRepository::from_env(mq_producer.clone())?;
    let user_sync_index_repository: Arc<dyn crate::domain::repository::UserSyncIndexRepository> =
        Arc::new(RedisUserSyncIndexRepository::new(
            redis_client.clone(),
            config.user_sync_index_max_changes_per_user,
            config.user_sync_index_ttl_seconds,
        ));
    let user_sync_compensation_repository: Arc<
        dyn crate::domain::repository::UserSyncCompensationRepository,
    > = Arc::new(RedisUserSyncCompensationRepository::new(
        redis_client.clone(),
    ));
    let user_sync_compensation_worker = Arc::new(UserSyncCompensationWorker::new(
        user_sync_compensation_repository.clone(),
        user_sync_index_repository.clone(),
    ));

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
    // 事件收件人要读单聊成员：orchestrator 的入口（MQ 与动作 RPC）都没有可转发的签名，
    // 以本服务身份签读请求（会话服务按租户读成员，不要求是成员）。
    let read_signer = flare_im_service_kit::signed_read::service_signer_from_env(
        flare_im_contracts::service_names::ORCHESTRATOR,
    )?;
    if read_signer.is_none() {
        tracing::warn!(
            "FLARE_INTERNAL_CTX_PRIVATE_KEY is missing: conversation member reads go unsigned"
        );
    }
    let conversation_repository = Arc::new(
        ConversationClient::new(conversation_channel)
            .with_service_read_signer(read_signer, flare_im_contracts::service_names::ORCHESTRATOR),
    );

    let recipient_repository: Arc<dyn crate::domain::repository::RecipientRepository> = Arc::new(
        RecipientRepositoryImpl::new(conversation_repository.clone()),
    );

    let sequence_allocator = SequenceAllocator::new(redis_client.clone(), 100)
        .await
        .map_err(|e| {
            flare_server_core::error::FlareError::system(format!("sequence allocator: {}", e))
        })?;

    // Client read acks that do not raise the reader's watermark are dropped before they
    // get a seq; see EventDomainService::admit_event.
    let read_watermark_connection = redis_client.get_connection_manager().await.map_err(|e| {
        flare_server_core::error::FlareError::system(format!(
            "read watermark redis connection: {}",
            e
        ))
    })?;
    let read_watermark_repository: Arc<dyn crate::domain::repository::ReadWatermarkRepository> =
        Arc::new(RedisReadWatermarkRepository::new(
            read_watermark_connection,
            RedisReadWatermarkRepository::DEFAULT_TTL_SECONDS,
        ));

    let message_fanout_service = Arc::new(
        MessageFanoutService::new(push_repository.clone())
            .with_inline_message_push_enabled(config.inline_message_push_enabled)
            .with_user_sync_index(user_sync_index_repository.clone())
            .with_user_sync_compensation(user_sync_compensation_repository.clone()),
    );

    let event_domain_service = Arc::new(
        EventDomainService::new(
            push_repository.clone(),
            recipient_repository.clone(),
            Arc::new(sequence_allocator.clone()),
            None,
        )
        .with_user_sync_index(user_sync_index_repository)
        .with_user_sync_compensation(user_sync_compensation_repository)
        .with_read_watermark(read_watermark_repository),
    );

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

    let consumer_config = main_queue_consumer_config(&config.mq_backend);

    Ok(ApplicationContext {
        message_action_grpc,
        message_event_execute_grpc,
        storage_consumer_handler,
        consumer_config,
        main_queue_dispatcher,
        retry_forwarder_dispatcher,
        failure_publishers,
        config,
        user_sync_compensation_worker,
    })
}

/// `TOPIC_MESSAGE_MAIN` 消费者配置。
///
/// NATS 后端没有重试 topic，失败的消息靠 JetStream 原生重投：扇出或下游（storage-writer 的流、
/// 会话服务、Redis）暂时不可用时 handler 报可重试错误，按
/// [`flare_im_service_kit::consumer_retry`] 的退避延迟重投，12 次重试的等待合计 135 s，之后仍
/// 失败才进 DLQ。默认的 3 次立即重投会在下游停机的同一瞬间用完。Kafka 后端走重试 topic
/// （`message_retry_delay_ms`），不变。
pub(crate) fn main_queue_consumer_config(mq_backend: &str) -> ConsumerConfig {
    let config = ConsumerConfig::default()
        .with_concurrency(32)
        .with_ordered(true);
    if mq_backend == "nats" {
        flare_im_service_kit::consumer_retry::with_downstream_outage_retry(config)
    } else {
        config
    }
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
        "nats" => {
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
        "nats" => ConsumerFailurePublishers::new().with_dead_letter(dlq),
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

#[cfg(test)]
mod consumer_retry_tests {
    use super::*;
    use flare_server_core::mq::nats::NatsConsumerConfig;

    /// 门禁：NATS 后端的主队列消费者按退避重投，重试预算覆盖下游停机与重启窗口，且首投加
    /// 全部重试落在编排服务 JetStream 消费者的 `max_deliver` 之内。Kafka 后端不变。
    #[test]
    fn nats_main_queue_retries_span_a_restart_window_inside_max_deliver() {
        let consumer = main_queue_consumer_config("nats");
        let max_deliver = MessageOrchestratorConfig::default().max_deliver();
        let window = flare_im_service_kit::consumer_retry::check_downstream_outage_retry(
            &consumer,
            max_deliver,
        )
        .unwrap_or_else(|error| panic!("orchestrator retry budget: {error}"));
        println!(
            "orchestrator retries {} over {window:?}, max_deliver {max_deliver}",
            consumer.max_retries
        );
        assert!(consumer.ordered);
        assert!(consumer.enable_dlq);

        let kafka = main_queue_consumer_config("kafka");
        assert!(kafka.retry_backoff.is_none());
        assert_eq!(kafka.max_retries, ConsumerConfig::default().max_retries);
    }
}
