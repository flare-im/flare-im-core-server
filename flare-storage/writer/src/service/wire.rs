//! Wire 风格依赖注入：仅组装「消息与操作消息存储」相关组件

use std::sync::Arc;
use std::time::Duration;

use flare_server_core::error::{AnyhowContext, Result};
use tracing::warn;

use crate::application::handlers::{
    MessageOperationCommandHandler, MessagePersistenceCommandHandler,
};
use crate::config::StorageWriterConfig;
use crate::domain::repository::MessageWriteLedgerRepository;
use crate::domain::service::{EventApplicationService, MessagePersistenceDomainService};
use crate::infrastructure::messaging::ack_publisher::MqAckPublisher;
use crate::infrastructure::persistence::repository::event_stream::PostgresEventStreamStore;
use crate::infrastructure::persistence::repository::postgres_store::PostgresMessageStore;
use crate::infrastructure::persistence::repository::redis_cache::RedisHotCacheRepository;
use crate::infrastructure::persistence::repository::redis_idempotency::RedisIdempotencyRepository;
use crate::infrastructure::persistence::repository::redis_wal_cleanup::RedisWalCleanupRepository;
use crate::interface::messaging::{MessageCreatedConsumerFactory, MessageEventsConsumerFactory};

use flare_im_core::metrics::StorageWriterMetrics;
use flare_server_core::mq::consumer::dispatcher::Dispatcher;
use flare_server_core::mq::consumer::{ConsumerConfig, MessageHandler, TopicDispatcher};
use flare_server_core::mq::consumer::{
    ConsumerFailurePublishers, FailureTopic, ProducerDeadLetterPublisher, ProducerRetryPublisher,
    RetryForwarderHandler,
};
use flare_server_core::mq::kafka::KafkaProducer;
use flare_server_core::mq::nats::NatsProducer;
use flare_server_core::mq::producer::Producer;

// 类型别名，简化泛型参数
type IdempotencyRepo = RedisIdempotencyRepository;
type HotCacheRepo = RedisHotCacheRepository;
type ArchiveRepo = PostgresMessageStore;
type EventStreamRepo = PostgresEventStreamStore;
type WalCleanupRepo = RedisWalCleanupRepository;
type AckPub = MqAckPublisher;

type MessagePersistenceService = MessagePersistenceDomainService<
    IdempotencyRepo,
    HotCacheRepo,
    ArchiveRepo,
    EventStreamRepo,
    WalCleanupRepo,
    AckPub,
>;

type MessagePersistenceHandler = MessagePersistenceCommandHandler<
    IdempotencyRepo,
    HotCacheRepo,
    ArchiveRepo,
    EventStreamRepo,
    WalCleanupRepo,
    AckPub,
>;

type EventApplicationServiceType = EventApplicationService<ArchiveRepo, EventStreamRepo>;

pub struct ApplicationContext {
    pub config: Arc<StorageWriterConfig>,
    pub consumer_config: ConsumerConfig,
    pub dispatcher: Arc<dyn Dispatcher>,
    pub retry_forwarder_dispatcher: Option<Arc<dyn Dispatcher>>,
    pub failure_publishers: ConsumerFailurePublishers,
}

pub async fn initialize(
    app_config: &flare_im_core::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    let config = Arc::new(
        StorageWriterConfig::from_app_config(app_config)
            .with_context(|| "Failed to load storage writer service configuration")?,
    );

    let mq_producer = build_mq_producer(&config).await?;
    let ack_publisher = build_ack_publisher(&config, mq_producer.clone())?;
    let failure_publishers = build_failure_publishers(&config, mq_producer.clone());
    let retry_forwarder_dispatcher = build_retry_forwarder_dispatcher(&config, mq_producer)?;
    let redis_client = build_redis_client(&config);

    let idempotency_repo = redis_client
        .as_ref()
        .map(|client| Arc::new(RedisIdempotencyRepository::new(client.clone(), &config)));

    let hot_cache_repo = redis_client
        .as_ref()
        .map(|client| Arc::new(RedisHotCacheRepository::new(client.clone(), &config)));

    let wal_cleanup_repo = match (&redis_client, &config.wal_hash_key) {
        (Some(client), Some(key)) => Some(Arc::new(RedisWalCleanupRepository::new(
            client.clone(),
            key.clone(),
        ))),
        _ => None,
    };

    let archive_repo = Arc::new(PostgresMessageStore::new(&config).await?.ok_or_else(|| {
        flare_server_core::error::FlareError::system(
            "PostgreSQL archive store is required".to_string(),
        )
    })?);

    let event_stream_repo = Arc::new(
        PostgresEventStreamStore::from_config(&config)
            .await?
            .ok_or_else(|| {
                flare_server_core::error::FlareError::system(
                    "PostgreSQL event stream store is required".to_string(),
                )
            })?,
    );

    let metrics = Arc::new(StorageWriterMetrics::new());

    // 使用具体类型创建 domain_service
    let write_ledger_repo: Arc<dyn MessageWriteLedgerRepository> = archive_repo.clone();
    let domain_service: Arc<MessagePersistenceService> = Arc::new(
        MessagePersistenceDomainService::new(
            idempotency_repo,
            hot_cache_repo,
            Some(archive_repo.clone()),
            Some(event_stream_repo.clone()),
            wal_cleanup_repo,
            Some(ack_publisher),
        )
        .with_write_ledger_repo(Some(write_ledger_repo)),
    );

    let event_service: Arc<EventApplicationServiceType> = Arc::new(EventApplicationService::new(
        Some(archive_repo.clone()),
        Some(event_stream_repo),
    ));

    let command_handler: Arc<MessagePersistenceHandler> = Arc::new(
        MessagePersistenceCommandHandler::new(domain_service, metrics.clone()),
    );

    let operation_command_handler = Arc::new(MessageOperationCommandHandler::new(
        event_service,
        metrics.clone(),
    ));

    let message_event_handler = MessageCreatedConsumerFactory::create_handler(command_handler);
    let operation_event_handler =
        MessageEventsConsumerFactory::create_handler(operation_command_handler);

    let consumer_cfg = ConsumerConfig::default()
        .with_concurrency(32)
        .with_batch_size(config.max_poll_records.max(1))
        .with_batch_timeout_ms(config.fetch_max_wait_ms.max(1));

    let mut dispatcher = TopicDispatcher::new();
    let message_adapter: Arc<dyn MessageHandler> = message_event_handler;
    Dispatcher::register(
        &mut dispatcher,
        MessageCreatedConsumerFactory::topic().to_string(),
        message_adapter,
    )
    .map_err(|err| {
        flare_server_core::error::FlareError::system(format!(
            "register storage writer consumer {}: {err}",
            MessageCreatedConsumerFactory::topic()
        ))
    })?;
    let operation_adapter: Arc<dyn MessageHandler> = operation_event_handler;
    Dispatcher::register(
        &mut dispatcher,
        MessageEventsConsumerFactory::topic().to_string(),
        operation_adapter,
    )
    .map_err(|err| {
        flare_server_core::error::FlareError::system(format!(
            "register storage writer consumer {}: {err}",
            MessageEventsConsumerFactory::topic()
        ))
    })?;

    Ok(ApplicationContext {
        config,
        consumer_config: consumer_cfg,
        dispatcher: Arc::new(dispatcher),
        retry_forwarder_dispatcher,
        failure_publishers,
    })
}

fn build_ack_publisher(
    config: &Arc<StorageWriterConfig>,
    producer: Arc<dyn Producer>,
) -> Result<Arc<AckPub>> {
    let topic = config.jetstream_ack_topic.as_ref().ok_or_else(|| {
        flare_server_core::error::FlareError::system(
            "STORAGE_JETSTREAM_ACK_SUBJECT is required".to_string(),
        )
    })?;
    Ok(Arc::new(MqAckPublisher::new(
        producer,
        config.clone(),
        topic.clone(),
    )))
}

async fn build_mq_producer(config: &Arc<StorageWriterConfig>) -> Result<Arc<dyn Producer>> {
    let producer: Arc<dyn Producer> = match config.mq_backend.as_str() {
        "kafka" => Arc::new(KafkaProducer::new(config.as_ref()).map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "Failed to create Kafka producer: {}",
                e
            ))
        })?),
        "nats" | "jetstream" => {
            Arc::new(NatsProducer::new(config.as_ref()).await.map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "Failed to create JetStream producer: {}",
                    e
                ))
            })?)
        }
        other => {
            return Err(flare_server_core::error::FlareError::system(format!(
                "unsupported mq backend: {other}"
            )));
        }
    };
    Ok(producer)
}

fn build_failure_publishers(
    config: &StorageWriterConfig,
    producer: Arc<dyn Producer>,
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
    config: &StorageWriterConfig,
    producer: Arc<dyn Producer>,
) -> Result<Option<Arc<dyn Dispatcher>>> {
    if config.mq_backend.as_str() != "kafka" {
        return Ok(None);
    }

    let handler: Arc<dyn MessageHandler> =
        Arc::new(RetryForwarderHandler::new(producer).with_name("storage-retry-forwarder"));
    let mut dispatcher = TopicDispatcher::new();
    Dispatcher::register(&mut dispatcher, config.message_retry_topic.clone(), handler).map_err(
        |err| {
            flare_server_core::error::FlareError::system(format!(
                "register storage retry-forwarder consumer {}: {err}",
                config.message_retry_topic
            ))
        },
    )?;
    Ok(Some(Arc::new(dispatcher)))
}

fn build_redis_client(config: &Arc<StorageWriterConfig>) -> Option<Arc<redis::Client>> {
    config
        .redis_url
        .as_ref()
        .and_then(|url| match redis::Client::open(url.as_str()) {
            Ok(client) => Some(Arc::new(client)),
            Err(err) => {
                warn!(error = ?err, "Redis init failed; Redis-backed features disabled");
                None
            }
        })
}
