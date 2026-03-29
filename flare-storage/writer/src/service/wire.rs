//! Wire 风格依赖注入：仅组装「消息与操作消息存储」相关组件

use std::sync::Arc;

use anyhow::{Context as AnyhowContext, Result};
use tracing::warn;

use crate::application::handlers::{
    MessageOperationCommandHandler, MessagePersistenceCommandHandler,
};
use crate::config::StorageWriterConfig;
use crate::domain::repository::{
    AckPublisher, ArchiveStoreRepository, EventStreamRepository, HotCacheRepository,
    MessageIdempotencyRepository, WalCleanupRepository,
};
use crate::domain::service::{EventApplicationService, MessagePersistenceDomainService};
use crate::infrastructure::messaging::ack_publisher::MqAckPublisher;
use crate::infrastructure::persistence::repository::event_stream::PostgresEventStreamStore;
use crate::infrastructure::persistence::repository::postgres_store::PostgresMessageStore;
use crate::infrastructure::persistence::repository::redis_cache::RedisHotCacheRepository;
use crate::infrastructure::persistence::repository::redis_idempotency::RedisIdempotencyRepository;
use crate::infrastructure::persistence::repository::redis_wal_cleanup::RedisWalCleanupRepository;
use crate::interface::messaging::{MessageEventConsumerFactory, OperationEventConsumerFactory};

use flare_im_core::metrics::StorageWriterMetrics;
use flare_server_core::event_bus::{EventHandler, MqEventHandler};
use flare_server_core::kafka::build_kafka_producer;
use flare_server_core::mq::consumer::dispatcher::Dispatcher;
use flare_server_core::mq::consumer::{ConsumerConfig, MessageHandler, TopicDispatcher};

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
}

pub async fn initialize(
    app_config: &flare_im_core::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    let config = Arc::new(
        StorageWriterConfig::from_app_config(app_config)
            .with_context(|| "Failed to load storage writer service configuration")?,
    );

    let ack_publisher = build_ack_publisher(&config)?;
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

    let archive_repo = match PostgresMessageStore::new(&config).await {
        Ok(Some(store)) => Some(Arc::new(store)),
        Ok(None) => None,
        Err(err) => {
            warn!(error = ?err, "PostgreSQL init failed, archive storage disabled");
            None
        }
    };

    let event_stream_repo = match PostgresEventStreamStore::from_config(&config).await {
        Ok(Some(store)) => Some(Arc::new(store)),
        Ok(None) => None,
        Err(err) => {
            warn!(error = ?err, "Event stream store init failed, sync event stream disabled");
            None
        }
    };

    let metrics = Arc::new(StorageWriterMetrics::new());

    // 使用具体类型创建 domain_service
    let domain_service: Arc<MessagePersistenceService> =
        Arc::new(MessagePersistenceDomainService::new(
            idempotency_repo,
            hot_cache_repo,
            archive_repo.clone(),
            event_stream_repo.clone(),
            wal_cleanup_repo,
            ack_publisher,
        ));

    let event_service: Arc<EventApplicationServiceType> = Arc::new(EventApplicationService::new(
        archive_repo.clone(),
        event_stream_repo,
    ));

    let command_handler: Arc<MessagePersistenceHandler> = Arc::new(
        MessagePersistenceCommandHandler::new(domain_service, metrics.clone()),
    );

    let operation_command_handler = Arc::new(MessageOperationCommandHandler::new(
        event_service,
        metrics.clone(),
    ));

    let message_event_handler: Arc<dyn EventHandler> =
        MessageEventConsumerFactory::create_handler(command_handler);
    let operation_event_handler: Arc<dyn EventHandler> =
        OperationEventConsumerFactory::create_handler(operation_command_handler);

    let consumer_cfg = ConsumerConfig::default().with_concurrency(32);

    let mut dispatcher = TopicDispatcher::new();
    let message_adapter: Arc<dyn MessageHandler> =
        Arc::new(MqEventHandler::new(message_event_handler));
    Dispatcher::register(
        &mut dispatcher,
        MessageEventConsumerFactory::topic().to_string(),
        message_adapter,
    )?;
    let operation_adapter: Arc<dyn MessageHandler> =
        Arc::new(MqEventHandler::new(operation_event_handler));
    Dispatcher::register(
        &mut dispatcher,
        OperationEventConsumerFactory::topic().to_string(),
        operation_adapter,
    )?;

    Ok(ApplicationContext {
        config,
        consumer_config: consumer_cfg,
        dispatcher: Arc::new(dispatcher),
    })
}

fn build_ack_publisher(config: &Arc<StorageWriterConfig>) -> Result<Option<Arc<AckPub>>> {
    if let Some(topic) = &config.kafka_ack_topic {
        let producer = build_kafka_producer(config.as_ref())
            .map_err(|e| anyhow::anyhow!("Failed to create Kafka producer for ACK: {}", e))?;
        let producer = Arc::new(producer);
        let publisher = Arc::new(MqAckPublisher::new(producer, config.clone(), topic.clone()));
        Ok(Some(publisher))
    } else {
        Ok(None)
    }
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
