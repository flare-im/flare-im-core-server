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

use flare_im_service_kit::metrics::StorageWriterMetrics;
use flare_im_service_kit::redis_connection::SharedRedisConnection;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StorageFailurePolicy {
    retry_topic_enabled: bool,
    dead_letter_enabled: bool,
    retry_forwarder_enabled: bool,
}

fn storage_failure_policy_for_backend(backend: &str) -> StorageFailurePolicy {
    match backend {
        "kafka" => StorageFailurePolicy {
            retry_topic_enabled: true,
            dead_letter_enabled: true,
            retry_forwarder_enabled: true,
        },
        "nats" => StorageFailurePolicy {
            retry_topic_enabled: false,
            dead_letter_enabled: true,
            retry_forwarder_enabled: false,
        },
        _ => StorageFailurePolicy {
            retry_topic_enabled: false,
            dead_letter_enabled: false,
            retry_forwarder_enabled: false,
        },
    }
}

/// storage-writer 消费者配置。
///
/// NATS 后端没有重试 topic，落库失败靠 JetStream 原生重投：数据库、归档存储暂时不可用时
/// handler 报可重试错误，按 [`flare_im_service_kit::consumer_retry`] 的退避延迟重投，12 次重试的
/// 等待合计 135 s，之后仍失败才进 DLQ。默认的 3 次立即重投会在数据库重启的同一瞬间用完，已
/// 受理的消息随即进 DLQ。Kafka 后端走重试 topic（`message_retry_delay_ms`），不变。
pub(crate) fn storage_consumer_config(config: &StorageWriterConfig) -> ConsumerConfig {
    let consumer = ConsumerConfig::default()
        .with_concurrency(32)
        .with_batch_size(config.max_poll_records.max(1))
        .with_batch_timeout_ms(config.fetch_max_wait_ms.max(1));
    if storage_failure_policy_for_backend(config.mq_backend.as_str()).retry_topic_enabled {
        consumer
    } else {
        flare_im_service_kit::consumer_retry::with_downstream_outage_retry(consumer)
    }
}

pub struct ApplicationContext {
    pub audit_delivery: Option<(
        Arc<sqlx::PgPool>,
        flare_im_service_kit::audit_effects::AuditEffects,
    )>,
    pub config: Arc<StorageWriterConfig>,
    pub consumer_config: ConsumerConfig,
    pub dispatcher: Arc<dyn Dispatcher>,
    pub retry_forwarder_dispatcher: Option<Arc<dyn Dispatcher>>,
    pub failure_publishers: ConsumerFailurePublishers,
}

pub async fn initialize(
    app_config: &flare_im_service_kit::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    let config = Arc::new(
        StorageWriterConfig::from_app_config(app_config)
            .with_context(|| "Failed to load storage writer service configuration")?,
    );

    let mq_producer = build_mq_producer(&config).await?;
    let ack_publisher = build_ack_publisher(&config, mq_producer.clone())?;
    let failure_publishers = build_failure_publishers(&config, mq_producer.clone());
    let retry_forwarder_dispatcher = build_retry_forwarder_dispatcher(&config, mq_producer)?;
    // 一个进程一条共享的 Redis 连接，去重、热缓存与 WAL 清理共用（有界建连、应答与重建）。
    let redis = build_redis_client(&config)
        .map(|client| Arc::new(SharedRedisConnection::new(client, config.redis_limits)));

    let idempotency_repo = redis
        .as_ref()
        .map(|redis| Arc::new(RedisIdempotencyRepository::new(redis.clone(), &config)));

    let hot_cache_repo = redis
        .as_ref()
        .map(|redis| Arc::new(RedisHotCacheRepository::new(redis.clone(), &config)));

    // metrics 要早于第一个使用者构造：WAL 清理仓储也要记 Redis 更新耗时。
    let metrics = Arc::new(StorageWriterMetrics::new());

    let wal_cleanup_repo = match (&redis, &config.wal_hash_key) {
        (Some(redis), Some(key)) => Some(Arc::new(
            RedisWalCleanupRepository::new(redis.clone(), key.clone())
                .with_metrics(Some(metrics.clone())),
        )),
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
        .with_write_ledger_repo(Some(write_ledger_repo))
        .with_metrics(Some(metrics.clone())),
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

    let message_event_handler = MessageCreatedConsumerFactory::create_handler(command_handler)?;
    let publisher =
        flare_im_service_kit::audit_effects::AuditEffects::from_env("storage-writer").await?;
    let audit_delivery = publisher
        .enabled()
        .then(|| (Arc::new(archive_repo.pool().clone()), publisher));
    let operation_event_handler =
        MessageEventsConsumerFactory::create_handler(operation_command_handler);

    let consumer_cfg = storage_consumer_config(&config);

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
        audit_delivery,
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
        "nats" => Arc::new(NatsProducer::new(config.as_ref()).await.map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "Failed to create JetStream producer: {}",
                e
            ))
        })?),
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
    let policy = storage_failure_policy_for_backend(config.mq_backend.as_str());
    let mut publishers = ConsumerFailurePublishers::new();

    if policy.retry_topic_enabled {
        publishers = publishers.with_retry(Arc::new(
            ProducerRetryPublisher::new(
                producer.clone(),
                FailureTopic::fixed(config.message_retry_topic.clone()),
            )
            .with_not_before_delay(Duration::from_millis(config.message_retry_delay_ms.max(1))),
        ));
    }

    if policy.dead_letter_enabled {
        publishers = publishers.with_dead_letter(Arc::new(ProducerDeadLetterPublisher::new(
            producer,
            FailureTopic::fixed(config.message_dlq_topic.clone()),
        )));
    }

    publishers
}

fn build_retry_forwarder_dispatcher(
    config: &StorageWriterConfig,
    producer: Arc<dyn Producer>,
) -> Result<Option<Arc<dyn Dispatcher>>> {
    if !storage_failure_policy_for_backend(config.mq_backend.as_str()).retry_forwarder_enabled {
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

fn build_redis_client(config: &Arc<StorageWriterConfig>) -> Option<redis::Client> {
    config
        .redis_url
        .as_ref()
        .and_then(|url| match redis::Client::open(url.as_str()) {
            Ok(client) => Some(client),
            Err(err) => {
                warn!(error = ?err, "Redis init failed; Redis-backed features disabled");
                None
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kafka_failure_policy_uses_retry_topic_dlq_and_retry_forwarder() {
        let policy = storage_failure_policy_for_backend("kafka");

        assert!(policy.retry_topic_enabled);
        assert!(policy.dead_letter_enabled);
        assert!(policy.retry_forwarder_enabled);
    }

    #[test]
    fn nats_failure_policy_uses_native_retry_and_dlq_without_retry_forwarder() {
        let policy = storage_failure_policy_for_backend("nats");
        assert!(!policy.retry_topic_enabled);
        assert!(policy.dead_letter_enabled);
        assert!(!policy.retry_forwarder_enabled);

        let policy = storage_failure_policy_for_backend("jetstream");
        assert!(!policy.retry_topic_enabled);
        assert!(!policy.dead_letter_enabled);
        assert!(!policy.retry_forwarder_enabled);
    }

    fn writer_config(mq_backend: &str) -> StorageWriterConfig {
        StorageWriterConfig {
            mq_backend: mq_backend.into(),
            jetstream_url: "nats://127.0.0.1:1".into(),
            jetstream_group: "storage-writer".into(),
            jetstream_ack_topic: None,
            jetstream_timeout_ms: 1_000,
            jetstream_retries: 0,
            jetstream_retry_backoff_ms: 0,
            jetstream_stream_specs: flare_server_core::mq::nats::default_stream_specs(),
            kafka_brokers: Vec::new(),
            kafka_client_id: String::new(),
            kafka_options: Default::default(),
            message_dlq_topic: String::new(),
            message_retry_topic: String::new(),
            message_retry_delay_ms: 5_000,
            max_poll_records: 64,
            fetch_min_bytes: 1,
            fetch_max_wait_ms: 50,
            redis_url: None,
            redis_hot_ttl_seconds: 0,
            redis_hot_tail_limit: 0,
            redis_idempotency_ttl_seconds: 0,
            redis_limits: Default::default(),
            wal_hash_key: None,
            postgres_url: None,
            postgres_max_connections: 1,
            postgres_min_connections: 0,
            postgres_acquire_timeout_seconds: 1,
            postgres_idle_timeout_seconds: 1,
            postgres_max_lifetime_seconds: 1,
            media_service_endpoint: None,
            metrics: flare_im_service_kit::metrics::MetricsEndpointConfig::new("127.0.0.1", 0),
        }
    }

    /// 门禁：NATS 后端的落库消费者按退避重投，重试预算覆盖数据库停机与重启窗口，且首投加
    /// 全部重试落在 storage-writer 自己的 JetStream 消费者的 `max_deliver` 之内。Kafka 后端走
    /// 重试 topic，不变。
    #[test]
    fn nats_storage_retries_span_a_restart_window_inside_max_deliver() {
        use flare_server_core::mq::nats::NatsConsumerConfig;

        let config = writer_config("nats");
        let consumer = storage_consumer_config(&config);
        let max_deliver = config.max_deliver();
        let window = flare_im_service_kit::consumer_retry::check_downstream_outage_retry(
            &consumer,
            max_deliver,
        )
        .unwrap_or_else(|error| panic!("storage-writer retry budget: {error}"));
        println!(
            "storage-writer retries {} over {window:?}, max_deliver {max_deliver}",
            consumer.max_retries
        );
        assert_eq!(consumer.batch_size, 64);
        assert!(consumer.enable_dlq);

        let kafka = storage_consumer_config(&writer_config("kafka"));
        assert!(kafka.retry_backoff.is_none());
        assert_eq!(kafka.max_retries, ConsumerConfig::default().max_retries);
    }
}
