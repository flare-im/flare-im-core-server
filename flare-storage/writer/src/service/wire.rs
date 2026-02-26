//! Wire 风格依赖注入：仅组装「消息与操作消息存储」相关组件

use std::sync::Arc;

use anyhow::{Context as AnyhowContext, Result};
use tracing::warn;

use crate::application::handlers::{MessageOperationCommandHandler, MessagePersistenceCommandHandler};
use crate::config::StorageWriterConfig;
use crate::domain::repository::{
    AckPublisher, ArchiveStoreRepository, HotCacheRepository, MessageIdempotencyRepository,
    WalCleanupRepository,
};
use crate::domain::service::{MessageOperationDomainService, MessagePersistenceDomainService};
use crate::infrastructure::messaging::ack_publisher::KafkaAckPublisher;
use crate::infrastructure::persistence::postgres_store::PostgresMessageStore;
use crate::infrastructure::persistence::redis_cache::RedisHotCacheRepository;
use crate::infrastructure::persistence::redis_idempotency::RedisIdempotencyRepository;
use crate::infrastructure::persistence::redis_wal_cleanup::RedisWalCleanupRepository;
use crate::interface::messaging::normal_consumer::NormalMessageConsumer;
use crate::interface::messaging::operation_consumer::OperationMessageConsumer;

use flare_im_core::metrics::StorageWriterMetrics;
use flare_server_core::kafka::build_kafka_producer;

pub struct ApplicationContext {
    pub normal_consumer: NormalMessageConsumer,
    pub operation_consumer: OperationMessageConsumer,
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

    let idempotency_repo = redis_client.as_ref().map(|client| {
        Arc::new(RedisIdempotencyRepository::new(client.clone(), &config))
            as Arc<dyn MessageIdempotencyRepository + Send + Sync>
    });

    let hot_cache_repo = redis_client.as_ref().map(|client| {
        Arc::new(RedisHotCacheRepository::new(client.clone(), &config))
            as Arc<dyn HotCacheRepository + Send + Sync>
    });

    let wal_cleanup_repo = match (&redis_client, &config.wal_hash_key) {
        (Some(client), Some(key)) => Some(
            Arc::new(RedisWalCleanupRepository::new(client.clone(), key.clone()))
                as Arc<dyn WalCleanupRepository + Send + Sync>,
        ),
        _ => None,
    };

    let archive_repo: Option<Arc<dyn ArchiveStoreRepository + Send + Sync>> =
        match PostgresMessageStore::new(&config).await {
            Ok(Some(store)) => Some(Arc::new(store) as Arc<dyn ArchiveStoreRepository + Send + Sync>),
            Ok(None) => None,
            Err(err) => {
                warn!(error = ?err, "PostgreSQL init failed, archive storage disabled");
                None
            }
        };

    let metrics = Arc::new(StorageWriterMetrics::new());

    let domain_service = Arc::new(MessagePersistenceDomainService::new(
        idempotency_repo,
        hot_cache_repo,
        archive_repo.clone(),
        wal_cleanup_repo,
        ack_publisher,
    ));

    let operation_service = Arc::new(MessageOperationDomainService::new(archive_repo.clone()));

    let command_handler = Arc::new(MessagePersistenceCommandHandler::new(
        domain_service,
        operation_service.clone(),
        metrics.clone(),
    ));

    let operation_command_handler = Arc::new(MessageOperationCommandHandler::new(
        operation_service,
        metrics.clone(),
    ));

    let normal_consumer = NormalMessageConsumer::new(config.clone(), command_handler.clone(), metrics.clone())
        .await
        .with_context(|| "Failed to create NormalMessageConsumer")?;

    let operation_consumer = OperationMessageConsumer::new(config.clone(), operation_command_handler, metrics.clone())
        .await
        .with_context(|| "Failed to create OperationMessageConsumer")?;

    Ok(ApplicationContext {
        normal_consumer,
        operation_consumer,
    })
}

fn build_ack_publisher(
    config: &Arc<StorageWriterConfig>,
) -> Result<Option<Arc<dyn AckPublisher + Send + Sync>>> {
    if let Some(topic) = &config.kafka_ack_topic {
        let producer = build_kafka_producer(
            config.as_ref() as &dyn flare_server_core::kafka::KafkaProducerConfig,
        )
        .with_context(|| "Failed to create Kafka producer for ACK")?;
        let producer = Arc::new(producer);
        let publisher: Arc<dyn AckPublisher + Send + Sync> =
            Arc::new(KafkaAckPublisher::new(producer, config.clone(), topic.clone()));
        Ok(Some(publisher))
    } else {
        Ok(None)
    }
}

fn build_redis_client(config: &Arc<StorageWriterConfig>) -> Option<Arc<redis::Client>> {
    config.redis_url.as_ref().and_then(|url| {
        match redis::Client::open(url.as_str()) {
            Ok(client) => Some(Arc::new(client)),
            Err(err) => {
                warn!(error = ?err, "Redis init failed; Redis-backed features disabled");
                None
            }
        }
    })
}
