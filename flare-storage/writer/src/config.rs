//! Storage Writer 配置
//!
//! **架构落地**：消费 **TOPIC_MESSAGE_EVENTS**（统一事件流），
//! 与 Orchestrator 单事件流对齐，处理 message.created 和 operation.* 事件。

use anyhow::Result;
use flare_im_core::config::FlareAppConfig;
use flare_im_core::constants::groups::STORAGE_GROUP_DEFAULT;
use flare_server_core::mq::kafka::{KafkaConsumerConfig, KafkaProducerConfig};
use flare_server_core::mq::nats::{
    NatsConsumerConfig, NatsProducerConfig, NatsStreamSpec, default_stream_specs,
};
use std::{collections::HashMap, env};

#[derive(Clone, Debug)]
pub struct StorageWriterConfig {
    pub mq_backend: String,
    pub jetstream_url: String,
    pub jetstream_group: String,
    pub jetstream_ack_topic: Option<String>,
    pub jetstream_timeout_ms: u64,
    pub jetstream_retries: u32,
    pub jetstream_retry_backoff_ms: u64,
    pub jetstream_stream_specs: Vec<NatsStreamSpec>,
    pub kafka_brokers: Vec<String>,
    pub kafka_client_id: String,
    pub kafka_options: HashMap<String, String>,

    // 批量消费配置
    pub max_poll_records: usize,
    pub fetch_min_bytes: usize,
    pub fetch_max_wait_ms: u64,
    pub redis_url: Option<String>,
    pub redis_hot_ttl_seconds: u64,
    pub redis_idempotency_ttl_seconds: u64,
    pub wal_hash_key: Option<String>,
    pub postgres_url: Option<String>,
    // PostgreSQL 连接池配置
    pub postgres_max_connections: u32,
    pub postgres_min_connections: u32,
    pub postgres_acquire_timeout_seconds: u64,
    pub postgres_idle_timeout_seconds: u64,
    pub postgres_max_lifetime_seconds: u64,
    pub media_service_endpoint: Option<String>,
}

impl StorageWriterConfig {
    /// 从应用配置加载（新方式，推荐）
    pub fn from_app_config(app: &FlareAppConfig) -> Result<Self> {
        let service_config = app.storage_writer_service();
        let mq_backend = app.mq_default_backend().to_string();
        let kafka_profile = app.kafka_profile("message");
        let jetstream_stream_specs = {
            let specs = app
                .jetstream_topology()
                .into_iter()
                .map(|spec| NatsStreamSpec::new(spec.stream_name, spec.subjects))
                .collect::<Vec<_>>();
            if specs.is_empty() {
                default_stream_specs()
            } else {
                specs
            }
        };

        // 解析 JetStream 配置引用
        let jetstream_url = env::var("STORAGE_JETSTREAM_URL")
            .ok()
            .or_else(|| {
                if let Some(jetstream_name) = &service_config.jetstream {
                    app.jetstream_profile(jetstream_name)
                        .map(|profile| profile.url.clone())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "nats://127.0.0.1:24222".to_string());

        let jetstream_group = env::var("STORAGE_JETSTREAM_GROUP")
            .ok()
            .or_else(|| service_config.consumer_group.clone())
            .unwrap_or_else(|| STORAGE_GROUP_DEFAULT.to_string());

        let jetstream_ack_topic = env::var("STORAGE_JETSTREAM_ACK_SUBJECT").ok();

        let jetstream_timeout_ms = env::var("STORAGE_JETSTREAM_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| {
                service_config
                    .jetstream
                    .as_ref()
                    .and_then(|jetstream_name| app.jetstream_profile(jetstream_name))
                    .and_then(|profile| profile.timeout_ms)
            })
            .unwrap_or(5000);
        let jetstream_profile = service_config
            .jetstream
            .as_ref()
            .and_then(|jetstream_name| app.jetstream_profile(jetstream_name));
        let jetstream_retries = env::var("STORAGE_JETSTREAM_RETRIES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .or_else(|| jetstream_profile.and_then(|profile| profile.retries))
            .unwrap_or(8);
        let jetstream_retry_backoff_ms = env::var("STORAGE_JETSTREAM_RETRY_BACKOFF_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| jetstream_profile.and_then(|profile| profile.retry_backoff_ms))
            .unwrap_or(25);

        let kafka_brokers = env::var("STORAGE_KAFKA_BROKERS")
            .ok()
            .map(|v| {
                v.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .or_else(|| kafka_profile.map(|p| p.brokers.clone()))
            .unwrap_or_else(|| vec!["127.0.0.1:29092".to_string()]);
        let kafka_client_id = env::var("STORAGE_KAFKA_CLIENT_ID")
            .ok()
            .or_else(|| kafka_profile.and_then(|p| p.client_id.clone()))
            .unwrap_or_else(|| "flare-im-storage-writer".to_string());
        let kafka_options = kafka_profile.map(|p| p.options.clone()).unwrap_or_default();

        // 批量消费配置
        let max_poll_records = env::var("STORAGE_MAX_POLL_RECORDS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(100);

        let fetch_min_bytes = env::var("STORAGE_FETCH_MIN_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1024);

        let fetch_max_wait_ms = env::var("STORAGE_FETCH_MAX_WAIT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(100);

        // 解析 Redis 配置引用（WAL 存储）
        let redis_url = env::var("STORAGE_REDIS_URL").ok().or_else(|| {
            if let Some(redis_name) = &service_config.wal_store {
                app.redis_profile(redis_name)
                    .map(|profile| profile.url.clone())
            } else {
                None
            }
        });

        let redis_hot_ttl_seconds = env::var("STORAGE_REDIS_HOT_TTL_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(7 * 24 * 3600);

        let redis_idempotency_ttl_seconds = env::var("STORAGE_REDIS_IDEMPOTENCY_TTL_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(24 * 3600);

        let wal_hash_key = env::var("STORAGE_WAL_HASH_KEY")
            .ok()
            .or_else(|| service_config.wal_hash_key.clone())
            .or_else(|| redis_url.as_ref().map(|_| "storage:wal:buffer".to_string()));

        // 解析 PostgreSQL 配置引用（可选）
        let postgres_url = env::var("STORAGE_POSTGRES_URL").ok().or_else(|| {
            if let Some(postgres_name) = &service_config.postgres {
                app.postgres_profile(postgres_name)
                    .map(|profile| profile.url.clone())
            } else {
                None
            }
        });

        // PostgreSQL 连接池配置（优化性能）
        let postgres_max_connections = env::var("STORAGE_POSTGRES_MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(50); // 默认 50 个连接（适合高并发写入）

        let postgres_min_connections = env::var("STORAGE_POSTGRES_MIN_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(10); // 默认保持 10 个最小连接

        let postgres_acquire_timeout_seconds = env::var("STORAGE_POSTGRES_ACQUIRE_TIMEOUT_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30); // 默认 30 秒获取连接超时

        let postgres_idle_timeout_seconds = env::var("STORAGE_POSTGRES_IDLE_TIMEOUT_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(600); // 默认 10 分钟空闲超时

        let postgres_max_lifetime_seconds = env::var("STORAGE_POSTGRES_MAX_LIFETIME_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(3600); // 默认 1 小时连接最大生命周期

        let media_service_endpoint = env::var("MEDIA_SERVICE_ENDPOINT").ok();

        Ok(Self {
            mq_backend,
            jetstream_url,
            jetstream_group,
            jetstream_ack_topic,
            jetstream_timeout_ms,
            jetstream_retries,
            jetstream_retry_backoff_ms,
            jetstream_stream_specs,
            kafka_brokers,
            kafka_client_id,
            kafka_options,
            max_poll_records,
            fetch_min_bytes,
            fetch_max_wait_ms,
            redis_url,
            redis_hot_ttl_seconds,
            redis_idempotency_ttl_seconds,
            wal_hash_key,
            postgres_url,
            postgres_max_connections,
            postgres_min_connections,
            postgres_acquire_timeout_seconds,
            postgres_idle_timeout_seconds,
            postgres_max_lifetime_seconds,
            media_service_endpoint,
        })
    }
}

// 实现 NatsConsumerConfig trait，使 StorageWriterConfig 可以使用通用的 JetStream 消费者构建器
impl NatsConsumerConfig for StorageWriterConfig {
    fn nats_url(&self) -> &str {
        &self.jetstream_url
    }

    fn consumer_group(&self) -> &str {
        &self.jetstream_group
    }

    fn enable_manual_ack(&self) -> bool {
        true
    }

    fn batch_size(&self) -> usize {
        self.max_poll_records
    }

    fn batch_timeout_ms(&self) -> u64 {
        self.fetch_max_wait_ms
    }

    fn enable_durable(&self) -> bool {
        true
    }

    fn stream_specs(&self) -> Vec<NatsStreamSpec> {
        self.jetstream_stream_specs.clone()
    }
}

// 实现 NatsProducerConfig trait，使 StorageWriterConfig 可以使用通用的 JetStream 生产者构建器
impl NatsProducerConfig for StorageWriterConfig {
    fn nats_url(&self) -> &str {
        &self.jetstream_url
    }

    fn timeout_ms(&self) -> u64 {
        self.jetstream_timeout_ms
    }

    fn retries(&self) -> u32 {
        self.jetstream_retries
    }

    fn retry_backoff_ms(&self) -> u64 {
        self.jetstream_retry_backoff_ms
    }

    fn stream_specs(&self) -> Vec<NatsStreamSpec> {
        self.jetstream_stream_specs.clone()
    }
}

impl KafkaProducerConfig for StorageWriterConfig {
    fn kafka_brokers(&self) -> Vec<String> {
        self.kafka_brokers.clone()
    }

    fn kafka_client_id(&self) -> &str {
        &self.kafka_client_id
    }

    fn kafka_options(&self) -> HashMap<String, String> {
        self.kafka_options.clone()
    }
}

impl KafkaConsumerConfig for StorageWriterConfig {
    fn kafka_consumer_group(&self) -> &str {
        &self.jetstream_group
    }
}
