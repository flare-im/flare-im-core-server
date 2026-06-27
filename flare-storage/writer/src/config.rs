//! Storage Writer 配置
//!
//! **架构落地**：消费 **TOPIC_MESSAGE_EVENTS**（统一事件流），
//! 与 Orchestrator 单事件流对齐，处理 message.created 和 operation.* 事件。

use flare_im_contracts::constants::groups::STORAGE_GROUP_DEFAULT;
use flare_im_contracts::constants::topics::{
    TOPIC_MESSAGE_MAIN_DLQ, TOPIC_MESSAGE_STORAGE_RETRY_5S, TOPIC_PUSH_ACKS,
};
use flare_im_service_kit::config::FlareAppConfig;
use flare_im_service_kit::metrics::MetricsEndpointConfig;
use flare_server_core::error::Result;
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
    pub message_dlq_topic: String,
    pub message_retry_topic: String,
    pub message_retry_delay_ms: u64,

    // 批量消费配置
    pub max_poll_records: usize,
    pub fetch_min_bytes: usize,
    pub fetch_max_wait_ms: u64,
    pub redis_url: Option<String>,
    pub redis_hot_ttl_seconds: u64,
    pub redis_hot_tail_limit: usize,
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
    pub metrics: MetricsEndpointConfig,
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

        let jetstream_ack_topic = Some(
            env::var("STORAGE_JETSTREAM_ACK_SUBJECT")
                .unwrap_or_else(|_| TOPIC_PUSH_ACKS.to_string()),
        );

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
        let message_dlq_topic = env::var("STORAGE_MESSAGE_DLQ_TOPIC")
            .ok()
            .or_else(|| service_config.message_dlq_topic.clone())
            .unwrap_or_else(|| TOPIC_MESSAGE_MAIN_DLQ.to_string());
        let message_retry_topic = env::var("STORAGE_MESSAGE_RETRY_TOPIC")
            .ok()
            .or_else(|| service_config.message_retry_topic.clone())
            .unwrap_or_else(|| TOPIC_MESSAGE_STORAGE_RETRY_5S.to_string());
        let message_retry_delay_ms = env::var("STORAGE_MESSAGE_RETRY_DELAY_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .or(service_config.message_retry_delay_ms)
            .unwrap_or(5000);

        // 批量消费配置
        let max_poll_records = env::var("STORAGE_MAX_POLL_RECORDS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(32);

        let fetch_min_bytes = env::var("STORAGE_FETCH_MIN_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1024);

        let fetch_max_wait_ms = env::var("STORAGE_FETCH_MAX_WAIT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(5);

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

        let redis_hot_tail_limit = env::var("STORAGE_REDIS_HOT_TAIL_LIMIT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .or(service_config.redis_hot_tail_limit)
            .unwrap_or(50)
            .max(1);

        let redis_idempotency_ttl_seconds = env::var("STORAGE_REDIS_IDEMPOTENCY_TTL_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(24 * 3600);

        let wal_hash_key = env::var("STORAGE_WAL_HASH_KEY")
            .ok()
            .or_else(|| service_config.wal_hash_key.clone())
            .or_else(|| redis_url.as_ref().map(|_| "storage:wal:buffer".to_string()));

        // 解析 PostgreSQL 配置引用（可选）
        let postgres_profile_name = service_config.postgres.as_deref();
        let postgres_profile = postgres_profile_name.and_then(|name| app.postgres_profile(name));
        let postgres_url = env::var("STORAGE_POSTGRES_URL")
            .ok()
            .or_else(|| postgres_profile.map(|profile| profile.url.clone()));

        // PostgreSQL 连接池配置（优化性能）
        let postgres_max_connections = env::var("STORAGE_POSTGRES_MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .or_else(|| postgres_profile.and_then(|profile| profile.max_connections))
            .unwrap_or(50); // 默认 50 个连接（适合高并发写入）

        let postgres_min_connections = env::var("STORAGE_POSTGRES_MIN_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .or_else(|| postgres_profile.and_then(|profile| profile.min_connections))
            .unwrap_or(10); // 默认保持 10 个最小连接

        let postgres_acquire_timeout_seconds = env::var("STORAGE_POSTGRES_ACQUIRE_TIMEOUT_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| postgres_profile.and_then(|profile| profile.acquire_timeout_seconds))
            .unwrap_or(30); // 默认 30 秒获取连接超时

        let postgres_idle_timeout_seconds = env::var("STORAGE_POSTGRES_IDLE_TIMEOUT_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| postgres_profile.and_then(|profile| profile.idle_timeout_seconds))
            .unwrap_or(600); // 默认 10 分钟空闲超时

        let postgres_max_lifetime_seconds = env::var("STORAGE_POSTGRES_MAX_LIFETIME_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| postgres_profile.and_then(|profile| profile.max_lifetime_seconds))
            .unwrap_or(3600); // 默认 1 小时连接最大生命周期

        let media_service_endpoint = env::var("MEDIA_SERVICE_ENDPOINT").ok();

        let metrics_enabled = parse_bool_env("STORAGE_WRITER_METRICS_ENABLED")
            .or_else(|| parse_bool_env("STORAGE_METRICS_ENABLED"))
            .or(service_config.metrics_enabled)
            .unwrap_or(true);
        let metrics_address = env::var("STORAGE_WRITER_METRICS_ADDRESS")
            .ok()
            .or_else(|| env::var("STORAGE_METRICS_ADDRESS").ok())
            .or_else(|| service_config.metrics_address.clone())
            .unwrap_or_else(|| "0.0.0.0".to_string());
        let metrics_port = env::var("STORAGE_WRITER_METRICS_PORT")
            .ok()
            .or_else(|| env::var("STORAGE_METRICS_PORT").ok())
            .and_then(|value| value.parse::<u16>().ok())
            .or(service_config.metrics_port)
            .unwrap_or(19182);
        let metrics_path = env::var("STORAGE_WRITER_METRICS_PATH")
            .ok()
            .or_else(|| env::var("STORAGE_METRICS_PATH").ok())
            .or_else(|| service_config.metrics_path.clone())
            .unwrap_or_else(|| "/metrics".to_string());
        let mut metrics =
            MetricsEndpointConfig::new(metrics_address, metrics_port).with_path(metrics_path);
        metrics.enabled = metrics_enabled;

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
            message_dlq_topic,
            message_retry_topic,
            message_retry_delay_ms,
            max_poll_records,
            fetch_min_bytes,
            fetch_max_wait_ms,
            redis_url,
            redis_hot_ttl_seconds,
            redis_hot_tail_limit,
            redis_idempotency_ttl_seconds,
            wal_hash_key,
            postgres_url,
            postgres_max_connections,
            postgres_min_connections,
            postgres_acquire_timeout_seconds,
            postgres_idle_timeout_seconds,
            postgres_max_lifetime_seconds,
            media_service_endpoint,
            metrics,
        })
    }
}

fn parse_bool_env(name: &str) -> Option<bool> {
    env::var(name)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "on" | "yes" => Some(true),
            "0" | "false" | "off" | "no" => Some(false),
            _ => None,
        })
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
