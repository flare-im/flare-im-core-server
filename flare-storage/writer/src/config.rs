use anyhow::Result;
use flare_im_core::config::FlareAppConfig;
use flare_server_core::kafka::{KafkaConsumerConfig, KafkaProducerConfig};
use std::env;

#[derive(Clone, Debug)]
pub struct StorageWriterConfig {
    pub kafka_bootstrap: String,
    pub kafka_topic: String,
    pub kafka_operation_topic: String,
    pub kafka_group: String,
    pub kafka_ack_topic: Option<String>,
    pub kafka_timeout_ms: u64,

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

        // 解析 Kafka 配置引用
        let kafka_bootstrap = env::var("STORAGE_KAFKA_BOOTSTRAP_SERVERS")
            .ok()
            .or_else(|| {
                if let Some(kafka_name) = &service_config.kafka {
                    app.kafka_profile(kafka_name)
                        .map(|profile| profile.bootstrap_servers.clone())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "127.0.0.1:29092".to_string());

        let kafka_topic = env::var("STORAGE_KAFKA_STORAGE_TOPIC")
            .ok()
            .or_else(|| service_config.kafka_topic.clone())
            .unwrap_or_else(|| "storage-messages".to_string());

        let kafka_operation_topic = env::var("STORAGE_KAFKA_OPERATION_TOPIC")
            .unwrap_or_else(|_| "storage-message-operations".to_string());

        let kafka_group = env::var("STORAGE_KAFKA_STORAGE_GROUP")
            .ok()
            .or_else(|| service_config.consumer_group.clone())
            .unwrap_or_else(|| "storage-writer".to_string());

        let kafka_ack_topic = env::var("STORAGE_KAFKA_ACK_TOPIC").ok();

        let kafka_timeout_ms = env::var("STORAGE_KAFKA_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| {
                service_config
                    .kafka
                    .as_ref()
                    .and_then(|kafka_name| app.kafka_profile(kafka_name))
                    .and_then(|profile| profile.timeout_ms)
            })
            .unwrap_or(5000);

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
            kafka_bootstrap,
            kafka_topic,
            kafka_operation_topic,
            kafka_group,
            kafka_ack_topic,
            kafka_timeout_ms,
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

    /// 从环境变量加载（保留用于向后兼容，但不推荐使用）
    #[deprecated(note = "Use from_app_config instead")]
    pub fn from_env() -> Self {
        let kafka_bootstrap = env::var("STORAGE_KAFKA_BOOTSTRAP_SERVERS")
            .unwrap_or_else(|_| "localhost:9092".to_string());
        let kafka_topic = env::var("STORAGE_KAFKA_STORAGE_TOPIC")
            .unwrap_or_else(|_| "storage-messages".to_string());
        let kafka_operation_topic = env::var("STORAGE_KAFKA_OPERATION_TOPIC")
            .unwrap_or_else(|_| "storage-message-operations".to_string());
        let kafka_group = env::var("STORAGE_KAFKA_STORAGE_GROUP")
            .unwrap_or_else(|_| "storage-writer".to_string());
        let kafka_ack_topic = env::var("STORAGE_KAFKA_ACK_TOPIC").ok();
        let kafka_timeout_ms = env::var("STORAGE_KAFKA_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(5000);

        // 批量消费配置（向后兼容）
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

        let redis_url = env::var("STORAGE_REDIS_URL").ok();
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
            .or_else(|| redis_url.as_ref().map(|_| "storage:wal:buffer".to_string()));

        let postgres_url = env::var("STORAGE_POSTGRES_URL").ok();

        // PostgreSQL 连接池配置（向后兼容，使用默认值）
        let postgres_max_connections = env::var("STORAGE_POSTGRES_MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(50);
        let postgres_min_connections = env::var("STORAGE_POSTGRES_MIN_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(10);
        let postgres_acquire_timeout_seconds = env::var("STORAGE_POSTGRES_ACQUIRE_TIMEOUT_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30);
        let postgres_idle_timeout_seconds = env::var("STORAGE_POSTGRES_IDLE_TIMEOUT_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(600);
        let postgres_max_lifetime_seconds = env::var("STORAGE_POSTGRES_MAX_LIFETIME_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(3600);

        let media_service_endpoint = env::var("MEDIA_SERVICE_ENDPOINT").ok();

        Self {
            kafka_bootstrap,
            kafka_topic,
            kafka_operation_topic,
            kafka_group,
            kafka_ack_topic,
            kafka_timeout_ms,
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
        }
    }
}

// 实现 KafkaConsumerConfig trait，使 StorageWriterConfig 可以使用通用的 Kafka 消费者构建器
impl KafkaConsumerConfig for StorageWriterConfig {
    fn kafka_bootstrap(&self) -> &str {
        &self.kafka_bootstrap
    }

    fn consumer_group(&self) -> &str {
        &self.kafka_group
    }

    fn kafka_topic(&self) -> &str {
        &self.kafka_topic
    }

    fn fetch_min_bytes(&self) -> usize {
        self.fetch_min_bytes
    }

    fn fetch_max_wait_ms(&self) -> u64 {
        self.fetch_max_wait_ms
    }

    // 使用默认值，或根据需要覆盖
    fn session_timeout_ms(&self) -> u64 {
        6000 // storage-writer 使用较短的超时
    }

    fn enable_auto_commit(&self) -> bool {
        false
    }

    fn auto_offset_reset(&self) -> &str {
        // 从最早位置开始消费，确保不丢失消息
        // 如果 consumer group 已经存在，会从上次提交的 offset 继续消费
        "earliest"
    }
}

// 实现 KafkaProducerConfig trait，使 StorageWriterConfig 可以使用通用的 Kafka 生产者构建器
impl KafkaProducerConfig for StorageWriterConfig {
    fn kafka_bootstrap(&self) -> &str {
        &self.kafka_bootstrap
    }

    fn message_timeout_ms(&self) -> u64 {
        self.kafka_timeout_ms
    }

    // 使用默认值，或根据需要覆盖
    fn enable_idempotence(&self) -> bool {
        true // ACK 消息需要保证不丢失
    }

    fn compression_type(&self) -> &str {
        "snappy" // ACK 消息使用 snappy 压缩
    }
}
