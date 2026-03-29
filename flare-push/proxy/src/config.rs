//! Push Proxy 配置:gRPC 监听地址与 Kafka Topic(以设计文档为准)

use flare_im_core::config::FlareAppConfig;
use flare_im_core::constants::topics::{
    TOPIC_PUSH_MESSAGES, TOPIC_PUSH_OFFLINE, TOPIC_PUSH_ONLINE,
};
use flare_server_core::mq::kafka::KafkaProducerConfig;
use std::env;

#[derive(Clone, Debug)]
pub struct PushProxyConfig {
    /// Kafka bootstrap(与 Push Server 一致)
    pub kafka_bootstrap: String,
    /// 推送消息入站 Topic(默认 `TOPIC_PUSH_MESSAGES` / `push-messages`)
    pub push_request_topic: String,
    /// 在线推送 Topic
    pub push_online_topic: String,
    /// 离线推送 Topic
    pub push_offline_topic: String,
    /// Kafka 发送超时(毫秒)
    pub kafka_timeout_ms: u64,
    /// Redis URL(任务粗粒度状态,供 QueryPushStatus)
    pub redis_url: String,
    /// Redis key 前缀
    pub redis_key_prefix: String,
}

impl PushProxyConfig {
    pub fn from_app_config(app: &FlareAppConfig) -> Self {
        let service = app.push_server_service();
        let kafka_name = service.kafka.as_deref().unwrap_or("push");
        let kafka_profile = app.kafka_profile(kafka_name);

        let kafka_bootstrap = env::var("PUSH_PROXY_KAFKA_BOOTSTRAP")
            .ok()
            .or_else(|| kafka_profile.map(|c| c.bootstrap_servers.clone()))
            .unwrap_or_else(|| "127.0.0.1:29092".to_string());

        let push_request_topic = env::var("PUSH_PROXY_PUSH_REQUEST_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_MESSAGES.to_string());

        let push_online_topic = env::var("PUSH_PROXY_PUSH_ONLINE_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_ONLINE.to_string());

        let push_offline_topic = env::var("PUSH_PROXY_PUSH_OFFLINE_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_OFFLINE.to_string());

        let kafka_timeout_ms = env::var("PUSH_PROXY_KAFKA_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5000);

        let redis_url = env::var("PUSH_PROXY_REDIS_URL")
            .ok()
            .unwrap_or_else(|| "redis://127.0.0.1/".to_string());

        let redis_key_prefix = env::var("PUSH_PROXY_REDIS_KEY_PREFIX")
            .ok()
            .unwrap_or_else(|| "flare:push:proxy".to_string());

        Self {
            kafka_bootstrap,
            push_request_topic,
            push_online_topic,
            push_offline_topic,
            kafka_timeout_ms,
            redis_url,
            redis_key_prefix,
        }
    }
}

impl KafkaProducerConfig for PushProxyConfig {
    fn kafka_bootstrap(&self) -> &str {
        &self.kafka_bootstrap
    }

    fn message_timeout_ms(&self) -> u64 {
        self.kafka_timeout_ms
    }

    fn enable_idempotence(&self) -> bool {
        true // 默认启用幂等性
    }

    fn compression_type(&self) -> &str {
        "snappy" // 使用 snappy 压缩
    }

    fn batch_size(&self) -> usize {
        16384 // 16KB
    }

    fn linger_ms(&self) -> u64 {
        5 // 5 毫秒
    }

    fn retries(&self) -> u32 {
        3 // 重试 3 次
    }

    fn retry_backoff_ms(&self) -> u64 {
        100 // 100 毫秒
    }

    fn metadata_max_age_ms(&self) -> u64 {
        300_000 // 5 分钟
    }
}
