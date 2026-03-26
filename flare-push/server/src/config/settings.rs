//! Push Server 配置（以设计文档为准）

use std::env;

use flare_im_core::config::{FlareAppConfig, ServiceEndpointConfig};
use flare_im_core::constants::groups::PUSH_SERVER_CONSUMER_GROUP_DEFAULT;
use flare_im_core::constants::topics::{
    TOPIC_PUSH_ACKS, TOPIC_PUSH_CUSTOM, TOPIC_PUSH_DLQ, TOPIC_PUSH_EVENTS, TOPIC_PUSH_MESSAGES,
    TOPIC_PUSH_NOTIFICATIONS, TOPIC_PUSH_OFFLINE, TOPIC_PUSH_ONLINE,
};
use flare_server_core::mq::kafka::{KafkaConsumerConfig, KafkaProducerConfig};

#[derive(Debug, Clone)]
pub struct PushServerConfig {
    pub kafka_bootstrap: String,
    pub consumer_group: String,

    pub push_message_topic: String,
    pub push_event_topic: String,
    pub push_notification_topic: String,
    pub push_ack_topic: String,
    pub push_custom_topic: String,
    pub push_online_topic: String,
    pub push_offline_topic: String,
    pub push_dlq_topic: String,

    /// flare-signaling/online 的 gRPC endpoint（与 `config/services/signaling-online.toml` 中 server.port 一致）
    pub online_service_endpoint: String,

    /// 默认 tenant（用于填充 Envelope）
    pub default_tenant_id: String,
}

/// 与 `signaling-online` 监听地址对齐：优先读 app 中 `[services.signaling_online.server]`，否则本地默认 50061。
fn default_signaling_online_grpc_endpoint(app: &FlareAppConfig) -> String {
    let so = app.signaling_online_service();
    let Some(server) = so.runtime.server.as_ref() else {
        return "http://127.0.0.1:50061".to_string();
    };
    endpoint_from_service_server(server)
}

fn endpoint_from_service_server(server: &ServiceEndpointConfig) -> String {
    let port = server.port.unwrap_or(50061);
    let host = server
        .address
        .as_deref()
        .filter(|a| !a.is_empty())
        .map(|a| if a == "0.0.0.0" { "127.0.0.1" } else { a })
        .unwrap_or("127.0.0.1");
    format!("http://{}:{}", host, port)
}

impl PushServerConfig {
    pub fn from_app_config(app: &FlareAppConfig) -> Self {
        // 复用现有 app config 的 kafka profile（不改变外部依赖边界）
        let service = app.push_server_service();
        let kafka_name = service.kafka.as_deref().unwrap_or("push");
        let kafka_profile = app.kafka_profile(kafka_name);

        let kafka_bootstrap = env::var("PUSH_SERVER_KAFKA_BOOTSTRAP")
            .ok()
            .or_else(|| kafka_profile.map(|cfg| cfg.bootstrap_servers.clone()))
            .unwrap_or_else(|| "127.0.0.1:29092".to_string());

        let consumer_group = env::var("PUSH_SERVER_CONSUMER_GROUP")
            .unwrap_or_else(|_| PUSH_SERVER_CONSUMER_GROUP_DEFAULT.to_string());

        let push_message_topic = env::var("PUSH_SERVER_PUSH_MESSAGE_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_MESSAGES.to_string());
        let push_event_topic = env::var("PUSH_SERVER_PUSH_EVENT_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_EVENTS.to_string());
        let push_notification_topic = env::var("PUSH_SERVER_PUSH_NOTIFICATION_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_NOTIFICATIONS.to_string());
        let push_ack_topic = env::var("PUSH_SERVER_PUSH_ACK_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_ACKS.to_string());
        let push_custom_topic = env::var("PUSH_SERVER_PUSH_CUSTOM_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_CUSTOM.to_string());
        let push_online_topic = env::var("PUSH_SERVER_PUSH_ONLINE_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_ONLINE.to_string());
        let push_offline_topic = env::var("PUSH_SERVER_PUSH_OFFLINE_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_OFFLINE.to_string());
        let push_dlq_topic = env::var("PUSH_SERVER_PUSH_DLQ_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_DLQ.to_string());

        let online_service_endpoint = env::var("PUSH_SERVER_ONLINE_SERVICE_ENDPOINT")
            .ok()
            .unwrap_or_else(|| default_signaling_online_grpc_endpoint(app));

        let default_tenant_id = env::var("PUSH_SERVER_DEFAULT_TENANT_ID")
            .ok()
            .or_else(|| service.default_tenant_id.clone())
            .unwrap_or_else(|| "default".to_string());

        Self {
            kafka_bootstrap,
            consumer_group,
            push_message_topic,
            push_event_topic,
            push_notification_topic,
            push_ack_topic,
            push_custom_topic,
            push_online_topic,
            push_offline_topic,
            push_dlq_topic,
            online_service_endpoint,
            default_tenant_id,
        }
    }
}

impl KafkaConsumerConfig for PushServerConfig {
    fn kafka_bootstrap(&self) -> &str {
        &self.kafka_bootstrap
    }
    fn consumer_group(&self) -> &str {
        &self.consumer_group
    }
    fn enable_auto_commit(&self) -> bool {
        false
    }
    fn session_timeout_ms(&self) -> u64 {
        30_000
    }
    fn auto_offset_reset(&self) -> &str {
        "earliest"
    }
    fn fetch_min_bytes(&self) -> usize {
        1
    }
    fn fetch_max_wait_ms(&self) -> u64 {
        50
    }
    fn fetch_message_max_bytes(&self) -> usize {
        1_048_576
    }
    fn max_partition_fetch_bytes(&self) -> usize {
        1_048_576
    }
    fn metadata_max_age_ms(&self) -> u64 {
        300_000
    }
}

impl KafkaProducerConfig for PushServerConfig {
    fn kafka_bootstrap(&self) -> &str {
        &self.kafka_bootstrap
    }
    fn message_timeout_ms(&self) -> u64 {
        5_000
    }
    fn enable_idempotence(&self) -> bool {
        true
    }
    fn compression_type(&self) -> &str {
        "snappy"
    }
    fn batch_size(&self) -> usize {
        16 * 1024
    }
    fn linger_ms(&self) -> u64 {
        5
    }
    fn retries(&self) -> u32 {
        3
    }
    fn retry_backoff_ms(&self) -> u64 {
        100
    }
    fn metadata_max_age_ms(&self) -> u64 {
        300_000
    }
}

