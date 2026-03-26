//! Push Worker 配置（以设计文档为准）

use std::env;

use flare_im_core::config::FlareAppConfig;
use flare_im_core::constants::groups::PUSH_WORKER_GROUP_DEFAULT;
use flare_im_core::constants::topics::{TOPIC_PUSH_DLQ, TOPIC_PUSH_OFFLINE, TOPIC_PUSH_ONLINE};
use flare_server_core::mq::kafka::{KafkaConsumerConfig, KafkaProducerConfig};

#[derive(Debug, Clone)]
pub struct PushWorkerConfig {
    pub kafka_bootstrap: String,
    pub consumer_group: String,

    pub push_online_topic: String,
    pub push_offline_topic: String,
    pub push_dlq_topic: String,

    /// flare-signaling/online（ListUserDevices）gRPC 地址（与 `config/services/signaling-online.toml` 中 server.port 一致）
    pub online_service_endpoint: String,
    /// 无注册中心时 Access Gateway gRPC 直连地址（与 `GatewayRouterConfig.static_fallback_endpoint` 一致）
    pub access_gateway_static_endpoint: Option<String>,
}

/// 与 `signaling-online` 监听地址对齐：优先读 app 中 `[services.signaling_online.server]`，否则本地默认 50061。
fn default_signaling_online_grpc_endpoint(app: &FlareAppConfig) -> String {
    let so = app.signaling_online_service();
    let Some(server) = so.runtime.server.as_ref() else {
        return "http://127.0.0.1:50061".to_string();
    };
    let port = server.port.unwrap_or(50061);
    let host = server
        .address
        .as_deref()
        .filter(|a| !a.is_empty())
        .map(|a| if a == "0.0.0.0" { "127.0.0.1" } else { a })
        .unwrap_or("127.0.0.1");
    format!("http://{}:{}", host, port)
}

impl PushWorkerConfig {
    pub fn from_app_config(app: &FlareAppConfig) -> Self {
        let service = app.push_worker_service();
        let kafka_name = service.kafka.as_deref().unwrap_or("push");
        let kafka_profile = app.kafka_profile(kafka_name);

        let kafka_bootstrap = env::var("PUSH_WORKER_KAFKA_BOOTSTRAP")
            .ok()
            .or_else(|| kafka_profile.map(|cfg| cfg.bootstrap_servers.clone()))
            .unwrap_or_else(|| "127.0.0.1:29092".to_string());

        let consumer_group = env::var("PUSH_WORKER_CONSUMER_GROUP")
            .unwrap_or_else(|_| PUSH_WORKER_GROUP_DEFAULT.to_string());

        let push_online_topic = env::var("PUSH_WORKER_PUSH_ONLINE_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_ONLINE.to_string());
        let push_offline_topic = env::var("PUSH_WORKER_PUSH_OFFLINE_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_OFFLINE.to_string());
        let push_dlq_topic = env::var("PUSH_WORKER_PUSH_DLQ_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_DLQ.to_string());

        let online_service_endpoint = env::var("PUSH_WORKER_ONLINE_SERVICE_ENDPOINT")
            .ok()
            .unwrap_or_else(|| default_signaling_online_grpc_endpoint(app));

        let access_gateway_static_endpoint = env::var("ACCESS_GATEWAY_GRPC_ENDPOINT").ok();

        Self {
            kafka_bootstrap,
            consumer_group,
            push_online_topic,
            push_offline_topic,
            push_dlq_topic,
            online_service_endpoint,
            access_gateway_static_endpoint,
        }
    }
}

impl KafkaConsumerConfig for PushWorkerConfig {
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

impl KafkaProducerConfig for PushWorkerConfig {
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

