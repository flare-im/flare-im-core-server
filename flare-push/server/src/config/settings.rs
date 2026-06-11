//! Push Server 配置（以设计文档为准）

use std::{collections::HashMap, env};

use flare_im_contracts::constants::groups::PUSH_SERVER_CONSUMER_GROUP_DEFAULT;
use flare_im_contracts::constants::topics::{
    TOPIC_PUSH_DLQ, TOPIC_PUSH_ENVELOPE, TOPIC_PUSH_EVENTS, TOPIC_PUSH_MESSAGES,
    TOPIC_PUSH_OFFLINE, TOPIC_PUSH_ONLINE,
};
use flare_im_contracts::utils::normalize_tenant_id;
use flare_im_service_kit::config::{FlareAppConfig, ServiceEndpointConfig};
use flare_server_core::mq::kafka::{KafkaConsumerConfig, KafkaProducerConfig};
use flare_server_core::mq::nats::{
    NatsConsumerConfig, NatsProducerConfig, NatsStreamSpec, default_stream_specs,
};

#[derive(Debug, Clone)]
pub struct PushServerConfig {
    pub mq_backend: String,
    pub jetstream_url: String,
    pub jetstream_timeout_ms: u64,
    pub jetstream_retries: u32,
    pub jetstream_retry_backoff_ms: u64,
    pub jetstream_stream_specs: Vec<NatsStreamSpec>,
    pub kafka_brokers: Vec<String>,
    pub kafka_client_id: String,
    pub kafka_options: HashMap<String, String>,
    pub consumer_group: String,

    pub push_message_topic: String,
    pub push_event_topic: String,
    pub push_envelope_topic: String,
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
        // 复用现有 app config 的 jetstream profile（不改变外部依赖边界）
        let service = app.push_server_service();
        let jetstream_name = service.jetstream.as_deref().unwrap_or("push");
        let jetstream_profile = app.jetstream_profile(jetstream_name);
        let mq_backend = app.mq_default_backend().to_string();
        let kafka_profile = app.kafka_profile("push");
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

        let jetstream_url = env::var("PUSH_SERVER_JETSTREAM_URL")
            .ok()
            .or_else(|| jetstream_profile.map(|cfg| cfg.url.clone()))
            .unwrap_or_else(|| "nats://127.0.0.1:24222".to_string());
        let jetstream_timeout_ms = env::var("PUSH_SERVER_JETSTREAM_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| jetstream_profile.and_then(|cfg| cfg.timeout_ms))
            .unwrap_or(5_000);
        let jetstream_retries = env::var("PUSH_SERVER_JETSTREAM_RETRIES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .or_else(|| jetstream_profile.and_then(|cfg| cfg.retries))
            .unwrap_or(8);
        let jetstream_retry_backoff_ms = env::var("PUSH_SERVER_JETSTREAM_RETRY_BACKOFF_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| jetstream_profile.and_then(|cfg| cfg.retry_backoff_ms))
            .unwrap_or(25);

        let consumer_group = env::var("PUSH_SERVER_CONSUMER_GROUP")
            .unwrap_or_else(|_| PUSH_SERVER_CONSUMER_GROUP_DEFAULT.to_string());
        let kafka_brokers = env::var("PUSH_SERVER_KAFKA_BROKERS")
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
        let kafka_client_id = env::var("PUSH_SERVER_KAFKA_CLIENT_ID")
            .ok()
            .or_else(|| kafka_profile.and_then(|p| p.client_id.clone()))
            .unwrap_or_else(|| "flare-im-push-server".to_string());
        let kafka_options = kafka_profile.map(|p| p.options.clone()).unwrap_or_default();

        let push_message_topic = env::var("PUSH_SERVER_PUSH_MESSAGE_TOPIC")
            .ok()
            .or_else(|| service.push_message_topic.clone())
            .unwrap_or_else(|| TOPIC_PUSH_MESSAGES.to_string());
        let push_event_topic = env::var("PUSH_SERVER_PUSH_EVENT_TOPIC")
            .ok()
            .or_else(|| service.push_event_topic.clone())
            .unwrap_or_else(|| TOPIC_PUSH_EVENTS.to_string());
        let push_envelope_topic = env::var("PUSH_SERVER_PUSH_ENVELOPE_TOPIC")
            .ok()
            .or_else(|| service.push_envelope_topic.clone())
            .unwrap_or_else(|| TOPIC_PUSH_ENVELOPE.to_string());
        let push_online_topic = env::var("PUSH_SERVER_PUSH_ONLINE_TOPIC")
            .ok()
            .or_else(|| service.push_online_topic.clone())
            .unwrap_or_else(|| TOPIC_PUSH_ONLINE.to_string());
        let push_offline_topic = env::var("PUSH_SERVER_PUSH_OFFLINE_TOPIC")
            .ok()
            .or_else(|| service.push_offline_topic.clone())
            .unwrap_or_else(|| TOPIC_PUSH_OFFLINE.to_string());
        let push_dlq_topic = env::var("PUSH_SERVER_PUSH_DLQ_TOPIC")
            .ok()
            .or_else(|| service.push_dlq_topic.clone())
            .unwrap_or_else(|| TOPIC_PUSH_DLQ.to_string());

        let online_service_endpoint = env::var("PUSH_SERVER_ONLINE_SERVICE_ENDPOINT")
            .ok()
            .unwrap_or_else(|| default_signaling_online_grpc_endpoint(app));

        let default_tenant_id = env::var("PUSH_SERVER_DEFAULT_TENANT_ID")
            .ok()
            .or_else(|| service.default_tenant_id.clone())
            .map(normalize_tenant_id)
            .unwrap_or_else(|| "0".to_string());

        Self {
            mq_backend,
            jetstream_url,
            jetstream_timeout_ms,
            jetstream_retries,
            jetstream_retry_backoff_ms,
            jetstream_stream_specs,
            kafka_brokers,
            kafka_client_id,
            kafka_options,
            consumer_group,
            push_message_topic,
            push_event_topic,
            push_envelope_topic,
            push_online_topic,
            push_offline_topic,
            push_dlq_topic,
            online_service_endpoint,
            default_tenant_id,
        }
    }
}

impl NatsConsumerConfig for PushServerConfig {
    fn nats_url(&self) -> &str {
        &self.jetstream_url
    }
    fn consumer_group(&self) -> &str {
        &self.consumer_group
    }
    fn enable_manual_ack(&self) -> bool {
        true
    }
    fn batch_size(&self) -> usize {
        64
    }
    fn batch_timeout_ms(&self) -> u64 {
        50
    }
    fn enable_durable(&self) -> bool {
        true
    }

    fn stream_specs(&self) -> Vec<NatsStreamSpec> {
        self.jetstream_stream_specs.clone()
    }
}

impl NatsProducerConfig for PushServerConfig {
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

impl KafkaProducerConfig for PushServerConfig {
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

impl KafkaConsumerConfig for PushServerConfig {
    fn kafka_consumer_group(&self) -> &str {
        &self.consumer_group
    }
}
