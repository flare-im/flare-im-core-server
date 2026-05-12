//! Push Proxy 配置:gRPC 监听地址与 JetStream Topic(以设计文档为准)

use flare_im_core::config::FlareAppConfig;
use flare_im_core::constants::topics::{
    TOPIC_PUSH_MESSAGES, TOPIC_PUSH_OFFLINE, TOPIC_PUSH_ONLINE,
};
use flare_server_core::mq::kafka::KafkaProducerConfig;
use flare_server_core::mq::nats::{NatsProducerConfig, NatsStreamSpec, default_stream_specs};
use std::{collections::HashMap, env};

#[derive(Clone, Debug)]
pub struct PushProxyConfig {
    pub mq_backend: String,
    /// JetStream bootstrap(与 Push Server 一致)
    pub jetstream_url: String,
    /// 推送消息入站 Topic(默认 `TOPIC_PUSH_MESSAGES` / `push-messages`)
    pub push_request_topic: String,
    /// 在线推送 Topic
    pub push_online_topic: String,
    /// 离线推送 Topic
    pub push_offline_topic: String,
    /// JetStream 发送超时(毫秒)
    pub jetstream_timeout_ms: u64,
    pub jetstream_retries: u32,
    pub jetstream_retry_backoff_ms: u64,
    pub jetstream_stream_specs: Vec<NatsStreamSpec>,
    pub kafka_brokers: Vec<String>,
    pub kafka_client_id: String,
    pub kafka_options: HashMap<String, String>,
    /// Redis URL(任务粗粒度状态,供 QueryPushStatus)
    pub redis_url: String,
    /// Redis key 前缀
    pub redis_key_prefix: String,
}

impl PushProxyConfig {
    pub fn from_app_config(app: &FlareAppConfig) -> Self {
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

        let jetstream_url = env::var("PUSH_PROXY_JETSTREAM_URL")
            .ok()
            .or_else(|| jetstream_profile.map(|c| c.url.clone()))
            .unwrap_or_else(|| "nats://127.0.0.1:24222".to_string());

        let push_request_topic = env::var("PUSH_PROXY_PUSH_REQUEST_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_MESSAGES.to_string());

        let push_online_topic = env::var("PUSH_PROXY_PUSH_ONLINE_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_ONLINE.to_string());

        let push_offline_topic = env::var("PUSH_PROXY_PUSH_OFFLINE_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_OFFLINE.to_string());

        let jetstream_timeout_ms = env::var("PUSH_PROXY_JETSTREAM_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .or_else(|| jetstream_profile.and_then(|c| c.timeout_ms))
            .unwrap_or(5000);
        let jetstream_retries = env::var("PUSH_PROXY_JETSTREAM_RETRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .or_else(|| jetstream_profile.and_then(|c| c.retries))
            .unwrap_or(8);
        let jetstream_retry_backoff_ms = env::var("PUSH_PROXY_JETSTREAM_RETRY_BACKOFF_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .or_else(|| jetstream_profile.and_then(|c| c.retry_backoff_ms))
            .unwrap_or(25);
        let kafka_brokers = env::var("PUSH_PROXY_KAFKA_BROKERS")
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
        let kafka_client_id = env::var("PUSH_PROXY_KAFKA_CLIENT_ID")
            .ok()
            .or_else(|| kafka_profile.and_then(|p| p.client_id.clone()))
            .unwrap_or_else(|| "flare-im-push-proxy".to_string());
        let kafka_options = kafka_profile.map(|p| p.options.clone()).unwrap_or_default();

        let redis_url = env::var("PUSH_PROXY_REDIS_URL")
            .ok()
            .unwrap_or_else(|| "redis://127.0.0.1/".to_string());

        let redis_key_prefix = env::var("PUSH_PROXY_REDIS_KEY_PREFIX")
            .ok()
            .unwrap_or_else(|| "flare:push:proxy".to_string());

        Self {
            mq_backend,
            jetstream_url,
            push_request_topic,
            push_online_topic,
            push_offline_topic,
            jetstream_timeout_ms,
            jetstream_retries,
            jetstream_retry_backoff_ms,
            jetstream_stream_specs,
            kafka_brokers,
            kafka_client_id,
            kafka_options,
            redis_url,
            redis_key_prefix,
        }
    }
}

impl NatsProducerConfig for PushProxyConfig {
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

impl KafkaProducerConfig for PushProxyConfig {
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
