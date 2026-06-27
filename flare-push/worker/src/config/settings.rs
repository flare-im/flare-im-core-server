//! Push Worker 配置（以设计文档为准）

use std::{collections::HashMap, env};

use flare_im_contracts::constants::groups::PUSH_WORKER_GROUP_DEFAULT;
use flare_im_contracts::constants::topics::{
    TOPIC_PUSH_DLQ, TOPIC_PUSH_OFFLINE, TOPIC_PUSH_ONLINE,
};
use flare_im_service_kit::config::FlareAppConfig;
use flare_im_service_kit::metrics::MetricsEndpointConfig;
use flare_server_core::mq::kafka::{KafkaConsumerConfig, KafkaProducerConfig};
use flare_server_core::mq::nats::{
    NatsConsumerConfig, NatsProducerConfig, NatsStreamSpec, default_stream_specs,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OfflineDeliveryBackend {
    Outbox,
    Getui,
    Disabled,
}

impl OfflineDeliveryBackend {
    fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "outbox" => Self::Outbox,
            "getui" => Self::Getui,
            "disabled" | "off" | "none" => Self::Disabled,
            other => panic!("unsupported PUSH_WORKER_OFFLINE_DELIVERY_BACKEND={other}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PushWorkerConfig {
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

    pub push_online_topic: String,
    pub push_offline_topic: String,
    pub push_dlq_topic: String,

    /// flare-signaling/online（ListUserDevices）gRPC 地址（与 `config/services/signaling-online.toml` 中 server.port 一致）
    pub online_service_endpoint: String,
    /// 无注册中心时 Access Gateway gRPC 直连地址（与 `GatewayRouterConfig.static_fallback_endpoint` 一致）
    pub access_gateway_static_endpoint: Option<String>,
    /// 未配置离线推送提供者时的有界本地 parking 容量。
    pub offline_parking_capacity: usize,
    /// 离线推送后端：outbox/getui/disabled。
    pub offline_delivery_backend: OfflineDeliveryBackend,
    /// 离线推送 outbox Redis 地址；None 表示禁用（PUSH_WORKER_OFFLINE_REDIS_URL=off）
    pub offline_outbox_redis_url: Option<String>,
    /// 离线推送 outbox Stream key
    pub offline_outbox_stream: String,
    /// 离线推送 outbox Stream MAXLEN（~ 近似裁剪）
    pub offline_outbox_maxlen: usize,
    /// 设备厂商 token registry Redis 地址。
    pub device_token_redis_url: String,
    /// 设备厂商 token registry Redis key 前缀。
    pub device_token_key_prefix: String,
    /// 个推 App ID。
    pub getui_app_id: Option<String>,
    /// 个推 App Key。
    pub getui_app_key: Option<String>,
    /// 个推 Master Secret。
    pub getui_master_secret: Option<String>,
    /// 个推 RestAPI V2 BaseUrl；未配置时按 app_id 生成。
    pub getui_base_url: Option<String>,
    /// 个推离线消息默认 TTL。
    pub getui_default_ttl_ms: u64,
    /// 个推 HTTP 请求超时。
    pub getui_request_timeout_ms: u64,
    /// 在线事件 ping 防抖窗口（毫秒），0 表示关闭。
    pub event_ping_debounce_window_ms: u64,
    pub metrics: MetricsEndpointConfig,
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

        let jetstream_url = env::var("PUSH_WORKER_JETSTREAM_URL")
            .ok()
            .or_else(|| jetstream_profile.map(|cfg| cfg.url.clone()))
            .unwrap_or_else(|| "nats://127.0.0.1:24222".to_string());
        let jetstream_timeout_ms = env::var("PUSH_WORKER_JETSTREAM_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| jetstream_profile.and_then(|cfg| cfg.timeout_ms))
            .unwrap_or(5_000);
        let jetstream_retries = env::var("PUSH_WORKER_JETSTREAM_RETRIES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .or_else(|| jetstream_profile.and_then(|cfg| cfg.retries))
            .unwrap_or(8);
        let jetstream_retry_backoff_ms = env::var("PUSH_WORKER_JETSTREAM_RETRY_BACKOFF_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| jetstream_profile.and_then(|cfg| cfg.retry_backoff_ms))
            .unwrap_or(25);

        let consumer_group = env::var("PUSH_WORKER_CONSUMER_GROUP")
            .unwrap_or_else(|_| PUSH_WORKER_GROUP_DEFAULT.to_string());
        let kafka_brokers = env::var("PUSH_WORKER_KAFKA_BROKERS")
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
        let kafka_client_id = env::var("PUSH_WORKER_KAFKA_CLIENT_ID")
            .ok()
            .or_else(|| kafka_profile.and_then(|p| p.client_id.clone()))
            .unwrap_or_else(|| "flare-im-push-worker".to_string());
        let kafka_options = kafka_profile.map(|p| p.options.clone()).unwrap_or_default();

        let push_online_topic = env::var("PUSH_WORKER_PUSH_ONLINE_TOPIC")
            .ok()
            .or_else(|| service.push_online_topic.clone())
            .unwrap_or_else(|| TOPIC_PUSH_ONLINE.to_string());
        let push_offline_topic = env::var("PUSH_WORKER_PUSH_OFFLINE_TOPIC")
            .ok()
            .or_else(|| service.push_offline_topic.clone())
            .unwrap_or_else(|| TOPIC_PUSH_OFFLINE.to_string());
        let push_dlq_topic = env::var("PUSH_WORKER_PUSH_DLQ_TOPIC")
            .ok()
            .or_else(|| service.push_dlq_topic.clone())
            .unwrap_or_else(|| TOPIC_PUSH_DLQ.to_string());

        let online_service_endpoint = env::var("PUSH_WORKER_ONLINE_SERVICE_ENDPOINT")
            .ok()
            .unwrap_or_else(|| default_signaling_online_grpc_endpoint(app));

        let access_gateway_static_endpoint = env::var("ACCESS_GATEWAY_GRPC_ENDPOINT").ok();
        let offline_parking_capacity = env::var("PUSH_WORKER_OFFLINE_PARKING_CAPACITY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .or(service.offline_parking_capacity)
            .unwrap_or(4096);
        let offline_delivery_backend = env::var("PUSH_WORKER_OFFLINE_DELIVERY_BACKEND")
            .ok()
            .map(|value| OfflineDeliveryBackend::parse(&value))
            .unwrap_or(OfflineDeliveryBackend::Outbox);
        let offline_outbox_redis_url = resolve_offline_outbox_redis_url(
            env::var("PUSH_WORKER_OFFLINE_REDIS_URL").ok(),
            app.redis_profile("push").map(|profile| profile.url.clone()),
        );
        let offline_outbox_stream = env::var("PUSH_WORKER_OFFLINE_OUTBOX_STREAM")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "flare:im:push:offline:outbox".to_string());
        let offline_outbox_maxlen = env::var("PUSH_WORKER_OFFLINE_OUTBOX_MAXLEN")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(100_000);
        let push_redis_profile_url = app.redis_profile("push").map(|profile| profile.url.clone());
        let device_token_redis_url = env::var("PUSH_WORKER_DEVICE_TOKEN_REDIS_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or(push_redis_profile_url)
            .unwrap_or_else(|| "redis://127.0.0.1:6379".to_string());
        let device_token_key_prefix = env::var("PUSH_WORKER_DEVICE_TOKEN_KEY_PREFIX")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "flare:im:push:device_tokens".to_string());
        let getui_app_id = non_empty_env("PUSH_WORKER_GETUI_APP_ID");
        let getui_app_key = non_empty_env("PUSH_WORKER_GETUI_APP_KEY");
        let getui_master_secret = non_empty_env("PUSH_WORKER_GETUI_MASTER_SECRET");
        let getui_base_url = non_empty_env("PUSH_WORKER_GETUI_BASE_URL");
        let getui_default_ttl_ms = env::var("PUSH_WORKER_GETUI_DEFAULT_TTL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(7_200_000);
        let getui_request_timeout_ms = env::var("PUSH_WORKER_GETUI_REQUEST_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(5_000);
        let event_ping_debounce_window_ms = env::var("PUSH_WORKER_EVENT_PING_DEBOUNCE_WINDOW_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .or(service.event_ping_debounce_window_ms)
            .unwrap_or(200);
        let metrics_enabled = parse_bool_env("PUSH_WORKER_METRICS_ENABLED")
            .or(service.metrics_enabled)
            .unwrap_or(true);
        let metrics_address = env::var("PUSH_WORKER_METRICS_ADDRESS")
            .ok()
            .or_else(|| service.metrics_address.clone())
            .unwrap_or_else(|| "0.0.0.0".to_string());
        let metrics_port = env::var("PUSH_WORKER_METRICS_PORT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .or(service.metrics_port)
            .unwrap_or(19186);
        let metrics_path = env::var("PUSH_WORKER_METRICS_PATH")
            .ok()
            .or_else(|| service.metrics_path.clone())
            .unwrap_or_else(|| "/metrics".to_string());
        let mut metrics =
            MetricsEndpointConfig::new(metrics_address, metrics_port).with_path(metrics_path);
        metrics.enabled = metrics_enabled;

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
            push_online_topic,
            push_offline_topic,
            push_dlq_topic,
            online_service_endpoint,
            access_gateway_static_endpoint,
            offline_parking_capacity,
            offline_delivery_backend,
            offline_outbox_redis_url,
            offline_outbox_stream,
            offline_outbox_maxlen,
            device_token_redis_url,
            device_token_key_prefix,
            getui_app_id,
            getui_app_key,
            getui_master_secret,
            getui_base_url,
            getui_default_ttl_ms,
            getui_request_timeout_ms,
            event_ping_debounce_window_ms,
            metrics,
        }
    }
}

fn non_empty_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
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

fn resolve_offline_outbox_redis_url(
    env_value: Option<String>,
    redis_profile_url: Option<String>,
) -> Option<String> {
    match env_value.as_deref().map(str::trim) {
        Some(value) if value.eq_ignore_ascii_case("off") => None,
        Some(value) if value.eq_ignore_ascii_case("disabled") => None,
        Some(value) if !value.is_empty() => Some(value.to_string()),
        _ => Some(redis_profile_url.unwrap_or_else(|| "redis://127.0.0.1:6379".to_string())),
    }
}

impl NatsConsumerConfig for PushWorkerConfig {
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

impl NatsProducerConfig for PushWorkerConfig {
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

impl KafkaProducerConfig for PushWorkerConfig {
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

impl KafkaConsumerConfig for PushWorkerConfig {
    fn kafka_consumer_group(&self) -> &str {
        &self.consumer_group
    }
}

#[cfg(test)]
mod tests {
    use super::{OfflineDeliveryBackend, resolve_offline_outbox_redis_url};

    #[test]
    fn offline_delivery_backend_is_explicit() {
        assert_eq!(
            OfflineDeliveryBackend::parse("getui"),
            OfflineDeliveryBackend::Getui
        );
        assert_eq!(
            OfflineDeliveryBackend::parse("off"),
            OfflineDeliveryBackend::Disabled
        );
    }

    #[test]
    fn offline_outbox_defaults_to_push_redis_profile() {
        let url = resolve_offline_outbox_redis_url(
            None,
            Some("redis://push-redis.internal:6379".to_string()),
        );

        assert_eq!(url.as_deref(), Some("redis://push-redis.internal:6379"));
    }

    #[test]
    fn offline_outbox_uses_local_redis_when_profile_is_absent() {
        let url = resolve_offline_outbox_redis_url(None, None);

        assert_eq!(url.as_deref(), Some("redis://127.0.0.1:6379"));
    }

    #[test]
    fn offline_outbox_can_be_disabled_explicitly() {
        assert_eq!(
            resolve_offline_outbox_redis_url(Some("off".to_string()), None),
            None
        );
        assert_eq!(
            resolve_offline_outbox_redis_url(Some(" disabled ".to_string()), None),
            None
        );
    }

    #[test]
    fn offline_outbox_env_url_overrides_profile() {
        let url = resolve_offline_outbox_redis_url(
            Some("redis://override:6379".to_string()),
            Some("redis://profile:6379".to_string()),
        );

        assert_eq!(url.as_deref(), Some("redis://override:6379"));
    }
}
