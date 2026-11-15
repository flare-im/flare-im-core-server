//! Push Server 配置（以设计文档为准）

use std::{collections::HashMap, env};

use super::notify::NotifyGateSettings;

use flare_im_contracts::constants::groups::PUSH_SERVER_CONSUMER_GROUP_DEFAULT;
use flare_im_contracts::constants::topics::{
    TOPIC_PUSH_DLQ, TOPIC_PUSH_ENVELOPE, TOPIC_PUSH_EVENTS, TOPIC_PUSH_MESSAGES,
    TOPIC_PUSH_OFFLINE, TOPIC_PUSH_OFFLINE_BULK, TOPIC_PUSH_OFFLINE_GROUP, TOPIC_PUSH_ONLINE,
};
use flare_im_contracts::domain::push_binding::PUSH_BINDING_INACTIVE_DAYS_DEFAULT;
use flare_im_contracts::utils::normalize_tenant_id;
use flare_im_service_kit::config::{FlareAppConfig, ServiceEndpointConfig};
use flare_im_service_kit::push_trace::PushTraceConfig;
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
    /// 离线任务的单聊通道（单聊、紧急、点名、修正）。
    pub push_offline_topic: String,
    /// 离线任务的群通道（群里的普通提示、尾部刷新）。
    pub push_offline_group_topic: String,
    /// 离线任务的批量通道（大群波次、@所有人 波次、摘要、补提示）。
    pub push_offline_bulk_topic: String,
    pub push_dlq_topic: String,
    pub redis_url: String,
    pub online_status_backend: String,
    /// 设备推送绑定所在的 Redis。必须与 Push Proxy 登记、Push Worker 读取的是同一个实例，
    /// 否则路由看不到任何绑定，一条离线任务都建不出来。
    pub device_token_redis_url: String,
    /// 设备推送绑定的 key 前缀，同样要与 Proxy、Worker 一致。
    pub device_token_key_prefix: String,
    /// 多少天没活跃的绑定视为不存在，应与 Worker 的同名设置一致：路由先不给它建任务，
    /// Worker 遇到时删除。
    pub binding_inactive_days: u32,

    /// flare-signaling/online 的 gRPC endpoint（与 `config/services/signaling-online.toml` 中 server.port 一致）
    pub online_service_endpoint: String,
    /// 大群 pure ping 在解析成员前按会话合并的窗口（毫秒），0 表示关闭。
    pub event_ping_coalesce_window_ms: u64,

    /// 默认 tenant（用于填充 Envelope）
    pub default_tenant_id: String,

    /// 防打扰闸门：状态所在的推送 Redis、到期队列与租户覆盖。
    pub notify: NotifyGateSettings,

    /// 推送决策链（`[services.push_server.trace]`），写在闸门所在的同一个推送 Redis。
    pub trace: PushTraceConfig,
}

/// 与 `signaling-online` 监听地址对齐：优先读 app 中 `[services.signaling_online.server]`，否则本地默认 50061。
fn default_signaling_online_grpc_endpoint(app: &FlareAppConfig) -> String {
    let so = app.signaling_online_service();
    let Some(server) = so.runtime.server.as_ref() else {
        return "http://127.0.0.1:50061".to_string();
    };
    endpoint_from_service_server(server, 50061)
}

fn endpoint_from_service_server(server: &ServiceEndpointConfig, default_port: u16) -> String {
    let port = server.port.unwrap_or(default_port);
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
                .map(|spec| {
                    NatsStreamSpec::new(spec.stream_name, spec.subjects)
                        .with_max_age(spec.max_age)
                        .with_duplicate_window(spec.duplicate_window)
                        .with_num_replicas(spec.num_replicas)
                        .with_max_bytes(spec.max_bytes)
                })
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
        // 另外两条通道只走环境变量，缺省与 Push Worker 的同名设置一致。
        let push_offline_group_topic = env::var("PUSH_SERVER_PUSH_OFFLINE_GROUP_TOPIC")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| TOPIC_PUSH_OFFLINE_GROUP.to_string());
        let push_offline_bulk_topic = env::var("PUSH_SERVER_PUSH_OFFLINE_BULK_TOPIC")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| TOPIC_PUSH_OFFLINE_BULK.to_string());
        let push_dlq_topic = env::var("PUSH_SERVER_PUSH_DLQ_TOPIC")
            .ok()
            .or_else(|| service.push_dlq_topic.clone())
            .unwrap_or_else(|| TOPIC_PUSH_DLQ.to_string());
        let redis_url = env::var("PUSH_SERVER_REDIS_URL")
            .ok()
            .or_else(|| {
                service
                    .redis
                    .as_deref()
                    .and_then(|name| app.redis_profile(name))
                    .map(|profile| profile.url.clone())
            })
            .unwrap_or_else(|| "redis://127.0.0.1:6379/0".to_string());
        let online_status_backend = env::var("PUSH_SERVER_ONLINE_STATUS_BACKEND")
            .ok()
            .or_else(|| service.online_status_backend.clone())
            .unwrap_or_else(|| "redis".to_string())
            .to_ascii_lowercase();
        // 缺省值与 Push Worker 的 PUSH_WORKER_DEVICE_TOKEN_* 逐项相同：两边读的是同一份绑定。
        let device_token_redis_url = env::var("PUSH_SERVER_DEVICE_TOKEN_REDIS_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| app.redis_profile("push").map(|profile| profile.url.clone()))
            .unwrap_or_else(|| "redis://127.0.0.1:6379".to_string());
        let device_token_key_prefix = env::var("PUSH_SERVER_DEVICE_TOKEN_KEY_PREFIX")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "flare:im:push:device_tokens".to_string());
        let binding_inactive_days = env::var("PUSH_SERVER_BINDING_INACTIVE_DAYS")
            .ok()
            .and_then(|value| value.trim().parse::<u32>().ok())
            .filter(|days| *days > 0)
            .unwrap_or(PUSH_BINDING_INACTIVE_DAYS_DEFAULT);

        let online_service_endpoint = env::var("PUSH_SERVER_ONLINE_SERVICE_ENDPOINT")
            .ok()
            .unwrap_or_else(|| default_signaling_online_grpc_endpoint(app));
        let event_ping_coalesce_window_ms = env::var("PUSH_SERVER_EVENT_PING_COALESCE_WINDOW_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .or(service.event_ping_coalesce_window_ms)
            .unwrap_or(200);

        let default_tenant_id = env::var("PUSH_SERVER_DEFAULT_TENANT_ID")
            .ok()
            .or_else(|| service.default_tenant_id.clone())
            .map(normalize_tenant_id)
            .unwrap_or_else(|| "0".to_string());

        // 闸门状态与设备绑定表默认在同一个推送 Redis（`[redis.push]`），可以单独指到别处。
        let gate_redis_url = env::var("PUSH_SERVER_GATE_REDIS_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| app.redis_profile("push").map(|profile| profile.url.clone()))
            .unwrap_or_else(|| "redis://127.0.0.1:6379".to_string());
        let notify =
            NotifyGateSettings::load(&flare_im_service_kit::resolve_config_path(), gate_redis_url);
        let trace =
            PushTraceConfig::load(&flare_im_service_kit::resolve_config_path(), "push_server");

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
            push_offline_group_topic,
            push_offline_bulk_topic,
            push_dlq_topic,
            redis_url,
            online_status_backend,
            device_token_redis_url,
            device_token_key_prefix,
            binding_inactive_days,
            online_service_endpoint,
            event_ping_coalesce_window_ms,
            default_tenant_id,
            notify,
            trace,
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

#[cfg(test)]
mod test_support {
    use super::{
        NotifyGateSettings, PUSH_BINDING_INACTIVE_DAYS_DEFAULT, PushServerConfig, PushTraceConfig,
    };

    impl PushServerConfig {
        /// 测试用配置：不指向任何真实依赖，各用例用结构体更新语法只覆盖自己关心的字段。
        /// 新增配置项时只改这一处，不必逐个用例补字段。
        pub(crate) fn for_tests() -> Self {
            Self {
                mq_backend: "nats".into(),
                jetstream_url: "nats://127.0.0.1:1".into(),
                jetstream_timeout_ms: 1_000,
                jetstream_retries: 0,
                jetstream_retry_backoff_ms: 0,
                jetstream_stream_specs: Vec::new(),
                kafka_brokers: Vec::new(),
                kafka_client_id: String::new(),
                kafka_options: Default::default(),
                consumer_group: "push-server".into(),
                push_message_topic: String::new(),
                push_event_topic: String::new(),
                push_envelope_topic: String::new(),
                push_online_topic: String::new(),
                push_offline_topic: String::new(),
                push_offline_group_topic: String::new(),
                push_offline_bulk_topic: String::new(),
                push_dlq_topic: String::new(),
                redis_url: String::new(),
                online_status_backend: "redis".into(),
                device_token_redis_url: String::new(),
                device_token_key_prefix: String::new(),
                binding_inactive_days: PUSH_BINDING_INACTIVE_DAYS_DEFAULT,
                online_service_endpoint: String::new(),
                event_ping_coalesce_window_ms: 0,
                default_tenant_id: "0".into(),
                notify: NotifyGateSettings::default(),
                trace: PushTraceConfig::default(),
            }
        }
    }
}
