//! Push Proxy 配置:gRPC 监听地址与 JetStream Topic(以设计文档为准)

use flare_im_contracts::constants::topics::{
    TOPIC_PUSH_ENVELOPE, TOPIC_PUSH_MESSAGES, TOPIC_PUSH_OFFLINE, TOPIC_PUSH_ONLINE,
};
use flare_im_service_kit::config::FlareAppConfig;
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
    /// 统一推送信封入站 Topic(通知/自定义推送)
    pub push_envelope_topic: String,
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
    /// 设备厂商 token registry Redis URL
    pub device_token_redis_url: String,
    /// 设备厂商 token registry key 前缀
    pub device_token_key_prefix: String,
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

        let jetstream_url = env::var("PUSH_PROXY_JETSTREAM_URL")
            .ok()
            .or_else(|| jetstream_profile.map(|c| c.url.clone()))
            .unwrap_or_else(|| "nats://127.0.0.1:24222".to_string());

        let push_request_topic = env::var("PUSH_PROXY_PUSH_REQUEST_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_MESSAGES.to_string());

        let push_envelope_topic = env::var("PUSH_PROXY_PUSH_ENVELOPE_TOPIC")
            .ok()
            .unwrap_or_else(|| TOPIC_PUSH_ENVELOPE.to_string());

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
        let device_token_redis_url = resolve_device_token_redis_url(
            env::var("PUSH_PROXY_DEVICE_TOKEN_REDIS_URL").ok(),
            app.redis_profile("push").map(|profile| profile.url.clone()),
        );
        let device_token_key_prefix = env::var("PUSH_PROXY_DEVICE_TOKEN_KEY_PREFIX")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "flare:im:push:device_tokens".to_string());

        Self {
            mq_backend,
            jetstream_url,
            push_request_topic,
            push_envelope_topic,
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
            device_token_redis_url,
            device_token_key_prefix,
        }
    }
}

/// 设备推送绑定登记表的地址。Proxy 在这里登记、Push Server 与 Push Worker 从这里读，三者缺省逐项
/// 相同：显式配置优先，其次 `[redis.push]` profile，最后本机 Redis。
fn resolve_device_token_redis_url(
    env_value: Option<String>,
    push_profile_url: Option<String>,
) -> String {
    env_value
        .filter(|value| !value.trim().is_empty())
        .or(push_profile_url)
        .unwrap_or_else(|| "redis://127.0.0.1:6379".to_string())
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

#[cfg(test)]
mod tests {
    use super::resolve_device_token_redis_url;

    /// 登记表缺省与 Push Server、Push Worker 相同。缺省若落到本机，单容器部署里那里没有 Redis，
    /// 登记全部失败，路由与投递也就看不到任何绑定。
    #[test]
    fn device_token_registry_defaults_to_the_push_redis_profile() {
        let profile = Some("redis://push-redis.internal:6379/5".to_string());
        assert_eq!(
            resolve_device_token_redis_url(None, profile.clone()),
            "redis://push-redis.internal:6379/5"
        );
        // 空串与只有空白按未配置处理。
        for blank in ["", "  "] {
            assert_eq!(
                resolve_device_token_redis_url(Some(blank.to_string()), profile.clone()),
                "redis://push-redis.internal:6379/5"
            );
        }
    }

    #[test]
    fn an_explicit_device_token_registry_wins_over_the_profile() {
        assert_eq!(
            resolve_device_token_redis_url(
                Some("redis://registry.internal:6379/7".to_string()),
                Some("redis://push-redis.internal:6379/5".to_string()),
            ),
            "redis://registry.internal:6379/7"
        );
    }

    #[test]
    fn without_a_profile_the_registry_is_the_local_redis() {
        assert_eq!(
            resolve_device_token_redis_url(None, None),
            "redis://127.0.0.1:6379"
        );
    }
}
