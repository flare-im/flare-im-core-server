use std::{collections::HashMap, env};

use flare_im_contracts::constants::groups::ORCHESTRATOR_MAIN_GROUP_DEFAULT;
use flare_im_contracts::constants::topics::{TOPIC_MESSAGE_MAIN_DLQ, TOPIC_MESSAGE_MAIN_RETRY_5S};
use flare_im_contracts::utils::normalize_tenant_id;
use flare_im_service_kit::config::FlareAppConfig;
use flare_im_service_kit::metrics::MetricsEndpointConfig;
use flare_server_core::mq::kafka::{KafkaConsumerConfig, KafkaProducerConfig};
use flare_server_core::mq::nats::{
    NatsConsumerConfig, NatsProducerConfig, NatsStreamSpec, default_stream_specs,
};

#[derive(Clone, Debug, Default)]
pub struct MessageOrchestratorConfig {
    pub mq_backend: String,
    pub jetstream_url: String,
    pub jetstream_timeout_ms: u64,
    pub jetstream_retries: u32,
    pub jetstream_retry_backoff_ms: u64,
    pub jetstream_stream_specs: Vec<NatsStreamSpec>,
    // 批量发送配置
    pub jetstream_batch_size: usize,      // 批量发送大小
    pub jetstream_flush_interval_ms: u64, // 刷新间隔（毫秒）
    pub kafka_brokers: Vec<String>,
    pub kafka_client_id: String,
    pub kafka_options: HashMap<String, String>,
    pub message_dlq_topic: String,
    pub message_retry_topic: String,
    pub message_retry_delay_ms: u64,
    pub redis_url: Option<String>,
    pub default_tenant_id: Option<String>,
    pub conversation_service_type: Option<String>,
    /// 服务器 ID（用于服务注册，标识服务实例）
    pub server_id: Option<String>,
    /// 业务系统标识符（SVID），用于服务发现时的过滤
    /// 例如："svid.im"、"svid.customer" 等
    pub svid: Option<String>,
    /// Prometheus 指标出口配置。
    pub metrics: MetricsEndpointConfig,
}

fn env_or_fallback(primary: &str, fallback: &str) -> Option<String> {
    env::var(primary).ok().or_else(|| env::var(fallback).ok())
}

fn stream_specs_from_app(app: Option<&FlareAppConfig>) -> Vec<NatsStreamSpec> {
    let specs = app
        .map(|cfg| cfg.jetstream_topology())
        .unwrap_or_default()
        .into_iter()
        .map(|spec| NatsStreamSpec::new(spec.stream_name, spec.subjects))
        .collect::<Vec<_>>();
    if specs.is_empty() {
        default_stream_specs()
    } else {
        specs
    }
}

fn parse_csv_strings(raw: Option<String>) -> Vec<String> {
    raw.unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn parse_bool(raw: Option<String>) -> Option<bool> {
    raw.and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" => Some(true),
        "0" | "false" | "off" | "no" => Some(false),
        _ => None,
    })
}

fn kafka_brokers_from_env_or_profile(
    env_name: &str,
    profile: Option<&flare_im_service_kit::config::KafkaClusterConfig>,
) -> Vec<String> {
    parse_csv_strings(env::var(env_name).ok())
        .into_iter()
        .chain(profile.map(|p| p.brokers.clone()).unwrap_or_default())
        .collect::<Vec<_>>()
        .into_iter()
        .filter(|v| !v.trim().is_empty())
        .collect::<Vec<_>>()
}

impl MessageOrchestratorConfig {
    pub fn from_sources(app: Option<&FlareAppConfig>) -> Self {
        let jetstream_stream_specs = stream_specs_from_app(app);
        let mq_backend = app
            .map(|cfg| cfg.mq_default_backend().to_string())
            .or_else(|| env::var("FLARE_MQ_DEFAULT_BACKEND").ok())
            .unwrap_or_else(|| "nats".to_string())
            .to_ascii_lowercase();

        let (service_config, jetstream_profile, kafka_profile, redis_profile) =
            if let Some(cfg) = app {
                let svc = cfg.orchestrator_service();
                let jetstream_profile = svc
                    .jetstream
                    .as_deref()
                    .and_then(|name| cfg.jetstream_profile(name))
                    .cloned();
                let kafka_profile = cfg.kafka_profile("message").cloned();
                let redis_profile = svc
                    .redis_store
                    .as_deref()
                    .and_then(|name| cfg.redis_profile(name))
                    .cloned();
                (Some(svc), jetstream_profile, kafka_profile, redis_profile)
            } else {
                (None, None, None, None)
            };

        let jetstream_url = env::var("MESSAGE_ORCHESTRATOR_JETSTREAM_URL")
            .ok()
            .or_else(|| {
                jetstream_profile
                    .as_ref()
                    .map(|profile| profile.url.clone())
            })
            .unwrap_or_else(|| "nats://127.0.0.1:24222".to_string());

        let jetstream_timeout_ms = env::var("MESSAGE_ORCHESTRATOR_JETSTREAM_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| {
                jetstream_profile
                    .as_ref()
                    .and_then(|profile| profile.timeout_ms)
            })
            .unwrap_or(5000);
        let jetstream_retries = env::var("MESSAGE_ORCHESTRATOR_JETSTREAM_RETRIES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .or_else(|| {
                jetstream_profile
                    .as_ref()
                    .and_then(|profile| profile.retries)
            })
            .unwrap_or(8);
        let jetstream_retry_backoff_ms =
            env::var("MESSAGE_ORCHESTRATOR_JETSTREAM_RETRY_BACKOFF_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .or_else(|| {
                    jetstream_profile
                        .as_ref()
                        .and_then(|profile| profile.retry_backoff_ms)
                })
                .unwrap_or(25);

        // 批量发送配置
        let jetstream_batch_size = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_JETSTREAM_BATCH_SIZE",
            "JETSTREAM_BATCH_SIZE",
        )
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100); // 默认批量大小：100

        let jetstream_flush_interval_ms = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_JETSTREAM_FLUSH_INTERVAL_MS",
            "JETSTREAM_FLUSH_INTERVAL_MS",
        )
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(50); // 默认刷新间隔：50ms

        let kafka_brokers = kafka_brokers_from_env_or_profile(
            "MESSAGE_ORCHESTRATOR_KAFKA_BROKERS",
            kafka_profile.as_ref(),
        );
        let kafka_brokers = if kafka_brokers.is_empty() {
            vec!["127.0.0.1:29092".to_string()]
        } else {
            kafka_brokers
        };
        let kafka_client_id = env::var("MESSAGE_ORCHESTRATOR_KAFKA_CLIENT_ID")
            .ok()
            .or_else(|| kafka_profile.as_ref().and_then(|p| p.client_id.clone()))
            .unwrap_or_else(|| "flare-im-message".to_string());
        let kafka_options = kafka_profile
            .as_ref()
            .map(|p| p.options.clone())
            .unwrap_or_default();
        let message_dlq_topic = env::var("MESSAGE_ORCHESTRATOR_MESSAGE_DLQ_TOPIC")
            .ok()
            .or_else(|| {
                service_config
                    .as_ref()
                    .and_then(|service| service.message_dlq_topic.clone())
            })
            .unwrap_or_else(|| TOPIC_MESSAGE_MAIN_DLQ.to_string());
        let message_retry_topic = env::var("MESSAGE_ORCHESTRATOR_MESSAGE_RETRY_TOPIC")
            .ok()
            .or_else(|| {
                service_config
                    .as_ref()
                    .and_then(|service| service.message_retry_topic.clone())
            })
            .unwrap_or_else(|| TOPIC_MESSAGE_MAIN_RETRY_5S.to_string());
        let message_retry_delay_ms = env::var("MESSAGE_ORCHESTRATOR_MESSAGE_RETRY_DELAY_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .or_else(|| {
                service_config
                    .as_ref()
                    .and_then(|service| service.message_retry_delay_ms)
            })
            .unwrap_or(5000);

        let redis_url = env::var("MESSAGE_ORCHESTRATOR_REDIS_URL")
            .ok()
            .or_else(|| redis_profile.as_ref().map(|profile| profile.url.clone()));

        let default_tenant_id = env::var("MESSAGE_ORCHESTRATOR_DEFAULT_TENANT_ID")
            .ok()
            .map(normalize_tenant_id);

        // 从配置中获取 conversation_service_type
        let conversation_service_type = service_config
            .as_ref()
            .and_then(|service| service.conversation_service_type.clone())
            .or_else(|| env::var("MESSAGE_ORCHESTRATOR_SESSION_SERVICE_TYPE").ok());

        // 从环境变量获取 server_id 和 svid
        let server_id = env_or_fallback("MESSAGE_ORCHESTRATOR_SERVER_ID", "SERVER_ID");

        let svid = env_or_fallback("MESSAGE_ORCHESTRATOR_SVID", "SVID")
            .or_else(|| Some("svid.im".to_string())); // 默认为 svid.im

        let metrics_enabled = parse_bool(env_or_fallback(
            "MESSAGE_ORCHESTRATOR_METRICS_ENABLED",
            "ORCHESTRATOR_METRICS_ENABLED",
        ))
        .or_else(|| {
            service_config
                .as_ref()
                .and_then(|service| service.metrics_enabled)
        })
        .unwrap_or(true);
        let metrics_address = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_METRICS_ADDRESS",
            "ORCHESTRATOR_METRICS_ADDRESS",
        )
        .or_else(|| {
            service_config
                .as_ref()
                .and_then(|service| service.metrics_address.clone())
        })
        .unwrap_or_else(|| "0.0.0.0".to_string());
        let metrics_port = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_METRICS_PORT",
            "ORCHESTRATOR_METRICS_PORT",
        )
        .and_then(|value| value.parse::<u16>().ok())
        .or_else(|| {
            service_config
                .as_ref()
                .and_then(|service| service.metrics_port)
        })
        .unwrap_or(19181);
        let metrics_path = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_METRICS_PATH",
            "ORCHESTRATOR_METRICS_PATH",
        )
        .or_else(|| {
            service_config
                .as_ref()
                .and_then(|service| service.metrics_path.clone())
        })
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
            jetstream_batch_size,
            jetstream_flush_interval_ms,
            kafka_brokers,
            kafka_client_id,
            kafka_options,
            message_dlq_topic,
            message_retry_topic,
            message_retry_delay_ms,
            redis_url,
            default_tenant_id,
            conversation_service_type,
            server_id,
            svid,
            metrics,
        }
    }

    /// 从应用配置加载（新方式，推荐）
    pub fn from_app_config(app: &FlareAppConfig) -> Self {
        Self::from_sources(Some(app))
    }
}

impl NatsProducerConfig for MessageOrchestratorConfig {
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

impl NatsConsumerConfig for MessageOrchestratorConfig {
    fn nats_url(&self) -> &str {
        &self.jetstream_url
    }

    fn consumer_group(&self) -> &str {
        ORCHESTRATOR_MAIN_GROUP_DEFAULT
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

impl KafkaProducerConfig for MessageOrchestratorConfig {
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

impl KafkaConsumerConfig for MessageOrchestratorConfig {
    fn kafka_consumer_group(&self) -> &str {
        ORCHESTRATOR_MAIN_GROUP_DEFAULT
    }
}
