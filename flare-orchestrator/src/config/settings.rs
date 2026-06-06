use std::sync::Once;
use std::{collections::HashMap, env};

use flare_im_core::config::FlareAppConfig;
use flare_im_core::constants::groups::ORCHESTRATOR_MAIN_GROUP_DEFAULT;
use flare_im_core::constants::topics::{TOPIC_MESSAGE_MAIN_DLQ, TOPIC_MESSAGE_MAIN_RETRY_5S};
use flare_im_core::metrics::MetricsEndpointConfig;
use flare_im_core::utils::normalize_tenant_id;
use flare_server_core::mq::kafka::{KafkaConsumerConfig, KafkaProducerConfig};
use flare_server_core::mq::nats::{
    NatsConsumerConfig, NatsProducerConfig, NatsStreamSpec, default_stream_specs,
};

use crate::domain::model::{ConversationType, MessageDefaults};

/// `flare-capability` gRPC 默认地址（无注册中心或未配置地址时使用）。
/// 与 `MessageOrchestratorConfig::resolve_capability_grpc_uri` 配套。
pub const DEFAULT_CAPABILITY_GRPC_URI: &str = "http://127.0.0.1:50110";

static WARN_EMPTY_CAPABILITY_GRPC_URI: Once = Once::new();

/// 会话生成模式：同步 gRPC 创建 vs 事件异步创建
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SessionCreationMode {
    /// 同步调用 Conversation 服务 ensure_conversation（默认，强一致）
    #[default]
    Sync,
    /// 发布 conversation.ensure 事件，由 Conversation 服务消费并幂等创建（低延迟，最终一致）
    Async,
}

impl SessionCreationMode {
    pub fn from_config_value(s: &str) -> Self {
        if s.eq_ignore_ascii_case("async") {
            Self::Async
        } else {
            Self::Sync
        }
    }
}

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
    pub wal_hash_key: Option<String>,
    pub wal_ttl_seconds: u64,
    pub wal_replay_enabled: bool,
    pub wal_replay_interval_ms: u64,
    pub wal_replay_error_backoff_ms: u64,
    pub wal_replay_batch_limit: usize,
    pub wal_replay_claim_lease_ms: u64,
    pub default_tenant_id: Option<String>,
    pub default_business_type: String,
    pub default_conversation_type: i32,
    pub default_sender_type: String,
    pub reader_endpoint: Option<String>,
    pub hook_config: Option<String>,
    pub hook_config_dir: Option<String>,
    pub conversation_service_type: Option<String>,
    /// 会话生成模式：sync（默认，同步 gRPC）| async（发布 conversation.ensure 事件）
    pub session_creation_mode: SessionCreationMode,
    /// 服务器 ID（用于服务注册，标识服务实例）
    pub server_id: Option<String>,
    /// 业务系统标识符（SVID），用于服务发现时的过滤
    /// 例如："svid.im"、"svid.customer" 等
    pub svid: Option<String>,
    /// 是否在加载 `hooks.toml` 后 **自动追加** 指向独立进程 `flare-capability` 的
    /// `HookPlugin.Call`（PreSend/PostSend）gRPC Hook。关闭时仅使用配置文件中的 Hook。
    /// 环境变量：`MESSAGE_ORCHESTRATOR_CAPABILITY_HOOKS_AUTO=1|true`。
    pub capability_hooks_auto: bool,
    /// `flare-capability` gRPC 地址（HookPlugin / ExtensionPlugin / CapabilityService 同端口），例如 `http://flare-capability:50051`。
    /// 环境变量：`MESSAGE_ORCHESTRATOR_CAPABILITY_GRPC_URI`（未设且开启 auto 时使用本机默认端口，见 wire 常量）。
    pub capability_grpc_uri: Option<String>,
    /// `EVENT_CALL_SIGNAL` 是否经 `CapabilityService.Dispatch` 联动 RTC（invite/accept/hangup）。
    /// `MESSAGE_ORCHESTRATOR_CAPABILITY_RTC_BRIDGE=1|true` 开启。
    pub capability_rtc_bridge_enabled: bool,
    /// CallSignal enrich 扩展失败是否 fail-open（默认 true：降级继续推送）。
    pub extension_call_signal_fail_open: bool,
    /// PostSend Hook 扩展失败是否 fail-open（默认 false：保持现有 fail-closed 语义）。
    pub extension_post_send_fail_open: bool,
    /// PreSend Hook 扩展单次执行超时（毫秒）。
    pub extension_pre_send_timeout_ms: u64,
    /// PreSend Hook 扩展失败重试次数。
    pub extension_pre_send_retry: u32,
    /// PostSend Hook 扩展单次执行超时（毫秒）。
    pub extension_post_send_timeout_ms: u64,
    /// PostSend Hook 扩展失败重试次数。
    pub extension_post_send_retry: u32,
    /// Event enrich 插件扩展单次执行超时（毫秒）。
    pub extension_event_enrich_timeout_ms: u64,
    /// Event enrich 插件扩展失败重试次数。
    pub extension_event_enrich_retry: u32,
    /// 扩展执行租户白名单（逗号分隔，空表示全量）。
    pub extension_tenant_allowlist: Vec<String>,
    /// Hook 扩展允许的消息类型（逗号分隔 i32，空表示全量）。
    pub extension_hook_message_type_allowlist: Vec<i32>,
    /// Plugin 扩展允许的事件类型（逗号分隔 i32，空表示全量）。
    /// 若非空，须包含 `EVENT_CALL_SIGNAL` 的整型值（与 `flare_proto::common::EventType` 一致，当前为 `10`），
    /// 否则 [`crate::application::handlers::plugin::CallCapabilityBridge`] enrich 会被扩展路由跳过。
    pub extension_plugin_event_type_allowlist: Vec<i32>,
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
    profile: Option<&flare_im_core::config::KafkaClusterConfig>,
) -> Vec<String> {
    parse_csv_strings(env::var(env_name).ok())
        .into_iter()
        .chain(profile.map(|p| p.brokers.clone()).unwrap_or_default())
        .collect::<Vec<_>>()
        .into_iter()
        .filter(|v| !v.trim().is_empty())
        .collect::<Vec<_>>()
}

fn parse_csv_i32(raw: Option<String>) -> Vec<i32> {
    raw.unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse::<i32>().ok())
        .collect()
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
                    .wal_store
                    .as_deref()
                    .and_then(|name| cfg.redis_profile(name))
                    .cloned();
                (Some(svc), jetstream_profile, kafka_profile, redis_profile)
            } else {
                (None, None, None, None)
            };

        let jetstream_url = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_JETSTREAM_URL",
            "STORAGE_JETSTREAM_URL",
        )
        .or_else(|| {
            jetstream_profile
                .as_ref()
                .map(|profile| profile.url.clone())
        })
        .unwrap_or_else(|| "nats://127.0.0.1:24222".to_string());

        let jetstream_timeout_ms = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_JETSTREAM_TIMEOUT_MS",
            "STORAGE_JETSTREAM_TIMEOUT_MS",
        )
        .and_then(|v| v.parse::<u64>().ok())
        .or_else(|| {
            jetstream_profile
                .as_ref()
                .and_then(|profile| profile.timeout_ms)
        })
        .unwrap_or(5000);
        let jetstream_retries = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_JETSTREAM_RETRIES",
            "STORAGE_JETSTREAM_RETRIES",
        )
        .and_then(|v| v.parse::<u32>().ok())
        .or_else(|| {
            jetstream_profile
                .as_ref()
                .and_then(|profile| profile.retries)
        })
        .unwrap_or(8);
        let jetstream_retry_backoff_ms = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_JETSTREAM_RETRY_BACKOFF_MS",
            "STORAGE_JETSTREAM_RETRY_BACKOFF_MS",
        )
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
        let message_dlq_topic = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_MESSAGE_DLQ_TOPIC",
            "STORAGE_MESSAGE_DLQ_TOPIC",
        )
        .or_else(|| {
            service_config
                .as_ref()
                .and_then(|service| service.message_dlq_topic.clone())
        })
        .unwrap_or_else(|| TOPIC_MESSAGE_MAIN_DLQ.to_string());
        let message_retry_topic = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_MESSAGE_RETRY_TOPIC",
            "STORAGE_MESSAGE_RETRY_TOPIC",
        )
        .or_else(|| {
            service_config
                .as_ref()
                .and_then(|service| service.message_retry_topic.clone())
        })
        .unwrap_or_else(|| TOPIC_MESSAGE_MAIN_RETRY_5S.to_string());
        let message_retry_delay_ms = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_MESSAGE_RETRY_DELAY_MS",
            "STORAGE_MESSAGE_RETRY_DELAY_MS",
        )
        .and_then(|value| value.parse::<u64>().ok())
        .or_else(|| {
            service_config
                .as_ref()
                .and_then(|service| service.message_retry_delay_ms)
        })
        .unwrap_or(5000);

        let redis_url = env_or_fallback("MESSAGE_ORCHESTRATOR_REDIS_URL", "STORAGE_REDIS_URL")
            .or_else(|| redis_profile.as_ref().map(|profile| profile.url.clone()));

        let wal_hash_key =
            env_or_fallback("MESSAGE_ORCHESTRATOR_WAL_HASH_KEY", "STORAGE_WAL_HASH_KEY")
                .or_else(|| {
                    service_config
                        .as_ref()
                        .and_then(|service| service.wal_hash_key.clone())
                })
                .or_else(|| redis_url.as_ref().map(|_| "storage:wal:buffer".to_string()));

        let wal_ttl_seconds = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_WAL_TTL_SECONDS",
            "STORAGE_WAL_TTL_SECONDS",
        )
        .and_then(|v| v.parse::<u64>().ok())
        .or_else(|| {
            service_config
                .as_ref()
                .and_then(|service| service.wal_ttl_seconds)
        })
        .or_else(|| {
            redis_profile
                .as_ref()
                .and_then(|profile| profile.ttl_seconds)
        })
        .unwrap_or(24 * 3600);

        let wal_replay_enabled = parse_bool(env_or_fallback(
            "MESSAGE_ORCHESTRATOR_WAL_REPLAY_ENABLED",
            "STORAGE_WAL_REPLAY_ENABLED",
        ))
        .or_else(|| {
            service_config
                .as_ref()
                .and_then(|service| service.wal_replay_enabled)
        })
        .unwrap_or(wal_hash_key.is_some());

        let wal_replay_interval_ms = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_WAL_REPLAY_INTERVAL_MS",
            "STORAGE_WAL_REPLAY_INTERVAL_MS",
        )
        .and_then(|v| v.parse::<u64>().ok())
        .or_else(|| {
            service_config
                .as_ref()
                .and_then(|service| service.wal_replay_interval_ms)
        })
        .unwrap_or(1000);

        let wal_replay_error_backoff_ms = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_WAL_REPLAY_ERROR_BACKOFF_MS",
            "STORAGE_WAL_REPLAY_ERROR_BACKOFF_MS",
        )
        .and_then(|v| v.parse::<u64>().ok())
        .or_else(|| {
            service_config
                .as_ref()
                .and_then(|service| service.wal_replay_error_backoff_ms)
        })
        .unwrap_or(5000);

        let wal_replay_batch_limit = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_WAL_REPLAY_BATCH_LIMIT",
            "STORAGE_WAL_REPLAY_BATCH_LIMIT",
        )
        .and_then(|v| v.parse::<usize>().ok())
        .or_else(|| {
            service_config
                .as_ref()
                .and_then(|service| service.wal_replay_batch_limit)
        })
        .unwrap_or(256);

        let wal_replay_claim_lease_ms = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_WAL_REPLAY_CLAIM_LEASE_MS",
            "STORAGE_WAL_REPLAY_CLAIM_LEASE_MS",
        )
        .and_then(|v| v.parse::<u64>().ok())
        .or_else(|| {
            service_config
                .as_ref()
                .and_then(|service| service.wal_replay_claim_lease_ms)
        })
        .unwrap_or(30_000);

        let default_tenant_id = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_DEFAULT_TENANT_ID",
            "STORAGE_DEFAULT_TENANT_ID",
        )
        .map(normalize_tenant_id);

        let default_business_type = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_DEFAULT_BUSINESS_TYPE",
            "STORAGE_DEFAULT_BUSINESS_TYPE",
        )
        .unwrap_or_else(|| "im".to_string());

        let default_conversation_type = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_DEFAULT_SESSION_TYPE",
            "STORAGE_DEFAULT_SESSION_TYPE",
        )
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(flare_proto::common::ConversationType::Single as i32);

        let default_sender_type = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_DEFAULT_SENDER_TYPE",
            "STORAGE_DEFAULT_SENDER_TYPE",
        )
        .unwrap_or_else(|| "user".to_string());

        // Storage Reader 用于编辑/撤回等操作时查询原消息；未配置时使用本地默认端口（与 config/services/storage-reader.toml 一致）
        let reader_endpoint = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_READER_ENDPOINT",
            "STORAGE_READER_ENDPOINT",
        )
        .or_else(|| Some("http://127.0.0.1:60083".to_string()));

        let hook_config =
            env_or_fallback("MESSAGE_ORCHESTRATOR_HOOKS_CONFIG", "STORAGE_HOOKS_CONFIG").or_else(
                || {
                    service_config
                        .as_ref()
                        .and_then(|service| service.hook_config.clone())
                },
            );

        let hook_config_dir = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_HOOKS_CONFIG_DIR",
            "STORAGE_HOOKS_CONFIG_DIR",
        )
        .or_else(|| {
            service_config
                .as_ref()
                .and_then(|service| service.hook_config_dir.clone())
        });

        // 从配置中获取 conversation_service_type
        let conversation_service_type = service_config
            .as_ref()
            .and_then(|service| service.conversation_service_type.clone())
            .or_else(|| env::var("MESSAGE_ORCHESTRATOR_SESSION_SERVICE_TYPE").ok());

        let session_creation_mode = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_SESSION_CREATION_MODE",
            "SESSION_CREATION_MODE",
        )
        .map(|s| SessionCreationMode::from_config_value(&s))
        .unwrap_or(SessionCreationMode::Async);

        // 从环境变量获取 server_id 和 svid
        let server_id = env_or_fallback("MESSAGE_ORCHESTRATOR_SERVER_ID", "SERVER_ID");

        let svid = env_or_fallback("MESSAGE_ORCHESTRATOR_SVID", "SVID")
            .or_else(|| Some("svid.im".to_string())); // 默认为 svid.im

        let capability_hooks_auto = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_CAPABILITY_HOOKS_AUTO",
            "ORCHESTRATOR_CAPABILITY_HOOKS_AUTO",
        )
        .map(|v| {
            let t = v.trim();
            matches!(t, "1" | "true" | "on" | "yes")
                || t.eq_ignore_ascii_case("true")
                || t.eq_ignore_ascii_case("yes")
                || t.eq_ignore_ascii_case("on")
        })
        .unwrap_or(false);

        let capability_grpc_uri = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_CAPABILITY_GRPC_URI",
            "FLARE_CAPABILITY_GRPC_URI",
        );

        let capability_rtc_bridge_enabled = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_CAPABILITY_RTC_BRIDGE",
            "ORCHESTRATOR_CAPABILITY_RTC_BRIDGE",
        )
        .map(|v| {
            let t = v.trim();
            matches!(t, "1" | "true" | "on" | "yes")
                || t.eq_ignore_ascii_case("true")
                || t.eq_ignore_ascii_case("yes")
                || t.eq_ignore_ascii_case("on")
        })
        .unwrap_or(false);

        let extension_call_signal_fail_open = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_EXTENSION_CALL_SIGNAL_FAIL_OPEN",
            "ORCHESTRATOR_EXTENSION_CALL_SIGNAL_FAIL_OPEN",
        )
        .map(|v| {
            let t = v.trim();
            matches!(t, "1" | "true" | "on" | "yes")
                || t.eq_ignore_ascii_case("true")
                || t.eq_ignore_ascii_case("yes")
                || t.eq_ignore_ascii_case("on")
        })
        .unwrap_or(true);

        let extension_post_send_fail_open = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_EXTENSION_POST_SEND_FAIL_OPEN",
            "ORCHESTRATOR_EXTENSION_POST_SEND_FAIL_OPEN",
        )
        .map(|v| {
            let t = v.trim();
            matches!(t, "1" | "true" | "on" | "yes")
                || t.eq_ignore_ascii_case("true")
                || t.eq_ignore_ascii_case("yes")
                || t.eq_ignore_ascii_case("on")
        })
        .unwrap_or(false);

        let extension_pre_send_timeout_ms = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_EXTENSION_PRE_SEND_TIMEOUT_MS",
            "ORCHESTRATOR_EXTENSION_PRE_SEND_TIMEOUT_MS",
        )
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1500);
        let extension_pre_send_retry = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_EXTENSION_PRE_SEND_RETRY",
            "ORCHESTRATOR_EXTENSION_PRE_SEND_RETRY",
        )
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);
        let extension_post_send_timeout_ms = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_EXTENSION_POST_SEND_TIMEOUT_MS",
            "ORCHESTRATOR_EXTENSION_POST_SEND_TIMEOUT_MS",
        )
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1200);
        let extension_post_send_retry = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_EXTENSION_POST_SEND_RETRY",
            "ORCHESTRATOR_EXTENSION_POST_SEND_RETRY",
        )
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);
        let extension_event_enrich_timeout_ms = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_EXTENSION_EVENT_ENRICH_TIMEOUT_MS",
            "ORCHESTRATOR_EXTENSION_EVENT_ENRICH_TIMEOUT_MS",
        )
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1800);
        let extension_event_enrich_retry = env_or_fallback(
            "MESSAGE_ORCHESTRATOR_EXTENSION_EVENT_ENRICH_RETRY",
            "ORCHESTRATOR_EXTENSION_EVENT_ENRICH_RETRY",
        )
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(1);

        let extension_tenant_allowlist = parse_csv_strings(env_or_fallback(
            "MESSAGE_ORCHESTRATOR_EXTENSION_TENANT_ALLOWLIST",
            "ORCHESTRATOR_EXTENSION_TENANT_ALLOWLIST",
        ));
        let extension_hook_message_type_allowlist = parse_csv_i32(env_or_fallback(
            "MESSAGE_ORCHESTRATOR_EXTENSION_HOOK_MESSAGE_TYPE_ALLOWLIST",
            "ORCHESTRATOR_EXTENSION_HOOK_MESSAGE_TYPE_ALLOWLIST",
        ));
        let extension_plugin_event_type_allowlist = parse_csv_i32(env_or_fallback(
            "MESSAGE_ORCHESTRATOR_EXTENSION_PLUGIN_EVENT_TYPE_ALLOWLIST",
            "ORCHESTRATOR_EXTENSION_PLUGIN_EVENT_TYPE_ALLOWLIST",
        ));

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
            wal_hash_key,
            wal_ttl_seconds,
            wal_replay_enabled,
            wal_replay_interval_ms,
            wal_replay_error_backoff_ms,
            wal_replay_batch_limit,
            wal_replay_claim_lease_ms,
            default_tenant_id,
            default_business_type,
            default_conversation_type,
            default_sender_type,
            reader_endpoint,
            hook_config,
            hook_config_dir,
            conversation_service_type,
            session_creation_mode,
            server_id,
            svid,
            capability_hooks_auto,
            capability_grpc_uri,
            capability_rtc_bridge_enabled,
            extension_call_signal_fail_open,
            extension_post_send_fail_open,
            extension_pre_send_timeout_ms,
            extension_pre_send_retry,
            extension_post_send_timeout_ms,
            extension_post_send_retry,
            extension_event_enrich_timeout_ms,
            extension_event_enrich_retry,
            extension_tenant_allowlist,
            extension_hook_message_type_allowlist,
            extension_plugin_event_type_allowlist,
            metrics,
        }
    }

    /// Hook `capability_hooks_auto` 与 RTC 桥共用的能力服务 gRPC 地址（环境变量未设或为空时打一次 warn 并使用 [`DEFAULT_CAPABILITY_GRPC_URI`]）。
    pub fn resolve_capability_grpc_uri(&self) -> String {
        if let Some(ref u) = self.capability_grpc_uri {
            let t = u.trim();
            if !t.is_empty() {
                return t.to_string();
            }
        }
        WARN_EMPTY_CAPABILITY_GRPC_URI.call_once(|| {
            tracing::warn!(
                fallback = %DEFAULT_CAPABILITY_GRPC_URI,
                "MESSAGE_ORCHESTRATOR_CAPABILITY_GRPC_URI unset or empty; using default for flare-capability (hooks / RTC bridge)"
            );
        });
        DEFAULT_CAPABILITY_GRPC_URI.to_string()
    }

    /// 从应用配置加载（新方式，推荐）
    pub fn from_app_config(app: &FlareAppConfig) -> Self {
        Self::from_sources(Some(app))
    }

    pub fn defaults(&self) -> MessageDefaults {
        let default_conversation_type =
            ConversationType::from_proto(self.default_conversation_type);

        MessageDefaults {
            default_business_type: self.default_business_type.clone(),
            default_conversation_type,
            default_sender_type: self.default_sender_type.clone(),
            default_tenant_id: self.default_tenant_id.clone(),
        }
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
