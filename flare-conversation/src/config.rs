use flare_im_contracts::constants::groups::CONVERSATION_READ_RECEIPT_GROUP_DEFAULT;
use flare_im_contracts::constants::topics::{TOPIC_CONVERSATION_ENSURE, TOPIC_MESSAGE_EVENTS};
use flare_im_service_kit::config::FlareAppConfig;
use flare_server_core::error::Result;
use flare_server_core::mq::nats::{NatsStreamSpec, default_stream_specs};
use std::collections::HashMap;
use std::env;

use crate::domain::model::{ConflictResolutionPolicy, ConversationPolicy};

#[derive(Clone, Debug)]
pub struct ConversationConfig {
    pub mq_backend: String,
    pub redis_url: String,
    pub postgres_url: Option<String>,
    pub postgres_max_connections: u32,
    pub postgres_min_connections: u32,
    pub postgres_acquire_timeout_seconds: u64,
    pub postgres_idle_timeout_seconds: u64,
    pub postgres_max_lifetime_seconds: u64,
    pub conversation_state_prefix: String,
    pub conversation_unread_prefix: String,
    pub user_cursor_prefix: String,
    pub presence_prefix: String,
    pub large_conversation_precise_unread_threshold: i32,
    pub storage_reader_service: Option<String>,
    pub recent_message_limit: i32,
    pub default_policy: ConversationPolicy,
    /// JetStream bootstrap（配置则启用 ReadReceipt 消费者，未读数零成本更新）
    pub jetstream_url: Option<String>,
    /// 与 Storage Writer 相同的 operation subject（仅 use_events_topic_only=false 时用）
    pub jetstream_operation_subject: Option<String>,
    /// 统一事件流 subject，优先于 jetstream_operation_subject（与 Orchestrator 单事件流对齐）
    pub jetstream_events_subject: Option<String>,
    /// Consumer group（与 storage-writer 不同，避免抢消息）
    pub jetstream_group: Option<String>,
    pub jetstream_stream_specs: Vec<NatsStreamSpec>,
    pub kafka_brokers: Vec<String>,
    pub kafka_client_id: String,
    /// 会话确保 Subject（Orchestrator 异步会话创建时发布；配置则启用 ConversationEnsure 消费者）
    pub jetstream_ensure_subject: Option<String>,
    /// 会话确保消费者 group
    pub jetstream_ensure_group: Option<String>,
}

impl ConversationConfig {
    /// 从应用配置加载（新方式，推荐）
    pub fn from_app_config(app: &FlareAppConfig) -> Result<Self> {
        let service_config = app.conversation_service();
        let mq_backend = app.mq_default_backend().to_string();
        let kafka_profile = app.kafka_profile("message");

        // 解析 Redis 配置引用
        let redis_url = env::var("CONVERSATION_REDIS_URL")
            .or_else(|_| env::var("STORAGE_REDIS_URL"))
            .ok()
            .or_else(|| {
                if let Some(redis_name) = &service_config.redis {
                    app.redis_profile(redis_name)
                        .map(|profile| profile.url.clone())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "redis://127.0.0.1:6379/0".to_string());

        let postgres_profile = service_config
            .postgres
            .as_deref()
            .and_then(|name| app.postgres_profile(name));
        let postgres_url = env::var("CONVERSATION_POSTGRES_URL")
            .ok()
            .or_else(|| postgres_profile.map(|profile| profile.url.clone()));
        let postgres_max_connections = env::var("CONVERSATION_POSTGRES_MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .or_else(|| postgres_profile.and_then(|profile| profile.max_connections))
            .unwrap_or(24);
        let postgres_min_connections = env::var("CONVERSATION_POSTGRES_MIN_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .or_else(|| postgres_profile.and_then(|profile| profile.min_connections))
            .unwrap_or(4);
        let postgres_acquire_timeout_seconds =
            env::var("CONVERSATION_POSTGRES_ACQUIRE_TIMEOUT_SECONDS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .or_else(|| postgres_profile.and_then(|profile| profile.acquire_timeout_seconds))
                .unwrap_or(10);
        let postgres_idle_timeout_seconds = env::var("CONVERSATION_POSTGRES_IDLE_TIMEOUT_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| postgres_profile.and_then(|profile| profile.idle_timeout_seconds))
            .unwrap_or(300);
        let postgres_max_lifetime_seconds = env::var("CONVERSATION_POSTGRES_MAX_LIFETIME_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .or_else(|| postgres_profile.and_then(|profile| profile.max_lifetime_seconds))
            .unwrap_or(1800);

        let conversation_state_prefix = env::var("CONVERSATION_STATE_PREFIX")
            .ok()
            .or_else(|| service_config.conversation_state_prefix.clone())
            .unwrap_or_else(|| "storage:conversation:state".to_string());

        let conversation_unread_prefix = env::var("CONVERSATION_UNREAD_PREFIX")
            .ok()
            .or_else(|| service_config.conversation_unread_prefix.clone())
            .unwrap_or_else(|| "storage:conversation:unread".to_string());

        let user_cursor_prefix = env::var("CONVERSATION_USER_CURSOR_PREFIX")
            .ok()
            .or_else(|| service_config.user_cursor_prefix.clone())
            .unwrap_or_else(|| "storage:user:cursor".to_string());

        let presence_prefix = env::var("CONVERSATION_PRESENCE_PREFIX")
            .ok()
            .or_else(|| service_config.presence_prefix.clone())
            .unwrap_or_else(|| "presence:user".to_string());
        let large_conversation_precise_unread_threshold =
            env::var("CONVERSATION_LARGE_CONVERSATION_PRECISE_UNREAD_THRESHOLD")
                .ok()
                .and_then(|v| v.parse::<i32>().ok())
                .or(service_config.large_conversation_precise_unread_threshold)
                .unwrap_or(500)
                .max(0);

        let storage_reader_service = env::var("CONVERSATION_STORAGE_READER_SERVICE")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| service_config.storage_reader_service.clone());

        let recent_message_limit = env::var("CONVERSATION_RECENT_MESSAGE_LIMIT")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .or(service_config.recent_message_limit)
            .unwrap_or(20);

        // 解析策略配置
        let policy_cfg = service_config.default_policy.as_ref();

        let conflict_resolution = env::var("CONVERSATION_CONFLICT_RESOLUTION")
            .ok()
            .and_then(|s| ConflictResolutionPolicy::parse_config_value(s.trim()))
            .or_else(|| {
                policy_cfg
                    .and_then(|p| p.conflict_resolution.as_ref())
                    .and_then(|s| ConflictResolutionPolicy::parse_config_value(s.trim()))
            })
            .unwrap_or(ConflictResolutionPolicy::Coexist);

        let max_devices = env::var("CONVERSATION_POLICY_MAX_DEVICES")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .filter(|v| *v > 0)
            .or_else(|| policy_cfg.and_then(|p| p.max_devices))
            .filter(|v| *v > 0)
            .unwrap_or(5);

        let allow_anonymous = env::var("CONVERSATION_POLICY_ALLOW_ANONYMOUS")
            .ok()
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .or_else(|| policy_cfg.and_then(|p| p.allow_anonymous))
            .unwrap_or(false);

        let allow_history_sync = env::var("CONVERSATION_POLICY_ALLOW_HISTORY_SYNC")
            .ok()
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .or_else(|| policy_cfg.and_then(|p| p.allow_history_sync))
            .unwrap_or(true);

        let mut policy_metadata = HashMap::new();
        if let Ok(raw) = env::var("CONVERSATION_POLICY_METADATA") {
            for kv in raw.split(',') {
                if let Some((k, v)) = kv.split_once('=') {
                    policy_metadata.insert(k.trim().to_string(), v.trim().to_string());
                }
            }
        }

        let default_policy = ConversationPolicy {
            conflict_resolution,
            max_devices,
            allow_anonymous,
            allow_history_sync,
            metadata: policy_metadata,
        };

        let jetstream_url = env::var("CONVERSATION_JETSTREAM_URL")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| {
                service_config
                    .jetstream
                    .as_ref()
                    .and_then(|name| app.jetstream_profile(name))
                    .map(|p| p.url.clone())
            });
        let jetstream_operation_subject = jetstream_url.as_ref().map(|_| {
            env::var("CONVERSATION_JETSTREAM_OPERATION_SUBJECT")
                .ok()
                .filter(|s| !s.is_empty())
                .or_else(|| service_config.operation_subject.clone())
                .unwrap_or_else(|| TOPIC_MESSAGE_EVENTS.to_string())
        });
        let jetstream_events_subject = jetstream_url.as_ref().map(|_| {
            env::var("CONVERSATION_JETSTREAM_EVENTS_SUBJECT")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| TOPIC_MESSAGE_EVENTS.to_string())
        });
        let jetstream_group = jetstream_url.as_ref().map(|_| {
            env::var("CONVERSATION_JETSTREAM_GROUP")
                .ok()
                .filter(|s| !s.is_empty())
                .or_else(|| service_config.consumer_group.clone())
                .unwrap_or_else(|| CONVERSATION_READ_RECEIPT_GROUP_DEFAULT.to_string())
        });
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
        let kafka_brokers = env::var("CONVERSATION_KAFKA_BROKERS")
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
        let kafka_client_id = env::var("CONVERSATION_KAFKA_CLIENT_ID")
            .ok()
            .or_else(|| kafka_profile.and_then(|p| p.client_id.clone()))
            .unwrap_or_else(|| "flare-im-conversation".to_string());
        let jetstream_ensure_subject = jetstream_url.as_ref().and_then(|_| {
            env::var("CONVERSATION_JETSTREAM_ENSURE_SUBJECT")
                .ok()
                .filter(|s| !s.is_empty())
                .or_else(|| Some(TOPIC_CONVERSATION_ENSURE.to_string()))
        });
        let jetstream_ensure_group = jetstream_ensure_subject.as_ref().map(|_| {
            env::var("CONVERSATION_JETSTREAM_ENSURE_GROUP")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "conversation-ensure".to_string())
        });

        Ok(Self {
            mq_backend,
            redis_url,
            postgres_url,
            postgres_max_connections,
            postgres_min_connections,
            postgres_acquire_timeout_seconds,
            postgres_idle_timeout_seconds,
            postgres_max_lifetime_seconds,
            conversation_state_prefix,
            conversation_unread_prefix,
            user_cursor_prefix,
            presence_prefix,
            large_conversation_precise_unread_threshold,
            storage_reader_service,
            recent_message_limit,
            default_policy,
            jetstream_url,
            jetstream_operation_subject,
            jetstream_events_subject,
            jetstream_group,
            jetstream_stream_specs,
            kafka_brokers,
            kafka_client_id,
            jetstream_ensure_subject,
            jetstream_ensure_group,
        })
    }
}
