//! Wire 风格的依赖注入模块
//!
//! Message Ingest 只负责上行消息摄入：发送 gRPC、seq 分配、WAL、Pre/PostSend Hook、
//! conversation ensure 与写入主消息流。主流消费、存储/推送 fanout 和消息操作事件由
//! `flare-orchestrator` 独立承担。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use flare_im_contracts::service_names::{CONVERSATION, MESSAGE_INGEST, STORAGE_READER};
use flare_im_hooks::hooks::adapters::DefaultHookFactory;
use flare_im_hooks::hooks::{
    HookConfig, HookConfigLoader, HookDefinition, HookDispatcher, HookRegistry, HookTransportConfig,
};
use flare_im_message_pipeline::{ConversationClient, MqPushRepository, RecipientRepositoryImpl};
use flare_im_seq::SequenceAllocator;
use flare_im_service_kit::metrics::MessageOrchestratorMetrics;
use flare_server_core::auth::SignedContextVerifier;
use flare_server_core::error::{AnyhowContext, Result};
use flare_server_core::mq::kafka::KafkaProducer;
use flare_server_core::mq::nats::NatsProducer;

use crate::application::extension::{
    ExtensionOrchestrator, ExtensionPolicy, ExtensionRouting, ExtensionRuntimePolicy,
};
use crate::application::handlers::{
    MessageIngestHandler, SendRateLimitConfig, SendRateLimiter, WalReplayHandler,
};
use crate::config::MessageIngestConfig;
use crate::domain::service::{
    ConversationEnsureService, HookExecutionService, MessageIngestService,
    MessageIngestServiceOptions,
};
use crate::infrastructure::messaging::conversation_ensure_publisher::MqConversationEnsurePublisher;
use crate::infrastructure::persistence::redis_wal::RedisWalRepository;
use crate::infrastructure::persistence::wal_repository_impl::WalRepositoryItem;
use crate::interface::grpc::MessageSendGrpcHandler;

/// 应用上下文 - 包含所有已初始化的服务
pub struct ApplicationContext {
    pub message_send_grpc: MessageSendGrpcHandler,
    pub wal_replay_handler: Arc<WalReplayHandler>,
    pub config: Arc<MessageIngestConfig>,
    /// 用量预聚合（`messages.accepted`）；启动层把它的 flush 循环挂进运行时。
    pub usage: flare_im_usage::UsageReporter,
}

/// 构建应用上下文。
pub async fn initialize(
    app_config: &flare_im_service_kit::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    let config = Arc::new(MessageIngestConfig::from_app_config(app_config));
    initialize_with_config(app_config, config).await
}

async fn initialize_with_config(
    app_config: &flare_im_service_kit::config::FlareAppConfig,
    config: Arc<MessageIngestConfig>,
) -> Result<ApplicationContext> {
    // Validate and install the policy before Redis/MQ I/O or any background work.
    // A broken explicit policy path must fail startup even when infrastructure is down.
    let hook_dispatcher =
        build_hook_dispatcher(config.as_ref(), app_config.uses_static_registry()).await?;

    let redis_client = build_redis_client(&config).await?;
    let mq_producer = build_mq_producer(&config).await?;
    let push_repository = MqPushRepository::from_env(mq_producer.clone())?;

    let conversation_service_type = config
        .conversation_service_type
        .as_deref()
        .unwrap_or(CONVERSATION);
    let conversation_channel =
        flare_im_service_kit::discovery::connect_grpc_channel_lazy_from_app_config(
            app_config,
            conversation_service_type,
            flare_im_service_kit::discovery::default_static_grpc_fallback(
                conversation_service_type,
            ),
        )
        .map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "lazy conversation channel ({conversation_service_type}) failed: {e}"
            ))
        })?;
    let conversation_repository = Arc::new(ConversationClient::new(conversation_channel));

    let recipient_repository: Arc<dyn crate::domain::repository::RecipientRepository> = Arc::new(
        RecipientRepositoryImpl::new(conversation_repository.clone()),
    );

    let wal_repository: Arc<WalRepositoryItem> = Arc::new(WalRepositoryItem::Redis(Arc::new(
        RedisWalRepository::new(redis_client.clone(), config.clone()),
    )));

    let sequence_allocator = SequenceAllocator::new(redis_client.clone(), 100)
        .await
        .map_err(|e| {
            flare_server_core::error::FlareError::system(format!("sequence allocator: {}", e))
        })?;

    // seq 回退自愈：懒连接 storage-reader，用其权威 max_seq 作 seq 分配 floor 下限。
    // 懒连接失败仅告警并退化为普通 INCR（绝不比现状更差），不阻断摄入服务启动。
    // floor 是摄入自己按租户与会话做的读：持有 Cell 私钥时以摄入的 Service 身份为消息租户签名，
    // 不再转发可能属于另一租户的入站签名；私钥存在但无效时启动失败。
    let floor_signer = flare_im_service_kit::signed_read::service_signer_from_env(MESSAGE_INGEST)?;
    if floor_signer.is_none() {
        tracing::warn!(
            "FLARE_INTERNAL_CTX_PRIVATE_KEY is missing: seq floor reads forward the inbound signature or go unsigned"
        );
    }
    let seq_floor_provider =
        match flare_im_service_kit::discovery::connect_grpc_channel_lazy_from_app_config(
            app_config,
            STORAGE_READER,
            flare_im_service_kit::discovery::default_static_grpc_fallback(STORAGE_READER),
        ) {
            Ok(channel) => {
                let provider: Arc<dyn crate::domain::repository::SeqFloorProvider> = Arc::new(
                crate::infrastructure::persistence::storage_seq_floor_provider::StorageSeqFloorProvider::from_channel(
                    channel,
                )
                .with_service_signer(floor_signer, MESSAGE_INGEST),
            );
                Some(provider)
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "storage-reader lazy channel unavailable; seq floor self-heal disabled (plain INCR)"
                );
                None
            }
        };

    let message_ingest_service = Arc::new(MessageIngestService::new(
        push_repository,
        recipient_repository,
        wal_repository.clone(),
        Arc::new(sequence_allocator),
        config.defaults(),
        MessageIngestServiceOptions {
            seq_floor_provider,
            ..MessageIngestServiceOptions::new()
        },
    ));

    let wal_replay_handler = Arc::new(
        WalReplayHandler::new(
            wal_repository,
            message_ingest_service.clone(),
            config.default_tenant_id.clone(),
        )
        .with_write_verifier(
            flare_im_service_kit::audit_effects::write_proof::ProofVerifier::from_env()
                .map_err(flare_server_core::error::FlareError::system)?,
        ),
    );

    let hook_execution_service = Arc::new(HookExecutionService::new(
        hook_dispatcher,
        config.default_tenant_id.clone(),
    ));

    let conversation_ensure_service = Arc::new(ConversationEnsureService::with_cache(
        Some(conversation_repository),
        config.session_creation_mode,
        Some(MqConversationEnsurePublisher::new(mq_producer)),
        config.conversation_ensure_cache_capacity,
        std::time::Duration::from_secs(config.conversation_ensure_cache_ttl_seconds),
    ));

    let extension_orchestrator = Arc::new(ExtensionOrchestrator::new(
        hook_execution_service,
        ExtensionPolicy::new(config.extension_post_send_fail_open).with_runtime(
            ExtensionRuntimePolicy::new(
                config.extension_pre_send_timeout_ms,
                config.extension_pre_send_retry,
            ),
            ExtensionRuntimePolicy::new(
                config.extension_post_send_timeout_ms,
                config.extension_post_send_retry,
            ),
        ),
        ExtensionRouting::new(
            config.extension_tenant_allowlist.clone(),
            config.extension_hook_message_type_allowlist.clone(),
        ),
    ));

    let ingest_metrics = Arc::new(MessageOrchestratorMetrics::new());

    // 摄入边界幂等(按 client_msg_id 去重重发)。reserve_ttl 须 ≥ 单次发送处理时长(覆盖客户端 ack 超时窗口),
    // 否则慢首发会被并发重试误判;result_ttl 覆盖合理重试窗口(含离线重连补发)。复用 WAL 的 Redis 连接。
    use crate::infrastructure::persistence::redis_idempotency::{
        DEFAULT_IDEMPOTENCY_RESERVE_TTL_SECS, DEFAULT_IDEMPOTENCY_RESULT_TTL_SECS,
        MIN_IDEMPOTENCY_RESULT_TTL_SECS,
    };
    let idem_reserve_ttl = std::env::var("INGEST_IDEMPOTENCY_RESERVE_TTL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_IDEMPOTENCY_RESERVE_TTL_SECS);
    let idem_result_ttl = std::env::var("INGEST_IDEMPOTENCY_RESULT_TTL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_IDEMPOTENCY_RESULT_TTL_SECS);
    if idem_result_ttl < MIN_IDEMPOTENCY_RESULT_TTL_SECS {
        tracing::warn!(
            configured_secs = idem_result_ttl,
            minimum_secs = MIN_IDEMPOTENCY_RESULT_TTL_SECS,
            "INGEST_IDEMPOTENCY_RESULT_TTL_SECS is shorter than the longest upstream redelivery span; a late retry of an accepted send can be stored twice"
        );
    }
    let idempotency_store = Arc::new(
        crate::infrastructure::persistence::redis_idempotency::RedisIngestIdempotencyStore::new(
            redis_client.clone(),
            idem_reserve_ttl,
            idem_result_ttl,
        ),
    );

    let mut message_ingest_handler = MessageIngestHandler::new(
        message_ingest_service,
        extension_orchestrator,
        conversation_ensure_service,
        ingest_metrics.clone(),
    )
    .with_idempotency(idempotency_store);
    // 租户运行时投影（tenants 表）：发送限流的 tenant 阈值先取投影 quota.core.send_qps。
    let tenant_runtime = match config.tenant_runtime_postgres_url.as_deref() {
        Some(url) => flare_im_service_kit::tenant_runtime::TenantRuntimeCache::connect(url, 2)
            .await
            .map_err(flare_server_core::error::FlareError::system)?,
        None => {
            tracing::info!(
                "tenant runtime database not configured; send quotas fall back to global rate limits"
            );
            flare_im_service_kit::tenant_runtime::TenantRuntimeCache::disabled()
        }
    };
    let tenant_policy = flare_im_service_kit::tenant_runtime::TenantAccessPolicy::from_env(
        "MESSAGE_INGEST_TENANT_POLICY",
    )
    .map_err(flare_server_core::error::FlareError::system)?
    .unwrap_or_default();
    if tenant_policy == flare_im_service_kit::tenant_runtime::TenantAccessPolicy::Strict
        && !tenant_runtime.is_enabled()
    {
        return Err(flare_server_core::error::FlareError::system(
            "strict message-ingest tenant policy requires tenant runtime projection database",
        ));
    }
    tracing::info!(
        policy = tenant_policy.as_str(),
        enabled = tenant_runtime.is_enabled(),
        "message-ingest tenant admission initialized"
    );
    message_ingest_handler = message_ingest_handler
        .with_tenant_runtime(tenant_runtime.clone())
        .with_tenant_policy(tenant_policy);

    let send_rate_limit_config = SendRateLimitConfig {
        enabled: config.send_rate_limit_enabled,
        tenant_per_second: config.send_rate_limit_tenant_per_second,
        tenant_sender_per_second: config.send_rate_limit_tenant_sender_per_second,
        tenant_conversation_per_second: config.send_rate_limit_tenant_conversation_per_second,
        window_ms: config.send_rate_limit_window_ms,
        max_tracked_keys: config.send_rate_limit_max_tracked_keys,
    };
    // 全局阈值全 0 但投影可用时限流器仍要在：租户配额可能随时投影进来。
    if send_rate_limit_config.is_effective()
        || (send_rate_limit_config.enabled && tenant_runtime.is_enabled())
    {
        message_ingest_handler = message_ingest_handler
            .with_send_rate_limiter(Arc::new(SendRateLimiter::new(send_rate_limit_config)));
    }
    if config.send_publish_timeout_ms > 0 {
        message_ingest_handler = message_ingest_handler.with_send_publish_timeout(
            std::time::Duration::from_millis(config.send_publish_timeout_ms),
        );
    }
    let usage = flare_im_usage::UsageReporter::from_env("flare-message-ingest").await?;
    let message_ingest_handler = Arc::new(message_ingest_handler.with_usage(usage.recorder()));

    let message_verifier = match std::env::var("FLARE_INTERNAL_CTX_PUBLIC_KEY") {
        Ok(material) => Some(
            SignedContextVerifier::from_public_key(&material).map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "invalid internal context public key: {e}"
                ))
            })?,
        ),
        Err(_) => {
            tracing::warn!(
                "FLARE_INTERNAL_CTX_PUBLIC_KEY is missing; message send RPCs fail closed"
            );
            None
        }
    };
    let message_send_grpc =
        MessageSendGrpcHandler::new(message_ingest_handler, ingest_metrics, message_verifier);

    tracing::info!(
        service = MESSAGE_INGEST,
        "Message ingest dependencies initialized"
    );

    Ok(ApplicationContext {
        message_send_grpc,
        wal_replay_handler,
        config,
        usage,
    })
}

async fn build_redis_client(config: &MessageIngestConfig) -> Result<Arc<redis::Client>> {
    let redis_url = config
        .redis_url
        .as_ref()
        .context("Redis URL not configured")?;

    let client = redis::Client::open(redis_url.as_str())
        .with_context(|| format!("failed to open Redis client for {}", redis_url))?;

    Ok(Arc::new(client))
}

async fn build_mq_producer(
    config: &MessageIngestConfig,
) -> Result<Arc<dyn flare_server_core::mq::producer::Producer>> {
    match config.mq_backend.as_str() {
        "kafka" => {
            let producer = KafkaProducer::new(config).map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "failed to create Kafka producer: {}",
                    e
                ))
            })?;
            Ok(Arc::new(producer))
        }
        "nats" => {
            let producer = NatsProducer::new(config).await.map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "failed to create JetStream producer: {}",
                    e
                ))
            })?;
            Ok(Arc::new(producer))
        }
        other => Err(flare_server_core::error::FlareError::system(format!(
            "unsupported mq backend: {}",
            other
        ))),
    }
}

fn inject_flare_capability_hook_plugin_targets(cfg: &mut HookConfig, endpoint: String) {
    cfg.pre_send.push(HookDefinition {
        name: "flare_capability_hook_plugin_pre_send".into(),
        description: Some("Remote HookPlugin.Call pre_send (flare-capability gRPC)".into()),
        enabled: true,
        priority: 100,
        transport: HookTransportConfig::Grpc {
            endpoint: endpoint.clone(),
            metadata: HashMap::new(),
        },
        ..Default::default()
    });
    cfg.post_send.push(HookDefinition {
        name: "flare_capability_hook_plugin_post_send".into(),
        description: Some("Remote HookPlugin.Call post_send (flare-capability gRPC)".into()),
        enabled: true,
        priority: 100,
        transport: HookTransportConfig::Grpc {
            endpoint,
            metadata: HashMap::new(),
        },
        ..Default::default()
    });
}

fn validate_static_hook_routes(hook_cfg: &HookConfig, static_registry_mode: bool) -> Result<()> {
    if !static_registry_mode {
        return Ok(());
    }
    for (kind, hooks) in [
        ("pre_send", &hook_cfg.pre_send),
        ("post_send", &hook_cfg.post_send),
        ("delivery", &hook_cfg.delivery),
        ("recall", &hook_cfg.recall),
    ] {
        for hook in hooks.iter().filter(|hook| hook.enabled) {
            if let HookTransportConfig::Grpc { endpoint, .. } = &hook.transport {
                let explicit_http =
                    endpoint.starts_with("http://") || endpoint.starts_with("https://");
                if !explicit_http
                    || tonic::transport::Endpoint::from_shared(endpoint.clone()).is_err()
                {
                    return Err(flare_server_core::error::FlareError::system(format!(
                        "{kind} hook {} requires an explicit private http(s) gRPC URI in FLARE_REGISTRY_MODE=static; discovery:// routes cannot resolve",
                        hook.name
                    )));
                }
            }
        }
    }
    Ok(())
}

async fn build_hook_dispatcher(
    ingest_cfg: &MessageIngestConfig,
    static_registry_mode: bool,
) -> Result<Arc<HookDispatcher>> {
    let registry = HookRegistry::new();
    let mut loader = HookConfigLoader::new();
    if let Some(ref p) = ingest_cfg.hook_config {
        loader = loader.add_candidate(PathBuf::from(p));
    }
    if let Some(ref d) = ingest_cfg.hook_config_dir {
        loader = loader.add_candidate(PathBuf::from(d));
    }

    let mut hook_cfg = loader.load().map_err(|e| {
        flare_server_core::error::FlareError::system(format!("load hook config: {e}"))
    })?;

    // Validate the declared policy before capability auto-dedup can remove a
    // discovery:// Social hook and conceal a broken static deployment profile.
    validate_static_hook_routes(&hook_cfg, static_registry_mode)?;

    if ingest_cfg.capability_hooks_auto {
        let ep = ingest_cfg.resolve_capability_grpc_uri();
        inject_flare_capability_hook_plugin_targets(&mut hook_cfg, ep);
        let before = hook_cfg.pre_send.len();
        hook_cfg.pre_send.retain(|hook| {
            !matches!(
                &hook.transport,
                HookTransportConfig::Grpc { endpoint, .. }
                    if endpoint.contains("flare-social-hook")
            )
        });
        let removed = before.saturating_sub(hook_cfg.pre_send.len());
        if removed > 0 {
            tracing::info!(
                removed,
                "deduped ingest pre_send hooks already executed via flare-capability"
            );
        }
    }

    // Also inspect generated capability targets before Redis/MQ initialization.
    validate_static_hook_routes(&hook_cfg, static_registry_mode)?;

    let factory = DefaultHookFactory::new().map_err(|e| {
        flare_server_core::error::FlareError::system(format!("hook DefaultHookFactory: {e}"))
    })?;

    // DefaultHookFactory installs each enabled definition or returns an error.
    // Disabled definitions must not masquerade as active policy gates in logs.
    let pre_n = hook_cfg.pre_send.iter().filter(|hook| hook.enabled).count();
    let post_n = hook_cfg
        .post_send
        .iter()
        .filter(|hook| hook.enabled)
        .count();
    let delivery_n = hook_cfg.delivery.iter().filter(|hook| hook.enabled).count();
    let recall_n = hook_cfg.recall.iter().filter(|hook| hook.enabled).count();

    hook_cfg
        .install(registry.clone(), &factory)
        .await
        .map_err(|e| flare_server_core::error::FlareError::system(format!("install hooks: {e}")))?;

    tracing::info!(
        pre_send = pre_n,
        post_send = post_n,
        delivery = delivery_n,
        recall = recall_n,
        capability_hooks_auto = ingest_cfg.capability_hooks_auto,
        "message ingest hooks installed"
    );

    Ok(Arc::new(HookDispatcher::new(registry)))
}

#[cfg(test)]
mod hook_startup_tests {
    use super::*;

    #[test]
    fn static_mode_rejects_discovery_hooks_but_accepts_explicit_uri() {
        let mut config = HookConfig::default();
        config.pre_send.push(HookDefinition {
            name: "social-policy-pre-send".into(),
            transport: HookTransportConfig::Grpc {
                endpoint: "discovery://flare-social-hook".into(),
                metadata: HashMap::new(),
            },
            ..Default::default()
        });
        assert!(validate_static_hook_routes(&config, false).is_ok());
        let error = validate_static_hook_routes(&config, true)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("explicit private http(s) gRPC URI"),
            "{error}"
        );
        config.pre_send[0].transport = HookTransportConfig::Grpc {
            endpoint: "http://social-hook.internal:50210".into(),
            metadata: HashMap::new(),
        };
        assert!(validate_static_hook_routes(&config, true).is_ok());
        config.pre_send[0].transport = HookTransportConfig::Grpc {
            endpoint: "http://".into(),
            metadata: HashMap::new(),
        };
        assert!(validate_static_hook_routes(&config, true).is_err());
    }

    #[test]
    fn static_mode_checks_all_enabled_hook_phases_only() {
        let discovery_hook = HookDefinition {
            name: "phase-hook".into(),
            transport: HookTransportConfig::Grpc {
                endpoint: "discovery://flare-social-hook".into(),
                metadata: HashMap::new(),
            },
            ..Default::default()
        };
        let mut config = HookConfig::default();
        config.pre_send.push(HookDefinition {
            enabled: false,
            ..discovery_hook.clone()
        });
        assert!(validate_static_hook_routes(&config, true).is_ok());
        for kind in 0..3 {
            config.post_send.clear();
            config.delivery.clear();
            config.recall.clear();
            match kind {
                0 => config.post_send.push(discovery_hook.clone()),
                1 => config.delivery.push(discovery_hook.clone()),
                _ => config.recall.push(discovery_hook.clone()),
            }
            assert!(validate_static_hook_routes(&config, true).is_err());
        }
    }

    #[test]
    fn shipped_social_profile_requires_static_endpoint_override() {
        let profile = concat!(env!("CARGO_MANIFEST_DIR"), "/../config/hooks.social.toml");
        let config = HookConfigLoader::new()
            .add_candidate(profile)
            .load()
            .unwrap();
        assert!(validate_static_hook_routes(&config, false).is_ok());
        assert!(validate_static_hook_routes(&config, true).is_err());
    }

    #[tokio::test]
    async fn capability_auto_cannot_hide_discovery_social_hook_in_static_mode() {
        let app_config = toml::from_str(
            r#"
            [service]
            name = "hook-static-test"
            version = "0.0.0"
            [server]
            address = "127.0.0.1"
            port = 0
            "#,
        )
        .unwrap();
        let mut config = MessageIngestConfig::from_app_config(&app_config);
        config.hook_config =
            Some(concat!(env!("CARGO_MANIFEST_DIR"), "/../config/hooks.social.toml").into());
        config.capability_hooks_auto = true;
        let error = match build_hook_dispatcher(&config, true).await {
            Ok(_) => panic!("discovery Social hook must fail before capability auto-dedup"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("social-policy-pre-send"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn missing_explicit_policy_fails_before_unconfigured_infrastructure() {
        let app_config = toml::from_str(
            r#"
            [service]
            name = "hook-startup-test"
            version = "0.0.0"
            [server]
            address = "127.0.0.1"
            port = 0
            "#,
        )
        .unwrap();
        let mut config = MessageIngestConfig::from_app_config(&app_config);
        config.hook_config = Some(
            std::env::temp_dir()
                .join(format!("missing-hook-{}", uuid::Uuid::new_v4()))
                .to_string_lossy()
                .into_owned(),
        );
        config.hook_config_dir = None;
        config.redis_url = None;
        config.mq_backend = "must-not-be-initialized".into();
        let error = match initialize_with_config(&app_config, Arc::new(config)).await {
            Ok(_) => panic!("missing explicit policy must prevent startup"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("load hook config"), "{error}");
    }
}
