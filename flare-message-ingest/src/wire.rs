//! Wire 风格的依赖注入模块
//!
//! Message Ingest 只负责上行消息摄入：发送 gRPC、seq 分配、WAL、PreSend Hook、
//! conversation ensure 与写入主消息流。主流消费、存储/推送 fanout 和消息操作事件由
//! `flare-orchestrator` 独立承担。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use flare_im_contracts::service_names::{CONVERSATION, MESSAGE_INGEST, STORAGE_READER};
use flare_im_hooks::hooks::adapters::DefaultHookFactory;
use flare_im_hooks::hooks::execution::{
    HookExecutionBuffer, HookExecutionGuard, HookExecutionMetrics, HookExecutionObserver,
    HookExecutionSink, HookRuntimeSettings, MetricsOnlyHookExecutions,
};
use flare_im_hooks::hooks::{
    HookConfig, HookConfigLoader, HookConfigReloader, HookConfigStore, HookDispatcher, HookRegistry,
};
use flare_im_message_pipeline::{ConversationClient, MqPushRepository, RecipientRepositoryImpl};
use flare_im_seq::SequenceAllocator;
use flare_im_service_kit::metrics::MessageOrchestratorMetrics;
use flare_server_core::auth::{
    INTERNAL_CTX_PUBLIC_KEY_ENV, INTERNAL_CTX_PUBLIC_KEYS_ENV, KeySet, SignedContextVerifier,
};
use flare_server_core::error::{AnyhowContext, Result};
use flare_server_core::mq::kafka::KafkaProducer;
use flare_server_core::mq::nats::NatsProducer;

use crate::application::extension::{ExtensionOrchestrator, ExtensionRouting};
use crate::application::handlers::{
    MessageIngestHandler, SendRateLimitConfig, SendRateLimiter, WalReplayHandler,
};
use crate::config::MessageIngestConfig;
use crate::domain::service::{
    ConversationEnsureService, HookExecutionService, MessageIngestService,
    MessageIngestServiceOptions,
};
use crate::infrastructure::messaging::conversation_ensure_publisher::MqConversationEnsurePublisher;
use crate::infrastructure::persistence::postgres_hook_store::PostgresHookStore;
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
    /// Hook 配置热加载与执行记录写入；启动层把它们挂进运行时。
    pub hook_tasks: HookBackgroundTasks,
}

/// Hook 运行时的后台任务（配了 Core 库时才有）。
#[derive(Default)]
pub struct HookBackgroundTasks {
    pub reloader: Option<HookConfigReloader>,
    pub writer: Option<HookExecutionWriter>,
}

/// 执行记录的批量写入：缓冲、去处与刷新间隔。
pub struct HookExecutionWriter {
    pub buffer: Arc<HookExecutionBuffer>,
    pub sink: Arc<dyn HookExecutionSink>,
    pub flush_interval: Duration,
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
    // Validate the deployment policy before any I/O: a broken explicit policy path must fail
    // startup even when infrastructure is down. The database hooks come next, ahead of Redis/MQ:
    // when they cannot be read the service does not become ready.
    let static_registry_mode = app_config.uses_static_registry();
    let deployment_hooks = load_deployment_hooks(config.as_ref(), static_registry_mode)?;
    let (hook_dispatcher, hook_tasks) =
        build_hook_runtime(config.as_ref(), &deployment_hooks, static_registry_mode).await?;

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
            CONVERSATION,
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
    let floor_channel = flare_im_service_kit::discovery::connect_grpc_channel_lazy_from_app_config(
        app_config,
        STORAGE_READER,
        STORAGE_READER,
        flare_im_service_kit::discovery::default_static_grpc_fallback(STORAGE_READER),
    )
    .map_err(|e| {
        flare_server_core::error::FlareError::system(format!("sequence floor reader: {e}"))
    })?;
    let sequence_allocator = sequence_allocator.with_floor_provider(Arc::new(
        flare_im_service_kit::sequence_floor::StorageSeqFloorProvider::from_channel(floor_channel)
            .with_service_signer(floor_signer, MESSAGE_INGEST),
    ));

    let message_ingest_service = Arc::new(MessageIngestService::new(
        push_repository,
        recipient_repository,
        wal_repository.clone(),
        Arc::new(sequence_allocator),
        config.defaults(),
        MessageIngestServiceOptions::new(),
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
    // 发送幂等账本与 tenants 表同在 Core 库，复用同一个连接串；池子懒建，供持久回执确认与过 Redis 窗口的重发共用。
    match config.tenant_runtime_postgres_url.as_deref() {
        Some(url) => {
            use crate::infrastructure::persistence::postgres_idempotency_ledger::PostgresIdempotencyLedger;
            let ledger = match std::env::var("MESSAGE_INGEST_LEDGER_MAX_CONNECTIONS") {
                Ok(raw) => {
                    let limit = raw.parse().map_err(|_| {
                        flare_server_core::error::FlareError::system(
                            "invalid MESSAGE_INGEST_LEDGER_MAX_CONNECTIONS",
                        )
                    })?;
                    PostgresIdempotencyLedger::connect_with_limit(url, limit)?
                }
                Err(std::env::VarError::NotPresent) => {
                    PostgresIdempotencyLedger::connect_lazy(url)?
                }
                Err(e) => return Err(flare_server_core::error::FlareError::system(e.to_string())),
            };
            message_ingest_handler = message_ingest_handler.with_idempotency_ledger(
                Arc::new(ledger),
                // 与 Redis 存储实际使用的结果 TTL 一致（它把 0 抬成 1 秒）。
                crate::domain::model::LedgerLookbackPolicy::new(std::time::Duration::from_secs(
                    idem_result_ttl.max(1),
                )),
            );
        }
        None => tracing::warn!(
            "Core database not configured for message-ingest: resends beyond the Redis dedup window are not checked against the idempotency ledger"
        ),
    }

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

    let message_verifier = send_verifier(
        std::env::var(INTERNAL_CTX_PUBLIC_KEYS_ENV).ok().as_deref(),
        std::env::var(INTERNAL_CTX_PUBLIC_KEY_ENV).ok().as_deref(),
    )?;
    if message_verifier.is_none() {
        tracing::warn!(
            "{INTERNAL_CTX_PUBLIC_KEYS_ENV} and {INTERNAL_CTX_PUBLIC_KEY_ENV} are missing; message send RPCs fail closed"
        );
    }
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
        hook_tasks,
    })
}

/// 发送接口的验签器：组件公钥集（`FLARE_INTERNAL_CTX_PUBLIC_KEYS`，按 kid）叠在 Cell 单钥
/// （`FLARE_INTERNAL_CTX_PUBLIC_KEY`）上，与 Orchestrator 的加载相同。平台代发插件卡的上下文由
/// Capability 以 `kid=capability` 签名，只认公钥集里这把命名的公钥：只装单钥时这类上下文全部
/// 验不过，每张卡都会被当成投递失败。两者都没有时返回 `None`，发送接口 fail closed。
pub fn send_verifier(
    key_set: Option<&str>,
    single_key: Option<&str>,
) -> Result<Option<SignedContextVerifier>> {
    KeySet::from_sources(key_set, single_key)
        .map(|keys| keys.map(SignedContextVerifier::from_key_set))
        .map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "invalid internal context public keys: {e}"
            ))
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

fn validate_static_hook_routes(hook_cfg: &HookConfig, static_registry_mode: bool) -> Result<()> {
    if !static_registry_mode {
        return Ok(());
    }
    hook_cfg
        .pre_send
        .iter()
        .filter(|hook| hook.enabled)
        .try_for_each(|hook| hook.validate_static_route())
}

/// 部署文件：缺失或非法就报错，服务不启动。
fn load_deployment_hooks(
    ingest_cfg: &MessageIngestConfig,
    static_registry_mode: bool,
) -> Result<HookConfig> {
    let mut loader = HookConfigLoader::new();
    if let Some(ref p) = ingest_cfg.hook_config {
        loader = loader.add_candidate(PathBuf::from(p));
    }
    if let Some(ref d) = ingest_cfg.hook_config_dir {
        loader = loader.add_candidate(PathBuf::from(d));
    }
    let hook_cfg = loader.load().map_err(|e| {
        flare_server_core::error::FlareError::system(format!("load hook config: {e}"))
    })?;
    validate_static_hook_routes(&hook_cfg, static_registry_mode)?;
    Ok(hook_cfg)
}

/// 部署摘要里写的来源：配置的路径，没配时是内置的空 profile。
fn deployment_source(ingest_cfg: &MessageIngestConfig) -> String {
    let paths: Vec<&str> = [
        ingest_cfg.hook_config.as_deref(),
        ingest_cfg.hook_config_dir.as_deref(),
    ]
    .into_iter()
    .flatten()
    .collect();
    if paths.is_empty() {
        "embedded:hooks.core.toml".to_string()
    } else {
        paths.join(",")
    }
}

/// 执行证据与熔断参数的配置文件（环境变量优先）。
fn hook_runtime_config_file() -> PathBuf {
    std::env::var("MESSAGE_INGEST_RUNTIME_CONFIG_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("config/services/message-ingest.toml"))
}

/// 一个 Hook 运行时：部署文件装进部署段，`hook_configs` 全量读进平台段和应用段。读不到库就报错，
/// 服务不就绪。没配 Core 库的进程（本地联调）只跑部署 Hook，不记录执行证据。
async fn build_hook_runtime(
    ingest_cfg: &MessageIngestConfig,
    deployment_hooks: &HookConfig,
    static_registry_mode: bool,
) -> Result<(Arc<HookDispatcher>, HookBackgroundTasks)> {
    let registry = HookRegistry::new();
    let factory = Arc::new(DefaultHookFactory::new().map_err(|e| {
        flare_server_core::error::FlareError::system(format!("hook DefaultHookFactory: {e}"))
    })?);

    // DefaultHookFactory installs each enabled definition or returns an error.
    // Disabled definitions must not masquerade as active policy gates in logs.
    deployment_hooks
        .install(&registry, factory.as_ref())
        .map_err(|e| flare_server_core::error::FlareError::system(format!("install hooks: {e}")))?;
    tracing::info!(
        pre_send = deployment_hooks.enabled_count(),
        "message ingest hooks installed"
    );

    let settings = HookRuntimeSettings::from_sources(Some(hook_runtime_config_file().as_path()));
    let metrics = HookExecutionMetrics::registered();
    let mut tasks = HookBackgroundTasks::default();
    let mut observer: Arc<dyn HookExecutionObserver> =
        Arc::new(MetricsOnlyHookExecutions::new(Arc::clone(&metrics)));

    match ingest_cfg.tenant_runtime_postgres_url.as_deref() {
        Some(url) => {
            let store = Arc::new(PostgresHookStore::connect_lazy(url)?);
            let mut reloader = HookConfigReloader::new(
                Arc::clone(&store) as Arc<dyn HookConfigStore>,
                Arc::clone(&registry),
                factory,
                static_registry_mode,
            );
            reloader.load_initial().await.map_err(|e| {
                flare_server_core::error::FlareError::system(format!("load database hooks: {e}"))
            })?;
            tasks.reloader = Some(reloader);

            // 摘要只给页面看：写不进去不影响发送。
            if let Err(error) = store
                .write_deployment_summary(
                    MESSAGE_INGEST,
                    &deployment_source(ingest_cfg),
                    &deployment_hooks.summary(),
                    ingest_cfg.server_id.as_deref(),
                )
                .await
            {
                tracing::warn!(%error, "deployment hook summary not written");
            }

            if settings.executions.enabled {
                let buffer = Arc::new(HookExecutionBuffer::new(
                    settings.executions.buffer_settings(),
                    metrics,
                ));
                observer = buffer.clone();
                tasks.writer = Some(HookExecutionWriter {
                    buffer,
                    sink: store,
                    flush_interval: settings.executions.flush_interval,
                });
            } else {
                tracing::info!("hook execution recording disabled; circuit breakers stay on");
            }
        }
        None => tracing::warn!(
            "Core database not configured for message-ingest: only deployment hooks run, and hook executions are not recorded"
        ),
    }

    let dispatcher = HookDispatcher::new(registry)
        .with_guard(Arc::new(HookExecutionGuard::new(
            settings.circuit,
            observer,
        )))
        .with_budget(Duration::from_millis(
            ingest_cfg.extension_pre_send_timeout_ms,
        ));
    Ok((Arc::new(dispatcher), tasks))
}

#[cfg(test)]
mod hook_startup_tests {
    use std::collections::HashMap;

    use flare_im_hooks::hooks::{HookDefinition, HookTransportConfig};

    use super::*;

    #[test]
    fn shipped_defaults_select_the_empty_core_profile_not_a_business_profile_link() {
        for (raw, name) in [
            (
                include_str!("../../config/services/message-ingest.toml"),
                "message_ingest",
            ),
            (
                include_str!("../../config/services/message-orchestrator.toml"),
                "message_orchestrator",
            ),
            (
                include_str!("../../config/services/push-server.toml"),
                "push_server",
            ),
            (
                include_str!("../../config/services/push-worker.toml"),
                "push_worker",
            ),
        ] {
            let value: toml::Value = toml::from_str(raw).unwrap();
            assert_eq!(
                value["services"][name]["hook_config"].as_str(),
                Some("config/hooks.core.toml")
            );
        }
        let cfg: HookConfig = toml::from_str(include_str!("../../config/hooks.core.toml")).unwrap();
        assert!(cfg.pre_send.is_empty());
    }

    #[test]
    fn static_mode_rejects_discovery_hooks_but_accepts_explicit_uri() {
        let mut config = HookConfig::default();
        config.pre_send.push(HookDefinition {
            name: "social-policy-pre-send".into(),
            transport: HookTransportConfig::Grpc {
                endpoint: "discovery://flare-im-hook".into(),
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
    fn static_mode_checks_enabled_hooks_only() {
        let discovery_hook = HookDefinition {
            name: "phase-hook".into(),
            transport: HookTransportConfig::Grpc {
                endpoint: "discovery://flare-im-hook".into(),
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
        config.pre_send.push(discovery_hook);
        assert!(validate_static_hook_routes(&config, true).is_err());
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

    #[test]
    fn the_deployment_gate_rejects_a_discovery_social_hook_in_static_mode() {
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
        config.hook_config_dir = None;
        let error = match load_deployment_hooks(&config, true) {
            Ok(_) => panic!("a discovery Social hook must fail the static gate"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("social-policy-pre-send"),
            "{error}"
        );
    }

    #[test]
    fn the_summary_names_the_configured_paths_or_the_embedded_profile() {
        let mut config = MessageIngestConfig::default();
        assert_eq!(deployment_source(&config), "embedded:hooks.core.toml");
        config.hook_config = Some("/etc/flare/hooks.toml".into());
        config.hook_config_dir = Some("/etc/flare/hooks.d".into());
        assert_eq!(
            deployment_source(&config),
            "/etc/flare/hooks.toml,/etc/flare/hooks.d"
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
