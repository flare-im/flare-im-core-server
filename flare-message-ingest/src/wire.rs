//! Wire 风格的依赖注入模块
//!
//! Message Ingest 只负责上行消息摄入：发送 gRPC、seq 分配、WAL、Pre/PostSend Hook、
//! conversation ensure 与写入主消息流。主流消费、存储/推送 fanout 和消息操作事件由
//! `flare-orchestrator` 独立承担。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use flare_im_contracts::service_names::{CONVERSATION, MESSAGE_INGEST};
use flare_im_hooks::hooks::adapters::DefaultHookFactory;
use flare_im_hooks::hooks::{
    HookConfig, HookConfigLoader, HookDefinition, HookDispatcher, HookRegistry, HookTransportConfig,
};
use flare_im_message_pipeline::{ConversationClient, MqPushRepository, RecipientRepositoryImpl};
use flare_im_seq::SequenceAllocator;
use flare_im_service_kit::metrics::MessageOrchestratorMetrics;
use flare_server_core::error::{AnyhowContext, Result};
use flare_server_core::mq::kafka::KafkaProducer;
use flare_server_core::mq::nats::NatsProducer;

use crate::application::extension::{
    ExtensionOrchestrator, ExtensionPolicy, ExtensionRouting, ExtensionRuntimePolicy,
};
use crate::application::handlers::{MessageIngestHandler, WalReplayHandler};
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
}

/// 构建应用上下文。
pub async fn initialize(
    app_config: &flare_im_service_kit::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    let config = Arc::new(MessageIngestConfig::from_app_config(app_config));

    let redis_client = build_redis_client(&config).await?;
    let mq_producer = build_mq_producer(&config).await?;
    let push_repository = MqPushRepository::new(mq_producer.clone());

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

    let message_ingest_service = Arc::new(MessageIngestService::new(
        push_repository,
        recipient_repository,
        wal_repository.clone(),
        Arc::new(sequence_allocator),
        config.defaults(),
        MessageIngestServiceOptions::new(config.large_conversation_materialize_threshold),
    ));

    let wal_replay_handler = Arc::new(WalReplayHandler::new(
        wal_repository,
        message_ingest_service.clone(),
        config.default_tenant_id.clone(),
    ));

    let hook_execution_service = Arc::new(HookExecutionService::new(
        build_hook_dispatcher(app_config, config.as_ref()).await?,
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
    let message_ingest_handler = Arc::new(MessageIngestHandler::new(
        message_ingest_service,
        extension_orchestrator,
        conversation_ensure_service,
        ingest_metrics.clone(),
    ));

    let message_send_grpc = MessageSendGrpcHandler::new(message_ingest_handler, ingest_metrics);

    tracing::info!(
        service = MESSAGE_INGEST,
        "Message ingest dependencies initialized"
    );

    Ok(ApplicationContext {
        message_send_grpc,
        wal_replay_handler,
        config,
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

async fn build_hook_dispatcher(
    _app_config: &flare_im_service_kit::config::FlareAppConfig,
    ingest_cfg: &MessageIngestConfig,
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

    let factory = DefaultHookFactory::new().map_err(|e| {
        flare_server_core::error::FlareError::system(format!("hook DefaultHookFactory: {e}"))
    })?;

    let pre_n = hook_cfg.pre_send.len();
    let post_n = hook_cfg.post_send.len();
    let delivery_n = hook_cfg.delivery.len();
    let recall_n = hook_cfg.recall.len();

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
