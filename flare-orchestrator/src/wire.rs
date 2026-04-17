//! Wire 风格的依赖注入模块
//!
//! 类似 Go 的 Wire 框架，提供简单的依赖构建方法
//!
//! **与 `flare-capability` 的集成**：不依赖 `flare-capability` crate。
//! - [`flare_im_core::hooks`] gRPC → `HookExtension`（PreSend/PostSend）。
//! - 可选：`EVENT_CALL_SIGNAL` → `CapabilityService.Dispatch`（`rtc.call.*`）；`Dispatch` 失败时**降级**
//!   仍推送事件（仅打 warn）。能力服务 URI 与 Hook auto 共用 [`MessageOrchestratorConfig::resolve_capability_grpc_uri`]。
//!
//! ## 依赖注入顺序
//! 1. 基础设施层（Infrastructure）：Redis、Kafka
//! 2. Repository 层：数据访问抽象
//! 3. Domain Service 层：业务逻辑
//! 4. Application Handler 层：用例编排
//! 5. Interface 层：gRPC/HTTP 控制器

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use flare_im_core::constants::topics::TOPIC_MESSAGE_MAIN;
use flare_im_core::hooks::adapters::DefaultHookFactory;
use flare_im_core::hooks::{HookConfig, HookConfigLoader, HookDefinition, HookDispatcher, HookRegistry, HookTransportConfig};
use flare_im_core::service_names::{CAPABILITY, CONVERSATION};
use flare_server_core::mq::consumer::dispatcher::{Dispatcher, TopicDispatcher};
use flare_server_core::mq::consumer::{ConsumerConfig, MessageHandler as MqMessageHandler};
use flare_server_core::mq::kafka::producer::KafkaProducer;

use crate::application::handlers::{
    EventHandler, MessageActionHandler, MessageHandler as AppMessageHandler, StorageHandler,
};
use crate::application::CallCapabilityBridge;
use crate::config::MessageOrchestratorConfig;
use crate::domain::service::{
    ConversationEnsureService, EventDomainService, HookExecutionService, MessageDomainService,
    SequenceAllocator,
};
use crate::infrastructure::rpc::{CapabilityDispatchClient, ConversationClient};
use crate::infrastructure::messaging::conversation_ensure_publisher::MqConversationEnsurePublisher;
use crate::infrastructure::messaging::push_repository::MqPushRepository;
use crate::infrastructure::persistence::redis_wal::RedisWalRepository;
use crate::infrastructure::persistence::recipient_repository::RecipientRepositoryImpl;
use crate::infrastructure::persistence::wal_repository_impl::WalRepositoryItem;
use crate::interface::grpc::{MessageActionGrpcHandler, MessageSendGrpcHandler};
use crate::interface::mq::StorageConsumerHandler;

/// 应用上下文 - 包含所有已初始化的服务
pub struct ApplicationContext {
    pub message_send_grpc: MessageSendGrpcHandler,
    pub message_action_grpc: MessageActionGrpcHandler,
    /// `TOPIC_MESSAGE_MAIN` 消费者逻辑（与 [ConsumerConfig]、[Dispatcher] 配套使用）
    pub storage_consumer_handler: Arc<StorageConsumerHandler>,
    pub consumer_config: ConsumerConfig,
    pub main_queue_dispatcher: Arc<dyn Dispatcher>,
    pub config: Arc<MessageOrchestratorConfig>,
}

/// 构建应用上下文
///
/// 类似 Go Wire 的 Initialize 函数，按照依赖顺序构建所有组件
pub async fn initialize(
    app_config: &flare_im_core::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    let config = Arc::new(MessageOrchestratorConfig::from_app_config(app_config));

    let redis_client = build_redis_client(&config).await?;

    let kafka_producer = build_kafka_producer(&config).await?;

    let push_repository = MqPushRepository::new(kafka_producer.clone());

    let conversation_service_type = config
        .conversation_service_type
        .as_deref()
        .unwrap_or(CONVERSATION);
    let conversation_channel = flare_im_core::discovery::connect_grpc_channel_from_app_config(
        app_config,
        conversation_service_type,
        "http://127.0.0.1:50090",
    )
    .await
    .map_err(|e| anyhow::anyhow!("connect conversation service ({conversation_service_type}) failed: {e}"))?;
    let conversation_repository = Arc::new(ConversationClient::new(conversation_channel));

    let recipient_repository: Arc<dyn crate::domain::repository::RecipientRepository> =
        Arc::new(RecipientRepositoryImpl::new(conversation_repository.clone()));

    let wal_repository: Arc<WalRepositoryItem> = Arc::new(WalRepositoryItem::Redis(
        Arc::new(RedisWalRepository::new(redis_client.clone(), config.clone())),
    ));

    let sequence_allocator = SequenceAllocator::new(redis_client.clone(), 100)
        .await
        .map_err(|e| anyhow::anyhow!("sequence allocator: {}", e))?;

    let message_domain_service = Arc::new(MessageDomainService::new(
        push_repository.clone(),
        recipient_repository.clone(),
        wal_repository,
        Arc::new(sequence_allocator.clone()),
        config.defaults(),
        None,
        None,
    ));

    let event_domain_service = Arc::new(EventDomainService::new(
        push_repository,
        recipient_repository,
        Arc::new(sequence_allocator),
        None,
    ));

    let hook_execution_service = Arc::new(HookExecutionService::new(
        build_hook_dispatcher(app_config, config.as_ref()).await?,
        config.default_tenant_id.clone(),
    ));

    let conversation_ensure_service = Arc::new(ConversationEnsureService::new(
        Some(conversation_repository.clone()),
        config.session_creation_mode,
        Some(MqConversationEnsurePublisher::new(kafka_producer)),
    ));

    let message_handler = Arc::new(AppMessageHandler::new(
        message_domain_service.clone(),
        hook_execution_service,
        conversation_ensure_service,
    ));

    let call_capability_bridge: Option<Arc<CallCapabilityBridge>> =
        if config.capability_rtc_bridge_enabled {
            let cap_fallback = config.resolve_capability_grpc_uri();
            let cap_channel = flare_im_core::discovery::connect_grpc_channel_from_app_config(
                app_config,
                CAPABILITY,
                &cap_fallback,
            )
            .await
            .map_err(|e| {
                anyhow::anyhow!("connect flare-capability for RTC bridge ({CAPABILITY}): {e}")
            })?;
            let cap_client = Arc::new(CapabilityDispatchClient::new(cap_channel));
            tracing::info!(
                endpoint = %cap_fallback,
                "Orchestrator RTC bridge: EVENT_CALL_SIGNAL → CapabilityService.Dispatch (Dispatch errors degrade to push-only)"
            );
            Some(Arc::new(CallCapabilityBridge::new(cap_client)))
        } else {
            None
        };

    let event_handler = Arc::new(EventHandler::new(
        event_domain_service.clone(),
        call_capability_bridge,
    ));

    let message_send_grpc =
        MessageSendGrpcHandler::new(message_handler, event_handler.clone());

    let message_action_handler = Arc::new(MessageActionHandler::new(event_handler));

    let message_action_grpc = MessageActionGrpcHandler::new(message_action_handler);

    let storage_handler = Arc::new(StorageHandler::new(
        message_domain_service,
        event_domain_service,
    ));
    let storage_consumer_handler = Arc::new(StorageConsumerHandler::new(storage_handler));

    let mut topic_dispatcher = TopicDispatcher::new();
    let mq_handler: Arc<dyn MqMessageHandler> = storage_consumer_handler.clone();
    topic_dispatcher
        .register(TOPIC_MESSAGE_MAIN.to_string(), mq_handler)
        .map_err(|e| anyhow::anyhow!("register main queue dispatcher: {}", e))?;
    let main_queue_dispatcher: Arc<dyn Dispatcher> = Arc::new(topic_dispatcher);

    let consumer_config = ConsumerConfig::default().with_concurrency(32);

    Ok(ApplicationContext {
        message_send_grpc,
        message_action_grpc,
        storage_consumer_handler,
        consumer_config,
        main_queue_dispatcher,
        config,
    })
}

async fn build_redis_client(
    config: &MessageOrchestratorConfig,
) -> Result<Arc<redis::Client>> {
    let redis_url = config
        .redis_url
        .as_ref()
        .context("Redis URL not configured")?;

    let client = redis::Client::open(redis_url.as_str())
        .with_context(|| format!("failed to open Redis client for {}", redis_url))?;

    Ok(Arc::new(client))
}

async fn build_kafka_producer(
    config: &MessageOrchestratorConfig,
) -> Result<Arc<dyn flare_server_core::mq::producer::Producer>> {
    let producer = KafkaProducer::new(config)
        .map_err(|e| anyhow::anyhow!("failed to create Kafka producer: {}", e))?;

    Ok(Arc::new(producer))
}

fn inject_flare_capability_hook_extension_targets(cfg: &mut HookConfig, endpoint: String) {
    cfg.pre_send.push(HookDefinition {
        name: "flare_capability_hook_extension_pre_send".into(),
        description: Some("Remote HookExtension.invoke_pre_send (flare-capability gRPC)".into()),
        enabled: true,
        priority: 100,
        transport: HookTransportConfig::Grpc {
            endpoint: endpoint.clone(),
            metadata: HashMap::new(),
        },
        ..Default::default()
    });
    cfg.post_send.push(HookDefinition {
        name: "flare_capability_hook_extension_post_send".into(),
        description: Some("Remote HookExtension.invoke_post_send (flare-capability gRPC)".into()),
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
    _app_config: &flare_im_core::config::FlareAppConfig,
    orchestrator_cfg: &MessageOrchestratorConfig,
) -> Result<Arc<HookDispatcher>> {
    let registry = HookRegistry::new();
    let mut loader = HookConfigLoader::new();
    if let Some(ref p) = orchestrator_cfg.hook_config {
        loader = loader.add_candidate(PathBuf::from(p));
    }
    if let Some(ref d) = orchestrator_cfg.hook_config_dir {
        loader = loader.add_candidate(PathBuf::from(d));
    }

    let mut hook_cfg = loader
        .load()
        .map_err(|e| anyhow::anyhow!("load hook config: {e}"))?;

    if orchestrator_cfg.capability_hooks_auto {
        let ep = orchestrator_cfg.resolve_capability_grpc_uri();
        inject_flare_capability_hook_extension_targets(&mut hook_cfg, ep);
    }

    let factory = DefaultHookFactory::new()
        .map_err(|e| anyhow::anyhow!("hook DefaultHookFactory: {e}"))?;

    let pre_n = hook_cfg.pre_send.len();
    let post_n = hook_cfg.post_send.len();
    let delivery_n = hook_cfg.delivery.len();
    let recall_n = hook_cfg.recall.len();

    hook_cfg
        .install(registry.clone(), &factory)
        .await
        .map_err(|e| anyhow::anyhow!("install hooks: {e}"))?;

    tracing::info!(
        pre_send = pre_n,
        post_send = post_n,
        delivery = delivery_n,
        recall = recall_n,
        capability_hooks_auto = orchestrator_cfg.capability_hooks_auto,
        "orchestrator message hooks installed"
    );

    Ok(Arc::new(HookDispatcher::new(registry)))
}
