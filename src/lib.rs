//! Flare IM Core 公共库
//!
//! 提供统一的配置加载、服务注册发现与 IM 核心领域（DDD + CQRS）。
//! 领域契约见 [domain]（跨服务端口）；应用层 Command/Query 在各微服务 crate 内实现。

pub mod abstractions;
pub mod ack;
pub mod config;
pub mod constants;
pub mod discovery;
pub mod domain;
pub mod event;
pub mod gateway;
pub mod hooks;
pub mod message;
pub mod metrics;
pub mod service_names;
pub mod signaling;
pub mod tracing;
pub mod utils;

// Re-export Ctx（与 flare-server-core 一致，统一使用 Arc<Context>）
pub use flare_server_core::context::Ctx;
// Re-export context utilities（gRPC 提取 + MQ 编解码）
pub use utils::{
    context_from_mq_metadata, context_to_mq_metadata, extract_context_opt,
    extract_session_id_from_context, require_context, require_request_id_from_context,
    require_tenant_id_from_context, require_user_id_from_context,
};

// 重新导出 ACK 相关类型（AckServiceConfig 通过 ack::AckServiceConfig 访问）
pub use ack::{
    AckEvent, AckManager, AckModule, AckStatus, AckTimeoutEvent, AckType, ImportanceLevel,
};

pub use config::{
    AccessGatewayServiceConfig, ConfigManager, ConversationServiceConfig, FlareAppConfig,
    KafkaClusterConfig, MediaServiceConfig, MessageOrchestratorServiceConfig, MongoInstanceConfig,
    ObjectStoreConfig, PostgresInstanceConfig, RedisPoolConfig, ServiceEndpointConfig,
    ServiceRuntimeConfig, SessionPolicyConfig, SignalingOnlineServiceConfig,
    SignalingRouteServiceConfig, StorageReaderServiceConfig, StorageWriterServiceConfig,
    app_config, load_config, load_config_with_validation,
};
pub use discovery::{
    BackendType,
    ChannelService,
    Discover,
    // 重新导出 flare-server-core 的发现相关类型
    DiscoveryBackend,
    DiscoveryConfig,
    DiscoveryFactory,
    HealthCheckConfig,
    InstanceMetadata,
    LoadBalanceStrategy,
    NamespaceConfig,
    // 类型别名
    Registry,
    ServiceClient,
    ServiceDiscover,
    ServiceDiscoverUpdater,
    ServiceInstance,
    ServiceRegistry,
    TagFilter,
    Updater,
    VersionConfig,
    build_gateway_router_from_app_config,
    connect_grpc_channel_from_app_config,
    init_from_app_config,
    init_from_config,
    init_from_registry_config,
    register_service_from_config,
    register_service_from_registry_config,
    register_service_only,
};
pub use error::*;
pub use gateway::{GatewayRouter, GatewayRouterConfig, GatewayRouterError, GatewayRouterTrait};
pub use hooks::*;
pub use message::{Attachment, Message as MessageDomain, message_from_proto, message_to_proto};
pub use service_names::service_names::*;
pub use service_names::{get_service_name, service_name_env_var, validate_service_name};
pub use signaling::{
    ConnectionInfo as SignalingConnectionInfo, OnlineStatusInfo, RouteInfo, SignalingError,
    SignalingResult, current_timestamp_ms, generate_id, validate_gateway_id, validate_user_id,
};
pub use tracing::init_tracing_from_config;
pub use utils::error;
pub use utils::*;

// IM 跨服务领域类型（Gateway / Orchestrator / Signaling / Hook 等共用）
pub use domain::{
    ClientMessageId, ConnectionEvent, ConnectionId, ConversationId, ConversationSyncSlice,
    DeleteType, DeviceId, EventMeta, GatewayId, MarkType, MessageCommand, MessageCommandHandler,
    MessageId, MultiDeviceSyncResult, OperationResult, ReactionAction, SendAckResult,
    SendMessageCommand, Seq, SyncQueryHandler, SyncResult, UserId,
};

pub use event::{
    EventBusPublishError, ImTopicEventPublisher, encode_topic_event_envelope,
    publish_proto_as_server_event_envelope, to_event_envelope,
};
// flare-server-core 事件总线（JSON 信封 / MQ / 内存 Topic），与 IM proto Topic 并行能力
pub use flare_server_core::{
    DEFAULT_TOPIC_BROADCAST_CAPACITY, EVENT_ENVELOPE_CONTENT_TYPE, EventEnvelope,
    HEADER_CONTENT_TYPE, InMemoryTopicEventBus, MqTopicEventBus, TopicBroadcast,
    TopicEnvelopeHandler, TopicEnvelopeMessageHandler, TopicEventBus,
    register_topic_envelope_dispatcher, run_topic_event_consumer,
};

// Re-export helper functions (already exported via utils::*)
