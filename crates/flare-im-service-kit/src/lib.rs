//! Runtime service kit for Flare IM server processes.

pub mod clients;
pub mod config;
pub mod discovery;
pub mod env_registry;
pub mod gateway;
pub mod gateway_auth;
pub mod health;
pub mod metrics;
pub mod runtime;
pub mod service_helper;
pub mod tracing;

pub use flare_im_contracts::{Ctx, service_names};
pub use flare_server_core::TopicEventBus;
pub use flare_server_core::discovery::{
    ServiceClient, ServiceDiscover, ServiceDiscoverUpdater, ServiceInstance, ServiceRegistry,
};
pub use flare_server_core::{Config, RegistryConfig, ServerConfig, ServiceConfig};

pub use clients::GrpcClients;
pub use config::{
    AccessGatewayServiceConfig, AdminGatewayServiceConfig, ApiGatewayServiceConfig,
    CapabilityServiceConfig, ConfigManager, ConversationServiceConfig, FlareAppConfig,
    JetStreamClusterConfig, KafkaClusterConfig, MediaServiceConfig, MessageIngestServiceConfig,
    MessageOrchestratorServiceConfig, MqBackendConfig, ObjectStoreConfig, PostgresInstanceConfig,
    PushProxyServiceConfig, RedisPoolConfig, ServiceEndpointConfig, ServiceRuntimeConfig,
    SessionPolicyConfig, SignalingOnlineServiceConfig, SignalingRouteServiceConfig,
    StorageReaderServiceConfig, StorageWriterServiceConfig, SyncOrchestratorServiceConfig,
    app_config, load_config, load_config_with_validation,
};
pub use discovery::{
    BackendType, ChannelService, Discover, DiscoveryBackend, DiscoveryConfig, DiscoveryFactory,
    HealthCheckConfig, InstanceMetadata, LoadBalanceStrategy, NamespaceConfig, Registry, TagFilter,
    Updater, VersionConfig, build_gateway_router_from_app_config,
    connect_grpc_channel_from_app_config, connect_grpc_channel_lazy_from_app_config,
    default_static_grpc_fallback, get_discovered_channel_with_timeout, init_from_app_config,
    init_from_config, init_from_registry_config, register_service_from_config,
    register_service_from_registry_config, register_service_only,
};
pub use gateway::{GatewayRouter, GatewayRouterConfig, GatewayRouterTrait};
pub use gateway_auth::{
    APP_ID_HEADER, auth_error_response, authenticate_http_request, build_validation_request,
    extract_bearer_token, header_value, inject_principal,
};
pub use runtime::{
    ImServiceRuntimePlan, background_service_runtime, build_background_service_runtime,
    build_service_runtime_plan, load_app_config_from_env, resolve_config_path,
};
pub use service_helper::ServiceHelper;
pub use tracing::init_tracing_from_config;
