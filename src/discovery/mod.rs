//! 服务发现模块
//!
//! `flare_server_core::discovery` 提供类型与后端；[init] 提供基于 `FlareAppConfig` 的启动封装（仅在本 crate）。
//! [grpc_connect] 提供通用 gRPC Channel / [`crate::gateway::GatewayRouter`] 构建，供 push-worker 等复用。
//!
//! ## 使用方式
//!
//! ```rust,ignore
//! use flare_im_core::discovery::init_from_app_config;
//! use std::net::SocketAddr;
//!
//! let address: SocketAddr = "127.0.0.1:8080".parse()?;
//! if let Some((registry, discover, updater)) = init_from_app_config("session", address, None).await? {
//!     // registry 会自动处理心跳续期
//!     // 当 registry 被 drop 时，会自动注销服务
//!     // 使用 discover 进行服务发现
//!     let _registry = registry;
//! }
//! ```

pub mod adapter;
pub mod grpc_connect;
pub mod init;

// 统一服务发现模块已移动到 flare-server-core
// 通过 re-export 提供访问
pub use flare_server_core::discovery::{
    BackendType, ChannelService, DiscoveryBackend, DiscoveryConfig, DiscoveryFactory,
    HealthCheckConfig, InstanceMetadata, LoadBalanceStrategy, NamespaceConfig, ServiceClient,
    ServiceDiscover, ServiceDiscoverUpdater, ServiceInstance, ServiceRegistry, TagFilter,
    VersionConfig,
};

pub use grpc_connect::{
    build_gateway_router_from_app_config, connect_grpc_channel_from_app_config,
};

// Re-exports
pub use init::{
    create_discover, create_discover_from_config, create_discover_from_registry_config,
    create_discover_from_registry_config_with_filters, init_from_app_config, init_from_config,
    init_from_registry_config, register_service_from_config,
    register_service_from_config_with_metadata, register_service_from_registry_config,
    register_service_from_registry_config_with_metadata, register_service_only,
    register_service_only_with_metadata, register_runtime_service_only,
    register_runtime_service_only_with_metadata,
};

// 适配器
pub use adapter::{ServiceRegistryAdapter, adapt_registry};

// 类型别名，方便使用
pub type Registry = ServiceRegistry;
pub type Discover = ServiceDiscover;
pub type Updater = ServiceDiscoverUpdater;
