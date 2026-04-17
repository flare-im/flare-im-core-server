//! gRPC 接口层：HookExtension / HookService / CapabilityService。

mod capability_service;
mod helpers;
mod hook_extension;
mod hook_service;

pub use capability_service::{
    CapabilityGrpcServer, CapabilityInvocationMetrics, CapabilityMetricsSnapshot,
};
pub use flare_grpc_proto::capability::capability_service_server::CapabilityServiceServer;
pub use hook_extension::HookExtensionServer;
pub use hook_service::HookServiceServer;
