//! gRPC 传输层：按 **控制面 / IM Hook / 媒体扩展 / 共享工具** 分包，避免单目录堆叠。

pub mod capability;
pub mod extensions;
pub mod hooks;
pub mod shared;

pub use capability::{
    CapabilityGrpcServer, CapabilityInvocationMetrics, CapabilityMetricsSnapshot,
};
pub use extensions::ExtensionPluginRouter;
pub use flare_grpc_proto::capability::capability_service_server::CapabilityServiceServer;
pub use hooks::{HookServiceServer, ImHookPluginServer};
