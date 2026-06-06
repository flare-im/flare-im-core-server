// =============================================================================
// Flare HTTP-gRPC Gateway
// =============================================================================
// 一个高性能的 HTTP → gRPC 网关,用于 IM 系统
//
// 架构: DDD + CQRS
// - domain/: 领域层(纯业务逻辑)
// - application/: 应用层(编排)
// - infrastructure/: 基础设施层(gRPC客户端等)
// - interface/: 接口层(HTTP/gRPC)
// =============================================================================

pub mod application;
pub mod domain;
pub mod interface;

// 重新导出常用类型
pub use flare_im_core::gateway::GatewaySettings;
pub use flare_server_core::context::Ctx;
pub use flare_server_core::http::{HttpApiError as GatewayError, Result};
