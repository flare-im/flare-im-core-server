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
pub mod config;
pub mod context;
pub mod domain;
pub mod error;
pub mod infrastructure;
pub mod interface;

// 重新导出常用类型
pub use config::Settings;
pub use context::Ctx;
pub use error::{GatewayError, Result};
