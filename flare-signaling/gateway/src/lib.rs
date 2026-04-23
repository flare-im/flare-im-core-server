//! # Flare Signaling Gateway
//!
//! 长连接网关：**连接管理** + **access_gateway.proto 服务** + **四条消息上行线**（DDD+CQRS）。
//!
//! ## 分层
//!
//! - **入口 (interface)**：连接（WebSocket/QUIC）+ gRPC AccessGateway；解析 PayloadCommand，组 Command 委托应用层。
//! - **应用 (application)**：CQRS 写侧 — Command + CommandHandler（SendMessage / SendEvent / ReportAck / SendData）；Push 命令；查询。
//! - **领域 (domain)**：模型、仓储接口、领域服务。
//! - **基础设施 (infrastructure)**：连接上下文、领域端口适配器；长连接认证由 `application::handlers::AuthHandler` 实现。

#![recursion_limit = "512"]

pub mod application;
pub mod call_signal;
pub mod config;
pub mod constants;
pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod service;
mod utils;

pub use config::AccessGatewayConfig;
pub use service::ApplicationBootstrap;
pub mod error;
