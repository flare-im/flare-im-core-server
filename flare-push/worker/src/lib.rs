//! Flare Push Worker
//!
//! 以设计文档为准：
//! - 消费 `push-online` / `push-offline`
//! - 在线：查 Online 设备、按网关定位后由 `GatewayRouter` 直推 Access Gateway（与 access_gateway 对齐）
//! - 离线：显式选择 FCM/APNs/Getui 或多通道 mobile；默认 disabled。
//!   Redis outbox 只属于开发暂存，不冒充手机投递。
//! - 失败：可重试错误 Nack 由 JetStream 重投；终态错误转入 `push-dlq`

pub mod application;
pub mod config;
pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod service;

pub use service::ApplicationBootstrap;
