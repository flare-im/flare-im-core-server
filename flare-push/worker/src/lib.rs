//! Flare Push Worker
//!
//! 以设计文档为准：
//! - 消费 `push-online` / `push-offline`
//! - 在线：查 Online 设备、按网关定位后由 `GatewayRouter` 直推 Access Gateway（与 access_gateway 对齐）
//! - 离线：写入 Redis Stream outbox 持久化暂存（`RedisOfflineOutbox`），厂商通道
//!   （APNs/FCM 等）作为 `OfflinePushExecutor` 实现接入或从 outbox 消费
//! - 失败：可重试错误 Nack 由 JetStream 重投；终态错误转入 `push-dlq`

pub mod application;
pub mod config;
pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod service;

pub use service::ApplicationBootstrap;
