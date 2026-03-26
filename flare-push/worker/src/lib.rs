//! Flare Push Worker
//!
//! 以设计文档为准：
//! - 消费 `push-online` / `push-offline`
//! - 在线：查 Online 设备、按网关定位后由 `GatewayRouter` 直推 Access Gateway（与 access_gateway 对齐）
//! - 离线：占位实现（打印日志）
//! - 失败：转入 `push-dlq`

pub mod application;
pub mod config;
pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod service;

pub use service::ApplicationBootstrap;
