//! # Flare Push Server（以设计文档为准）
//!
//! 职责：
//! - 分别消费 `push-message` / `push-event` / `push-notification` / `push-ack` / `push-custom` topic
//! - 查询用户在线状态（flare-signaling/online）
//! - 路由到 `push-online` / `push-offline`
//! - 失败转入 `push-dlq`

pub mod application;
pub mod config;
pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod service;

pub use service::ApplicationBootstrap;
