//! # Flare Push Proxy
//!
//! 暴露 PushService gRPC API：PushMessage / PushNotification 入队 MQ，可选 QueryPushStatus（Redis 粗粒度状态）。
//! 在线/离线分流由 Push Server / Worker 完成。

pub mod application;
pub mod config;
pub mod infrastructure;
pub mod interface;
pub mod service;

pub use service::ApplicationBootstrap;
