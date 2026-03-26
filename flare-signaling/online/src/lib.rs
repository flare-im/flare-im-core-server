//! Signaling Online 服务
//!
//! 在消息事件流中支撑 Gateway 与 Route：连接注册与在线状态、设备路由；Push 侧查询在线目标与设备路由。
//! 详见 `flare-im-core/docs/message_event_flow.md`。
//!
//! 目录结构与 flare-storage/writer、reader 对齐：application、config、convert、domain、infrastructure、interface、service。

pub mod application;
pub mod config;
pub mod convert;
pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod service;
pub mod util;

pub use config::OnlineConfig;
pub use service::ApplicationBootstrap;
pub use service::ApplicationContext;
