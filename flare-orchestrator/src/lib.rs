//! # Flare Message Orchestrator
//!
//! 消息编排服务：承接**上行**末端（Client → Gateway → Router → **Orchestrator** → JetStream），事件归一化后写入 JetStream，
//! 由 Storage Writer 与 Push Server 分别消费；Push Server 负责**下行**（JetStream → Push Server → Gateway → Client）。见 docs/message_event_flow.md。

pub mod application;
pub mod bootstrap;
pub mod config;
pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod wire;

pub use bootstrap::ApplicationBootstrap;
