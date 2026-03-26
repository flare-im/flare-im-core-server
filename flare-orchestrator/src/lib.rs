//! # Flare Message Orchestrator
//!
//! 消息编排服务：承接**上行**末端（Client → Gateway → Router → **Orchestrator** → Kafka），事件归一化后写入 Kafka，
//! 由 Storage Writer 与 Push Server 分别消费；Push Server 负责**下行**（Kafka → Push Server → Gateway → Client）。见 docs/message_event_flow.md。
//!
//! 实现 [flare_im_core::MessageCommandHandler] 的 [application::handlers::CoreMessageCommandHandlerAdapter]
//! 供 Gateway 注入后长连接发消息/操作统一走本服务。

pub mod application;
pub mod config;
pub mod domain;
pub mod error;
pub mod infrastructure;
pub mod interface;
pub mod service;

pub use application::handlers::CoreMessageCommandHandlerAdapter;
pub use service::ApplicationBootstrap;
