//! # Flare Message Ingest
//!
//! 消息摄入服务：承接**上行消息发送**末端（Client → Gateway → Router/API → Message Ingest → `flare.im.message.main`），
//! 实现 `MessageSendService.SendMessage`，负责发送校验、Pre/PostSend Hook、seq 分配、conversation ensure、WAL 与 broker accepted ACK。
//! **事件/操作上行不经本服务**，走 flare-orchestrator（`MessageEventService` / `MessageActionService`）。

pub mod application;
pub mod bootstrap;
pub mod config;
pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod wire;

pub use bootstrap::ApplicationBootstrap;
