//! # Flare Message Orchestrator
//!
//! 事件编排服务：承接**上行操作事件**末端（Client → Gateway → Router/API → **Orchestrator** → JetStream），
//! 实现 `MessageEventService.ExecuteEvent` 与 `MessageActionService`（撤回/编辑/已读/反应等），事件归一化后写入 JetStream，
//! 同时消费 `flare.im.message.main` 做 storage/push fanout。**消息发送上行不经本服务**，走 flare-message-ingest（`MessageSendService.SendMessage`）。
//! 见 docs/message_event_flow.md。

pub mod application;
pub mod bootstrap;
pub mod config;
pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod wire;

pub use bootstrap::ApplicationBootstrap;
