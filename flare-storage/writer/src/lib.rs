//! # Flare Storage Writer
//!
//! 消息存储写侧服务（CQRS Command 侧）：消费 Kafka 并持久化，与项目 docs/message_event_flow.md 一致。
//!
//! ## 流程对应
//! - **普通消息**：Kafka 普通消息 topic → `NormalMessageConsumer` → `MessagePersistenceCommandHandler` → DB（保存消息）
//! - **操作事件**：Kafka 操作 topic → `OperationMessageConsumer` → `MessageOperationCommandHandler` → DB（更新撤回/编辑/删除/已读/反应/置顶/标记等）
//!
//! ## 模块
//! - 普通消息：`MessagePersistenceDomainService` → PostgreSQL + 可选 Redis/WAL
//! - 操作事件：`MessageOperationDomainService` → `ArchiveStoreRepository`（messages/operation_history/edit_history/visibility/read_records/reactions/pinned_messages/marked_messages）

pub mod application;
pub mod call;
pub mod config;
pub mod convert;
pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod service;

pub use service::ApplicationBootstrap;
