//! # Flare Storage Writer
//!
//! 消息存储写侧服务（CQRS Command 侧）：消费 Kafka 消息并持久化，支持普通消息与操作消息（撤回/编辑/删除/已读/反应/置顶/标记等）。
//!
//! ## 写入流程
//! - Kafka 普通消息 → `NormalMessageConsumer` → `MessagePersistenceCommandHandler` → `MessagePersistenceDomainService` → PostgreSQL + 可选 Redis 热缓存/WAL
//! - Kafka 操作消息 → `OperationMessageConsumer` → `MessageOperationCommandHandler` → `MessageOperationDomainService` → `ArchiveStoreRepository`（messages/operation_history/edit_history/visibility/read_records/reactions/pinned_messages/marked_messages）
//! - 存储实现：`PostgresMessageStore`（实现 `ArchiveStoreRepository`），操作写入 `OperationStore` 与各 FSM 表

pub mod application;
pub mod config;
pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod service;

pub use service::ApplicationBootstrap;
