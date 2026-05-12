//! # Flare Storage Reader
//!
//! 消息存储读侧（CQRS Query 侧）：仅提供消息查询，不写入。
//! 为 Conversation 同步（SyncRequest/SyncMessages）、Orchestrator 查询等提供读模型；写模型由 Storage Writer 消费 JetStream 更新。
//!
//! 流程：gRPC → `StorageReaderGrpcHandler` → `MessageStorageQueryHandler` → `MessageStorage`（PostgreSQL + 可选 Redis 缓存）。

pub mod application;
pub mod config;
pub mod convert;
pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod service;

pub use domain::repository::MessageStorage;
pub use service::ApplicationBootstrap;
