//! # Flare Storage Reader
//!
//! 消息存储读侧（CQRS Query 侧）：仅提供消息与可见性等查询，不写入。
//! 流程：gRPC → `StorageReaderGrpcHandler` → `MessageStorageQueryHandler` → `MessageStorage` / `VisibilityStorage`（PostgreSQL + 可选 Redis 缓存）。

pub mod application;
pub mod config;
pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod service;

pub use domain::repository::MessageStorage;
pub use service::ApplicationBootstrap;
