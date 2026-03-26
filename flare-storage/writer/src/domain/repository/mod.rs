//! 仓储接口定义（Port）
//!
//! 各 Repository 职责拆分到独立文件，便于维护与实现定位：
//! - [MessageIdempotencyRepository]：消息幂等判断（写前去重）
//! - [HotCacheRepository]：热数据缓存（落库后写缓存）
//! - [ArchiveStoreRepository]：归档存储（消息与操作持久化）
//! - [EventStreamRepository]：事件流（Sync 拉取）
//! - [WalCleanupRepository]：WAL 清理（持久化后移除 WAL 条目）
//! - [AckPublisher]：回执发布（ack 下发）

mod ack_publisher;
mod archive_store_repository;
mod event_stream_repository;
mod hot_cache_repository;
mod message_idempotency_repository;
mod wal_cleanup_repository;

pub use ack_publisher::AckPublisher;
pub use archive_store_repository::ArchiveStoreRepository;
pub use event_stream_repository::EventStreamRepository;
pub use hot_cache_repository::HotCacheRepository;
pub use message_idempotency_repository::MessageIdempotencyRepository;
pub use wal_cleanup_repository::WalCleanupRepository;
