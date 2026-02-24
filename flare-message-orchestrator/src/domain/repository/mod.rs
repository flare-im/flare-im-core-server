// 导入各个独立的仓储模块
pub mod message_publisher;
pub mod wal_repository;
pub mod conversation_repository;
pub mod message_event_publisher;

// 重新导出各个仓储 trait，保持外部 API 兼容性
pub use message_publisher::MessageEventPublisher;
pub use wal_repository::WalRepository;
pub use conversation_repository::ConversationRepository;
pub use message_event_publisher::EventPublisher;

// 重新导出基础设施层的具体实现类型
pub use crate::infrastructure::messaging::message_publisher_impl::MessageEventPublisherItem;
pub use crate::infrastructure::persistence::wal_repository_impl::WalRepositoryItem;
pub use crate::infrastructure::external::conversation_repository_impl::ConversationRepositoryItem;
