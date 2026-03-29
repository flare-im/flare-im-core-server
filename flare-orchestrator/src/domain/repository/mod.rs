// 导入各个独立的仓储模块
pub mod conversation_repository;
pub mod message_publisher;
pub mod wal_repository;

// 重新导出各个仓储 trait
pub use conversation_repository::ConversationRepository;
pub use message_publisher::MessageEventPublisher;
pub use wal_repository::WalRepository;

// 重新导出基础设施层的具体实现类型
pub use crate::infrastructure::external::session_client::ConversationRepositoryItem;
pub use crate::infrastructure::messaging::mq_publisher::OrchestratorPublisher;
pub use crate::infrastructure::persistence::wal_repository_impl::WalRepositoryItem;
