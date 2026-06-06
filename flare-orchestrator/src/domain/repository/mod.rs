// 导入各个独立的仓储模块
pub mod capability_dispatch_gateway;
pub mod conversation_repository;
pub mod push_repository;
pub mod recipient_repository;
pub mod wal_repository;

// 重新导出各个仓储 trait
pub use capability_dispatch_gateway::CapabilityDispatchGateway;
pub use conversation_repository::ConversationRepository;
pub use push_repository::PushRepository;
pub use recipient_repository::{RecipientRepository, needs_member_lookup};
pub use wal_repository::{WalPendingMessage, WalRepository};

// 重新导出基础设施层的具体实现类型
pub use crate::infrastructure::persistence::wal_repository_impl::WalRepositoryItem;
pub use crate::infrastructure::rpc::ConversationClient;
