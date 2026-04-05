// 导入各个独立的仓储模块
pub mod conversation_repository;
pub mod wal_repository;
pub mod push_repository;
pub mod recipient_repository;

// 重新导出各个仓储 trait
pub use conversation_repository::ConversationRepository;
pub use push_repository::PushRepository;
pub use wal_repository::WalRepository;
pub use recipient_repository::{
    RecipientRepository,
    needs_member_lookup,
};

// 重新导出基础设施层的具体实现类型
pub use crate::infrastructure::rpc::ConversationClient;
pub use crate::infrastructure::persistence::wal_repository_impl::WalRepositoryItem;
