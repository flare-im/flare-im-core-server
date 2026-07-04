pub mod conversation_repository;
pub mod ingest_idempotency;
pub mod wal_repository;

// 重新导出各个仓储 trait
pub use conversation_repository::ConversationRepository;
pub use flare_im_message_pipeline::{
    ConversationClient, PushRepository, RecipientRepository, needs_member_lookup,
};
pub use ingest_idempotency::{IdempotencyBegin, IdempotentRecord, IngestIdempotencyStore};
pub use wal_repository::{WalPendingMessage, WalRepository};

// 重新导出基础设施层的具体实现类型
pub use crate::infrastructure::persistence::wal_repository_impl::WalRepositoryItem;
