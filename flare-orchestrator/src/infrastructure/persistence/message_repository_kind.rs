//! 将 Storage Reader 与 Noop 仓储合并为单一具体类型，使 [MessageOperationService] 在 wire 中单态化。

use flare_server_core::context::Ctx;

use crate::domain::model::Message;
use crate::domain::service::message_operation_service::MessageRepository;
use crate::error::Result;

use super::message_repository_adapter::StorageReaderMessageRepository;
use super::noop_message_repository::NoopMessageRepository;

/// 消息仓储变体：有 Reader 走 gRPC，否则 Noop。
pub enum MessageRepositoryKind {
    Storage(StorageReaderMessageRepository),
    Noop(NoopMessageRepository),
}

impl MessageRepository for MessageRepositoryKind {
    async fn find_by_id(&self, ctx: &Ctx, message_id: &str) -> Result<Option<Message>> {
        match self {
            Self::Storage(r) => r.find_by_id(ctx, message_id).await,
            Self::Noop(r) => r.find_by_id(ctx, message_id).await,
        }
    }

    async fn save(&self, ctx: &Ctx, message: &Message) -> Result<()> {
        match self {
            Self::Storage(r) => r.save(ctx, message).await,
            Self::Noop(r) => r.save(ctx, message).await,
        }
    }
}
