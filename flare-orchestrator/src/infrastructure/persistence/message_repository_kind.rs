//! 将 Storage Reader 与 Noop 仓储合并为单一具体类型，使 [MessageOperationService] 在 wire 中单态化。

use flare_server_core::context::Ctx;

use crate::domain::model::Message;
use crate::domain::service::message_operation_service::{
    ConversationServerIdsPage, MessageRepository,
};
use crate::error::Result;

use super::message_repository_adapter::StorageReaderMessageRepository;

/// 无 Storage Reader 时的消息仓储占位（操作类在 WAL 未命中时视为无消息）。
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMessageRepository;

impl MessageRepository for NoopMessageRepository {
    async fn find_by_id(&self, _ctx: &Ctx, _message_id: &str) -> Result<Option<Message>> {
        Ok(None)
    }

    async fn save(&self, _ctx: &Ctx, _message: &Message) -> Result<()> {
        Ok(())
    }

    async fn page_server_ids_in_conversation(
        &self,
        _ctx: &Ctx,
        _conversation_id: &str,
        _limit: i32,
        _cursor: Option<&str>,
    ) -> Result<ConversationServerIdsPage> {
        Ok(ConversationServerIdsPage {
            server_ids: Vec::new(),
            next_cursor: String::new(),
            has_more: false,
        })
    }
}

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

    async fn page_server_ids_in_conversation(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        limit: i32,
        cursor: Option<&str>,
    ) -> Result<ConversationServerIdsPage> {
        match self {
            Self::Storage(r) => {
                r.page_server_ids_in_conversation(ctx, conversation_id, limit, cursor)
                    .await
            }
            Self::Noop(r) => {
                r.page_server_ids_in_conversation(ctx, conversation_id, limit, cursor)
                    .await
            }
        }
    }
}
