//! 无 Storage Reader 时的消息仓储占位实现（操作类命令在 WAL 未命中时视为无消息）。

use flare_server_core::context::Ctx;

use crate::domain::model::Message;
use crate::domain::service::message_operation_service::MessageRepository;
use crate::error::Result;

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMessageRepository;

impl MessageRepository for NoopMessageRepository {
    async fn find_by_id(&self, _ctx: &Ctx, _message_id: &str) -> Result<Option<Message>> {
        Ok(None)
    }

    async fn save(&self, _ctx: &Ctx, _message: &Message) -> Result<()> {
        Ok(())
    }
}
