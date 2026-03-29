//! 取消置顶事件应用

use crate::domain::model::{Event, UnpinPayload};
use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository};
use anyhow::Result;

use super::{EventContext, append_event_and_stream};

pub async fn apply_unpin<A, E>(
    ctx: &EventContext<'_, A, E>,
    event: &Event,
    unpin: &UnpinPayload,
) -> Result<()>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    let message_id = unpin.server_msg_id.as_str();
    ctx.repo
        .pin_message(
            ctx.ctx,
            ctx.tenant_id,
            message_id,
            ctx.conversation_id,
            event.operator_id.as_str(),
            false,
            None,
            None,
        )
        .await?;
    append_event_and_stream(ctx, message_id, event).await
}
