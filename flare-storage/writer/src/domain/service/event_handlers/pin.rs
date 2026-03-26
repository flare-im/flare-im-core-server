//! 置顶事件应用

use anyhow::Result;
use crate::domain::model::{Event, PinPayload};
use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository};

use super::{append_event_and_stream, EventContext};

pub async fn apply_pin<A, E>(
    ctx: &EventContext<'_, A, E>,
    event: &Event,
    pin: &PinPayload,
) -> Result<()>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    let message_id = pin.server_msg_id.as_str();
    let expire_at = pin
        .expire_at
        .as_ref()
        .and_then(flare_im_core::utils::timestamp_to_datetime);
    ctx.repo
        .pin_message(
            ctx.ctx,
            ctx.tenant_id,
            message_id,
            ctx.conversation_id,
            pin.pinned_by.as_str(),
            true,
            expire_at,
            pin.reason.as_deref().filter(|s| !s.is_empty()),
        )
        .await?;
    append_event_and_stream(ctx, message_id, event).await
}
