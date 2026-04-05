//! 已读回执事件应用

use crate::domain::model::{Event, ReadPayload};
use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository};
use flare_im_core::error::{ErrorCode, Result, map_infra_error};

use super::EventContext;

pub async fn apply_read<A, E>(
    ctx: &EventContext<'_, A, E>,
    event: &Event,
    read: &ReadPayload,
) -> Result<()>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    for msg_id in &read.message_ids {
        ctx.repo
            .record_message_read(
                ctx.ctx,
                ctx.tenant_id,
                msg_id.as_str(),
                read.user_id.as_str(),
            )
            .await.map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Database operation failed"))?;
        ctx.repo
            .append_event(ctx.ctx, ctx.tenant_id, msg_id.as_str(), event)
            .await.map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Database operation failed"))?;
    }
    if let Some(stream) = ctx.stream {
        let _ = stream.append_event_to_stream(ctx.ctx, event).await;
    }
    Ok(())
}
