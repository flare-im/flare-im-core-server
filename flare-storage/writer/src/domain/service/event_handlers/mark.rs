//! 标记事件应用

use crate::domain::model::{Event, MarkPayload};
use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository};
use flare_im_core::error::{ErrorCode, Result, map_infra_error};
use flare_server_core::flare_err;

use super::{EventContext, append_event_and_stream};

// 与 proto MarkType 对齐
const MARK_IMPORTANT: i32 = 1;
const MARK_TODO: i32 = 2;
const MARK_DONE: i32 = 3;
const MARK_CUSTOM: i32 = 4;

pub async fn apply_mark<A, E>(
    ctx: &EventContext<'_, A, E>,
    event: &Event,
    mark: &MarkPayload,
) -> Result<()>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    let message_id = mark.server_msg_id.as_str();
    let mark_type = match mark.mark_type {
        MARK_IMPORTANT => "IMPORTANT",
        MARK_TODO => "TODO",
        MARK_DONE => "DONE",
        MARK_CUSTOM => "CUSTOM",
        _ => return Err(flare_err!(ErrorCode::InvalidParameter, "Invalid mark_type")),
    };
    let color = (!mark.color.is_empty()).then_some(mark.color.as_str());
    ctx.repo
        .mark_message(
            ctx.ctx,
            ctx.tenant_id,
            message_id,
            ctx.conversation_id,
            mark.user_id.as_str(),
            mark_type,
            color,
            true,
        )
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Database operation failed"))?;
    append_event_and_stream(ctx, message_id, event).await
}
