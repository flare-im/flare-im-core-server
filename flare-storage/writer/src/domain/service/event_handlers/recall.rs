//! 撤回事件应用

use crate::domain::model::{Event, RecallPayload};
use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository};
use flare_im_core::error::{ErrorCode, Result, map_infra_error};

use super::{EventContext, append_event_and_stream};

pub async fn apply_recall<A, E>(
    ctx: &EventContext<'_, A, E>,
    event: &Event,
    recall: &RecallPayload,
) -> Result<()>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    let message_id = recall.server_msg_id.as_str();
    let reason = (!recall.reason.is_empty()).then_some(recall.reason.as_str());
    ctx.repo
        .update_message_fsm_state(ctx.ctx, ctx.tenant_id, message_id, "RECALLED", reason)
        .await.map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Database operation failed"))?;
    append_event_and_stream(ctx, message_id, event).await
}
