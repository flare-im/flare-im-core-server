//! 删除事件应用（硬删 / 软删可见性）

use crate::domain::model::{DeletePayload, Event};
use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository};
use anyhow::Result;

use super::{EventContext, append_event_and_stream};

// 与 proto DeleteType 对齐：SOFT=1, HARD=2
const DELETE_TYPE_HARD: i32 = 2;

pub async fn apply_delete<A, E>(
    ctx: &EventContext<'_, A, E>,
    event: &Event,
    delete: &DeletePayload,
) -> Result<()>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    let message_id = delete.server_msg_id.as_str();
    let user_id = event.operator_id.as_str();
    match delete.delete_type.unwrap_or(0) {
        DELETE_TYPE_HARD => {
            ctx.repo
                .update_message_fsm_state(ctx.ctx, ctx.tenant_id, message_id, "DELETED_HARD", None)
                .await?;
        }
        _ => {
            ctx.repo
                .update_message_visibility(ctx.ctx, ctx.tenant_id, message_id, user_id, "HIDDEN")
                .await?;
        }
    }
    append_event_and_stream(ctx, message_id, event).await
}
