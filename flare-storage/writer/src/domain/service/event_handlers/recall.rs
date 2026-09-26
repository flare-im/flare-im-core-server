//! 撤回事件应用

use crate::domain::model::{Event, RecallPayload};
use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository};
use flare_server_core::error::{ErrorCode, Result, map_infra_error};

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
    // 没有 server_msg_id 的撤回(消息根本没落库:钩子拒发、发送失败)无事可做;
    // 空 id 传下去曾让状态更新命中全部历史行。直接 ack,别让 MQ 重投。
    if message_id.trim().is_empty() {
        tracing::warn!(tenant_id = ctx.tenant_id, event_id = %event.event_id, "recall without server_msg_id ignored");
        return Ok(());
    }
    let reason = (!recall.reason.is_empty()).then_some(recall.reason.as_str());
    ctx.repo
        .update_message_fsm_state(ctx.ctx, ctx.tenant_id, message_id, "RECALLED", reason)
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Database operation failed"))?;
    append_event_and_stream(ctx, message_id, event).await
}
