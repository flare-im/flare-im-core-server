//! 编辑事件应用

use crate::convert;
use crate::domain::model::{EditPayload, Event};
use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository};
use flare_server_core::error::{ErrorCode, Result, map_infra_error};

use super::{EventContext, append_event_and_stream};

pub async fn apply_edit<A, E>(
    ctx: &EventContext<'_, A, E>,
    event: &Event,
    edit: &EditPayload,
) -> Result<()>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    let message_id = edit.server_msg_id.as_str();
    let content_text_opt = convert::content_bytes_to_text(&edit.new_content);
    let content_text = content_text_opt.as_deref();
    ctx.repo
        .update_message_content(
            ctx.ctx,
            ctx.tenant_id,
            message_id,
            &edit.new_content,
            event.operator_id.as_str(),
            (!edit.reason.is_empty()).then_some(edit.reason.as_str()),
            content_text,
        )
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Database operation failed"))?;
    append_event_and_stream(ctx, message_id, event).await
}
