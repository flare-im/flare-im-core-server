//! 取消标记事件应用

use crate::domain::model::{Event, UnmarkPayload};
use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository};
use anyhow::Result;

use super::{EventContext, append_event_and_stream};

const MARK_IMPORTANT: i32 = 1;
const MARK_TODO: i32 = 2;
const MARK_DONE: i32 = 3;
const MARK_CUSTOM: i32 = 4;

pub async fn apply_unmark<A, E>(
    ctx: &EventContext<'_, A, E>,
    event: &Event,
    unmark: &UnmarkPayload,
) -> Result<()>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    let message_id = unmark.server_msg_id.as_str();
    let mark_type = match unmark.mark_type {
        MARK_IMPORTANT => Some("IMPORTANT"),
        MARK_TODO => Some("TODO"),
        MARK_DONE => Some("DONE"),
        MARK_CUSTOM => Some("CUSTOM"),
        _ => None,
    };
    if let Some(mt) = mark_type {
        ctx.repo
            .mark_message(
                ctx.ctx,
                ctx.tenant_id,
                message_id,
                ctx.conversation_id,
                unmark.user_id.as_str(),
                mt,
                None,
                false,
            )
            .await?;
    } else {
        for mt in ["IMPORTANT", "TODO", "DONE", "CUSTOM"] {
            let _ = ctx
                .repo
                .mark_message(
                    ctx.ctx,
                    ctx.tenant_id,
                    message_id,
                    ctx.conversation_id,
                    unmark.user_id.as_str(),
                    mt,
                    None,
                    false,
                )
                .await;
        }
    }
    append_event_and_stream(ctx, message_id, event).await
}
