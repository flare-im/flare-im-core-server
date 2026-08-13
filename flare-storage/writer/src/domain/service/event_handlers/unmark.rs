//! 取消标记事件应用

use crate::domain::model::{Event, UnmarkPayload};
use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository};
use flare_server_core::error::{ErrorCode, Result, map_infra_error};

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
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::DatabaseError, "Database operation failed")
            })?;
    } else {
        // 未指定类型 = 清掉全部标记。逐个失败不该让其余几种也取消不掉，所以这里
        // 不用 ? 中断；但静默会让用户看到标记还在、又查不出任何原因——逐个记下来。
        for mt in ["IMPORTANT", "TODO", "DONE", "CUSTOM"] {
            if let Err(err) = ctx
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
                .await
            {
                tracing::warn!(
                    tenant_id = %ctx.tenant_id, %message_id, mark_type = %mt,
                    user_id = %unmark.user_id, error = %err,
                    "取消标记失败：该类型的标记仍会显示在客户端"
                );
            }
        }
    }
    append_event_and_stream(ctx, message_id, event).await
}
