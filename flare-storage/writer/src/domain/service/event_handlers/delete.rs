//! 删除事件应用（硬删 / 软删可见性）

use crate::domain::model::{DeletePayload, Event};
use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository};
use flare_im_core::error::{ErrorCode, Result, map_infra_error};

use super::{EventContext, append_event_and_stream};

// 与 proto DeleteType 对齐：SOFT=1, HARD=2
const DELETE_TYPE_HARD: i32 = 2;
// 与 proto DeleteScope 对齐：1=UserPrivate, 2=ConversationGlobal
const DELETE_SCOPE_USER_PRIVATE: i32 = 1;
const DELETE_SCOPE_CONVERSATION_GLOBAL: i32 = 2;
// 全局删除不绑定用户，落库 user_id 为空串，查询按 scope 解释。
const GLOBAL_SCOPE_USER_ID: &str = "";

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
    match delete.delete_type.unwrap_or(0) {
        DELETE_TYPE_HARD => {
            ctx.repo
                .update_message_fsm_state(ctx.ctx, ctx.tenant_id, message_id, "DELETED_HARD", None)
                .await.map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Database operation failed"))?;
        }
        _ => {
            let scope = delete.scope.unwrap_or(DELETE_SCOPE_USER_PRIVATE);
            let visibility_user_id = if scope == DELETE_SCOPE_CONVERSATION_GLOBAL {
                GLOBAL_SCOPE_USER_ID
            } else {
                delete
                    .target_user_id
                    .as_deref()
                    .filter(|id| !id.is_empty())
                    .unwrap_or(event.operator_id.as_str())
            };
            ctx.repo
                .update_message_visibility(
                    ctx.ctx,
                    ctx.tenant_id,
                    message_id,
                    visibility_user_id,
                    scope,
                    "HIDDEN",
                )
                .await.map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Database operation failed"))?;
        }
    }
    append_event_and_stream(ctx, message_id, event).await
}
