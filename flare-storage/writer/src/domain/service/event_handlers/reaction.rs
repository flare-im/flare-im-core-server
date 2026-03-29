//! 反应事件应用

use crate::domain::model::{Event, ReactionPayload};
use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository};
use anyhow::Result;

use super::{EventContext, append_event_and_stream};

// 与 proto ReactionAction 对齐：ADD=1
const REACTION_ACTION_ADD: i32 = 1;

pub async fn apply_reaction<A, E>(
    ctx: &EventContext<'_, A, E>,
    event: &Event,
    reaction: &ReactionPayload,
) -> Result<()>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    let message_id = reaction.server_msg_id.as_str();
    let add = reaction.action == REACTION_ACTION_ADD;
    ctx.repo
        .upsert_message_reaction(
            ctx.ctx,
            ctx.tenant_id,
            message_id,
            reaction.emoji.as_str(),
            reaction.user_id.as_str(),
            add,
        )
        .await?;
    append_event_and_stream(ctx, message_id, event).await
}
