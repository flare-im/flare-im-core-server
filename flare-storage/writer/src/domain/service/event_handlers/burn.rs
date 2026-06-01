//! 阅后即焚事件应用。

use crate::domain::model::{BurnScheduledPayload, BurnedPayload, Event, HardDeletedPayload};
use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository};
use flare_im_core::error::{ErrorCode, Result, map_infra_error};

use super::{EventContext, append_event_and_stream};

pub async fn apply_burn_scheduled<A, E>(
    ctx: &EventContext<'_, A, E>,
    event: &Event,
    burn: &BurnScheduledPayload,
) -> Result<()>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    let changed = ctx
        .repo
        .schedule_message_burn(
            ctx.ctx,
            ctx.tenant_id,
            burn.message_id.as_str(),
            burn.reader_id.as_deref(),
            burn.event_time,
            burn.burn_at,
        )
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Database operation failed"))?;

    if changed {
        append_event_and_stream(ctx, burn.message_id.as_str(), event).await?;
    } else {
        tracing::debug!(
            tenant_id = %ctx.tenant_id,
            message_id = %burn.message_id,
            "Burn schedule was already applied; skipping duplicate event append"
        );
    }
    Ok(())
}

pub async fn apply_burned<A, E>(
    ctx: &EventContext<'_, A, E>,
    event: &Event,
    burned: &BurnedPayload,
) -> Result<()>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    let changed = ctx
        .repo
        .mark_message_burned(
            ctx.ctx,
            ctx.tenant_id,
            burned.message_id.as_str(),
            burned.burned_at,
        )
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Database operation failed"))?;

    if changed {
        append_event_and_stream(ctx, burned.message_id.as_str(), event).await?;
    } else {
        tracing::debug!(
            tenant_id = %ctx.tenant_id,
            message_id = %burned.message_id,
            "Burn was already applied; skipping duplicate event append"
        );
    }
    Ok(())
}

pub async fn apply_hard_deleted<A, E>(
    ctx: &EventContext<'_, A, E>,
    event: &Event,
    hard_deleted: &HardDeletedPayload,
) -> Result<()>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    let changed = ctx
        .repo
        .mark_message_hard_deleted(
            ctx.ctx,
            ctx.tenant_id,
            hard_deleted.message_id.as_str(),
            hard_deleted.event_time,
        )
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Database operation failed"))?;

    if changed {
        append_event_and_stream(ctx, hard_deleted.message_id.as_str(), event).await?;
    } else {
        tracing::debug!(
            tenant_id = %ctx.tenant_id,
            message_id = %hard_deleted.message_id,
            "Hard delete was already applied; skipping duplicate event append"
        );
    }
    Ok(())
}
