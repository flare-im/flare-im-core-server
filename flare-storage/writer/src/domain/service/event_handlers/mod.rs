//! 领域事件 Payload 处理器（策略式分发）- 使用领域 Event，不依赖 proto

use anyhow::Result;
use flare_server_core::context::Ctx;

use crate::domain::model::{Event, EventPayload};
use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository};

mod delete;
mod edit;
mod mark;
mod pin;
mod reaction;
mod read;
mod recall;
mod unmark;
mod unpin;

/// 事件应用上下文（只读依赖 + 租户/会话标识）
pub struct EventContext<'a, A, E>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    pub repo: &'a A,
    pub stream: Option<&'a E>,
    pub ctx: &'a Ctx,
    pub tenant_id: &'a str,
    pub conversation_id: &'a str,
}

/// 追加事件到操作历史并写入事件流（供 Sync）；供各 handler 复用
pub async fn append_event_and_stream<A, E>(
    ctx: &EventContext<'_, A, E>,
    message_id: &str,
    event: &Event,
) -> Result<()>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    ctx.repo.append_event(ctx.ctx, ctx.tenant_id, message_id, event).await?;
    if let Some(stream) = ctx.stream {
        let _ = stream.append_event_to_stream(ctx.ctx, event).await;
    }
    Ok(())
}

/// 从事件 Payload 提取“主”消息 ID（用于指标/日志；Read 取第一条）
pub fn primary_message_id_for_metrics(event: &Event) -> String {
    event
        .payload
        .as_ref()
        .and_then(|p| match p {
            EventPayload::Recall(r) => Some(r.server_msg_id.clone()),
            EventPayload::Edit(e) => Some(e.server_msg_id.clone()),
            EventPayload::Delete(d) => Some(d.server_msg_id.clone()),
            EventPayload::Pin(p) => Some(p.server_msg_id.clone()),
            EventPayload::Unpin(u) => Some(u.server_msg_id.clone()),
            EventPayload::Mark(m) => Some(m.server_msg_id.clone()),
            EventPayload::Unmark(u) => Some(u.server_msg_id.clone()),
            EventPayload::Reaction(r) => Some(r.server_msg_id.clone()),
            EventPayload::Read(r) => r.message_ids.first().cloned(),
            _ => None,
        })
        .unwrap_or_default()
}

/// 根据 Payload 类型分发到对应 apply_* 处理器
pub async fn dispatch<A, E>(ctx: &EventContext<'_, A, E>, event: &Event) -> Result<()>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    match event.payload.as_ref() {
        Some(EventPayload::Recall(p)) => recall::apply_recall(ctx, event, p).await,
        Some(EventPayload::Edit(p)) => edit::apply_edit(ctx, event, p).await,
        Some(EventPayload::Delete(p)) => delete::apply_delete(ctx, event, p).await,
        Some(EventPayload::Read(p)) => read::apply_read(ctx, event, p).await,
        Some(EventPayload::Reaction(p)) => reaction::apply_reaction(ctx, event, p).await,
        Some(EventPayload::Pin(p)) => pin::apply_pin(ctx, event, p).await,
        Some(EventPayload::Unpin(p)) => unpin::apply_unpin(ctx, event, p).await,
        Some(EventPayload::Mark(p)) => mark::apply_mark(ctx, event, p).await,
        Some(EventPayload::Unmark(p)) => unmark::apply_unmark(ctx, event, p).await,
        _ => {
            tracing::debug!(r#type = ?event.r#type, "Unsupported or non-operation event, skip");
            Ok(())
        }
    }
}
