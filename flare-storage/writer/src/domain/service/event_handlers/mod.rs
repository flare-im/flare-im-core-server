//! 领域事件 Payload 处理器（策略式分发）- 使用领域 Event，不依赖 proto

use flare_im_contracts::Ctx;
use flare_server_core::error::{ErrorCode, FlareError, Result, map_infra_error};

use crate::domain::model::{Event, EventPayload};
use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository};

mod burn;
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

/// 写事件流失败的分类。预检时该 seq 还空着、另一个事件却抢先写入了同一 seq：事件流报不可重试的
/// 冲突（`HttpConflict`），原样透传——与预检发现的冲突一样直接进死信（`is_operation_conflict`），
/// 不再包成可重试的数据库错误、白白重投一次后才由预检送进死信。其余失败仍是数据库错误。
pub(crate) fn stream_append_failure(event: &Event, error: FlareError) -> FlareError {
    if error.code() == Some(ErrorCode::HttpConflict) {
        tracing::warn!(
            tenant_id = %event.tenant_id,
            conversation_id = %event.conversation_id,
            seq = event.seq,
            new_type = ?event.r#type,
            "operation event lost its seq to another event written after the pre-check; not recorded in the event stream"
        );
        return error;
    }
    map_infra_error(
        error,
        ErrorCode::DatabaseError,
        "Failed to append operation event to durable event stream",
    )
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
    ctx.repo
        .append_event(ctx.ctx, ctx.tenant_id, message_id, event)
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Database operation failed"))?;
    if let Some(stream) = ctx.stream {
        stream
            .append_event_to_stream(ctx.ctx, event)
            .await
            .map_err(|e| stream_append_failure(event, e))?;
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
            EventPayload::BurnScheduled(b) => Some(b.message_id.clone()),
            EventPayload::Burned(b) => Some(b.message_id.clone()),
            EventPayload::HardDeleted(b) => Some(b.message_id.clone()),
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
        Some(EventPayload::BurnScheduled(p)) => burn::apply_burn_scheduled(ctx, event, p).await,
        Some(EventPayload::Burned(p)) => burn::apply_burned(ctx, event, p).await,
        Some(EventPayload::HardDeleted(p)) => burn::apply_hard_deleted(ctx, event, p).await,
        _ => {
            tracing::trace!(r#type = ?event.r#type, "Unsupported or non-operation event, skip");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::EventType;
    use flare_im_contracts::utils::Context;
    use flare_server_core::error::Result as AnyhowResult;
    use std::sync::Arc;

    struct NoopArchiveRepository;

    impl ArchiveStoreRepository for NoopArchiveRepository {
        async fn store_archive(
            &self,
            _ctx: &Ctx,
            _message: &crate::domain::model::Message,
        ) -> AnyhowResult<()> {
            Ok(())
        }

        async fn append_event(
            &self,
            _ctx: &Ctx,
            _tenant_id: &str,
            _message_id: &str,
            _event: &Event,
        ) -> AnyhowResult<()> {
            Ok(())
        }
    }

    struct FailingEventStreamRepository;

    impl EventStreamRepository for FailingEventStreamRepository {
        async fn append_event_to_stream(&self, _ctx: &Ctx, _event: &Event) -> AnyhowResult<()> {
            Err(flare_server_core::error::FlareError::system(
                "operation event stream unavailable".to_string(),
            ))
        }

        async fn match_stored_event(
            &self,
            _ctx: &Ctx,
            _event: &crate::domain::model::Event,
        ) -> AnyhowResult<crate::domain::repository::StoredEventMatch> {
            Ok(crate::domain::repository::StoredEventMatch::Absent)
        }
    }

    /// 预检之后另一个事件抢先写入了同一 seq：事件流报冲突。
    struct ConflictingEventStreamRepository;

    impl EventStreamRepository for ConflictingEventStreamRepository {
        async fn append_event_to_stream(&self, _ctx: &Ctx, _event: &Event) -> AnyhowResult<()> {
            Err(flare_server_core::flare_err!(
                ErrorCode::HttpConflict,
                "event stream conflict tenant_id=tenant-a conversation_id=conversation-a seq=2 existing_type=8 new_type=2"
            ))
        }

        async fn match_stored_event(
            &self,
            _ctx: &Ctx,
            _event: &crate::domain::model::Event,
        ) -> AnyhowResult<crate::domain::repository::StoredEventMatch> {
            Ok(crate::domain::repository::StoredEventMatch::Absent)
        }
    }

    fn test_ctx() -> Ctx {
        Arc::new(Context::with_request_id("req-operation-stream-test").with_tenant_id("tenant-a"))
    }

    fn operation_event() -> Event {
        Event {
            tenant_id: "tenant-a".to_string(),
            conversation_id: "conversation-a".to_string(),
            seq: 2,
            r#type: EventType::MessageRecall,
            created_at: None,
            operator_id: "operator-a".to_string(),
            event_seq: None,
            request_id: None,
            payload: None,
        }
    }

    #[tokio::test]
    async fn append_event_and_stream_returns_error_when_event_stream_append_fails() {
        let repo = NoopArchiveRepository;
        let stream = FailingEventStreamRepository;
        let ctx = test_ctx();
        let event_ctx = EventContext {
            repo: &repo,
            stream: Some(&stream),
            ctx: &ctx,
            tenant_id: "tenant-a",
            conversation_id: "conversation-a",
        };

        let err = append_event_and_stream(&event_ctx, "message-a", &operation_event())
            .await
            .expect_err("operation event stream failure must fail the durable write path");

        assert!(
            err.to_string()
                .contains("Failed to append operation event to durable event stream"),
            "unexpected error: {err}"
        );
        assert_eq!(err.code(), Some(ErrorCode::DatabaseError), "{err}");
    }

    /// 写事件流时撞上同一 seq 上的另一个事件（预检之后被抢先写入）：冲突分类原样透传，消费者据此
    /// 直接进死信（W14-R (d)）。修复前被包成 `DatabaseError`，重投一次后才由预检送进死信。
    #[tokio::test]
    async fn a_seq_taken_after_the_pre_check_is_reported_as_a_conflict() {
        let repo = NoopArchiveRepository;
        let stream = ConflictingEventStreamRepository;
        let ctx = test_ctx();
        let event_ctx = EventContext {
            repo: &repo,
            stream: Some(&stream),
            ctx: &ctx,
            tenant_id: "tenant-a",
            conversation_id: "conversation-a",
        };

        let err = append_event_and_stream(&event_ctx, "message-a", &operation_event())
            .await
            .expect_err("another event holds the seq");

        assert_eq!(err.code(), Some(ErrorCode::HttpConflict), "{err}");
        assert!(!err.is_retryable(), "{err}");
    }
}
