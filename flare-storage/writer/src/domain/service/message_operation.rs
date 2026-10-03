//! 领域事件应用服务：将 Event（撤回/编辑/删除/已读/反应/置顶/标记）应用到存储
//! 与 common/event.proto 对齐，通过 ArchiveStoreRepository 更新写模型与旁路表。
//! 具体 Payload 处理委托给 [event_handlers] 策略分发。

use std::sync::Arc;

use crate::domain::model::Event;
use flare_im_contracts::Ctx;
use flare_server_core::error::{ErrorCode, Result, map_infra_error};
use flare_server_core::flare_err;
use tracing::instrument;

use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository, StoredEventMatch};
use crate::domain::service::event_handlers::{EventContext, dispatch};

/// 领域事件应用服务
pub struct EventApplicationService<A, E>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    archive_repo: Option<Arc<A>>,
    event_stream_repo: Option<Arc<E>>,
}

impl<A, E> EventApplicationService<A, E>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    pub fn new(archive_repo: Option<Arc<A>>, event_stream_repo: Option<Arc<E>>) -> Self {
        Self {
            archive_repo,
            event_stream_repo,
        }
    }

    #[instrument(skip(self), fields(r#type = ?event.r#type))]
    pub async fn process_event(&self, ctx: &Ctx, event: &Event) -> Result<()> {
        let repo = self.archive_repo.as_ref().ok_or_else(|| {
            flare_err!(
                ErrorCode::InternalError,
                "Archive repository not configured"
            )
        })?;

        let conversation_id = event.conversation_id.as_str();
        let tenant_id = event.tenant_id.as_str();
        if tenant_id.is_empty() {
            return Err(flare_err!(
                ErrorCode::InvalidParameter,
                "Event.tenant_id is required"
            ));
        }

        let stream = self.event_stream_repo.as_ref().ok_or_else(|| {
            flare_err!(
                ErrorCode::InternalError,
                "Event stream repository not configured"
            )
        })?;

        // 幂等：events 表是该事件已应用完的标记（各 handler 最后才写它）。同一位置上已有同一个事件
        // （重投）就跳过；已有的是另一个事件时不应用，报不可重试的冲突——重试不会改变已有行，
        // 原先只按键查、不比内容，这种事件会被静默吞掉。
        let seq = event.seq as i64;
        match stream.match_stored_event(ctx, event).await.map_err(|e| {
            map_infra_error(e, ErrorCode::DatabaseError, "Failed to check stored event")
        })? {
            StoredEventMatch::Absent => {}
            StoredEventMatch::Duplicate => {
                tracing::debug!(
                    tenant_id = %tenant_id,
                    conversation_id = %conversation_id,
                    seq = seq,
                    "Event already applied, skipping (idempotent)"
                );
                return Ok(());
            }
            StoredEventMatch::Conflict {
                existing_event_type,
            } => {
                tracing::warn!(
                    tenant_id = %tenant_id,
                    conversation_id = %conversation_id,
                    seq = seq,
                    existing_type = existing_event_type,
                    new_type = ?event.r#type,
                    "operation event conflicts with another event at the same seq; not applied"
                );
                return Err(flare_err!(
                    ErrorCode::HttpConflict,
                    format!(
                        "operation event conflict tenant_id={tenant_id} conversation_id={conversation_id} seq={seq} existing_type={existing_event_type}"
                    )
                ));
            }
        }

        let event_ctx = EventContext {
            repo: repo.as_ref(),
            stream: Some(stream.as_ref()),
            ctx,
            tenant_id,
            conversation_id,
        };
        dispatch(&event_ctx, event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::{EventPayload, EventType, RecallPayload};
    use flare_im_contracts::utils::Context;
    use flare_server_core::error::Result as AnyhowResult;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct NoopArchiveRepository;

    impl ArchiveStoreRepository for NoopArchiveRepository {
        async fn store_archive(
            &self,
            _ctx: &Ctx,
            _message: &crate::domain::model::Message,
        ) -> AnyhowResult<()> {
            Ok(())
        }
    }

    struct NoopEventStreamRepository;

    impl EventStreamRepository for NoopEventStreamRepository {
        async fn append_event_to_stream(&self, _ctx: &Ctx, _event: &Event) -> AnyhowResult<()> {
            Ok(())
        }

        async fn match_stored_event(
            &self,
            _ctx: &Ctx,
            _event: &Event,
        ) -> AnyhowResult<StoredEventMatch> {
            Ok(StoredEventMatch::Absent)
        }
    }

    /// 记下状态更新、操作历史与事件流写入次数的仓储。
    #[derive(Default)]
    struct Recorded {
        status_updates: AtomicUsize,
        history_rows: AtomicUsize,
        stream_rows: AtomicUsize,
    }

    struct RecordingArchiveRepository(Arc<Recorded>);

    impl ArchiveStoreRepository for RecordingArchiveRepository {
        async fn store_archive(
            &self,
            _ctx: &Ctx,
            _message: &crate::domain::model::Message,
        ) -> AnyhowResult<()> {
            Ok(())
        }

        async fn update_message_fsm_state(
            &self,
            _ctx: &Ctx,
            _tenant_id: &str,
            _message_id: &str,
            _fsm_state: &str,
            _recall_reason: Option<&str>,
        ) -> AnyhowResult<()> {
            self.0.status_updates.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn append_event(
            &self,
            _ctx: &Ctx,
            _tenant_id: &str,
            _message_id: &str,
            _event: &Event,
        ) -> AnyhowResult<()> {
            self.0.history_rows.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// 事件流里该位置上的判重结果固定为 `stored`。
    struct StoredEventStreamRepository {
        stored: StoredEventMatch,
        recorded: Arc<Recorded>,
    }

    impl EventStreamRepository for StoredEventStreamRepository {
        async fn append_event_to_stream(&self, _ctx: &Ctx, _event: &Event) -> AnyhowResult<()> {
            self.recorded.stream_rows.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn match_stored_event(
            &self,
            _ctx: &Ctx,
            _event: &Event,
        ) -> AnyhowResult<StoredEventMatch> {
            Ok(self.stored)
        }
    }

    fn service_with_stored(
        stored: StoredEventMatch,
    ) -> (
        EventApplicationService<RecordingArchiveRepository, StoredEventStreamRepository>,
        Arc<Recorded>,
    ) {
        let recorded = Arc::new(Recorded::default());
        let service = EventApplicationService::new(
            Some(Arc::new(RecordingArchiveRepository(recorded.clone()))),
            Some(Arc::new(StoredEventStreamRepository {
                stored,
                recorded: recorded.clone(),
            })),
        );
        (service, recorded)
    }

    fn counts(recorded: &Recorded) -> (usize, usize, usize) {
        (
            recorded.status_updates.load(Ordering::SeqCst),
            recorded.history_rows.load(Ordering::SeqCst),
            recorded.stream_rows.load(Ordering::SeqCst),
        )
    }

    fn test_ctx() -> Ctx {
        Arc::new(
            Context::with_request_id("req-operation-missing-stream").with_tenant_id("tenant-a"),
        )
    }

    fn recall_event() -> Event {
        Event {
            tenant_id: "tenant-a".to_string(),
            conversation_id: "conversation-a".to_string(),
            seq: 3,
            r#type: EventType::MessageRecall,
            created_at: None,
            operator_id: "operator-a".to_string(),
            event_seq: None,
            request_id: None,
            payload: Some(EventPayload::Recall(RecallPayload {
                server_msg_id: "message-a".to_string(),
                reason: "test".to_string(),
                time_limit_seconds: None,
                allow_admin_recall: None,
            })),
        }
    }

    #[tokio::test]
    async fn process_event_returns_error_when_event_stream_repository_is_missing() {
        let service: EventApplicationService<NoopArchiveRepository, NoopEventStreamRepository> =
            EventApplicationService::new(Some(Arc::new(NoopArchiveRepository)), None);

        let err = service
            .process_event(&test_ctx(), &recall_event())
            .await
            .expect_err("operation event stream must be required");

        assert!(
            err.to_string()
                .contains("Event stream repository not configured"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn process_event_applies_an_event_whose_seq_is_free() {
        let (service, recorded) = service_with_stored(StoredEventMatch::Absent);
        service
            .process_event(&test_ctx(), &recall_event())
            .await
            .expect("a new operation event is applied");
        assert_eq!(counts(&recorded), (1, 1, 1));
    }

    #[tokio::test]
    async fn process_event_skips_a_redelivered_event() {
        let (service, recorded) = service_with_stored(StoredEventMatch::Duplicate);
        service
            .process_event(&test_ctx(), &recall_event())
            .await
            .expect("a redelivery of the stored event is acknowledged");
        assert_eq!(counts(&recorded), (0, 0, 0));
    }

    /// 同一位置上已有另一个事件（W13-R1）：修复前按键查到就跳过、返回成功，这条操作被静默吞掉；
    /// 现在不应用，报不可重试的冲突。
    #[tokio::test]
    async fn process_event_rejects_another_event_at_the_same_seq_as_a_non_retryable_conflict() {
        let (service, recorded) = service_with_stored(StoredEventMatch::Conflict {
            existing_event_type: 8,
        });
        let err = service
            .process_event(&test_ctx(), &recall_event())
            .await
            .expect_err("another event at the same seq must not be acknowledged");
        assert_eq!(err.code(), Some(ErrorCode::HttpConflict), "{err}");
        assert!(!err.is_retryable(), "{err}");
        assert_eq!(counts(&recorded), (0, 0, 0));
    }
}
