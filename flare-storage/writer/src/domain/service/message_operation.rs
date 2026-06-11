//! 领域事件应用服务：将 Event（撤回/编辑/删除/已读/反应/置顶/标记）应用到存储
//! 与 common/event.proto 对齐，通过 ArchiveStoreRepository 更新写模型与旁路表。
//! 具体 Payload 处理委托给 [event_handlers] 策略分发。

use std::sync::Arc;

use crate::domain::model::Event;
use flare_im_contracts::Ctx;
use flare_server_core::error::{ErrorCode, Result, map_infra_error};
use flare_server_core::flare_err;
use tracing::instrument;

use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository};
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

        // 撤回/编辑一致性：幂等——若该事件已写入 events 表则跳过应用，避免重复更新
        let seq = event.seq as i64;
        if stream
            .event_exists(ctx, tenant_id, conversation_id, seq)
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::DatabaseError, "Failed to check event exists")
            })?
        {
            tracing::trace!(
                tenant_id = %tenant_id,
                conversation_id = %conversation_id,
                seq = seq,
                "Event already applied, skipping (idempotent)"
            );
            return Ok(());
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

        async fn event_exists(
            &self,
            _ctx: &Ctx,
            _tenant_id: &str,
            _conversation_id: &str,
            _seq: i64,
        ) -> AnyhowResult<bool> {
            Ok(false)
        }
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
}
