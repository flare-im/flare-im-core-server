//! 已读回执事件应用

use crate::domain::model::{Event, ReadPayload};
use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository};
use flare_server_core::error::{ErrorCode, Result, map_infra_error};

use super::EventContext;

pub async fn apply_read<A, E>(
    ctx: &EventContext<'_, A, E>,
    event: &Event,
    read: &ReadPayload,
) -> Result<()>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    let first_read_at = read
        .read_at
        .as_ref()
        .map(|ts| ts.seconds)
        .or_else(|| event.created_at.as_ref().map(|ts| ts.seconds))
        .unwrap_or_else(|| chrono::Utc::now().timestamp());
    for msg_id in &read.message_ids {
        ctx.repo
            .record_message_read(
                ctx.ctx,
                ctx.tenant_id,
                msg_id.as_str(),
                read.user_id.as_str(),
            )
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::DatabaseError, "Database operation failed")
            })?;
        if read.burn_after_read.unwrap_or(false) {
            ctx.repo
                .schedule_message_burn_after_read(
                    ctx.ctx,
                    ctx.tenant_id,
                    msg_id.as_str(),
                    Some(read.user_id.as_str()),
                    first_read_at,
                )
                .await
                .map_err(|e| {
                    map_infra_error(e, ErrorCode::DatabaseError, "Database operation failed")
                })?;
        }
        ctx.repo
            .append_event(ctx.ctx, ctx.tenant_id, msg_id.as_str(), event)
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::DatabaseError, "Database operation failed")
            })?;
    }
    if let Some(stream) = ctx.stream {
        stream
            .append_event_to_stream(ctx.ctx, event)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::DatabaseError,
                    "Failed to append operation event to durable event stream",
                )
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::{EventPayload, EventType};
    use flare_im_contracts::utils::Context;
    use flare_server_core::error::Result as AnyhowResult;
    use std::sync::Arc;

    struct NoopArchiveRepository;

    impl ArchiveStoreRepository for NoopArchiveRepository {
        async fn store_archive(
            &self,
            _ctx: &flare_im_contracts::Ctx,
            _message: &crate::domain::model::Message,
        ) -> AnyhowResult<()> {
            Ok(())
        }

        // 该测试聚焦事件流失败路径：这两个写方法在此显式声明为 no-op（成功），
        // 否则会命中 trait 的“未实现即失败”默认体而提前出错。
        async fn record_message_read(
            &self,
            _ctx: &flare_im_contracts::Ctx,
            _tenant_id: &str,
            _message_id: &str,
            _user_id: &str,
        ) -> AnyhowResult<()> {
            Ok(())
        }

        async fn append_event(
            &self,
            _ctx: &flare_im_contracts::Ctx,
            _tenant_id: &str,
            _message_id: &str,
            _event: &Event,
        ) -> AnyhowResult<()> {
            Ok(())
        }
    }

    struct FailingEventStreamRepository;

    impl EventStreamRepository for FailingEventStreamRepository {
        async fn append_event_to_stream(
            &self,
            _ctx: &flare_im_contracts::Ctx,
            _event: &Event,
        ) -> AnyhowResult<()> {
            Err(flare_server_core::error::FlareError::system(
                "read receipt event stream unavailable".to_string(),
            ))
        }

        async fn event_exists(
            &self,
            _ctx: &flare_im_contracts::Ctx,
            _tenant_id: &str,
            _conversation_id: &str,
            _seq: i64,
        ) -> AnyhowResult<bool> {
            Ok(false)
        }
    }

    fn test_ctx() -> flare_im_contracts::Ctx {
        Arc::new(Context::with_request_id("req-read-stream-test").with_tenant_id("tenant-a"))
    }

    fn read_event(read: ReadPayload) -> Event {
        Event {
            tenant_id: "tenant-a".to_string(),
            conversation_id: "conversation-a".to_string(),
            seq: 8,
            r#type: EventType::ReadReceipt,
            created_at: None,
            operator_id: "reader-a".to_string(),
            event_seq: None,
            request_id: Some("request-a".to_string()),
            payload: Some(EventPayload::Read(read)),
        }
    }

    #[tokio::test]
    async fn apply_read_returns_error_when_event_stream_append_fails() {
        let repo = NoopArchiveRepository;
        let stream = FailingEventStreamRepository;
        let runtime_ctx = test_ctx();
        let event_ctx = EventContext {
            repo: &repo,
            stream: Some(&stream),
            ctx: &runtime_ctx,
            tenant_id: "tenant-a",
            conversation_id: "conversation-a",
        };
        let read = ReadPayload {
            conversation_id: "conversation-a".to_string(),
            read_seq: 8,
            user_id: "reader-a".to_string(),
            message_ids: vec!["message-a".to_string()],
            read_at: None,
            burn_after_read: Some(false),
        };
        let event = read_event(read.clone());

        let err = apply_read(&event_ctx, &event, &read)
            .await
            .expect_err("read receipt stream failure must fail the durable write path");

        assert!(
            err.to_string()
                .contains("Failed to append operation event to durable event stream"),
            "unexpected error: {err}"
        );
    }
}
