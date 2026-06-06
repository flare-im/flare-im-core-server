//! 命令处理器（编排层）- 轻量级，只负责编排领域服务

use flare_im_core::Ctx;
use flare_im_core::metrics::StorageWriterMetrics;
use flare_server_core::error::{ErrorCode, Result, map_infra_error};
use std::sync::Arc;
use std::time::Instant;
use tracing::instrument;

use crate::application::commands::ProcessStoreMessageCommand;
use crate::domain::model::PersistenceResult;
use crate::domain::service::MessagePersistenceDomainService;

/// 普通消息持久化命令处理器（编排层）
///
/// 使用泛型参数而非 trait objects，符合 Rust 2024 规范
pub struct MessagePersistenceCommandHandler<I, H, A, E, W, P>
where
    I: crate::domain::repository::MessageIdempotencyRepository + Send + Sync,
    H: crate::domain::repository::HotCacheRepository + Send + Sync,
    A: crate::domain::repository::ArchiveStoreRepository + Send + Sync,
    E: crate::domain::repository::EventStreamRepository + Send + Sync,
    W: crate::domain::repository::WalCleanupRepository + Send + Sync,
    P: crate::domain::repository::AckPublisher + Send + Sync,
{
    domain_service: Arc<MessagePersistenceDomainService<I, H, A, E, W, P>>,
    metrics: Arc<StorageWriterMetrics>,
}

impl<I, H, A, E, W, P> MessagePersistenceCommandHandler<I, H, A, E, W, P>
where
    I: crate::domain::repository::MessageIdempotencyRepository + Send + Sync,
    H: crate::domain::repository::HotCacheRepository + Send + Sync,
    A: crate::domain::repository::ArchiveStoreRepository + Send + Sync,
    E: crate::domain::repository::EventStreamRepository + Send + Sync,
    W: crate::domain::repository::WalCleanupRepository + Send + Sync,
    P: crate::domain::repository::AckPublisher + Send + Sync,
{
    pub fn new(
        domain_service: Arc<MessagePersistenceDomainService<I, H, A, E, W, P>>,
        metrics: Arc<StorageWriterMetrics>,
    ) -> Self {
        Self {
            domain_service,
            metrics,
        }
    }

    /// 处理存储消息命令 - 只处理普通消息，如果是操作消息则返回 None
    #[instrument(skip(self, ctx, command), fields(tenant_id, message_id))]
    pub async fn handle(
        &self,
        ctx: &Ctx,
        command: ProcessStoreMessageCommand,
    ) -> Result<Option<PersistenceResult>> {
        let start = Instant::now();

        let message_id_for_error = command.message.as_ref().map(|m| m.server_id.clone());

        // 提取租户ID用于指标
        let tenant_id = command
            .tenant
            .as_ref()
            .map(|t| t.tenant_id.as_str())
            .unwrap_or("unknown")
            .to_string();

        let prepared = match self.domain_service.prepare_message(command.clone()) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    message_id = ?message_id_for_error,
                    "Failed to prepare message"
                );
                return Err(map_infra_error(
                    e,
                    ErrorCode::InternalError,
                    "Failed to prepare message",
                ));
            }
        };

        let message_id = prepared.message_id.clone();
        let conversation_id = prepared.conversation_id.clone();
        let db_start = Instant::now();
        let result = match self.domain_service.ensure_consistency(ctx, prepared).await {
            Ok(result) => result,
            Err(e) => {
                self.metrics.record_storage_persist("message", "error");
                tracing::error!(
                    error = %e,
                    message_id = %message_id,
                    conversation_id = %conversation_id,
                    "Failed to ensure message consistency"
                );
                return Err(e);
            }
        };

        if result.deduplicated {
            self.metrics.messages_duplicate_total.inc();
            self.metrics
                .record_storage_persist("message", "deduplicated");
            tracing::trace!(
                message_id = %result.message_id,
                "Message is duplicate, skipping persistence"
            );
        } else {
            self.metrics
                .db_write_duration_seconds
                .observe(db_start.elapsed().as_secs_f64());

            let total_duration = start.elapsed();
            self.metrics
                .messages_persisted_duration_seconds
                .observe(total_duration.as_secs_f64());
            self.metrics
                .messages_persisted_total
                .with_label_values(&[tenant_id.as_str()])
                .inc();
            self.metrics.record_storage_persist("message", "success");

            tracing::trace!(
                message_id = %result.message_id,
                conversation_id = %result.conversation_id,
                duration_ms = total_duration.as_millis(),
                "Message persisted successfully"
            );
        }

        Ok(Some(result))
    }

    /// 批量处理存储消息命令（优化性能）
    #[instrument(skip(self, ctx, commands), fields(batch_size = commands.len()))]
    pub async fn handle_batch(
        &self,
        ctx: &Ctx,
        commands: Vec<ProcessStoreMessageCommand>,
    ) -> Result<Vec<PersistenceResult>> {
        let start = Instant::now();

        // 1. 批量准备消息
        let mut prepared_messages = Vec::with_capacity(commands.len());
        for command in &commands {
            match self.domain_service.prepare_message(command.clone()) {
                Ok(prepared) => {
                    prepared_messages.push(prepared);
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to prepare message in batch");
                    return Err(map_infra_error(
                        e,
                        ErrorCode::InternalError,
                        "Failed to prepare message in batch",
                    ));
                }
            }
        }

        if prepared_messages.is_empty() {
            return Ok(Vec::new());
        }

        let db_start = Instant::now();
        let results = match self
            .domain_service
            .ensure_batch_consistency(ctx, prepared_messages)
            .await
        {
            Ok(results) => results,
            Err(e) => {
                self.metrics.record_storage_persist("batch", "error");
                tracing::error!(error = %e, "Failed to ensure batch message consistency");
                return Err(e);
            }
        };

        let deduplicated_count = results.iter().filter(|result| result.deduplicated).count();
        for _ in 0..deduplicated_count {
            self.metrics.messages_duplicate_total.inc();
            self.metrics.record_storage_persist("batch", "deduplicated");
        }

        let persisted_count = results.len().saturating_sub(deduplicated_count);
        if persisted_count > 0 {
            self.metrics
                .db_write_duration_seconds
                .observe(db_start.elapsed().as_secs_f64());

            let total_duration = start.elapsed();
            self.metrics
                .messages_persisted_duration_seconds
                .observe(total_duration.as_secs_f64());
            self.metrics
                .messages_persisted_total
                .with_label_values(&["batch"])
                .inc_by(persisted_count as u64);
            for _ in 0..persisted_count {
                self.metrics.record_storage_persist("batch", "success");
            }

            tracing::trace!(
                batch_size = persisted_count,
                duration_ms = total_duration.as_millis(),
                "Batch messages persisted successfully"
            );
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::commands::ProcessStoreMessageCommand;
    use crate::domain::events::AckEvent;
    use crate::domain::model::{Event, TenantContext};
    use crate::domain::repository::{
        AckPublisher, ArchiveStoreRepository, EventStreamRepository, HotCacheRepository,
        MessageIdempotencyRepository, WalCleanupRepository,
    };
    use flare_im_core::message::Message;
    use flare_im_core::utils::Context;
    use flare_server_core::error::Result as AnyhowResult;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct FailingIdempotencyRepository;

    impl MessageIdempotencyRepository for FailingIdempotencyRepository {
        async fn is_new(&self, _ctx: &Ctx, _message_id: &str) -> AnyhowResult<bool> {
            Err(flare_server_core::error::FlareError::system(
                "idempotency store unavailable".to_string(),
            ))
        }
    }

    struct ReservingIdempotencyRepository {
        reserved: Arc<AtomicBool>,
    }

    impl MessageIdempotencyRepository for ReservingIdempotencyRepository {
        async fn is_new(&self, _ctx: &Ctx, _message_id: &str) -> AnyhowResult<bool> {
            Ok(!self.reserved.swap(true, Ordering::SeqCst))
        }

        async fn release(&self, _ctx: &Ctx, _message_id: &str) -> AnyhowResult<()> {
            self.reserved.store(false, Ordering::SeqCst);
            Ok(())
        }
    }

    struct NoopHotCacheRepository;

    impl HotCacheRepository for NoopHotCacheRepository {
        async fn store_hot(&self, _ctx: &Ctx, _message: &Message) -> AnyhowResult<()> {
            Ok(())
        }
    }

    struct CountingArchiveRepository {
        writes: Arc<AtomicUsize>,
    }

    impl ArchiveStoreRepository for CountingArchiveRepository {
        async fn store_archive(&self, _ctx: &Ctx, _message: &Message) -> AnyhowResult<()> {
            self.writes.fetch_add(1, Ordering::SeqCst);
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

    struct FailOnceEventStreamRepository {
        attempts: Arc<AtomicUsize>,
    }

    impl EventStreamRepository for FailOnceEventStreamRepository {
        async fn append_event_to_stream(&self, _ctx: &Ctx, _event: &Event) -> AnyhowResult<()> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(flare_server_core::error::FlareError::system(
                    "event stream unavailable on first attempt".to_string(),
                ))
            } else {
                Ok(())
            }
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

    struct NoopWalCleanupRepository;

    impl WalCleanupRepository for NoopWalCleanupRepository {
        async fn remove(&self, _ctx: &Ctx, _message_id: &str) -> AnyhowResult<()> {
            Ok(())
        }
    }

    struct NoopAckPublisher;

    impl AckPublisher for NoopAckPublisher {
        async fn publish(&self, _ctx: &Ctx, _event: AckEvent<'_>) -> AnyhowResult<()> {
            Ok(())
        }
    }

    type TestDomainService = MessagePersistenceDomainService<
        FailingIdempotencyRepository,
        NoopHotCacheRepository,
        CountingArchiveRepository,
        NoopEventStreamRepository,
        NoopWalCleanupRepository,
        NoopAckPublisher,
    >;

    type TestCommandHandler = MessagePersistenceCommandHandler<
        FailingIdempotencyRepository,
        NoopHotCacheRepository,
        CountingArchiveRepository,
        NoopEventStreamRepository,
        NoopWalCleanupRepository,
        NoopAckPublisher,
    >;

    fn test_ctx() -> Ctx {
        Arc::new(Context::with_request_id("req-batch-idempotency-test").with_tenant_id("tenant-a"))
    }

    fn test_command(message_id: &str) -> ProcessStoreMessageCommand {
        let mut extra = HashMap::new();
        extra.insert("tenant_id".to_string(), "tenant-a".to_string());
        ProcessStoreMessageCommand {
            conversation_id: "conversation-a".to_string(),
            message: Some(Message {
                server_id: message_id.to_string(),
                conversation_id: "conversation-a".to_string(),
                sender_id: "sender-a".to_string(),
                conversation_seq: 1,
                status: 2,
                extra,
                ..Message::default()
            }),
            sync: true,
            context: None,
            tenant: Some(TenantContext {
                tenant_id: "tenant-a".to_string(),
                user_id: Some("sender-a".to_string()),
            }),
            tags: HashMap::new(),
            metadata: HashMap::new(),
        }
    }

    fn handler_with_failing_idempotency(writes: Arc<AtomicUsize>) -> TestCommandHandler {
        let domain_service: Arc<TestDomainService> =
            Arc::new(MessagePersistenceDomainService::new(
                Some(Arc::new(FailingIdempotencyRepository)),
                None,
                Some(Arc::new(CountingArchiveRepository { writes })),
                None,
                None,
                None,
            ));
        MessagePersistenceCommandHandler::new(domain_service, Arc::new(StorageWriterMetrics::new()))
    }

    type RetryDomainService = MessagePersistenceDomainService<
        ReservingIdempotencyRepository,
        NoopHotCacheRepository,
        CountingArchiveRepository,
        FailOnceEventStreamRepository,
        NoopWalCleanupRepository,
        NoopAckPublisher,
    >;

    type RetryCommandHandler = MessagePersistenceCommandHandler<
        ReservingIdempotencyRepository,
        NoopHotCacheRepository,
        CountingArchiveRepository,
        FailOnceEventStreamRepository,
        NoopWalCleanupRepository,
        NoopAckPublisher,
    >;

    fn handler_with_reservation_and_fail_once_event_stream(
        reserved: Arc<AtomicBool>,
        archive_writes: Arc<AtomicUsize>,
        stream_attempts: Arc<AtomicUsize>,
    ) -> RetryCommandHandler {
        let domain_service: Arc<RetryDomainService> =
            Arc::new(MessagePersistenceDomainService::new(
                Some(Arc::new(ReservingIdempotencyRepository { reserved })),
                None,
                Some(Arc::new(CountingArchiveRepository {
                    writes: archive_writes,
                })),
                Some(Arc::new(FailOnceEventStreamRepository {
                    attempts: stream_attempts,
                })),
                None,
                Some(Arc::new(NoopAckPublisher)),
            ));
        MessagePersistenceCommandHandler::new(domain_service, Arc::new(StorageWriterMetrics::new()))
    }

    #[tokio::test]
    async fn handle_batch_returns_error_and_skips_writes_when_idempotency_check_fails() {
        let writes = Arc::new(AtomicUsize::new(0));
        let handler = handler_with_failing_idempotency(writes.clone());

        let err = handler
            .handle_batch(&test_ctx(), vec![test_command("message-a")])
            .await
            .expect_err("batch idempotency failures must fail closed");

        assert!(
            err.to_string().contains("Failed to check idempotency"),
            "unexpected error: {err}"
        );
        assert_eq!(
            writes.load(Ordering::SeqCst),
            0,
            "idempotency failures must not continue into archive writes"
        );
    }

    #[tokio::test]
    async fn handle_releases_idempotency_reservation_after_durable_write_failure() {
        let reserved = Arc::new(AtomicBool::new(false));
        let archive_writes = Arc::new(AtomicUsize::new(0));
        let stream_attempts = Arc::new(AtomicUsize::new(0));
        let handler = handler_with_reservation_and_fail_once_event_stream(
            reserved,
            archive_writes.clone(),
            stream_attempts.clone(),
        );
        let ctx = test_ctx();

        handler
            .handle(&ctx, test_command("message-a"))
            .await
            .expect_err("first attempt should fail at durable event stream append");

        let retry = handler
            .handle(&ctx, test_command("message-a"))
            .await
            .expect("retry after a failed durable write must be allowed")
            .expect("message handler should return a persistence result");

        assert!(
            !retry.deduplicated,
            "handler retry after a failed durable write must not be reported as duplicate"
        );
        assert_eq!(archive_writes.load(Ordering::SeqCst), 2);
        assert_eq!(stream_attempts.load(Ordering::SeqCst), 2);
    }
}
