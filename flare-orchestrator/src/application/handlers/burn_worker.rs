//! 阅后即焚 due worker 编排。

use crate::application::commands::BurnDueMessagesCommand;
use crate::domain::builder::build_burned_event;
use flare_im_core::Ctx;
use flare_proto::common::Event;
use flare_server_core::error::Result;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::EventHandler;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BurnDueMessage {
    pub tenant_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub reader_id: Option<String>,
    pub burn_at: i64,
}

pub trait BurnDueMessageRepository: Send + Sync {
    async fn scan_due_burn_messages(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        now: i64,
        limit: i64,
    ) -> Result<Vec<BurnDueMessage>>;
}

pub trait BurnEventSink: Send + Sync {
    async fn publish_burn_event(&self, ctx: &Ctx, event: Event) -> Result<()>;
}

impl BurnEventSink for EventHandler {
    async fn publish_burn_event(&self, ctx: &Ctx, event: Event) -> Result<()> {
        self.handle_event(ctx, event).await
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BurnWorkerBatchResult {
    pub scanned: usize,
    pub succeeded: usize,
    pub failed: usize,
}

pub struct MessageBurnWorker<R, S = EventHandler>
where
    R: BurnDueMessageRepository,
    S: BurnEventSink,
{
    repo: Arc<R>,
    sink: Arc<S>,
}

impl<R, S> MessageBurnWorker<R, S>
where
    R: BurnDueMessageRepository,
    S: BurnEventSink,
{
    pub fn new(repo: Arc<R>, sink: Arc<S>) -> Self {
        Self { repo, sink }
    }

    pub async fn run_due_batch(
        &self,
        ctx: &Ctx,
        cmd: BurnDueMessagesCommand,
    ) -> Result<BurnWorkerBatchResult> {
        let limit = cmd.limit.clamp(1, 1000);
        info!(
            tenant_id = %cmd.tenant_id,
            now = cmd.now,
            limit,
            "burn worker batch started"
        );
        debug!(
            tenant_id = %cmd.tenant_id,
            burn_status = "BurnPending",
            burn_at_lte = cmd.now,
            limit,
            "scanning due burn messages"
        );

        let due_messages = self
            .repo
            .scan_due_burn_messages(ctx, &cmd.tenant_id, cmd.now, limit)
            .await
            .map_err(|err| {
                error!(error = ?err, "burn worker scan failed");
                err
            })?;

        let mut result = BurnWorkerBatchResult {
            scanned: due_messages.len(),
            succeeded: 0,
            failed: 0,
        };

        for due in due_messages {
            let event = build_burned_event(
                &due.tenant_id,
                &due.conversation_id,
                &due.message_id,
                due.reader_id.as_deref(),
                due.burn_at,
                cmd.now,
            );
            match self.sink.publish_burn_event(ctx, event).await {
                Ok(()) => result.succeeded += 1,
                Err(err) => {
                    result.failed += 1;
                    warn!(
                        error = ?err,
                        tenant_id = %due.tenant_id,
                        conversation_id = %due.conversation_id,
                        message_id = %due.message_id,
                        "burn worker item failed"
                    );
                }
            }
        }

        info!(
            tenant_id = %cmd.tenant_id,
            scanned = result.scanned,
            succeeded = result.succeeded,
            failed = result.failed,
            "burn worker batch completed"
        );
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_server_core::context::Context;
    use std::sync::Mutex;

    struct FakeRepo;

    impl BurnDueMessageRepository for FakeRepo {
        async fn scan_due_burn_messages(
            &self,
            _ctx: &Ctx,
            tenant_id: &str,
            _now: i64,
            _limit: i64,
        ) -> Result<Vec<BurnDueMessage>> {
            Ok(vec![
                BurnDueMessage {
                    tenant_id: tenant_id.to_string(),
                    conversation_id: "c1".to_string(),
                    message_id: "m1".to_string(),
                    reader_id: Some("u1".to_string()),
                    burn_at: 10,
                },
                BurnDueMessage {
                    tenant_id: tenant_id.to_string(),
                    conversation_id: "c1".to_string(),
                    message_id: "fail".to_string(),
                    reader_id: Some("u1".to_string()),
                    burn_at: 11,
                },
                BurnDueMessage {
                    tenant_id: tenant_id.to_string(),
                    conversation_id: "c1".to_string(),
                    message_id: "m2".to_string(),
                    reader_id: Some("u1".to_string()),
                    burn_at: 12,
                },
            ])
        }
    }

    struct FakeSink {
        seen: Mutex<Vec<String>>,
    }

    impl BurnEventSink for FakeSink {
        async fn publish_burn_event(&self, _ctx: &Ctx, event: Event) -> Result<()> {
            let message_id = match event.payload.as_ref() {
                Some(flare_proto::common::event::Payload::RetentionExpired(b)) => {
                    b.server_msg_id.clone()
                }
                _ => String::new(),
            };
            if message_id == "fail" {
                return Err(flare_server_core::flare_err!(
                    flare_server_core::error::ErrorCode::InternalError,
                    "simulated item failure"
                ));
            }
            self.seen.lock().unwrap().push(message_id);
            Ok(())
        }
    }

    #[tokio::test]
    async fn worker_item_failure_does_not_abort_batch() {
        let worker = MessageBurnWorker::new(
            Arc::new(FakeRepo),
            Arc::new(FakeSink {
                seen: Mutex::new(Vec::new()),
            }),
        );
        let ctx = Arc::new(Context::root());

        let result = worker
            .run_due_batch(
                &ctx,
                BurnDueMessagesCommand {
                    tenant_id: "t1".to_string(),
                    now: 20,
                    limit: 100,
                },
            )
            .await
            .unwrap();

        assert_eq!(result.scanned, 3);
        assert_eq!(result.succeeded, 2);
        assert_eq!(result.failed, 1);
    }
}
