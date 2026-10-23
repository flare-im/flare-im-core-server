//! 阅后即焚 due worker 编排。

use crate::application::commands::BurnDueMessagesCommand;
use crate::domain::builder::build_burned_event;
use flare_im_contracts::Ctx;
use flare_im_contracts::utils::normalize_optional_tenant_id;
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
        self.handle_system_event(ctx, event).await
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
            // 焚毁事件本身不带租户，下游（seq 分配、落库、推送）只按上下文的租户处理，所以每条
            // 都在该行自己的租户下发布，不沿用批次上下文的租户（定时任务的根上下文没有租户，
            // 也可能与行的租户不同）。行里没有租户就不发布，计为失败，不借批次的租户兜底。
            let Some(tenant_id) = normalize_optional_tenant_id(Some(due.tenant_id.as_str())) else {
                result.failed += 1;
                warn!(
                    conversation_id = %due.conversation_id,
                    message_id = %due.message_id,
                    "burn worker item has no tenant; not published"
                );
                continue;
            };
            let item_ctx: Ctx = Arc::new(ctx.with_tenant_id(&tenant_id));
            let event = build_burned_event(
                &tenant_id,
                &due.conversation_id,
                &due.message_id,
                due.reader_id.as_deref(),
                due.burn_at,
                cmd.now,
            );
            match self.sink.publish_burn_event(&item_ctx, event).await {
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

    /// 扫出来的每一行按自己的 `tenant_id` 发布：事件本身不带租户，下游只看上下文的租户。
    struct RowsRepo(Vec<BurnDueMessage>);

    impl BurnDueMessageRepository for RowsRepo {
        async fn scan_due_burn_messages(
            &self,
            _ctx: &Ctx,
            _tenant_id: &str,
            _now: i64,
            _limit: i64,
        ) -> Result<Vec<BurnDueMessage>> {
            Ok(self.0.clone())
        }
    }

    /// 记下每次发布时上下文里的租户与请求号。
    #[derive(Default)]
    struct TenantSink {
        seen: Mutex<Vec<(String, Option<String>, String)>>,
    }

    impl BurnEventSink for TenantSink {
        async fn publish_burn_event(&self, ctx: &Ctx, event: Event) -> Result<()> {
            let message_id = match event.payload.as_ref() {
                Some(flare_proto::common::event::Payload::RetentionExpired(b)) => {
                    b.server_msg_id.clone()
                }
                _ => String::new(),
            };
            self.seen.lock().unwrap().push((
                message_id,
                ctx.tenant_id().map(str::to_string),
                ctx.request_id().to_string(),
            ));
            Ok(())
        }
    }

    fn due(tenant_id: &str, message_id: &str) -> BurnDueMessage {
        BurnDueMessage {
            tenant_id: tenant_id.to_string(),
            conversation_id: "c1".to_string(),
            message_id: message_id.to_string(),
            reader_id: Some("u1".to_string()),
            burn_at: 10,
        }
    }

    /// 批次上下文可能不带租户（定时任务的根上下文），也可能是别的租户：每条事件都要在该行
    /// 自己的租户下发布；以前直接用批次上下文，事件落到上下文的租户或因缺租户被拒。
    /// 行里没有租户时不发布、计为失败，不借批次上下文的租户。
    #[tokio::test]
    async fn each_item_is_published_under_its_own_tenant() {
        let sink = Arc::new(TenantSink::default());
        let worker = MessageBurnWorker::new(
            Arc::new(RowsRepo(vec![
                due("tenant-a", "m-a"),
                due("tenant-b", "m-b"),
                due(" ", "m-blank"),
                due("0", "m-zero"),
            ])),
            sink.clone(),
        );

        for ctx in [
            Arc::new(Context::with_request_id("burn-req")),
            Arc::new(Context::with_request_id("burn-req").with_tenant_id("tenant-other")),
        ] {
            sink.seen.lock().unwrap().clear();
            let ctx: Ctx = ctx;
            let result = worker
                .run_due_batch(
                    &ctx,
                    BurnDueMessagesCommand {
                        tenant_id: "tenant-a".to_string(),
                        now: 20,
                        limit: 100,
                    },
                )
                .await
                .unwrap();

            assert_eq!(result.scanned, 4);
            assert_eq!(result.succeeded, 3);
            assert_eq!(result.failed, 1);
            let seen = sink.seen.lock().unwrap().clone();
            assert_eq!(
                seen,
                vec![
                    (
                        "m-a".to_string(),
                        Some("tenant-a".to_string()),
                        "burn-req".to_string()
                    ),
                    (
                        "m-b".to_string(),
                        Some("tenant-b".to_string()),
                        "burn-req".to_string()
                    ),
                    (
                        "m-zero".to_string(),
                        Some("0".to_string()),
                        "burn-req".to_string()
                    ),
                ]
            );
        }
    }
}
