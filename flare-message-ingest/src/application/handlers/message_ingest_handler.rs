//! 消息处理器（编排层）- 负责编排消息摄入主链
//!
//! ## 核心职责
//! 1. 消息校验（调用 MessageIngestService）
//! 2. Hook 执行（调用 ExtensionOrchestrator）
//! 3. 摄入主链编排（调用 MessageIngestService）
//! 4. 会话确保（调用 ConversationEnsureService）
//!
//! ## 设计原则
//! - 编排层：不包含业务逻辑，只负责流程编排
//! - 依赖注入：通过构造函数注入所有依赖
//! - CQRS：Command Handler 负责写操作

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flare_im_contracts::Ctx;
use flare_im_service_kit::metrics::MessageOrchestratorMetrics;
use flare_proto::common::{
    ContentVisibility, MessageRetentionPolicy, RetentionMode, RetentionTrigger, SendAckDurability,
};
use futures::{StreamExt, stream};
use tokio::sync::Semaphore;
use tracing::instrument;

use crate::application::commands::{
    SendMessageCommand, SendMessageOutcome, SendSystemMessageCommand,
};
use crate::application::extension::ExtensionOrchestrator;
use crate::domain::PersistenceMode;
use crate::domain::service::{
    ConversationEnsureService, MessageIngestService, build_conversation_ensure_request_from_message,
};
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};

const MAX_BATCH_SEND_CONCURRENCY: usize = 32;
const MAX_BACKGROUND_WAL_CLEANUP_CONCURRENCY: usize = 128;

/// 消息处理器（编排层）
pub struct MessageIngestHandler {
    /// 消息摄入服务
    message_ingest_service: Arc<MessageIngestService>,
    /// 扩展编排器（统一 Hook / Plugin 执行策略）
    extension_orchestrator: Arc<ExtensionOrchestrator>,
    /// 会话确保服务
    conversation_ensure_service: Arc<ConversationEnsureService>,
    /// 发送链路指标
    metrics: Arc<MessageOrchestratorMetrics>,
    /// broker 接受后的 WAL 清理可以后台执行；限制并发避免高峰期无界堆积。
    wal_cleanup_permits: Arc<Semaphore>,
}

impl MessageIngestHandler {
    pub fn new(
        message_ingest_service: Arc<MessageIngestService>,
        extension_orchestrator: Arc<ExtensionOrchestrator>,
        conversation_ensure_service: Arc<ConversationEnsureService>,
        metrics: Arc<MessageOrchestratorMetrics>,
    ) -> Self {
        Self {
            message_ingest_service,
            extension_orchestrator,
            conversation_ensure_service,
            metrics,
            wal_cleanup_permits: Arc::new(Semaphore::new(MAX_BACKGROUND_WAL_CLEANUP_CONCURRENCY)),
        }
    }

    fn validate_send_ack_message_id(message_id: &str) -> Result<&str> {
        let message_id = message_id.trim();
        if message_id.is_empty() {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "send_ack message_id is required",
            )
            .build_error());
        }
        Ok(message_id)
    }

    fn unsupported_send_ack_error(message_id: &str) -> FlareError {
        ErrorBuilder::new(
            ErrorCode::OperationNotSupported,
            "send_ack durable state update is not implemented",
        )
        .param("message_id", message_id)
        .build_error()
    }

    async fn measure_stage<T, F>(&self, stage: &'static str, future: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let start = Instant::now();
        let result = future.await;
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics
            .observe_send_stage(stage, outcome, start.elapsed());
        result
    }

    fn observe_skipped_stage(&self, stage: &'static str) {
        self.metrics
            .observe_send_stage(stage, "skipped", Duration::ZERO);
    }

    async fn cleanup_wal_after_broker_accept(
        &self,
        submission: crate::domain::model::MessageSubmission,
    ) {
        let permit = self.wal_cleanup_permits.clone().try_acquire_owned();
        let Ok(permit) = permit else {
            let cleanup_start = Instant::now();
            self.remove_wal_after_broker_accept(submission, cleanup_start)
                .await;
            return;
        };

        self.metrics
            .observe_send_stage("wal_cleanup", "scheduled", Duration::ZERO);

        let message_ingest_service = Arc::clone(&self.message_ingest_service);
        let metrics = Arc::clone(&self.metrics);
        tokio::spawn(async move {
            let _permit = permit;
            let cleanup_start = Instant::now();
            remove_wal_after_broker_accept(
                message_ingest_service,
                metrics,
                submission,
                cleanup_start,
            )
            .await;
        });
    }

    async fn remove_wal_after_broker_accept(
        &self,
        submission: crate::domain::model::MessageSubmission,
        cleanup_start: Instant,
    ) {
        remove_wal_after_broker_accept(
            Arc::clone(&self.message_ingest_service),
            Arc::clone(&self.metrics),
            submission,
            cleanup_start,
        )
        .await;
    }

    /// 处理发送消息命令
    ///
    /// # 编排流程
    /// 1. 校验消息
    /// 2. 执行 PreSend Hook
    /// 3. 准备消息并分配序列号
    /// 4. 确保会话存在
    /// 5. 消息装饰
    /// 6. 计算持久化模式
    /// 7. 写入最终消息 WAL
    /// 8. 推送消息
    /// 9. broker 接受后清理 WAL
    /// 10. 执行 PostSend Hook
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        conversation_id = %cmd.conversation_id,
    ))]
    pub async fn handle_send_message(
        &self,
        ctx: &Ctx,
        cmd: SendMessageCommand,
    ) -> Result<SendMessageOutcome> {
        let total_start = Instant::now();
        let result = self.handle_send_message_inner(ctx, cmd).await;
        match &result {
            Ok(outcome) => {
                self.metrics
                    .observe_send_stage("total", "success", total_start.elapsed());
                self.metrics
                    .record_send_total(durability_label(&outcome.durability), "success");
            }
            Err(error) => {
                self.metrics
                    .observe_send_stage("total", "error", total_start.elapsed());
                self.metrics.record_send_total("unknown", "error");
                tracing::warn!(
                    error = %error,
                    duration_ms = total_start.elapsed().as_millis(),
                    "Message send failed"
                );
            }
        }
        result
    }

    async fn handle_send_message_inner(
        &self,
        ctx: &Ctx,
        cmd: SendMessageCommand,
    ) -> Result<SendMessageOutcome> {
        if cmd.sync {
            return Err(ErrorBuilder::new(
                ErrorCode::OperationNotSupported,
                "sync persisted send is not implemented; use async send and wait for persistence ack",
            )
            .build_error());
        }

        let tenant_id = ctx.tenant_id().unwrap_or("0").to_string();
        let mut message = cmd.message;
        let burn_enabled = cmd.burn_enabled || message.retention_policy.is_some();
        let burn_after_read_seconds = cmd.burn_after_read_seconds.or_else(|| {
            message
                .retention_policy
                .as_ref()
                .and_then(|policy| policy.expire_after_seconds)
        });
        if burn_enabled {
            let Some(after_read_seconds) = burn_after_read_seconds.filter(|seconds| *seconds > 0)
            else {
                return Err(flare_server_core::flare_err!(
                    flare_server_core::error::ErrorCode::InvalidParameter,
                    "burn_after_read_seconds must be positive when burn is enabled"
                ));
            };
            message.retention_policy = Some(MessageRetentionPolicy {
                mode: RetentionMode::AfterRead as i32,
                trigger: RetentionTrigger::AfterRead as i32,
                expire_after_seconds: Some(after_read_seconds),
                expire_at: None,
                visibility_after_expiration: ContentVisibility::Redacted as i32,
                attributes: Default::default(),
            });
            message.retention_state = None;
        }

        // 1. 校验消息
        self.measure_stage(
            "validate",
            self.message_ingest_service
                .validate_message(ctx, &tenant_id, &message),
        )
        .await?;

        // 2. 执行 PreSend Hook（经统一扩展编排器）
        let hook_context = self
            .measure_stage(
                "pre_send_hook",
                self.extension_orchestrator
                    .execute_pre_send(ctx, message, true),
            )
            .await?;

        // 3. 准备消息并分配序列号
        let (mut submission, profile) = self
            .measure_stage(
                "prepare_allocate_seq",
                self.message_ingest_service.prepare_and_allocate_seq(
                    ctx,
                    &tenant_id,
                    hook_context.message,
                ),
            )
            .await?;

        // 4. 确保会话存在（Social SyncSignal 内部路由 `sync:{owner}` 仅在线推送，不落会话表）
        if !submission.message.conversation_id.starts_with("sync:") {
            let ensure_request =
                build_conversation_ensure_request_from_message(&submission.message, &tenant_id);
            self.measure_stage(
                "conversation_ensure",
                self.conversation_ensure_service
                    .ensure_conversation(ctx, &ensure_request),
            )
            .await?;
        } else {
            self.observe_skipped_stage("conversation_ensure");
        }

        // 5. 消息装饰
        submission.message = self
            .measure_stage(
                "decorate",
                self.message_ingest_service
                    .decorate_message(submission.message.clone()),
            )
            .await?;

        // 6. 计算最终持久化模式。WAL 必须在最终消息成形之后写入，便于失败恢复重放。
        let persistence_mode = if profile.is_temporary() {
            PersistenceMode::ForcePushOnly
        } else if profile.is_notification() {
            // NotificationContent.persistent=false → 仅在线推送，不走 storage 队列（BusinessEphemeral）
            match crate::domain::model::notification_persistent(&submission.message) {
                Some(false) => PersistenceMode::ForcePushOnly,
                _ => PersistenceMode::Auto,
            }
        } else {
            PersistenceMode::Auto
        };

        // 7. 写入 WAL。只要进入持久化主队列，就先落 WAL；push-only 不承诺离线恢复。
        self.measure_stage(
            "wal_write",
            self.message_ingest_service.write_wal_if_needed(
                &submission,
                &profile,
                persistence_mode,
                &tenant_id,
            ),
        )
        .await?;

        // 8. 推送消息
        self.measure_stage(
            "mq_publish",
            self.message_ingest_service
                .push_message(ctx, &submission, &profile, persistence_mode),
        )
        .await?;

        let durability = send_ack_durability(&profile, &persistence_mode);

        // 9. broker 已确认接受后，主队列成为恢复来源，可以清理发送侧 WAL。
        // 清理失败只会增加一次未来重放/去重成本，不能让已被 broker 接受的发送变成失败。
        if durability != SendAckDurability::TransientAccepted {
            self.cleanup_wal_after_broker_accept(submission.clone())
                .await;
        } else {
            self.observe_skipped_stage("wal_cleanup");
        }

        // 10. 执行 PostSend Hook（经统一扩展编排器）
        self.measure_stage(
            "post_send_hook",
            self.extension_orchestrator.execute_post_send(
                ctx,
                &submission,
                &hook_context.hook_context,
            ),
        )
        .await?;

        Ok(SendMessageOutcome {
            message_id: submission.message_id,
            conversation_seq: submission.message.conversation_seq,
            durability,
        })
    }

    /// 发送系统消息
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        conversation_id = %cmd.conversation_id,
    ))]
    pub async fn send_system_message(
        &self,
        ctx: &Ctx,
        cmd: SendSystemMessageCommand,
    ) -> Result<String> {
        let send_cmd = SendMessageCommand {
            message: cmd.message,
            conversation_id: cmd.conversation_id,
            sync: false,
            burn_enabled: false,
            burn_after_read_seconds: None,
        };
        let outcome = self.handle_send_message(ctx, send_cmd).await?;
        Ok(outcome.message_id)
    }

    #[instrument(skip(self, ctx))]
    pub async fn batch_send_message(
        &self,
        ctx: &Ctx,
        messages: Vec<SendMessageCommand>,
    ) -> Result<Vec<Result<SendMessageOutcome>>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        self.metrics.observe_batch_size(messages.len());

        let concurrency = messages.len().clamp(1, MAX_BATCH_SEND_CONCURRENCY);
        let results = stream::iter(messages.into_iter())
            .map(|cmd| async move { self.handle_send_message(ctx, cmd).await })
            .buffered(concurrency)
            .collect::<Vec<_>>()
            .await;

        Ok(results)
    }

    #[instrument(skip(self, ctx))]
    pub async fn send_ack(&self, ctx: &Ctx, message_id: &str) -> Result<()> {
        let message_id = Self::validate_send_ack_message_id(message_id)?;
        let _ = ctx;
        Err(Self::unsupported_send_ack_error(message_id))
    }

    #[instrument(skip(self, ctx))]
    pub async fn send_custom_data(&self, ctx: &Ctx, data: Vec<u8>) -> Result<()> {
        let _ = (ctx, data);
        Ok(())
    }
}

async fn remove_wal_after_broker_accept(
    message_ingest_service: Arc<MessageIngestService>,
    metrics: Arc<MessageOrchestratorMetrics>,
    submission: crate::domain::model::MessageSubmission,
    cleanup_start: Instant,
) {
    if let Err(error) = message_ingest_service
        .remove_wal_after_broker_accept(&submission)
        .await
    {
        metrics.observe_send_stage("wal_cleanup", "error_ignored", cleanup_start.elapsed());
        tracing::warn!(
            message_id = %submission.message_id,
            conversation_id = %submission.message.conversation_id,
            error = %error,
            "Failed to remove WAL after broker accept; keeping entry for recovery/dedup"
        );
    } else {
        metrics.observe_send_stage("wal_cleanup", "success", cleanup_start.elapsed());
    }
}

fn send_ack_durability(
    profile: &crate::domain::MessageProfile,
    persistence_mode: &PersistenceMode,
) -> SendAckDurability {
    if persistence_mode.should_push_only(profile.is_temporary()) {
        SendAckDurability::TransientAccepted
    } else {
        SendAckDurability::BrokerAccepted
    }
}

fn durability_label(durability: &SendAckDurability) -> &'static str {
    match durability {
        SendAckDurability::Unspecified => "unspecified",
        SendAckDurability::WalAccepted => "wal_accepted",
        SendAckDurability::BrokerAccepted => "broker_accepted",
        SendAckDurability::Persisted => "persisted",
        SendAckDurability::TransientAccepted => "transient_accepted",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_ack_rejects_empty_message_id() {
        let err = MessageIngestHandler::validate_send_ack_message_id("  ").unwrap_err();

        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
        assert!(err.reason().contains("message_id"));
    }

    #[test]
    fn send_ack_without_durable_ack_store_is_explicitly_unsupported() {
        let err = MessageIngestHandler::unsupported_send_ack_error("msg-1");

        assert_eq!(err.code(), Some(ErrorCode::OperationNotSupported));
        assert!(err.reason().contains("send_ack"));
    }
}
