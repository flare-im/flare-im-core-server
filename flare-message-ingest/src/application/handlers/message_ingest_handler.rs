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
use flare_im_service_kit::tenant_runtime::{TenantAccessPolicy, TenantLookup, TenantRuntimeCache};
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
use crate::application::handlers::send_rate_limiter::SendRateLimiter;
use crate::domain::PersistenceMode;
use crate::domain::service::{
    ConversationEnsureService, MessageIngestService, build_conversation_ensure_request_from_message,
};
use flare_server_core::auth::SignedActorType;
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use flare_server_core::grpc::middleware::signed_context::VerifiedContext;

const MAX_BATCH_SEND_CONCURRENCY: usize = 32;
const MAX_BATCH_SEND_SIZE: usize = 1024;
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
    /// 摄入边界幂等存储（可选）：按 client_msg_id 去重重发，闭合丢-ACK 重试导致的重复 echo + seq 空洞。
    idempotency: Option<Arc<dyn crate::domain::repository::IngestIdempotencyStore>>,
    /// 发送入口限流（可选）：按 tenant / tenant+sender / tenant+conversation 保护摄入边界。
    send_rate_limiter: Option<Arc<SendRateLimiter>>,
    /// 租户投影：`quota.core.send_qps` 优先于全局 tenant 阈值（0 = 回落）。
    tenant_runtime: TenantRuntimeCache,
    tenant_policy: TenantAccessPolicy,
    /// WAL 后 MQ publish 阶段超时。持久消息超时后返回 WalAccepted，交给 WAL replay 恢复。
    send_publish_timeout: Option<Duration>,
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
            idempotency: None,
            send_rate_limiter: None,
            tenant_runtime: TenantRuntimeCache::disabled(),
            tenant_policy: TenantAccessPolicy::Lenient,
            send_publish_timeout: None,
        }
    }

    /// 接通摄入幂等存储（按 client_msg_id 去重重发）。未设置则退化为原行为（不去重）。
    pub fn with_idempotency(
        mut self,
        store: Arc<dyn crate::domain::repository::IngestIdempotencyStore>,
    ) -> Self {
        self.idempotency = Some(store);
        self
    }

    pub fn with_tenant_runtime(mut self, tenant_runtime: TenantRuntimeCache) -> Self {
        self.tenant_runtime = tenant_runtime;
        self
    }

    pub fn with_tenant_policy(mut self, tenant_policy: TenantAccessPolicy) -> Self {
        self.tenant_policy = tenant_policy;
        self
    }

    pub fn with_send_rate_limiter(mut self, limiter: Arc<SendRateLimiter>) -> Self {
        self.send_rate_limiter = Some(limiter);
        self
    }

    pub fn with_send_publish_timeout(mut self, timeout: Duration) -> Self {
        if timeout > Duration::ZERO {
            self.send_publish_timeout = Some(timeout);
        }
        self
    }

    /// 租户闸门。停用 / 删除中，或严格策略下没有投影：`PERMISSION_DENIED TENANT_UNAVAILABLE`，
    /// 是确定的租户结论。严格策略下投影读不到（数据库抖动等）：仍然拒绝，但只是此刻放不了行，
    /// 答可重试的 `SERVICE_UNAVAILABLE`，API 网关据此回 503 而不是 403，与它自己的租户门一致。
    /// 宽松策略下未投影或读不到都放行。
    fn tenant_gate(
        tenant_id: &str,
        lookup: &TenantLookup,
        policy: TenantAccessPolicy,
    ) -> Result<()> {
        if lookup.admits_connection(policy) {
            return Ok(());
        }
        let reason = match lookup {
            TenantLookup::Known(snapshot) => snapshot.status.as_str(),
            TenantLookup::Unknown => "unknown",
            TenantLookup::Unavailable => "unavailable",
        };
        if matches!(lookup, TenantLookup::Unavailable) {
            tracing::warn!(
                tenant_id,
                reason,
                policy = policy.as_str(),
                "message rejected for now: tenant runtime projection unreadable (retryable)"
            );
            return Err(ErrorBuilder::new(
                ErrorCode::ServiceUnavailable,
                "tenant runtime projection unavailable",
            )
            .build_error());
        }
        tracing::warn!(
            tenant_id,
            reason,
            policy = policy.as_str(),
            "message rejected: tenant unavailable"
        );
        Err(ErrorBuilder::new(ErrorCode::PermissionDenied, "TENANT_UNAVAILABLE").build_error())
    }

    /// 摄入幂等 key：
    /// - 有可信设备上下文：`idem:{tenant}:{sender}:device:{device}:{conversation}:{client_msg_id}`。
    /// - 无设备上下文：`idem:{tenant}:{sender}:{conversation}:{client_msg_id}`。
    ///
    /// client_msg_id 为空 → 无法去重 → None。
    fn idempotency_key(
        tenant_id: &str,
        device_id: Option<&str>,
        message: &flare_proto::common::Message,
    ) -> Option<String> {
        let client_msg_id = message.client_msg_id.trim();
        if client_msg_id.is_empty() {
            return None;
        }
        let conversation_id = message.conversation_id.trim();
        let sender_id = message.sender_id.trim();
        if let Some(device_id) = device_id.map(str::trim).filter(|id| !id.is_empty()) {
            return Some(format!(
                "idem:{tenant_id}:{sender_id}:device:{device_id}:{conversation_id}:{client_msg_id}"
            ));
        }
        Some(format!(
            "idem:{tenant_id}:{sender_id}:{conversation_id}:{client_msg_id}"
        ))
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

    fn validate_batch_send_size(batch_size: usize) -> Result<()> {
        if batch_size > MAX_BATCH_SEND_SIZE {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "batch_send_message batch size exceeds limit",
            )
            .param("batch_size", batch_size.to_string())
            .param("max_batch_size", MAX_BATCH_SEND_SIZE.to_string())
            .build_error());
        }
        Ok(())
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
    /// 3. 准备消息提交（不分配序列号）
    /// 4. 确保会话存在
    /// 5. 消息装饰
    /// 6. 分配序列号
    /// 7. 计算持久化模式
    /// 8. 写入最终消息 WAL
    /// 9. 推送消息
    /// 10. broker 接受后清理 WAL
    /// 11. 执行 PostSend Hook
    // The command carries the message body; never let `#[instrument]` record it.
    #[instrument(skip_all, fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        conversation_id = %cmd.conversation_id,
        client_msg_id = %cmd.message.client_msg_id,
        message_type = cmd.message.message_type,
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

        // A revoked tenant must not bypass admission through idempotency replay.
        // Run the tenant gate before limiter, WAL, hooks and every dedup path.
        // 租户只来自已验证的上下文；缺失即拒绝，不再按租户 "0" 处理（显式的 "0" 照常）。
        let tenant_id = ctx
            .tenant_id()
            .filter(|tenant_id| !tenant_id.trim().is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                ErrorBuilder::new(
                    ErrorCode::InvalidParameter,
                    "tenant required to send a message",
                )
                .build_error()
            })?;
        let tenant_lookup = self.tenant_runtime.lookup(&tenant_id).await;
        Self::tenant_gate(&tenant_id, &tenant_lookup, self.tenant_policy)?;
        // 摄入边界幂等：按 client_msg_id 去重重发，闭合"首发成功但 ACK 丢失 → SDK 重试"导致的重复 echo + seq 空洞。
        let device_id = ctx.device_id().filter(|device_id| !device_id.is_empty());
        let idem_key = self
            .idempotency
            .as_ref()
            .and_then(|_| Self::idempotency_key(&tenant_id, device_id, &cmd.message));

        let mut reservation = None;
        if let (Some(store), Some(key)) = (self.idempotency.as_ref(), idem_key.as_ref()) {
            let idempotency_start = Instant::now();
            match store.begin(key).await? {
                crate::domain::repository::IdempotencyBegin::Replay(record) => {
                    // 真正的重试：返回首发的 (server_id, conversation_seq)，绝不再分配 seq / 再投递。
                    self.metrics.observe_send_stage(
                        "idempotency",
                        "replay",
                        idempotency_start.elapsed(),
                    );
                    return Ok(SendMessageOutcome {
                        message_id: record.server_id,
                        conversation_seq: record.conversation_seq,
                        durability: record.durability,
                    });
                }
                crate::domain::repository::IdempotencyBegin::InFlight => {
                    self.metrics.observe_send_stage(
                        "idempotency",
                        "inflight",
                        idempotency_start.elapsed(),
                    );
                    return Err(ErrorBuilder::new(
                        ErrorCode::ServiceUnavailable,
                        "duplicate send in progress; retry shortly",
                    )
                    .build_error());
                }
                crate::domain::repository::IdempotencyBegin::Fresh => {
                    self.metrics.observe_send_stage(
                        "idempotency",
                        "fresh",
                        idempotency_start.elapsed(),
                    );
                    // 从这里起每条退出路径都要释放或提交占位，包括被取消（见守卫说明）。
                    reservation = Some(IdempotencyReservation::new(Arc::clone(store), key));
                }
            }
        }

        if let Some(limiter) = self.send_rate_limiter.as_ref() {
            let rate_limit_start = Instant::now();
            let tenant_send_qps = tenant_lookup.core_quota().send_qps;
            if let Err(error) = limiter.check(&tenant_id, tenant_send_qps, &cmd.message) {
                self.metrics.observe_send_stage(
                    "rate_limit",
                    "rejected",
                    rate_limit_start.elapsed(),
                );
                return Err(error);
            }
            self.metrics
                .observe_send_stage("rate_limit", "accepted", rate_limit_start.elapsed());
        }

        let result = self
            .ingest_send_inner(ctx, cmd, &tenant_id, reservation.as_ref())
            .await;

        if let Some(reservation) = reservation {
            reservation.settle(&result).await;
        }

        result
    }

    async fn ingest_send_inner(
        &self,
        ctx: &Ctx,
        cmd: SendMessageCommand,
        tenant_id: &str,
        reservation: Option<&IdempotencyReservation>,
    ) -> Result<SendMessageOutcome> {
        let tenant_id = tenant_id.to_string();
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
        // Social Bridge system/notification messages have a verified Service
        // provenance. They do not represent a user pre-send action; the Social
        // PreSend hook only accepts signed User callers. Never infer this
        // bypass from message.source or attributes supplied by the client.
        let execute_user_pre_send = !ctx
            .get_data::<VerifiedContext>()
            .and_then(|verified| verified.claims())
            .is_some_and(|claims| {
                claims.actor_type == SignedActorType::Service
                    && (claims.actor_id == "flare-social-bridge"
                        || flare_im_service_kit::platform_send::is_platform_system_sender(claims))
            });
        let hook_context = self
            .measure_stage(
                "pre_send_hook",
                self.extension_orchestrator
                    .execute_pre_send(ctx, message, execute_user_pre_send),
            )
            .await?;

        // PreSend Hook 可以整体改写 attributes：改出别的租户声明即拒绝。落库租户只认消息头，
        // 但带着别的租户声明的消息不该再往下游走。
        ensure_tenant_attributes_match(&hook_context.message, &tenant_id)?;

        // 3. 准备消息提交。此阶段只填充默认值/server_id/timeline，不分配 seq；
        // seq 在 ensure/decorate 成功后再分配，减少失败路径制造会话序列空洞。
        let mut submission = self
            .measure_stage(
                "prepare",
                self.message_ingest_service
                    .prepare_submission(hook_context.message),
            )
            .await?;

        // 4. 确保会话存在（Social SyncSignal 内部路由 `sync:{owner}` 仅在线推送，不落会话表）
        if !flare_im_contracts::constants::sync_inbox::is_sync_inbox_conversation_id(
            &submission.message.conversation_id,
        ) {
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

        // 6. 分配会话序列号。必须在 WAL/MQ 前完成，但应晚于可能失败且不需要 seq 的前置阶段。
        let (submission, profile) = self
            .measure_stage(
                "allocate_seq",
                self.message_ingest_service
                    .allocate_seq_for_submission(ctx, &tenant_id, submission),
            )
            .await?;

        // 7. 计算最终持久化模式。WAL 必须在最终消息成形之后写入，便于失败恢复重放。
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

        // 8. 写入 WAL。只要进入持久化主队列，就先落 WAL；push-only 不承诺离线恢复。
        self.measure_stage(
            "wal_write",
            self.message_ingest_service.write_wal_if_needed(
                ctx,
                &submission,
                &profile,
                persistence_mode,
                &tenant_id,
            ),
        )
        .await?;

        let durability = send_ack_durability(&profile, &persistence_mode);
        // WAL 已写入：从这里起显式路径一定回成功（发布失败也回 WalAccepted，靠重放恢复），
        // 结果已确定。此后被取消，占位按这个结果提交，重发拿回同一个 server_id 与 seq。
        if let (Some(reservation), Some(outcome)) = (
            reservation,
            recoverable_publish_failure_outcome(&submission, durability),
        ) {
            reservation.mark_accepted(&outcome);
        }

        // 9. 推送消息
        let publish_result = self
            .measure_stage(
                "mq_publish",
                apply_stage_timeout(
                    "mq_publish",
                    self.send_publish_timeout,
                    self.message_ingest_service.push_message(
                        ctx,
                        &submission,
                        &profile,
                        persistence_mode,
                    ),
                ),
            )
            .await;
        if let Err(error) = publish_result {
            if let Some(outcome) = recoverable_publish_failure_outcome(&submission, durability) {
                tracing::warn!(
                    message_id = %submission.message_id,
                    conversation_id = %submission.message.conversation_id,
                    error = %error,
                    "MQ publish failed after WAL append; returning WAL accepted and relying on replay"
                );
                return Ok(outcome);
            }
            return Err(error);
        }
        let outcome = SendMessageOutcome {
            message_id: submission.message_id.clone(),
            conversation_seq: submission.message.conversation_seq,
            durability,
        };
        if let Some(reservation) = reservation {
            reservation.mark_accepted(&outcome);
        }

        // 10. broker 已确认接受后，主队列成为恢复来源，可以清理发送侧 WAL。
        // 清理失败只会增加一次未来重放/去重成本，不能让已被 broker 接受的发送变成失败。
        if durability != SendAckDurability::TransientAccepted {
            self.cleanup_wal_after_broker_accept(submission.clone())
                .await;
        } else {
            self.observe_skipped_stage("wal_cleanup");
        }

        // 11. 执行 PostSend Hook（经统一扩展编排器）
        if let Err(error) = self
            .measure_stage(
                "post_send_hook",
                self.extension_orchestrator.execute_post_send(
                    ctx,
                    &submission,
                    &hook_context.hook_context,
                ),
            )
            .await
        {
            tracing::warn!(
                message_id = %submission.message_id,
                conversation_id = %submission.message.conversation_id,
                error = %error,
                "Post-send hook failed after message acceptance; preserving send outcome"
            );
        }

        Ok(outcome)
    }

    /// 发送系统消息
    #[instrument(skip_all, fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        conversation_id = %cmd.conversation_id,
        client_msg_id = %cmd.message.client_msg_id,
        message_type = cmd.message.message_type,
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

    #[instrument(skip_all, fields(batch_size = messages.len()))]
    pub async fn batch_send_message(
        &self,
        ctx: &Ctx,
        messages: Vec<SendMessageCommand>,
    ) -> Result<Vec<Result<SendMessageOutcome>>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }
        Self::validate_batch_send_size(messages.len())?;

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

    #[instrument(skip_all, fields(data_len = data.len()))]
    pub async fn send_custom_data(&self, ctx: &Ctx, data: Vec<u8>) -> Result<()> {
        let _ = (ctx, data);
        Ok(())
    }
}

/// 消息体 attributes 中声明租户的键（入口的签名校验同样检查这两个键）。
const MESSAGE_TENANT_ATTRIBUTES: [&str; 2] = ["tenant_id", "x-tenant-id"];

/// attributes 里的租户声明必须与发送上下文的租户一致：Storage Writer 曾按 `tenant_id`
/// 选择落库租户，PreSend Hook 又能整体改写 attributes（W11-T）。
fn ensure_tenant_attributes_match(
    message: &flare_proto::common::Message,
    tenant_id: &str,
) -> Result<()> {
    for key in MESSAGE_TENANT_ATTRIBUTES {
        if message
            .attributes
            .get(key)
            .is_some_and(|declared| declared != tenant_id)
        {
            return Err(ErrorBuilder::new(
                ErrorCode::PermissionDenied,
                "message attributes declare another tenant",
            )
            .param("attribute", key)
            .build_error());
        }
    }
    Ok(())
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

async fn apply_stage_timeout<T, F>(
    stage: &'static str,
    timeout: Option<Duration>,
    future: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    let Some(timeout) = timeout else {
        return future.await;
    };
    match tokio::time::timeout(timeout, future).await {
        Ok(result) => result,
        Err(_) => Err(stage_timeout_error(stage, timeout)),
    }
}

fn stage_timeout_error(stage: &'static str, timeout: Duration) -> FlareError {
    ErrorBuilder::new(ErrorCode::OperationTimeout, "message send stage timed out")
        .param("stage", stage)
        .param("timeout_ms", timeout.as_millis().to_string())
        .build_error()
}

/// 摄入幂等占位的守卫：`begin` 占位成功后，保证每条退出路径都释放或提交占位。
///
/// 发送处理可能在提交或回滚之前被取消：调用方断开、上游超时、停机时入口与本服务一起停止
/// （W6-S2 F3）。占位不释放的话，同一 `client_msg_id` 的重发在占位 TTL（默认 180 s）内都得到
/// `duplicate send in progress`。守卫被丢弃时若还没结算，就按显式路径在同一阶段会做的收尾：
/// 已经到达确定结果的（WAL 已写入，或已发布）提交该结果，重发拿回同一个 server_id 与 seq；
/// 否则回滚，重发重新处理，与显式失败相同。丢弃时只能另起任务做 Redis 调用，进程同时退出时
/// 这次收尾可能来不及，占位就留到 TTL 到期。
struct IdempotencyReservation {
    store: Arc<dyn crate::domain::repository::IngestIdempotencyStore>,
    key: String,
    state: std::sync::Mutex<ReservationState>,
}

enum ReservationState {
    /// 已占位，还没有确定的结果：取消时回滚。
    Reserved,
    /// 结果已确定：取消时提交它。
    Accepted(crate::domain::repository::IdempotentRecord),
    /// 已显式结算完毕。
    Settled,
}

impl IdempotencyReservation {
    fn new(store: Arc<dyn crate::domain::repository::IngestIdempotencyStore>, key: &str) -> Self {
        Self {
            store,
            key: key.to_string(),
            state: std::sync::Mutex::new(ReservationState::Reserved),
        }
    }

    fn replace_state(&self, next: ReservationState) -> ReservationState {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::mem::replace(&mut *state, next)
    }

    /// 发送已到达确定结果：此后被取消就提交它。
    fn mark_accepted(&self, outcome: &SendMessageOutcome) {
        self.replace_state(ReservationState::Accepted(record_of(outcome)));
    }

    /// 显式结算：成功提交结果，失败回滚。结算途中被取消，由 `Drop` 重做同一件事。
    async fn settle(self, result: &Result<SendMessageOutcome>) {
        match result {
            Ok(outcome) => {
                let record = record_of(outcome);
                self.replace_state(ReservationState::Accepted(record.clone()));
                if let Err(error) = self.store.commit(&self.key, &record).await {
                    // commit 失败只削弱后续去重能力，不影响本次已 durable 的发送。
                    tracing::warn!(key = %self.key, error = %error, "ingest idempotency commit failed");
                }
            }
            // 失败：清除占位，让后续重试可干净地重新处理（否则重试会被误判为 InFlight 直到 TTL）。
            Err(_) => {
                self.replace_state(ReservationState::Reserved);
                let _ = self.store.rollback(&self.key).await;
            }
        }
        self.replace_state(ReservationState::Settled);
    }
}

impl Drop for IdempotencyReservation {
    fn drop(&mut self) {
        let state = self.replace_state(ReservationState::Settled);
        if matches!(state, ReservationState::Settled) {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(
                key = %self.key,
                "send cancelled outside a runtime; idempotency reservation kept until its TTL"
            );
            return;
        };
        let store = Arc::clone(&self.store);
        let key = std::mem::take(&mut self.key);
        runtime.spawn(async move {
            match state {
                ReservationState::Accepted(record) => {
                    tracing::info!(
                        key = %key,
                        message_id = %record.server_id,
                        "send cancelled after acceptance; committing its idempotency result"
                    );
                    if let Err(error) = store.commit(&key, &record).await {
                        tracing::warn!(key = %key, error = %error, "ingest idempotency commit failed");
                    }
                }
                ReservationState::Reserved => {
                    tracing::info!(
                        key = %key,
                        "send cancelled before acceptance; releasing its idempotency reservation"
                    );
                    if let Err(error) = store.rollback(&key).await {
                        tracing::warn!(key = %key, error = %error, "ingest idempotency rollback failed");
                    }
                }
                ReservationState::Settled => {}
            }
        });
    }
}

fn record_of(outcome: &SendMessageOutcome) -> crate::domain::repository::IdempotentRecord {
    crate::domain::repository::IdempotentRecord {
        server_id: outcome.message_id.clone(),
        conversation_seq: outcome.conversation_seq,
        durability: outcome.durability,
    }
}

fn send_ack_durability(
    profile: &crate::domain::MessageProfile,
    persistence_mode: &PersistenceMode,
) -> SendAckDurability {
    if persistence_mode.should_push_only(profile.is_temporary()) {
        SendAckDurability::TransientAccepted
    } else {
        // 统一单写模型：消息已写入 WAL + 分配 conversation_seq + 发布到 JetStream 持久主队列，
        // 即"提交点"已 durable（storage-writer 必定消费落库）。直接同步回 Persisted 作为发送方
        // 权威持久化确认——发送方无需再等"自己的消息穿过整条 fanout 回流"(根治 116s 尾延迟)。
        SendAckDurability::Persisted
    }
}

fn recoverable_publish_failure_outcome(
    submission: &crate::domain::model::MessageSubmission,
    durability: SendAckDurability,
) -> Option<SendMessageOutcome> {
    if durability == SendAckDurability::TransientAccepted {
        return None;
    }

    Some(SendMessageOutcome {
        message_id: submission.message_id.clone(),
        conversation_seq: submission.message.conversation_seq,
        durability: SendAckDurability::WalAccepted,
    })
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

    #[test]
    fn batch_send_size_accepts_configured_limit() {
        MessageIngestHandler::validate_batch_send_size(MAX_BATCH_SEND_SIZE)
            .expect("limit should be accepted");
    }

    #[test]
    fn batch_send_size_rejects_oversized_request() {
        let err = MessageIngestHandler::validate_batch_send_size(MAX_BATCH_SEND_SIZE + 1)
            .expect_err("oversized batch should fail");

        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
        assert!(err.reason().contains("batch size"));
    }

    #[test]
    fn idempotency_key_is_scoped_by_conversation() {
        let mut first = flare_proto::common::Message {
            conversation_id: "conv-a".to_string(),
            sender_id: "sender-1".to_string(),
            client_msg_id: "client-1".to_string(),
            ..Default::default()
        };
        let mut second = first.clone();
        second.conversation_id = "conv-b".to_string();

        assert_eq!(
            MessageIngestHandler::idempotency_key("tenant-1", None, &first).as_deref(),
            Some("idem:tenant-1:sender-1:conv-a:client-1")
        );
        assert_ne!(
            MessageIngestHandler::idempotency_key("tenant-1", None, &first),
            MessageIngestHandler::idempotency_key("tenant-1", None, &second)
        );

        first.client_msg_id = " ".to_string();
        assert_eq!(
            MessageIngestHandler::idempotency_key("tenant-1", None, &first),
            None
        );
    }

    #[test]
    fn idempotency_key_is_scoped_by_trusted_device_when_available() {
        let message = flare_proto::common::Message {
            conversation_id: "conv-a".to_string(),
            sender_id: "sender-1".to_string(),
            client_msg_id: "client-1".to_string(),
            ..Default::default()
        };

        assert_eq!(
            MessageIngestHandler::idempotency_key("tenant-1", Some("device-a"), &message)
                .as_deref(),
            Some("idem:tenant-1:sender-1:device:device-a:conv-a:client-1")
        );
        assert_ne!(
            MessageIngestHandler::idempotency_key("tenant-1", Some("device-a"), &message),
            MessageIngestHandler::idempotency_key("tenant-1", Some("device-b"), &message)
        );
        assert_eq!(
            MessageIngestHandler::idempotency_key("tenant-1", Some("  "), &message).as_deref(),
            Some("idem:tenant-1:sender-1:conv-a:client-1")
        );
    }

    #[test]
    fn durable_publish_failure_returns_wal_accepted_outcome() {
        let submission = crate::domain::model::MessageSubmission {
            message: flare_proto::common::Message {
                conversation_id: "conv-1".to_string(),
                conversation_seq: 42,
                ..Default::default()
            },
            message_id: "msg-1".to_string(),
            timeline: Default::default(),
        };

        let outcome =
            recoverable_publish_failure_outcome(&submission, SendAckDurability::Persisted)
                .expect("durable messages should recover through WAL replay");

        assert_eq!(outcome.message_id, "msg-1");
        assert_eq!(outcome.conversation_seq, 42);
        assert_eq!(outcome.durability, SendAckDurability::WalAccepted);
    }

    #[test]
    fn transient_publish_failure_is_not_recovered_by_wal() {
        let submission = crate::domain::model::MessageSubmission {
            message: flare_proto::common::Message {
                conversation_id: "conv-1".to_string(),
                conversation_seq: 7,
                ..Default::default()
            },
            message_id: "msg-1".to_string(),
            timeline: Default::default(),
        };

        assert!(
            recoverable_publish_failure_outcome(&submission, SendAckDurability::TransientAccepted)
                .is_none()
        );
    }

    fn known(status: flare_im_contracts::domain::tenant::TenantStatus) -> TenantLookup {
        TenantLookup::Known(Arc::new(
            flare_im_contracts::domain::tenant::TenantRuntimeSnapshot {
                tenant_id: "tenant-a".to_string(),
                status,
                quota: Default::default(),
                version: 1,
            },
        ))
    }

    /// 停用 / 删除中的租户、严格策略下没有投影的租户：确定的租户结论，`TENANT_UNAVAILABLE`。
    #[test]
    fn tenant_gate_rejects_unavailable_tenants_with_a_typed_reason() {
        use flare_im_contracts::domain::tenant::TenantStatus;
        for (lookup, policy) in [
            (known(TenantStatus::Suspended), TenantAccessPolicy::Lenient),
            (known(TenantStatus::Suspended), TenantAccessPolicy::Strict),
            (known(TenantStatus::Deleting), TenantAccessPolicy::Strict),
            (TenantLookup::Unknown, TenantAccessPolicy::Strict),
        ] {
            let err = MessageIngestHandler::tenant_gate("tenant-a", &lookup, policy)
                .expect_err("unavailable tenant must be rejected");
            assert_eq!(err.code(), Some(ErrorCode::PermissionDenied), "{policy:?}");
            assert_eq!(err.reason(), "TENANT_UNAVAILABLE", "{policy:?}");
        }
    }

    /// 严格策略下投影读不到：仍然拒绝，但按可重试的服务不可用答，不是 `TENANT_UNAVAILABLE`——
    /// 后者经 API 网关是确定的 403，客户端会当成租户被停用。
    #[test]
    fn tenant_gate_reports_a_projection_outage_as_retryable() {
        let err = MessageIngestHandler::tenant_gate(
            "tenant-a",
            &TenantLookup::Unavailable,
            TenantAccessPolicy::Strict,
        )
        .expect_err("strict policy fails closed while the projection is unreadable");

        assert_eq!(err.code(), Some(ErrorCode::ServiceUnavailable));
        assert!(
            !err.reason().contains("TENANT_UNAVAILABLE"),
            "{}",
            err.reason()
        );
    }

    #[test]
    fn tenant_attributes_must_match_the_send_tenant() {
        let mut message = flare_proto::common::Message::default();
        assert!(ensure_tenant_attributes_match(&message, "tenant-a").is_ok());
        for key in MESSAGE_TENANT_ATTRIBUTES {
            message.attributes.clear();
            message.attributes.insert(key.into(), "tenant-a".into());
            assert!(ensure_tenant_attributes_match(&message, "tenant-a").is_ok());
            message.attributes.insert(key.into(), "tenant-b".into());
            let err = ensure_tenant_attributes_match(&message, "tenant-a").unwrap_err();
            assert_eq!(err.code(), Some(ErrorCode::PermissionDenied), "{key}");
        }
        message.attributes.clear();
        message.attributes.insert("tenant_id".into(), "0".into());
        assert!(ensure_tenant_attributes_match(&message, "0").is_ok());
    }

    #[test]
    fn tenant_gate_admits_active_and_lenient_cases() {
        use flare_im_contracts::domain::tenant::TenantStatus;
        for (lookup, policy) in [
            (known(TenantStatus::Active), TenantAccessPolicy::Strict),
            (TenantLookup::Unknown, TenantAccessPolicy::Lenient),
            (TenantLookup::Unavailable, TenantAccessPolicy::Lenient),
        ] {
            MessageIngestHandler::tenant_gate("tenant-a", &lookup, policy)
                .unwrap_or_else(|err| panic!("{policy:?}: {err}"));
        }
    }

    #[tokio::test]
    async fn stage_timeout_returns_operation_timeout() {
        let result: Result<()> =
            apply_stage_timeout("mq_publish", Some(Duration::from_millis(1)), async {
                tokio::time::sleep(Duration::from_millis(25)).await;
                Ok(())
            })
            .await;

        let err = result.expect_err("slow stage should time out");
        assert_eq!(err.code(), Some(ErrorCode::OperationTimeout));
        assert!(err.reason().contains("timed out"));
    }
}
