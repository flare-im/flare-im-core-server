//! 事件领域服务 - 重构版
//!
//! ## 核心职责
//! 1. 事件校验（使用策略模式）
//! 2. 序列号分配（使用公共 SequenceAllocator）
//! 3. 事件推送（使用 PushRepository）
//!
//! ## 支持的事件类型
//! - 消息撤回 (EVENT_MESSAGE_RECALL)
//! - 消息编辑 (EVENT_MESSAGE_EDIT)
//! - 消息删除 (EVENT_MESSAGE_DELETE)
//! - 已读回执 (EVENT_READ_RECEIPT)
//! - 表情反应 (EVENT_REACTION)
//! - 置顶/取消置顶 (EVENT_PIN / EVENT_UNPIN)
//! - 标记/取消标记 (EVENT_MARK / EVENT_UNMARK)
//! - 自定义事件 (EVENT_CUSTOM)
//!
//! Realtime control packets such as typing, presence, and RTC signaling are not durable events.

use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_message_pipeline::{MqPushRepository, PushRepository};
use flare_im_seq::SequenceAllocator;
use flare_proto::common::event::Payload as EventPayload;
use flare_proto::common::{Event, EventType};
use flare_server_core::{flare_err, flare_err_details};
use tracing::instrument;

use crate::domain::PersistenceMode;
use crate::domain::repository::{
    ConversationMembershipRepository, MessageTargetRepository, ReadWatermarkAdvance,
    ReadWatermarkRepository, RecipientRepository, UserSyncCompensationRepository,
    UserSyncCompensationTask, UserSyncIndexRepository,
};
use crate::domain::service::message_operation_policy::MessageOperationPolicy;
use flare_im_message_pipeline::{
    CompositeEventValidationStrategy, EventValidationStrategy, ValidationContext,
};
use flare_server_core::error::{ErrorCode, Result};

/// 事件领域服务
pub trait SequenceAllocatorPort: Send + Sync {
    async fn allocate_seq(&self, conversation_id: &str, tenant_id: &str) -> Result<u64>;
}

impl SequenceAllocatorPort for SequenceAllocator {
    async fn allocate_seq(&self, conversation_id: &str, tenant_id: &str) -> Result<u64> {
        SequenceAllocator::allocate_seq(self, conversation_id, tenant_id).await
    }
}

/// Admission decision for an event, taken after validation and before a conversation
/// seq is allocated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventAdmission {
    /// Sequence and fan out the event. `read_claim` is set when the event raised its
    /// reader's watermark; release it if the event never goes out.
    Admit {
        read_claim: Option<ReadWatermarkClaim>,
    },
    /// A read receipt at or below its reader's watermark: it carries nothing new, so it
    /// gets no seq and no fanout.
    StaleReadReceipt { read_seq: u64, watermark: u64 },
}

/// Watermark raise made on behalf of one admitted read receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadWatermarkClaim {
    conversation_id: String,
    user_id: String,
    read_seq: u64,
    previous: u64,
}

pub struct EventDomainService<PR = MqPushRepository, SA = SequenceAllocator>
where
    PR: PushRepository,
    SA: SequenceAllocatorPort,
{
    /// 推送仓储（使用具体类型以支持 async fn in traits）
    push_repository: Arc<PR>,
    /// 接收者仓储
    recipient_repository: Arc<dyn RecipientRepository>,
    /// 序列号分配器
    sequence_allocator: Arc<SA>,
    /// 事件校验策略
    validation_strategy: Arc<dyn EventValidationStrategy>,
    user_sync_index: Option<Arc<dyn UserSyncIndexRepository>>,
    user_sync_compensation: Option<Arc<dyn UserSyncCompensationRepository>>,
    read_watermark: Option<Arc<dyn ReadWatermarkRepository>>,
    /// Who may do what to which message (see [`MessageOperationPolicy`]).
    operation_policy: Arc<MessageOperationPolicy>,
    /// What the operation policy is built from.
    message_targets: Option<Arc<dyn MessageTargetRepository>>,
    conversation_members: Option<Arc<dyn ConversationMembershipRepository>>,
}

impl<PR, SA> EventDomainService<PR, SA>
where
    PR: PushRepository,
    SA: SequenceAllocatorPort,
{
    pub fn new(
        push_repository: Arc<PR>,
        recipient_repository: Arc<dyn RecipientRepository>,
        sequence_allocator: Arc<SA>,
        validation_strategy: Option<Arc<dyn EventValidationStrategy>>,
    ) -> Self {
        Self {
            push_repository,
            recipient_repository,
            sequence_allocator,
            validation_strategy: validation_strategy
                .unwrap_or_else(|| Arc::new(CompositeEventValidationStrategy::default_composite())),
            user_sync_index: None,
            user_sync_compensation: None,
            read_watermark: None,
            operation_policy: Arc::new(MessageOperationPolicy::default()),
            message_targets: None,
            conversation_members: None,
        }
    }

    fn rebuild_operation_policy(mut self) -> Self {
        self.operation_policy = Arc::new(
            MessageOperationPolicy::new(self.message_targets.clone())
                .with_members(self.conversation_members.clone()),
        );
        self
    }

    /// The stored messages operations are checked against. Without them, operations that name a
    /// message are refused (retryable).
    pub fn with_message_targets(mut self, targets: Arc<dyn MessageTargetRepository>) -> Self {
        self.message_targets = Some(targets);
        self.rebuild_operation_policy()
    }

    /// The conversation membership user events are checked against. Without it, every user event
    /// is refused (retryable).
    pub fn with_conversation_members(
        mut self,
        members: Arc<dyn ConversationMembershipRepository>,
    ) -> Self {
        self.conversation_members = Some(members);
        self.rebuild_operation_policy()
    }

    /// Replace the operation policy (tests: a fixed clock).
    pub fn with_operation_policy(mut self, policy: MessageOperationPolicy) -> Self {
        self.operation_policy = Arc::new(policy);
        self
    }

    /// Authorize an event's operation and return it normalized (see [`MessageOperationPolicy`]).
    pub async fn authorize_operation(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        event: Event,
    ) -> Result<Event> {
        self.operation_policy.authorize(ctx, tenant_id, event).await
    }

    pub fn with_user_sync_index(
        mut self,
        user_sync_index: Arc<dyn UserSyncIndexRepository>,
    ) -> Self {
        self.user_sync_index = Some(user_sync_index);
        self
    }

    pub fn with_user_sync_compensation(
        mut self,
        user_sync_compensation: Arc<dyn UserSyncCompensationRepository>,
    ) -> Self {
        self.user_sync_compensation = Some(user_sync_compensation);
        self
    }

    /// Gate client read acks on the reader's watermark. Without it every read receipt
    /// is sequenced, including repeats of a position the reader already reported.
    pub fn with_read_watermark(mut self, read_watermark: Arc<dyn ReadWatermarkRepository>) -> Self {
        self.read_watermark = Some(read_watermark);
        self
    }

    /// 校验事件
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `tenant_id`: 租户 ID
    /// - `event`: 事件
    ///
    /// # 返回
    /// - `Ok(())`: 校验通过
    /// - `Err`: 校验失败
    #[instrument(skip(self), fields(
        event_id = %event.event_id,
        conversation_id = %event.conversation_id,
    ))]
    pub async fn validate_event(&self, ctx: &Ctx, tenant_id: &str, event: &Event) -> Result<()> {
        let validation_context = ValidationContext {
            ctx,
            tenant_id,
            conversation_id: &event.conversation_id,
        };

        let validation_result = self
            .validation_strategy
            .validate(&validation_context, event)
            .await
            .map_err(|e| {
                flare_err!(
                    ErrorCode::InvalidParameter,
                    &format!("Event validation failed: {}", e)
                )
            })?;

        if !validation_result.is_valid {
            return Err(flare_err_details!(
                ErrorCode::InvalidParameter,
                "Event validation failed",
                format!("{:?}", validation_result.errors)
            ));
        }

        Ok(())
    }

    /// Decide whether an event earns a conversation seq.
    ///
    /// Only cursor read receipts are gated: one whose `read_seq` does not raise its
    /// reader's watermark is a repeat (a resend, or another device of the same user
    /// reporting the same position) and is dropped before it costs a seq and a fanout
    /// to every member. The watermark is keyed per reader, so one member's position
    /// never suppresses another member's receipt. A watermark failure admits the event:
    /// losing the dedupe for a moment is cheaper than losing a real read position.
    pub async fn admit_event(&self, tenant_id: &str, event: &Event) -> EventAdmission {
        let (Some(read_watermark), Some((user_id, read_seq))) =
            (&self.read_watermark, cursor_read_receipt(event))
        else {
            return EventAdmission::Admit { read_claim: None };
        };

        match read_watermark
            .advance(tenant_id, &event.conversation_id, user_id, read_seq)
            .await
        {
            Ok(ReadWatermarkAdvance::Advanced { previous }) => EventAdmission::Admit {
                read_claim: Some(ReadWatermarkClaim {
                    conversation_id: event.conversation_id.clone(),
                    user_id: user_id.to_string(),
                    read_seq,
                    previous,
                }),
            },
            Ok(ReadWatermarkAdvance::NotAdvanced { current }) => EventAdmission::StaleReadReceipt {
                read_seq,
                watermark: current,
            },
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    conversation_id = %event.conversation_id,
                    read_seq,
                    "read watermark unavailable; admitting read receipt without dedupe"
                );
                EventAdmission::Admit { read_claim: None }
            }
        }
    }

    /// Hand back a watermark raise whose event was never sequenced or fanned out, so
    /// the client's retry of the same position is admitted.
    pub async fn release_read_claim(&self, tenant_id: &str, claim: ReadWatermarkClaim) {
        let Some(read_watermark) = &self.read_watermark else {
            return;
        };
        if let Err(error) = read_watermark
            .rollback(
                tenant_id,
                &claim.conversation_id,
                &claim.user_id,
                claim.read_seq,
                claim.previous,
            )
            .await
        {
            tracing::warn!(
                error = %error,
                conversation_id = %claim.conversation_id,
                read_seq = claim.read_seq,
                "read watermark rollback failed; a retry of this read position will be skipped"
            );
        }
    }

    /// 分配序列号
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `tenant_id`: 租户 ID
    /// - `event`: 事件
    ///
    /// # 返回
    /// - `Ok(event_with_seq)`: 分配序列号后的事件
    /// - `Err`: 错误
    #[instrument(skip(self), fields(
        event_id = %event.event_id,
        conversation_id = %event.conversation_id,
    ))]
    pub async fn allocate_seq(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        mut event: Event,
    ) -> Result<Event> {
        let session_seq = self
            .sequence_allocator
            .allocate_seq(&event.conversation_id, tenant_id)
            .await
            .map_err(|e| {
                flare_err!(
                    ErrorCode::InternalError,
                    &format!(
                        "allocate seq failed for conversation_id={}: {}",
                        event.conversation_id, e
                    )
                )
            })?;

        tracing::trace!(
            conversation_id = %event.conversation_id,
            seq = session_seq,
            "Allocated session sequence for event"
        );

        event.conversation_seq = session_seq;
        Ok(event)
    }

    /// 获取接收者用户 ID 列表
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `event`: 事件
    ///
    /// # 返回
    /// - `Ok(recipient_user_ids)`: 接收者用户 ID 列表
    /// - `Err`: 错误
    #[instrument(skip(self), fields(
        conversation_id = %event.conversation_id,
    ))]
    pub async fn get_recipient_user_ids(&self, ctx: &Ctx, event: &Event) -> Result<Vec<String>> {
        if let Some(recipients) = resolve_recipients_from_event_payload(event) {
            return Ok(recipients);
        }

        // 成员制（大）会话统一读扩散：永不物化收件人 → 空列表，投递走会话级广播、
        // 变更发现走会话级 version（见 persist_and_push_with_recipients 的 version bump）。
        // 与消息路径 needs_member_lookup 语义一致，避免十万群事件/已读回执做 O(成员) 成员拉取与 per-member 写。
        if is_member_based_conversation(&event.conversation_id) {
            return Ok(Vec::new());
        }

        let event_type = EventType::try_from(event.r#type).unwrap_or(EventType::Unspecified);
        let mut members = self
            .recipient_repository
            .get_conversation_members(ctx, &event.conversation_id)
            .await
            .map_err(|e| {
                flare_err!(
                    ErrorCode::InternalError,
                    &format!("Failed to get conversation members for event: {}", e)
                )
            })?;

        if event_type == EventType::EventReadReceipt
            && let Some(EventPayload::Read(r)) = &event.payload
        {
            members.retain(|uid| uid != &r.user_id);
        }

        Ok(members)
    }

    /// 仅推送事件（不持久化），由服务内部自动解析接收者
    ///
    /// 用于临时实时事件（如输入态、presence、通话信令）等场景。
    #[instrument(skip(self), fields(
        event_id = %event.event_id,
        conversation_id = %event.conversation_id,
        event_type = ?EventType::try_from(event.r#type),
    ))]
    pub async fn push_only(&self, ctx: &Ctx, event: Event) -> Result<()> {
        let recipient_user_ids = self.get_recipient_user_ids(ctx, &event).await?;
        self.push_only_with_recipients(ctx, event, recipient_user_ids)
            .await
    }

    /// 仅推送事件（不持久化），接收者由调用方显式提供
    ///
    /// 适用于上游已完成路由决策的场景，避免重复成员查询。
    #[instrument(skip(self, recipient_user_ids), fields(
        event_id = %event.event_id,
        conversation_id = %event.conversation_id,
        event_type = ?EventType::try_from(event.r#type),
        recipient_count = recipient_user_ids.len(),
    ))]
    pub async fn push_only_with_recipients(
        &self,
        ctx: &Ctx,
        event: Event,
        recipient_user_ids: Vec<String>,
    ) -> Result<()> {
        tracing::trace!(
            event_id = %event.event_id,
            conversation_id = %event.conversation_id,
            event_type = ?EventType::try_from(event.r#type),
            conversation_seq = event.conversation_seq,
            recipient_count = recipient_user_ids.len(),
            "Pushing event only (no persistence)"
        );

        let conversation_id = event.conversation_id.clone();
        self.push_repository
            .push_only_event(ctx, event, recipient_user_ids, conversation_id)
            .await
    }

    /// 判断是否为临时事件（仅推送，不持久化）
    ///
    /// Durable event types are persisted by default. Realtime control packets
    /// are modeled outside `Event` and therefore do not enter this classifier.
    fn is_temporary_event(_event_type: EventType) -> bool {
        false
    }

    /// 仅保存事件（持久化但不推送）
    ///
    /// 用于需要持久化但不需要实时推送的场景：
    /// - 事件记录（只需保存记录，不需要推送）
    /// - 审计日志（只需持久化，不需要推送）
    /// - 系统内部事件（只需保存，不需要推送）
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `event`: 事件
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self), fields(
        event_id = %event.event_id,
        conversation_id = %event.conversation_id,
        event_type = ?EventType::try_from(event.r#type),
    ))]
    pub async fn persistence_only(&self, ctx: &Ctx, event: Event) -> Result<()> {
        tracing::trace!(
            event_id = %event.event_id,
            conversation_id = %event.conversation_id,
            event_type = ?EventType::try_from(event.r#type),
            conversation_seq = event.conversation_seq,
            "Persisting event only (no push)"
        );

        let conversation_id = event.conversation_id.clone();
        self.push_repository
            .persistence_only_event(ctx, event, conversation_id)
            .await
    }

    /// 持久事件主流 fanout：先写入存储 topic，再写入推送 topic。
    #[instrument(skip(self, recipient_user_ids), fields(
        event_id = %event.event_id,
        conversation_id = %event.conversation_id,
        event_type = ?EventType::try_from(event.r#type),
        recipient_count = recipient_user_ids.len(),
    ))]
    pub async fn persist_and_push_with_recipients(
        &self,
        ctx: &Ctx,
        event: Event,
        recipient_user_ids: Vec<String>,
    ) -> Result<()> {
        tracing::trace!(
            event_id = %event.event_id,
            conversation_id = %event.conversation_id,
            event_type = ?EventType::try_from(event.r#type),
            conversation_seq = event.conversation_seq,
            recipient_count = recipient_user_ids.len(),
            "Fanout persistent event to storage and push topics"
        );

        let conversation_id = event.conversation_id.clone();
        self.push_repository
            .persistence_only_event(ctx, event.clone(), conversation_id.clone())
            .await?;
        let sync_result = if let Some(user_sync_index) = &self.user_sync_index {
            if is_member_based_conversation(&conversation_id) {
                // 大会话：会话级 version bump（O(1)），客户端靠会话 version 增量发现变更，
                // 不做 O(成员) 的 per-member sync 写——这是消除十万群 read-fanout 瓶颈的核心。
                Some(
                    user_sync_index
                        .record_conversation_version_bump(
                            ctx,
                            &conversation_id,
                            event.conversation_seq,
                            event.created_at,
                        )
                        .await
                        .map(|_| ()),
                )
            } else {
                // 单聊/临时：精确 per-member 写（成员少，O(N) 廉价）。
                Some(
                    user_sync_index
                        .record_conversation_change(
                            ctx,
                            &recipient_user_ids,
                            &conversation_id,
                            event.conversation_seq,
                            event.created_at,
                        )
                        .await,
                )
            }
        } else {
            None
        };
        if let Some(Err(error)) = sync_result {
            tracing::warn!(
                error = %error,
                conversation_id = %conversation_id,
                event_id = %event.event_id,
                conversation_seq = event.conversation_seq,
                recipient_count = recipient_user_ids.len(),
                "User sync index event update deferred; event fanout continues"
            );
            let source_error = error.to_string();
            self.enqueue_user_sync_compensation(
                ctx,
                &recipient_user_ids,
                &conversation_id,
                event.conversation_seq,
                event.created_at,
                &source_error,
            )
            .await;
        }
        self.push_repository
            .push_only_event(ctx, event, recipient_user_ids, conversation_id)
            .await
    }

    async fn enqueue_user_sync_compensation(
        &self,
        ctx: &Ctx,
        recipient_user_ids: &[String],
        conversation_id: &str,
        max_conversation_seq: u64,
        occurred_at_ms: i64,
        source_error: &str,
    ) {
        let Some(repository) = &self.user_sync_compensation else {
            return;
        };
        let Some(task) = UserSyncCompensationTask::eager_user_changes(
            ctx,
            recipient_user_ids,
            conversation_id,
            max_conversation_seq,
            occurred_at_ms,
            UserSyncCompensationTask::due_now_ms(),
        ) else {
            return;
        };
        if let Err(error) = repository.enqueue(task.clone()).await {
            tracing::warn!(
                error = %error,
                source_error = %source_error,
                task_id = %task.task_id,
                conversation_id = %conversation_id,
                "failed to enqueue event user_sync compensation task"
            );
        }
    }

    /// 推送事件
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `event`: 事件
    /// - `persistence_mode`: 持久化模式
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self), fields(
        event_id = %event.event_id,
        conversation_id = %event.conversation_id,
        event_type = ?EventType::try_from(event.r#type),
    ))]
    pub async fn push_event(
        &self,
        ctx: &Ctx,
        event: Event,
        persistence_mode: PersistenceMode,
    ) -> Result<()> {
        let event_type = EventType::try_from(event.r#type).unwrap_or(EventType::Unspecified);
        let is_temporary = Self::is_temporary_event(event_type);
        let should_push_only = persistence_mode.should_push_only(is_temporary);
        if should_push_only {
            return self.push_only(ctx, event).await;
        }

        let recipient_user_ids = self.get_recipient_user_ids(ctx, &event).await?;
        tracing::trace!(
            event_id = %event.event_id,
            conversation_id = %event.conversation_id,
            event_type = ?event.r#type(),
            conversation_seq = event.conversation_seq,
            persistence_mode = ?persistence_mode,
            "Publishing event (persistence + push)"
        );

        self.push_repository
            .publish_event(
                ctx,
                event.clone(),
                recipient_user_ids,
                event.conversation_id.clone(),
            )
            .await
    }
}

/// 从事件载荷解析推送目标，避免不必要的成员列表查询。
fn resolve_recipients_from_event_payload(event: &Event) -> Option<Vec<String>> {
    match &event.payload {
        Some(EventPayload::Read(_)) => None,
        _ => None,
    }
}

/// The reader and position of a read receipt that only moves a read cursor, which is
/// what client read acks produce. Receipts naming individual `message_ids` say more
/// than a cursor position and are never gated.
fn cursor_read_receipt(event: &Event) -> Option<(&str, u64)> {
    if event.r#type != EventType::EventReadReceipt as i32 {
        return None;
    }
    let Some(EventPayload::Read(read)) = &event.payload else {
        return None;
    };
    let user_id = read.user_id.trim();
    if user_id.is_empty() || read.read_seq == 0 || !read.message_ids.is_empty() {
        return None;
    }
    Some((user_id, read.read_seq))
}

/// 成员制（大）会话判定：按 CID 前缀，与消息路径 `needs_member_lookup` 语义一致。
///
/// 群(2)/AI(3)/系统(4)/客服(5)/频道(7)/广播(8) 走会话级读扩散：事件/已读回执不物化收件人、
/// 不做 O(成员) 的 per-member sync 写，改会话级 version bump + 广播，客户端靠会话 version 增量发现。
/// 单聊(1)/临时(6) 成员少，仍走精确 per-member 物化（O(N) 廉价）。
fn is_member_based_conversation(conversation_id: &str) -> bool {
    matches!(
        conversation_id.trim().as_bytes().first(),
        Some(b'2' | b'3' | b'4' | b'5' | b'7' | b'8')
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::{ReadReceiptEvent, event};

    #[test]
    fn read_receipt_does_not_resolve_to_reader_from_payload() {
        let event = Event {
            conversation_id: "c1".to_string(),
            conversation_seq: 0,
            r#type: EventType::EventReadReceipt as i32,
            created_at: 1,
            event_id: "e1".to_string(),
            request_id: None,
            payload: Some(event::Payload::Read(ReadReceiptEvent {
                conversation_id: "c1".to_string(),
                read_seq: 7,
                user_id: "reader".to_string(),
                message_ids: Vec::new(),
                read_at: Some(1),
            })),
        };

        assert!(resolve_recipients_from_event_payload(&event).is_none());
    }

    #[test]
    fn only_cursor_read_receipts_are_gated() {
        let receipt = |user_id: &str, read_seq: u64, message_ids: Vec<String>| Event {
            conversation_id: "c1".to_string(),
            r#type: EventType::EventReadReceipt as i32,
            event_id: "e1".to_string(),
            payload: Some(event::Payload::Read(ReadReceiptEvent {
                conversation_id: "c1".to_string(),
                read_seq,
                user_id: user_id.to_string(),
                message_ids,
                read_at: Some(1),
            })),
            ..Default::default()
        };

        assert_eq!(
            cursor_read_receipt(&receipt(" reader ", 7, Vec::new())),
            Some(("reader", 7))
        );
        assert_eq!(cursor_read_receipt(&receipt("reader", 0, Vec::new())), None);
        assert_eq!(cursor_read_receipt(&receipt("  ", 7, Vec::new())), None);
        assert_eq!(
            cursor_read_receipt(&receipt("reader", 7, vec!["m1".to_string()])),
            None
        );

        let mut recall = receipt("reader", 7, Vec::new());
        recall.r#type = EventType::EventMessageRecall as i32;
        assert_eq!(cursor_read_receipt(&recall), None);
    }

    #[test]
    fn member_based_conversation_by_cid_prefix() {
        // 成员制大会话（走会话级读扩散，不做 O(成员) per-member 写）
        assert!(is_member_based_conversation("2AW1QQ2SKVWFEPJRXN")); // 群
        assert!(is_member_based_conversation("7ACHANNELXXXXXXXXX")); // 频道
        assert!(is_member_based_conversation("8ABROADCASTXXXXXXX")); // 广播
        assert!(is_member_based_conversation("3AAIXXXXXXXXXXXXXX")); // AI
        assert!(is_member_based_conversation("4ASYSXXXXXXXXXXXXX")); // 系统
        assert!(is_member_based_conversation("5ACUSTOMERXXXXXXXX")); // 客服
        // 非成员制（仍走精确 per-member 物化）
        assert!(!is_member_based_conversation("1ADW0CVC4ESXQ0046R")); // 单聊
        assert!(!is_member_based_conversation("6ATEMPXXXXXXXXXXXX")); // 临时
        assert!(!is_member_based_conversation("")); // 空
    }
}
