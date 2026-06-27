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
    RecipientRepository, UserSyncCompensationRepository, UserSyncCompensationTask,
    UserSyncIndexRepository,
};
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
        }
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
}
