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

use std::sync::Arc;

use flare_im_core::Ctx;
use flare_proto::common::{Event, EventType};
use flare_server_core::{flare_err, flare_err_details};
use tracing::instrument;

use crate::error::{ErrorCode, Result};
use crate::domain::PersistenceMode;
use crate::domain::repository::{PushRepository, RecipientRepository};
use crate::domain::service::sequence_allocator::SequenceAllocator;
use crate::domain::service::validation_strategy::{
    CompositeEventValidationStrategy, EventValidationStrategy, ValidationContext,
};
use crate::infrastructure::messaging::push_repository::MqPushRepository;

/// 事件领域服务
pub struct EventDomainService {
    /// 推送仓储（使用具体类型以支持 async fn in traits）
    push_repository: Arc<MqPushRepository>,
    /// 接收者仓储
    recipient_repository: Arc<dyn RecipientRepository>,
    /// 序列号分配器
    sequence_allocator: Arc<SequenceAllocator>,
    /// 事件校验策略
    validation_strategy: Arc<dyn EventValidationStrategy>,
}

impl EventDomainService {
    pub fn new(
        push_repository: Arc<MqPushRepository>,
        recipient_repository: Arc<dyn RecipientRepository>,
        sequence_allocator: Arc<SequenceAllocator>,
        validation_strategy: Option<Arc<dyn EventValidationStrategy>>,
    ) -> Self {
        Self {
            push_repository,
            recipient_repository,
            sequence_allocator,
            validation_strategy: validation_strategy
                .unwrap_or_else(|| Arc::new(CompositeEventValidationStrategy::default_composite())),
        }
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
    pub async fn validate_event(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        event: &Event,
    ) -> Result<()> {
        let validation_context = ValidationContext {
            ctx,
            tenant_id,
            conversation_id: &event.conversation_id,
        };
        
        let validation_result = self.validation_strategy
            .validate(&validation_context, event)
            .await
            .map_err(|e| flare_err!(ErrorCode::InvalidParameter, &format!("Event validation failed: {}", e)))?;
        
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
        
        tracing::debug!(
            conversation_id = %event.conversation_id,
            seq = session_seq,
            "Allocated session sequence for event"
        );
        
        event.seq = session_seq;
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
        self.recipient_repository
            .get_conversation_members(ctx, &event.conversation_id)
            .await
            .map_err(|e| flare_err!(ErrorCode::InternalError, &format!("Failed to get conversation members for event: {}", e)))
    }

    /// 仅推送事件（不持久化），由服务内部自动解析接收者
    ///
    /// 用于临时实时事件（如输入态、presence、通话信令）等场景。
    #[instrument(skip(self), fields(
        event_id = %event.event_id,
        conversation_id = %event.conversation_id,
        event_type = ?EventType::try_from(event.r#type),
    ))]
    pub async fn push_only(
        &self,
        ctx: &Ctx,
        event: Event,
    ) -> Result<()> {
        let recipient_user_ids = self.get_recipient_user_ids(ctx, &event).await?;
        self.push_only_with_recipients(ctx, event, recipient_user_ids).await
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
        tracing::debug!(
            event_id = %event.event_id,
            conversation_id = %event.conversation_id,
            event_type = ?EventType::try_from(event.r#type),
            seq = event.seq,
            recipient_count = recipient_user_ids.len(),
            "Pushing event only (no persistence)"
        );

        let conversation_id = event.conversation_id.clone();
        self.push_repository
            .push_only_event(
                ctx,
                event,
                recipient_user_ids,
                conversation_id,
            )
            .await
            .map_err(|e| flare_err!(ErrorCode::InternalError, &format!("Failed to publish push-only event to MQ: {}", e)))
    }

    /// 判断是否为临时事件（仅推送，不持久化）
    ///
    /// 根据 event.proto 定义，以下事件类型为临时事件：
    /// - EVENT_TYPING：正在输入（高频，无需持久化）
    /// - EVENT_PRESENCE：在线状态（实时性，无需持久化）
    /// - EVENT_CALL_SIGNAL：通话信令（实时性，无需持久化）
    fn is_temporary_event(event_type: EventType) -> bool {
        match event_type {
            EventType::EventTyping => true,        // 正在输入：高频，仅推送
            EventType::EventPresence => true,      // 在线状态：实时性，仅推送
            EventType::EventCallSignal => true,    // 通话信令：实时性，仅推送
            _ => false,                           // 其他事件：需要持久化
        }
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
    pub async fn persistence_only(
        &self,
        ctx: &Ctx,
        event: Event,
    ) -> Result<()> {
        tracing::debug!(
            event_id = %event.event_id,
            conversation_id = %event.conversation_id,
            event_type = ?EventType::try_from(event.r#type),
            seq = event.seq,
            "Persisting event only (no push)"
        );
        
        let conversation_id = event.conversation_id.clone();
        self.push_repository
            .persistence_only_event(
                ctx,
                event,
                conversation_id,
            )
            .await
            .map_err(|e| flare_err!(ErrorCode::InternalError, &format!("Failed to publish persistence-only event to MQ: {}", e)))
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
        tracing::debug!(
            event_id = %event.event_id,
            conversation_id = %event.conversation_id,
            event_type = ?event.r#type(),
            seq = event.seq,
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
            .map_err(|e| flare_err!(ErrorCode::InternalError, &format!("Failed to publish event to MQ: {}", e)))
    }
}
