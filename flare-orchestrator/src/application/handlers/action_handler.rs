//! 消息操作处理器（编排层）- 负责编排领域服务
//!
//! ## 核心职责
//! 1. 构建事件（使用 EventBuilder）
//! 2. 调用 EventHandler 处理事件
//!
//! ## 设计原则
//! - 编排层：不包含业务逻辑，只负责流程编排
//! - 依赖注入：通过构造函数注入所有依赖
//! - CQRS：Command Handler 负责写操作

use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_message_pipeline::{MqPushRepository, PushRepository};
use flare_im_seq::SequenceAllocator;
use flare_proto::common::{DeleteType as ProtoDeleteType, MarkType, ReactionAction};
use tracing::instrument;

use crate::application::commands::{
    AddReactionCommand, DeleteMessageCommand, EditMessageCommand, MarkMessageCommand,
    MessageOperationCommand, PinMessageCommand, ReadBurnMessageCommand, RecallMessageCommand,
    RemoveReactionCommand, UnmarkMessageCommand, UnpinMessageCommand,
};
use crate::application::handlers::EventHandler;
use crate::domain::builder::{
    EventBuilder, build_burn_scheduled_event, build_delete_event, build_edit_event,
    build_mark_event, build_reaction_event, build_recall_event, build_unmark_event,
};
use crate::domain::repository::MessageTargetRepository;
use crate::domain::service::event_domain_service::SequenceAllocatorPort;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};

/// 消息操作处理器（编排层）
pub struct MessageActionHandler<PR = MqPushRepository, SA = SequenceAllocator>
where
    PR: PushRepository,
    SA: SequenceAllocatorPort,
{
    /// 事件处理器
    event_handler: Arc<EventHandler<PR, SA>>,
    /// 按服务端消息 ID 找消息所在的会话（与操作授权读的是同一份存储）。
    message_targets: Option<Arc<dyn MessageTargetRepository>>,
}

impl<PR, SA> MessageActionHandler<PR, SA>
where
    PR: PushRepository,
    SA: SequenceAllocatorPort,
{
    pub fn new(event_handler: Arc<EventHandler<PR, SA>>) -> Self {
        Self {
            event_handler,
            message_targets: None,
        }
    }

    /// 操作请求没写会话时（反应、置顶、标记的请求没有这个字段，撤回与编辑的可选），按目标消息
    /// 找它所在的会话。没有它，这些请求都会因为事件缺会话被统一事件入口拒绝。
    pub fn with_message_targets(mut self, targets: Arc<dyn MessageTargetRepository>) -> Self {
        self.message_targets = Some(targets);
        self
    }

    /// 填上操作的会话：请求写了就用请求的（授权时仍按存储的消息核对，不一致即拒），没写就取
    /// 目标消息所在的会话。消息还没落库与授权时一样回可重试。
    async fn resolve_conversation(
        &self,
        ctx: &Ctx,
        base: &mut MessageOperationCommand,
    ) -> Result<()> {
        if !base.conversation_id.trim().is_empty() {
            return Ok(());
        }
        let message_id = base.message_id.trim();
        if message_id.is_empty() {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "the operation names no message",
            )
            .build_error());
        }
        let targets = self.message_targets.as_ref().ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::ServiceUnavailable,
                "message operation authorization unavailable",
            )
            .build_error()
        })?;
        let located = targets
            .conversations_of(ctx, &base.tenant_id, &[message_id.to_string()])
            .await?;
        base.conversation_id = located
            .get(message_id)
            .filter(|conversation_id| !conversation_id.is_empty())
            .cloned()
            // 刚投递、尚未落库：与授权时一样，稍后重试。
            .ok_or_else(|| {
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "message not stored yet")
                    .details("retry the operation shortly")
                    .build_error()
            })?;
        Ok(())
    }

    /// 撤回消息
    ///
    /// # 编排流程
    /// 1. 构建撤回事件
    /// 2. 调用 EventHandler 处理事件
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `cmd`: 撤回消息命令
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = %cmd.base.message_id,
    ))]
    pub async fn recall_message(&self, ctx: &Ctx, mut cmd: RecallMessageCommand) -> Result<()> {
        self.resolve_conversation(ctx, &mut cmd.base).await?;
        // 1. 构建撤回事件
        let event = build_recall_event(
            &cmd.base.conversation_id,
            &cmd.base.message_id,
            cmd.reason.as_deref(),
        );

        // 2. 调用 EventHandler 处理事件
        self.event_handler.handle_event(ctx, event).await
    }

    /// 编辑消息
    ///
    /// # 编排流程
    /// 1. 构建编辑事件
    /// 2. 调用 EventHandler 处理事件
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `cmd`: 编辑消息命令
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = %cmd.base.message_id,
    ))]
    pub async fn edit_message(&self, ctx: &Ctx, mut cmd: EditMessageCommand) -> Result<()> {
        self.resolve_conversation(ctx, &mut cmd.base).await?;
        // 1. 构建编辑事件
        // 编辑版本不在发布端分配：异步扇出架构下发布时无法权威递增；客户端按
        // conversation_seq 收敛编辑事件，writer 侧另行维护持久化版本号。
        let event = build_edit_event(
            &cmd.base.conversation_id,
            &cmd.base.message_id,
            cmd.new_content,
        );

        // 2. 调用 EventHandler 处理事件
        self.event_handler.handle_event(ctx, event).await
    }

    /// 删除消息
    ///
    /// # 编排流程
    /// 1. 构建删除事件
    /// 2. 调用 EventHandler 处理事件
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `cmd`: 删除消息命令
    ///
    /// # 返回
    /// - `Ok(deleted_count)`: 删除的消息数量
    /// - `Err`: 错误
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_ids = ?cmd.message_ids,
    ))]
    pub async fn delete_message(&self, ctx: &Ctx, cmd: DeleteMessageCommand) -> Result<i32> {
        let mut deleted_count = 0;

        // 批量删除：为每个消息构建删除事件
        for message_id in &cmd.message_ids {
            // 1. 构建删除事件
            let proto_delete_type = match cmd.delete_type {
                crate::application::commands::DeleteType::Soft => ProtoDeleteType::Soft,
                crate::application::commands::DeleteType::Hard => ProtoDeleteType::Hard,
            };

            let event =
                build_delete_event(&cmd.base.conversation_id, message_id, proto_delete_type);

            // 2. 调用 EventHandler 处理事件
            self.event_handler.handle_event(ctx, event).await?;
            deleted_count += 1;
        }

        Ok(deleted_count)
    }

    /// 阅后即焚单条已读：由调用方先完成消息存在性、可见性和 burn 配置查询后进入本命令。
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = %cmd.message_id,
    ))]
    pub async fn read_burn_message(&self, ctx: &Ctx, cmd: ReadBurnMessageCommand) -> Result<i64> {
        if cmd.burn_after_read_seconds <= 0 {
            return Err(flare_server_core::flare_err!(
                flare_server_core::error::ErrorCode::InvalidParameter,
                "burn_after_read_seconds must be positive"
            ));
        }
        let read_at = cmd.read_at.timestamp();
        let burn_at = read_at.saturating_add(cmd.burn_after_read_seconds);
        let event = build_burn_scheduled_event(
            &cmd.tenant_id,
            &cmd.conversation_id,
            &cmd.message_id,
            Some(&cmd.reader_id),
            burn_at,
            read_at,
        );
        // The caller already checked the message, its visibility and its burn setting.
        self.event_handler.handle_system_event(ctx, event).await?;
        Ok(burn_at)
    }

    /// 添加反应
    ///
    /// # 编排流程
    /// 1. 构建反应事件
    /// 2. 调用 EventHandler 处理事件
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `cmd`: 添加反应命令
    ///
    /// # 返回
    /// - `Ok(())`: 已发布反应事件（当前数量不在此同步返回——异步扇出下无法同步算出，
    ///   客户端按推送的反应事件 per-user 收敛获得权威计数）
    /// - `Err`: 错误
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = %cmd.base.message_id,
        emoji = %cmd.emoji,
    ))]
    pub async fn add_reaction(&self, ctx: &Ctx, mut cmd: AddReactionCommand) -> Result<()> {
        self.resolve_conversation(ctx, &mut cmd.base).await?;
        // 1. 构建反应事件
        let event = build_reaction_event(
            &cmd.base.conversation_id,
            &cmd.base.message_id,
            &cmd.base.operator_id,
            &cmd.emoji,
            ReactionAction::Add,
        );

        // 2. 调用 EventHandler 处理事件
        self.event_handler.handle_event(ctx, event).await
    }

    /// 移除反应
    ///
    /// # 编排流程
    /// 1. 构建反应事件
    /// 2. 调用 EventHandler 处理事件
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `cmd`: 移除反应命令
    ///
    /// # 返回
    /// - `Ok(())`: 已发布反应事件（当前数量不在此同步返回，理由同 [`add_reaction`]）
    /// - `Err`: 错误
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = %cmd.base.message_id,
        emoji = %cmd.emoji,
    ))]
    pub async fn remove_reaction(&self, ctx: &Ctx, mut cmd: RemoveReactionCommand) -> Result<()> {
        self.resolve_conversation(ctx, &mut cmd.base).await?;
        // 1. 构建反应事件
        let event = build_reaction_event(
            &cmd.base.conversation_id,
            &cmd.base.message_id,
            &cmd.base.operator_id,
            &cmd.emoji,
            ReactionAction::Remove,
        );

        // 2. 调用 EventHandler 处理事件
        self.event_handler.handle_event(ctx, event).await
    }

    /// 置顶消息
    ///
    /// # 编排流程
    /// 1. 构建置顶事件
    /// 2. 调用 EventHandler 处理事件
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `cmd`: 置顶消息命令
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = %cmd.base.message_id,
    ))]
    pub async fn pin_message(&self, ctx: &Ctx, mut cmd: PinMessageCommand) -> Result<()> {
        self.resolve_conversation(ctx, &mut cmd.base).await?;
        // 1. 构建置顶事件
        let event = EventBuilder::pin(&cmd, 0);

        // 2. 调用 EventHandler 处理事件
        self.event_handler.handle_event(ctx, event).await
    }

    /// 取消置顶消息
    ///
    /// # 编排流程
    /// 1. 构建取消置顶事件
    /// 2. 调用 EventHandler 处理事件
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `cmd`: 取消置顶消息命令
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = %cmd.base.message_id,
    ))]
    pub async fn unpin_message(&self, ctx: &Ctx, mut cmd: UnpinMessageCommand) -> Result<()> {
        self.resolve_conversation(ctx, &mut cmd.base).await?;
        // 1. 构建取消置顶事件
        let event = EventBuilder::unpin(&cmd, 0);

        // 2. 调用 EventHandler 处理事件
        self.event_handler.handle_event(ctx, event).await
    }

    /// 标记消息
    ///
    /// # 编排流程
    /// 1. 构建标记事件
    /// 2. 调用 EventHandler 处理事件
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `cmd`: 标记消息命令
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = %cmd.base.message_id,
        mark_type = cmd.mark_type,
    ))]
    pub async fn mark_message(&self, ctx: &Ctx, mut cmd: MarkMessageCommand) -> Result<()> {
        self.resolve_conversation(ctx, &mut cmd.base).await?;
        // 1. 构建标记事件
        let mark_type = match cmd.mark_type {
            0 => MarkType::Important,
            1 => MarkType::Todo,
            2 => MarkType::Done,
            _ => MarkType::Custom,
        };

        let event = build_mark_event(
            &cmd.base.conversation_id,
            &cmd.base.message_id,
            &cmd.base.operator_id,
            mark_type,
        );

        // 2. 调用 EventHandler 处理事件
        self.event_handler.handle_event(ctx, event).await
    }

    /// 取消标记消息
    ///
    /// # 编排流程
    /// 1. 构建取消标记事件
    /// 2. 调用 EventHandler 处理事件
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `cmd`: 取消标记消息命令
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = %cmd.base.message_id,
    ))]
    pub async fn unmark_message(&self, ctx: &Ctx, mut cmd: UnmarkMessageCommand) -> Result<()> {
        self.resolve_conversation(ctx, &mut cmd.base).await?;
        // 1. 构建取消标记事件
        let mark_type = match cmd.mark_type {
            Some(0) => MarkType::Important,
            Some(1) => MarkType::Todo,
            Some(2) => MarkType::Done,
            _ => MarkType::Custom,
        };

        let event = build_unmark_event(
            &cmd.base.conversation_id,
            &cmd.base.message_id,
            &cmd.user_id,
            mark_type,
        );

        // 2. 调用 EventHandler 处理事件
        self.event_handler.handle_event(ctx, event).await
    }
}
