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
use flare_proto::common::{DeleteType as ProtoDeleteType, MarkType, ReactionAction};
use tracing::instrument;

use crate::application::commands::{
    AddReactionCommand, DeleteMessageCommand, EditMessageCommand, MarkMessageCommand,
    PinMessageCommand, ReadBurnMessageCommand, RecallMessageCommand, RemoveReactionCommand,
    UnmarkMessageCommand, UnpinMessageCommand,
};
use crate::application::handlers::EventHandler;
use crate::domain::builder::{
    EventBuilder, build_burn_scheduled_event, build_delete_event, build_edit_event,
    build_mark_event, build_reaction_event, build_recall_event, build_unmark_event,
};
use flare_server_core::error::Result;

/// 消息操作处理器（编排层）
pub struct MessageActionHandler {
    /// 事件处理器
    event_handler: Arc<EventHandler>,
}

impl MessageActionHandler {
    pub fn new(event_handler: Arc<EventHandler>) -> Self {
        Self { event_handler }
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
    pub async fn recall_message(&self, ctx: &Ctx, cmd: RecallMessageCommand) -> Result<()> {
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
    pub async fn edit_message(&self, ctx: &Ctx, cmd: EditMessageCommand) -> Result<()> {
        // 1. 构建编辑事件
        let event = build_edit_event(
            &cmd.base.conversation_id,
            &cmd.base.message_id,
            cmd.new_content,
            1, // TODO: 从存储获取当前编辑版本并递增
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
        self.event_handler.handle_event(ctx, event).await?;
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
    /// - `Ok(reaction_count)`: 反应数量
    /// - `Err`: 错误
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = %cmd.base.message_id,
        emoji = %cmd.emoji,
    ))]
    pub async fn add_reaction(&self, ctx: &Ctx, cmd: AddReactionCommand) -> Result<i32> {
        // 1. 构建反应事件
        let event = build_reaction_event(
            &cmd.base.conversation_id,
            &cmd.base.message_id,
            &cmd.base.operator_id,
            &cmd.emoji,
            ReactionAction::Add,
        );

        // 2. 调用 EventHandler 处理事件
        self.event_handler.handle_event(ctx, event).await?;

        // TODO: 从存储获取当前反应数量
        Ok(1)
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
    /// - `Ok(reaction_count)`: 反应数量
    /// - `Err`: 错误
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = %cmd.base.message_id,
        emoji = %cmd.emoji,
    ))]
    pub async fn remove_reaction(&self, ctx: &Ctx, cmd: RemoveReactionCommand) -> Result<i32> {
        // 1. 构建反应事件
        let event = build_reaction_event(
            &cmd.base.conversation_id,
            &cmd.base.message_id,
            &cmd.base.operator_id,
            &cmd.emoji,
            ReactionAction::Remove,
        );

        // 2. 调用 EventHandler 处理事件
        self.event_handler.handle_event(ctx, event).await?;

        // TODO: 从存储获取当前反应数量
        Ok(0)
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
    pub async fn pin_message(&self, ctx: &Ctx, cmd: PinMessageCommand) -> Result<()> {
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
    pub async fn unpin_message(&self, ctx: &Ctx, cmd: UnpinMessageCommand) -> Result<()> {
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
    pub async fn mark_message(&self, ctx: &Ctx, cmd: MarkMessageCommand) -> Result<()> {
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
    pub async fn unmark_message(&self, ctx: &Ctx, cmd: UnmarkMessageCommand) -> Result<()> {
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
