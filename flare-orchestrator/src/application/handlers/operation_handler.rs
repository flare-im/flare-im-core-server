//! 消息操作处理器：对接 gRPC 应用层 API，将 App*Command 解析为内部 Command 并委托 CommandHandler。
//! 通过辅助函数与「解析→基座→执行」模式保持方法简短（≤50 行）。

use std::sync::Arc;
use tracing::instrument;

use crate::application::{
    app_command_resolver::AppCommandResolver,
    commands::{
        AddReactionCommand, AppAddReactionCommand, AppBatchMarkMessageReadCommand, AppDeleteMessageCommand,
        AppEditMessageCommand, AppMarkConversationReadCommand, AppMarkMessageCommand,
        AppMarkMessagesReadUntilCommand, AppMarkAllConversationsReadCommand,
        AppPinMessageCommand, AppRecallMessageCommand,
        AppRemoveReactionCommand, AppUnmarkMessageCommand, AppUnpinMessageCommand, AppGetPinnedMessagesCommand,
        AppGetMarkedMessagesCommand, AppGetThreadsCommand, AppGetThreadRepliesCommand,
        BatchMarkMessageReadCommand, DeleteMessageCommand, DeleteType, EditMessageCommand,
        MarkConversationReadCommand, MarkMessageCommand, MessageOperationCommand,
        PinMessageCommand, ReadMessageCommand, RecallMessageCommand, RemoveReactionCommand,
        UnmarkMessageCommand, UnpinMessageCommand,
    },
    handlers::{MessageCommandHandler, MessageQueryHandler},
    queries::QueryMessagesQuery,
};
use flare_im_core::error::to_system_err;
use flare_im_core::utils::optional_fallback_conversation;
use flare_server_core::context::{ActorType, Ctx};
use flare_server_core::error::Result;

fn is_admin_actor(ctx: &Ctx) -> bool {
    let Some(actor) = ctx.actor() else {
        return false;
    };
    if matches!(actor.actor_type, ActorType::TenantAdmin | ActorType::System) {
        return true;
    }
    actor
        .roles
        .iter()
        .any(|r| r.eq_ignore_ascii_case("admin") || r.eq_ignore_ascii_case("owner"))
}

/// 消息操作处理器（应用层）：对接 gRPC 应用层 API，将 App*Command 解析为内部 Command 并委托 CommandHandler。
///
/// 使用 AppCommandResolver 统一「按 message_id 查消息 → 取 conversation_id/server_msg_id → 拼 MessageOperationCommand」，
/// 避免各 handle_*_app 重复相同逻辑，与 message_event_flow 中 ExecuteEvent 一致。
#[derive(Clone)]
pub struct MessageOperationHandler {
    command_handler: Arc<MessageCommandHandler>,
    query_handler: Arc<MessageQueryHandler>,
    resolver: Arc<AppCommandResolver>,
}

impl MessageOperationHandler {
    pub fn new(
        command_handler: Arc<MessageCommandHandler>,
        query_handler: Arc<MessageQueryHandler>,
    ) -> Self {
        let resolver = Arc::new(AppCommandResolver::new(query_handler.clone()));
        Self {
            command_handler,
            query_handler,
            resolver,
        }
    }

    /// 解析 message_id 得到 (conversation_id, base)，供单条消息操作复用。
    async fn resolve_and_base(
        &self,
        ctx: &Ctx,
        message_id: &str,
        fallback_conv: Option<&str>,
    ) -> Result<(String, MessageOperationCommand)> {
        let (conversation_id, target_message_id) = self
            .resolver
            .resolve_message_for_operation(message_id, fallback_conv)
            .await?;
        let base = self
            .resolver
            .build_operation_base(ctx, conversation_id.clone(), target_message_id);
        Ok((conversation_id, base))
    }

    /// 用 command 中的 operator_id/tenant_id 覆盖 base（非空时）。
    fn apply_operator_tenant_overrides(
        base: &mut MessageOperationCommand,
        operator_id: &str,
        tenant_id: &str,
    ) {
        if !operator_id.is_empty() {
            base.operator_id = operator_id.to_string();
        }
        if !tenant_id.is_empty() {
            base.tenant_id = tenant_id.to_string();
        }
    }

    /// 收集会话中直到 until_message_id 的 server_id 列表；未找到目标则报错。
    async fn collect_message_ids_until(
        &self,
        conversation_id: &str,
        until_message_id: &str,
    ) -> Result<Vec<String>> {
        let query = QueryMessagesQuery {
            conversation_id: conversation_id.to_string(),
            limit: Some(1000),
            cursor: None,
            start_time: None,
            end_time: None,
        };
        let result = self
            .query_handler
            .query_messages_with_pagination(query)
            .await
            .map_err(to_system_err)?;
        let mut ids = Vec::new();
        for msg in result.messages {
            if msg.server_id == until_message_id {
                return Ok(ids);
            }
            ids.push(msg.server_id);
        }
        Err(flare_im_core::error::FlareError::system(&format!(
            "Target message not found: {}",
            until_message_id
        )))
    }

    /// 从应用层删除命令与指定删除类型构建内部 DeleteMessageCommand。
    fn     build_delete_cmd(
        ctx: &Ctx,
        command: &AppDeleteMessageCommand,
        delete_type: DeleteType,
    ) -> DeleteMessageCommand {
        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id().to_string())
            .unwrap_or_default();
        let primary_message_id = command
            .message_ids
            .first()
            .cloned()
            .unwrap_or_default();
        DeleteMessageCommand {
            base: MessageOperationCommand {
                message_id: primary_message_id,
                operator_id: operator_id.clone(),
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: command.conversation_id.clone(),
            },
            delete_type,
            delete_scope: command.delete_scope,
            reason: command.reason.clone(),
            message_ids: command.message_ids.clone(),
            notify_others: command.notify_others,
            target_user_id: command
                .target_user_id
                .clone()
                .or_else(|| Some(operator_id.clone())),
            allow_admin_override: is_admin_actor(ctx),
        }
    }
}

impl MessageOperationHandler {
    /// 标记消息直到指定消息已读 - 使用应用层命令
    #[instrument(skip(self, ctx), fields(conversation_id = %command.conversation_id, user_id = %command.user_id))]
    pub async fn handle_mark_messages_read_until_app(
        &self,
        ctx: &Ctx,
        command: &AppMarkMessagesReadUntilCommand,
    ) -> Result<()> {
        let messages_to_mark = self
            .collect_message_ids_until(&command.conversation_id, &command.until_message_id)
            .await?;
        let batch_read_cmd = BatchMarkMessageReadCommand {
            conversation_id: command.conversation_id.clone(),
            user_id: command.user_id.clone(),
            message_ids: messages_to_mark,
            read_at: command.read_at,
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        };
        self.command_handler
            .handle_batch_mark_message_read(ctx, batch_read_cmd)
            .await
            .map_err(to_system_err)?;
        Ok(())
    }

    /// 标记全部会话已读 - 使用应用层命令
    #[instrument(skip(self, _ctx), fields(user_id = %command.user_id))]
    pub async fn handle_mark_all_conversations_read_app(
        &self,
        _ctx: &Ctx,
        command: &AppMarkAllConversationsReadCommand,
    ) -> Result<()> {
        // 当前版本暂不支持通过存储服务获取用户所有会话的功能
        // 该功能需要额外的API支持
        
        // 可以通过其他方式实现，例如从会话服务获取用户的所有会话
        // 或者要求前端传入具体的会话列表
        
        Ok(())
    }

    /// 获取置顶消息 - 使用应用层命令
    #[instrument(skip(self, _ctx), fields(conversation_id = %command.conversation_id))]
    pub async fn handle_get_pinned_messages_app(
        &self,
        _ctx: &Ctx,
        command: &AppGetPinnedMessagesCommand,
    ) -> Result<Vec<flare_proto::common::Message>> {
        // 当前版本暂不支持直接获取置顶消息
        // 置顶消息功能需要额外的API支持
        
        Ok(Vec::new())
    }

    /// 获取标记消息 - 使用应用层命令
    #[instrument(skip(self, _ctx), fields(user_id = %command.user_id))]
    pub async fn handle_get_marked_messages_app(
        &self,
        _ctx: &Ctx,
        command: &AppGetMarkedMessagesCommand,
    ) -> Result<Vec<flare_proto::common::Message>> {
        // 当前版本暂不支持直接获取标记消息
        // 标记消息功能需要额外的API支持
        
        Ok(Vec::new())
    }

    /// 获取话题 - 使用应用层命令
    #[instrument(skip(self, _ctx), fields(conversation_id = %command.conversation_id))]
    pub async fn handle_get_threads_app(
        &self,
        _ctx: &Ctx,
        command: &AppGetThreadsCommand,
    ) -> Result<Vec<flare_proto::common::ThreadInfo>> {
        // 当前版本暂不支持直接获取话题
        // 话题功能需要额外的API支持
        
        Ok(Vec::new())
    }

    /// 获取话题回复 - 使用应用层命令
    #[instrument(skip(self, _ctx), fields(thread_id = %command.thread_id))]
    pub async fn handle_get_thread_replies_app(
        &self,
        _ctx: &Ctx,
        command: &AppGetThreadRepliesCommand,
    ) -> Result<Vec<flare_proto::common::Message>> {
        // 当前版本暂不支持获取话题回复
        // 话题回复功能需要额外的API支持
        
        Ok(Vec::new())
    }
}

impl MessageOperationHandler {
    /// 撤回消息 - 使用应用层命令；通过 Resolver 解析消息上下文后委托 CommandHandler。
    #[instrument(skip(self, ctx), fields(message_id = %command.message_id))]
    pub async fn handle_recall_message_app(
        &self,
        ctx: &Ctx,
        command: &AppRecallMessageCommand,
    ) -> Result<(String, i64)> {
        let fallback = optional_fallback_conversation(&command.conversation_id);
        let (_, mut base) = self.resolve_and_base(ctx, &command.message_id, fallback).await?;
        Self::apply_operator_tenant_overrides(&mut base, &command.operator_id, &command.tenant_id);
        let recall_cmd = RecallMessageCommand {
            base,
            reason: command.reason.clone(),
            time_limit_seconds: command.time_limit_seconds,
            allow_admin_override: is_admin_actor(ctx),
        };
        self.command_handler
            .handle_recall_message(ctx, recall_cmd)
            .await
            .map_err(to_system_err)?;
        Ok((command.message_id.clone(), 0))
    }

    /// 编辑消息 - 使用应用层命令；通过 Resolver 解析消息上下文后委托 CommandHandler。
    #[instrument(skip(self, ctx), fields(message_id = %command.message_id))]
    pub async fn handle_edit_message_app(
        &self,
        ctx: &Ctx,
        command: &AppEditMessageCommand,
    ) -> Result<(String, i64)> {
        let fallback = optional_fallback_conversation(&command.conversation_id);
        let (_, mut base) = self.resolve_and_base(ctx, &command.message_id, fallback).await?;
        Self::apply_operator_tenant_overrides(&mut base, &command.operator_id, &command.tenant_id);
        let edit_cmd = EditMessageCommand {
            base,
            new_content: command.new_content.clone(),
            reason: command.reason.clone(),
            allow_admin_override: is_admin_actor(ctx),
        };
        self.command_handler
            .handle_edit_message(ctx, edit_cmd)
            .await
            .map_err(to_system_err)?;
        Ok((command.message_id.clone(), 0))
    }

    /// 删除消息 - 使用应用层命令
    #[instrument(skip(self, ctx), fields(conversation_id = %command.conversation_id))]
    pub async fn handle_delete_message_app(
        &self,
        ctx: &Ctx,
        command: &AppDeleteMessageCommand,
    ) -> Result<(bool, i32)> {
        let delete_cmd = Self::build_delete_cmd(ctx, command, command.delete_type);
        self.command_handler
            .handle_delete_message(ctx, delete_cmd)
            .await
            .map_err(to_system_err)?;
        Ok((true, command.message_ids.len() as i32))
    }

    /// 软删除消息 - 使用应用层命令
    #[instrument(skip(self, ctx), fields(message_ids = ?command.message_ids))]
    pub async fn handle_soft_delete_message_app(
        &self,
        ctx: &Ctx,
        command: &AppDeleteMessageCommand,
    ) -> Result<(bool, i32)> {
        let delete_cmd = Self::build_delete_cmd(ctx, command, DeleteType::Soft);
        self.command_handler
            .handle_delete_message(ctx, delete_cmd)
            .await
            .map_err(to_system_err)?;
        Ok((true, command.message_ids.len() as i32))
    }

    /// 标记消息已读；通过 Resolver 解析消息上下文后委托 CommandHandler。
    #[instrument(skip(self, ctx), fields(message_id = %command.message_id))]
    pub async fn handle_mark_message_read_app(
        &self,
        ctx: &Ctx,
        command: &AppMarkMessageCommand,
    ) -> Result<()> {
        let (_, base) = self.resolve_and_base(ctx, &command.message_id, None).await?;
        let read_cmd = ReadMessageCommand {
            base,
            message_ids: vec![command.message_id.clone()],
            read_at: Some(chrono::Utc::now()),
            burn_after_read: false,
        };
        self.command_handler
            .handle_read_message(ctx, read_cmd)
            .await
            .map_err(to_system_err)?;
        Ok(())
    }

    /// 批量标记消息已读
    #[instrument(skip(self, ctx), fields(conversation_id = %command.conversation_id))]
    pub async fn handle_batch_mark_message_read_app(
        &self,
        ctx: &Ctx,
        command: &AppBatchMarkMessageReadCommand,
    ) -> Result<i32> {
        let batch_read_cmd = BatchMarkMessageReadCommand {
            conversation_id: command.conversation_id.clone(),
            user_id: command.user_id.clone(),
            message_ids: command.message_ids.clone(),
            read_at: command.read_at,
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        };
        self.command_handler
            .handle_batch_mark_message_read(ctx, batch_read_cmd)
            .await
            .map_err(to_system_err)
    }

    /// 标记会话已读
    #[instrument(skip(self, ctx), fields(conversation_id = %command.conversation_id))]
    pub async fn handle_mark_conversation_read_app(
        &self,
        ctx: &Ctx,
        command: &AppMarkConversationReadCommand,
    ) -> Result<()> {
        let cmd = MarkConversationReadCommand {
            conversation_id: command.conversation_id.clone(),
            user_id: command.user_id.clone(),
            read_at: command.read_at,
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        };
        self.command_handler
            .handle_mark_conversation_read(cmd)
            .await
            .map_err(to_system_err)
    }

    /// 添加反应；通过 Resolver 解析消息上下文后委托 CommandHandler。
    #[instrument(skip(self, ctx), fields(message_id = %command.message_id))]
    pub async fn handle_add_reaction_app(
        &self,
        ctx: &Ctx,
        command: &AppAddReactionCommand,
    ) -> Result<()> {
        let fallback = optional_fallback_conversation(&command.conversation_id);
        let (_, base) = self.resolve_and_base(ctx, &command.message_id, fallback).await?;
        let cmd = AddReactionCommand {
            base,
            emoji: command.emoji.clone(),
        };
        self.command_handler
            .handle_add_reaction(ctx, cmd)
            .await
            .map_err(to_system_err)?;
        Ok(())
    }

    /// 移除反应；通过 Resolver 解析消息上下文后委托 CommandHandler。
    #[instrument(skip(self, ctx), fields(message_id = %command.message_id))]
    pub async fn handle_remove_reaction_app(
        &self,
        ctx: &Ctx,
        command: &AppRemoveReactionCommand,
    ) -> Result<()> {
        let fallback = optional_fallback_conversation(&command.conversation_id);
        let (_, base) = self.resolve_and_base(ctx, &command.message_id, fallback).await?;
        let cmd = RemoveReactionCommand {
            base,
            emoji: command.emoji.clone(),
        };
        self.command_handler
            .handle_remove_reaction(ctx, cmd)
            .await
            .map_err(to_system_err)?;
        Ok(())
    }

    /// 置顶消息；通过 Resolver 解析消息上下文后委托 CommandHandler。
    #[instrument(skip(self, ctx), fields(message_id = %command.message_id))]
    pub async fn handle_pin_message_app(
        &self,
        ctx: &Ctx,
        command: &AppPinMessageCommand,
    ) -> Result<()> {
        let fallback = optional_fallback_conversation(&command.conversation_id);
        let (_, base) = self.resolve_and_base(ctx, &command.message_id, fallback).await?;
        let cmd = PinMessageCommand {
            base,
            reason: command.reason.clone(),
            expire_at: command.expire_at,
        };
        self.command_handler
            .handle_pin_message(ctx, cmd)
            .await
            .map_err(to_system_err)?;
        Ok(())
    }

    /// 取消置顶消息；通过 Resolver 解析消息上下文后委托 CommandHandler。
    #[instrument(skip(self, ctx), fields(message_id = %command.message_id))]
    pub async fn handle_unpin_message_app(
        &self,
        ctx: &Ctx,
        command: &AppUnpinMessageCommand,
    ) -> Result<()> {
        let fallback = optional_fallback_conversation(&command.conversation_id);
        let (_, base) = self.resolve_and_base(ctx, &command.message_id, fallback).await?;
        let cmd = UnpinMessageCommand { base };
        self.command_handler
            .handle_unpin_message(ctx, cmd)
            .await
            .map_err(to_system_err)?;
        Ok(())
    }

    /// 标记消息；通过 Resolver 解析消息上下文后委托 CommandHandler。
    #[instrument(skip(self, ctx), fields(message_id = %command.message_id))]
    pub async fn handle_mark_message_app(
        &self,
        ctx: &Ctx,
        command: &AppMarkMessageCommand,
    ) -> Result<()> {
        let fallback = optional_fallback_conversation(&command.conversation_id);
        let (_, base) = self.resolve_and_base(ctx, &command.message_id, fallback).await?;
        let cmd = MarkMessageCommand {
            base,
            mark_type: command.mark_type,
        };
        self.command_handler
            .handle_mark_message(ctx, cmd)
            .await
            .map_err(to_system_err)?;
        Ok(())
    }

    /// 取消标记消息；通过 Resolver 解析消息上下文后委托 CommandHandler。
    #[instrument(skip(self, ctx), fields(message_id = %command.message_id))]
    pub async fn handle_unmark_message_app(
        &self,
        ctx: &Ctx,
        command: &AppUnmarkMessageCommand,
    ) -> Result<()> {
        let fallback = optional_fallback_conversation(&command.conversation_id);
        let (_, base) = self.resolve_and_base(ctx, &command.message_id, fallback).await?;
        let mark_type = if command.mark_type < 0 {
            None
        } else {
            Some(command.mark_type)
        };
        let cmd = UnmarkMessageCommand {
            base,
            mark_type,
            user_id: command.user_id.clone(),
        };
        self.command_handler
            .handle_unmark_message(ctx, cmd)
            .await
            .map_err(to_system_err)?;
        Ok(())
    }
}
