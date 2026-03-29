//! 消息操作处理器：对接 gRPC 应用层 API，将 App*Command 解析为内部 Command 并委托 CommandHandler。
//! 通过辅助函数与「解析→基座→执行」模式保持方法简短（≤50 行）。

use std::sync::Arc;
use tracing::instrument;

use crate::application::{
    commands::{
        AddReactionCommand, AppAddReactionCommand, AppBatchMarkMessageReadCommand,
        AppDeleteMessageCommand, AppEditMessageCommand,
        AppMarkAllConversationsReadCommand, AppMarkConversationReadCommand, AppMarkMessageCommand,
        AppMarkMessagesReadUntilCommand, AppPinMessageCommand, AppRecallMessageCommand,
        AppRemoveReactionCommand, AppUnmarkMessageCommand, AppUnpinMessageCommand,
        BatchMarkMessageReadCommand, DeleteMessageCommand, DeleteScope, DeleteType,
        EditMessageCommand, MarkConversationReadCommand, MarkMessageCommand,
        MessageOperationCommand, PinMessageCommand, ReadMessageCommand, RecallMessageCommand,
        RemoveReactionCommand, UnmarkMessageCommand, UnpinMessageCommand,
    },
    handlers::MessageCommandHandler,
};
use flare_im_core::error::to_system_err;
use flare_im_core::utils::optional_fallback_conversation;
use flare_proto::common::event::Payload as EventPayload;
use flare_proto::common::{ErrorCode, Event, OperationResponse, RpcStatus};
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
/// `message_id → conversation_id` 由领域 `MessageOperationService`（WAL / Storage Reader）解析，不经 application 查询模块。
#[derive(Clone)]
pub struct MessageOperationHandler {
    command_handler: Arc<MessageCommandHandler>,
}

impl MessageOperationHandler {
    pub fn new(command_handler: Arc<MessageCommandHandler>) -> Self {
        Self { command_handler }
    }

    /// 解析 message_id 得到 (conversation_id, base)，供单条消息操作复用。
    async fn resolve_and_base(
        &self,
        ctx: &Ctx,
        message_id: &str,
        fallback_conv: Option<&str>,
    ) -> Result<(String, MessageOperationCommand)> {
        let (conversation_id, target_message_id) = self
            .command_handler
            .resolve_message_ids_for_operation(ctx, message_id, fallback_conv)
            .await
            .map_err(to_system_err)?;
        let base = MessageOperationCommand {
            message_id: target_message_id,
            operator_id: ctx
                .actor()
                .map(|a| a.actor_id().to_string())
                .unwrap_or_default(),
            timestamp: chrono::Utc::now(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: conversation_id.clone(),
        };
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

    /// 收集会话中直到 `until_message_id` 的 server_id 列表（时间倒序下，比目标新的消息）；未找到目标则报错。
    async fn collect_message_ids_until(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        until_message_id: &str,
    ) -> Result<Vec<String>> {
        let mut cursor: Option<String> = None;
        let mut collected: Vec<String> = Vec::new();
        for _ in 0..10 {
            let page = self
                .command_handler
                .page_conversation_server_ids(ctx, conversation_id, 1000, cursor.as_deref())
                .await
                .map_err(to_system_err)?;
            for id in page.server_ids {
                if id == until_message_id {
                    return Ok(collected);
                }
                collected.push(id);
            }
            if !page.has_more || page.next_cursor.is_empty() {
                break;
            }
            cursor = Some(page.next_cursor);
        }
        Err(to_system_err(format!(
            "Target message not found: {}",
            until_message_id
        )))
    }

    /// 从应用层删除命令与指定删除类型构建内部 DeleteMessageCommand。
    fn build_delete_cmd(
        ctx: &Ctx,
        command: &AppDeleteMessageCommand,
        delete_type: DeleteType,
    ) -> DeleteMessageCommand {
        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id().to_string())
            .unwrap_or_default();
        let primary_message_id = command.message_ids.first().cloned().unwrap_or_default();
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
    pub async fn handle_execute_event_app(&self, ctx: &Ctx, event: Event) -> OperationResponse {
        let request_id = event.request_id.clone();
        let op_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        let tenant = ctx.tenant_id().unwrap_or("0").to_string();
        let conv_id = event.conversation_id.clone();

        let ok_resp = || OperationResponse {
            request_id: request_id.clone(),
            status: Some(RpcStatus {
                code: ErrorCode::Ok.into(),
                message: String::new(),
                ..Default::default()
            }),
        };
        let err_resp = |err: flare_im_core::error::FlareError| OperationResponse {
            request_id: request_id.clone(),
            status: Some(RpcStatus {
                code: err
                    .code()
                    .map(|c| c.as_u32() as i32)
                    .unwrap_or(ErrorCode::Internal as i32),
                message: err.to_string(),
                ..Default::default()
            }),
        };

        match event.payload {
            Some(EventPayload::Recall(r)) => {
                let cmd = AppRecallMessageCommand {
                    message_id: r.server_msg_id,
                    reason: if r.reason.is_empty() {
                        None
                    } else {
                        Some(r.reason)
                    },
                    time_limit_seconds: r.time_limit_seconds,
                    operator_id: op_id.to_string(),
                    tenant_id: tenant.clone(),
                    conversation_id: conv_id.clone(),
                };
                match self.handle_recall_message_app(ctx, &cmd).await {
                    Ok(_) => ok_resp(),
                    Err(e) => err_resp(e),
                }
            }
            Some(EventPayload::Edit(e)) => {
                let cmd = AppEditMessageCommand {
                    message_id: e.server_msg_id,
                    new_content: e.new_content,
                    reason: if e.reason.is_empty() {
                        None
                    } else {
                        Some(e.reason)
                    },
                    show_edited_mark: e.show_edited_mark,
                    edit_version: e.edit_version,
                    operator_id: op_id.to_string(),
                    tenant_id: tenant.clone(),
                    conversation_id: conv_id.clone(),
                };
                match self.handle_edit_message_app(ctx, &cmd).await {
                    Ok(_) => ok_resp(),
                    Err(err) => err_resp(err),
                }
            }
            Some(EventPayload::Delete(d)) => {
                let delete_type = match d.delete_type {
                    Some(2) => DeleteType::Hard,
                    _ => DeleteType::Soft,
                };
                let delete_scope = d
                    .scope
                    .and_then(DeleteScope::from_proto_value)
                    .unwrap_or_else(|| DeleteScope::default_for_type(delete_type));
                if d.server_msg_id.trim().is_empty() {
                    OperationResponse {
                        request_id: request_id.clone(),
                        status: Some(RpcStatus {
                            code: ErrorCode::InvalidArgument as i32,
                            message: "Delete event requires non-empty server_msg_id".to_string(),
                            ..Default::default()
                        }),
                    }
                } else {
                    let cmd = AppDeleteMessageCommand {
                        conversation_id: conv_id.clone(),
                        message_ids: vec![d.server_msg_id],
                        delete_type,
                        delete_scope,
                        reason: d.reason,
                        notify_others: d.notify_others.unwrap_or(true),
                        target_user_id: d.target_user_id.filter(|s| !s.is_empty()),
                        hard_delete: delete_type == DeleteType::Hard,
                        operator_id: op_id.to_string(),
                        tenant_id: tenant.clone(),
                    };
                    match self.handle_delete_message_app(ctx, &cmd).await {
                        Ok(_) => ok_resp(),
                        Err(err) => err_resp(err),
                    }
                }
            }
            Some(EventPayload::Read(r)) => {
                let read_at = r
                    .read_at
                    .as_ref()
                    .and_then(|t| chrono::DateTime::from_timestamp(t.seconds, t.nanos as u32));
                let cmd = AppBatchMarkMessageReadCommand {
                    conversation_id: r.conversation_id.clone(),
                    user_id: op_id.to_string(),
                    message_ids: if r.message_ids.is_empty() {
                        vec![]
                    } else {
                        r.message_ids
                    },
                    read_at: read_at.or_else(|| Some(chrono::Utc::now())),
                    tenant_id: tenant.clone(),
                };
                match self.handle_batch_mark_message_read_app(ctx, &cmd).await {
                    Ok(_) => ok_resp(),
                    Err(err) => err_resp(err),
                }
            }
            Some(EventPayload::Reaction(re)) => {
                if re.action == 1 {
                    let cmd = AppAddReactionCommand {
                        message_id: re.server_msg_id.clone(),
                        emoji: re.emoji.clone(),
                        user_id: op_id.to_string(),
                        tenant_id: tenant.clone(),
                        conversation_id: conv_id.clone(),
                    };
                    match self.handle_add_reaction_app(ctx, &cmd).await {
                        Ok(_) => ok_resp(),
                        Err(err) => err_resp(err),
                    }
                } else {
                    let cmd = AppRemoveReactionCommand {
                        message_id: re.server_msg_id,
                        emoji: re.emoji,
                        user_id: op_id.to_string(),
                        tenant_id: tenant.clone(),
                        conversation_id: conv_id.clone(),
                    };
                    match self.handle_remove_reaction_app(ctx, &cmd).await {
                        Ok(_) => ok_resp(),
                        Err(err) => err_resp(err),
                    }
                }
            }
            Some(EventPayload::Pin(p)) => {
                let expire_at = p
                    .expire_at
                    .as_ref()
                    .and_then(|t| chrono::DateTime::from_timestamp(t.seconds, t.nanos as u32));
                let cmd = AppPinMessageCommand {
                    message_id: p.server_msg_id,
                    reason: p.reason.filter(|s| !s.is_empty()),
                    expire_at,
                    operator_id: op_id.to_string(),
                    tenant_id: tenant.clone(),
                    conversation_id: conv_id.clone(),
                };
                match self.handle_pin_message_app(ctx, &cmd).await {
                    Ok(_) => ok_resp(),
                    Err(err) => err_resp(err),
                }
            }
            Some(EventPayload::Unpin(u)) => {
                let cmd = AppUnpinMessageCommand {
                    message_id: u.server_msg_id,
                    operator_id: op_id.to_string(),
                    tenant_id: tenant.clone(),
                    conversation_id: conv_id.clone(),
                };
                match self.handle_unpin_message_app(ctx, &cmd).await {
                    Ok(_) => ok_resp(),
                    Err(err) => err_resp(err),
                }
            }
            Some(EventPayload::Mark(m)) => {
                let cmd = AppMarkMessageCommand {
                    message_id: m.server_msg_id,
                    mark_type: m.mark_type,
                    color: if m.color.is_empty() {
                        None
                    } else {
                        Some(m.color)
                    },
                    user_id: op_id.to_string(),
                    tenant_id: tenant.clone(),
                    conversation_id: conv_id.clone(),
                };
                match self.handle_mark_message_app(ctx, &cmd).await {
                    Ok(_) => ok_resp(),
                    Err(err) => err_resp(err),
                }
            }
            Some(EventPayload::Unmark(u)) => {
                let cmd = AppUnmarkMessageCommand {
                    message_id: u.server_msg_id,
                    mark_type: u.mark_type,
                    user_id: op_id.to_string(),
                    tenant_id: tenant,
                    conversation_id: conv_id,
                };
                match self.handle_unmark_message_app(ctx, &cmd).await {
                    Ok(_) => ok_resp(),
                    Err(err) => err_resp(err),
                }
            }
            Some(EventPayload::Typing(_)) => ok_resp(),
            _ => OperationResponse {
                request_id,
                status: Some(RpcStatus {
                    code: ErrorCode::UnsupportedOperation as i32,
                    message: "Unsupported event type or missing payload".to_string(),
                    ..Default::default()
                }),
            },
        }
    }

    /// 标记消息直到指定消息已读 - 使用应用层命令
    #[instrument(skip(self, ctx), fields(conversation_id = %command.conversation_id, user_id = %command.user_id))]
    pub async fn handle_mark_messages_read_until_app(
        &self,
        ctx: &Ctx,
        command: &AppMarkMessagesReadUntilCommand,
    ) -> Result<()> {
        let messages_to_mark = self
            .collect_message_ids_until(
                ctx,
                &command.conversation_id,
                &command.until_message_id,
            )
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
        let (_, mut base) = self
            .resolve_and_base(ctx, &command.message_id, fallback)
            .await?;
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
        let (_, mut base) = self
            .resolve_and_base(ctx, &command.message_id, fallback)
            .await?;
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
        let (_, base) = self
            .resolve_and_base(ctx, &command.message_id, None)
            .await?;
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
        let (_, base) = self
            .resolve_and_base(ctx, &command.message_id, fallback)
            .await?;
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
        let (_, base) = self
            .resolve_and_base(ctx, &command.message_id, fallback)
            .await?;
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
        let (_, base) = self
            .resolve_and_base(ctx, &command.message_id, fallback)
            .await?;
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
        let (_, base) = self
            .resolve_and_base(ctx, &command.message_id, fallback)
            .await?;
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
        let (_, base) = self
            .resolve_and_base(ctx, &command.message_id, fallback)
            .await?;
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
        let (_, base) = self
            .resolve_and_base(ctx, &command.message_id, fallback)
            .await?;
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
