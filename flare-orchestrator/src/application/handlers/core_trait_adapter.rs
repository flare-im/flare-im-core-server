//! IM 核心 MessageCommandHandler 适配器
//!
//! 将 flare_im_core 的 SendMessageCommand / MessageCommand 转为 Orchestrator 内部命令，
//! 委托现有 MessageCommandHandler 执行，并返回 core 的 SendAckResult / OperationResult。
//! 供 Gateway 注入后长连接发消息/操作统一走 Orchestrator。

use std::sync::Arc;

use flare_im_core::{
    DeleteType as CoreDeleteType, MarkType as CoreMarkType,
    MessageCommand, MessageCommandHandler as CoreMessageCommandHandler,
    OperationResult, ReactionAction as CoreReactionAction,
    SendAckResult, SendMessageCommand,
};
use flare_proto::common::Message;
use flare_server_core::context::{Context, Ctx};

use crate::application::commands::{
    AddReactionCommand, DeleteMessageCommand, DeleteScope, DeleteType, EditMessageCommand,
    MarkMessageCommand, MessageOperationCommand, PinMessageCommand, ReadMessageCommand,
    RecallMessageCommand, RemoveReactionCommand, UnmarkMessageCommand, UnpinMessageCommand,
};
use crate::application::handlers::MessageCommandHandler;

/// 适配器：实现 core MessageCommandHandler，内部委托 Orchestrator 的 MessageCommandHandler
pub struct CoreMessageCommandHandlerAdapter {
    inner: Arc<MessageCommandHandler>,
}

impl CoreMessageCommandHandlerAdapter {
    pub fn new(inner: Arc<MessageCommandHandler>) -> Self {
        Self { inner }
    }
}

impl CoreMessageCommandHandler for CoreMessageCommandHandlerAdapter {
    async fn handle_send_message(
        &self,
        ctx: &Ctx,
        cmd: &SendMessageCommand,
    ) -> anyhow::Result<SendAckResult> {
        let ctx = std::sync::Arc::new(Context::root());
        let message = Message {
            server_id: String::new(),
            conversation_id: cmd.conversation_id.as_str().to_string(),
            client_msg_id: cmd.client_msg_id.as_str().to_string(),
            sender_id: cmd.sender_id.as_str().to_string(),
            sender_name: String::new(),
            sender_avatar: String::new(),
            source: 1, // MessageSource::User
            seq: 0,
            timestamp: Some(prost_types::Timestamp {
                seconds: chrono::Utc::now().timestamp(),
                nanos: 0,
            }),
            conversation_type: 0, // Unspecified
            message_type: cmd.message_type,
            channel_id: cmd.receiver_id.clone().unwrap_or_default(), // 单聊时 channel_id=对方 user_id
            content: cmd.content.clone(),
            status: 1, // MessageStatus::Created
            offline_push_info: None,
            extra: cmd.extra.clone(),
            extensions: std::collections::HashMap::new(),
        };
        let send_cmd = crate::application::commands::SendMessageCommand {
            message,
            conversation_id: cmd.conversation_id.as_str().to_string(),
            sync: false,
        };
        match self.inner.handle_send_message(&ctx, send_cmd).await {
            Ok((server_msg_id, seq)) => Ok(SendAckResult {
                success: true,
                server_msg_id: Some(server_msg_id),
                seq: Some(seq),
                error_code: None,
                error_message: None,
            }),
            Err(e) => Ok(SendAckResult {
                success: false,
                server_msg_id: None,
                seq: None,
                error_code: Some(500),
                error_message: Some(e.to_string()),
            }),
        }
    }

    async fn handle_message_operation(
        &self,
        ctx: &Ctx,
        cmd: &MessageCommand,
    ) -> anyhow::Result<Option<OperationResult>> {
        let ctx = std::sync::Arc::new(Context::root());
        let base = |conversation_id: &str, server_msg_id: &str, operator_id: &str| MessageOperationCommand {
            message_id: server_msg_id.to_string(),
            operator_id: operator_id.to_string(),
            timestamp: chrono::Utc::now(),
            tenant_id: "0".to_string(),
            conversation_id: conversation_id.to_string(),
        };
        let request_id = match cmd {
            MessageCommand::Recall { request_id, .. }
            | MessageCommand::Edit { request_id, .. }
            | MessageCommand::Delete { request_id, .. }
            | MessageCommand::ReadReceipt { request_id, .. }
            | MessageCommand::Reaction { request_id, .. }
            | MessageCommand::Pin { request_id, .. }
            | MessageCommand::Unpin { request_id, .. }
            | MessageCommand::Mark { request_id, .. }
            | MessageCommand::Unmark { request_id, .. } => request_id.clone(),
            MessageCommand::Send(_) => return Ok(None),
        };

        let result = match cmd {
            MessageCommand::Recall {
                conversation_id,
                server_msg_id,
                operator_id,
                reason,
                request_id: _,
            } => {
                let recall_cmd = RecallMessageCommand {
                    base: base(conversation_id.as_str(), server_msg_id.as_str(), operator_id.as_str()),
                    reason: reason.clone(),
                    time_limit_seconds: None,
                    allow_admin_override: false,
                };
                self.inner.handle_recall_message(&ctx, recall_cmd).await
            }
            MessageCommand::Edit {
                conversation_id,
                server_msg_id,
                operator_id,
                new_content,
                request_id: _,
            } => {
                let edit_cmd = EditMessageCommand {
                    base: base(conversation_id.as_str(), server_msg_id.as_str(), operator_id.as_str()),
                    new_content: new_content.clone(),
                    reason: None,
                    allow_admin_override: false,
                };
                self.inner.handle_edit_message(&ctx, edit_cmd).await
            }
            MessageCommand::Delete {
                conversation_id,
                server_msg_id,
                operator_id,
                delete_type,
                request_id: _,
            } => {
                let (delete_type_impl, scope) = match delete_type {
                    CoreDeleteType::Soft => (DeleteType::Soft, DeleteScope::UserPrivate),
                    CoreDeleteType::Hard => (DeleteType::Hard, DeleteScope::ConversationGlobal),
                };
                let delete_cmd = DeleteMessageCommand {
                    base: base(conversation_id.as_str(), server_msg_id.as_str(), operator_id.as_str()),
                    delete_type: delete_type_impl,
                    delete_scope: scope,
                    reason: None,
                    message_ids: vec![server_msg_id.as_str().to_string()],
                    notify_others: false,
                    target_user_id: Some(operator_id.as_str().to_string()),
                    allow_admin_override: false,
                };
                self.inner.handle_delete_message(&ctx, delete_cmd).await
            }
            MessageCommand::ReadReceipt {
                conversation_id,
                user_id,
                read_seq: _read_seq,
                message_ids,
                request_id: _,
            } => {
                let msg_ids = message_ids.clone().unwrap_or_else(|| vec![]);
                let read_cmd = ReadMessageCommand {
                    base: MessageOperationCommand {
                        message_id: String::new(),
                        operator_id: user_id.as_str().to_string(),
                        timestamp: chrono::Utc::now(),
                        tenant_id: "0".to_string(),
                        conversation_id: conversation_id.as_str().to_string(),
                    },
                    message_ids: if msg_ids.is_empty() {
                        vec!["".to_string()]
                    } else {
                        msg_ids
                    },
                    read_at: Some(chrono::Utc::now()),
                    burn_after_read: false,
                };
                self.inner.handle_read_message(&ctx, read_cmd).await
            }
            MessageCommand::Reaction {
                conversation_id,
                server_msg_id,
                user_id,
                emoji,
                action,
                request_id: _,
            } => {
                let reaction_cmd = match action {
                    CoreReactionAction::Add => {
                        let add_cmd = AddReactionCommand {
                            base: base(conversation_id.as_str(), server_msg_id.as_str(), user_id.as_str()),
                            emoji: emoji.clone(),
                        };
                        self.inner.handle_add_reaction(&ctx, add_cmd).await.map(|_| ())
                    }
                    CoreReactionAction::Remove => {
                        let remove_cmd = RemoveReactionCommand {
                            base: base(conversation_id.as_str(), server_msg_id.as_str(), user_id.as_str()),
                            emoji: emoji.clone(),
                        };
                        self.inner.handle_remove_reaction(&ctx, remove_cmd).await.map(|_| ())
                    }
                };
                reaction_cmd
            }
            MessageCommand::Pin {
                conversation_id,
                server_msg_id,
                pinned_by,
                request_id: _,
            } => {
                let pin_cmd = PinMessageCommand {
                    base: base(conversation_id.as_str(), server_msg_id.as_str(), pinned_by.as_str()),
                    reason: None,
                    expire_at: None,
                };
                self.inner.handle_pin_message(&ctx, pin_cmd).await
            }
            MessageCommand::Unpin {
                conversation_id,
                server_msg_id,
                request_id: _,
            } => {
                let unpin_cmd = UnpinMessageCommand {
                    base: base(conversation_id.as_str(), server_msg_id.as_str(), "system"),
                };
                self.inner.handle_unpin_message(&ctx, unpin_cmd).await
            }
            MessageCommand::Mark {
                conversation_id,
                server_msg_id,
                user_id,
                mark_type,
                request_id: _,
            } => {
                let mark_type_i32 = match mark_type {
                    CoreMarkType::Important => 0,
                    CoreMarkType::Todo => 1,
                    CoreMarkType::Done => 2,
                    CoreMarkType::Custom => 3,
                };
                let mark_cmd = MarkMessageCommand {
                    base: base(conversation_id.as_str(), server_msg_id.as_str(), user_id.as_str()),
                    mark_type: mark_type_i32,
                };
                self.inner.handle_mark_message(&ctx, mark_cmd).await
            }
            MessageCommand::Unmark {
                conversation_id,
                server_msg_id,
                user_id,
                mark_type,
                request_id: _,
            } => {
                let mark_type_i32 = match mark_type {
                    CoreMarkType::Important => 0,
                    CoreMarkType::Todo => 1,
                    CoreMarkType::Done => 2,
                    CoreMarkType::Custom => 3,
                };
                let unmark_cmd = UnmarkMessageCommand {
                    base: base(conversation_id.as_str(), server_msg_id.as_str(), user_id.as_str()),
                    mark_type: Some(mark_type_i32),
                    user_id: user_id.as_str().to_string(),
                };
                self.inner.handle_unmark_message(&ctx, unmark_cmd).await
            }
            MessageCommand::Send(_) => return Ok(None),
        };

        let op_result = match result {
            Ok(()) => OperationResult {
                request_id,
                success: true,
                error_code: None,
                error_message: None,
            },
            Err(e) => OperationResult {
                request_id,
                success: false,
                error_code: Some(500),
                error_message: Some(e.to_string()),
            },
        };
        Ok(Some(op_result))
    }
}
