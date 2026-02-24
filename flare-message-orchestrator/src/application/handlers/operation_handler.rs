use std::sync::Arc;
use tracing::{error, info, instrument};

use crate::application::{
    commands::{
        AddReactionCommand, AppAddReactionCommand, AppBatchMarkMessageReadCommand, AppDeleteMessageCommand,
        AppEditMessageCommand, AppMarkConversationReadCommand, AppMarkMessageCommand,
        AppMarkMessagesReadUntilCommand, AppMarkAllConversationsReadCommand,
        AppPinMessageCommand, AppRecallMessageCommand,
        AppRemoveReactionCommand, AppUnmarkMessageCommand, AppUnpinMessageCommand, AppGetPinnedMessagesCommand,
        AppGetMarkedMessagesCommand, AppGetThreadsCommand, AppGetThreadRepliesCommand,
        BatchMarkMessageReadCommand, PinMessageCommand,
        DeleteMessageCommand, EditMessageCommand, MarkConversationReadCommand, MarkMessageCommand,
        ReadMessageCommand, RecallMessageCommand, RemoveReactionCommand, UnmarkMessageCommand,
        UnpinMessageCommand,
    },
    handlers::{MessageCommandHandler, MessageQueryHandler},
    queries::QueryMessageQuery,
    utils::OperationMessageBuilder,
};
use flare_server_core::error::Result;
use flare_server_core::context::Context;

/// 消息操作处理器 - 负责处理所有消息操作的业务逻辑
///
/// 在IM系统中，消息操作包括：撤回、编辑、删除、标记已读、反应、置顶等
/// 这些操作通常涉及复杂的业务规则和状态管理，需要集中处理
#[derive(Clone)]
pub struct MessageOperationHandler {
    command_handler: Arc<MessageCommandHandler>,
    query_handler: Arc<MessageQueryHandler>,
}

impl MessageOperationHandler {
    pub fn new(
        command_handler: Arc<MessageCommandHandler>,
        query_handler: Arc<MessageQueryHandler>,
    ) -> Self {
        Self {
            command_handler,
            query_handler,
        }
    }
}

impl MessageOperationHandler {
    /// 标记消息直到指定消息已读 - 使用应用层命令
    #[instrument(skip(self, ctx), fields(conversation_id = %command.conversation_id, user_id = %command.user_id))]
    pub async fn handle_mark_messages_read_until_app(
        &self,
        ctx: &Context,
        command: &AppMarkMessagesReadUntilCommand,
    ) -> Result<()> {
        // 查询会话中直到指定消息ID的所有消息
        // 通过查询服务获取消息列表
        let query = crate::application::queries::QueryMessagesQuery {
            conversation_id: command.conversation_id.clone(),
            limit: Some(1000), // 限制查询数量
            cursor: None,
            start_time: None,
            end_time: None,
        };

        let messages_result = self.query_handler.query_messages_with_pagination(query).await
            .map_err(|e| flare_im_core::error::FlareError::system(&format!("Internal error: {}", e)))?;

        // 找到目标消息之前的全部消息
        let mut messages_to_mark = Vec::new();
        let mut found_target = false;

        for msg in messages_result.messages {
            if msg.server_id == command.until_message_id {
                found_target = true;
                break;
            }
            if !found_target {
                messages_to_mark.push(msg.server_id);
            }
        }

        if !found_target {
            return Err(flare_im_core::error::FlareError::system(
                &format!("Target message not found: {}", command.until_message_id)
            ));
        }

        // 构建批量标记已读命令
        let batch_read_cmd = BatchMarkMessageReadCommand {
            conversation_id: command.conversation_id.clone(),
            user_id: command.user_id.clone(),
            message_ids: messages_to_mark,
            read_at: command.read_at,
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        };

        // 执行批量已读操作
        self.command_handler
            .handle_batch_mark_message_read(batch_read_cmd)
            .await
            .map_err(|e| flare_im_core::error::FlareError::system(&format!("Internal error: {}", e)))?;

        Ok(())
    }

    /// 标记全部会话已读 - 使用应用层命令
    #[instrument(skip(self, ctx), fields(user_id = %command.user_id))]
    pub async fn handle_mark_all_conversations_read_app(
        &self,
        ctx: &Context,
        command: &AppMarkAllConversationsReadCommand,
    ) -> Result<()> {
        // 当前版本暂不支持通过存储服务获取用户所有会话的功能
        // 该功能需要额外的API支持
        
        // 可以通过其他方式实现，例如从会话服务获取用户的所有会话
        // 或者要求前端传入具体的会话列表
        
        Ok(())
    }

    /// 获取置顶消息 - 使用应用层命令
    #[instrument(skip(self, ctx), fields(conversation_id = %command.conversation_id))]
    pub async fn handle_get_pinned_messages_app(
        &self,
        ctx: &Context,
        command: &AppGetPinnedMessagesCommand,
    ) -> Result<Vec<flare_proto::common::Message>> {
        // 当前版本暂不支持直接获取置顶消息
        // 置顶消息功能需要额外的API支持
        
        Ok(Vec::new())
    }

    /// 获取标记消息 - 使用应用层命令
    #[instrument(skip(self, ctx), fields(user_id = %command.user_id))]
    pub async fn handle_get_marked_messages_app(
        &self,
        ctx: &Context,
        command: &AppGetMarkedMessagesCommand,
    ) -> Result<Vec<flare_proto::common::Message>> {
        // 当前版本暂不支持直接获取标记消息
        // 标记消息功能需要额外的API支持
        
        Ok(Vec::new())
    }

    /// 获取话题 - 使用应用层命令
    #[instrument(skip(self, ctx), fields(conversation_id = %command.conversation_id))]
    pub async fn handle_get_threads_app(
        &self,
        ctx: &Context,
        command: &AppGetThreadsCommand,
    ) -> Result<Vec<flare_proto::common::ThreadInfo>> {
        // 当前版本暂不支持直接获取话题
        // 话题功能需要额外的API支持
        
        Ok(Vec::new())
    }

    /// 获取话题回复 - 使用应用层命令
    #[instrument(skip(self, ctx), fields(thread_id = %command.thread_id))]
    pub async fn handle_get_thread_replies_app(
        &self,
        ctx: &Context,
        command: &AppGetThreadRepliesCommand,
    ) -> Result<Vec<flare_proto::common::Message>> {
        // 当前版本暂不支持获取话题回复
        // 话题回复功能需要额外的API支持
        
        Ok(Vec::new())
    }
}

impl MessageOperationHandler {
    /// 撤回消息 - 使用应用层命令
    #[instrument(skip(self, ctx), fields(message_id = %command.message_id))]
    pub async fn handle_recall_message_app(
        &self,
        ctx: &Context,
        command: &AppRecallMessageCommand,
    ) -> Result<(String, i64)> {
        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id.clone())
            .unwrap_or_default();

        // 查询原消息获取 conversation_id
        let original_message = self
            .query_handler
            .query_message(QueryMessageQuery {
                message_id: command.message_id.clone(),
                conversation_id: String::new(),
            })
            .await
            .map_err(|e| {
                if e.to_string().contains("not found") {
                    flare_im_core::error::FlareError::localized(
                        flare_im_core::error::ErrorCode::MessageNotFound,
                        format!("Message not found: {}", command.message_id)
                    )
                } else {
                    flare_im_core::error::FlareError::system(&format!("Internal error: {}", e))
                }
            })?;

        let conversation_id = original_message.conversation_id.clone();

        // 使用服务器ID作为操作的目标ID
        let target_message_id = if !original_message.server_id.is_empty() {
            original_message.server_id.clone()
        } else {
            command.message_id.clone()
        };

        // 构建内部应用命令
        let recall_cmd = RecallMessageCommand {
            base: crate::application::commands::MessageOperationCommand {
                message_id: target_message_id,
                operator_id: operator_id.clone(),
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: conversation_id.clone(),
            },
            reason: command.reason.clone(),
            time_limit_seconds: command.time_limit_seconds,
        };

        // 通过命令处理器执行撤回操作
        self.command_handler
            .handle_recall_message(recall_cmd)
            .await
            .map_err(|e| flare_im_core::error::FlareError::system(&format!("Internal error: {}", e)))?;

        // 操作已处理，由底层服务负责通知相关客户端
        Ok((command.message_id.clone(), 0))
    }

    /// 编辑消息 - 使用应用层命令
    #[instrument(skip(self, ctx), fields(message_id = %command.message_id))]
    pub async fn handle_edit_message_app(
        &self,
        ctx: &Context,
        command: &AppEditMessageCommand,
    ) -> Result<(String, i64)> {
        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id.clone())
            .unwrap_or_default();

        // 查询原消息获取 conversation_id
        let original_message = self
            .query_handler
            .query_message(QueryMessageQuery {
                message_id: command.message_id.clone(),
                conversation_id: String::new(),
            })
            .await
            .map_err(|e| {
                if e.to_string().contains("not found") {
                    flare_im_core::error::FlareError::localized(
                        flare_im_core::error::ErrorCode::MessageNotFound,
                        format!("Message not found: {}", command.message_id)
                    )
                } else {
                    flare_im_core::error::FlareError::system(&format!("Internal error: {}", e))
                }
            })?;

        let conversation_id = original_message.conversation_id.clone();

        // 使用服务器ID作为操作的目标ID
        let target_message_id = if !original_message.server_id.is_empty() {
            original_message.server_id.clone()
        } else {
            command.message_id.clone()
        };

        // 构建内部应用命令
        let edit_cmd = EditMessageCommand {
            base: crate::application::commands::MessageOperationCommand {
                message_id: target_message_id,
                operator_id: operator_id.clone(),
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: conversation_id.clone(),
            },
            new_content: command.new_content.clone(),
            reason: command.reason.clone(),
        };

        // 通过命令处理器执行编辑操作
        self.command_handler
            .handle_edit_message(edit_cmd)
            .await
            .map_err(|e| flare_im_core::error::FlareError::system(&format!("Internal error: {}", e)))?;

        // 操作已处理，由底层服务负责通知相关客户端
        Ok((command.message_id.clone(), 0))
    }

    /// 删除消息 - 使用应用层命令
    #[instrument(skip(self, ctx), fields(conversation_id = %command.conversation_id))]
    pub async fn handle_delete_message_app(
        &self,
        ctx: &Context,
        command: &AppDeleteMessageCommand,
    ) -> Result<(bool, i32)> {
        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id.clone())
            .unwrap_or_default();

        let delete_type = command.delete_type;

        // 构建内部应用命令
        let delete_cmd = DeleteMessageCommand {
            base: crate::application::commands::MessageOperationCommand {
                message_id: String::new(), // 批量删除不需要特定消息ID
                operator_id: operator_id.clone(),
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: command.conversation_id.clone(),
            },
            delete_type,
            reason: command.reason.clone(),

            message_ids: command.message_ids.clone(),
            notify_others: command.notify_others,
        };

        // 通过命令处理器执行删除操作
        self.command_handler
            .handle_delete_message(delete_cmd)
            .await
            .map_err(|e| flare_im_core::error::FlareError::system(&format!("Internal error: {}", e)))?;

        Ok((true, command.message_ids.len() as i32))
    }

    /// 标记消息已读
    #[instrument(skip(self, ctx), fields(message_id = %command.message_id))]
    pub async fn handle_mark_message_read_app(
        &self,
        ctx: &Context,
        command: &AppMarkMessageCommand,
    ) -> Result<()> {
        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id.clone())
            .unwrap_or_default();

        // 查询原消息获取 conversation_id
        let original_message = self
            .query_handler
            .query_message(QueryMessageQuery {
                message_id: command.message_id.clone(),
                conversation_id: String::new(),
            })
            .await
            .map_err(|e| {
                if e.to_string().contains("not found") {
                    flare_im_core::error::FlareError::localized(
                        flare_im_core::error::ErrorCode::MessageNotFound,
                        format!("Message not found: {}", command.message_id)
                    )
                } else {
                    flare_im_core::error::FlareError::system(&format!("Internal error: {}", e))
                }
            })?;

        let conversation_id = original_message.conversation_id.clone();

        // 构建内部应用命令
        let read_cmd = ReadMessageCommand {
            base: crate::application::commands::MessageOperationCommand {
                message_id: command.message_id.clone(),
                operator_id: operator_id.clone(),
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: conversation_id.clone(),
            },
            message_ids: vec![command.message_id.clone()],
            read_at: Some(chrono::Utc::now()),
            burn_after_read: false,
        };

        // 执行已读操作
        self.command_handler.handle_read_message(read_cmd).await
            .map_err(|e| flare_im_core::error::FlareError::system(&format!("Internal error: {}", e)))?;
        
        Ok(())
    }

    /// 批量标记消息已读
    #[instrument(skip(self, ctx), fields(conversation_id = %command.conversation_id))]
    pub async fn handle_batch_mark_message_read_app(
        &self,
        ctx: &Context,
        command: &AppBatchMarkMessageReadCommand,
    ) -> Result<i32> {
        let batch_read_cmd = BatchMarkMessageReadCommand {
            conversation_id: command.conversation_id.clone(),
            user_id: command.user_id.clone(),
            message_ids: command.message_ids.clone(),
            read_at: command.read_at,
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        };

        // 执行批量已读操作
        match self.command_handler.handle_batch_mark_message_read(batch_read_cmd).await {
            Ok(count) => Ok(count),
            Err(e) => Err(flare_im_core::error::FlareError::system(&format!("Internal error: {}", e))),
        }
    }

    /// 标记会话已读
    #[instrument(skip(self, ctx), fields(conversation_id = %command.conversation_id))]
    pub async fn handle_mark_conversation_read_app(
        &self,
        ctx: &Context,
        command: &AppMarkConversationReadCommand,
    ) -> Result<()> {
        let cmd = MarkConversationReadCommand {
            conversation_id: command.conversation_id.clone(),
            user_id: command.user_id.clone(),
            read_at: command.read_at,
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        };

        // 执行标记会话已读操作
        self.command_handler.handle_mark_conversation_read(cmd).await
            .map_err(|e| flare_im_core::error::FlareError::system(e.to_string()))
    }

    /// 添加反应
    #[instrument(skip(self, ctx), fields(message_id = %command.message_id))]
    pub async fn handle_add_reaction_app(
        &self,
        ctx: &Context,
        command: &AppAddReactionCommand,
    ) -> Result<()> {
        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id.clone())
            .unwrap_or_default();

        // 查询原消息获取 conversation_id
        let original_message = self
            .query_handler
            .query_message(QueryMessageQuery {
                message_id: command.message_id.clone(),
                conversation_id: String::new(),
            })
            .await
            .map_err(|e| {
                if e.to_string().contains("not found") {
                    flare_im_core::error::FlareError::localized(
                        flare_im_core::error::ErrorCode::MessageNotFound,
                        format!("Message not found: {}", command.message_id)
                    )
                } else {
                    flare_im_core::error::FlareError::system(&format!("Internal error: {}", e))
                }
            })?;

        let conversation_id = original_message.conversation_id.clone();

        // 构建内部应用命令
        let add_reaction_cmd = AddReactionCommand {
            base: crate::application::commands::MessageOperationCommand {
                message_id: command.message_id.clone(),
                operator_id: operator_id.clone(),
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: conversation_id.clone(),
            },
            emoji: command.emoji.clone(),
        };

        // 通过命令处理器执行添加反应操作
        self.command_handler
            .handle_add_reaction(add_reaction_cmd)
            .await
            .map_err(|e| flare_im_core::error::FlareError::system(&format!("Internal error: {}", e)))?;

        Ok(())
    }

    /// 移除反应
    #[instrument(skip(self, ctx), fields(message_id = %command.message_id))]
    pub async fn handle_remove_reaction_app(
        &self,
        ctx: &Context,
        command: &AppRemoveReactionCommand,
    ) -> Result<()> {
        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id.clone())
            .unwrap_or_default();

        // 查询原消息获取 conversation_id
        let original_message = self
            .query_handler
            .query_message(QueryMessageQuery {
                message_id: command.message_id.clone(),
                conversation_id: String::new(),
            })
            .await
            .map_err(|e| {
                if e.to_string().contains("not found") {
                    flare_im_core::error::FlareError::localized(
                        flare_im_core::error::ErrorCode::MessageNotFound,
                        format!("Message not found: {}", command.message_id)
                    )
                } else {
                    flare_im_core::error::FlareError::system(&format!("Internal error: {}", e))
                }
            })?;

        let conversation_id = original_message.conversation_id.clone();

        // 构建内部应用命令
        let remove_reaction_cmd = RemoveReactionCommand {
            base: crate::application::commands::MessageOperationCommand {
                message_id: command.message_id.clone(),
                operator_id: operator_id.clone(),
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: conversation_id.clone(),
            },
            emoji: command.emoji.clone(),
        };

        // 通过命令处理器执行移除反应操作
        self.command_handler
            .handle_remove_reaction(remove_reaction_cmd)
            .await
            .map_err(|e| flare_im_core::error::FlareError::system(&format!("Internal error: {}", e)))?;

        Ok(())
    }

    /// 置顶消息
    #[instrument(skip(self, ctx), fields(message_id = %command.message_id))]
    pub async fn handle_pin_message_app(
        &self,
        ctx: &Context,
        command: &AppPinMessageCommand,
    ) -> Result<()> {
        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id.clone())
            .unwrap_or_default();

        // 查询原消息获取 conversation_id
        let original_message = self
            .query_handler
            .query_message(QueryMessageQuery {
                message_id: command.message_id.clone(),
                conversation_id: String::new(),
            })
            .await
            .map_err(|e| {
                if e.to_string().contains("not found") {
                    flare_im_core::error::FlareError::localized(
                        flare_im_core::error::ErrorCode::MessageNotFound,
                        format!("Message not found: {}", command.message_id)
                    )
                } else {
                    flare_im_core::error::FlareError::system(&format!("Internal error: {}", e))
                }
            })?;

        let conversation_id = original_message.conversation_id.clone();

        // 构建内部应用命令
        let pin_cmd = PinMessageCommand {
            base: crate::application::commands::MessageOperationCommand {
                message_id: command.message_id.clone(),
                operator_id: operator_id.clone(),
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: conversation_id.clone(),
            },
            reason: command.reason.clone(),
            expire_at: command.expire_at,
        };

        // 通过命令处理器执行置顶操作
        self.command_handler
            .handle_pin_message(pin_cmd)
            .await
            .map_err(|e| flare_im_core::error::FlareError::system(&format!("Internal error: {}", e)))?;

        Ok(())
    }

    /// 取消置顶消息
    #[instrument(skip(self, ctx), fields(message_id = %command.message_id))]
    pub async fn handle_unpin_message_app(
        &self,
        ctx: &Context,
        command: &AppUnpinMessageCommand,
    ) -> Result<()> {
        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id.clone())
            .unwrap_or_default();

        // 查询原消息获取 conversation_id
        let original_message = self
            .query_handler
            .query_message(QueryMessageQuery {
                message_id: command.message_id.clone(),
                conversation_id: String::new(),
            })
            .await
            .map_err(|e| {
                if e.to_string().contains("not found") {
                    flare_im_core::error::FlareError::localized(
                        flare_im_core::error::ErrorCode::MessageNotFound,
                        format!("Message not found: {}", command.message_id)
                    )
                } else {
                    flare_im_core::error::FlareError::system(&format!("Internal error: {}", e))
                }
            })?;

        let conversation_id = original_message.conversation_id.clone();

        // 构建内部应用命令
        let unpin_cmd = UnpinMessageCommand {
            base: crate::application::commands::MessageOperationCommand {
                message_id: command.message_id.clone(),
                operator_id: operator_id.clone(),
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: conversation_id.clone(),
            },
        };

        // 通过命令处理器执行取消置顶操作
        self.command_handler
            .handle_unpin_message(unpin_cmd)
            .await
            .map_err(|e| flare_im_core::error::FlareError::system(&format!("Internal error: {}", e)))?;

        Ok(())
    }

    /// 标记消息
    #[instrument(skip(self, ctx), fields(message_id = %command.message_id))]
    pub async fn handle_mark_message_app(
        &self,
        ctx: &Context,
        command: &AppMarkMessageCommand,
    ) -> Result<()> {
        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id.clone())
            .unwrap_or_default();

        // 查询原消息获取 conversation_id
        let original_message = self
            .query_handler
            .query_message(QueryMessageQuery {
                message_id: command.message_id.clone(),
                conversation_id: String::new(),
            })
            .await
            .map_err(|e| {
                if e.to_string().contains("not found") {
                    flare_im_core::error::FlareError::localized(
                        flare_im_core::error::ErrorCode::MessageNotFound,
                        format!("Message not found: {}", command.message_id)
                    )
                } else {
                    flare_im_core::error::FlareError::system(&format!("Internal error: {}", e))
                }
            })?;

        let conversation_id = original_message.conversation_id.clone();

        // 构建内部应用命令
        let mark_cmd = MarkMessageCommand {
            base: crate::application::commands::MessageOperationCommand {
                message_id: command.message_id.clone(),
                operator_id: operator_id.clone(),
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: conversation_id.clone(),
            },
            mark_type: command.mark_type,

        };

        // 通过命令处理器执行标记操作
        self.command_handler
            .handle_mark_message(mark_cmd)
            .await
            .map_err(|e| flare_im_core::error::FlareError::system(&format!("Internal error: {}", e)))?;

        Ok(())
    }

    /// 取消标记消息
    #[instrument(skip(self, ctx), fields(message_id = %command.message_id))]
    pub async fn handle_unmark_message_app(
        &self,
        ctx: &Context,
        command: &AppUnmarkMessageCommand,
    ) -> Result<()> {
        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id.clone())
            .unwrap_or_default();

        // 查询原消息获取 conversation_id
        let original_message = self
            .query_handler
            .query_message(QueryMessageQuery {
                message_id: command.message_id.clone(),
                conversation_id: String::new(),
            })
            .await
            .map_err(|e| {
                if e.to_string().contains("not found") {
                    flare_im_core::error::FlareError::localized(
                        flare_im_core::error::ErrorCode::MessageNotFound,
                        format!("Message not found: {}", command.message_id)
                    )
                } else {
                    flare_im_core::error::FlareError::system(&format!("Internal error: {}", e))
                }
            })?;

        let conversation_id = original_message.conversation_id.clone();

        // 构建内部应用命令
        let unmark_cmd = UnmarkMessageCommand {
            base: crate::application::commands::MessageOperationCommand {
                message_id: command.message_id.clone(),
                operator_id: operator_id.clone(),
                timestamp: chrono::Utc::now(),
                tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                conversation_id: conversation_id.clone(),
            },
            mark_type: if command.mark_type < 0 { None } else { Some(command.mark_type) },
            user_id: command.user_id.clone(),
        };

        // 通过命令处理器执行取消标记操作
        self.command_handler
            .handle_unmark_message(unmark_cmd)
            .await
            .map_err(|e| flare_im_core::error::FlareError::system(&format!("Internal error: {}", e)))?;

        Ok(())
    }
}
