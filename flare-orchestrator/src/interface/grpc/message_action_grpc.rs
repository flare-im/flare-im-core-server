//! `MessageActionService` gRPC 实现：撤回、编辑、删除、已读、反应、置顶、标记等。

use std::sync::Arc;

use crate::application::handlers::MessageOperationHandler;
use flare_im_core::error::ok_status;
use flare_proto::message::{
    AddReactionRequest as MessageAddReactionRequest,
    AddReactionResponse as MessageAddReactionResponse,
    BatchMarkMessageReadRequest as MessageBatchMarkMessageReadRequest,
    BatchMarkMessageReadResponse as MessageBatchMarkMessageReadResponse,
    DeleteMessageRequest as MessageDeleteMessageRequest,
    DeleteMessageResponse as MessageDeleteMessageResponse,
    EditMessageRequest as MessageEditMessageRequest,
    EditMessageResponse as MessageEditMessageResponse,
    MarkAllConversationsReadRequest as MessageMarkAllConversationsReadRequest,
    MarkAllConversationsReadResponse as MessageMarkAllConversationsReadResponse,
    MarkConversationReadRequest as MessageMarkConversationReadRequest,
    MarkConversationReadResponse as MessageMarkConversationReadResponse,
    MarkMessageReadRequest as MessageMarkMessageReadRequest,
    MarkMessageReadResponse as MessageMarkMessageReadResponse,
    MarkMessageRequest as MessageMarkMessageRequest,
    MarkMessageResponse as MessageMarkMessageResponse,
    MarkMessagesReadUntilRequest as MessageMarkMessagesReadUntilRequest,
    MarkMessagesReadUntilResponse as MessageMarkMessagesReadUntilResponse,
    PinMessageRequest as MessagePinMessageRequest, PinMessageResponse as MessagePinMessageResponse,
    RecallMessageRequest as MessageRecallMessageRequest,
    RecallMessageResponse as MessageRecallMessageResponse,
    RemoveReactionRequest as MessageRemoveReactionRequest,
    RemoveReactionResponse as MessageRemoveReactionResponse,
    UnmarkMessageRequest as MessageUnmarkMessageRequest,
    UnmarkMessageResponse as MessageUnmarkMessageResponse,
    UnpinMessageRequest as MessageUnpinMessageRequest,
    UnpinMessageResponse as MessageUnpinMessageResponse,
};
use prost_types;
use tonic::{Request, Response, Status};
use tracing::{error, instrument};

use crate::application::commands::{
    AppAddReactionCommand, AppBatchMarkMessageReadCommand, AppDeleteMessageCommand,
    AppEditMessageCommand, AppMarkAllConversationsReadCommand, AppMarkConversationReadCommand,
    AppMarkMessageCommand, AppMarkMessagesReadUntilCommand, AppPinMessageCommand,
    AppRecallMessageCommand, AppRemoveReactionCommand, AppUnmarkMessageCommand,
    AppUnpinMessageCommand, DeleteScope,
};
use flare_proto::message::message_action_service_server::MessageActionService;
use flare_proto::message_content_ext::MessageContentExt;
use flare_server_core::utils::require_ctx_from_request;

/// 消息操作 gRPC：撤回、编辑、删除、已读、反应、置顶、标记等。
#[derive(Clone)]
pub struct MessageActionGrpcHandler {
    operation_handler: Arc<MessageOperationHandler>,
}

impl MessageActionGrpcHandler {
    pub fn new(operation_handler: Arc<MessageOperationHandler>) -> Self {
        Self { operation_handler }
    }
}

#[tonic::async_trait]
impl MessageActionService for MessageActionGrpcHandler {
    #[instrument(skip(self, request))]
    async fn recall_message(
        &self,
        request: Request<MessageRecallMessageRequest>,
    ) -> Result<Response<MessageRecallMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let app_command = AppRecallMessageCommand {
            message_id: req.message_id.clone(),
            reason: if req.reason.is_empty() {
                None
            } else {
                Some(req.reason.clone())
            },
            time_limit_seconds: if req.recall_time_limit_seconds > 0 {
                Some(req.recall_time_limit_seconds)
            } else {
                None
            },
            operator_id: ctx
                .actor()
                .map(|a| a.actor_id().to_string())
                .unwrap_or_default(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(),
        };

        match self
            .operation_handler
            .handle_recall_message_app(&ctx, &app_command)
            .await
        {
            Ok((_message_id, _seq)) => {
                let now = chrono::Utc::now();
                Ok(Response::new(MessageRecallMessageResponse {
                    success: true,
                    error_message: String::new(),
                    recalled_at: Some(prost_types::Timestamp {
                        seconds: now.timestamp(),
                        nanos: now.timestamp_subsec_nanos() as i32,
                    }),
                    status: Some(ok_status()),
                }))
            }
            Err(err) => {
                error!(error = %err, "Failed to recall message");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn edit_message(
        &self,
        request: Request<MessageEditMessageRequest>,
    ) -> Result<Response<MessageEditMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let app_command = AppEditMessageCommand {
            message_id: req.message_id.clone(),
            new_content: req
                .new_content
                .as_ref()
                .and_then(|content| content.encode_to_bytes().ok())
                .unwrap_or_default(),
            reason: if req.reason.is_empty() {
                None
            } else {
                Some(req.reason.clone())
            },
            show_edited_mark: req.show_edited_mark,
            edit_version: req.edit_version,
            operator_id: ctx
                .actor()
                .map(|a| a.actor_id().to_string())
                .unwrap_or_default(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(),
        };

        match self
            .operation_handler
            .handle_edit_message_app(&ctx, &app_command)
            .await
        {
            Ok((_message_id, _seq)) => {
                let now = chrono::Utc::now();
                Ok(Response::new(MessageEditMessageResponse {
                    success: true,
                    error_message: String::new(),
                    message_id: req.message_id,
                    edit_version: req.edit_version,
                    edited_at: Some(prost_types::Timestamp {
                        seconds: now.timestamp(),
                        nanos: now.timestamp_subsec_nanos() as i32,
                    }),
                    status: Some(ok_status()),
                }))
            }
            Err(err) => {
                error!(error = %err, "Failed to edit message");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn delete_message(
        &self,
        request: Request<MessageDeleteMessageRequest>,
    ) -> Result<Response<MessageDeleteMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let delete_type = if req.delete_type == 2 {
            crate::application::commands::DeleteType::Hard
        } else {
            crate::application::commands::DeleteType::Soft
        };
        let delete_scope = req
            .scope
            .and_then(DeleteScope::from_proto_value)
            .unwrap_or_else(|| DeleteScope::default_for_type(delete_type));
        let operator_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        let app_command = AppDeleteMessageCommand {
            message_ids: req.message_ids.clone(),
            conversation_id: req.conversation_id.clone(),
            delete_type,
            delete_scope,
            reason: if req.reason.is_empty() {
                None
            } else {
                Some(req.reason.clone())
            },
            notify_others: req.notify_others,
            target_user_id: Some(operator_id.to_string()),
            hard_delete: req.delete_type == 2,
            operator_id: operator_id.to_string(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        };

        match self
            .operation_handler
            .handle_delete_message_app(&ctx, &app_command)
            .await
        {
            Ok((success, deleted_count)) => Ok(Response::new(MessageDeleteMessageResponse {
                success,
                deleted_count,
                status: Some(ok_status()),
            })),
            Err(err) => {
                error!(error = %err, "Failed to delete message");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn mark_message_read(
        &self,
        request: Request<MessageMarkMessageReadRequest>,
    ) -> Result<Response<MessageMarkMessageReadResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        let app_command = AppMarkMessageCommand {
            message_id: req.message_id.clone(),
            user_id: user_id.to_string(),
            mark_type: 0,
            color: None,
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(),
        };

        match self
            .operation_handler
            .handle_mark_message_read_app(&ctx, &app_command)
            .await
        {
            Ok(()) => {
                let now = chrono::Utc::now();
                Ok(Response::new(MessageMarkMessageReadResponse {
                    success: true,
                    error_message: String::new(),
                    read_at: Some(prost_types::Timestamp {
                        seconds: now.timestamp(),
                        nanos: now.timestamp_subsec_nanos() as i32,
                    }),
                    burned_at: None,
                    status: Some(ok_status()),
                }))
            }
            Err(err) => {
                error!(error = %err, "Failed to mark message as read");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn batch_mark_message_read(
        &self,
        request: Request<MessageBatchMarkMessageReadRequest>,
    ) -> Result<Response<MessageBatchMarkMessageReadResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        let app_command = AppBatchMarkMessageReadCommand {
            conversation_id: req.conversation_id.clone(),
            user_id: user_id.to_string(),
            message_ids: req.message_ids.clone(),
            read_at: req.read_at.clone().map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(|| chrono::Utc::now())
            }),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        };

        match self
            .operation_handler
            .handle_batch_mark_message_read_app(&ctx, &app_command)
            .await
        {
            Ok(read_count) => {
                let now = chrono::Utc::now();
                Ok(Response::new(MessageBatchMarkMessageReadResponse {
                    success: read_count > 0,
                    error_message: if read_count == 0 {
                        "No messages marked as read".to_string()
                    } else {
                        String::new()
                    },
                    read_count,
                    read_at: Some(prost_types::Timestamp {
                        seconds: now.timestamp(),
                        nanos: now.timestamp_subsec_nanos() as i32,
                    }),
                    status: Some(ok_status()),
                }))
            }
            Err(err) => {
                error!(error = %err, "Failed to batch mark messages as read");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn mark_messages_read_until(
        &self,
        request: Request<MessageMarkMessagesReadUntilRequest>,
    ) -> Result<Response<MessageMarkMessagesReadUntilResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        let app_command = AppMarkMessagesReadUntilCommand {
            conversation_id: req.conversation_id.clone(),
            user_id: user_id.to_string(),
            until_message_id: req.until_message_id.clone(),
            read_at: req.read_at.clone().map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(|| chrono::Utc::now())
            }),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        };

        match self
            .operation_handler
            .handle_mark_messages_read_until_app(&ctx, &app_command)
            .await
        {
            Ok(()) => {
                let now = chrono::Utc::now();
                Ok(Response::new(MessageMarkMessagesReadUntilResponse {
                    success: true,
                    error_message: String::new(),
                    read_count: 0,
                    read_at: Some(prost_types::Timestamp {
                        seconds: now.timestamp(),
                        nanos: now.timestamp_subsec_nanos() as i32,
                    }),
                    status: Some(ok_status()),
                }))
            }
            Err(err) => {
                error!(error = %err, "Failed to mark messages read until");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn mark_conversation_read(
        &self,
        request: Request<MessageMarkConversationReadRequest>,
    ) -> Result<Response<MessageMarkConversationReadResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        let app_command = AppMarkConversationReadCommand {
            conversation_id: req.conversation_id.clone(),
            user_id: user_id.to_string(),
            read_at: req.read_at.clone().map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(|| chrono::Utc::now())
            }),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        };

        match self
            .operation_handler
            .handle_mark_conversation_read_app(&ctx, &app_command)
            .await
        {
            Ok(()) => {
                let now = chrono::Utc::now();
                Ok(Response::new(MessageMarkConversationReadResponse {
                    success: true,
                    error_message: String::new(),
                    read_count: 0,
                    read_at: Some(prost_types::Timestamp {
                        seconds: now.timestamp(),
                        nanos: now.timestamp_subsec_nanos() as i32,
                    }),
                    last_read_message_id: String::new(),
                    status: Some(ok_status()),
                }))
            }
            Err(err) => {
                error!(error = %err, "Failed to mark conversation as read");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn mark_all_conversations_read(
        &self,
        request: Request<MessageMarkAllConversationsReadRequest>,
    ) -> Result<Response<MessageMarkAllConversationsReadResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        let app_command = AppMarkAllConversationsReadCommand {
            user_id: user_id.to_string(),
            read_at: req.read_at.clone().map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(|| chrono::Utc::now())
            }),
            conversation_types: req.conversation_types.clone(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        };

        match self
            .operation_handler
            .handle_mark_all_conversations_read_app(&ctx, &app_command)
            .await
        {
            Ok(()) => {
                let now = chrono::Utc::now();
                Ok(Response::new(MessageMarkAllConversationsReadResponse {
                    success: true,
                    error_message: String::new(),
                    conversation_count: 0,
                    total_read_count: 0,
                    read_at: Some(prost_types::Timestamp {
                        seconds: now.timestamp(),
                        nanos: now.timestamp_subsec_nanos() as i32,
                    }),
                    conversation_stats: vec![],
                    status: Some(ok_status()),
                }))
            }
            Err(err) => {
                error!(error = %err, "Failed to mark all conversations as read");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn add_reaction(
        &self,
        request: Request<MessageAddReactionRequest>,
    ) -> Result<Response<MessageAddReactionResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        let app_command = AppAddReactionCommand {
            message_id: req.message_id.clone(),
            user_id: user_id.to_string(),
            emoji: req.emoji.clone(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(),
        };

        match self
            .operation_handler
            .handle_add_reaction_app(&ctx, &app_command)
            .await
        {
            Ok(()) => Ok(Response::new(MessageAddReactionResponse {
                success: true,
                error_message: String::new(),
                new_count: 0,
                status: Some(ok_status()),
            })),
            Err(err) => {
                error!(error = %err, "Failed to add reaction");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn remove_reaction(
        &self,
        request: Request<MessageRemoveReactionRequest>,
    ) -> Result<Response<MessageRemoveReactionResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        let app_command = AppRemoveReactionCommand {
            message_id: req.message_id.clone(),
            user_id: user_id.to_string(),
            emoji: req.emoji.clone(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(),
        };

        match self
            .operation_handler
            .handle_remove_reaction_app(&ctx, &app_command)
            .await
        {
            Ok(()) => Ok(Response::new(MessageRemoveReactionResponse {
                success: true,
                error_message: String::new(),
                new_count: 0,
                status: Some(ok_status()),
            })),
            Err(err) => {
                error!(error = %err, "Failed to remove reaction");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn pin_message(
        &self,
        request: Request<MessagePinMessageRequest>,
    ) -> Result<Response<MessagePinMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id().to_string())
            .unwrap_or_default();
        let app_command = AppPinMessageCommand {
            message_id: req.message_id.clone(),
            operator_id,
            reason: if req.reason.is_empty() {
                None
            } else {
                Some(req.reason.clone())
            },
            expire_at: req.expire_at.clone().map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(|| chrono::Utc::now())
            }),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(),
        };

        match self
            .operation_handler
            .handle_pin_message_app(&ctx, &app_command)
            .await
        {
            Ok(()) => {
                let now = chrono::Utc::now();
                Ok(Response::new(MessagePinMessageResponse {
                    success: true,
                    error_message: String::new(),
                    pinned_at: Some(prost_types::Timestamp {
                        seconds: now.timestamp(),
                        nanos: now.timestamp_subsec_nanos() as i32,
                    }),
                    status: Some(ok_status()),
                }))
            }
            Err(err) => {
                error!(error = %err, "Failed to pin message");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn unpin_message(
        &self,
        request: Request<MessageUnpinMessageRequest>,
    ) -> Result<Response<MessageUnpinMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let operator_id = ctx
            .actor()
            .map(|a| a.actor_id().to_string())
            .unwrap_or_default();
        let app_command = AppUnpinMessageCommand {
            message_id: req.message_id.clone(),
            operator_id,
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(),
        };

        match self
            .operation_handler
            .handle_unpin_message_app(&ctx, &app_command)
            .await
        {
            Ok(()) => Ok(Response::new(MessageUnpinMessageResponse {
                success: true,
                error_message: String::new(),
                status: Some(ok_status()),
            })),
            Err(err) => {
                error!(error = %err, "Failed to unpin message");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn mark_message(
        &self,
        request: Request<MessageMarkMessageRequest>,
    ) -> Result<Response<MessageMarkMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        let app_command = AppMarkMessageCommand {
            message_id: req.message_id.clone(),
            user_id: user_id.to_string(),
            mark_type: req.mark_type,
            color: if req.color.is_empty() {
                None
            } else {
                Some(req.color.clone())
            },
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(),
        };

        match self
            .operation_handler
            .handle_mark_message_app(&ctx, &app_command)
            .await
        {
            Ok(()) => {
                let now = chrono::Utc::now();
                Ok(Response::new(MessageMarkMessageResponse {
                    success: true,
                    error_message: String::new(),
                    marked_at: Some(prost_types::Timestamp {
                        seconds: now.timestamp(),
                        nanos: now.timestamp_subsec_nanos() as i32,
                    }),
                    status: Some(ok_status()),
                }))
            }
            Err(err) => {
                error!(error = %err, "Failed to mark message");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn unmark_message(
        &self,
        request: Request<MessageUnmarkMessageRequest>,
    ) -> Result<Response<MessageUnmarkMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        let app_command = AppUnmarkMessageCommand {
            message_id: req.message_id.clone(),
            user_id: user_id.to_string(),
            mark_type: req.mark_type,
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(),
        };

        match self
            .operation_handler
            .handle_unmark_message_app(&ctx, &app_command)
            .await
        {
            Ok(()) => Ok(Response::new(MessageUnmarkMessageResponse {
                success: true,
                error_message: String::new(),
                status: Some(ok_status()),
            })),
            Err(err) => {
                error!(error = %err, "Failed to unmark message");
                Err(Status::internal(err.to_string()))
            }
        }
    }
}
