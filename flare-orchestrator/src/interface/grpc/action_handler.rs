//! `MessageActionService` gRPC 实现：撤回、编辑、删除、已读、反应、置顶、标记等。

use std::sync::Arc;

use crate::application::handlers::MessageActionHandler;
use flare_grpc_proto::message::{
    AddReactionRequest as MessageAddReactionRequest,
    AddReactionResponse as MessageAddReactionResponse, BatchMarkMessageReadRequest,
    BatchMarkMessageReadResponse, DeleteMessageRequest as MessageDeleteMessageRequest,
    DeleteMessageResponse as MessageDeleteMessageResponse,
    EditMessageRequest as MessageEditMessageRequest,
    EditMessageResponse as MessageEditMessageResponse, MarkAllConversationsReadRequest,
    MarkAllConversationsReadResponse, MarkConversationReadRequest, MarkConversationReadResponse,
    MarkMessageReadRequest as MessageMarkMessageReadRequest,
    MarkMessageReadResponse as MessageMarkMessageReadResponse,
    MarkMessageRequest as MessageMarkMessageRequest,
    MarkMessageResponse as MessageMarkMessageResponse, MarkMessagesReadUntilRequest,
    MarkMessagesReadUntilResponse, PinMessageRequest as MessagePinMessageRequest,
    PinMessageResponse as MessagePinMessageResponse,
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
use tracing::instrument;

use crate::application::commands::{
    AddReactionCommand, DeleteMessageCommand, EditMessageCommand, MarkMessageCommand,
    PinMessageCommand, ReadMessageCommand, RecallMessageCommand, RemoveReactionCommand,
    UnmarkMessageCommand, UnpinMessageCommand,
};
use flare_grpc_proto::message::message_action_service_server::MessageActionService;
use flare_server_core::error::grpc::IntoGrpc;
use flare_server_core::utils::require_ctx_from_request;

/// 消息操作 gRPC：撤回、编辑、删除、已读、反应、置顶、标记等。
#[derive(Clone)]
pub struct MessageActionGrpcHandler {
    action_handler: Arc<MessageActionHandler>,
}

impl MessageActionGrpcHandler {
    pub fn new(action_handler: Arc<MessageActionHandler>) -> Self {
        Self { action_handler }
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

        // 使用命令的 from_request 方法构建命令
        let cmd = RecallMessageCommand::from_request(&req, &ctx);

        // 调用应用层处理，使用 into_grpc 转换错误
        self.action_handler
            .recall_message(&ctx, cmd)
            .await
            .into_grpc()?;

        let now = chrono::Utc::now();
        Ok(Response::new(MessageRecallMessageResponse {
            success: true,
            error_message: String::new(),
            recalled_at: Some(prost_types::Timestamp {
                seconds: now.timestamp(),
                nanos: now.timestamp_subsec_nanos() as i32,
            }),
        }))
    }

    #[instrument(skip(self, request))]
    async fn edit_message(
        &self,
        request: Request<MessageEditMessageRequest>,
    ) -> Result<Response<MessageEditMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        // 使用命令的 from_request 方法构建命令
        let cmd = EditMessageCommand::from_request(&req, &ctx);

        // 调用应用层处理，使用 into_grpc 转换错误
        self.action_handler
            .edit_message(&ctx, cmd)
            .await
            .into_grpc()?;

        let now = chrono::Utc::now();
        Ok(Response::new(MessageEditMessageResponse {
            success: true,
            error_message: String::new(),
            message_id: req.message_id,
            edit_version: 0,
            edited_at: Some(prost_types::Timestamp {
                seconds: now.timestamp(),
                nanos: now.timestamp_subsec_nanos() as i32,
            }),
        }))
    }

    #[instrument(skip(self, request))]
    async fn delete_message(
        &self,
        request: Request<MessageDeleteMessageRequest>,
    ) -> Result<Response<MessageDeleteMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        // 使用命令的 from_request 方法构建命令
        let cmd = DeleteMessageCommand::from_request(&req, &ctx);

        // 调用应用层处理，使用 into_grpc 转换错误
        let deleted_count = self
            .action_handler
            .delete_message(&ctx, cmd)
            .await
            .into_grpc()?;

        Ok(Response::new(MessageDeleteMessageResponse {
            success: true,
            deleted_count,
        }))
    }

    #[instrument(skip(self, request))]
    async fn mark_message_read(
        &self,
        request: Request<MessageMarkMessageReadRequest>,
    ) -> Result<Response<MessageMarkMessageReadResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        // 使用命令的 from_request 方法构建命令
        let cmd = ReadMessageCommand::from_request(&req, &ctx);

        // 调用应用层处理，使用 into_grpc 转换错误
        self.action_handler
            .mark_message_read(&ctx, cmd)
            .await
            .into_grpc()?;

        let now = chrono::Utc::now();
        Ok(Response::new(MessageMarkMessageReadResponse {
            success: true,
            error_message: String::new(),
            read_at: Some(prost_types::Timestamp {
                seconds: now.timestamp(),
                nanos: now.timestamp_subsec_nanos() as i32,
            }),
            burned_at: None,
        }))
    }

    #[instrument(skip(self, request))]
    async fn add_reaction(
        &self,
        request: Request<MessageAddReactionRequest>,
    ) -> Result<Response<MessageAddReactionResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        // 使用命令的 from_request 方法构建命令
        let cmd = AddReactionCommand::from_request(&req, &ctx);

        // 调用应用层处理，使用 into_grpc 转换错误
        let count = self
            .action_handler
            .add_reaction(&ctx, cmd)
            .await
            .into_grpc()?;

        Ok(Response::new(MessageAddReactionResponse {
            success: true,
            error_message: String::new(),
            new_count: count,
        }))
    }

    #[instrument(skip(self, request))]
    async fn remove_reaction(
        &self,
        request: Request<MessageRemoveReactionRequest>,
    ) -> Result<Response<MessageRemoveReactionResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        // 使用命令的 from_request 方法构建命令
        let cmd = RemoveReactionCommand::from_request(&req, &ctx);

        // 调用应用层处理，使用 into_grpc 转换错误
        let count = self
            .action_handler
            .remove_reaction(&ctx, cmd)
            .await
            .into_grpc()?;

        Ok(Response::new(MessageRemoveReactionResponse {
            success: true,
            error_message: String::new(),
            new_count: count,
        }))
    }

    #[instrument(skip(self, request))]
    async fn pin_message(
        &self,
        request: Request<MessagePinMessageRequest>,
    ) -> Result<Response<MessagePinMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        // 使用命令的 from_request 方法构建命令
        let cmd = PinMessageCommand::from_request(&req, &ctx);

        // 调用应用层处理，使用 into_grpc 转换错误
        self.action_handler
            .pin_message(&ctx, cmd)
            .await
            .into_grpc()?;

        Ok(Response::new(MessagePinMessageResponse {
            success: true,
            error_message: String::new(),
            pinned_at: Some(prost_types::Timestamp::default()),
        }))
    }

    #[instrument(skip(self, request))]
    async fn unpin_message(
        &self,
        request: Request<MessageUnpinMessageRequest>,
    ) -> Result<Response<MessageUnpinMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        // 使用命令的 from_request 方法构建命令
        let cmd = UnpinMessageCommand::from_request(&req, &ctx);

        // 调用应用层处理，使用 into_grpc 转换错误
        self.action_handler
            .unpin_message(&ctx, cmd)
            .await
            .into_grpc()?;

        Ok(Response::new(MessageUnpinMessageResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn mark_message(
        &self,
        request: Request<MessageMarkMessageRequest>,
    ) -> Result<Response<MessageMarkMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        // 使用命令的 from_request 方法构建命令
        let cmd = MarkMessageCommand::from_request(&req, &ctx);

        // 调用应用层处理，使用 into_grpc 转换错误
        self.action_handler
            .mark_message(&ctx, cmd)
            .await
            .into_grpc()?;

        Ok(Response::new(MessageMarkMessageResponse {
            success: true,
            error_message: String::new(),
            marked_at: Some(prost_types::Timestamp::default()),
        }))
    }

    #[instrument(skip(self, request))]
    async fn unmark_message(
        &self,
        request: Request<MessageUnmarkMessageRequest>,
    ) -> Result<Response<MessageUnmarkMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        // 使用命令的 from_request 方法构建命令
        let cmd = UnmarkMessageCommand::from_request(&req, &ctx);

        // 调用应用层处理，使用 into_grpc 转换错误
        self.action_handler
            .unmark_message(&ctx, cmd)
            .await
            .into_grpc()?;

        Ok(Response::new(MessageUnmarkMessageResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn batch_mark_message_read(
        &self,
        request: Request<BatchMarkMessageReadRequest>,
    ) -> Result<Response<BatchMarkMessageReadResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        // 调用 application 层的批量标记已读方法
        self.action_handler
            .batch_mark_message_read(&ctx, req.message_ids)
            .await
            .into_grpc()?;

        Ok(Response::new(BatchMarkMessageReadResponse {
            success: true,
            error_message: String::new(),
            read_at: Some(prost_types::Timestamp::default()),
            read_count: 0,
        }))
    }

    #[instrument(skip(self, request))]
    async fn mark_messages_read_until(
        &self,
        request: Request<MarkMessagesReadUntilRequest>,
    ) -> Result<Response<MarkMessagesReadUntilResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        // 调用 application 层的标记直到某条消息已读方法
        self.action_handler
            .mark_messages_read_until(&ctx, &req.until_message_id)
            .await
            .into_grpc()?;

        Ok(Response::new(MarkMessagesReadUntilResponse {
            success: true,
            error_message: String::new(),
            read_at: Some(prost_types::Timestamp::default()),
            read_count: 0,
        }))
    }

    #[instrument(skip(self, request))]
    async fn mark_conversation_read(
        &self,
        request: Request<MarkConversationReadRequest>,
    ) -> Result<Response<MarkConversationReadResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        // 调用 application 层的标记会话已读方法
        self.action_handler
            .mark_conversation_read(&ctx, &req.conversation_id)
            .await
            .into_grpc()?;

        Ok(Response::new(MarkConversationReadResponse {
            success: true,
            error_message: String::new(),
            last_read_message_id: String::new(),
            read_at: Some(prost_types::Timestamp::default()),
            read_count: 0,
        }))
    }

    #[instrument(skip(self, request))]
    async fn mark_all_conversations_read(
        &self,
        request: Request<MarkAllConversationsReadRequest>,
    ) -> Result<Response<MarkAllConversationsReadResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;

        // 调用 application 层的标记所有会话已读方法
        let user_id = ctx.user_id().unwrap_or_default();
        self.action_handler
            .mark_all_conversations_read(&ctx, &user_id)
            .await
            .into_grpc()?;

        Ok(Response::new(MarkAllConversationsReadResponse {
            success: true,
            error_message: String::new(),
            conversation_count: 0,
            conversation_stats: Vec::new(),
            read_at: Some(prost_types::Timestamp::default()),
            total_read_count: 0,
        }))
    }
}
