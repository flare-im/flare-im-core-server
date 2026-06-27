//! `MessageActionService` gRPC 实现：撤回、编辑、删除、反应、置顶、标记等。

use std::sync::Arc;

use crate::application::handlers::MessageActionHandler;
use flare_grpc_proto::message::{
    AddReactionRequest as MessageAddReactionRequest,
    AddReactionResponse as MessageAddReactionResponse,
    DeleteMessageRequest as MessageDeleteMessageRequest,
    DeleteMessageResponse as MessageDeleteMessageResponse,
    EditMessageRequest as MessageEditMessageRequest,
    EditMessageResponse as MessageEditMessageResponse,
    MarkMessageRequest as MessageMarkMessageRequest,
    MarkMessageResponse as MessageMarkMessageResponse,
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
use tracing::instrument;

use crate::application::commands::{
    AddReactionCommand, DeleteMessageCommand, EditMessageCommand, MarkMessageCommand,
    PinMessageCommand, RecallMessageCommand, RemoveReactionCommand, UnmarkMessageCommand,
    UnpinMessageCommand,
};
use flare_grpc_proto::message::message_action_service_server::MessageActionService;
use flare_server_core::error::grpc::IntoGrpc;
use flare_server_core::utils::require_ctx_from_request;

/// 消息操作 gRPC：撤回、编辑、删除、反应、置顶、标记等。
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
}
