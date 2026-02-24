use std::sync::Arc;

use flare_im_core::error::ok_status;
use flare_proto::message::{
    AddReactionRequest as MessageAddReactionRequest,
    AddReactionResponse as MessageAddReactionResponse,
    BatchMarkMessageReadRequest as MessageBatchMarkMessageReadRequest,
    BatchMarkMessageReadResponse as MessageBatchMarkMessageReadResponse, BatchSendMessageRequest,
    BatchSendMessageResponse, DeleteMessageRequest as MessageDeleteMessageRequest,
    DeleteMessageResponse as MessageDeleteMessageResponse,
    EditMessageRequest as MessageEditMessageRequest,
    EditMessageResponse as MessageEditMessageResponse,
    GetMessageRequest as MessageGetMessageRequest, GetMessageResponse as MessageGetMessageResponse,
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
    QueryMessagesRequest as MessageQueryMessagesRequest,
    QueryMessagesResponse as MessageQueryMessagesResponse,
    RecallMessageRequest as MessageRecallMessageRequest,
    RecallMessageResponse as MessageRecallMessageResponse,
    RemoveReactionRequest as MessageRemoveReactionRequest,
    RemoveReactionResponse as MessageRemoveReactionResponse,
    SearchMessagesRequest as MessageSearchMessagesRequest,
    SearchMessagesResponse as MessageSearchMessagesResponse, SendMessageRequest,
    SendMessageResponse, SendSystemMessageRequest, SendSystemMessageResponse,
    UnmarkMessageRequest as MessageUnmarkMessageRequest,
    UnmarkMessageResponse as MessageUnmarkMessageResponse,
    UnpinMessageRequest as MessageUnpinMessageRequest,
    UnpinMessageResponse as MessageUnpinMessageResponse,
};
use flare_proto::storage::StoreMessage;
use prost_types;
use tonic::{Request, Response, Status};
use tracing::{error, info, instrument, warn};

use crate::application::commands::{
    AppAddReactionCommand, AppBatchMarkMessageReadCommand, AppDeleteMessageCommand,
    AppEditMessageCommand, AppGetMarkedMessagesCommand, AppGetPinnedMessagesCommand,
    AppGetThreadRepliesCommand, AppGetThreadsCommand, AppMarkAllConversationsReadCommand,
    AppMarkConversationReadCommand, AppMarkMessageCommand, AppMarkMessagesReadUntilCommand,
    AppPinMessageCommand, AppRecallMessageCommand, AppRemoveReactionCommand,
    AppUnmarkMessageCommand, AppUnpinMessageCommand, LocalPagination,
};
use flare_proto::message_content_ext::MessageContentExt;
use crate::application::handlers::{MessageCommandHandler, MessageQueryHandler, MessageOperationHandler};
use crate::application::utils::OperationMessageBuilder;
use crate::application::queries::QueryMessageQuery;
use flare_proto::message::message_service_server::MessageService;
use flare_im_core::utils::context::require_context;
use flare_server_core::context::Context;
use chrono::Utc;

/// 消息 gRPC 处理器 - 处理所有消息相关的 gRPC 请求（接口层）
///
/// 职责：
/// 1. 将 gRPC 请求转换为应用层命令/查询
/// 2. 调用应用层 handlers
/// 3. 构建 gRPC 响应
///
/// 架构原则：
/// - 接口层不包含业务逻辑
/// - 所有业务处理都委托给应用层（CommandHandler/QueryHandler）
/// - 只负责协议转换和错误处理
#[derive(Clone)]
pub struct MessageGrpcHandler {
    command_handler: Arc<MessageCommandHandler>,
    query_handler: Arc<MessageQueryHandler>,
    operation_handler: Arc<MessageOperationHandler>,
}

impl MessageGrpcHandler {
    pub fn new(
        command_handler: Arc<MessageCommandHandler>,
        query_handler: Arc<MessageQueryHandler>,
        operation_handler: Arc<MessageOperationHandler>,
    ) -> Self {
        Self {
            command_handler,
            query_handler,
            operation_handler,
        }
    }
}

    #[tonic::async_trait]
    impl MessageService for MessageGrpcHandler {
    #[instrument(skip(self, request))]
        async fn send_message(
        &self,
        request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
            // 从请求中提取 Context
            let ctx = require_context(&request)?;
            
        let req = request.into_inner();
        let message = req
            .message
                .clone()
            .ok_or_else(|| Status::invalid_argument("message required"))?;

            // 构建发送消息命令
            let cmd = crate::application::commands::SendMessageCommand {
                message,
            conversation_id: req.conversation_id.clone(),
            sync: req.sync,
            context: None, // 从上下文获取或构建
            tenant: None,  // 从上下文获取或构建
        };

            // 调用应用层处理器处理发送消息逻辑
            match self.command_handler.handle_send_message(&ctx, cmd).await {
            Ok((message_id, seq)) => {
                let now = chrono::Utc::now();
                let timeline = Some(flare_proto::common::MessageTimeline {
                    created_at: Some(prost_types::Timestamp {
                        seconds: now.timestamp(),
                        nanos: now.timestamp_subsec_nanos() as i32,
                    }),
                    persisted_at: None,
                    delivered_at: None,
                    read_at: None,
                });

                Ok(Response::new(SendMessageResponse {
                    success: true,
                    server_msg_id: message_id,
                        seq,
                    sent_at: Some(prost_types::Timestamp {
                        seconds: now.timestamp(),
                        nanos: now.timestamp_subsec_nanos() as i32,
                    }),
                    timeline,
                    status: Some(ok_status()),
                }))
            }
            Err(err) => {
                    error!(error = %err, "Failed to send message");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
        async fn batch_send_message(
        &self,
        request: Request<BatchSendMessageRequest>,
    ) -> Result<Response<BatchSendMessageResponse>, Status> {
            // 从请求中提取 Context
            let ctx = require_context(&request)?;

        let req = request.into_inner();

            // 构建批量发送消息命令
            let cmd = crate::application::commands::BatchSendMessageCommand {
                requests: req.messages,
            };

            // 调用应用层处理器处理批量发送消息逻辑
            match self.command_handler.handle_batch_send_message(&ctx, cmd).await {
                Ok((successes, failure_messages)) => {
                    let success_count = successes.len() as i32;
                    let fail_count = failure_messages.len() as i32;
        let mut message_ids = Vec::new();
        let mut failures = Vec::new();

                    for (message_id, _seq) in successes {
                    message_ids.push(message_id);
                }

                    for error_msg in failure_messages {
                    failures.push(flare_proto::message::FailedMessage {
                        message_id: String::new(),
                        code: 500, // InternalError
                            error_message: error_msg,
                    });
        }

        Ok(Response::new(BatchSendMessageResponse {
            success_count,
            fail_count,
            message_ids,
            failures,
            status: Some(ok_status()),
        }))
    }
                Err(err) => {
                    error!(error = %err, "Failed to batch send messages");
                    Err(Status::internal(err.to_string()))
                }
            }
        }
    #[instrument(skip(self, request))]
        async fn send_system_message(
        &self,
        request: Request<SendSystemMessageRequest>,
    ) -> Result<Response<SendSystemMessageResponse>, Status> {
            // 从请求中提取 Context
            let ctx = require_context(&request)?;
            
        let req = request.into_inner();

        // 验证必需字段
        if req.conversation_id.is_empty() {
            return Err(Status::invalid_argument("conversation_id is required"));
        }

        let mut message = req
            .message
            .ok_or_else(|| Status::invalid_argument("message is required"))?;

        if req.system_message_type.is_empty() {
            return Err(Status::invalid_argument("system_message_type is required"));
        }

        // 构建 StoreMessageRequest，添加系统消息类型标签
        let mut tags = std::collections::HashMap::new();
        tags.insert(
            "system_message_type".to_string(),
            req.system_message_type.clone(),
        );
        tags.insert("is_system_message".to_string(), "true".to_string());
        // 确保消息类型标记为系统消息
        message.extra.insert(
            "system_message_type".to_string(),
            req.system_message_type.clone(),
        );
        message
            .extra
            .insert("sender_type".to_string(), "system".to_string());

        let mut metadata = std::collections::HashMap::new();
        
        // 从 Context 中获取租户ID并放入 metadata
        if let Some(tenant_id) = ctx.tenant_id() {
            metadata.insert("tenant_id".to_string(), tenant_id.to_string());
        }
        
        let store_request = StoreMessage {
            conversation_id: req.conversation_id.clone(),
            message: Some(message),
            sync: false, // 系统消息默认异步
            tags,
            metadata, // 使用包含租户ID的 metadata
        };

        // 调用 command_handler，跳过 PreSend Hook
        match self
            .command_handler
            .handle_store_message_without_pre_hook(crate::application::commands::StoreMessageCommand {
                request: store_request,
            })
            .await
            {
            Ok((message_id, _seq)) => {
                info!(
                    message_id = %message_id,
                    conversation_id = %req.conversation_id,
                    system_message_type = %req.system_message_type,
                    "System message sent successfully"
                );
                Ok(Response::new(SendSystemMessageResponse {
                    success: true,
                    message_id,
                    status: Some(ok_status()),
                }))
            }
            Err(err) => {
                error!(
                    error = %err,
                    conversation_id = %req.conversation_id,
                    system_message_type = %req.system_message_type,
                    "Failed to send system message"
                );
                Err(Status::internal(err.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
        async fn recall_message(
        &self,
        request: Request<MessageRecallMessageRequest>,
    ) -> Result<Response<MessageRecallMessageResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppRecallMessageCommand {
            message_id: req.message_id.clone(),
            reason: if req.reason.is_empty() { None } else { Some(req.reason.clone()) },
            time_limit_seconds: if req.recall_time_limit_seconds > 0 { Some(req.recall_time_limit_seconds) } else { None },
            operator_id: ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(), // 会在后续填充
        };

        // 调用应用层操作处理器处理撤回消息逻辑
        match self.operation_handler.handle_recall_message_app(&ctx, &app_command).await {
            Ok((message_id, seq)) => {
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
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppEditMessageCommand {
            message_id: req.message_id.clone(),
            new_content: req
                .new_content
                .as_ref()
                .and_then(|content| content.encode_to_bytes().ok())
                .unwrap_or_default(),
            reason: if req.reason.is_empty() { None } else { Some(req.reason.clone()) },
            show_edited_mark: req.show_edited_mark,
            edit_version: req.edit_version,
            operator_id: ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(), // 会在后续填充
        };

        // 调用应用层操作处理器处理编辑消息逻辑
        match self.operation_handler.handle_edit_message_app(&ctx, &app_command).await {
            Ok((message_id, seq)) => {
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
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppDeleteMessageCommand {
            message_ids: req.message_ids.clone(),
            conversation_id: req.conversation_id.clone(),
            delete_type: if req.delete_type == 1 {
                crate::application::commands::DeleteType::Hard
            } else {
                crate::application::commands::DeleteType::Soft
            },
            reason: if req.reason.is_empty() { None } else { Some(req.reason.clone()) },
            notify_others: req.notify_others,
            hard_delete: req.delete_type == 1,
            operator_id: ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        };

        // 调用应用层操作处理器处理删除消息逻辑
        match self.operation_handler.handle_delete_message_app(&ctx, &app_command).await {
            Ok((success, deleted_count)) => {
                Ok(Response::new(MessageDeleteMessageResponse {
                    success,
                    deleted_count,
                    status: Some(ok_status()),
                }))
            }
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
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppMarkMessageCommand {
            message_id: req.message_id.clone(),
            user_id: req.user_id.clone(),
            mark_type: 0, // 已读操作固定为0
            color: None,
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(), // 会在后续填充
        };

        // 调用应用层操作处理器处理标记消息已读逻辑
        match self.operation_handler.handle_mark_message_read_app(&ctx, &app_command).await {
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
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppBatchMarkMessageReadCommand {
            conversation_id: req.conversation_id.clone(),
            user_id: req.user_id.clone(),
            message_ids: req.message_ids.clone(),
            read_at: req.read_at.clone().map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(|| chrono::Utc::now())
            }),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        };

        // 调用应用层操作处理器处理批量标记消息已读逻辑
        match self.operation_handler.handle_batch_mark_message_read_app(&ctx, &app_command).await {
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
        async fn mark_conversation_read(
        &self,
        request: Request<MessageMarkConversationReadRequest>,
    ) -> Result<Response<MessageMarkConversationReadResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppMarkConversationReadCommand {
            conversation_id: req.conversation_id.clone(),
            user_id: req.user_id.clone(),
            read_at: req.read_at.clone().map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(|| chrono::Utc::now())
            }),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        };

        // 调用应用层操作处理器处理标记会话已读逻辑
        match self.operation_handler.handle_mark_conversation_read_app(&ctx, &app_command).await {
            Ok(()) => {
                let now = chrono::Utc::now();
                Ok(Response::new(MessageMarkConversationReadResponse {
                    success: true,
                    error_message: String::new(),
                    read_count: 0, // 由于是标记整个会话已读，无法准确统计数量
                    read_at: Some(prost_types::Timestamp {
                        seconds: now.timestamp(),
                        nanos: now.timestamp_subsec_nanos() as i32,
                    }),
                    last_read_message_id: String::new(), // 标记会话已读时不需要特定消息ID
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
        // 这里需要查询用户的所有会话，然后对每个会话调用 mark_conversation_read
        // 简化实现：返回未实现错误，实际应该查询用户会话列表并批量处理
        
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppMarkAllConversationsReadCommand {
            user_id: req.user_id.clone(),
            read_at: req.read_at.clone().map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(|| chrono::Utc::now())
            }),
            conversation_types: req.conversation_types.clone(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        };

        // 调用应用层操作处理器处理标记全部会话已读逻辑
        match self.operation_handler.handle_mark_all_conversations_read_app(&ctx, &app_command).await {
            Ok(()) => {
                let now = chrono::Utc::now();
                Ok(Response::new(MessageMarkAllConversationsReadResponse {
                    success: true,
                    error_message: String::new(),
                    conversation_count: 0, // 暂时返回0，实际应该从操作结果中获取
                    total_read_count: 0, // 暂时返回0，实际应该从操作结果中获取
                    read_at: Some(prost_types::Timestamp {
                        seconds: now.timestamp(),
                        nanos: now.timestamp_subsec_nanos() as i32,
                    }),
                    conversation_stats: vec![], // 暂时返回空列表
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
    async fn mark_messages_read_until(
        &self,
        request: Request<MessageMarkMessagesReadUntilRequest>,
    ) -> Result<Response<MessageMarkMessagesReadUntilResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppMarkMessagesReadUntilCommand {
            conversation_id: req.conversation_id.clone(),
            user_id: req.user_id.clone(),
            until_message_id: req.until_message_id.clone(),
            read_at: req.read_at.clone().map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(|| chrono::Utc::now())
            }),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
        };

        // 调用应用层操作处理器处理标记消息直到指定消息已读逻辑
        match self.operation_handler.handle_mark_messages_read_until_app(&ctx, &app_command).await {
            Ok(()) => {
                let now = chrono::Utc::now();
                Ok(Response::new(MessageMarkMessagesReadUntilResponse {
                    success: true,
                    error_message: String::new(),
                    read_count: 0, // 暂时返回0，实际应该从操作结果中获取
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
    async fn get_pinned_messages(
        &self,
        request: Request<flare_proto::message::GetPinnedMessagesRequest>,
    ) -> Result<Response<flare_proto::message::GetPinnedMessagesResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppGetPinnedMessagesCommand {
            conversation_id: req.conversation_id.clone(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            pagination: req.pagination.as_ref().map(|p| LocalPagination::from(p)),
        };

        // 调用应用层操作处理器处理获取置顶消息逻辑
        match self.operation_handler.handle_get_pinned_messages_app(&ctx, &app_command).await {
            Ok(messages) => {
                Ok(Response::new(flare_proto::message::GetPinnedMessagesResponse {
                    messages,
                    pinned_infos: vec![], // 暂时返回空列表
                    pagination: req.pagination,
                    status: Some(ok_status()),
                }))
            }
            Err(err) => {
                error!(error = %err, "Failed to get pinned messages");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn get_marked_messages(
        &self,
        request: Request<flare_proto::message::GetMarkedMessagesRequest>,
    ) -> Result<Response<flare_proto::message::GetMarkedMessagesResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppGetMarkedMessagesCommand {
            user_id: req.user_id.clone(),
            mark_type: if req.mark_type == 0 { None } else { Some(req.mark_type) },
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            pagination: req.pagination.as_ref().map(|p| LocalPagination::from(p)),
        };

        // 调用应用层操作处理器处理获取标记消息逻辑
        match self.operation_handler.handle_get_marked_messages_app(&ctx, &app_command).await {
            Ok(messages) => {
                Ok(Response::new(flare_proto::message::GetMarkedMessagesResponse {
                    messages,
                    marked_infos: vec![], // 暂时返回空列表
                    pagination: req.pagination,
                    status: Some(ok_status()),
                }))
            }
            Err(err) => {
                error!(error = %err, "Failed to get marked messages");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn get_threads(
        &self,
        request: Request<flare_proto::message::GetThreadsRequest>,
    ) -> Result<Response<flare_proto::message::GetThreadsResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppGetThreadsCommand {
            conversation_id: req.conversation_id.clone(),
            status: req.status as i32,
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            pagination: req.pagination.as_ref().map(|p| LocalPagination::from(p)),
        };

        // 调用应用层操作处理器处理获取话题逻辑
        match self.operation_handler.handle_get_threads_app(&ctx, &app_command).await {
            Ok(threads) => {
                Ok(Response::new(flare_proto::message::GetThreadsResponse {
                    threads,
                    pagination: req.pagination,
                    status: Some(ok_status()),
                }))
            }
            Err(err) => {
                error!(error = %err, "Failed to get threads");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn get_thread_replies(
        &self,
        request: Request<flare_proto::message::GetThreadRepliesRequest>,
    ) -> Result<Response<flare_proto::message::GetThreadRepliesResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppGetThreadRepliesCommand {
            thread_id: req.thread_id.clone(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            pagination: req.pagination.as_ref().map(|p| LocalPagination::from(p)),
        };

        // 调用应用层操作处理器处理获取话题回复逻辑
        match self.operation_handler.handle_get_thread_replies_app(&ctx, &app_command).await {
            Ok(messages) => {
                Ok(Response::new(flare_proto::message::GetThreadRepliesResponse {
                    messages,
                    thread_info: None, // 暂时返回None
                    pagination: req.pagination,
                    status: Some(ok_status()),
                }))
            }
            Err(err) => {
                error!(error = %err, "Failed to get thread replies");
                Err(Status::internal(err.to_string()))
            }
        }
    }


    #[instrument(skip(self, request))]
        async fn add_reaction(
        &self,
        request: Request<MessageAddReactionRequest>,
    ) -> Result<Response<MessageAddReactionResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppAddReactionCommand {
            message_id: req.message_id.clone(),
            user_id: req.user_id.clone(),
            emoji: req.emoji.clone(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(), // 会在后续填充
        };

        // 调用应用层操作处理器处理添加反应逻辑
        match self.operation_handler.handle_add_reaction_app(&ctx, &app_command).await {
            Ok(()) => {
                Ok(Response::new(MessageAddReactionResponse {
                    success: true,
                    error_message: String::new(),
                    new_count: 0, // 需要从操作结果中获取，暂时设为0
                    status: Some(ok_status()),
                }))
            }
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
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppRemoveReactionCommand {
            message_id: req.message_id.clone(),
            user_id: req.user_id.clone(),
            emoji: req.emoji.clone(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(), // 会在后续填充
        };

        // 调用应用层操作处理器处理移除反应逻辑
        match self.operation_handler.handle_remove_reaction_app(&ctx, &app_command).await {
            Ok(()) => {
                Ok(Response::new(MessageRemoveReactionResponse {
                    success: true,
                    error_message: String::new(),
                    new_count: 0, // 需要从操作结果中获取，暂时设为0
                    status: Some(ok_status()),
                }))
            }
            Err(err) => {
                error!(error = %err, "Failed to remove reaction");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    // reply_message 和 quote_message 已废弃：现在通过 SendMessage + Message.quote 字段实现

        #[instrument(skip(self, request))]
    async fn pin_message(
        &self,
        request: Request<MessagePinMessageRequest>,
    ) -> Result<Response<MessagePinMessageResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppPinMessageCommand {
            message_id: req.message_id.clone(),
            operator_id: req.operator_id.clone(),
            reason: if req.reason.is_empty() { None } else { Some(req.reason.clone()) },
            expire_at: req.expire_at.clone().map(|ts| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(|| chrono::Utc::now())
            }),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(), // 会在后续填充
        };

        // 调用应用层操作处理器处理置顶消息逻辑
        match self.operation_handler.handle_pin_message_app(&ctx, &app_command).await {
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
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppUnpinMessageCommand {
            message_id: req.message_id.clone(),
            operator_id: req.operator_id.clone(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(), // 会在后续填充
        };

        // 调用应用层操作处理器处理取消置顶消息逻辑
        match self.operation_handler.handle_unpin_message_app(&ctx, &app_command).await {
            Ok(()) => {
                Ok(Response::new(MessageUnpinMessageResponse {
                    success: true,
                    error_message: String::new(),
                    status: Some(ok_status()),
                }))
            }
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
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppMarkMessageCommand {
            message_id: req.message_id.clone(),
            user_id: req.user_id.clone(),
            mark_type: req.mark_type,
            color: if req.color.is_empty() { None } else { Some(req.color.clone()) },
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(), // 会在后续填充
        };

        // 调用应用层操作处理器处理标记消息逻辑
        match self.operation_handler.handle_mark_message_app(&ctx, &app_command).await {
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
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppUnmarkMessageCommand {
            message_id: req.message_id.clone(),
            user_id: req.user_id.clone(),
            mark_type: req.mark_type,
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(), // 会在后续填充
        };

        // 调用应用层操作处理器处理取消标记消息逻辑
        match self.operation_handler.handle_unmark_message_app(&ctx, &app_command).await {
            Ok(()) => {
                Ok(Response::new(MessageUnmarkMessageResponse {
                    success: true,
                    error_message: String::new(),
                    status: Some(ok_status()),
                }))
            }
            Err(err) => {
                error!(error = %err, "Failed to unmark message");
                Err(Status::internal(err.to_string()))
            }
        }
    }

        #[instrument(skip(self, request))]
    async fn query_messages(
        &self,
        request: Request<MessageQueryMessagesRequest>,
    ) -> Result<Response<MessageQueryMessagesResponse>, Status> {
            let req = request.into_inner();

            // 构建查询对象
            let query = crate::application::queries::QueryMessagesQuery {
                conversation_id: req.conversation_id.clone(),
                limit: Some(req.limit),
                cursor: if req.cursor.is_empty() {
                    None
                } else {
                    Some(req.cursor.clone())
                },
                start_time: if req.start_time == 0 {
                    None
                } else {
                    Some(req.start_time)
                },
                end_time: if req.end_time == 0 {
                    None
                } else {
                    Some(req.end_time)
                },
            };

            // 调用查询处理器
            let result = self
                .query_handler
                .query_messages_with_pagination(query)
                .await
                .map_err(|err| {
                    error!(error = %err, "Failed to query messages");
                    Status::internal(format!("Query messages failed: {}", err))
                })?;

            // 构建响应
            let pagination = if let Some(mut pagination) = req.pagination {
                pagination.has_more = result.has_more;
                pagination.cursor = result.next_cursor.clone();
                Some(pagination)
            } else {
                None
            };

            Ok(Response::new(MessageQueryMessagesResponse {
                messages: result.messages,
                next_cursor: result.next_cursor,
                has_more: result.has_more,
                pagination,
                status: Some(flare_proto::common::RpcStatus {
                    code: flare_proto::common::ErrorCode::Ok as i32,
                    message: "Success".to_string(),
                    details: vec![],
                    context: None,
                }),
            }))
        }

        #[instrument(skip(self, request))]
    async fn search_messages(
        &self,
        request: Request<MessageSearchMessagesRequest>,
    ) -> Result<Response<MessageSearchMessagesResponse>, Status> {
            let req = request.into_inner();

            // 构建搜索查询对象
            let query = crate::application::queries::SearchMessagesQuery {
                conversation_id: None,       // SearchMessagesRequest中没有conversation_id字段
                keyword: String::new(), // SearchMessagesRequest中没有keyword字段，应在filters中处理
                limit: req.pagination.as_ref().map(|p| p.limit),
                cursor: req.pagination.as_ref().and_then(|p| {
                    if !p.cursor.is_empty() {
                        Some(p.cursor.clone())
                    } else {
                        None
                    }
                }),
            };

            // 调用查询处理器
            let messages = self
                .query_handler
                .search_messages(query)
                .await
                .map_err(|err| {
                    error!(error = %err, "Failed to search messages");
                    Status::internal(format!("Search messages failed: {}", err))
                })?;

            // 构建响应
            Ok(Response::new(MessageSearchMessagesResponse {
                messages,
                pagination: req.pagination,
                status: Some(flare_proto::common::RpcStatus {
                    code: flare_proto::common::ErrorCode::Ok as i32,
                    message: "Success".to_string(),
                    details: vec![],
                    context: None,
                }),
            }))
        }

        #[instrument(skip(self, request))]
    async fn get_message(
        &self,
        request: Request<MessageGetMessageRequest>,
    ) -> Result<Response<MessageGetMessageResponse>, Status> {
            let req = request.into_inner();

            // 构建查询对象
            let query = crate::application::queries::QueryMessageQuery {
                message_id: req.message_id.clone(),
                conversation_id: String::new(), // GetMessageRequest中没有conversation_id字段
            };

            // 调用查询处理器
            let message = self
                .query_handler
                .query_message(query)
                .await
                .map_err(|err| {
                    error!(error = %err, "Failed to get message");
                    Status::internal(format!("Get message failed: {}", err))
                })?;

            // 构建响应
            Ok(Response::new(MessageGetMessageResponse {
                message: Some(message),
                status: Some(flare_proto::common::RpcStatus {
                    code: flare_proto::common::ErrorCode::Ok as i32,
                    message: "Success".to_string(),
                    details: vec![],
                    context: None,
                }),
            }))
    }








}