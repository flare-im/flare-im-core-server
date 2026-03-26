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
    SearchMessagesResponse as MessageSearchMessagesResponse,
    ExecuteEventRequest, SendAckRequest, SendAckResponse, SendCustomDataRequest, SendCustomDataResponse,
    SendMessageRequest, SendMessageResponse, SendSystemMessageRequest, SendSystemMessageResponse,
    UnmarkMessageRequest as MessageUnmarkMessageRequest,
    UnmarkMessageResponse as MessageUnmarkMessageResponse,
    UnpinMessageRequest as MessageUnpinMessageRequest,
    UnpinMessageResponse as MessageUnpinMessageResponse,
};
use prost_types;
use tonic::{Request, Response, Status};
use tracing::{error, info, instrument};

use crate::application::commands::{
    AppAddReactionCommand, AppBatchMarkMessageReadCommand, AppDeleteMessageCommand, DeleteScope, DeleteType,
    AppEditMessageCommand, AppGetMarkedMessagesCommand, AppGetPinnedMessagesCommand,
    AppGetThreadRepliesCommand, AppGetThreadsCommand, AppMarkAllConversationsReadCommand,
    AppMarkConversationReadCommand, AppMarkMessageCommand, AppMarkMessagesReadUntilCommand,
    AppPinMessageCommand, AppRecallMessageCommand, AppRemoveReactionCommand,
    AppUnmarkMessageCommand, AppUnpinMessageCommand, LocalPagination,
};
use flare_proto::message_content_ext::MessageContentExt;
use crate::application::handlers::{MessageCommandHandler, MessageQueryHandler, MessageOperationHandler};
use flare_proto::message::message_send_service_server::MessageSendService;
use flare_proto::message::message_action_service_server::MessageActionService;
use flare_proto::message::message_query_service_server::MessageQueryService;
use flare_server_core::utils::require_ctx_from_request;

/// 消息 gRPC 适配器：位于 `interface::grpc`，仅做 proto ↔ application 命令/查询映射。
///
/// - 入参：`require_ctx_from_request` + 构造 `application::commands` / 查询 DTO
/// - 编排：委托 `MessageCommandHandler` / `MessageQueryHandler` / `MessageOperationHandler`
/// - 出参：领域/应用结果组装为 proto Response（无业务规则）
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
    impl MessageSendService for MessageGrpcHandler {
    #[instrument(skip(self, request))]
        async fn send_message(
        &self,
        request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
            // 从请求中提取 Context
            let ctx = require_ctx_from_request(&request)?;
            
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
            let ctx = require_ctx_from_request(&request)?;

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
            let ctx = require_ctx_from_request(&request)?;
            
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

        message.extra.insert(
            "system_message_type".to_string(),
            req.system_message_type.clone(),
        );
        message.extra.insert("sender_type".to_string(), "system".to_string());
        message.extra.insert(flare_im_core::abstractions::storage_payload::EXTRA_KEY_SYNC.to_string(), "false".to_string());
        let tags = std::collections::HashMap::from([
            ("system_message_type".to_string(), req.system_message_type.clone()),
            ("is_system_message".to_string(), "true".to_string()),
        ]);
        if let Ok(tags_json) = serde_json::to_string(&tags) {
            message.extra.insert(flare_im_core::abstractions::storage_payload::EXTRA_KEY_TAGS.to_string(), tags_json);
        }
        if let Some(tenant_id) = ctx.tenant_id() {
            message.extra.insert("x-tenant-id".to_string(), tenant_id.to_string());
        }
        if message.conversation_id.is_empty() {
            message.conversation_id = req.conversation_id.clone();
        }
        match self
            .command_handler
            .handle_store_message_without_pre_hook(&ctx, crate::application::commands::StoreMessageCommand {
                request: message,
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

    /// 统一事件入口：ExecuteEventRequest（与 RouterUpstream.RouteEvent 对齐）→ OperationResponse
    #[instrument(skip(self, request))]
    async fn execute_event(
        &self,
        request: Request<ExecuteEventRequest>,
    ) -> Result<Response<flare_proto::common::OperationResponse>, Status> {
        use flare_proto::common::event::Payload as EventPayload;
        use flare_proto::common::{ErrorCode, RpcStatus};
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        if !req.svid.is_empty() {
            tracing::debug!(svid = %req.svid, "ExecuteEvent");
        }
        let event = req
            .event
            .ok_or_else(|| Status::invalid_argument("event required"))?;
        let request_id = event.request_id.clone();
        let op_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        let tenant = ctx.tenant_id().unwrap_or("0").to_string();
        let conv_id = event.conversation_id.clone();
        let ok_resp = || flare_proto::common::OperationResponse {
            request_id: request_id.clone(),
            status: Some(RpcStatus {
                code: ErrorCode::Ok.into(),
                message: String::new(),
                ..Default::default()
            }),
        };
        let err_resp = |err: flare_im_core::error::FlareError| flare_proto::common::OperationResponse {
            request_id: request_id.clone(),
            status: Some(RpcStatus {
                code: err.code().map(|c| c.as_u32() as i32).unwrap_or(ErrorCode::Internal as i32),
                message: err.to_string(),
                ..Default::default()
            }),
        };
        let resp = match event.payload {
            Some(EventPayload::Recall(r)) => {
                let cmd = AppRecallMessageCommand {
                    message_id: r.server_msg_id,
                    reason: if r.reason.is_empty() { None } else { Some(r.reason) },
                    time_limit_seconds: r.time_limit_seconds,
                    operator_id: op_id.to_string(),
                    tenant_id: tenant.clone(),
                    conversation_id: conv_id.clone(),
                };
                match self.operation_handler.handle_recall_message_app(&ctx, &cmd).await {
                    Ok(_) => ok_resp(),
                    Err(e) => err_resp(e),
                }
            }
            Some(EventPayload::Edit(e)) => {
                let cmd = AppEditMessageCommand {
                    message_id: e.server_msg_id,
                    new_content: e.new_content,
                    reason: if e.reason.is_empty() { None } else { Some(e.reason) },
                    show_edited_mark: e.show_edited_mark,
                    edit_version: e.edit_version,
                    operator_id: op_id.to_string(),
                    tenant_id: tenant.clone(),
                    conversation_id: conv_id.clone(),
                };
                match self.operation_handler.handle_edit_message_app(&ctx, &cmd).await {
                    Ok(_) => ok_resp(),
                    Err(err) => err_resp(err),
                }
            }
            Some(EventPayload::Delete(d)) => {
                let delete_type = match d.delete_type {
                    Some(2) => DeleteType::Hard, // proto: DELETE_TYPE_HARD = 2
                    _ => DeleteType::Soft,
                };
                let delete_scope = d
                    .scope
                    .and_then(DeleteScope::from_proto_value)
                    .unwrap_or_else(|| DeleteScope::default_for_type(delete_type));
                if d.server_msg_id.trim().is_empty() {
                    flare_proto::common::OperationResponse {
                        request_id: request_id.clone(),
                        status: Some(flare_proto::common::RpcStatus {
                            code: flare_proto::common::ErrorCode::InvalidArgument as i32,
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
                match self.operation_handler.handle_delete_message_app(&ctx, &cmd).await {
                    Ok(_) => ok_resp(),
                    Err(err) => err_resp(err),
                }
                }
            }
            Some(EventPayload::Read(r)) => {
                let user_id = op_id.clone();
                let tenant_id = tenant.clone();
                let read_at = r.read_at.as_ref().and_then(|t| chrono::DateTime::from_timestamp(t.seconds, t.nanos as u32));
                let cmd = AppBatchMarkMessageReadCommand {
                    conversation_id: r.conversation_id.clone(),
                    user_id: user_id.to_string(),
                    message_ids: if r.message_ids.is_empty() { vec![] } else { r.message_ids },
                    read_at: read_at.or_else(|| Some(chrono::Utc::now())),
                    tenant_id,
                };
                match self.operation_handler.handle_batch_mark_message_read_app(&ctx, &cmd).await {
                    Ok(_) => ok_resp(),
                    Err(err) => err_resp(err),
                }
            }
            Some(EventPayload::Reaction(re)) => {
                let user_id = op_id.clone();
                let tenant_id = tenant.clone();
                let conversation_id = conv_id.clone();
                if re.action == 1 {
                    let cmd = AppAddReactionCommand { message_id: re.server_msg_id.clone(), emoji: re.emoji.clone(), user_id: user_id.to_string(), tenant_id: tenant_id.clone(), conversation_id: conversation_id.clone() };
                    match self.operation_handler.handle_add_reaction_app(&ctx, &cmd).await {
                        Ok(_) => ok_resp(),
                        Err(err) => err_resp(err),
                    }
                } else {
                    let cmd = AppRemoveReactionCommand { message_id: re.server_msg_id, emoji: re.emoji, user_id: user_id.to_string(), tenant_id, conversation_id };
                    match self.operation_handler.handle_remove_reaction_app(&ctx, &cmd).await {
                        Ok(_) => ok_resp(),
                        Err(err) => err_resp(err),
                    }
                }
            }
            Some(EventPayload::Pin(p)) => {
                let expire_at = p.expire_at.as_ref().and_then(|t| chrono::DateTime::from_timestamp(t.seconds, t.nanos as u32));
                let cmd = AppPinMessageCommand {
                    message_id: p.server_msg_id,
                    reason: p.reason.filter(|s| !s.is_empty()),
                    expire_at,
                    operator_id: op_id.to_string(),
                    tenant_id: tenant.clone(),
                    conversation_id: conv_id.clone(),
                };
                match self.operation_handler.handle_pin_message_app(&ctx, &cmd).await {
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
                match self.operation_handler.handle_unpin_message_app(&ctx, &cmd).await {
                    Ok(_) => ok_resp(),
                    Err(err) => err_resp(err),
                }
            }
            Some(EventPayload::Mark(m)) => {
                let cmd = AppMarkMessageCommand {
                    message_id: m.server_msg_id,
                    mark_type: m.mark_type,
                    color: if m.color.is_empty() { None } else { Some(m.color.clone()) },
                    user_id: op_id.to_string(),
                    tenant_id: tenant.clone(),
                    conversation_id: conv_id.clone(),
                };
                match self.operation_handler.handle_mark_message_app(&ctx, &cmd).await {
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
                match self.operation_handler.handle_unmark_message_app(&ctx, &cmd).await {
                    Ok(_) => ok_resp(),
                    Err(err) => err_resp(err),
                }
            }
            Some(EventPayload::Typing(_)) => {
                ok_resp()
            }
            _ => flare_proto::common::OperationResponse {
                request_id: request_id.clone(),
                status: Some(flare_proto::common::RpcStatus {
                    code: flare_proto::common::ErrorCode::UnsupportedOperation as i32,
                    message: "Unsupported event type or missing payload".to_string(),
                    ..Default::default()
                }),
            },
        };
        Ok(Response::new(resp))
    }

    // TODO: 上行客户端 ACK（SendAck）— 对接 Push/会话 ACK 持久化与观测，见 RouterUpstream.RouteAck
    #[instrument(skip(self, request))]
    async fn send_ack(&self, request: Request<SendAckRequest>) -> Result<Response<SendAckResponse>, Status> {
        let _ = require_ctx_from_request(&request)?;
        let _req = request.into_inner();
        Err(Status::unimplemented(
            "TODO: SendAck — uplink client ACK not implemented in orchestrator yet",
        ))
    }

    // TODO: 上行 CustomData（SendCustomData）— 业务扩展/控制面，见 RouterUpstream.RouteData
    #[instrument(skip(self, request))]
    async fn send_custom_data(
        &self,
        request: Request<SendCustomDataRequest>,
    ) -> Result<Response<SendCustomDataResponse>, Status> {
        let _ = require_ctx_from_request(&request)?;
        let _req = request.into_inner();
        Err(Status::unimplemented(
            "TODO: SendCustomData — uplink CustomData not implemented in orchestrator yet",
        ))
    }
}

    #[tonic::async_trait]
    impl MessageActionService for MessageGrpcHandler {
    #[instrument(skip(self, request))]
        async fn recall_message(
        &self,
        request: Request<MessageRecallMessageRequest>,
    ) -> Result<Response<MessageRecallMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        // 将protobuf请求转换为应用层命令
        let app_command = AppRecallMessageCommand {
            message_id: req.message_id.clone(),
            reason: if req.reason.is_empty() { None } else { Some(req.reason.clone()) },
            time_limit_seconds: if req.recall_time_limit_seconds > 0 { Some(req.recall_time_limit_seconds) } else { None },
            operator_id: ctx.actor().map(|a| a.actor_id().to_string()).unwrap_or_default(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(), // 会在后续填充
        };

        // 调用应用层操作处理器处理撤回消息逻辑
        match self.operation_handler.handle_recall_message_app(&ctx, &app_command).await {
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
            operator_id: ctx.actor().map(|a| a.actor_id().to_string()).unwrap_or_default(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id: String::new(), // 会在后续填充
        };

        // 调用应用层操作处理器处理编辑消息逻辑
        match self.operation_handler.handle_edit_message_app(&ctx, &app_command).await {
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

        // 将protobuf请求转换为应用层命令
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
            reason: if req.reason.is_empty() { None } else { Some(req.reason.clone()) },
            notify_others: req.notify_others,
            target_user_id: Some(operator_id.to_string()),
            hard_delete: req.delete_type == 2,
            operator_id: operator_id.to_string(),
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
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        // 将protobuf请求转换为应用层命令（user_id 从 context 取）
        let app_command = AppMarkMessageCommand {
            message_id: req.message_id.clone(),
            user_id: user_id.to_string(),
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
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        // 将protobuf请求转换为应用层命令（user_id 从 context 取）
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
    async fn mark_messages_read_until(
        &self,
        request: Request<MessageMarkMessagesReadUntilRequest>,
    ) -> Result<Response<MessageMarkMessagesReadUntilResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        // 将protobuf请求转换为应用层命令（user_id 从 context 取）
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
        async fn mark_conversation_read(
        &self,
        request: Request<MessageMarkConversationReadRequest>,
    ) -> Result<Response<MessageMarkConversationReadResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        // 将protobuf请求转换为应用层命令（user_id 从 context 取）
        let app_command = AppMarkConversationReadCommand {
            conversation_id: req.conversation_id.clone(),
            user_id: user_id.to_string(),
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
        
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        // 将protobuf请求转换为应用层命令（user_id 从 context 取）
        let app_command = AppMarkAllConversationsReadCommand {
            user_id: user_id.to_string(),
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
        async fn add_reaction(
        &self,
        request: Request<MessageAddReactionRequest>,
    ) -> Result<Response<MessageAddReactionResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        // 将protobuf请求转换为应用层命令（user_id 从 context 取）
        let app_command = AppAddReactionCommand {
            message_id: req.message_id.clone(),
            user_id: user_id.to_string(),
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
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        // 将protobuf请求转换为应用层命令（user_id 从 context 取）
        let app_command = AppRemoveReactionCommand {
            message_id: req.message_id.clone(),
            user_id: user_id.to_string(),
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

        #[instrument(skip(self, request))]
    async fn pin_message(
        &self,
        request: Request<MessagePinMessageRequest>,
    ) -> Result<Response<MessagePinMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let operator_id = ctx.actor().map(|a| a.actor_id().to_string()).unwrap_or_default();
        // 将protobuf请求转换为应用层命令（operator_id 从 context 取）
        let app_command = AppPinMessageCommand {
            message_id: req.message_id.clone(),
            operator_id,
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
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let operator_id = ctx.actor().map(|a| a.actor_id().to_string()).unwrap_or_default();
        // 将protobuf请求转换为应用层命令（operator_id 从 context 取）
        let app_command = AppUnpinMessageCommand {
            message_id: req.message_id.clone(),
            operator_id,
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
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        // 将protobuf请求转换为应用层命令（user_id 从 context 取）
        let app_command = AppMarkMessageCommand {
            message_id: req.message_id.clone(),
            user_id: user_id.to_string(),
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
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        // 将protobuf请求转换为应用层命令（user_id 从 context 取）
        let app_command = AppUnmarkMessageCommand {
            message_id: req.message_id.clone(),
            user_id: user_id.to_string(),
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
}

    #[tonic::async_trait]
    impl MessageQueryService for MessageGrpcHandler {
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
                    ..Default::default()
                }),
            }))
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
                    ..Default::default()
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
                    ..Default::default()
                }),
            }))
        }

    #[instrument(skip(self, request))]
    async fn get_pinned_messages(
        &self,
        request: Request<flare_proto::message::GetPinnedMessagesRequest>,
    ) -> Result<Response<flare_proto::message::GetPinnedMessagesResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
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
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let user_id = ctx.actor().map(|a| a.actor_id.clone()).unwrap_or_default();
        // 将protobuf请求转换为应用层命令（user_id 从 context 取）
        let app_command = AppGetMarkedMessagesCommand {
            user_id: user_id.to_string(),
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
        let ctx = require_ctx_from_request(&request)?;
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
        let ctx = require_ctx_from_request(&request)?;
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
}
