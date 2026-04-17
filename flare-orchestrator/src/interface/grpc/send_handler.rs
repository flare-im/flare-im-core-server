//! `MessageSendService` gRPC 实现：发送、批量发送、系统消息、ExecuteEvent 等。

use std::sync::Arc;

use crate::application::handlers::{EventHandler, MessageHandler};
use flare_grpc_proto::message::{
    BatchSendMessageRequest, BatchSendMessageResponse, ExecuteEventRequest, SendAckRequest,
    SendAckResponse, SendCustomDataRequest, SendCustomDataResponse, SendMessageRequest,
    SendMessageResponse, SendSystemMessageRequest, SendSystemMessageResponse,
    FailedMessage,
};
use prost_types;
use tonic::{Request, Response, Status};
use tracing::{debug, instrument};
use flare_grpc_proto::message::message_send_service_server::MessageSendService;
use flare_server_core::utils::require_ctx_from_request;
use flare_server_core::error::grpc::IntoGrpc;

/// 上行发送 gRPC：`SendMessage` / `BatchSendMessage` / `SendSystemMessage` / `ExecuteEvent` 等。
#[derive(Clone)]
pub struct MessageSendGrpcHandler {
    message_handler: Arc<MessageHandler>,
    execute_event_handler: Arc<EventHandler>,
}

impl MessageSendGrpcHandler {
    pub fn new(
        message_handler: Arc<MessageHandler>,
        execute_event_handler: Arc<EventHandler>,
    ) -> Self {
        Self {
            message_handler,
            execute_event_handler,
        }
    }
}

#[tonic::async_trait]
impl MessageSendService for MessageSendGrpcHandler {
    #[instrument(skip(self, request))]
    async fn send_message(
        &self,
        request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;

        let req = request.into_inner();
        let message = req
            .message
            .clone()
            .ok_or_else(|| Status::invalid_argument("message required"))?;

        let cmd = crate::application::commands::SendMessageCommand {
            message,
            conversation_id: req.conversation_id.clone(),
            sync: req.sync,
        };

        match self.message_handler.handle_send_message(&ctx, cmd).await {
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
                }))
            }
            Err(err) => {
                // 使用 IntoGrpc trait 将 FlareError 转换为包含 ErrorDetail 的 Status
                Err(Status::from(err))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn batch_send_message(
        &self,
        request: Request<BatchSendMessageRequest>,
    ) -> Result<Response<BatchSendMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        // 构建批量发送命令
        let messages: Vec<crate::application::commands::SendMessageCommand> = req
            .messages
            .into_iter()
            .filter_map(|m| {
                m.message.map(|msg| crate::application::commands::SendMessageCommand {
                    message: msg,
                    conversation_id: m.conversation_id,
                    sync: m.sync,
                })
            })
            .collect();

        // 调用 application 层的批量发送方法
        let results = self.message_handler.batch_send_message(&ctx, messages).await.into_grpc()?;

        // 统计成功和失败数量，构建响应
        let mut message_ids = Vec::new();
        let mut failures = Vec::new();

        for result in results {
            match result {
                Ok((msg_id, _seq)) => message_ids.push(msg_id),
                Err(e) => failures.push(FailedMessage {
                    message_id: String::new(),
                    code: 1, // TODO: 使用实际的错误码
                    error_message: e.to_string(),
                }),
            }
        }

        let success_count = message_ids.len() as i32;
        let fail_count = failures.len() as i32;

        Ok(Response::new(BatchSendMessageResponse {
            success_count,
            fail_count,
            message_ids,
            failures,
        }))
    }

    #[instrument(skip(self, request))]
    async fn send_system_message(
        &self,
        request: Request<SendSystemMessageRequest>,
    ) -> Result<Response<SendSystemMessageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let cmd = crate::application::commands::SendSystemMessageCommand {
            conversation_id: req.conversation_id,
            message: req.message.unwrap_or_default(),
            system_message_type: req.system_message_type,
        };

        match self.message_handler.send_system_message(&ctx, cmd).await {
            Ok(message_id) => {
                Ok(Response::new(SendSystemMessageResponse {
                    success: true,
                    message_id,
                }))
            }
            Err(err) => {
                // 使用 IntoGrpc trait 将 FlareError 转换为包含 ErrorDetail 的 Status
                Err(Status::from(err))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn execute_event(
        &self,
        request: Request<ExecuteEventRequest>,
    ) -> Result<Response<()>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        if !req.svid.is_empty() {
            debug!(svid = %req.svid, "ExecuteEvent");
        }
        let event = req
            .event
            .ok_or_else(|| Status::invalid_argument("event required"))?;

        // 使用 IntoGrpc trait 将 FlareError 转换为包含 ErrorDetail 的 Status
        self.execute_event_handler
            .handle_event(&ctx, event)
            .await
            .into_grpc()?;

        Ok(Response::new(()))
    }

    #[instrument(skip(self, request))]
    async fn send_ack(
        &self,
        request: Request<SendAckRequest>,
    ) -> Result<Response<SendAckResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        // 从 Ack 中提取 ack_id (作为 message_id 使用)
        let ack = req.ack.ok_or_else(|| Status::invalid_argument("ack required"))?;
        let message_id = ack.ack_id.clone().unwrap_or_default();

        // 调用 application 层的 ACK 方法
        self.message_handler.send_ack(&ctx, &message_id).await.into_grpc()?;

        Ok(Response::new(SendAckResponse {
            routed_endpoint: String::new(), // 可选字段，用于监控/调试
        }))
    }

    #[instrument(skip(self, request))]
    async fn send_custom_data(
        &self,
        request: Request<SendCustomDataRequest>,
    ) -> Result<Response<SendCustomDataResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        // 从 CustomData 中提取数据
        let custom_data = req.data.ok_or_else(|| Status::invalid_argument("data required"))?;

        // 调用 application 层的自定义数据发送方法
        self.message_handler.send_custom_data(&ctx, custom_data.payload).await.into_grpc()?;

        Ok(Response::new(SendCustomDataResponse {
            response_data: vec![], // 可选：下游返回的扩展载荷
            routed_endpoint: String::new(),
        }))
    }
}
