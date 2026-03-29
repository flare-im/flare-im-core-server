//! `MessageSendService` gRPC 实现：发送、批量发送、系统消息、ExecuteEvent 等。

use std::sync::Arc;

use crate::application::handlers::{MessageCommandHandler, MessageOperationHandler};
use flare_im_core::error::ok_status;
use flare_proto::message::{
    BatchSendMessageRequest, BatchSendMessageResponse, ExecuteEventRequest, SendAckRequest,
    SendAckResponse, SendCustomDataRequest, SendCustomDataResponse, SendMessageRequest,
    SendMessageResponse, SendSystemMessageRequest, SendSystemMessageResponse,
};
use prost_types;
use tonic::{Request, Response, Status};
use tracing::{error, info, instrument};

use crate::application::commands::SendSystemMessageCommand;
use flare_proto::message::message_send_service_server::MessageSendService;
use flare_server_core::utils::require_ctx_from_request;

/// 上行发送 gRPC：`SendMessage` / `BatchSendMessage` / `SendSystemMessage` / `ExecuteEvent` 等。
#[derive(Clone)]
pub struct MessageSendGrpcHandler {
    command_handler: Arc<MessageCommandHandler>,
    operation_handler: Arc<MessageOperationHandler>,
}

impl MessageSendGrpcHandler {
    pub fn new(
        command_handler: Arc<MessageCommandHandler>,
        operation_handler: Arc<MessageOperationHandler>,
    ) -> Self {
        Self {
            command_handler,
            operation_handler,
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
        let ctx = require_ctx_from_request(&request)?;

        let req = request.into_inner();

        let cmd = crate::application::commands::BatchSendMessageCommand {
            requests: req.messages,
        };

        match self
            .command_handler
            .handle_batch_send_message(&ctx, cmd)
            .await
        {
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
                        code: 500,
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
        let ctx = require_ctx_from_request(&request)?;

        let req = request.into_inner();

        let command = SendSystemMessageCommand {
            conversation_id: req.conversation_id.clone(),
            message: req
                .message
                .ok_or_else(|| Status::invalid_argument("message is required"))?,
            system_message_type: req.system_message_type.clone(),
        };

        match self
            .command_handler
            .handle_send_system_message(&ctx, command)
            .await
        {
            Ok(message_id) => {
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
    async fn execute_event(
        &self,
        request: Request<ExecuteEventRequest>,
    ) -> Result<Response<flare_proto::common::OperationResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        if !req.svid.is_empty() {
            tracing::debug!(svid = %req.svid, "ExecuteEvent");
        }
        let event = req
            .event
            .ok_or_else(|| Status::invalid_argument("event required"))?;
        let resp = self
            .operation_handler
            .handle_execute_event_app(&ctx, event)
            .await;
        Ok(Response::new(resp))
    }

    #[instrument(skip(self, request))]
    async fn send_ack(
        &self,
        request: Request<SendAckRequest>,
    ) -> Result<Response<SendAckResponse>, Status> {
        let _ = require_ctx_from_request(&request)?;
        let _req = request.into_inner();
        Err(Status::unimplemented(
            "TODO: SendAck — uplink client ACK not implemented in orchestrator yet",
        ))
    }

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
