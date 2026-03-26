//! [`IMessageCommandPort`]：经 Signaling **Router** `RouteMessage` 转发（与 `domain/ports/message_port.rs` 对应）
//!
//! 请求体为 [`flare_proto::common::Message`]；响应解码为 [`flare_proto::message::SendMessageResponse`] 并映射为 [`flare_proto::common::SendAck`]。

use std::sync::Arc;

use async_trait::async_trait;
use flare_im_core::Ctx;
use flare_proto::common::{ErrorCode, Message, RpcStatus, SendAck};
use flare_proto::message::SendMessageResponse;
use flare_proto::signaling::router::{RouteMessageRequest, RouteOptions};
use flare_server_core::client::request_with_context;
use flare_server_core::error::{ErrorBuilder, ErrorCode as ServerErrorCode, Result};
use prost::Message as ProstMessage;
use prost_types::Timestamp;

use crate::constants::DEFAULT_ROUTE_SVID;
use crate::domain::ports::IMessageCommandPort;

use super::route_grpc_pool::SignalingRouteGrpcPool;

/// 通过 `RouterService.RouteMessage` 投递上行消息
pub struct RouterMessageCommandPort {
    pool: Arc<SignalingRouteGrpcPool>,
}

impl RouterMessageCommandPort {
    pub fn new(pool: Arc<SignalingRouteGrpcPool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl IMessageCommandPort for RouterMessageCommandPort {
    async fn send_message(&self, tx: &Ctx, message: Message) -> Result<SendAck> {
        let mut client = self.pool.ensure_client().await?;
        let req = RouteMessageRequest {
            svid: DEFAULT_ROUTE_SVID.to_string(),
            message: Some(message.clone()),
            options: Some(RouteOptions {
                timeout_seconds: 15,
                enable_tracing: true,
                retry_strategy: 1, // RETRY_STRATEGY_NONE
                load_balance_strategy: 1,
                priority: 0,
            }),
        };

        let request = request_with_context(req, tx);
        let resp = client.route_message(request).await
            .map_err(|status| {
                ErrorBuilder::new(ServerErrorCode::ServiceUnavailable, "RouteMessage RPC failed")
                    .details(status.to_string())
                    .build_error()
            })?
            .into_inner();

        if let Some(st) = resp.status.as_ref()
            && st.code != ErrorCode::Ok as i32 {
                return Ok(send_ack_from_failure(
                    &message,
                    st.code,
                    st.message.clone(),
                ));
            }

        if resp.response_data.is_empty() {
            return Ok(send_ack_from_failure(
                &message,
                ErrorCode::Internal as i32,
                "empty RouteMessageResponse.response_data".to_string(),
            ));
        }

        let send_resp = SendMessageResponse::decode(resp.response_data.as_slice()).map_err(|e| {
            ErrorBuilder::new(ServerErrorCode::InternalError, "decode SendMessageResponse")
                .details(e.to_string())
                .build_error()
        })?;

        let status = send_resp
            .status
            .or(resp.status)
            .unwrap_or_else(|| RpcStatus {
                code: ErrorCode::Internal as i32,
                message: "missing status".to_string(),
                details: Vec::new(),
                context: None,
                localization_key: String::new(),
                localization_params: Default::default(),
            });

        if status.code != ErrorCode::Ok as i32 || !send_resp.success {
            return Ok(send_ack_from_failure(
                &message,
                status.code,
                status.message,
            ));
        }

        Ok(SendAck {
            client_msg_id: message.client_msg_id.clone(),
            server_msg_id: send_resp.server_msg_id,
            seq: send_resp.seq,
            conversation_id: message.conversation_id.clone(),
            success: true,
            error_code: ErrorCode::Ok as i32,
            error_message: String::new(),
            server_time: send_resp.sent_at.or_else(|| Some(Timestamp {
                seconds: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0),
                nanos: 0,
            })),
            ack_id: None,
            metadata: Default::default(),
        })
    }
}

fn send_ack_from_failure(message: &Message, code: i32, msg: String) -> SendAck {
    SendAck {
        client_msg_id: message.client_msg_id.clone(),
        server_msg_id: String::new(),
        seq: 0,
        conversation_id: message.conversation_id.clone(),
        success: false,
        error_code: code,
        error_message: msg,
        server_time: None,
        ack_id: None,
        metadata: Default::default(),
    }
}
