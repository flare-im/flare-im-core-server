//! [`IMessageCommandPort`]：经 Signaling **Router** `RouteMessage` 转发（与 `domain/ports/message_port.rs` 对应）
//!
//! 请求体为 [`flare_proto::common::Message`]；响应解码为 [`flare_grpc_proto::message::SendMessageResponse`] 并映射为 [`flare_proto::common::SendAck`]。
//! 路由层错误仅通过 gRPC `Status` 表达；`RouteMessageResponse` / `SendMessageResponse` 不再携带 `RpcStatus`。

use std::sync::Arc;

use async_trait::async_trait;
use flare_grpc_proto::message::SendMessageResponse;
use flare_grpc_proto::signaling::router::{RouteMessageRequest, RouteOptions};
use flare_im_contracts::Ctx;
use flare_proto::common::{
    ErrorCode, Message, MessageSource, SendAccepted, SendAck, SendAckDurability, send_ack,
};
use flare_server_core::client::request_with_context;
use flare_server_core::error::{ErrorBuilder, ErrorCode as ServerErrorCode, Result};
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, set_signed_context_token,
};
use prost::Message as ProstMessage;

use crate::constants::DEFAULT_ROUTE_SVID;
use crate::domain::ports::IMessageCommandPort;

use super::route_grpc_pool::SignalingRouteGrpcPool;

/// 通过 `RouterService.RouteMessage` 投递上行消息
pub struct RouterMessageCommandPort {
    pool: Arc<SignalingRouteGrpcPool>,
    signer: Option<SignedContextSigner>,
}

impl RouterMessageCommandPort {
    pub fn new(pool: Arc<SignalingRouteGrpcPool>, signer: Option<SignedContextSigner>) -> Self {
        Self { pool, signer }
    }
}

#[async_trait]
impl IMessageCommandPort for RouterMessageCommandPort {
    async fn send_message(&self, tx: &Ctx, message: Message) -> Result<SendAck> {
        let origin = tx
            .get_data::<crate::infrastructure::connection_context::AuthenticatedConnectionOrigin>()
            .ok_or_else(|| {
                ErrorBuilder::new(
                    ServerErrorCode::PermissionDenied,
                    "authenticated connection required",
                )
                .build_error()
            })?;
        if tx.user_id() != Some(origin.user_id())
            || tx.tenant_id() != Some(origin.tenant_id())
            || message.sender_id != origin.user_id()
            || message.source != MessageSource::User as i32
        {
            return Err(ErrorBuilder::new(
                ServerErrorCode::PermissionDenied,
                "message sender does not match authenticated connection",
            )
            .build_error());
        }
        let signer = self.signer.as_ref().ok_or_else(|| {
            ErrorBuilder::new(
                ServerErrorCode::ServiceUnavailable,
                "signed message context unavailable",
            )
            .build_error()
        })?;
        let token = signer
            .sign(&SignedContextIdentity::new(
                SignedActorType::User,
                origin.user_id(),
                origin.tenant_id(),
                tx.request_id(),
            ))
            .map_err(|_| {
                ErrorBuilder::new(
                    ServerErrorCode::ServiceUnavailable,
                    "signed message context unavailable",
                )
                .build_error()
            })?;
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

        let mut request = request_with_context(req, tx);
        set_signed_context_token(&mut request, &token).map_err(|_| {
            ErrorBuilder::new(
                ServerErrorCode::ServiceUnavailable,
                "signed message context unavailable",
            )
            .build_error()
        })?;
        let resp = client
            .route_message(request)
            .await
            .map_err(|status| {
                if let Some(detail) = flare_server_core::error::grpc::decode_error_detail(&status) {
                    let code = ServerErrorCode::from_u32(detail.code.max(0) as u32)
                        .unwrap_or(ServerErrorCode::GeneralError);
                    return ErrorBuilder::new(code, detail.reason)
                        .details(detail.message)
                        .build_error();
                }
                ErrorBuilder::new(
                    ServerErrorCode::ServiceUnavailable,
                    "RouteMessage RPC failed",
                )
                .details(status.to_string())
                .build_error()
            })?
            .into_inner();

        if resp.response_data.is_empty() {
            return Ok(send_ack_from_failure(
                &message,
                ErrorCode::Internal as i32,
                "empty RouteMessageResponse.response_data".to_string(),
            ));
        }

        let send_resp =
            SendMessageResponse::decode(resp.response_data.as_slice()).map_err(|e| {
                ErrorBuilder::new(ServerErrorCode::InternalError, "decode SendMessageResponse")
                    .details(e.to_string())
                    .build_error()
            })?;

        if !send_resp.success {
            return Ok(send_ack_from_failure(
                &message,
                ErrorCode::Internal as i32,
                "SendMessageResponse.success is false".to_string(),
            ));
        }
        if send_resp.server_msg_id.trim().is_empty() {
            return Ok(send_ack_from_failure(
                &message,
                ErrorCode::Internal as i32,
                "SendMessageResponse.server_msg_id is empty".to_string(),
            ));
        }
        if send_resp.conversation_seq == 0 {
            return Ok(send_ack_from_failure(
                &message,
                ErrorCode::Internal as i32,
                "SendMessageResponse.conversation_seq is zero".to_string(),
            ));
        }
        if send_resp.durability() == SendAckDurability::Unspecified {
            return Ok(send_ack_from_failure(
                &message,
                ErrorCode::Internal as i32,
                "SendMessageResponse.durability is unspecified".to_string(),
            ));
        }

        Ok(SendAck {
            client_msg_id: message.client_msg_id.clone(),
            conversation_id: message.conversation_id.clone(),
            ack_id: None,
            result: Some(send_ack::Result::Accepted(SendAccepted {
                server_msg_id: send_resp.server_msg_id.trim().to_string(),
                conversation_seq: send_resp.conversation_seq,
                server_time: send_resp
                    .sent_at
                    .as_ref()
                    .map(timestamp_to_millis)
                    .unwrap_or_else(now_millis),
                durability: send_resp.durability,
            })),
        })
    }
}

fn send_ack_from_failure(message: &Message, code: i32, msg: String) -> SendAck {
    SendAck {
        client_msg_id: message.client_msg_id.clone(),
        conversation_id: message.conversation_id.clone(),
        ack_id: None,
        result: Some(send_ack::Result::Error(flare_proto::common::ErrorDetail {
            code,
            reason: "MESSAGE_SEND_FAILED".to_string(),
            message: msg,
            track: String::new(),
        })),
    }
}

fn timestamp_to_millis(ts: &prost_types::Timestamp) -> i64 {
    ts.seconds.saturating_mul(1000) + (ts.nanos as i64 / 1_000_000)
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}
