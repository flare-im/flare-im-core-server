//! `MessageSendService` gRPC 实现：发送、批量发送、系统消息等。

use std::sync::Arc;
use std::time::Instant;

use crate::application::commands::SendMessageOutcome;
use crate::application::handlers::MessageIngestHandler;
use flare_grpc_proto::message::message_send_service_server::MessageSendService;
use flare_grpc_proto::message::{
    BatchSendMessageRequest, BatchSendMessageResponse, FailedMessage, SendAckRequest,
    SendAckResponse, SendCustomDataRequest, SendCustomDataResponse, SendMessageRequest,
    SendMessageResponse, SendSystemMessageRequest, SendSystemMessageResponse,
};
use flare_im_contracts::utils::normalize_tenant_id;
use flare_im_service_kit::metrics::MessageOrchestratorMetrics;
use flare_proto::common::{Message, MessageSource, MessageType};
use flare_server_core::auth::{SignedActorType, SignedContextClaims, SignedContextVerifier};
use flare_server_core::error::grpc::IntoGrpc;
use flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata;
use flare_server_core::utils::require_ctx_from_request;
use prost_types;
use tonic::{Request, Response, Status};
use tracing::instrument;

/// 上行发送 gRPC：`SendMessage` / `BatchSendMessage` / `SendSystemMessage` 等。
#[derive(Clone)]
pub struct MessageSendGrpcHandler {
    message_ingest_handler: Arc<MessageIngestHandler>,
    metrics: Arc<MessageOrchestratorMetrics>,
    message_verifier: Option<SignedContextVerifier>,
}

impl MessageSendGrpcHandler {
    pub fn new(
        message_ingest_handler: Arc<MessageIngestHandler>,
        metrics: Arc<MessageOrchestratorMetrics>,
        message_verifier: Option<SignedContextVerifier>,
    ) -> Self {
        Self {
            message_ingest_handler,
            metrics,
            message_verifier,
        }
    }

    fn verified_send_ctx<T>(
        &self,
        request: &Request<T>,
    ) -> Result<flare_server_core::context::Ctx, Status> {
        let verifier = self
            .message_verifier
            .as_ref()
            .ok_or_else(|| Status::unavailable("message identity verifier not configured"))?;
        verify_signed_context_metadata(request.metadata(), verifier)
    }
}

/// The message envelope is untrusted data. Only a signed, source-bound actor
/// may send it; legacy message attributes never confer internal privileges.
fn authorize_message(
    ctx: &flare_server_core::context::Ctx,
    message: &Message,
    system_only: bool,
) -> Result<(), Status> {
    use flare_server_core::grpc::middleware::signed_context::VerifiedContext;
    let claims = ctx
        .get_data::<VerifiedContext>()
        .and_then(|verified| verified.claims())
        .ok_or_else(|| Status::unauthenticated("signed message identity required"))?;
    authorize_message_claims(claims, message, system_only)
}

fn authorize_message_claims(
    claims: &SignedContextClaims,
    message: &Message,
    system_only: bool,
) -> Result<(), Status> {
    if claims.tenant_id != normalize_tenant_id(&claims.tenant_id) {
        return Err(Status::permission_denied("non-canonical tenant identity"));
    }
    if message
        .attributes
        .contains_key("x-social-internal-send-token")
    {
        return Err(Status::permission_denied(
            "legacy internal send credential forbidden",
        ));
    }
    for (key, expected) in [
        ("x-tenant-id", claims.tenant_id.as_str()),
        ("x-request-id", claims.request_id.as_str()),
    ] {
        if message
            .attributes
            .get(key)
            .is_some_and(|actual| actual != expected)
        {
            return Err(Status::permission_denied(
                "message identity attribute mismatch",
            ));
        }
    }
    match claims.actor_type {
        SignedActorType::User if !system_only => {
            if message.sender_id != claims.actor_id
                || message.source != MessageSource::User as i32
                || matches!(
                    MessageType::try_from(message.message_type),
                    Ok(MessageType::System | MessageType::Notification)
                )
                || matches!(
                    message.attributes.get("message_type").map(String::as_str),
                    Some("system" | "notification")
                )
                || message
                    .attributes
                    .get("x-user-id")
                    .is_some_and(|actual| actual != &claims.actor_id)
            {
                return Err(Status::permission_denied(
                    "message sender does not match signed user",
                ));
            }
        }
        SignedActorType::Service if claims.actor_id == "flare-social-bridge" => {
            let expected_type = if system_only {
                MessageType::System
            } else {
                match MessageType::try_from(message.message_type) {
                    Ok(MessageType::System) => MessageType::System,
                    Ok(MessageType::Notification) => MessageType::Notification,
                    _ => {
                        return Err(Status::permission_denied(
                            "service may only send internal messages",
                        ));
                    }
                }
            };
            if message.source != MessageSource::System as i32
                || message.message_type != expected_type as i32
                || message.sender_id.trim().is_empty()
                || message.attributes.contains_key("x-user-id")
            {
                return Err(Status::permission_denied("invalid signed bridge message"));
            }
        }
        _ => {
            return Err(Status::permission_denied(
                "actor not permitted to send message",
            ));
        }
    }
    Ok(())
}

#[tonic::async_trait]
impl MessageSendService for MessageSendGrpcHandler {
    #[instrument(skip(self, request))]
    async fn send_message(
        &self,
        request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        let decode_start = Instant::now();
        let ctx = match self.verified_send_ctx(&request) {
            Ok(ctx) => ctx,
            Err(status) => {
                self.metrics.observe_send_stage(
                    "grpc_request_decode",
                    "error",
                    decode_start.elapsed(),
                );
                return Err(status);
            }
        };

        let req = request.into_inner();
        let message = req.message.clone().ok_or_else(|| {
            self.metrics
                .observe_send_stage("grpc_request_decode", "error", decode_start.elapsed());
            Status::invalid_argument("message required")
        })?;
        authorize_message(&ctx, &message, false)?;
        self.metrics
            .observe_send_stage("grpc_request_decode", "success", decode_start.elapsed());

        let cmd = crate::application::commands::SendMessageCommand {
            burn_enabled: false,
            burn_after_read_seconds: None,
            message,
            conversation_id: req.conversation_id.clone(),
            sync: req.sync,
        };

        match self
            .message_ingest_handler
            .handle_send_message(&ctx, cmd)
            .await
        {
            Ok(outcome) => {
                let response_start = Instant::now();
                let response = send_response_from_outcome(outcome);
                self.metrics.observe_send_stage(
                    "grpc_response_build",
                    "success",
                    response_start.elapsed(),
                );
                Ok(Response::new(response))
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
        let decode_start = Instant::now();
        let ctx = match self.verified_send_ctx(&request) {
            Ok(ctx) => ctx,
            Err(status) => {
                self.metrics.observe_send_stage(
                    "grpc_batch_request_decode",
                    "error",
                    decode_start.elapsed(),
                );
                return Err(status);
            }
        };
        let req = request.into_inner();
        if req.messages.len() > 100 {
            self.metrics.observe_send_stage(
                "grpc_batch_request_decode",
                "error",
                decode_start.elapsed(),
            );
            return Err(Status::invalid_argument(
                "batch send supports at most 100 messages",
            ));
        }

        // 构建批量发送命令；缺失 message 的请求显式进入 failures，不静默丢弃。
        let mut messages = Vec::with_capacity(req.messages.len());
        let mut failures = Vec::new();
        for m in req.messages {
            match m.message {
                Some(msg) => {
                    authorize_message(&ctx, &msg, false)?;
                    messages.push(crate::application::commands::SendMessageCommand {
                        burn_enabled: false,
                        burn_after_read_seconds: None,
                        message: msg,
                        conversation_id: m.conversation_id,
                        sync: m.sync,
                    })
                }
                None => failures.push(FailedMessage {
                    message_id: String::new(),
                    code: 400,
                    error_message: "message required".to_string(),
                }),
            }
        }
        self.metrics.observe_send_stage(
            "grpc_batch_request_decode",
            "success",
            decode_start.elapsed(),
        );

        // 调用 application 层的批量发送方法
        let results = self
            .message_ingest_handler
            .batch_send_message(&ctx, messages)
            .await
            .into_grpc()?;

        // 统计成功和失败数量，构建响应
        let mut successes = Vec::new();

        for result in results {
            match result {
                Ok(outcome) => successes.push(send_response_from_outcome(outcome)),
                Err(e) => failures.push(FailedMessage {
                    message_id: String::new(),
                    code: e.code().map(|code| code.as_u32() as i32).unwrap_or(1),
                    error_message: e.to_string(),
                }),
            }
        }

        let success_count = successes.len() as i32;
        let fail_count = failures.len() as i32;

        let response_start = Instant::now();
        let response = BatchSendMessageResponse {
            success_count,
            fail_count,
            successes,
            failures,
        };
        self.metrics.observe_send_stage(
            "grpc_batch_response_build",
            "success",
            response_start.elapsed(),
        );

        Ok(Response::new(response))
    }

    #[instrument(skip(self, request))]
    async fn send_system_message(
        &self,
        request: Request<SendSystemMessageRequest>,
    ) -> Result<Response<SendSystemMessageResponse>, Status> {
        let ctx = self.verified_send_ctx(&request)?;
        let req = request.into_inner();
        let message = req.message.unwrap_or_default();
        authorize_message(&ctx, &message, true)?;

        let cmd = crate::application::commands::SendSystemMessageCommand {
            conversation_id: req.conversation_id,
            message,
            system_message_type: req.system_message_type,
        };

        match self
            .message_ingest_handler
            .send_system_message(&ctx, cmd)
            .await
        {
            Ok(message_id) => Ok(Response::new(SendSystemMessageResponse {
                success: true,
                message_id,
            })),
            Err(err) => {
                // 使用 IntoGrpc trait 将 FlareError 转换为包含 ErrorDetail 的 Status
                Err(Status::from(err))
            }
        }
    }

    #[instrument(skip(self, request))]
    async fn send_ack(
        &self,
        request: Request<SendAckRequest>,
    ) -> Result<Response<SendAckResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        // 从 Ack 中提取 ack_id (作为 message_id 使用)
        let ack = req
            .ack
            .ok_or_else(|| Status::invalid_argument("ack required"))?;
        let message_id = ack.ack_id.clone().unwrap_or_default();

        // 调用 application 层的 ACK 方法
        self.message_ingest_handler
            .send_ack(&ctx, &message_id)
            .await
            .into_grpc()?;

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
        let custom_data = req
            .data
            .ok_or_else(|| Status::invalid_argument("data required"))?;

        // 调用 application 层的自定义数据发送方法
        self.message_ingest_handler
            .send_custom_data(&ctx, custom_data.payload)
            .await
            .into_grpc()?;

        Ok(Response::new(SendCustomDataResponse {
            response_data: vec![], // 可选：下游返回的扩展载荷
            routed_endpoint: String::new(),
        }))
    }
}

fn send_response_from_outcome(outcome: SendMessageOutcome) -> SendMessageResponse {
    let now = chrono::Utc::now();
    SendMessageResponse {
        success: true,
        server_msg_id: outcome.message_id,
        conversation_seq: outcome.conversation_seq,
        sent_at: Some(prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        }),
        timeline: Some(flare_proto::common::MessageTimeline {
            created_at: now.timestamp_millis(),
            persisted_at: None,
            delivered_at: None,
            read_at: None,
        }),
        durability: outcome.durability as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::SendAckDurability;

    fn claims(actor_type: SignedActorType, actor_id: &str) -> SignedContextClaims {
        SignedContextClaims {
            sub: actor_id.into(),
            iss: "flare".into(),
            aud: "internal".into(),
            token_use: "internal_context".into(),
            iat: 1,
            exp: 2,
            jti: "test".into(),
            actor_type,
            actor_id: actor_id.into(),
            tenant_id: "tenant-a".into(),
            roles: Vec::new(),
            credential_id: None,
            request_id: "req-1".into(),
        }
    }

    fn message(sender: &str, source: MessageSource, kind: MessageType) -> Message {
        Message {
            sender_id: sender.into(),
            source: source as i32,
            message_type: kind as i32,
            ..Default::default()
        }
    }

    #[test]
    fn user_sender_and_internal_source_cannot_be_spoofed() {
        let user = claims(SignedActorType::User, "alice");
        assert!(
            authorize_message_claims(
                &user,
                &message("alice", MessageSource::User, MessageType::Text),
                false
            )
            .is_ok()
        );
        assert_eq!(
            authorize_message_claims(
                &user,
                &message("bob", MessageSource::User, MessageType::Text),
                false
            )
            .unwrap_err()
            .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            authorize_message_claims(
                &user,
                &message("alice", MessageSource::System, MessageType::Notification),
                false
            )
            .unwrap_err()
            .code(),
            tonic::Code::PermissionDenied
        );
        assert!(
            authorize_message_claims(
                &user,
                &message("alice", MessageSource::User, MessageType::Notification),
                false,
            )
            .is_err()
        );
        assert_eq!(
            authorize_message_claims(
                &user,
                &message("alice", MessageSource::User, MessageType::Text),
                true
            )
            .unwrap_err()
            .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn only_exact_bridge_identity_can_send_internal_message() {
        let bridge = claims(SignedActorType::Service, "flare-social-bridge");
        assert!(
            authorize_message_claims(
                &bridge,
                &message("alice", MessageSource::System, MessageType::Notification),
                false
            )
            .is_ok()
        );
        assert!(
            authorize_message_claims(
                &bridge,
                &message("alice", MessageSource::System, MessageType::System),
                true
            )
            .is_ok()
        );
        assert!(
            authorize_message_claims(
                &claims(SignedActorType::Service, "other"),
                &message("alice", MessageSource::System, MessageType::System),
                true
            )
            .is_err()
        );
        assert!(
            authorize_message_claims(
                &bridge,
                &message("alice", MessageSource::System, MessageType::Text),
                false
            )
            .is_err()
        );
        assert!(
            authorize_message_claims(
                &bridge,
                &message("alice", MessageSource::User, MessageType::System),
                true
            )
            .is_err()
        );
        assert!(
            authorize_message_claims(
                &bridge,
                &message("alice", MessageSource::System, MessageType::Notification),
                true
            )
            .is_err()
        );
    }

    #[test]
    fn legacy_secret_and_cross_tenant_attributes_are_not_authority() {
        let user = claims(SignedActorType::User, "alice");
        let mut msg = message("alice", MessageSource::User, MessageType::Text);
        msg.attributes
            .insert("x-tenant-id".into(), "tenant-b".into());
        assert!(authorize_message_claims(&user, &msg, false).is_err());
        msg.attributes.clear();
        msg.attributes
            .insert("x-social-internal-send-token".into(), "secret".into());
        assert!(authorize_message_claims(&user, &msg, false).is_err());
        msg.attributes.clear();
        msg.attributes.insert("x-user-id".into(), "bob".into());
        assert!(authorize_message_claims(&user, &msg, false).is_err());
    }

    #[test]
    fn send_response_preserves_ack_durability() {
        let response = send_response_from_outcome(SendMessageOutcome {
            message_id: "server-1".to_string(),
            conversation_seq: 42,
            durability: SendAckDurability::BrokerAccepted,
        });

        assert!(response.success);
        assert_eq!(response.server_msg_id, "server-1");
        assert_eq!(response.conversation_seq, 42);
        assert_eq!(response.durability(), SendAckDurability::BrokerAccepted);
    }
}
