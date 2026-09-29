//! Access Gateway gRPC 服务处理器
//!
//! 结构：仅持有 PushHandler，所有推送与连接查询 RPC 委托应用层 PushHandler；
//! PushHandler 再委托领域层 PushDomainService，业务下沉到领域层。

use std::sync::Arc;

use crate::application::UserConnectionsQuery;
use crate::application::commands::{
    PushAckCommand, PushCustomDataCommand, PushEventCommand, PushMessageCommand,
    PushNotificationCommand,
};
use crate::application::handlers::{ConnectionQueryHandler, PushHandler};
use flare_grpc_proto::access_gateway::access_gateway_server::AccessGateway;
use flare_grpc_proto::access_gateway::{
    ConnectionInfo as ProtoConnectionInfo, DeliverToConversationRequest, GetUserConnectionsRequest,
    GetUserConnectionsResponse, PushAckRequest, PushAckResponse, PushCustomRequest,
    PushEventRequest, PushMessageRequest, PushNotificationRequest, PushNotificationResponse,
    PushResponse, PushResult, RelayRealtimeControlRequest,
};
use flare_im_contracts::Ctx;
use flare_proto::common::DataPacket;
use flare_proto::common::data_packet::Payload as DataPayload;
use flare_proto::common::realtime_control_packet::Payload as RealtimePayload;
use flare_server_core::auth::{SignedActorType, SignedContextClaims, SignedContextVerifier};
use flare_server_core::grpc::middleware::signed_context::{
    VerifiedContext, verify_signed_context_metadata,
};
use prost::Message as ProstMessage;
use prost_types::Timestamp;
use tonic::{Request, Response, Status};
use tracing::debug;

/// AccessGateway gRPC 处理器
///
/// 仅做协议层：接收 gRPC 请求 → 转交 PushHandler → 返回 gRPC 响应。
/// 业务逻辑在 PushHandler → PushDomainService 中完成。
#[derive(Clone)]
pub struct AccessGatewayHandler {
    push_handler: Arc<PushHandler>,
    connection_query_handler: Arc<ConnectionQueryHandler>,
    /// 轻量信令直转：与上行路径用的是同一个实例，所以两条入口的语义逐字相同。
    realtime_relay: Arc<crate::domain::service::RealtimeControlRelay>,
    identity_verifier: Option<SignedContextVerifier>,
}

impl AccessGatewayHandler {
    pub fn new(
        push_handler: Arc<PushHandler>,
        connection_query_handler: Arc<ConnectionQueryHandler>,
        realtime_relay: Arc<crate::domain::service::RealtimeControlRelay>,
    ) -> Self {
        Self {
            push_handler,
            connection_query_handler,
            realtime_relay,
            identity_verifier: None,
        }
    }

    pub fn with_identity_verifier(mut self, verifier: Option<SignedContextVerifier>) -> Self {
        self.identity_verifier = verifier;
        self
    }

    /// Push RPCs originate from a broker consumer, never an end-user token or
    /// an arbitrary tenant header. The worker may forward a verified event
    /// context, but cannot manufacture one from an unsigned MQ envelope.
    fn require_push_producer<T>(&self, request: &Request<T>) -> Result<Ctx, Status> {
        let verifier = self
            .identity_verifier
            .as_ref()
            .ok_or_else(|| Status::unavailable("gateway push identity verifier not configured"))?;
        let ctx = verify_signed_context_metadata(request.metadata(), verifier)?;
        let claims = ctx
            .get_data::<VerifiedContext>()
            .and_then(|marker| marker.claims())
            .ok_or_else(|| Status::unauthenticated("signed push identity required"))?;
        validate_push_producer_claims(claims)?;
        Ok(ctx)
    }
}

fn validate_push_producer_claims(claims: &SignedContextClaims) -> Result<(), Status> {
    if claims.actor_type != SignedActorType::Service
        || claims.actor_id != "flare-push-worker"
        || !flare_im_contracts::domain::tenant::is_valid_tenant_id(&claims.tenant_id)
        || flare_im_contracts::utils::normalize_tenant_id(&claims.tenant_id) != claims.tenant_id
    {
        return Err(Status::permission_denied(
            "gateway push producer not permitted",
        ));
    }
    Ok(())
}

fn validate_delivery_body(body: &DeliverToConversationRequest) -> Result<(), Status> {
    use flare_grpc_proto::access_gateway::deliver_to_conversation_request::Payload;
    if body.conversation_id.is_empty() || body.conversation_id.trim() != body.conversation_id {
        return Err(Status::invalid_argument(
            "canonical conversation id required",
        ));
    }
    match body.payload.as_ref() {
        Some(Payload::Messages(delivery)) if !delivery.messages.is_empty() => {
            if delivery
                .messages
                .iter()
                .any(|message| message.conversation_id != body.conversation_id)
            {
                return Err(Status::permission_denied("message conversation mismatch"));
            }
        }
        Some(Payload::Events(delivery)) if !delivery.events.is_empty() => {
            if delivery
                .events
                .iter()
                .any(|event| event.conversation_id != body.conversation_id)
            {
                return Err(Status::permission_denied("event conversation mismatch"));
            }
        }
        Some(Payload::Ping(ping)) if ping.max_conversation_seq > 0 => {}
        _ => {
            return Err(Status::invalid_argument(
                "invalid conversation delivery payload",
            ));
        }
    }
    Ok(())
}

fn validate_realtime_relay_body(
    claims: &SignedContextClaims,
    body: &RelayRealtimeControlRequest,
) -> Result<(), Status> {
    if claims.actor_type != SignedActorType::User
        || claims.credential_id.as_deref() != Some("signaling-gateway")
        || claims.actor_id.is_empty()
        || !flare_im_contracts::domain::tenant::is_valid_tenant_id(&claims.tenant_id)
        || flare_im_contracts::utils::normalize_tenant_id(&claims.tenant_id) != claims.tenant_id
    {
        return Err(Status::permission_denied(
            "gateway relay identity not permitted",
        ));
    }
    if body.conversation_id.is_empty() || body.conversation_id.trim() != body.conversation_id {
        return Err(Status::invalid_argument(
            "canonical conversation id required",
        ));
    }
    if claims.request_id
        != crate::domain::service::realtime_relay::relay_body_request_id(
            &body.conversation_id,
            &body.packet,
        )
    {
        return Err(Status::permission_denied(
            "realtime relay token/body mismatch",
        ));
    }
    let packet = DataPacket::decode(body.packet.as_slice())
        .map_err(|_| Status::invalid_argument("realtime relay packet invalid"))?;
    let Some(DataPayload::RealtimeControl(rc)) = packet.payload else {
        return Err(Status::invalid_argument("realtime control packet required"));
    };
    if rc.conversation_id.as_deref() != Some(body.conversation_id.as_str()) {
        return Err(Status::permission_denied(
            "realtime relay conversation mismatch",
        ));
    }
    if let Some(RealtimePayload::TypingAggregate(aggregate)) = rc.payload.as_ref()
        && aggregate.conversation_id != body.conversation_id
    {
        return Err(Status::permission_denied(
            "typing aggregate conversation mismatch",
        ));
    }
    Ok(())
}

#[tonic::async_trait]
impl AccessGateway for AccessGatewayHandler {
    async fn push_message(
        &self,
        request: Request<PushMessageRequest>,
    ) -> Result<Response<PushResponse>, Status> {
        let ctx = self.require_push_producer(&request)?;
        if request
            .get_ref()
            .messages
            .iter()
            .any(|message| message.conversation_id.trim().is_empty())
        {
            return Err(Status::invalid_argument("message conversation id required"));
        }
        let req = request.into_inner();
        debug!("PushMessage request: {} users", req.user_ids.len());
        let response = self
            .push_handler
            .handle_push_message(
                &ctx,
                PushMessageCommand::new(req.user_ids, req.messages, req.options),
            )
            .await
            .map_err(|e| {
                tracing::error!(?e, "Failed to push message");
                Status::internal(e.to_string())
            })?;
        Ok(Response::new(response))
    }

    async fn deliver_to_conversation(
        &self,
        request: Request<DeliverToConversationRequest>,
    ) -> Result<Response<PushResponse>, Status> {
        let ctx = self.require_push_producer(&request)?;
        validate_delivery_body(request.get_ref())?;
        let req = request.into_inner();
        {
            use flare_grpc_proto::access_gateway::deliver_to_conversation_request::Payload;
            let payload_kind = match &req.payload {
                Some(Payload::Messages(delivery)) => {
                    format!("messages({})", delivery.messages.len())
                }
                Some(Payload::Events(delivery)) => format!("events({})", delivery.events.len()),
                Some(Payload::Ping(ping)) => format!("ping(max_seq={})", ping.max_conversation_seq),
                None => "none".to_string(),
            };
            debug!(
                "DeliverToConversation request: conv={} payload={}",
                req.conversation_id, payload_kind
            );
        }
        let response = self
            .push_handler
            .handle_deliver_to_conversation(&ctx, req)
            .await
            .map_err(|e| {
                tracing::error!(?e, "Failed to deliver to conversation");
                Status::internal(e.to_string())
            })?;
        Ok(Response::new(response))
    }

    /// 另一个网关节点转来的轻量信令：只发给**本节点**订阅该会话的在线连接。
    ///
    /// 不再向外广播——广播由收到上行帧的那个节点做一次，这里再广播就会无限转圈。
    /// 连接 id 是节点本地的，所以对端节点没有「发送方」可排除，全发。
    async fn relay_realtime_control(
        &self,
        request: Request<RelayRealtimeControlRequest>,
    ) -> Result<Response<PushResponse>, Status> {
        let verifier = self
            .identity_verifier
            .as_ref()
            .ok_or_else(|| Status::unavailable("gateway relay identity verifier not configured"))?;
        let ctx = verify_signed_context_metadata(request.metadata(), verifier)?;
        let claims = ctx
            .get_data::<VerifiedContext>()
            .and_then(|marker| marker.claims())
            .ok_or_else(|| Status::unauthenticated("signed gateway relay identity required"))?;
        validate_realtime_relay_body(claims, request.get_ref())?;
        let req = request.into_inner();
        let delivered = self
            .realtime_relay
            .relay_locally(&ctx, &req.conversation_id, req.packet, None)
            .await;
        // 投递数是给排查用的：0 表示「本节点没有该会话的在线订阅者」，不是失败。
        Ok(Response::new(PushResponse {
            result: Some(PushResult {
                pushed_device_count: delivered as i32,
                offline_pending_count: 0,
                window_id: String::new(),
                at: Some(Timestamp::from(std::time::SystemTime::now())),
            }),
            user_results: Vec::new(),
        }))
    }

    async fn push_event(
        &self,
        request: Request<PushEventRequest>,
    ) -> Result<Response<PushResponse>, Status> {
        let ctx = self.require_push_producer(&request)?;
        if request
            .get_ref()
            .events
            .iter()
            .any(|event| event.conversation_id.trim().is_empty())
        {
            return Err(Status::invalid_argument("event conversation id required"));
        }
        if !request.get_ref().conversation_id.is_empty()
            && request
                .get_ref()
                .events
                .iter()
                .any(|event| event.conversation_id != request.get_ref().conversation_id)
        {
            return Err(Status::permission_denied("event conversation mismatch"));
        }
        let req = request.into_inner();
        debug!("PushEvent request: {} users", req.user_ids.len());
        let response = self
            .push_handler
            .handle_push_event(
                &ctx,
                PushEventCommand::new(
                    req.user_ids,
                    req.events,
                    req.options,
                    req.conversation_id,
                    req.max_conversation_seq,
                    req.delivery_mode,
                    req.inline_events_truncated,
                ),
            )
            .await
            .map_err(|e| {
                tracing::error!(?e, "Failed to push event");
                Status::internal(e.to_string())
            })?;
        Ok(Response::new(response))
    }

    async fn push_notification(
        &self,
        request: Request<PushNotificationRequest>,
    ) -> Result<Response<PushNotificationResponse>, Status> {
        let ctx = self.require_push_producer(&request)?;
        let req = request.into_inner();
        debug!("PushNotification request: {} users", req.user_ids.len());
        let notification = req
            .notification
            .ok_or_else(|| Status::invalid_argument("missing notification"))?;
        let response = self
            .push_handler
            .handle_push_notification(
                &ctx,
                PushNotificationCommand::new(req.user_ids, notification, req.options),
            )
            .await
            .map_err(|e| {
                tracing::error!(?e, "Failed to push notification");
                Status::internal(e.to_string())
            })?;
        Ok(Response::new(response))
    }

    async fn push_ack(
        &self,
        request: Request<PushAckRequest>,
    ) -> Result<Response<PushAckResponse>, Status> {
        let ctx = self.require_push_producer(&request)?;
        let req = request.into_inner();
        debug!("PushAck request: {} users", req.user_ids.len());
        let ack = req
            .ack
            .ok_or_else(|| Status::invalid_argument("missing ack"))?;
        let response = self
            .push_handler
            .handle_push_ack(&ctx, PushAckCommand::new(req.user_ids, ack, req.options))
            .await
            .map_err(|e| {
                tracing::error!(?e, "Failed to push ack");
                Status::internal(e.to_string())
            })?;
        Ok(Response::new(response))
    }

    async fn push_custom(
        &self,
        request: Request<PushCustomRequest>,
    ) -> Result<Response<PushResponse>, Status> {
        let ctx = self.require_push_producer(&request)?;
        let req = request.into_inner();
        debug!("PushCustom request: {} users", req.user_ids.len());
        let custom_data = req
            .custom_data
            .ok_or_else(|| Status::invalid_argument("missing custom_data"))?;
        let response = self
            .push_handler
            .handle_push_custom(
                &ctx,
                PushCustomDataCommand::new(req.user_ids, custom_data, req.options),
            )
            .await
            .map_err(|e| {
                tracing::error!(?e, "Failed to push custom");
                Status::internal(e.to_string())
            })?;
        Ok(Response::new(response))
    }

    async fn get_user_connections(
        &self,
        request: Request<GetUserConnectionsRequest>,
    ) -> Result<Response<GetUserConnectionsResponse>, Status> {
        let verifier = self.identity_verifier.as_ref().ok_or_else(|| {
            Status::unavailable("gateway online identity verifier not configured")
        })?;
        let ctx = verify_signed_context_metadata(request.metadata(), verifier)?;
        let claims = ctx
            .get_data::<VerifiedContext>()
            .and_then(|marker| marker.claims())
            .ok_or_else(|| Status::unauthenticated("signed gateway identity required"))?;
        if !flare_im_contracts::domain::tenant::is_valid_tenant_id(&claims.tenant_id)
            || claims.tenant_id != flare_im_contracts::utils::normalize_tenant_id(&claims.tenant_id)
            || (claims.actor_type == SignedActorType::User
                && claims.actor_id != request.get_ref().user_id)
            || (claims.actor_type != SignedActorType::User
                && !(claims.actor_type == SignedActorType::Service
                    && claims.actor_id == "flare-push-worker"))
        {
            return Err(Status::permission_denied(
                "gateway user connection query not permitted",
            ));
        }
        let tenant_id = claims.tenant_id.clone();
        let req = request.into_inner();
        debug!("GetUserConnections request: user_id={}", req.user_id);
        let rows = self
            .connection_query_handler
            .query_user_connections(
                &tenant_id,
                UserConnectionsQuery::new(req.user_id, req.platforms, req.limit),
            )
            .await
            .map_err(|e: flare_server_core::error::FlareError| {
                tracing::error!(?e, "Failed to get user connections");
                Status::internal(e.to_string())
            })?;

        let connections: Vec<ProtoConnectionInfo> = rows
            .into_iter()
            .map(|c| ProtoConnectionInfo {
                device_id: c.device_id,
                platform: c.platform.unwrap_or_default(),
                connected_at: c.connected_at.map(|dt| Timestamp {
                    seconds: dt.timestamp(),
                    nanos: dt.timestamp_subsec_nanos() as i32,
                }),
                app_version: None,
                quality: None,
            })
            .collect();
        let total_online = connections.len() as i32;

        Ok(Response::new(GetUserConnectionsResponse {
            connections,
            total_online,
        }))
    }
}

#[cfg(test)]
mod realtime_relay_tests {
    use super::*;
    use flare_proto::common::RealtimeControlPacket;

    fn claims() -> SignedContextClaims {
        SignedContextClaims {
            sub: "alice".into(),
            iss: "flare-cell".into(),
            aud: "flare-cell-internal".into(),
            token_use: "internal_context".into(),
            iat: 1,
            exp: 2,
            jti: "test".into(),
            actor_type: SignedActorType::User,
            actor_id: "alice".into(),
            tenant_id: "t1".into(),
            roles: Vec::new(),
            credential_id: Some("signaling-gateway".into()),
            request_id: crate::domain::service::realtime_relay::relay_body_request_id(
                "same",
                &body("same").packet,
            ),
        }
    }

    fn body(conversation_id: &str) -> RelayRealtimeControlRequest {
        RelayRealtimeControlRequest {
            conversation_id: conversation_id.into(),
            packet: DataPacket {
                payload: Some(DataPayload::RealtimeControl(RealtimeControlPacket {
                    control_type: "presence".into(),
                    conversation_id: Some(conversation_id.into()),
                    correlation_id: None,
                    attributes: Default::default(),
                    payload: None,
                })),
            }
            .encode_to_vec(),
        }
    }

    #[test]
    fn relay_rejects_non_gateway_or_non_user_claims() {
        let mut identity = claims();
        assert!(validate_realtime_relay_body(&identity, &body("same")).is_ok());
        identity.actor_type = SignedActorType::Service;
        assert_eq!(
            validate_realtime_relay_body(&identity, &body("same"))
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        identity.actor_type = SignedActorType::User;
        identity.credential_id = None;
        assert_eq!(
            validate_realtime_relay_body(&identity, &body("same"))
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        identity.credential_id = Some("signaling-gateway".into());
        identity.tenant_id = "default".into();
        assert_eq!(
            validate_realtime_relay_body(&identity, &body("same"))
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn relay_rejects_body_conversation_spoofing() {
        let mut forged = body("other");
        forged.conversation_id = "same".into();
        assert_eq!(
            validate_realtime_relay_body(&claims(), &forged)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            validate_realtime_relay_body(&claims(), &body(""))
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        let mut tampered = body("same");
        tampered.packet.push(0);
        assert_eq!(
            validate_realtime_relay_body(&claims(), &tampered)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn push_producer_claims_reject_user_wrong_service_and_noncanonical_tenant() {
        let mut identity = claims();
        assert_eq!(
            validate_push_producer_claims(&identity).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        identity.actor_type = SignedActorType::Service;
        identity.actor_id = "flare-push-worker".into();
        identity.tenant_id = "t1".into();
        assert!(validate_push_producer_claims(&identity).is_ok());
        identity.actor_id = "untrusted-worker".into();
        assert_eq!(
            validate_push_producer_claims(&identity).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        identity.actor_id = "flare-push-worker".into();
        identity.tenant_id = "default".into();
        assert_eq!(
            validate_push_producer_claims(&identity).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn conversation_delivery_rejects_body_target_mismatch() {
        use flare_grpc_proto::access_gateway::{
            ConversationMessagesDelivery, deliver_to_conversation_request::Payload,
        };
        let message = flare_proto::common::Message {
            conversation_id: "other".into(),
            ..Default::default()
        };
        let request = DeliverToConversationRequest {
            conversation_id: "same".into(),
            options: None,
            payload: Some(Payload::Messages(ConversationMessagesDelivery {
                messages: vec![message],
            })),
        };
        assert_eq!(
            validate_delivery_body(&request).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
    }
}
