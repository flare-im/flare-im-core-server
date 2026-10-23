//! RouterUpstreamService gRPC Handler
//!
//! 处理上行请求（Message/Event/Ack/Data）

use std::sync::Arc;
use tonic::{Request, Response, Status};

use flare_grpc_proto::signaling::router::{
    RouteAckRequest, RouteAckResponse, RouteDataRequest, RouteDataResponse, RouteEventRequest,
    RouteEventResponse, RouteMessageRequest, RouteMessageResponse,
    router_upstream_service_server::RouterUpstreamService,
};
use flare_proto::common::MessageSource;
use flare_server_core::VerifiedContext;
use flare_server_core::error::grpc::IntoGrpc;
use flare_server_core::flare_err;
use flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata;
use flare_server_core::utils::require_ctx_from_request;
use flare_server_core::{SignedActorType, SignedContextVerifier};

use crate::application::handlers::{
    AckRoutingHandler, DataRoutingHandler, EventRoutingHandler, MessageRoutingHandler,
};

/// RouterUpstreamService Handler
pub struct RouterUpstreamHandler {
    message_handler: Arc<MessageRoutingHandler>,
    event_handler: Arc<EventRoutingHandler>,
    ack_handler: Arc<AckRoutingHandler>,
    data_handler: Arc<DataRoutingHandler>,
    message_verifier: Option<SignedContextVerifier>,
}

impl RouterUpstreamHandler {
    /// 创建新的 Handler 实例
    pub fn new(
        message_handler: Arc<MessageRoutingHandler>,
        event_handler: Arc<EventRoutingHandler>,
        ack_handler: Arc<AckRoutingHandler>,
        data_handler: Arc<DataRoutingHandler>,
        message_verifier: Option<SignedContextVerifier>,
    ) -> Self {
        Self {
            message_handler,
            event_handler,
            ack_handler,
            data_handler,
            message_verifier,
        }
    }
}

#[tonic::async_trait]
impl RouterUpstreamService for RouterUpstreamHandler {
    /// 路由消息
    async fn route_message(
        &self,
        request: Request<RouteMessageRequest>,
    ) -> Result<Response<RouteMessageResponse>, Status> {
        let verifier = self
            .message_verifier
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("signed message context unavailable"))?;
        let ctx = verify_signed_context_metadata(request.metadata(), verifier)?;
        let claims = ctx
            .get_data::<VerifiedContext>()
            .and_then(VerifiedContext::claims)
            .ok_or_else(|| Status::unauthenticated("verified message context required"))?;

        // Never use the legacy ContextLayer metadata as message identity.
        let req = request.into_inner();
        let svid = req.svid;
        let message = req.message.ok_or_else(|| {
            flare_err!(
                flare_server_core::error::ErrorCode::InvalidParameter,
                "Message is required"
            )
        })?;
        if claims.actor_type != SignedActorType::User
            || message.sender_id != claims.actor_id
            || message.source != MessageSource::User as i32
        {
            return Err(Status::permission_denied(
                "message sender does not match signed context",
            ));
        }
        let options = req.options.unwrap_or_default();

        let result = self
            .message_handler
            .route_message(&ctx, &svid, message, options)
            .await
            .into_grpc()?;

        // 4. 构建响应
        let response = RouteMessageResponse {
            response_data: result.response_data,
            routed_endpoint: result.routed_endpoint,
            metadata: Some(result.metadata),
        };

        Ok(Response::new(response))
    }

    /// 路由事件
    ///
    /// 网关为已认证连接签了 `x-flare-ctx` 时，操作者只取自签名（必须是 User），验签后的上下文
    /// （连同原令牌）转发给 Orchestrator，由它的消息操作授权按签名身份判断；没有令牌的调用保持
    /// 元数据身份（Orchestrator 只把它当作用户，从不当作受信服务）。
    async fn route_event(
        &self,
        request: Request<RouteEventRequest>,
    ) -> Result<Response<RouteEventResponse>, Status> {
        // 1. 提取上下文
        let ctx = match (
            self.message_verifier.as_ref(),
            request
                .metadata()
                .get(flare_server_core::grpc::middleware::SIGNED_CONTEXT_HEADER),
        ) {
            (Some(verifier), Some(_)) => {
                let ctx = verify_signed_context_metadata(request.metadata(), verifier)?;
                let user = ctx
                    .get_data::<VerifiedContext>()
                    .and_then(VerifiedContext::claims)
                    .is_some_and(|claims| claims.actor_type == SignedActorType::User);
                if !user {
                    return Err(Status::permission_denied(
                        "event context must be a signed user",
                    ));
                }
                ctx
            }
            _ => require_ctx_from_request(&request)?,
        };

        // 2. 解析请求
        let req = request.into_inner();
        let svid = req.svid;
        let event = req.event.ok_or_else(|| {
            flare_err!(
                flare_server_core::error::ErrorCode::InvalidParameter,
                "Event is required"
            )
        })?;
        let options = req.options.unwrap_or_default();

        let result = self
            .event_handler
            .route_event(&ctx, &svid, event, options)
            .await
            .into_grpc()?;

        // 4. 构建响应
        let response = RouteEventResponse {
            response_data: result.response_data,
            routed_endpoint: result.routed_endpoint,
            metadata: Some(result.metadata),
        };

        Ok(Response::new(response))
    }

    /// 路由 ACK
    async fn route_ack(
        &self,
        request: Request<RouteAckRequest>,
    ) -> Result<Response<RouteAckResponse>, Status> {
        // 1. 提取上下文
        let ctx = require_ctx_from_request(&request)?;

        // 2. 解析请求
        let req = request.into_inner();
        let svid = req.svid;
        let ack = req.ack.ok_or_else(|| {
            flare_err!(
                flare_server_core::error::ErrorCode::InvalidParameter,
                "Ack is required"
            )
        })?;
        let options = req.options.unwrap_or_default();

        let result = self
            .ack_handler
            .route_ack(&ctx, &svid, ack, options)
            .await
            .into_grpc()?;

        // 4. 构建响应
        let response = RouteAckResponse {
            routed_endpoint: result.routed_endpoint,
            metadata: Some(result.metadata),
        };

        Ok(Response::new(response))
    }

    /// 路由数据
    async fn route_data(
        &self,
        request: Request<RouteDataRequest>,
    ) -> Result<Response<RouteDataResponse>, Status> {
        // 1. 提取上下文
        let ctx = require_ctx_from_request(&request)?;

        // 2. 解析请求
        let req = request.into_inner();
        let svid = req.svid;
        let data = req.data.ok_or_else(|| {
            flare_err!(
                flare_server_core::error::ErrorCode::InvalidParameter,
                "Data is required"
            )
        })?;
        let options = req.options.unwrap_or_default();

        let result = self
            .data_handler
            .route_data(&ctx, &svid, data, options)
            .await
            .into_grpc()?;

        // 4. 构建响应
        let response = RouteDataResponse {
            response_data: result.response_data,
            routed_endpoint: result.routed_endpoint,
            metadata: Some(result.metadata),
        };

        Ok(Response::new(response))
    }
}
