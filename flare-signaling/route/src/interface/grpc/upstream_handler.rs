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
use flare_server_core::utils::require_ctx_from_request;
use flare_server_core::error::grpc::IntoGrpc;
use flare_server_core::flare_err;

use crate::application::handlers::{
    AckRoutingHandler, DataRoutingHandler, EventRoutingHandler, MessageRoutingHandler,
};

/// RouterUpstreamService Handler
pub struct RouterUpstreamHandler {
    message_handler: Arc<MessageRoutingHandler>,
    event_handler: Arc<EventRoutingHandler>,
    ack_handler: Arc<AckRoutingHandler>,
    data_handler: Arc<DataRoutingHandler>,
}

impl RouterUpstreamHandler {
    /// 创建新的 Handler 实例
    pub fn new(
        message_handler: Arc<MessageRoutingHandler>,
        event_handler: Arc<EventRoutingHandler>,
        ack_handler: Arc<AckRoutingHandler>,
        data_handler: Arc<DataRoutingHandler>,
    ) -> Self {
        Self {
            message_handler,
            event_handler,
            ack_handler,
            data_handler,
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
        // 1. 提取上下文
        let ctx = require_ctx_from_request(&request)?;

        // 2. 解析请求
        let req = request.into_inner();
        let svid = req.svid;
        let message = req.message.ok_or_else(|| {
            flare_err!(flare_server_core::error::ErrorCode::InvalidParameter, "Message is required")
        })?;
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
    async fn route_event(
        &self,
        request: Request<RouteEventRequest>,
    ) -> Result<Response<RouteEventResponse>, Status> {
        // 1. 提取上下文
        let ctx = require_ctx_from_request(&request)?;

        // 2. 解析请求
        let req = request.into_inner();
        let svid = req.svid;
        let event = req.event.ok_or_else(|| {
            flare_err!(flare_server_core::error::ErrorCode::InvalidParameter, "Event is required")
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
            flare_err!(flare_server_core::error::ErrorCode::InvalidParameter, "Ack is required")
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
            flare_err!(flare_server_core::error::ErrorCode::InvalidParameter, "Data is required")
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
