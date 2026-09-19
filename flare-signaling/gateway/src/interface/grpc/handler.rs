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
use flare_im_contracts::require_context;
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
        }
    }
}

#[tonic::async_trait]
impl AccessGateway for AccessGatewayHandler {
    async fn push_message(
        &self,
        request: Request<PushMessageRequest>,
    ) -> Result<Response<PushResponse>, Status> {
        let ctx = require_context(&request)?;
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
        let ctx = require_context(&request)?;
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
        let ctx = require_context(&request)?;
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
        let ctx = require_context(&request)?;
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
        let ctx = require_context(&request)?;
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
        let ctx = require_context(&request)?;
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
        let ctx = require_context(&request)?;
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
        let req = request.into_inner();
        debug!("GetUserConnections request: user_id={}", req.user_id);
        let rows = self
            .connection_query_handler
            .query_user_connections(UserConnectionsQuery::new(
                req.user_id,
                req.platforms,
                req.limit,
            ))
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
