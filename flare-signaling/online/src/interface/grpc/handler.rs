//! OnlineService gRPC Handler
//!
//! 统一的在线服务处理器，包含会话管理和用户在线状态管理功能

use std::sync::Arc;

use flare_proto::signaling::online::online_service_server::OnlineService;
use flare_proto::signaling::online::*;
use prost_types::Timestamp;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::error;

use crate::application::commands::{HeartbeatCommand, LoginCommand, LogoutCommand};
use crate::application::handlers::{OnlineCommandHandler, OnlineQueryHandler};
use crate::application::queries::GetOnlineStatusQuery;
use crate::domain::repository::PresenceWatcher;
use crate::domain::service::UserDomainService;

#[derive(Clone)]
pub struct OnlineHandler {
    command_handler: Arc<OnlineCommandHandler>,
    query_handler: Arc<OnlineQueryHandler>,
    user_domain_service: Arc<UserDomainService>,
    presence_watcher: Arc<dyn PresenceWatcher>,
}

impl OnlineHandler {
    pub fn new(
        command_handler: Arc<OnlineCommandHandler>,
        query_handler: Arc<OnlineQueryHandler>,
        user_domain_service: Arc<UserDomainService>,
        presence_watcher: Arc<dyn PresenceWatcher>,
    ) -> Self {
        Self {
            command_handler,
            query_handler,
            user_domain_service,
            presence_watcher,
        }
    }

    // ========== 会话管理方法 ==========

    pub async fn handle_login(
        &self,
        request: Request<LoginRequest>,
    ) -> std::result::Result<Response<LoginResponse>, Status> {
        let command = LoginCommand {
            request: request.into_inner(),
        };
        match self.command_handler.handle_login(command).await {
            Ok(response) => Ok(Response::new(response)),
            Err(err) => {
                error!(?err, "login failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    pub async fn handle_logout(
        &self,
        request: Request<LogoutRequest>,
    ) -> std::result::Result<Response<LogoutResponse>, Status> {
        let command = LogoutCommand {
            request: request.into_inner(),
        };
        match self.command_handler.handle_logout(command).await {
            Ok(response) => Ok(Response::new(response)),
            Err(err) => {
                error!(?err, "logout failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    pub async fn handle_heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> std::result::Result<Response<HeartbeatResponse>, Status> {
        let command = HeartbeatCommand {
            request: request.into_inner(),
        };
        match self.command_handler.handle_heartbeat(command).await {
            Ok(response) => Ok(Response::new(response)),
            Err(err) => {
                error!(?err, "heartbeat failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    pub async fn handle_get_online_status(
        &self,
        request: Request<GetOnlineStatusRequest>,
    ) -> std::result::Result<Response<GetOnlineStatusResponse>, Status> {
        let query = GetOnlineStatusQuery {
            request: request.into_inner(),
        };
        match self.query_handler.get_online_status(query).await {
            Ok(response) => Ok(Response::new(response)),
            Err(err) => {
                error!(?err, "get_online_status failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    pub async fn handle_watch_presence(
        &self,
        request: Request<WatchPresenceRequest>,
    ) -> std::result::Result<
        Response<ReceiverStream<std::result::Result<PresenceEvent, Status>>>,
        Status,
    > {
        let req = request.into_inner();
        let user_ids = req.user_ids;

        if user_ids.is_empty() {
            return Err(Status::invalid_argument("user_ids is empty"));
        }

        // 创建事件接收器
        let mut receiver = match self.presence_watcher.watch_presence(&user_ids).await {
            Ok(rx) => rx,
            Err(err) => {
                error!(?err, "failed to watch presence");
                return Err(Status::internal(err.to_string()));
            }
        };

        // 创建发送器用于流式响应
        let (stream_tx, stream_rx) = mpsc::channel(100);

        // 启动后台任务转发事件
        tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Some(Ok(event)) => {
                        let presence_event = PresenceEvent {
                            user_id: event.user_id,
                            status: Some(OnlineStatus {
                                online: event.status.online,
                                server_id: event.status.server_id,
                                cluster_id: event.status.cluster_id.unwrap_or_default(),
                                last_seen: event.status.last_seen.as_ref().map(|dt| Timestamp {
                                    seconds: dt.timestamp(),
                                    nanos: dt.timestamp_subsec_nanos() as i32,
                                }),
                                device_id: event.status.device_id.unwrap_or_default(),
                                device_platform: event.status.device_platform.unwrap_or_default(),
                                gateway_id: event.status.gateway_id.unwrap_or_default(),
                            }),
                            occurred_at: Some(Timestamp {
                                seconds: event.occurred_at.timestamp(),
                                nanos: event.occurred_at.timestamp_subsec_nanos() as i32,
                            }),
                            conflict_action: event.conflict_action.unwrap_or(0),
                            reason: event.reason.unwrap_or_default(),
                        };

                        if stream_tx.send(Ok(presence_event)).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(_)) | None => break,
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(stream_rx)))
    }

    // ========== 用户在线状态方法 ==========

    pub async fn handle_get_user_presence(
        &self,
        request: Request<GetUserPresenceRequest>,
    ) -> std::result::Result<Response<GetUserPresenceResponse>, Status> {
        match self
            .user_domain_service
            .get_user_presence(request.into_inner())
            .await
        {
            Ok(response) => Ok(Response::new(response)),
            Err(err) => {
                error!(?err, "get_user_presence failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    pub async fn handle_batch_get_user_presence(
        &self,
        request: Request<BatchGetUserPresenceRequest>,
    ) -> std::result::Result<Response<BatchGetUserPresenceResponse>, Status> {
        match self
            .user_domain_service
            .batch_get_user_presence(request.into_inner())
            .await
        {
            Ok(response) => Ok(Response::new(response)),
            Err(err) => {
                error!(?err, "batch_get_user_presence failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    pub async fn handle_subscribe_user_presence(
        &self,
        request: Request<SubscribeUserPresenceRequest>,
    ) -> std::result::Result<
        Response<ReceiverStream<std::result::Result<UserPresenceEvent, Status>>>,
        Status,
    > {
        let req = request.into_inner();
        let user_ids = req.user_ids;

        if user_ids.is_empty() {
            return Err(Status::invalid_argument("user_ids is empty"));
        }

        // 创建事件接收器
        let mut receiver = match self.presence_watcher.watch_presence(&user_ids).await {
            Ok(rx) => rx,
            Err(err) => {
                error!(?err, "failed to watch presence");
                return Err(Status::internal(err.to_string()));
            }
        };

        // 创建发送器用于流式响应
        let (stream_tx, stream_rx) = mpsc::channel(100);

        // 启动后台任务转发事件
        tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Some(Ok(event)) => {
                        let presence_event = UserPresenceEvent {
                            user_id: event.user_id.clone(),
                            is_online: event.status.online,
                            device_id: event.status.device_id.unwrap_or_default(),
                            timestamp: Some(Timestamp {
                                seconds: event.occurred_at.timestamp(),
                                nanos: event.occurred_at.timestamp_subsec_nanos() as i32,
                            }),
                        };

                        if stream_tx.send(Ok(presence_event)).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(_)) | None => break,
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(stream_rx)))
    }

    pub async fn handle_list_user_devices(
        &self,
        request: Request<ListUserDevicesRequest>,
    ) -> std::result::Result<Response<ListUserDevicesResponse>, Status> {
        let ctx = flare_im_core::utils::context::require_context(&request)?;
        match self
            .user_domain_service
            .list_user_devices(&ctx, request.into_inner())
            .await
        {
            Ok(response) => Ok(Response::new(response)),
            Err(err) => {
                error!(?err, "list_user_devices failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    pub async fn handle_kick_device(
        &self,
        request: Request<KickDeviceRequest>,
    ) -> std::result::Result<Response<KickDeviceResponse>, Status> {
        match self.user_domain_service.kick_device(request.into_inner()).await {
            Ok(response) => Ok(Response::new(response)),
            Err(err) => {
                error!(?err, "kick_device failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    pub async fn handle_get_device(
        &self,
        request: Request<GetDeviceRequest>,
    ) -> std::result::Result<Response<GetDeviceResponse>, Status> {
        let ctx = flare_im_core::utils::context::require_context(&request)?;
        match self.user_domain_service.get_device(&ctx, request.into_inner()).await {
            Ok(response) => Ok(Response::new(response)),
            Err(err) => {
                error!(?err, "get_device failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }
}

#[tonic::async_trait]
impl OnlineService for OnlineHandler {
    // ========== 会话管理 RPC ==========

    async fn login(
        &self,
        request: Request<LoginRequest>,
    ) -> std::result::Result<Response<LoginResponse>, Status> {
        self.handle_login(request).await
    }

    async fn logout(
        &self,
        request: Request<LogoutRequest>,
    ) -> std::result::Result<Response<LogoutResponse>, Status> {
        self.handle_logout(request).await
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> std::result::Result<Response<HeartbeatResponse>, Status> {
        self.handle_heartbeat(request).await
    }

    async fn get_online_status(
        &self,
        request: Request<GetOnlineStatusRequest>,
    ) -> std::result::Result<Response<GetOnlineStatusResponse>, Status> {
        self.handle_get_online_status(request).await
    }

    type WatchPresenceStream = ReceiverStream<std::result::Result<PresenceEvent, Status>>;

    async fn watch_presence(
        &self,
        request: Request<WatchPresenceRequest>,
    ) -> std::result::Result<Response<Self::WatchPresenceStream>, Status> {
        self.handle_watch_presence(request).await
    }

    // ========== 用户在线状态 RPC ==========

    async fn get_user_presence(
        &self,
        request: Request<GetUserPresenceRequest>,
    ) -> std::result::Result<Response<GetUserPresenceResponse>, Status> {
        self.handle_get_user_presence(request).await
    }

    async fn batch_get_user_presence(
        &self,
        request: Request<BatchGetUserPresenceRequest>,
    ) -> std::result::Result<Response<BatchGetUserPresenceResponse>, Status> {
        self.handle_batch_get_user_presence(request).await
    }

    type SubscribeUserPresenceStream =
        ReceiverStream<std::result::Result<UserPresenceEvent, Status>>;

    async fn subscribe_user_presence(
        &self,
        request: Request<SubscribeUserPresenceRequest>,
    ) -> std::result::Result<Response<Self::SubscribeUserPresenceStream>, Status> {
        self.handle_subscribe_user_presence(request).await
    }

    async fn list_user_devices(
        &self,
        request: Request<ListUserDevicesRequest>,
    ) -> std::result::Result<Response<ListUserDevicesResponse>, Status> {
        self.handle_list_user_devices(request).await
    }

    async fn kick_device(
        &self,
        request: Request<KickDeviceRequest>,
    ) -> std::result::Result<Response<KickDeviceResponse>, Status> {
        self.handle_kick_device(request).await
    }

    async fn get_device(
        &self,
        request: Request<GetDeviceRequest>,
    ) -> std::result::Result<Response<GetDeviceResponse>, Status> {
        self.handle_get_device(request).await
    }
}
