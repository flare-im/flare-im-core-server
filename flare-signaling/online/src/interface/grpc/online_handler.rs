//! OnlineService gRPC 适配（`interface::grpc`）
//!
//! 仅将请求映射为命令/查询并编排 `application::handlers`，不直连领域或基础设施实现。

use std::sync::Arc;

use flare_proto::signaling::online::*;
use prost_types::Timestamp;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::error;

use flare_server_core::middleware::context::extract_context;

use crate::application::commands::{HeartbeatCommand, LoginCommand, LogoutCommand};
use crate::application::queries::GetOnlineStatusQuery;

use crate::application::handlers::{
    OnlineCommandHandler, OnlinePresenceWatcherHandler, OnlineQueryHandler, OnlineUserHandler,
};
use crate::domain::repository::{
    ConversationRepository, PresencePublisher, PresenceWatcher, SubscriptionRepository,
};

pub struct OnlineHandler<CR, SR, PP, PW>
where
    CR: ConversationRepository + Send + Sync,
    SR: SubscriptionRepository + Send + Sync,
    PP: PresencePublisher + Send + Sync,
    PW: PresenceWatcher + Send + Sync,
{
    command_handler: Arc<OnlineCommandHandler<CR, SR, PP>>,
    query_handler: Arc<OnlineQueryHandler<CR>>,
    user_handler: Arc<OnlineUserHandler<CR>>,
    presence_watcher_handler: Arc<OnlinePresenceWatcherHandler<PW>>,
}

impl<CR, SR, PP, PW> Clone for OnlineHandler<CR, SR, PP, PW>
where
    CR: ConversationRepository + Send + Sync,
    SR: SubscriptionRepository + Send + Sync,
    PP: PresencePublisher + Send + Sync,
    PW: PresenceWatcher + Send + Sync,
{
    fn clone(&self) -> Self {
        Self {
            command_handler: self.command_handler.clone(),
            query_handler: self.query_handler.clone(),
            user_handler: self.user_handler.clone(),
            presence_watcher_handler: self.presence_watcher_handler.clone(),
        }
    }
}

impl<CR, SR, PP, PW> OnlineHandler<CR, SR, PP, PW>
where
    CR: ConversationRepository + Send + Sync,
    SR: SubscriptionRepository + Send + Sync,
    PP: PresencePublisher + Send + Sync,
    PW: PresenceWatcher + Send + Sync,
{
    pub fn new(
        command_handler: Arc<OnlineCommandHandler<CR, SR, PP>>,
        query_handler: Arc<OnlineQueryHandler<CR>>,
        user_handler: Arc<OnlineUserHandler<CR>>,
        presence_watcher_handler: Arc<OnlinePresenceWatcherHandler<PW>>,
    ) -> Self {
        Self {
            command_handler,
            query_handler,
            user_handler,
            presence_watcher_handler,
        }
    }

    pub async fn handle_login(
        &self,
        request: Request<LoginRequest>,
    ) -> std::result::Result<Response<LoginResponse>, Status> {
        let ctx = extract_context(&request)?;
        let command = LoginCommand {
            request: request.into_inner(),
            ctx: ctx.clone(),
        };
        match self.command_handler.handle_login(&ctx, command).await {
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
        let ctx = extract_context(&request)?;
        let command = LogoutCommand {
            request: request.into_inner(),
            ctx: ctx.clone(),
        };
        match self.command_handler.handle_logout(&ctx, command).await {
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
        let ctx = extract_context(&request)?;
        let command = HeartbeatCommand {
            request: request.into_inner(),
            ctx: ctx.clone(),
        };
        match self.command_handler.handle_heartbeat(&ctx, command).await {
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

        let mut receiver = match self
            .presence_watcher_handler
            .watch_presence(&user_ids)
            .await
        {
            Ok(rx) => rx,
            Err(err) => {
                error!(?err, "failed to watch presence");
                return Err(Status::internal(err.to_string()));
            }
        };

        let (stream_tx, stream_rx) = mpsc::channel(100);

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

    pub async fn handle_get_user_presence(
        &self,
        request: Request<GetUserPresenceRequest>,
    ) -> std::result::Result<Response<GetUserPresenceResponse>, Status> {
        match self
            .user_handler
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
            .user_handler
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

        let mut receiver = match self
            .presence_watcher_handler
            .watch_presence(&user_ids)
            .await
        {
            Ok(rx) => rx,
            Err(err) => {
                error!(?err, "failed to watch presence");
                return Err(Status::internal(err.to_string()));
            }
        };

        let (stream_tx, stream_rx) = mpsc::channel(100);

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
        let ctx = extract_context(&request)
            .map_err(|e| Status::invalid_argument(format!("Context is required: {}", e)))?;
        match self
            .user_handler
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
        match self.user_handler.kick_device(request.into_inner()).await {
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
        let ctx = extract_context(&request)
            .map_err(|e| Status::invalid_argument(format!("Context is required: {}", e)))?;
        match self
            .user_handler
            .get_device(&ctx, request.into_inner())
            .await
        {
            Ok(response) => Ok(Response::new(response)),
            Err(err) => {
                error!(?err, "get_device failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }
}
