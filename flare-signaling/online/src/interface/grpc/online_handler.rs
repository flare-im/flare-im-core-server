//! OnlineService gRPC 适配（`interface::grpc`）
//!
//! 仅将请求映射为命令/查询并编排 `application::handlers`，不直连领域或基础设施实现。

use std::sync::Arc;

use flare_grpc_proto::signaling::online::*;
use prost_types::Timestamp;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use flare_im_service_kit::control_auth::{
    ConsoleControlVerifier, ControlActorKind, require_grpc_control,
};
use flare_server_core::auth::{SignedActorType, SignedContextClaims, SignedContextVerifier};
use flare_server_core::context::Ctx;
use flare_server_core::error::grpc::IntoGrpc;
use flare_server_core::flare_err;
use flare_server_core::grpc::middleware::signed_context::{
    VerifiedContext, verify_signed_context_metadata,
};

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
    control_verifier: Option<ConsoleControlVerifier>,
    identity_verifier: Option<SignedContextVerifier>,
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
            control_verifier: self.control_verifier.clone(),
            identity_verifier: self.identity_verifier.clone(),
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
            control_verifier: None,
            identity_verifier: None,
        }
    }

    pub fn with_control_verifier(mut self, verifier: Option<ConsoleControlVerifier>) -> Self {
        self.control_verifier = verifier;
        self
    }

    pub fn with_identity_verifier(mut self, verifier: Option<SignedContextVerifier>) -> Self {
        self.identity_verifier = verifier;
        self
    }

    fn verified_identity<T>(
        &self,
        request: &Request<T>,
    ) -> Result<(Ctx, SignedContextClaims), Status> {
        let verifier = self
            .identity_verifier
            .as_ref()
            .ok_or_else(|| Status::unavailable("online identity verifier not configured"))?;
        let ctx = verify_signed_context_metadata(request.metadata(), verifier)?;
        let claims = ctx
            .get_data::<VerifiedContext>()
            .and_then(|marker| marker.claims())
            .cloned()
            .ok_or_else(|| Status::unauthenticated("online identity required"))?;
        if claims.tenant_id != flare_im_contracts::utils::normalize_tenant_id(&claims.tenant_id) {
            return Err(Status::permission_denied("non-canonical online tenant"));
        }
        Ok((ctx, claims))
    }

    fn require_user(claims: &SignedContextClaims, user_id: &str) -> Result<(), Status> {
        if claims.actor_type != SignedActorType::User || claims.actor_id != user_id {
            return Err(Status::permission_denied("online user identity mismatch"));
        }
        Ok(())
    }

    fn require_query_actor(claims: &SignedContextClaims) -> Result<(), Status> {
        if claims.actor_type == SignedActorType::User
            || (claims.actor_type == SignedActorType::Service
                && matches!(
                    claims.actor_id.as_str(),
                    "flare-push-server" | "flare-push-worker"
                ))
        {
            return Ok(());
        }
        Err(Status::permission_denied(
            "online query actor not permitted",
        ))
    }

    pub async fn handle_login(
        &self,
        request: Request<LoginRequest>,
    ) -> Result<Response<LoginResponse>, Status> {
        let (ctx, claims) = self.verified_identity(&request)?;
        Self::require_user(&claims, &request.get_ref().user_id)?;
        if claims.credential_id.as_deref() != Some("signaling-gateway")
            || request
                .get_ref()
                .metadata
                .get("connection_id")
                .map(String::as_str)
                != Some(claims.request_id.as_str())
        {
            return Err(Status::permission_denied(
                "authenticated signaling connection required for Online login",
            ));
        }
        if request
            .get_ref()
            .metadata
            .get("tenant_id")
            .is_some_and(|tenant| tenant != &claims.tenant_id)
            || request
                .get_ref()
                .metadata
                .get("x-tenant-id")
                .is_some_and(|tenant| tenant != &claims.tenant_id)
        {
            return Err(Status::permission_denied("online login tenant mismatch"));
        }
        let command = LoginCommand {
            request: request.into_inner(),
            ctx: ctx.as_ref().clone(),
        };
        let response = self
            .command_handler
            .handle_login(&ctx, command)
            .await
            .into_grpc()?;
        Ok(Response::new(response))
    }

    pub async fn handle_logout(
        &self,
        request: Request<LogoutRequest>,
    ) -> Result<Response<LogoutResponse>, Status> {
        let (ctx, claims) = self.verified_identity(&request)?;
        Self::require_user(&claims, &request.get_ref().user_id)?;
        let command = LogoutCommand {
            request: request.into_inner(),
            ctx: ctx.as_ref().clone(),
        };
        let response = self
            .command_handler
            .handle_logout(&ctx, command)
            .await
            .into_grpc()?;
        Ok(Response::new(response))
    }

    pub async fn handle_heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let (ctx, claims) = self.verified_identity(&request)?;
        Self::require_user(&claims, &request.get_ref().user_id)?;
        let command = HeartbeatCommand {
            request: request.into_inner(),
            ctx: ctx.as_ref().clone(),
        };
        let response = self
            .command_handler
            .handle_heartbeat(&ctx, command)
            .await
            .into_grpc()?;
        Ok(Response::new(response))
    }

    pub async fn handle_get_online_status(
        &self,
        request: Request<GetOnlineStatusRequest>,
    ) -> Result<Response<GetOnlineStatusResponse>, Status> {
        let (_, claims) = self.verified_identity(&request)?;
        Self::require_query_actor(&claims)?;
        let query = GetOnlineStatusQuery {
            request: request.into_inner(),
        };
        let response = self
            .query_handler
            .get_online_status(&claims.tenant_id, query)
            .await
            .into_grpc()?;
        Ok(Response::new(response))
    }

    pub async fn handle_watch_presence(
        &self,
        request: Request<WatchPresenceRequest>,
    ) -> Result<Response<ReceiverStream<Result<PresenceEvent, Status>>>, Status> {
        let (_, claims) = self.verified_identity(&request)?;
        Self::require_query_actor(&claims)?;
        let req = request.into_inner();
        let user_ids = req.user_ids;

        if user_ids.is_empty() {
            return Err(flare_err!(
                flare_server_core::error::ErrorCode::InvalidParameter,
                "user_ids is empty"
            )
            .into());
        }

        let mut receiver = self
            .presence_watcher_handler
            .watch_presence(&claims.tenant_id, &user_ids)
            .await
            .into_grpc()?;

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
                    Some(Err(err)) => {
                        let _ = stream_tx
                            .send(Err(Status::internal(format!(
                                "presence watcher failed: {err}"
                            ))))
                            .await;
                        break;
                    }
                    None => break,
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(stream_rx)))
    }

    pub async fn handle_get_user_presence(
        &self,
        request: Request<GetUserPresenceRequest>,
    ) -> Result<Response<GetUserPresenceResponse>, Status> {
        let (_, claims) = self.verified_identity(&request)?;
        Self::require_query_actor(&claims)?;
        let response = self
            .user_handler
            .get_user_presence(&claims.tenant_id, request.into_inner())
            .await
            .into_grpc()?;
        Ok(Response::new(response))
    }

    pub async fn handle_batch_get_user_presence(
        &self,
        request: Request<BatchGetUserPresenceRequest>,
    ) -> Result<Response<BatchGetUserPresenceResponse>, Status> {
        let (_, claims) = self.verified_identity(&request)?;
        Self::require_query_actor(&claims)?;
        let response = self
            .user_handler
            .batch_get_user_presence(&claims.tenant_id, request.into_inner())
            .await
            .into_grpc()?;
        Ok(Response::new(response))
    }

    pub async fn handle_subscribe_user_presence(
        &self,
        request: Request<SubscribeUserPresenceRequest>,
    ) -> Result<Response<ReceiverStream<Result<UserPresenceEvent, Status>>>, Status> {
        let (_, claims) = self.verified_identity(&request)?;
        Self::require_query_actor(&claims)?;
        let req = request.into_inner();
        let user_ids = req.user_ids;

        if user_ids.is_empty() {
            return Err(flare_err!(
                flare_server_core::error::ErrorCode::InvalidParameter,
                "user_ids is empty"
            )
            .into());
        }

        let mut receiver = self
            .presence_watcher_handler
            .watch_presence(&claims.tenant_id, &user_ids)
            .await
            .into_grpc()?;

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
                    Some(Err(err)) => {
                        let _ = stream_tx
                            .send(Err(Status::internal(format!(
                                "presence watcher failed: {err}"
                            ))))
                            .await;
                        break;
                    }
                    None => break,
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(stream_rx)))
    }

    pub async fn handle_list_user_devices(
        &self,
        request: Request<ListUserDevicesRequest>,
    ) -> Result<Response<ListUserDevicesResponse>, Status> {
        let (_, claims) = self.verified_identity(&request)?;
        if claims.actor_type == SignedActorType::User {
            Self::require_user(&claims, &request.get_ref().user_id)?;
        } else if claims.actor_type != SignedActorType::Service
            || claims.actor_id != "flare-push-worker"
        {
            return Err(Status::permission_denied("device list actor not permitted"));
        }
        let response = self
            .user_handler
            .list_user_devices(&claims.tenant_id, request.into_inner())
            .await
            .into_grpc()?;
        Ok(Response::new(response))
    }

    pub async fn handle_kick_device(
        &self,
        request: Request<KickDeviceRequest>,
    ) -> Result<Response<KickDeviceResponse>, Status> {
        let actor = require_grpc_control(&request, self.control_verifier.as_ref())?;
        actor.require_kind(ControlActorKind::Employee)?;
        actor.require_tenant(&actor.tenant_id)?;
        tracing::info!(actor_id = %actor.subject, tenant_id = %actor.tenant_id, "control kick_device authorized");
        let response = self
            .user_handler
            .kick_device(request.into_inner(), &actor.tenant_id)
            .await
            .into_grpc()?;
        Ok(Response::new(response))
    }

    pub async fn handle_self_kick_device(
        &self,
        request: Request<KickDeviceRequest>,
    ) -> Result<Response<KickDeviceResponse>, Status> {
        let (_, claims) = self.verified_identity(&request)?;
        Self::require_user(&claims, &request.get_ref().user_id)?;
        let response = self
            .user_handler
            .kick_device(request.into_inner(), &claims.tenant_id)
            .await
            .into_grpc()?;
        Ok(Response::new(response))
    }

    pub async fn handle_kick_tenant(
        &self,
        request: Request<KickTenantRequest>,
    ) -> Result<Response<KickTenantResponse>, Status> {
        let actor = require_grpc_control(&request, self.control_verifier.as_ref())?;
        if actor.kind != ControlActorKind::Employee && actor.kind != ControlActorKind::Scheduler {
            return Err(Status::permission_denied(
                "control actor not permitted to kick tenant",
            ));
        }
        actor.require_tenant(&request.get_ref().tenant_id)?;
        tracing::info!(actor_id = %actor.subject, tenant_id = %actor.tenant_id, "control kick_tenant authorized");
        let response = self
            .user_handler
            .kick_tenant(request.into_inner())
            .await
            .into_grpc()?;
        Ok(Response::new(response))
    }

    pub async fn handle_get_device(
        &self,
        request: Request<GetDeviceRequest>,
    ) -> Result<Response<GetDeviceResponse>, Status> {
        let (_, claims) = self.verified_identity(&request)?;
        Self::require_user(&claims, &request.get_ref().user_id)?;
        let response = self
            .user_handler
            .get_device(&claims.tenant_id, request.into_inner())
            .await
            .into_grpc()?;
        Ok(Response::new(response))
    }
}
