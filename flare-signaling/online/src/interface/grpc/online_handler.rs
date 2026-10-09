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
    PlatformActorKind, PlatformAssertionVerifier, require_platform_assertion,
};
use flare_im_service_kit::staff_context::{VerifiedStaff, require_staff, require_staff_any};
use flare_server_core::auth::core_admin::permission::{TENANT_KICK, USERS_KICK, USERS_READ};
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
    /// 平台公钥（`FLARE_PLATFORM_PUBLIC_KEY`）：只验 `KickTenant` 的调度器断言。
    control_verifier: Option<PlatformAssertionVerifier>,
    /// Cell 公钥集：用户态 RPC 的签名上下文。
    identity_verifier: Option<SignedContextVerifier>,
    /// Cell 公钥集，Staff 钉死 `kid=gateway`：admin-gateway 转发的员工上下文。
    staff_verifier: Option<SignedContextVerifier>,
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
            staff_verifier: self.staff_verifier.clone(),
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
            staff_verifier: None,
        }
    }

    pub fn with_control_verifier(mut self, verifier: Option<PlatformAssertionVerifier>) -> Self {
        self.control_verifier = verifier;
        self
    }

    pub fn with_identity_verifier(mut self, verifier: Option<SignedContextVerifier>) -> Self {
        self.identity_verifier = verifier;
        self
    }

    pub fn with_staff_verifier(mut self, verifier: Option<SignedContextVerifier>) -> Self {
        self.staff_verifier = verifier;
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

    /// 管理员分支：只认 admin-gateway 原样转发的 Staff `x-flare-ctx`（网关 admin 分区签，
    /// `kid=gateway`），按 RPC 族复核签名里的权限码；租户取自签名。
    fn staff<T>(&self, request: &Request<T>, permission: &str) -> Result<VerifiedStaff, Status> {
        let verifier = self
            .staff_verifier
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("staff context verifier not configured"))?;
        require_staff(request, verifier, permission)
    }

    /// [`Self::staff`] admitting any of `codes`.
    fn staff_any<T>(&self, request: &Request<T>, codes: &[&str]) -> Result<VerifiedStaff, Status> {
        let verifier = self
            .staff_verifier
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("staff context verifier not configured"))?;
        require_staff_any(request, verifier, codes)
    }

    fn require_admin_user_id(user_id: &str) -> Result<(), Status> {
        if user_id.trim().is_empty() || user_id.trim() != user_id {
            return Err(Status::invalid_argument("user_id required"));
        }
        Ok(())
    }

    pub async fn handle_admin_list_user_devices(
        &self,
        request: Request<AdminListUserDevicesRequest>,
    ) -> Result<Response<ListUserDevicesResponse>, Status> {
        // 设备列表既是只读查询（`core:users:read`），也是整用户踢线（`core:users:kick`）的
        // 第一步：admin-gateway 先列设备再逐台 KickDevice，转发的是同一个 kick 上下文。
        let staff = self.staff_any(&request, &[USERS_READ, USERS_KICK])?;
        let user_id = request.into_inner().user_id;
        Self::require_admin_user_id(&user_id)?;
        tracing::info!(actor_id = %staff.actor_id, tenant_id = %staff.tenant_id, request_id = %staff.request_id, "admin list_user_devices");
        let response = self
            .user_handler
            .list_user_devices(&staff.tenant_id, ListUserDevicesRequest { user_id })
            .await
            .into_grpc()?;
        Ok(Response::new(response))
    }

    pub async fn handle_admin_get_user_presence(
        &self,
        request: Request<AdminGetUserPresenceRequest>,
    ) -> Result<Response<GetUserPresenceResponse>, Status> {
        let staff = self.staff(&request, USERS_READ)?;
        let user_id = request.into_inner().user_id;
        Self::require_admin_user_id(&user_id)?;
        tracing::info!(actor_id = %staff.actor_id, tenant_id = %staff.tenant_id, request_id = %staff.request_id, "admin get_user_presence");
        let response = self
            .user_handler
            .get_user_presence(&staff.tenant_id, GetUserPresenceRequest { user_id })
            .await
            .into_grpc()?;
        Ok(Response::new(response))
    }

    pub async fn handle_kick_device(
        &self,
        request: Request<KickDeviceRequest>,
    ) -> Result<Response<KickDeviceResponse>, Status> {
        // admin-gateway 转发的 Staff 上下文（`core:users:kick`）；租户取自签名，会话须属于它。
        let staff = self.staff(&request, USERS_KICK)?;
        tracing::info!(actor_id = %staff.actor_id, tenant_id = %staff.tenant_id, request_id = %staff.request_id, "staff kick_device authorized");
        let response = self
            .user_handler
            .kick_device(request.into_inner(), &staff.tenant_id)
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
        // 两条分支：admin-gateway 转发的 Staff 上下文（`core:tenant:kick`，签名租户须等于请求
        // 租户），或平台调度器的控制断言（`FLARE_PLATFORM_PUBLIC_KEY`，租户须等于请求租户）。
        // 带 `x-flare-ctx` 的调用走 Staff 分支，否则走调度器分支；两边各自 fail closed。
        let tenant_id = request.get_ref().tenant_id.clone();
        if request.metadata().contains_key("x-flare-ctx") {
            let staff = self.staff(&request, TENANT_KICK)?;
            staff.require_tenant(&tenant_id)?;
            tracing::info!(actor_id = %staff.actor_id, tenant_id = %staff.tenant_id, request_id = %staff.request_id, "staff kick_tenant authorized");
        } else {
            let actor = require_platform_assertion(&request, self.control_verifier.as_ref())?;
            actor.require_kind(PlatformActorKind::Scheduler)?;
            actor.require_tenant(&tenant_id)?;
            tracing::info!(actor_id = %actor.subject, tenant_id = %actor.tenant_id, "scheduler kick_tenant authorized");
        }
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

#[cfg(test)]
mod admin_branch_tests {
    //! 管理员分支：缺上下文、错误 kid、平台断言、权限码不符、跨租户的负向用例，外加租户只取自
    //! 签名的正向对照；`KickTenant` 另有调度器断言分支。

    use std::collections::HashMap;
    use std::sync::Mutex;

    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_im_service_kit::control_auth::{
        CORE_PROJECTION_AUDIENCE, PLATFORM_ISSUER, PROJECTION_WORKER_SUBJECT, SCHEDULER_SUBJECT,
    };
    use flare_server_core::auth::{KeySet, kid};
    use flare_server_core::context::Context;
    use flare_server_core::error::Result as FlareResult;
    use flare_server_core::{SignedContextIdentity, SignedContextSigner};
    use tonic::Code;

    use super::*;
    use crate::domain::aggregate::{Connection, ConnectionCreateParams};
    use crate::domain::model::OnlineStatusRecord;
    use crate::domain::repository::{EvictedConnection, PresenceChangeEvent};
    use crate::domain::service::{OnlineStatusService, SubscriptionService, UserService};
    use crate::domain::value_object::{
        ConnectionId, DeviceId, DevicePriority, TokenVersion, UserId,
    };

    /// 按 (tenant, user) 分桶的会话存储；只实现管理员分支会走到的读写。
    #[derive(Default)]
    struct TenantSessions {
        sessions: Mutex<HashMap<(String, String), Vec<Connection>>>,
        kicked: Mutex<Vec<(String, String)>>,
    }

    impl TenantSessions {
        fn seed(&self, tenant: &str, user: &str, device: &str) {
            let connection = Connection::create(ConnectionCreateParams {
                user_id: UserId::new(user.to_string()).unwrap(),
                device_id: DeviceId::new(device.to_string()).unwrap(),
                tenant_id: tenant.to_string(),
                device_platform: "ios".to_string(),
                server_id: "s".to_string(),
                gateway_id: "g".to_string(),
                device_priority: DevicePriority::Normal,
                token_version: TokenVersion::from(0),
                initial_quality: None,
            });
            self.sessions
                .lock()
                .unwrap()
                .entry((tenant.to_string(), user.to_string()))
                .or_default()
                .push(connection);
        }

        fn devices(&self, tenant: &str, user: &str) -> Vec<String> {
            self.sessions
                .lock()
                .unwrap()
                .get(&(tenant.to_string(), user.to_string()))
                .map(|list| {
                    list.iter()
                        .map(|c| c.device_id().as_str().to_string())
                        .collect()
                })
                .unwrap_or_default()
        }
    }

    impl ConversationRepository for TenantSessions {
        async fn save_connection(&self, _: &Connection) -> FlareResult<()> {
            unreachable!("admin branch never saves connections")
        }
        async fn admit_connection(
            &self,
            _: &Connection,
            _: &str,
            _: DeviceConflictStrategy,
            _: u32,
        ) -> FlareResult<Vec<EvictedConnection>> {
            unreachable!("admin branch never admits connections")
        }
        async fn remove_connection(
            &self,
            tenant_id: &str,
            connection_id: &ConnectionId,
            user_id: &UserId,
        ) -> FlareResult<()> {
            if let Some(list) = self
                .sessions
                .lock()
                .unwrap()
                .get_mut(&(tenant_id.to_string(), user_id.as_str().to_string()))
            {
                list.retain(|c| c.id() != connection_id);
            }
            Ok(())
        }
        async fn touch_connection(&self, _: &str, _: &ConnectionId, _: &UserId) -> FlareResult<()> {
            Ok(())
        }
        async fn fetch_statuses(
            &self,
            _: &str,
            _: &[String],
        ) -> FlareResult<HashMap<String, OnlineStatusRecord>> {
            Ok(HashMap::new())
        }
        async fn get_user_connections(
            &self,
            tenant_id: &str,
            user_id: &UserId,
        ) -> FlareResult<Vec<Connection>> {
            Ok(self
                .sessions
                .lock()
                .unwrap()
                .get(&(tenant_id.to_string(), user_id.as_str().to_string()))
                .cloned()
                .unwrap_or_default())
        }
        async fn remove_user_connections(
            &self,
            _: &str,
            _: &UserId,
            _: Option<&[DeviceId]>,
        ) -> FlareResult<()> {
            Ok(())
        }
        async fn get_connection_by_device(
            &self,
            tenant_id: &str,
            user_id: &UserId,
            device_id: &DeviceId,
        ) -> FlareResult<Option<Connection>> {
            Ok(self
                .get_user_connections(tenant_id, user_id)
                .await?
                .into_iter()
                .find(|c| c.device_id() == device_id))
        }
        async fn list_user_connections(&self, _: &Context) -> FlareResult<Vec<Connection>> {
            Ok(Vec::new())
        }
        async fn scan_tenant_sessions(
            &self,
            tenant_id: &str,
            _: u64,
            _: usize,
        ) -> FlareResult<(u64, Vec<String>)> {
            let members = self
                .sessions
                .lock()
                .unwrap()
                .iter()
                .filter(|((tenant, _), _)| tenant == tenant_id)
                .flat_map(|((_, user), list)| {
                    list.iter()
                        .map(|c| {
                            crate::domain::repository::tenant_session_member(
                                user,
                                c.device_id().as_str(),
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .collect();
            Ok((0, members))
        }
        async fn unindex_tenant_session(&self, _: &str, _: &str) -> FlareResult<()> {
            Ok(())
        }
        async fn broadcast_user_kick(
            &self,
            tenant_id: &str,
            user_id: &str,
            _: Option<&str>,
            _: Option<&str>,
            _: &str,
        ) -> FlareResult<()> {
            self.kicked
                .lock()
                .unwrap()
                .push((tenant_id.to_string(), user_id.to_string()));
            Ok(())
        }
    }

    struct NoSubscriptions;

    impl SubscriptionRepository for NoSubscriptions {
        async fn add_subscription(&self, _: &str, _: String, _: String) -> FlareResult<()> {
            Ok(())
        }
        async fn remove_subscription(&self, _: &Context, _: &[String]) -> FlareResult<()> {
            Ok(())
        }
        async fn get_user_subscriptions(
            &self,
            _: &Context,
        ) -> FlareResult<Vec<(String, HashMap<String, String>)>> {
            Ok(Vec::new())
        }
        async fn get_topic_subscribers(&self, _: &str, _: &str) -> FlareResult<Vec<String>> {
            Ok(Vec::new())
        }
    }

    struct NoPresence;

    impl PresencePublisher for NoPresence {
        async fn publish_presence_event(&self, _: &str, _: PresenceEvent) -> FlareResult<()> {
            Ok(())
        }
        async fn publish_user_presence_event(
            &self,
            _: &str,
            _: UserPresenceEvent,
        ) -> FlareResult<()> {
            Ok(())
        }
    }

    struct NoWatcher;

    impl PresenceWatcher for NoWatcher {
        async fn watch_presence(
            &self,
            _: &str,
            _: &[String],
        ) -> FlareResult<mpsc::Receiver<FlareResult<PresenceChangeEvent>>> {
            Ok(mpsc::channel(1).1)
        }
    }

    type Handler = OnlineHandler<TenantSessions, NoSubscriptions, NoPresence, NoWatcher>;

    /// 平台钥（调度器断言）、网关钥与 hook 钥（Staff 上下文）。
    const PLATFORM_KEY: [u8; 32] = [37; 32];
    const GATEWAY_KEY: [u8; 32] = [38; 32];
    const HOOK_KEY: [u8; 32] = [39; 32];

    fn pem(seed: [u8; 32]) -> (String, String) {
        let key = SigningKey::from_bytes(&seed);
        (
            key.to_pkcs8_pem(Default::default()).unwrap().to_string(),
            key.verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap(),
        )
    }

    fn handler(repo: Arc<TenantSessions>, with_key: bool) -> Handler {
        let control = with_key
            .then(|| PlatformAssertionVerifier::from_public_key(&pem(PLATFORM_KEY).1).unwrap());
        let staff = with_key.then(|| {
            let mut keys = KeySet::empty();
            keys.insert(kid::GATEWAY, &pem(GATEWAY_KEY).1).unwrap();
            keys.insert(kid::HOOK, &pem(HOOK_KEY).1).unwrap();
            SignedContextVerifier::from_key_set(keys).require_staff_kid([kid::GATEWAY])
        });
        OnlineHandler::new(
            Arc::new(OnlineCommandHandler::new(
                Arc::new(OnlineStatusService::new(repo.clone(), "gw".to_string())),
                Arc::new(SubscriptionService::new(
                    Arc::new(NoSubscriptions),
                    Arc::new(NoPresence),
                )),
            )),
            Arc::new(OnlineQueryHandler::new(repo.clone())),
            Arc::new(OnlineUserHandler::new(Arc::new(UserService::new(repo)))),
            Arc::new(OnlinePresenceWatcherHandler::new(Arc::new(NoWatcher))),
        )
        .with_control_verifier(control)
        .with_staff_verifier(staff)
    }

    fn seeded() -> Arc<TenantSessions> {
        let repo = Arc::new(TenantSessions::default());
        repo.seed("tenant-a", "u1", "a1");
        repo.seed("tenant-b", "u1", "b1");
        repo.seed("tenant-b", "u1", "b2");
        repo.seed("tenant-b", "u9", "b9");
        repo
    }

    /// 平台签的控制断言，放在 `authorization`。
    fn signed<T>(
        payload: T,
        subject: &str,
        tenant: &str,
        token_use: &str,
        aud: &str,
    ) -> Request<T> {
        let token = flare_server_core::TokenService::from_ed25519_private_key(
            &pem(PLATFORM_KEY).0,
            PLATFORM_ISSUER,
            60,
        )
        .unwrap()
        .generate_with_ed25519(subject, Some(tenant), token_use, aud)
        .unwrap();
        let mut request = Request::new(payload);
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        request
    }

    fn staff_token(seed: [u8; 32], key_id: &str, tenant: &str, permission: &str) -> String {
        SignedContextSigner::from_private_key(&pem(seed).0)
            .unwrap()
            .with_kid(key_id)
            .unwrap()
            .sign(
                &SignedContextIdentity::new(SignedActorType::Staff, "employee-1", tenant, "req-1")
                    .with_staff_grant(Some("org-1".into()), Some(permission.into()), 2),
            )
            .unwrap()
    }

    fn with_ctx<T>(payload: T, token: &str) -> Request<T> {
        let mut request = Request::new(payload);
        request
            .metadata_mut()
            .insert("x-flare-ctx", token.parse().unwrap());
        request
    }

    /// 网关 admin 分区签的 Staff 上下文，带 `permission`。
    fn staff<T>(payload: T, tenant: &str, permission: &str) -> Request<T> {
        with_ctx(
            payload,
            &staff_token(GATEWAY_KEY, kid::GATEWAY, tenant, permission),
        )
    }

    fn admin<T>(payload: T, tenant: &str) -> Request<T> {
        staff(payload, tenant, USERS_READ)
    }

    fn devices_req(user: &str) -> AdminListUserDevicesRequest {
        AdminListUserDevicesRequest {
            user_id: user.to_string(),
        }
    }

    fn presence_req(user: &str) -> AdminGetUserPresenceRequest {
        AdminGetUserPresenceRequest {
            user_id: user.to_string(),
        }
    }

    fn device_ids(response: ListUserDevicesResponse) -> Vec<String> {
        let mut ids: Vec<_> = response.devices.into_iter().map(|d| d.device_id).collect();
        ids.sort();
        ids
    }

    #[tokio::test]
    async fn admin_reads_require_the_forwarded_staff_context() {
        fn code<T>(result: Result<T, Status>) -> Code {
            result.map(|_| ()).unwrap_err().code()
        }
        let online = handler(seeded(), true);
        assert_eq!(
            code(
                online
                    .handle_admin_list_user_devices(Request::new(devices_req("u1")))
                    .await
            ),
            Code::Unauthenticated
        );
        assert_eq!(
            code(
                online
                    .handle_admin_get_user_presence(Request::new(presence_req("u1")))
                    .await
            ),
            Code::Unauthenticated
        );
        // 平台断言（旧 admin 受众或 control 受众）放在 authorization 里不是身份。
        for (token_use, aud) in [
            ("admin", "flare-core-admin"),
            ("control", CORE_PROJECTION_AUDIENCE),
        ] {
            let request = signed(devices_req("u1"), "employee-1", "tenant-a", token_use, aud);
            assert_eq!(
                code(online.handle_admin_list_user_devices(request).await),
                Code::Unauthenticated,
                "{token_use}/{aud}"
            );
        }
        // 没配 Cell 公钥集时 fail closed。
        let unkeyed = handler(seeded(), false);
        assert_eq!(
            code(
                unkeyed
                    .handle_admin_list_user_devices(admin(devices_req("u1"), "tenant-a"))
                    .await
            ),
            Code::Unauthenticated
        );
    }

    #[tokio::test]
    async fn admin_reads_reject_wrong_kid_permission_and_non_staff_contexts() {
        let online = handler(seeded(), true);
        // hook 钥签的 Staff 上下文：Staff 钉死 gateway。
        let forged = staff_token(HOOK_KEY, kid::HOOK, "tenant-a", USERS_READ);
        assert_eq!(
            online
                .handle_admin_list_user_devices(with_ctx(devices_req("u1"), &forged))
                .await
                .unwrap_err()
                .code(),
            Code::Unauthenticated
        );
        // 别的权限码（含踢线码调只读）。
        for permission in [USERS_KICK, "core:conversations:read"] {
            assert_eq!(
                online
                    .handle_admin_get_user_presence(staff(
                        presence_req("u1"),
                        "tenant-a",
                        permission
                    ))
                    .await
                    .unwrap_err()
                    .code(),
                Code::PermissionDenied,
                "{permission}"
            );
        }
        // 没有应用（平台范围）或通配租户的 Staff 上下文。
        for tenant in ["", "*"] {
            assert_eq!(
                online
                    .handle_admin_get_user_presence(staff(presence_req("u1"), tenant, USERS_READ))
                    .await
                    .unwrap_err()
                    .code(),
                Code::PermissionDenied,
                "{tenant:?}"
            );
        }
        assert_eq!(
            online
                .handle_admin_list_user_devices(admin(devices_req(""), "tenant-a"))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn device_listing_also_admits_the_whole_user_kick_code() {
        // POST /users/{user_id}/kick 带 `core:users:kick`：admin-gateway 用同一个上下文先列设备。
        let online = handler(seeded(), true);
        let listed = online
            .handle_admin_list_user_devices(staff(devices_req("u1"), "tenant-b", USERS_KICK))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(device_ids(listed), vec!["b1", "b2"]);
        // 在场状态仍只认只读码；其它码（含租户踢线码）列设备也不行。
        assert_eq!(
            online
                .handle_admin_get_user_presence(staff(presence_req("u1"), "tenant-b", USERS_KICK))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        for permission in [TENANT_KICK, "core:conversations:read"] {
            assert_eq!(
                online
                    .handle_admin_list_user_devices(staff(
                        devices_req("u1"),
                        "tenant-b",
                        permission
                    ))
                    .await
                    .unwrap_err()
                    .code(),
                Code::PermissionDenied,
                "{permission}"
            );
        }
        // 没有权限码的 Staff 上下文：两个码都对不上。
        let codeless = SignedContextSigner::from_private_key(&pem(GATEWAY_KEY).0)
            .unwrap()
            .with_kid(kid::GATEWAY)
            .unwrap()
            .sign(
                &SignedContextIdentity::new(SignedActorType::Staff, "employee-1", "tenant-b", "r")
                    .with_staff_grant(Some("org-1".into()), None, 2),
            )
            .unwrap();
        assert_eq!(
            online
                .handle_admin_list_user_devices(with_ctx(devices_req("u1"), &codeless))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
    }

    #[tokio::test]
    async fn admin_reads_are_scoped_to_the_token_tenant() {
        let online = handler(seeded(), true);
        let a = online
            .handle_admin_list_user_devices(admin(devices_req("u1"), "tenant-a"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(device_ids(a), vec!["a1"]);
        let b = online
            .handle_admin_list_user_devices(admin(devices_req("u1"), "tenant-b"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(device_ids(b), vec!["b1", "b2"]);

        // 跨租户：u9 只在 tenant-b 在线，tenant-a 的管理员看不到。
        let cross = online
            .handle_admin_list_user_devices(admin(devices_req("u9"), "tenant-a"))
            .await
            .unwrap()
            .into_inner();
        assert!(cross.devices.is_empty());
        let presence = online
            .handle_admin_get_user_presence(admin(presence_req("u9"), "tenant-a"))
            .await
            .unwrap()
            .into_inner()
            .presence
            .unwrap();
        assert!(!presence.is_online);
        assert!(presence.devices.is_empty());
        let presence = online
            .handle_admin_get_user_presence(admin(presence_req("u9"), "tenant-b"))
            .await
            .unwrap()
            .into_inner()
            .presence
            .unwrap();
        assert!(presence.is_online);
    }

    fn kick_device_req(user: &str, device: &str) -> KickDeviceRequest {
        KickDeviceRequest {
            user_id: user.to_string(),
            device_id: device.to_string(),
            reason: "admin".to_string(),
        }
    }

    #[tokio::test]
    async fn kick_device_accepts_the_staff_kick_permission_within_the_signed_tenant() {
        let repo = seeded();
        let online = handler(repo.clone(), true);
        assert_eq!(
            online
                .handle_kick_device(Request::new(kick_device_req("u1", "a1")))
                .await
                .unwrap_err()
                .code(),
            Code::Unauthenticated
        );
        // 平台断言（调度器也好、员工主体也好）不能踢设备。
        for subject in [SCHEDULER_SUBJECT, "employee-1"] {
            assert_eq!(
                online
                    .handle_kick_device(signed(
                        kick_device_req("u1", "a1"),
                        subject,
                        "tenant-a",
                        "control",
                        CORE_PROJECTION_AUDIENCE,
                    ))
                    .await
                    .unwrap_err()
                    .code(),
                Code::Unauthenticated,
                "{subject}"
            );
        }
        // 只读码不能踢。
        assert_eq!(
            online
                .handle_kick_device(staff(kick_device_req("u1", "a1"), "tenant-a", USERS_READ))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );

        // 跨租户：b9 只在 tenant-b，tenant-a 的员工踢不到，会话原样保留。
        assert!(
            online
                .handle_kick_device(staff(kick_device_req("u9", "b9"), "tenant-a", USERS_KICK))
                .await
                .is_err()
        );
        assert_eq!(repo.devices("tenant-b", "u9"), vec!["b9"]);

        assert!(
            online
                .handle_kick_device(staff(kick_device_req("u1", "a1"), "tenant-a", USERS_KICK))
                .await
                .unwrap()
                .into_inner()
                .success
        );
        assert!(repo.devices("tenant-a", "u1").is_empty());
        assert_eq!(repo.devices("tenant-b", "u1"), vec!["b1", "b2"]);
    }

    fn kick_tenant_req(tenant: &str) -> KickTenantRequest {
        KickTenantRequest {
            tenant_id: tenant.to_string(),
            reason: "admin".to_string(),
        }
    }

    #[tokio::test]
    async fn kick_tenant_takes_the_staff_kick_code_or_the_scheduler_assertion() {
        let repo = seeded();
        let online = handler(repo.clone(), true);
        assert_eq!(
            online
                .handle_kick_tenant(Request::new(kick_tenant_req("tenant-a")))
                .await
                .unwrap_err()
                .code(),
            Code::Unauthenticated
        );
        // 旧 admin 断言与员工主体的 control 断言都不是身份。
        for (subject, token_use, aud) in [
            ("employee-1", "admin", "flare-core-admin"),
            ("employee-1", "control", CORE_PROJECTION_AUDIENCE),
            (SCHEDULER_SUBJECT, "admin", "flare-core-admin"),
        ] {
            assert_eq!(
                online
                    .handle_kick_tenant(signed(
                        kick_tenant_req("tenant-a"),
                        subject,
                        "tenant-a",
                        token_use,
                        aud,
                    ))
                    .await
                    .unwrap_err()
                    .code(),
                Code::Unauthenticated,
                "{subject}/{token_use}/{aud}"
            );
        }
        // 投影 worker 不能踢租户。
        assert_eq!(
            online
                .handle_kick_tenant(signed(
                    kick_tenant_req("tenant-a"),
                    PROJECTION_WORKER_SUBJECT,
                    "tenant-a",
                    "control",
                    CORE_PROJECTION_AUDIENCE,
                ))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        // Staff 分支：踢设备的码不够；签名租户与请求租户不一致也拒绝。
        assert_eq!(
            online
                .handle_kick_tenant(staff(kick_tenant_req("tenant-a"), "tenant-a", USERS_KICK))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            online
                .handle_kick_tenant(staff(kick_tenant_req("tenant-b"), "tenant-a", TENANT_KICK))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(repo.devices("tenant-b", "u1"), vec!["b1", "b2"]);

        let kicked = online
            .handle_kick_tenant(staff(kick_tenant_req("tenant-a"), "tenant-a", TENANT_KICK))
            .await
            .unwrap()
            .into_inner()
            .kicked;
        assert_eq!(kicked, 1);
        assert!(repo.devices("tenant-a", "u1").is_empty());
        assert_eq!(repo.devices("tenant-b", "u9"), vec!["b9"]);

        // 调度器分支不变：平台的 control 断言仍可踢自己的租户。
        let kicked = online
            .handle_kick_tenant(signed(
                kick_tenant_req("tenant-b"),
                SCHEDULER_SUBJECT,
                "tenant-b",
                "control",
                CORE_PROJECTION_AUDIENCE,
            ))
            .await
            .unwrap()
            .into_inner()
            .kicked;
        assert_eq!(kicked, 3);
        assert!(
            repo.kicked
                .lock()
                .unwrap()
                .iter()
                .all(|(tenant, _)| tenant == "tenant-a" || tenant == "tenant-b")
        );
    }
}
