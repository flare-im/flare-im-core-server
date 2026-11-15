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

use crate::application::commands::{
    HeartbeatCommand, LoginCommand, LogoutCommand, ReportDeviceStateCommand,
};
use crate::application::queries::GetOnlineStatusQuery;
use flare_im_service_kit::audit_effects::durable::StaffAudit;

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
    tenant_kick_fence: Option<flare_im_service_kit::tenant_kick_fence::TenantKickFence>,
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
            tenant_kick_fence: self.tenant_kick_fence.clone(),
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
            tenant_kick_fence: None,
        }
    }

    pub fn with_control_verifier(mut self, verifier: Option<PlatformAssertionVerifier>) -> Self {
        self.control_verifier = verifier;
        self
    }

    pub fn with_tenant_kick_fence(
        mut self,
        fence: Option<flare_im_service_kit::tenant_kick_fence::TenantKickFence>,
    ) -> Self {
        self.tenant_kick_fence = fence;
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
            .get("identity_session_id")
            .map(String::as_str)
            != claims.session_id.as_deref()
        {
            return Err(Status::permission_denied(
                "authenticated identity session mismatch",
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

    /// 设备状态上报：与心跳同一个调用方（网关替这位用户签的上下文）、同一个租户边界，
    /// 租户取自签名。
    pub async fn handle_report_device_state(
        &self,
        request: Request<ReportDeviceStateRequest>,
    ) -> Result<Response<ReportDeviceStateResponse>, Status> {
        let (ctx, claims) = self.verified_identity(&request)?;
        Self::require_user(&claims, &request.get_ref().user_id)?;
        let command = ReportDeviceStateCommand {
            request: request.into_inner(),
            ctx: ctx.as_ref().clone(),
        };
        let response = self
            .command_handler
            .handle_report_device_state(&ctx, command)
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
            .staff_audit(&staff.ctx, "signaling-online")
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
        let mut audit_ctx = None;
        if request.metadata().contains_key("x-flare-ctx") {
            let staff = self.staff(&request, TENANT_KICK)?;
            staff.require_tenant(&tenant_id)?;
            audit_ctx = Some(staff.ctx.clone());
            tracing::info!(actor_id = %staff.actor_id, tenant_id = %staff.tenant_id, request_id = %staff.request_id, "staff kick_tenant authorized");
        } else {
            let actor = require_platform_assertion(&request, self.control_verifier.as_ref())?;
            actor.require_kind(PlatformActorKind::Scheduler)?;
            actor.require_tenant(&tenant_id)?;
            if request.get_ref().expected_tenant_version.is_none() {
                return Err(Status::invalid_argument(
                    "scheduler kick requires tenant version",
                ));
            }
            tracing::info!(actor_id = %actor.subject, tenant_id = %actor.tenant_id, "scheduler kick_tenant authorized");
        }
        let lease = if let Some(version) = request.get_ref().expected_tenant_version {
            let fence = self
                .tenant_kick_fence
                .as_ref()
                .ok_or_else(|| Status::failed_precondition("tenant fence not configured"))?;
            match fence.acquire(&tenant_id, version).await? {
                Some(lease) => Some(lease),
                None => {
                    return Ok(Response::new(KickTenantResponse {
                        kicked: 0,
                        superseded: true,
                    }));
                }
            }
        } else {
            None
        };
        let response = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let work = self.user_handler.kick_tenant(request.into_inner());
            match audit_ctx {
                Some(ctx) => work.staff_audit(&ctx, "signaling-online").await,
                None => work.await,
            }
        })
        .await
        .map_err(|_| Status::deadline_exceeded("tenant kick deadline"))?
        .into_grpc()?;
        if let Some(lease) = lease {
            lease.finish().await?;
        }
        Ok(Response::new(response))
    }

    /// Private durable identity-owner effect. Staff and ordinary Service contexts
    /// cannot use it, even when they can kick a device through another route.
    pub async fn handle_revoke_identity_sessions(
        &self,
        request: Request<flare_grpc_proto::auth_revoke::RevokeIdentitySessionsRequest>,
    ) -> Result<Response<flare_grpc_proto::auth_revoke::RevokeIdentitySessionsResponse>, Status>
    {
        let (_, claims) = self.verified_identity(&request)?;
        if claims.actor_type != SignedActorType::Service
            || claims.actor_id != "flare-social-bridge"
            || claims.roles.as_slice() != ["identity.revoke"]
            || claims.permission.is_some()
        {
            return Err(Status::permission_denied(
                "identity revocation owner required",
            ));
        }
        let input = request.into_inner();
        let now = chrono::Utc::now().timestamp_millis();
        if input.user_id.is_empty()
            || input.user_id.len() > 128
            || input.user_id.chars().any(char::is_control)
            || uuid::Uuid::parse_str(&input.revocation_id).is_err()
            || input.cutoff_ms <= 0
            || input.cutoff_ms > now.saturating_add(30_000)
            || input.session_ids.len() > 100
            || input
                .session_ids
                .iter()
                .any(|sid| uuid::Uuid::parse_str(sid).is_err())
        {
            return Err(Status::invalid_argument(
                "invalid identity revocation intent",
            ));
        }
        let kicked = self
            .user_handler
            .revoke_identity_sessions(
                &claims.tenant_id,
                &input.user_id,
                &input.session_ids,
                input.cutoff_ms,
                &claims.request_id,
                &claims.kid,
            )
            .await
            .into_grpc()?;
        Ok(Response::new(
            flare_grpc_proto::auth_revoke::RevokeIdentitySessionsResponse { kicked },
        ))
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
    use flare_proto::common::{DeviceAppState as ProtoAppState, DeviceAttention as ProtoAttention};
    use flare_server_core::auth::{KeySet, kid};
    use flare_server_core::context::Context;
    use flare_server_core::error::Result as FlareResult;
    use flare_server_core::{SignedContextIdentity, SignedContextSigner};
    use tonic::Code;

    use super::*;
    use crate::domain::aggregate::{Connection, ConnectionCreateParams};
    use crate::domain::connection_event_publisher::NoopConnectionEventPublisher;
    use crate::domain::model::OnlineStatusRecord;
    use crate::domain::push_binding::{
        BindingRevocation, BindingRevocationReason, BindingRevocationSink,
    };
    use crate::domain::repository::{EvictedConnection, PresenceChangeEvent};
    use crate::domain::service::{OnlineStatusService, SubscriptionService, UserService};
    use crate::domain::value_object::{
        ConnectionId, DeviceAppState, DeviceAttention, DeviceId, DevicePriority, DeviceStateUpdate,
        TokenVersion, UserId,
    };

    /// 按 (tenant, user) 分桶的会话存储；实现管理员分支与设备状态上报会走到的读写。
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
                app_state: DeviceAppState::Active,
                attention: DeviceAttention::Unspecified,
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
            connection: &Connection,
            _: &str,
            _: DeviceConflictStrategy,
            _: u32,
        ) -> FlareResult<Vec<EvictedConnection>> {
            self.sessions
                .lock()
                .unwrap()
                .entry((
                    connection.tenant_id().to_string(),
                    connection.user_id().as_str().to_string(),
                ))
                .or_default()
                .push(connection.clone());
            Ok(Vec::new())
        }
        async fn report_device_state(
            &self,
            tenant_id: &str,
            connection_id: &ConnectionId,
            user_id: &UserId,
            update: DeviceStateUpdate,
        ) -> FlareResult<bool> {
            let mut sessions = self.sessions.lock().unwrap();
            let Some(connection) = sessions
                .get_mut(&(tenant_id.to_string(), user_id.as_str().to_string()))
                .and_then(|list| list.iter_mut().find(|c| c.id() == connection_id))
            else {
                return Ok(false);
            };
            connection.report_device_state(update, chrono::Utc::now());
            Ok(true)
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
        async fn revoke_identity_sessions(
            &self,
            tenant: &str,
            user: &UserId,
            _: &[String],
            _: i64,
            _: &str,
            _: &str,
        ) -> FlareResult<u32> {
            self.kicked
                .lock()
                .unwrap()
                .push((tenant.to_owned(), user.as_str().to_owned()));
            Ok(1)
        }
        async fn touch_connection(&self, _: &str, _: &ConnectionId, _: &UserId) -> FlareResult<()> {
            Ok(())
        }

        async fn remove_connection_for_kick(
            &self,
            tenant: &str,
            id: &ConnectionId,
            user: &UserId,
            reason: &str,
        ) -> FlareResult<Option<String>> {
            let session = self
                .get_user_connections(tenant, user)
                .await?
                .into_iter()
                .find(|s| s.id() == id);
            let Some(session) = session else {
                return Ok(None);
            };
            self.broadcast_user_kick(
                tenant,
                user.as_str(),
                Some(session.device_id().as_str()),
                Some(&format!("physical-{}", id.as_str())),
                reason,
            )
            .await?;
            self.remove_connection(tenant, id, user).await?;
            Ok(Some(format!("physical-{}", id.as_str())))
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
        handler_revoking(repo, with_key, None)
    }

    /// 同 [`handler`]，领域服务接上推送绑定吊销的出口。
    fn handler_revoking(
        repo: Arc<TenantSessions>,
        with_key: bool,
        revocations: Option<Arc<dyn BindingRevocationSink>>,
    ) -> Handler {
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
                Arc::new(
                    OnlineStatusService::<_, NoopConnectionEventPublisher>::new(
                        repo.clone(),
                        "gw".to_string(),
                    )
                    .with_binding_revocations(revocations.clone()),
                ),
                Arc::new(SubscriptionService::new(
                    Arc::new(NoSubscriptions),
                    Arc::new(NoPresence),
                )),
            )),
            Arc::new(OnlineQueryHandler::new(repo.clone())),
            Arc::new(OnlineUserHandler::new(Arc::new(
                UserService::new(repo).with_binding_revocations(revocations),
            ))),
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
            expected_tenant_version: None,
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

        // Scheduler cannot issue an unconditional kick, even with a valid assertion.
        let denied = online
            .handle_kick_tenant(signed(
                kick_tenant_req("tenant-b"),
                SCHEDULER_SUBJECT,
                "tenant-b",
                "control",
                CORE_PROJECTION_AUDIENCE,
            ))
            .await
            .unwrap_err();
        assert_eq!(denied.code(), Code::InvalidArgument);
        assert_eq!(repo.devices("tenant-b", "u1"), vec!["b1", "b2"]);
        assert!(
            repo.kicked
                .lock()
                .unwrap()
                .iter()
                .all(|(tenant, _)| tenant == "tenant-a" || tenant == "tenant-b")
        );
    }
    #[tokio::test]
    async fn identity_revocation_is_owner_only_and_uses_the_verified_tenant() {
        use flare_grpc_proto::auth_revoke::RevokeIdentitySessionsRequest;
        let repo = seeded();
        let verifier = SignedContextVerifier::from_public_key(&pem(GATEWAY_KEY).1).unwrap();
        let online = handler(repo.clone(), true).with_identity_verifier(Some(verifier));
        let input = RevokeIdentitySessionsRequest {
            user_id: "u1".into(),
            revocation_id: uuid::Uuid::new_v4().to_string(),
            cutoff_ms: chrono::Utc::now().timestamp_millis(),
            session_ids: vec![uuid::Uuid::new_v4().to_string()],
        };
        assert_eq!(
            online
                .handle_revoke_identity_sessions(Request::new(input.clone()))
                .await
                .unwrap_err()
                .code(),
            Code::Unauthenticated
        );
        let call = |kind, actor: &str, roles: Vec<String>, app: &str, input| {
            let identity =
                SignedContextIdentity::new(kind, actor, app, "owned-revocation").with_roles(roles);
            let identity = if kind == SignedActorType::Staff {
                identity.with_staff_grant(
                    Some("org-a".into()),
                    Some("im:users:reset-password".into()),
                    1,
                )
            } else {
                identity
            };
            let token = SignedContextSigner::from_private_key(&pem(GATEWAY_KEY).0)
                .unwrap()
                .sign(&identity)
                .unwrap();
            with_ctx(input, &token)
        };
        for (kind, actor, roles) in [
            (
                SignedActorType::Staff,
                "employee",
                vec!["identity.revoke".into()],
            ),
            (SignedActorType::User, "u1", vec!["identity.revoke".into()]),
            (
                SignedActorType::Service,
                "another-service",
                vec!["identity.revoke".into()],
            ),
            (SignedActorType::Service, "flare-social-bridge", vec![]),
        ] {
            assert!(
                online
                    .handle_revoke_identity_sessions(call(
                        kind,
                        actor,
                        roles,
                        "tenant-a",
                        input.clone()
                    ))
                    .await
                    .is_err()
            );
        }
        assert!(repo.kicked.lock().unwrap().is_empty());
        let result = online
            .handle_revoke_identity_sessions(call(
                SignedActorType::Service,
                "flare-social-bridge",
                vec!["identity.revoke".into()],
                "tenant-b",
                input.clone(),
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(result.kicked, 1);
        assert_eq!(
            repo.kicked.lock().unwrap().as_slice(),
            &[("tenant-b".into(), "u1".into())]
        );
        let mut malformed = input;
        malformed.session_ids.push("untrusted-session".into());
        assert_eq!(
            online
                .handle_revoke_identity_sessions(call(
                    SignedActorType::Service,
                    "flare-social-bridge",
                    vec!["identity.revoke".into()],
                    "tenant-a",
                    malformed
                ))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
        assert_eq!(repo.kicked.lock().unwrap().len(), 1);
    }
    #[tokio::test]
    async fn login_rejects_unbound_or_tampered_identity_sid_metadata() {
        let verifier = SignedContextVerifier::from_public_key(&pem(GATEWAY_KEY).1).unwrap();
        let online = handler(seeded(), true).with_identity_verifier(Some(verifier));
        let sid = uuid::Uuid::new_v4().to_string();
        let different = uuid::Uuid::new_v4().to_string();
        for (claim, body) in [
            (None, Some(sid.as_str())),
            (Some(sid.as_str()), Some(different.as_str())),
            (Some(sid.as_str()), None),
        ] {
            let identity = SignedContextIdentity::new(
                SignedActorType::User,
                "u1",
                "tenant-a",
                "physical-owned",
            )
            .with_credential_id("signaling-gateway");
            let identity = if let Some(sid) = claim {
                identity.with_session_id(sid)
            } else {
                identity
            };
            let token = SignedContextSigner::from_private_key(&pem(GATEWAY_KEY).0)
                .unwrap()
                .sign(&identity)
                .unwrap();
            let mut metadata = HashMap::from([("connection_id".into(), "physical-owned".into())]);
            if let Some(sid) = body {
                metadata.insert("identity_session_id".into(), sid.into());
            }
            let request = LoginRequest {
                user_id: "u1".into(),
                device_id: "owned-device".into(),
                metadata,
                ..Default::default()
            };
            assert_eq!(
                online
                    .handle_login(with_ctx(request, &token))
                    .await
                    .unwrap_err()
                    .code(),
                Code::PermissionDenied
            );
        }
    }

    /// 网关替 `user` 签的用户上下文（`request_id` 是物理连接 ID）。
    fn user_ctx<T>(payload: T, tenant: &str, user: &str, connection: &str) -> Request<T> {
        let identity = SignedContextIdentity::new(SignedActorType::User, user, tenant, connection)
            .with_credential_id("signaling-gateway");
        let token = SignedContextSigner::from_private_key(&pem(GATEWAY_KEY).0)
            .unwrap()
            .sign(&identity)
            .unwrap();
        with_ctx(payload, &token)
    }

    fn device_state_handler(repo: Arc<TenantSessions>) -> Handler {
        let verifier = SignedContextVerifier::from_public_key(&pem(GATEWAY_KEY).1).unwrap();
        handler(repo, true).with_identity_verifier(Some(verifier))
    }

    fn state_report(
        user: &str,
        session: &str,
        app_state: ProtoAppState,
        attention: ProtoAttention,
        badge: u32,
    ) -> ReportDeviceStateRequest {
        ReportDeviceStateRequest {
            user_id: user.to_string(),
            conversation_id: session.to_string(),
            app_state: app_state as i32,
            attention: attention as i32,
            badge,
        }
    }

    /// 以 `state` 建连，返回会话 ID。
    async fn login_with_state(
        online: &Handler,
        tenant: &str,
        user: &str,
        app_state: ProtoAppState,
        attention: ProtoAttention,
    ) -> String {
        let login = online
            .handle_login(user_ctx(
                LoginRequest {
                    user_id: user.to_string(),
                    device_id: "phone".to_string(),
                    device_platform: "ios".to_string(),
                    desired_conflict_strategy: DeviceConflictStrategy::Coexist as i32,
                    metadata: HashMap::from([(
                        "connection_id".to_string(),
                        "physical-1".to_string(),
                    )]),
                    app_state: app_state as i32,
                    attention: attention as i32,
                    ..Default::default()
                },
                tenant,
                user,
                "physical-1",
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(login.success);
        login.conversation_id
    }

    async fn only_device(online: &Handler, tenant: &str, user: &str) -> DeviceInfo {
        let mut devices = online
            .handle_list_user_devices(user_ctx(
                ListUserDevicesRequest {
                    user_id: user.to_string(),
                },
                tenant,
                user,
                "physical-1",
            ))
            .await
            .unwrap()
            .into_inner()
            .devices;
        assert_eq!(devices.len(), 1);
        devices.remove(0)
    }

    fn since(device: &DeviceInfo) -> (i64, i32) {
        let at = device.state_since.as_ref().unwrap();
        (at.seconds, at.nanos)
    }

    /// 建连带来的状态是初始状态；之后的上报整体覆盖应用状态、注意力与角标，设备列表读到的就是
    /// 最近一次上报。进入当前应用状态的时刻只在应用状态变化时前移。
    #[tokio::test]
    async fn a_device_state_report_is_what_the_device_list_shows() {
        let online = device_state_handler(Arc::new(TenantSessions::default()));
        let session = login_with_state(
            &online,
            "tenant-a",
            "u1",
            ProtoAppState::Standby,
            ProtoAttention::Idle,
        )
        .await;

        let connected = only_device(&online, "tenant-a", "u1").await;
        assert_eq!(connected.conversation_id, session);
        assert_eq!(connected.app_state, ProtoAppState::Standby as i32);
        assert_eq!(connected.attention, ProtoAttention::Idle as i32);
        assert_eq!(connected.badge, 0);

        let report = |app_state, attention, badge| {
            user_ctx(
                state_report("u1", &session, app_state, attention, badge),
                "tenant-a",
                "u1",
                "physical-1",
            )
        };
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        // 同一应用状态下只改注意力与角标：不重新计时。
        assert!(
            online
                .handle_report_device_state(report(
                    ProtoAppState::Standby,
                    ProtoAttention::Attentive,
                    3
                ))
                .await
                .unwrap()
                .into_inner()
                .success
        );
        let refreshed = only_device(&online, "tenant-a", "u1").await;
        assert_eq!(refreshed.app_state, ProtoAppState::Standby as i32);
        assert_eq!(refreshed.attention, ProtoAttention::Attentive as i32);
        assert_eq!(refreshed.badge, 3);
        assert_eq!(since(&refreshed), since(&connected));

        // 托管：应用状态变了，重新计时。
        assert!(
            online
                .handle_report_device_state(report(
                    ProtoAppState::Parking,
                    ProtoAttention::Unspecified,
                    5
                ))
                .await
                .unwrap()
                .into_inner()
                .success
        );
        let parked = only_device(&online, "tenant-a", "u1").await;
        assert_eq!(parked.app_state, ProtoAppState::Parking as i32);
        assert_eq!(parked.attention, ProtoAttention::Unspecified as i32);
        assert_eq!(parked.badge, 5);
        assert!(since(&parked) > since(&connected));
    }

    /// 没带应用状态的建连与上报都按前台处理。
    #[tokio::test]
    async fn an_unspecified_app_state_counts_as_active() {
        let online = device_state_handler(Arc::new(TenantSessions::default()));
        let session = login_with_state(
            &online,
            "tenant-a",
            "u1",
            ProtoAppState::Unspecified,
            ProtoAttention::Unspecified,
        )
        .await;
        let connected = only_device(&online, "tenant-a", "u1").await;
        assert_eq!(connected.app_state, ProtoAppState::Active as i32);
        assert_eq!(connected.attention, ProtoAttention::Unspecified as i32);

        let parked = state_report(
            "u1",
            &session,
            ProtoAppState::Parking,
            ProtoAttention::Idle,
            1,
        );
        online
            .handle_report_device_state(user_ctx(parked, "tenant-a", "u1", "physical-1"))
            .await
            .unwrap();
        let back = state_report(
            "u1",
            &session,
            ProtoAppState::Unspecified,
            ProtoAttention::Idle,
            1,
        );
        online
            .handle_report_device_state(user_ctx(back, "tenant-a", "u1", "physical-1"))
            .await
            .unwrap();
        assert_eq!(
            only_device(&online, "tenant-a", "u1").await.app_state,
            ProtoAppState::Active as i32
        );
    }

    /// 「后台、保着链路、不能自己提示」在建连与上报时都被接受，设备列表原样带出。
    #[tokio::test]
    async fn a_deferred_device_is_accepted_on_login_and_report_and_exposed() {
        let online = device_state_handler(Arc::new(TenantSessions::default()));
        let session = login_with_state(
            &online,
            "tenant-a",
            "u1",
            ProtoAppState::Deferred,
            ProtoAttention::Unspecified,
        )
        .await;
        let connected = only_device(&online, "tenant-a", "u1").await;
        assert_eq!(connected.app_state, ProtoAppState::Deferred as i32);

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        for (app_state, badge) in [(ProtoAppState::Active, 0), (ProtoAppState::Deferred, 2)] {
            let report = state_report("u1", &session, app_state, ProtoAttention::Idle, badge);
            assert!(
                online
                    .handle_report_device_state(user_ctx(report, "tenant-a", "u1", "physical-1"))
                    .await
                    .unwrap()
                    .into_inner()
                    .success
            );
        }
        let deferred = only_device(&online, "tenant-a", "u1").await;
        assert_eq!(deferred.app_state, ProtoAppState::Deferred as i32);
        assert_eq!(deferred.badge, 2);
        // 回到前台再进后台：进入当前状态的时刻重新计。
        assert!(since(&deferred) > since(&connected));
    }

    /// 上报与心跳同一个边界：必须是网关替这位用户签的上下文，租户取自签名。会话不在签名租户下
    /// （已断开，或属于别的租户）时 `success = false`，别的租户的记录不受影响。
    #[tokio::test]
    async fn a_device_state_report_is_bound_to_the_signed_user_and_tenant() {
        let online = device_state_handler(Arc::new(TenantSessions::default()));
        let session = login_with_state(
            &online,
            "tenant-a",
            "u1",
            ProtoAppState::Active,
            ProtoAttention::Attentive,
        )
        .await;
        let parking = || {
            state_report(
                "u1",
                &session,
                ProtoAppState::Parking,
                ProtoAttention::Idle,
                9,
            )
        };

        // 没有签名上下文。
        assert_eq!(
            online
                .handle_report_device_state(Request::new(parking()))
                .await
                .unwrap_err()
                .code(),
            Code::Unauthenticated
        );
        // 别的用户的上下文替 u1 上报。
        assert_eq!(
            online
                .handle_report_device_state(user_ctx(parking(), "tenant-a", "u2", "physical-2"))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        // 同一个用户 ID 在另一个租户：找不到这条会话。
        assert!(
            !online
                .handle_report_device_state(user_ctx(parking(), "tenant-b", "u1", "physical-1"))
                .await
                .unwrap()
                .into_inner()
                .success
        );
        // 会话已经不在。
        let gone = state_report(
            "u1",
            &uuid::Uuid::new_v4().to_string(),
            ProtoAppState::Parking,
            ProtoAttention::Idle,
            9,
        );
        assert!(
            !online
                .handle_report_device_state(user_ctx(gone, "tenant-a", "u1", "physical-1"))
                .await
                .unwrap()
                .into_inner()
                .success
        );

        let untouched = only_device(&online, "tenant-a", "u1").await;
        assert_eq!(untouched.app_state, ProtoAppState::Active as i32);
        assert_eq!(untouched.attention, ProtoAttention::Attentive as i32);
        assert_eq!(untouched.badge, 0);
    }

    /// 一条吊销请求针对谁：`(租户, 用户, 设备, 原因)`；设备为 `None` 是全部设备。
    type RevocationTarget = (String, String, Option<String>, BindingRevocationReason);

    fn revocation_target(
        tenant: &str,
        user: &str,
        device: Option<&str>,
        reason: BindingRevocationReason,
    ) -> RevocationTarget {
        (
            tenant.to_string(),
            user.to_string(),
            device.map(str::to_string),
            reason,
        )
    }

    /// 记下提交的推送绑定吊销请求。
    #[derive(Default)]
    struct RecordedRevocations(Mutex<Vec<BindingRevocation>>);

    impl RecordedRevocations {
        /// 各条请求针对谁，按 (租户, 用户, 设备) 排序。
        fn targets(&self) -> Vec<RevocationTarget> {
            let mut targets: Vec<RevocationTarget> = self
                .0
                .lock()
                .unwrap()
                .iter()
                .map(|r| {
                    (
                        r.tenant_id.clone(),
                        r.user_id.clone(),
                        r.device_id.clone(),
                        r.reason,
                    )
                })
                .collect();
            targets.sort_by(|a, b| (&a.0, &a.1, &a.2).cmp(&(&b.0, &b.1, &b.2)));
            targets
        }

        /// 各条请求的登记时刻上限。
        fn cutoffs(&self) -> Vec<i64> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .map(|r| r.registered_before_ms)
                .collect()
        }
    }

    impl BindingRevocationSink for RecordedRevocations {
        fn submit(&self, revocation: BindingRevocation) {
            self.0.lock().unwrap().push(revocation);
        }
    }

    /// 带吊销出口与用户上下文验证的 handler。
    fn revoking_handler(
        repo: Arc<TenantSessions>,
        revocations: &Arc<RecordedRevocations>,
    ) -> Handler {
        let verifier = SignedContextVerifier::from_public_key(&pem(GATEWAY_KEY).1).unwrap();
        handler_revoking(
            repo,
            true,
            Some(revocations.clone() as Arc<dyn BindingRevocationSink>),
        )
        .with_identity_verifier(Some(verifier))
    }

    /// 运维踢设备与用户踢自己的设备：被踢设备的推送绑定吊销，只吊销踢线那一刻之前登记的。正常
    /// 登出不吊销。
    #[tokio::test]
    async fn kick_rpcs_revoke_the_push_bindings_of_the_kicked_device() {
        let repo = seeded();
        let revocations = Arc::new(RecordedRevocations::default());
        let online = revoking_handler(repo.clone(), &revocations);

        let before_ms = chrono::Utc::now().timestamp_millis();
        assert!(
            online
                .handle_kick_device(staff(kick_device_req("u1", "a1"), "tenant-a", USERS_KICK))
                .await
                .unwrap()
                .into_inner()
                .success
        );
        assert!(
            online
                .handle_self_kick_device(user_ctx(
                    kick_device_req("u1", "b1"),
                    "tenant-b",
                    "u1",
                    "physical-b2"
                ))
                .await
                .unwrap()
                .into_inner()
                .success
        );
        let after_ms = chrono::Utc::now().timestamp_millis();
        // u1 在 tenant-b 的另一台设备自己登出。
        let session = repo
            .sessions
            .lock()
            .unwrap()
            .get(&("tenant-b".to_string(), "u1".to_string()))
            .and_then(|list| list.iter().find(|c| c.device_id().as_str() == "b2"))
            .map(|c| c.id().as_str().to_string())
            .unwrap();
        assert!(
            online
                .handle_logout(user_ctx(
                    LogoutRequest {
                        user_id: "u1".to_string(),
                        conversation_id: session,
                    },
                    "tenant-b",
                    "u1",
                    "physical-b2"
                ))
                .await
                .unwrap()
                .into_inner()
                .success
        );
        assert!(repo.devices("tenant-b", "u1").is_empty());

        assert_eq!(
            revocations.targets(),
            vec![
                revocation_target(
                    "tenant-a",
                    "u1",
                    Some("a1"),
                    BindingRevocationReason::Kicked
                ),
                revocation_target(
                    "tenant-b",
                    "u1",
                    Some("b1"),
                    BindingRevocationReason::Kicked
                ),
            ]
        );
        assert!(
            revocations
                .cutoffs()
                .iter()
                .all(|cutoff| (before_ms..=after_ms).contains(cutoff))
        );
    }

    /// 点名踢一台没有在线会话的设备（已托管的手机）：两个入口照旧回「设备不存在」，那台设备的
    /// 推送绑定仍然吊销。
    #[tokio::test]
    async fn kick_rpcs_revoke_the_bindings_of_a_named_device_that_is_not_online() {
        let revocations = Arc::new(RecordedRevocations::default());
        let online = revoking_handler(seeded(), &revocations);

        assert!(
            online
                .handle_kick_device(staff(
                    kick_device_req("u1", "parked-a"),
                    "tenant-a",
                    USERS_KICK
                ))
                .await
                .is_err()
        );
        assert!(
            online
                .handle_self_kick_device(user_ctx(
                    kick_device_req("u1", "parked-b"),
                    "tenant-b",
                    "u1",
                    "physical-b2"
                ))
                .await
                .is_err()
        );

        assert_eq!(
            revocations.targets(),
            vec![
                revocation_target(
                    "tenant-a",
                    "u1",
                    Some("parked-a"),
                    BindingRevocationReason::Kicked
                ),
                revocation_target(
                    "tenant-b",
                    "u1",
                    Some("parked-b"),
                    BindingRevocationReason::Kicked
                ),
            ]
        );
    }

    /// 身份会话吊销：点名会话时吊销被结束的在线会话所属设备的绑定，没点名时一次吊销该用户在
    /// 签名租户下的全部设备。租户取自已验证的上下文，登记时刻的上限是请求的截止时刻。
    #[tokio::test]
    async fn identity_revocation_rpc_revokes_push_bindings_within_the_signed_tenant() {
        use flare_grpc_proto::auth_revoke::RevokeIdentitySessionsRequest;

        let revocations = Arc::new(RecordedRevocations::default());
        let online = revoking_handler(seeded(), &revocations);
        let cutoff_ms = chrono::Utc::now().timestamp_millis();
        let revoke = |tenant: &str, session_ids: Vec<String>| {
            let identity = SignedContextIdentity::new(
                SignedActorType::Service,
                "flare-social-bridge",
                tenant,
                "owned-revocation",
            )
            .with_roles(vec!["identity.revoke".into()]);
            let token = SignedContextSigner::from_private_key(&pem(GATEWAY_KEY).0)
                .unwrap()
                .sign(&identity)
                .unwrap();
            with_ctx(
                RevokeIdentitySessionsRequest {
                    user_id: "u1".into(),
                    revocation_id: uuid::Uuid::new_v4().to_string(),
                    cutoff_ms,
                    session_ids,
                },
                &token,
            )
        };

        // 点名了会话：tenant-b 下 u1 的两条连接都早于截止时刻、没有身份会话 ID，被这次吊销结束。
        online
            .handle_revoke_identity_sessions(revoke(
                "tenant-b",
                vec![uuid::Uuid::new_v4().to_string()],
            ))
            .await
            .unwrap();
        let identity_revoked = |device: &str| {
            revocation_target(
                "tenant-b",
                "u1",
                Some(device),
                BindingRevocationReason::IdentityRevoked,
            )
        };
        assert_eq!(
            revocations.targets(),
            vec![identity_revoked("b1"), identity_revoked("b2")]
        );
        assert_eq!(revocations.cutoffs(), vec![cutoff_ms; 2]);

        // 没点名会话：一次吊销全部设备，只在签名租户内。
        revocations.0.lock().unwrap().clear();
        online
            .handle_revoke_identity_sessions(revoke("tenant-a", Vec::new()))
            .await
            .unwrap();
        assert_eq!(
            revocations.targets(),
            vec![revocation_target(
                "tenant-a",
                "u1",
                None,
                BindingRevocationReason::IdentityRevoked
            )]
        );
        assert_eq!(revocations.cutoffs(), vec![cutoff_ms]);
    }
}
