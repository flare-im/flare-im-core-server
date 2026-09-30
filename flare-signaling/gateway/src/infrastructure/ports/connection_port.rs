//! [`IConnectionPort`] 基础设施实现（与 `domain/ports/connection_port.rs` 对应）
//!
//! - RPC（login/logout/heartbeat/get_online_status）：服务发现连接 Signaling Online。
//! - 本地连接信息：从 `ConnectionManager` 读取并映射为领域 `ConnectionInfo`。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use flare_core::server::connection::{ConnectionManagerTrait, TraitConnectionInfo};
use flare_grpc_proto::signaling::online::online_service_client::OnlineServiceClient;
use flare_grpc_proto::signaling::online::{
    GetOnlineStatusRequest, GetOnlineStatusResponse, HeartbeatRequest, HeartbeatResponse,
    LoginRequest, LoginResponse, LogoutRequest, LogoutResponse,
};
use flare_im_contracts::service_names::{SIGNALING_ONLINE, get_service_name};
use flare_im_contracts::utils::normalize_tenant_id;
use flare_im_service_kit::ServiceClient;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, set_signed_context_token,
};
use tokio::sync::Mutex;
use tonic::transport::Channel;

use flare_im_contracts::Ctx;

use crate::constants::{AUTH_FAILURE_MSG_TENANT_UNAVAILABLE, METADATA_KEY_TENANT_ID};
use crate::domain::model::ConnectionInfo as DomainConnectionInfo;
use crate::domain::ports::IConnectionPort;
use crate::infrastructure::connection_context::AuthenticatedConnectionOrigin;

pub struct ConnectionRepository {
    connection_manager: Arc<dyn ConnectionManagerTrait>,
    service_name: String,
    service_client: Mutex<Option<ServiceClient>>,
    client: Mutex<Option<OnlineServiceClient<Channel>>>,
    default_tenant_id: String,
    signer: Option<SignedContextSigner>,
    static_registry_mode: bool,
}

fn static_online_fallback(service_name: &str, static_registry_mode: bool) -> Option<&'static str> {
    (static_registry_mode && service_name == SIGNALING_ONLINE)
        .then(|| flare_im_service_kit::discovery::default_static_grpc_fallback(SIGNALING_ONLINE))
}

impl ConnectionRepository {
    pub fn new(
        connection_manager: Arc<dyn ConnectionManagerTrait>,
        default_tenant_id: String,
    ) -> Self {
        let service_name = get_service_name(SIGNALING_ONLINE);
        Self {
            connection_manager,
            service_name,
            service_client: Mutex::new(None),
            client: Mutex::new(None),
            default_tenant_id,
            signer: None,
            static_registry_mode: false,
        }
    }

    pub fn with_service_client(
        connection_manager: Arc<dyn ConnectionManagerTrait>,
        default_tenant_id: String,
        service_client: ServiceClient,
    ) -> Self {
        let service_name = get_service_name(SIGNALING_ONLINE);
        Self {
            connection_manager,
            service_name,
            service_client: Mutex::new(Some(service_client)),
            client: Mutex::new(None),
            default_tenant_id,
            signer: None,
            static_registry_mode: false,
        }
    }

    pub fn with_static_registry_mode(mut self, enabled: bool) -> Self {
        self.static_registry_mode = enabled;
        self
    }

    pub fn with_signer(mut self, signer: Option<SignedContextSigner>) -> Self {
        self.signer = signer;
        self
    }

    fn signed_request<T>(
        &self,
        ctx: &Ctx,
        payload: T,
        expected_user_id: &str,
    ) -> Result<tonic::Request<T>> {
        let origin = ctx
            .get_data::<AuthenticatedConnectionOrigin>()
            .ok_or_else(|| {
                ErrorBuilder::new(
                    ErrorCode::PermissionDenied,
                    "authenticated online connection required",
                )
                .build_error()
            })?;
        if origin.user_id() != expected_user_id
            || ctx.user_id() != Some(expected_user_id)
            || ctx.tenant_id() != Some(origin.tenant_id())
        {
            return Err(ErrorBuilder::new(
                ErrorCode::PermissionDenied,
                "online connection identity mismatch",
            )
            .build_error());
        }
        let signer = self.signer.as_ref().ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::ServiceUnavailable,
                "online context signer not configured",
            )
            .build_error()
        })?;
        let identity = SignedContextIdentity::new(
            SignedActorType::User,
            origin.user_id(),
            origin.tenant_id(),
            origin.connection_id(),
        )
        .with_credential_id("signaling-gateway");
        let token = signer.sign(&identity).map_err(|_| {
            ErrorBuilder::new(
                ErrorCode::PermissionDenied,
                "unable to sign online identity",
            )
            .build_error()
        })?;
        let mut request = tonic::Request::new(payload);
        set_signed_context_token(&mut request, &token).map_err(|_| {
            ErrorBuilder::new(
                ErrorCode::PermissionDenied,
                "unable to encode online identity",
            )
            .build_error()
        })?;
        Ok(request)
    }

    async fn ensure_client(&self) -> Result<OnlineServiceClient<Channel>> {
        let mut guard = self.client.lock().await;
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }

        let mut service_client_guard = self.service_client.lock().await;
        if service_client_guard.is_none() {
            let discover = flare_im_service_kit::discovery::create_discover(&self.service_name)
                .await
                .map_err(|e| {
                    ErrorBuilder::new(
                        ErrorCode::ServiceUnavailable,
                        "signaling online service unavailable",
                    )
                    .details(format!(
                        "Failed to create service discover for {}: {}",
                        self.service_name, e
                    ))
                    .build_error()
                })?;

            if let Some(discover) = discover {
                *service_client_guard = Some(ServiceClient::new(discover));
            } else {
                let Some(uri) =
                    static_online_fallback(&self.service_name, self.static_registry_mode)
                else {
                    return Err(flare_server_core::error::FlareError::system(
                        "Service discovery not configured; explicit static Signaling Online route unavailable",
                    ));
                };
                drop(service_client_guard);
                let channel = flare_im_service_kit::discovery::connect_grpc_channel_with_fallback(
                    &self.service_name,
                    uri,
                )
                .await
                .map_err(|e| {
                    ErrorBuilder::new(
                        ErrorCode::ServiceUnavailable,
                        "signaling online unavailable",
                    )
                    .details(format!("Failed to connect to static Signaling Online: {e}"))
                    .build_error()
                })?;
                let client = OnlineServiceClient::new(channel);
                *guard = Some(client.clone());
                return Ok(client);
            }
        }

        let service_client = service_client_guard.as_mut().ok_or_else(|| {
            flare_server_core::error::FlareError::system(
                "Service client not initialized".to_string(),
            )
        })?;
        let channel = flare_im_service_kit::discovery::get_discovered_channel_with_timeout(
            &self.service_name,
            service_client,
        )
        .await
        .map_err(|e| {
            ErrorBuilder::new(
                ErrorCode::ServiceUnavailable,
                "signaling online unavailable",
            )
            .details(format!("Failed to get channel: {}", e))
            .build_error()
        })?;

        tracing::trace!(
            service_name = %self.service_name,
            "ConnectionRepository: got channel from service discovery"
        );

        let client = OnlineServiceClient::new(channel);
        *guard = Some(client.clone());
        Ok(client)
    }
}

#[async_trait]
impl IConnectionPort for ConnectionRepository {
    async fn login(&self, ctx: &Ctx, request: LoginRequest) -> Result<LoginResponse> {
        let origin = ctx
            .get_data::<AuthenticatedConnectionOrigin>()
            .ok_or_else(|| {
                ErrorBuilder::new(
                    ErrorCode::PermissionDenied,
                    "authenticated connection required",
                )
                .build_error()
            })?;
        if request.metadata.get("connection_id").map(String::as_str) != Some(origin.connection_id())
        {
            return Err(ErrorBuilder::new(
                ErrorCode::PermissionDenied,
                "online login connection mismatch",
            )
            .build_error());
        }
        let request = self.signed_request(ctx, request.clone(), &request.user_id)?;
        let mut client = self.ensure_client().await?;
        client
            .login(request)
            .await
            .map(|resp| resp.into_inner())
            .map_err(online_login_error)
    }

    async fn logout(&self, ctx: &Ctx, request: LogoutRequest) -> Result<LogoutResponse> {
        let request = self.signed_request(ctx, request.clone(), &request.user_id)?;
        let mut client = self.ensure_client().await?;
        client
            .logout(request)
            .await
            .map(|resp| resp.into_inner())
            .map_err(|status| {
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "signaling logout failed")
                    .details(status.to_string())
                    .build_error()
            })
    }

    async fn heartbeat(&self, ctx: &Ctx, request: HeartbeatRequest) -> Result<HeartbeatResponse> {
        let request = self.signed_request(ctx, request.clone(), &request.user_id)?;
        let mut client = self.ensure_client().await?;
        client
            .heartbeat(request)
            .await
            .map(|resp| resp.into_inner())
            .map_err(|status| {
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "signaling heartbeat failed")
                    .details(status.to_string())
                    .build_error()
            })
    }

    async fn get_online_status(
        &self,
        ctx: &Ctx,
        request: GetOnlineStatusRequest,
    ) -> Result<GetOnlineStatusResponse> {
        let user_id = ctx.user_id().ok_or_else(|| {
            ErrorBuilder::new(ErrorCode::PermissionDenied, "online user required").build_error()
        })?;
        let request = self.signed_request(ctx, request, user_id)?;
        let mut client = self.ensure_client().await?;
        client
            .get_online_status(request)
            .await
            .map(|resp| resp.into_inner())
            .map_err(|status| {
                ErrorBuilder::new(
                    ErrorCode::ServiceUnavailable,
                    "signaling get_online_status failed",
                )
                .details(status.to_string())
                .build_error()
            })
    }

    async fn list_user_connections(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Vec<DomainConnectionInfo>> {
        let connection_ids = self.connection_manager.get_user_connections(user_id).await;
        let mut list = Vec::with_capacity(connection_ids.len());
        for connection_id in connection_ids {
            if let Some((_, core_info)) =
                self.connection_manager.get_connection(&connection_id).await
            {
                if !core_info.authenticated
                    || core_info.user_id.as_deref() != Some(user_id)
                    || core_info
                        .metadata
                        .get(METADATA_KEY_TENANT_ID)
                        .map(String::as_str)
                        != Some(tenant_id)
                {
                    continue;
                }
                list.push(core_info_to_domain(
                    connection_id,
                    &core_info,
                    &self.default_tenant_id,
                ));
            }
        }
        Ok(list)
    }

    async fn get_connection_info(&self, connection_id: &str) -> Result<DomainConnectionInfo> {
        let (_, core_info) = self
            .connection_manager
            .get_connection(connection_id)
            .await
            .ok_or_else(|| {
                ErrorBuilder::new(ErrorCode::InvalidParameter, "connection not found")
                    .details(format!("connection_id={}", connection_id))
                    .build_error()
            })?;
        Ok(core_info_to_domain(
            connection_id.to_string(),
            &core_info,
            &self.default_tenant_id,
        ))
    }

    async fn get_connection_metadata(
        &self,
        connection_id: &str,
    ) -> Result<HashMap<String, String>> {
        let (_, core_info) = self
            .connection_manager
            .get_connection(connection_id)
            .await
            .ok_or_else(|| {
                ErrorBuilder::new(ErrorCode::InvalidParameter, "connection not found")
                    .details(format!("connection_id={}", connection_id))
                    .build_error()
            })?;
        Ok(core_info.metadata)
    }

    async fn build_ctx(&self, connection_id: &str) -> Result<Ctx> {
        let (_, core_info) = self
            .connection_manager
            .get_connection(connection_id)
            .await
            .ok_or_else(|| {
                ErrorBuilder::new(ErrorCode::InvalidParameter, "connection not found").build_error()
            })?;
        let user_id = core_info.user_id.as_deref().unwrap_or_default();
        let metadata_user = core_info.metadata.get("user_id").map(String::as_str);
        let tenant_id = core_info
            .metadata
            .get(METADATA_KEY_TENANT_ID)
            .map(String::as_str);
        let tenant_id = match tenant_id {
            Some(tenant)
                if core_info.authenticated
                    && !user_id.is_empty()
                    && metadata_user == Some(user_id)
                    && !tenant.is_empty()
                    && normalize_tenant_id(tenant) == tenant =>
            {
                tenant
            }
            _ => {
                return Err(ErrorBuilder::new(
                    ErrorCode::PermissionDenied,
                    "authenticated connection required",
                )
                .build_error());
            }
        };
        let connection_info = core_info_to_domain(
            connection_id.to_string(),
            &core_info,
            &self.default_tenant_id,
        );
        let ctx = super::context_resolver::build_gateway_ctx_from_info(
            &connection_info,
            &self.default_tenant_id,
        );
        Ok(Arc::new((*ctx).clone().insert_data(
            crate::infrastructure::connection_context::AuthenticatedConnectionOrigin::new(
                connection_id.to_owned(),
                user_id.to_owned(),
                tenant_id.to_owned(),
            ),
        )))
    }
}

/// 将 flare_core trait 侧 `ConnectionInfo`（[`TraitConnectionInfo`]）转为领域 [`DomainConnectionInfo`]
pub(crate) fn core_info_to_domain(
    connection_id: String,
    core_info: &TraitConnectionInfo,
    default_tenant_id: &str,
) -> DomainConnectionInfo {
    let user_id = core_info.user_id.clone().unwrap_or_default();
    let device_id = core_info
        .device_info
        .as_ref()
        .map(|d| d.device_id.clone())
        .unwrap_or_else(|| "unknown".to_string());
    let platform = core_info
        .device_info
        .as_ref()
        .map(|d| d.platform.as_str().to_string());
    let mut metadata = core_info.metadata.clone();
    if let Some(device_info) = core_info.device_info.as_ref() {
        if let Some(app_version) = device_info.app_version.as_ref()
            && !app_version.trim().is_empty()
        {
            metadata.insert("app_version".to_string(), app_version.clone());
        }
        if let Some(system_version) = device_info.system_version.as_ref()
            && !system_version.trim().is_empty()
        {
            metadata.insert("system_version".to_string(), system_version.clone());
        }
        if let Some(model) = device_info.model.as_ref()
            && !model.trim().is_empty()
        {
            metadata.insert("model".to_string(), model.clone());
        }
    }
    let tenant_id = core_info
        .metadata
        .get(METADATA_KEY_TENANT_ID)
        .map(normalize_tenant_id)
        .unwrap_or_else(|| normalize_tenant_id(default_tenant_id));
    let mut info = DomainConnectionInfo::new(connection_id, user_id, tenant_id, device_id);
    if let Some(p) = platform {
        info = info.with_platform(p);
    }
    info.with_metadata(metadata)
}

/// Online 登录失败的网关侧错误。租户闸门的拒绝（PERMISSION_DENIED，`TENANT_UNAVAILABLE`）保留为
/// 类型化的权限拒绝，一路交给 flare-core，由它在 CONNECT_ACK 之后给客户端下发同一个原因；其余失败
/// 仍按「Online 不可用」处理，状态文本只进 details，不会下发给客户端。
fn online_login_error(status: tonic::Status) -> flare_server_core::error::FlareError {
    if status.code() == tonic::Code::PermissionDenied
        && status.message() == AUTH_FAILURE_MSG_TENANT_UNAVAILABLE
    {
        return ErrorBuilder::new(
            ErrorCode::PermissionDenied,
            AUTH_FAILURE_MSG_TENANT_UNAVAILABLE,
        )
        .build_error();
    }
    ErrorBuilder::new(ErrorCode::ServiceUnavailable, "signaling login failed")
        .details(status.to_string())
        .build_error()
}

#[cfg(test)]
mod static_route_tests {
    use super::*;

    /// Online 的租户拒绝保留为 PERMISSION_DENIED `TENANT_UNAVAILABLE`；别的权限拒绝和不可用
    /// 仍是「Online 不可用」，原文只进 details。
    #[test]
    fn online_tenant_rejection_stays_typed() {
        let tenant = online_login_error(tonic::Status::permission_denied(
            AUTH_FAILURE_MSG_TENANT_UNAVAILABLE,
        ));
        assert_eq!(tenant.code(), Some(ErrorCode::PermissionDenied));
        assert_eq!(tenant.reason(), AUTH_FAILURE_MSG_TENANT_UNAVAILABLE);

        for status in [
            tonic::Status::permission_denied("online login tenant mismatch"),
            tonic::Status::unavailable("connection refused"),
            tonic::Status::internal(AUTH_FAILURE_MSG_TENANT_UNAVAILABLE),
            // Online 读不到租户投影时的登录失败：暂时故障，客户端应当重连
            tonic::Status::internal("Failed to login"),
        ] {
            let error = online_login_error(status);
            assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
            assert_eq!(error.reason(), "signaling login failed");
        }
    }

    #[test]
    fn static_online_route_requires_explicit_mode_and_exact_service_name() {
        assert_eq!(
            static_online_fallback(SIGNALING_ONLINE, true),
            Some("http://127.0.0.1:50061")
        );
        assert_eq!(static_online_fallback(SIGNALING_ONLINE, false), None);
        assert_eq!(
            static_online_fallback("renamed-signaling-online", true),
            None
        );
    }
}
