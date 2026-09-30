use flare_server_core::error::Result;
use tonic::transport::Channel;

use crate::clients::{Ctx, admin_bearer_request};
use flare_grpc_proto::signaling::online::online_admin_service_client::OnlineAdminServiceClient;
use flare_grpc_proto::signaling::online::online_service_client::OnlineServiceClient;
use flare_grpc_proto::signaling::online::{
    AdminGetUserPresenceRequest, AdminListUserDevicesRequest, BatchGetUserPresenceRequest,
    BatchGetUserPresenceResponse, GetDeviceRequest, GetDeviceResponse, GetUserPresenceRequest,
    GetUserPresenceResponse, KickDeviceRequest, KickDeviceResponse, KickTenantRequest,
    KickTenantResponse, ListUserDevicesRequest, ListUserDevicesResponse, LogoutRequest,
    LogoutResponse,
};

/// Online 客户端。用户态方法带 Cell 签名上下文；管理员方法（`kick_*`、`admin_*`）只带
/// admin-gateway 原样转发的 admin 令牌，租户由 Online 从令牌里取。
#[derive(Clone)]
pub struct OnlineServiceClientWrapper {
    client: OnlineServiceClient<Channel>,
    admin: OnlineAdminServiceClient<Channel>,
}

impl OnlineServiceClientWrapper {
    fn request_with_ctx<T>(ctx: &Ctx, payload: T, token: &str) -> Result<tonic::Request<T>> {
        let mut request = flare_server_core::request_with_context(payload, ctx);
        flare_server_core::set_signed_context_token(&mut request, token)?;
        Ok(request)
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            client: OnlineServiceClient::new(channel.clone()),
            admin: OnlineAdminServiceClient::new(channel),
        }
    }

    /// 管理员踢设备：Online 要求员工主体且设备会话属于令牌租户。
    pub async fn kick_device(
        &mut self,
        admin_token: &str,
        request: KickDeviceRequest,
    ) -> Result<KickDeviceResponse> {
        let response = self
            .client
            .kick_device(admin_bearer_request(request, admin_token)?)
            .await?;
        Ok(response.into_inner())
    }

    /// 管理员踢出整个租户：请求里的 `tenant_id` 必须等于令牌租户。
    pub async fn kick_tenant(
        &mut self,
        admin_token: &str,
        request: KickTenantRequest,
    ) -> Result<KickTenantResponse> {
        let response = self
            .client
            .kick_tenant(admin_bearer_request(request, admin_token)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn admin_list_user_devices(
        &mut self,
        admin_token: &str,
        request: AdminListUserDevicesRequest,
    ) -> Result<ListUserDevicesResponse> {
        let response = self
            .admin
            .admin_list_user_devices(admin_bearer_request(request, admin_token)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn admin_get_user_presence(
        &mut self,
        admin_token: &str,
        request: AdminGetUserPresenceRequest,
    ) -> Result<GetUserPresenceResponse> {
        let response = self
            .admin
            .admin_get_user_presence(admin_bearer_request(request, admin_token)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn get_user_presence_with_ctx(
        &mut self,
        ctx: &Ctx,
        token: &str,
        request: GetUserPresenceRequest,
    ) -> Result<GetUserPresenceResponse> {
        let response = self
            .client
            .get_user_presence(Self::request_with_ctx(ctx, request, token)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn batch_get_user_presence_with_ctx(
        &mut self,
        ctx: &Ctx,
        token: &str,
        request: BatchGetUserPresenceRequest,
    ) -> Result<BatchGetUserPresenceResponse> {
        let response = self
            .client
            .batch_get_user_presence(Self::request_with_ctx(ctx, request, token)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn list_user_devices_with_ctx(
        &mut self,
        ctx: &Ctx,
        token: &str,
        request: ListUserDevicesRequest,
    ) -> Result<ListUserDevicesResponse> {
        let response = self
            .client
            .list_user_devices(Self::request_with_ctx(ctx, request, token)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn get_device_with_ctx(
        &mut self,
        ctx: &Ctx,
        token: &str,
        request: GetDeviceRequest,
    ) -> Result<GetDeviceResponse> {
        let response = self
            .client
            .get_device(Self::request_with_ctx(ctx, request, token)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn self_kick_device_with_ctx(
        &mut self,
        ctx: &Ctx,
        token: &str,
        request: KickDeviceRequest,
    ) -> Result<KickDeviceResponse> {
        let response = self
            .client
            .self_kick_device(Self::request_with_ctx(ctx, request, token)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn logout_with_ctx(
        &mut self,
        ctx: &Ctx,
        token: &str,
        request: LogoutRequest,
    ) -> Result<LogoutResponse> {
        let response = self
            .client
            .logout(Self::request_with_ctx(ctx, request, token)?)
            .await?;
        Ok(response.into_inner())
    }
}
