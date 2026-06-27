use flare_server_core::error::Result;
use tonic::transport::Channel;

use crate::clients::Ctx;
use flare_grpc_proto::signaling::online::online_service_client::OnlineServiceClient;
use flare_grpc_proto::signaling::online::{
    BatchGetUserPresenceRequest, BatchGetUserPresenceResponse, GetDeviceRequest, GetDeviceResponse,
    GetUserPresenceRequest, GetUserPresenceResponse, KickDeviceRequest, KickDeviceResponse,
    ListUserDevicesRequest, ListUserDevicesResponse, LogoutRequest, LogoutResponse,
};

#[derive(Clone)]
pub struct OnlineServiceClientWrapper {
    client: OnlineServiceClient<Channel>,
}

impl OnlineServiceClientWrapper {
    fn request_with_ctx<T>(ctx: &Ctx, payload: T) -> tonic::Request<T> {
        flare_server_core::request_with_context(payload, ctx)
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            client: OnlineServiceClient::new(channel),
        }
    }

    pub async fn get_user_presence_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: GetUserPresenceRequest,
    ) -> Result<GetUserPresenceResponse> {
        let response = self
            .client
            .get_user_presence(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn batch_get_user_presence_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: BatchGetUserPresenceRequest,
    ) -> Result<BatchGetUserPresenceResponse> {
        let response = self
            .client
            .batch_get_user_presence(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn list_user_devices_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: ListUserDevicesRequest,
    ) -> Result<ListUserDevicesResponse> {
        let response = self
            .client
            .list_user_devices(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn get_device_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: GetDeviceRequest,
    ) -> Result<GetDeviceResponse> {
        let response = self
            .client
            .get_device(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn kick_device_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: KickDeviceRequest,
    ) -> Result<KickDeviceResponse> {
        let response = self
            .client
            .kick_device(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn logout_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: LogoutRequest,
    ) -> Result<LogoutResponse> {
        let response = self
            .client
            .logout(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }
}
