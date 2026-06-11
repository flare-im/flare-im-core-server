//! Online 服务 gRPC 客户端（ListUserDevices，用于按网关选端推送）。
//!
//! 本模块提供基于 tonic 的 Online 服务客户端实现。

use flare_grpc_proto::signaling::online::{
    ListUserDevicesRequest, ListUserDevicesResponse,
    online_service_client::OnlineServiceClient as ProtoOnlineServiceClient,
};
use flare_server_core::context::{Context, ContextExt};
use flare_server_core::error::{FlareError, Result};
use tonic::transport::Channel;
use tracing::instrument;

/// Online 服务客户端（基于 tonic）
///
/// 用于查询用户设备列表，支持按网关选端推送。
pub struct OnlineServiceClient {
    client: ProtoOnlineServiceClient<Channel>,
}

impl OnlineServiceClient {
    /// 直连 URI（测试或无注册中心）。
    pub async fn new(endpoint: String) -> Result<Self> {
        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|err| {
                FlareError::system(format!("invalid online grpc uri {endpoint}: {err}"))
            })?
            .connect()
            .await
            .map_err(|err| FlareError::system(format!("connect online grpc {endpoint}: {err}")))?;
        Ok(Self::from_channel(channel))
    }

    /// 由 [`flare_im_service_kit::discovery::connect_grpc_channel_from_app_config`] 等得到的共享 Channel。
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            client: ProtoOnlineServiceClient::new(channel),
        }
    }

    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        user_id = %user_id,
    ))]
    pub async fn list_user_devices(
        &self,
        ctx: &Context,
        user_id: &str,
    ) -> Result<ListUserDevicesResponse> {
        ctx.ensure_not_cancelled().map_err(|e| {
            flare_server_core::error::FlareError::system(format!("Request cancelled: {}", e))
        })?;
        let request = tonic::Request::new(ListUserDevicesRequest {
            user_id: user_id.to_string(),
        });
        let response = self.client.clone().list_user_devices(request).await?;
        Ok(response.into_inner())
    }
}
