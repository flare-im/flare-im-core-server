//! Online 服务 gRPC 客户端
//!
//! Gateway 通过此客户端调用 Online 服务的 OnlineService 接口查询设备信息

use anyhow::Result;
use flare_proto::signaling::online::{
    GetDeviceRequest, GetDeviceResponse, ListUserDevicesRequest, ListUserDevicesResponse,
    online_service_client::OnlineServiceClient as ProtoOnlineServiceClient,
};
use flare_proto::common::{RequestContext, TenantContext};
use flare_server_core::context::{Context, ContextExt};
use tonic::transport::Channel;
use tracing::instrument;

pub struct OnlineServiceClient {
    client: ProtoOnlineServiceClient<Channel>,
}

impl OnlineServiceClient {
    pub async fn new(endpoint: String) -> Result<Self> {
        let channel = Channel::from_shared(endpoint)?.connect().await?;

        let client = ProtoOnlineServiceClient::new(channel);

        Ok(Self { client })
    }

    /// 查询用户的所有在线设备
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        user_id = %user_id,
    ))]
    pub async fn list_user_devices(&self, ctx: &Context, user_id: &str) -> Result<ListUserDevicesResponse> {
        ctx.ensure_not_cancelled().map_err(|e| {
            anyhow::anyhow!("Request cancelled: {}", e)
        })?;
        let mut client = self.client.clone();

        // 从 Context 中提取 RequestContext 和 TenantContext（用于 protobuf 兼容性）
        let request_context: flare_proto::common::RequestContext = ctx.request()
            .cloned()
            .map(|req_ctx| req_ctx.into())
            .unwrap_or_else(|| {
                let request_id = if ctx.request_id().is_empty() {
                    uuid::Uuid::new_v4().to_string()
                } else {
                    ctx.request_id().to_string()
                };
                flare_proto::common::RequestContext {
                    request_id,
                    trace: None,
                    actor: None,
                    device: None,
                    channel: String::new(),
                    user_agent: String::new(),
                    attributes: std::collections::HashMap::new(),
                }
            });

        let tenant: flare_proto::common::TenantContext = ctx.tenant()
            .cloned()
            .map(|t| t.into())
            .or_else(|| {
                ctx.tenant_id().map(|tenant_id| {
                    let tenant: flare_server_core::context::TenantContext = 
                        flare_server_core::context::TenantContext::new(tenant_id);
                    tenant.into()
                })
            })
            .unwrap_or_else(|| {
                flare_proto::common::TenantContext::default()
            });

        let request = tonic::Request::new(ListUserDevicesRequest {
            user_id: user_id.to_string(),
        });

        let response = client.list_user_devices(request).await?;
        Ok(response.into_inner())
    }

    /// 获取设备信息
    pub async fn get_device(&self, ctx: &Context, user_id: &str, device_id: &str) -> Result<GetDeviceResponse> {
        let mut client = self.client.clone();

        // 从 Context 中提取 RequestContext 和 TenantContext（用于 protobuf 兼容性）
        let request_context: flare_proto::common::RequestContext = ctx.request()
            .cloned()
            .map(|req_ctx| req_ctx.into())
            .unwrap_or_else(|| {
                let request_id = if ctx.request_id().is_empty() {
                    uuid::Uuid::new_v4().to_string()
                } else {
                    ctx.request_id().to_string()
                };
                flare_proto::common::RequestContext {
                    request_id,
                    trace: None,
                    actor: None,
                    device: None,
                    channel: String::new(),
                    user_agent: String::new(),
                    attributes: std::collections::HashMap::new(),
                }
            });

        let tenant: flare_proto::common::TenantContext = ctx.tenant()
            .cloned()
            .map(|t| t.into())
            .or_else(|| {
                ctx.tenant_id().map(|tenant_id| {
                    let tenant: flare_server_core::context::TenantContext = 
                        flare_server_core::context::TenantContext::new(tenant_id);
                    tenant.into()
                })
            })
            .unwrap_or_else(|| {
                flare_proto::common::TenantContext::default()
            });

        let request = tonic::Request::new(GetDeviceRequest {
            user_id: user_id.to_string(),
            device_id: device_id.to_string(),
        });

        let response = client.get_device(request).await?;
        Ok(response.into_inner())
    }

    /// 获取底层的 OnlineServiceClient
    pub fn get_online_service_client(&self) -> ProtoOnlineServiceClient<Channel> {
        self.client.clone()
    }
}
