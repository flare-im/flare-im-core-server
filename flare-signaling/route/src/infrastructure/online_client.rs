//! Online 服务 gRPC 客户端
//!
//! Router 通过此客户端调用 Online 服务的 OnlineService 接口查询设备信息

use anyhow::Result;
use flare_proto::signaling::online::{
    ListUserDevicesRequest, ListUserDevicesResponse,
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
        let _request_context: RequestContext = ctx.request().cloned().map(|rc| rc.into()).unwrap_or_else(|| RequestContext {
            request_id: ctx.request_id().to_string(),
            trace: None,
            actor: None,
            device: None,
            channel: String::new(),
            user_agent: String::new(),
            attributes: std::collections::HashMap::new(),
        });

        let _tenant: TenantContext = ctx.tenant().cloned().map(|tc| tc.into()).or_else(|| {
            ctx.tenant_id().map(|tenant_id| TenantContext {
                tenant_id: tenant_id.to_string(),
                business_type: String::new(),
                environment: String::new(),
                organization_id: String::new(),
                labels: std::collections::HashMap::new(),
                attributes: std::collections::HashMap::new(),
            })
        }).unwrap_or_else(|| TenantContext::default());

        let request = tonic::Request::new(ListUserDevicesRequest {
            user_id: user_id.to_string(),
        });

        let response = client.list_user_devices(request).await?;
        Ok(response.into_inner())
    }
}
