//! Capability 管理员只读客户端：admin-gateway 专用的可选下游。
//!
//! 只有 admin-gateway 需要读租户能力开关与租户运行时投影；api-gateway 启动时不加载
//! 这份配置，也不会去连 Capability。

use flare_server_core::error::Result;
use tonic::transport::Channel;

use crate::clients::{DownstreamGrpcResolver, admin_bearer_request};
use crate::service_names::CAPABILITY;
use flare_grpc_proto::capability::capability_admin_service_client::CapabilityAdminServiceClient;
use flare_grpc_proto::capability::{
    GetTenantRuntimeRequest, GetTenantRuntimeResponse, ListTenantCapabilitySwitchesRequest,
    ListTenantCapabilitySwitchesResponse,
};

/// Capability 下游路由（`discovery://flare-capability` 或静态 `http(s)://`）。
///
/// 由 [`crate::gateway::capability_downstream_from_env_source`] 只在 admin 作用域加载。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityDownstreamConfig {
    pub service_url: String,
    /// 无注册中心或发现失败时的静态回退 URI；空 = 不回退。
    pub static_fallback: String,
}

#[derive(Clone)]
pub struct CapabilityAdminClientWrapper {
    admin: CapabilityAdminServiceClient<Channel>,
}

impl CapabilityAdminClientWrapper {
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            admin: CapabilityAdminServiceClient::new(channel),
        }
    }

    /// 按配置解析通道；注册中心与静态回退的规则同其他下游。
    pub async fn connect(
        resolver: &DownstreamGrpcResolver,
        config: &CapabilityDownstreamConfig,
    ) -> Result<Self> {
        let channel = resolver
            .connect_route(
                "capability",
                CAPABILITY,
                &config.service_url,
                &config.static_fallback,
            )
            .await?;
        Ok(Self::from_channel(channel))
    }

    /// 令牌租户的全部能力开关（按 capability_id 升序）。
    pub async fn list_tenant_capability_switches(
        &mut self,
        admin_token: &str,
    ) -> Result<ListTenantCapabilitySwitchesResponse> {
        let response = self
            .admin
            .list_tenant_capability_switches(admin_bearer_request(
                ListTenantCapabilitySwitchesRequest {},
                admin_token,
            )?)
            .await?;
        Ok(response.into_inner())
    }

    /// 令牌租户在 Capability 本地持有的投影；未投影时 `projected=false`。
    pub async fn get_tenant_runtime(
        &mut self,
        admin_token: &str,
    ) -> Result<GetTenantRuntimeResponse> {
        let response = self
            .admin
            .get_tenant_runtime(admin_bearer_request(
                GetTenantRuntimeRequest {},
                admin_token,
            )?)
            .await?;
        Ok(response.into_inner())
    }
}
