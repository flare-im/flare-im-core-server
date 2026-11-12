//! Capability 管理员客户端：admin-gateway 专用的可选下游。
//!
//! 只有 admin-gateway 需要读租户能力开关、租户运行时投影、能力目录与插件簿，并经
//! `CapabilityService.Administer` 治理 Hook 配置；api-gateway 启动时不加载这份配置，也不会去
//! 连 Capability。调用原样转发中间件验证过的 Staff `x-flare-ctx`；插件管理的 JSON 调用还可
//! 转发私有上下文中的独立二次验证凭据。Capability 按 RPC 族复核签名里的权限码。

use flare_server_core::error::Result;
use prost::Message;
use tonic::transport::Channel;

use crate::clients::DownstreamGrpcResolver;
use crate::service_names::CAPABILITY;
use crate::staff_context::staff_context_request;
use flare_grpc_proto::capability::capability_admin_service_client::CapabilityAdminServiceClient;
use flare_grpc_proto::capability::capability_service_client::CapabilityServiceClient;
use flare_grpc_proto::capability::{
    GenericRequest, GenericResponse, GetTenantRuntimeRequest, GetTenantRuntimeResponse, ListCapabilitiesRequest,
    ListCapabilitiesResponse, ListRegisteredPluginsRequest, ListRegisteredPluginsResponse,
    ListTenantCapabilitySwitchesRequest, ListTenantCapabilitySwitchesResponse,
    SetTenantCapabilitySwitchRequest, SetTenantCapabilitySwitchResponse,
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

/// `type.googleapis.com/` 前缀：`Administer` 的载荷按 proto 消息全名打包成 `google.protobuf.Any`。
pub const TYPE_URL_PREFIX: &str = "type.googleapis.com/flare.capability.v1.";

/// Capability 管理员客户端：`CapabilityAdminService` 的两个只读方法，外加同一通道上
/// `CapabilityService` 的目录、开关、插件簿与 `Administer`。
#[derive(Clone)]
pub struct CapabilityAdminClientWrapper {
    admin: CapabilityAdminServiceClient<Channel>,
    service: CapabilityServiceClient<Channel>,
}

impl CapabilityAdminClientWrapper {
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            admin: CapabilityAdminServiceClient::new(channel.clone()),
            service: CapabilityServiceClient::new(channel),
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

    /// 令牌租户的全部能力开关（按 capability_id 升序）；`core:capabilities:read`。
    pub async fn list_tenant_capability_switches(
        &mut self,
        staff_token: &str,
    ) -> Result<ListTenantCapabilitySwitchesResponse> {
        let response = self
            .admin
            .list_tenant_capability_switches(staff_context_request(
                ListTenantCapabilitySwitchesRequest {},
                staff_token,
            )?)
            .await?;
        Ok(response.into_inner())
    }

    /// 令牌租户在 Capability 本地持有的投影；未投影时 `projected=false`；`core:tenant:read`。
    pub async fn get_tenant_runtime(
        &mut self,
        staff_token: &str,
    ) -> Result<GetTenantRuntimeResponse> {
        let response = self
            .admin
            .get_tenant_runtime(staff_context_request(
                GetTenantRuntimeRequest {},
                staff_token,
            )?)
            .await?;
        Ok(response.into_inner())
    }

    /// 已登记的能力目录（本部署支持的能力描述符）；`core:capabilities:read`。
    pub async fn list_capabilities(
        &mut self,
        staff_token: &str,
    ) -> Result<ListCapabilitiesResponse> {
        let response = self
            .service
            .list_capabilities(staff_context_request(
                ListCapabilitiesRequest {},
                staff_token,
            )?)
            .await?;
        Ok(response.into_inner())
    }

    /// 设置令牌租户的一个能力开关；`core:capabilities:write`。`tenant_id` 必须是签名里的租户，
    /// Capability 会再核对一次。
    pub async fn set_tenant_capability_switch(
        &mut self,
        staff_token: &str,
        request: SetTenantCapabilitySwitchRequest,
    ) -> Result<SetTenantCapabilitySwitchResponse> {
        let response = self
            .service
            .set_tenant_capability_switch(staff_context_request(request, staff_token)?)
            .await?;
        Ok(response.into_inner())
    }

    /// 令牌租户下已登记的扩展插件实例；`capability_id` 为空表示不过滤；`core:plugins:read`。
    ///
    /// `tenant_id` 必须是已验签 Staff 上下文里的租户：Capability 会再核对一次，不一致即
    /// PERMISSION_DENIED。
    pub async fn list_registered_plugins(
        &mut self,
        staff_token: &str,
        tenant_id: &str,
        capability_id: &str,
    ) -> Result<ListRegisteredPluginsResponse> {
        let response = self
            .service
            .list_registered_plugins(staff_context_request(
                ListRegisteredPluginsRequest {
                    tenant_id: tenant_id.to_string(),
                    capability_id: capability_id.to_string(),
                },
                staff_token,
            )?)
            .await?;
        Ok(response.into_inner())
    }

    /// 一次 Hook 治理操作：`operation` 是 `flare.extension.v1.hook_*`，`payload` 是对应请求消息
    /// （打包成 `Any`，类型名 `flare.capability.v1.<Request>`）。读操作要求 `core:hooks:read`，写操作
    /// 要求 `core:hooks:write` 或 `core:hooks:platform:write`，由 Capability 按 operation 和范围复核。
    pub async fn administer<P: Message>(
        &mut self,
        staff_token: &str,
        operation: &str,
        request_type: &str,
        payload: &P,
        request_id: &str,
    ) -> Result<GenericResponse> {
        let request = GenericRequest {
            operation: operation.to_string(),
            metadata: Default::default(),
            payload: Some(prost_types::Any {
                type_url: format!("{TYPE_URL_PREFIX}{request_type}"),
                value: payload.encode_to_vec(),
            }),
            request_id: request_id.to_string(),
        };
        let response = self
            .service
            .administer(staff_context_request(request, staff_token)?)
            .await?;
        Ok(response.into_inner())
    }

    /// `CapabilityService.Administer` with a `PayloadJson` body (plugin admin pages:
    /// `flare.plugin.v1.admin.query` / `.dispatch`), forwarding the verified Staff token and
    /// its optional opaque reauthentication credential as separate sensitive metadata.
    pub async fn administer_json(
        &mut self,
        staff: &crate::staff_context::VerifiedStaff,
        operation: &str,
        payload_json: Vec<u8>,
        request_id: &str,
    ) -> Result<GenericResponse> {
        let request = GenericRequest {
            operation: operation.to_string(),
            metadata: Default::default(),
            payload: Some(prost_types::Any {
                type_url: format!("{TYPE_URL_PREFIX}PayloadJson"),
                value: payload_json,
            }),
            request_id: request_id.to_string(),
        };
        let response = self
            .service
            .administer(crate::staff_context::staff_reauthenticated_request(
                request, staff,
            )?)
            .await?;
        Ok(response.into_inner())
    }
}
