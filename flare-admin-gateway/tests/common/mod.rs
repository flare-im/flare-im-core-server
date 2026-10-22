//! Admin Gateway 契约测试的公共夹具：网关 admin 分区签的 Staff `x-flare-ctx`，以及只监听
//! 127.0.0.1 的替身 tonic 服务（Conversation / Online / Capability 的管理员接口）。
//!
//! 替身按输入给出固定回答，并记下每次调用收到的 `x-flare-ctx`、`authorization` 与所有 `x-*`
//! 元数据，用来断言网关只原样转发已验过的 Staff 上下文、不带任何未签名的身份头。
//!
//! 替身也像真实服务一样把关：记下调用后，用网关同一份 Cell 公钥集（Staff 钉死 `kid=gateway`）
//! 验转发来的上下文，并按真实服务每个 RPC 认的权限码集合复核（例如 Online 的
//! `AdminListUserDevices` 认 `core:users:read` 与 `core:users:kick`，`AdminGetUserPresence` 只认
//! `core:users:read`），码不对就 PERMISSION_DENIED；请求里带租户的，租户须等于签名租户。网关
//! 转发的码与下游认的码对不上，契约测试就会失败。
//!
//! 媒资路由的替身 Media 与夹具在 [`media`]。
#![allow(dead_code)]

pub mod media;

use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_admin_gateway::interface::http::{AdminCapabilityDownstream, create_admin_router};
use flare_grpc_proto::capability::capability_admin_service_server::{
    CapabilityAdminService, CapabilityAdminServiceServer,
};
use flare_grpc_proto::capability::capability_service_server::{
    CapabilityService, CapabilityServiceServer,
};
use flare_grpc_proto::capability::{
    CapabilityDescriptor, CreateHookConfigRequest, CreateHookConfigResponse,
    DeleteHookConfigRequest, DeleteHookConfigResponse, DeregisterPluginEndpointRequest,
    DeregisterPluginEndpointResponse, DispatchCapabilityRequest, DispatchCapabilityResponse,
    GenericRequest, GenericResponse, GetHookConfigRequest, GetHookConfigResponse,
    GetHookStatisticsRequest, GetHookStatisticsResponse, GetTenantRuntimeRequest,
    GetTenantRuntimeResponse, GrantUserCapabilityRequest, GrantUserCapabilityResponse, HookConfig,
    HookExecution, HookPoint, HookStatistics, HookTransport, ListCapabilitiesRequest,
    ListCapabilitiesResponse, ListHookConfigsRequest, ListHookConfigsResponse,
    ListRegisteredPluginsRequest, ListRegisteredPluginsResponse,
    ListTenantCapabilitySwitchesRequest, ListTenantCapabilitySwitchesResponse,
    ListUserCapabilitiesRequest, ListUserCapabilitiesResponse, QueryHookExecutionsRequest,
    QueryHookExecutionsResponse, RegisterPluginEndpointRequest, RegisterPluginEndpointResponse,
    RegisteredPluginInstance, RevokeUserCapabilityRequest, RevokeUserCapabilityResponse,
    SetHookStatusRequest, SetHookStatusResponse, SetTenantCapabilitySwitchRequest,
    SetTenantCapabilitySwitchResponse, TenantCapabilitySwitch, UpdateHookConfigRequest,
    UpdateHookConfigResponse,
};
use flare_grpc_proto::control::{CoreQuota, TenantQuota, TenantStatus};
use flare_grpc_proto::conversation::conversation_admin_service_server::{
    ConversationAdminService, ConversationAdminServiceServer,
};
use flare_grpc_proto::conversation::{
    AdminGetConversationRequest, AdminGetConversationResponse, AdminListConversationsRequest,
    AdminListConversationsResponse, AdminListParticipantsRequest, AdminListParticipantsResponse,
};
use flare_grpc_proto::signaling::online::online_admin_service_server::{
    OnlineAdminService, OnlineAdminServiceServer,
};
use flare_grpc_proto::signaling::online::online_service_server::{
    OnlineService, OnlineServiceServer,
};
use flare_grpc_proto::signaling::online::*;
use flare_im_service_kit::audit_effects::AuditEffects;
use flare_im_service_kit::clients::{
    AdminGrpcClients, CapabilityAdminClientWrapper, CapabilityDownstreamConfig,
    ConversationReadServiceClientWrapper, MediaServiceClientWrapper, OnlineServiceClientWrapper,
    StorageReaderServiceClientWrapper,
};
use flare_im_service_kit::staff_context::{
    VerifiedStaff, admin_staff_verifier_from_sources, require_staff_any,
};
use flare_proto::common::{
    ConversationDetail, ConversationLifecycleState, ConversationParticipant,
    ConversationVisibility, DevicePriority,
};
use flare_server_core::auth::core_admin;
use flare_server_core::auth::core_admin::permission::{
    CAPABILITIES_READ, CAPABILITIES_WRITE, CONVERSATIONS_READ, HOOKS_READ, HOOKS_WRITE,
    PLUGINS_READ, PLUGINS_WRITE, TENANT_KICK, TENANT_READ, USERS_KICK, USERS_READ,
};
use flare_server_core::auth::kid;
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
};
use prost::Message;
use tonic::transport::Channel;
use tonic::{Request as GrpcRequest, Response as GrpcResponse, Status};
use tower::ServiceExt;

pub const EMPLOYEE: &str = "employee-1";
pub const TENANT: &str = "tenant-a";
pub const OTHER_TENANT: &str = "tenant-b";

/// 一次下游调用：方法名、收到的 `x-flare-ctx`、`authorization`、所有 `x-*` 元数据键、请求摘要。
#[derive(Debug, Clone)]
pub struct Call {
    pub method: &'static str,
    pub signed_context: Vec<String>,
    pub authorization: Vec<String>,
    pub x_metadata: Vec<String>,
    pub request: String,
}

#[derive(Clone, Default)]
pub struct Recorder(Arc<Mutex<Vec<Call>>>);

impl Recorder {
    fn record<T>(&self, method: &'static str, request: &GrpcRequest<T>, summary: String) {
        let metadata = request.metadata();
        let values = |name: &str| -> Vec<String> {
            metadata
                .get_all(name)
                .iter()
                .map(|value| value.to_str().unwrap_or("<non-ascii>").to_string())
                .collect()
        };
        let x_metadata = metadata
            .keys()
            .map(|key| match key {
                tonic::metadata::KeyRef::Ascii(key) => key.as_str().to_string(),
                tonic::metadata::KeyRef::Binary(key) => key.as_str().to_string(),
            })
            .filter(|key| key.starts_with("x-") && key != "x-flare-ctx")
            .collect();
        self.0.lock().unwrap().push(Call {
            method,
            signed_context: values("x-flare-ctx"),
            authorization: values("authorization"),
            x_metadata,
            request: summary,
        });
    }

    pub fn calls(&self) -> Vec<Call> {
        self.0.lock().unwrap().clone()
    }

    pub fn clear(&self) {
        self.0.lock().unwrap().clear();
    }
}

/// prost 的 Timestamp 实现了 `From<SystemTime>`；按字段类型推断，测试不必依赖 prost-types。
fn at<T: From<std::time::SystemTime>>(seconds: u64) -> T {
    (UNIX_EPOCH + Duration::from_secs(seconds)).into()
}

#[derive(Clone)]
pub struct Fakes {
    pub recorder: Recorder,
    /// The Cell key set the real downstreams hold: the gateway's verifier.
    verifier: SignedContextVerifier,
}

impl Fakes {
    /// The real service's admin gate for one RPC: the forwarded Staff context, verified with
    /// the Cell key set, carrying one of `codes` (the set that RPC admits in the real service).
    fn gate<T>(&self, request: &GrpcRequest<T>, codes: &[&str]) -> Result<VerifiedStaff, Status> {
        require_staff_any(request, &self.verifier, codes)
    }

    /// [`Self::gate`] plus the tenant check of an RPC whose request names a tenant.
    fn gate_tenant<T>(
        &self,
        request: &GrpcRequest<T>,
        codes: &[&str],
        tenant_id: &str,
    ) -> Result<VerifiedStaff, Status> {
        let staff = self.gate(request, codes)?;
        staff.require_tenant(tenant_id)?;
        Ok(staff)
    }

    /// Capability's plugin route book: the code, platform staff for registration, the tenant.
    fn gate_plugins<T>(
        &self,
        request: &GrpcRequest<T>,
        code: &str,
        tenant_id: &str,
    ) -> Result<VerifiedStaff, Status> {
        let staff = self.gate_tenant(request, &[code], tenant_id)?;
        if code == PLUGINS_WRITE && !staff.is_platform_staff() {
            return Err(Status::permission_denied(
                "plugin registration is for platform staff only",
            ));
        }
        Ok(staff)
    }
}

/// The hook administer operations Capability reads and writes (its `READ_OPERATIONS` and
/// `WRITE_OPERATIONS`): reads need `core:hooks:read`, writes `core:hooks:write`.
const HOOK_WRITE_OPERATIONS: [&str; 4] = [
    "flare.extension.v1.hook_config.create",
    "flare.extension.v1.hook_config.update",
    "flare.extension.v1.hook_config.delete",
    "flare.extension.v1.hook_config.set_status",
];

fn conversation(conversation_id: &str) -> ConversationDetail {
    let mut attributes = std::collections::HashMap::new();
    attributes.insert("topic".to_string(), "ops".to_string());
    ConversationDetail {
        conversation_id: conversation_id.to_string(),
        conversation_type: "group".to_string(),
        display_name: "Ops".to_string(),
        avatar_url: "https://cdn.example/ops.png".to_string(),
        description: "operations".to_string(),
        visibility: ConversationVisibility::Tenant as i32,
        lifecycle_state: ConversationLifecycleState::ConversationLifecycleActive as i32,
        created_at: 1_700_000_000_000,
        updated_at: 1_700_000_100_000,
        member_count: 3,
        channel_id: "g-1".to_string(),
        attributes,
        ..Default::default()
    }
}

fn downstream_failure(key: &str) -> Option<Status> {
    match key {
        "missing" | "d-gone" => Some(Status::not_found("not here: id-secret-1")),
        "denied" | "d-foreign" => Some(Status::permission_denied("tenant id-secret-1")),
        "down" | "d-down" => Some(Status::unavailable("backend id-secret-1")),
        "slow" => Some(Status::deadline_exceeded("backend id-secret-1")),
        _ => None,
    }
}

#[tonic::async_trait]
impl ConversationAdminService for Fakes {
    async fn admin_list_conversations(
        &self,
        request: GrpcRequest<AdminListConversationsRequest>,
    ) -> Result<GrpcResponse<AdminListConversationsResponse>, Status> {
        let r = request.get_ref();
        let summary = format!(
            "cursor={} limit={} include_deleted={}",
            r.cursor, r.limit, r.include_deleted
        );
        self.recorder
            .record("AdminListConversations", &request, summary);
        self.gate(&request, &[CONVERSATIONS_READ])?;
        Ok(GrpcResponse::new(AdminListConversationsResponse {
            conversations: vec![conversation("c1")],
            next_cursor: "c1:next".to_string(),
            has_more: true,
        }))
    }

    async fn admin_get_conversation(
        &self,
        request: GrpcRequest<AdminGetConversationRequest>,
    ) -> Result<GrpcResponse<AdminGetConversationResponse>, Status> {
        let id = request.get_ref().conversation_id.clone();
        self.recorder.record(
            "AdminGetConversation",
            &request,
            format!("conversation_id={id}"),
        );
        self.gate(&request, &[CONVERSATIONS_READ])?;
        if let Some(status) = downstream_failure(&id) {
            return Err(status);
        }
        Ok(GrpcResponse::new(AdminGetConversationResponse {
            detail: Some(conversation(&id)),
        }))
    }

    async fn admin_list_participants(
        &self,
        request: GrpcRequest<AdminListParticipantsRequest>,
    ) -> Result<GrpcResponse<AdminListParticipantsResponse>, Status> {
        let r = request.get_ref().clone();
        let summary = format!(
            "conversation_id={} cursor={} limit={} include_removed={}",
            r.conversation_id, r.cursor, r.limit, r.include_removed
        );
        self.recorder
            .record("AdminListParticipants", &request, summary);
        self.gate(&request, &[CONVERSATIONS_READ])?;
        if let Some(status) = downstream_failure(&r.conversation_id) {
            return Err(status);
        }
        Ok(GrpcResponse::new(AdminListParticipantsResponse {
            conversation_id: r.conversation_id,
            participants: vec![ConversationParticipant {
                user_id: "u1".to_string(),
                roles: vec!["owner".to_string()],
                muted: true,
                pinned: false,
                attributes: Default::default(),
                joined_at: 1_700_000_000_000,
                visible_from_seq: 12,
                mention_only: true,
            }],
            next_cursor: String::new(),
            has_more: false,
            member_count: 1,
        }))
    }
}

fn device(device_id: &str) -> DeviceInfo {
    DeviceInfo {
        device_id: device_id.to_string(),
        platform: "ios".to_string(),
        model: "iPhone".to_string(),
        os_version: "18.0".to_string(),
        last_active_time: Some((UNIX_EPOCH + Duration::from_millis(1_700_000_000_250)).into()),
        priority: DevicePriority::High as i32,
        token_version: 4,
        connection_quality: None,
        conversation_id: "conn-internal".to_string(),
        gateway_id: "gw-1".to_string(),
        server_id: "srv-1".to_string(),
    }
}

/// 用户 → 在线设备：`u1` 两台，其中 `d-gone` 在列出后、踢之前已下线。
fn devices_of(user_id: &str) -> Vec<DeviceInfo> {
    match user_id {
        "u1" => vec![device("d1"), device("d-gone")],
        "u-foreign" => vec![device("d1"), device("d-foreign")],
        _ => Vec::new(),
    }
}

fn unimplemented<T>() -> Result<GrpcResponse<T>, Status> {
    Err(Status::unimplemented("not used by admin-gateway"))
}

#[tonic::async_trait]
impl OnlineAdminService for Fakes {
    async fn admin_list_user_devices(
        &self,
        request: GrpcRequest<AdminListUserDevicesRequest>,
    ) -> Result<GrpcResponse<ListUserDevicesResponse>, Status> {
        let user_id = request.get_ref().user_id.clone();
        self.recorder.record(
            "AdminListUserDevices",
            &request,
            format!("user_id={user_id}"),
        );
        // Online lists devices for the read route and as the first step of the whole-user kick.
        self.gate(&request, &[USERS_READ, USERS_KICK])?;
        if let Some(status) = downstream_failure(&user_id) {
            return Err(status);
        }
        Ok(GrpcResponse::new(ListUserDevicesResponse {
            devices: devices_of(&user_id),
        }))
    }

    async fn admin_get_user_presence(
        &self,
        request: GrpcRequest<AdminGetUserPresenceRequest>,
    ) -> Result<GrpcResponse<GetUserPresenceResponse>, Status> {
        let user_id = request.get_ref().user_id.clone();
        self.recorder.record(
            "AdminGetUserPresence",
            &request,
            format!("user_id={user_id}"),
        );
        self.gate(&request, &[USERS_READ])?;
        let presence = (user_id == "u1").then(|| UserPresence {
            user_id: user_id.clone(),
            is_online: true,
            devices: vec![device("d1")],
            last_seen: Some(at(1_700_000_000)),
        });
        Ok(GrpcResponse::new(GetUserPresenceResponse { presence }))
    }
}

#[tonic::async_trait]
impl OnlineService for Fakes {
    type WatchPresenceStream = tonic::codegen::BoxStream<PresenceEvent>;
    type SubscribeUserPresenceStream = tonic::codegen::BoxStream<UserPresenceEvent>;

    async fn login(
        &self,
        _: GrpcRequest<LoginRequest>,
    ) -> Result<GrpcResponse<LoginResponse>, Status> {
        unimplemented()
    }
    async fn logout(
        &self,
        _: GrpcRequest<LogoutRequest>,
    ) -> Result<GrpcResponse<LogoutResponse>, Status> {
        unimplemented()
    }
    async fn heartbeat(
        &self,
        _: GrpcRequest<HeartbeatRequest>,
    ) -> Result<GrpcResponse<HeartbeatResponse>, Status> {
        unimplemented()
    }
    async fn get_online_status(
        &self,
        _: GrpcRequest<GetOnlineStatusRequest>,
    ) -> Result<GrpcResponse<GetOnlineStatusResponse>, Status> {
        unimplemented()
    }
    async fn watch_presence(
        &self,
        _: GrpcRequest<WatchPresenceRequest>,
    ) -> Result<GrpcResponse<Self::WatchPresenceStream>, Status> {
        unimplemented()
    }
    async fn get_user_presence(
        &self,
        _: GrpcRequest<GetUserPresenceRequest>,
    ) -> Result<GrpcResponse<GetUserPresenceResponse>, Status> {
        unimplemented()
    }
    async fn batch_get_user_presence(
        &self,
        _: GrpcRequest<BatchGetUserPresenceRequest>,
    ) -> Result<GrpcResponse<BatchGetUserPresenceResponse>, Status> {
        unimplemented()
    }
    async fn subscribe_user_presence(
        &self,
        _: GrpcRequest<SubscribeUserPresenceRequest>,
    ) -> Result<GrpcResponse<Self::SubscribeUserPresenceStream>, Status> {
        unimplemented()
    }
    async fn list_user_devices(
        &self,
        _: GrpcRequest<ListUserDevicesRequest>,
    ) -> Result<GrpcResponse<ListUserDevicesResponse>, Status> {
        unimplemented()
    }

    async fn kick_device(
        &self,
        request: GrpcRequest<KickDeviceRequest>,
    ) -> Result<GrpcResponse<KickDeviceResponse>, Status> {
        let r = request.get_ref().clone();
        let summary = format!(
            "user_id={} device_id={} reason={}",
            r.user_id, r.device_id, r.reason
        );
        self.recorder.record("KickDevice", &request, summary);
        self.gate(&request, &[USERS_KICK])?;
        if let Some(status) = downstream_failure(&r.device_id) {
            return Err(status);
        }
        Ok(GrpcResponse::new(KickDeviceResponse { success: true }))
    }

    async fn self_kick_device(
        &self,
        _: GrpcRequest<KickDeviceRequest>,
    ) -> Result<GrpcResponse<KickDeviceResponse>, Status> {
        unimplemented()
    }

    async fn kick_tenant(
        &self,
        request: GrpcRequest<KickTenantRequest>,
    ) -> Result<GrpcResponse<KickTenantResponse>, Status> {
        let r = request.get_ref().clone();
        let summary = format!("tenant_id={} reason={}", r.tenant_id, r.reason);
        self.recorder.record("KickTenant", &request, summary);
        self.gate_tenant(&request, &[TENANT_KICK], &r.tenant_id)?;
        Ok(GrpcResponse::new(KickTenantResponse {
            kicked: 3,
            superseded: false,
        }))
    }

    async fn get_device(
        &self,
        _: GrpcRequest<GetDeviceRequest>,
    ) -> Result<GrpcResponse<GetDeviceResponse>, Status> {
        unimplemented()
    }
}

#[tonic::async_trait]
impl CapabilityAdminService for Fakes {
    async fn list_tenant_capability_switches(
        &self,
        request: GrpcRequest<ListTenantCapabilitySwitchesRequest>,
    ) -> Result<GrpcResponse<ListTenantCapabilitySwitchesResponse>, Status> {
        self.recorder
            .record("ListTenantCapabilitySwitches", &request, String::new());
        self.gate(&request, &[CAPABILITIES_READ])?;
        Ok(GrpcResponse::new(ListTenantCapabilitySwitchesResponse {
            tenant_id: TENANT.to_string(),
            switches: vec![
                TenantCapabilitySwitch {
                    capability_id: "rtc.call".to_string(),
                    enabled: true,
                    updated_at: Some(at(1_700_000_000)),
                },
                TenantCapabilitySwitch {
                    capability_id: "translate".to_string(),
                    enabled: false,
                    updated_at: None,
                },
            ],
        }))
    }

    async fn get_tenant_runtime(
        &self,
        request: GrpcRequest<GetTenantRuntimeRequest>,
    ) -> Result<GrpcResponse<GetTenantRuntimeResponse>, Status> {
        self.recorder
            .record("GetTenantRuntime", &request, String::new());
        self.gate(&request, &[TENANT_READ])?;
        Ok(GrpcResponse::new(GetTenantRuntimeResponse {
            tenant_id: TENANT.to_string(),
            projected: true,
            name: "Tenant A".to_string(),
            status: TenantStatus::Suspended as i32,
            quota: Some(TenantQuota {
                max_users: 100,
                max_groups: 10,
                max_group_members: 500,
                core: Some(CoreQuota {
                    send_qps: 20,
                    sync_pull_qps: 40,
                    max_online_devices_per_user: 3,
                }),
            }),
            settings_json: r#"{"locale":"zh-CN"}"#.to_string(),
            version: 9,
            projected_at: Some(at(1_700_000_000)),
            updated_at: Some(at(1_700_000_001)),
        }))
    }
}

/// 替身的 Hook 配置：`hook-missing` 不存在。
fn hook_config(hook_id: &str, name: &str, enabled: bool) -> HookConfig {
    HookConfig {
        hook_id: hook_id.to_string(),
        name: name.to_string(),
        hook_type: "message_pre_send".to_string(),
        tenant_id: TENANT.to_string(),
        priority: 10,
        enabled,
        transport: Some(HookTransport {
            r#type: "webhook".to_string(),
            endpoint: "https://hooks.example/dlp".to_string(),
            secret: "webhook-secret-id-secret-1".to_string(),
            timeout_ms: 5000,
            ..Default::default()
        }),
        selector: None,
        retry_policy: None,
        created_at: Some(at(1_700_000_000)),
        updated_at: Some(at(1_700_000_001)),
        hook_points: vec![HookPoint::MessagePreSend as i32],
        config_extensions: None,
    }
}

fn pack(type_name: &str, message: &impl Message) -> GenericResponse {
    GenericResponse {
        ok: true,
        request_id: String::new(),
        payload: Some(prost_types::Any {
            type_url: format!("type.googleapis.com/flare.capability.v1.{type_name}"),
            value: message.encode_to_vec(),
        }),
        error_code: String::new(),
        error_message: String::new(),
    }
}

fn decode<M: Message + Default>(request: &GenericRequest) -> Result<M, Status> {
    let payload = request
        .payload
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("payload required"))?;
    M::decode(payload.value.as_slice()).map_err(|e| Status::invalid_argument(e.to_string()))
}

#[tonic::async_trait]
impl CapabilityService for Fakes {
    async fn list_capabilities(
        &self,
        request: GrpcRequest<ListCapabilitiesRequest>,
    ) -> Result<GrpcResponse<ListCapabilitiesResponse>, Status> {
        self.recorder
            .record("ListCapabilities", &request, String::new());
        self.gate(&request, &[CAPABILITIES_READ])?;
        Ok(GrpcResponse::new(ListCapabilitiesResponse {
            capabilities: ["rtc.call", "ai.summary"]
                .into_iter()
                .map(|id| CapabilityDescriptor {
                    capability_id: id.to_string(),
                    plugin_id: "builtin".to_string(),
                    version: "1.0".to_string(),
                    scope: "tenant".to_string(),
                    visibility: "public".to_string(),
                    permissions: vec!["call".to_string()],
                    message_types: Vec::new(),
                    timeout_ms: 3000,
                    description: format!("{id} capability"),
                })
                .collect(),
        }))
    }
    async fn list_user_capabilities(
        &self,
        _: GrpcRequest<ListUserCapabilitiesRequest>,
    ) -> Result<GrpcResponse<ListUserCapabilitiesResponse>, Status> {
        unimplemented()
    }
    async fn dispatch(
        &self,
        _: GrpcRequest<DispatchCapabilityRequest>,
    ) -> Result<GrpcResponse<DispatchCapabilityResponse>, Status> {
        unimplemented()
    }
    async fn grant_user_capability(
        &self,
        _: GrpcRequest<GrantUserCapabilityRequest>,
    ) -> Result<GrpcResponse<GrantUserCapabilityResponse>, Status> {
        unimplemented()
    }
    async fn revoke_user_capability(
        &self,
        _: GrpcRequest<RevokeUserCapabilityRequest>,
    ) -> Result<GrpcResponse<RevokeUserCapabilityResponse>, Status> {
        unimplemented()
    }
    async fn set_tenant_capability_switch(
        &self,
        request: GrpcRequest<SetTenantCapabilitySwitchRequest>,
    ) -> Result<GrpcResponse<SetTenantCapabilitySwitchResponse>, Status> {
        let r = request.get_ref().clone();
        let summary = format!(
            "tenant_id={} capability_id={} enabled={}",
            r.tenant_id, r.capability_id, r.enabled
        );
        self.recorder
            .record("SetTenantCapabilitySwitch", &request, summary);
        self.gate_tenant(&request, &[CAPABILITIES_WRITE], &r.tenant_id)?;
        if let Some(status) = downstream_failure(&r.capability_id) {
            return Err(status);
        }
        Ok(GrpcResponse::new(SetTenantCapabilitySwitchResponse {
            message: "tenant capability switch updated".to_string(),
        }))
    }
    async fn register_plugin_endpoint(
        &self,
        request: GrpcRequest<RegisterPluginEndpointRequest>,
    ) -> Result<GrpcResponse<RegisterPluginEndpointResponse>, Status> {
        let r = request.get_ref().clone();
        let summary = format!(
            "tenant_id={} plugin_id={} capability_id={} grpc_authority={} request_id={} declared={}",
            r.tenant_id,
            r.plugin_id,
            r.capability_id,
            r.grpc_authority,
            r.request_id,
            r.declared_operations.join("+")
        );
        self.recorder
            .record("RegisterPluginEndpoint", &request, summary);
        self.gate_plugins(&request, PLUGINS_WRITE, &r.tenant_id)?;
        if let Some(status) = downstream_failure(&r.plugin_id) {
            return Err(status);
        }
        Ok(GrpcResponse::new(RegisterPluginEndpointResponse {
            accepted: true,
            message: "registered".to_string(),
        }))
    }
    async fn deregister_plugin_endpoint(
        &self,
        request: GrpcRequest<DeregisterPluginEndpointRequest>,
    ) -> Result<GrpcResponse<DeregisterPluginEndpointResponse>, Status> {
        let r = request.get_ref().clone();
        let summary = format!(
            "tenant_id={} plugin_id={} request_id={}",
            r.tenant_id, r.plugin_id, r.request_id
        );
        self.recorder
            .record("DeregisterPluginEndpoint", &request, summary);
        self.gate_plugins(&request, PLUGINS_WRITE, &r.tenant_id)?;
        Ok(GrpcResponse::new(DeregisterPluginEndpointResponse {
            accepted: r.plugin_id != "plugin-unknown",
        }))
    }

    async fn list_registered_plugins(
        &self,
        request: GrpcRequest<ListRegisteredPluginsRequest>,
    ) -> Result<GrpcResponse<ListRegisteredPluginsResponse>, Status> {
        let r = request.get_ref().clone();
        let summary = format!(
            "tenant_id={} capability_id={}",
            r.tenant_id, r.capability_id
        );
        self.recorder
            .record("ListRegisteredPlugins", &request, summary);
        self.gate_plugins(&request, PLUGINS_READ, &r.tenant_id)?;
        let instances = ["plugin-b", "plugin-a"]
            .into_iter()
            .map(|plugin_id| RegisteredPluginInstance {
                plugin_id: plugin_id.to_string(),
                capability_id: "rtc.call".to_string(),
                grpc_authority: "discovery://flare-strom-sfu".to_string(),
                plugin_version: "1.2.0".to_string(),
                api_version: "v2".to_string(),
                manifest_sha256: "ab".repeat(32),
                declared_operations: vec!["rtc.call".to_string()],
                unverified: false,
                ..Default::default()
            })
            .collect();
        Ok(GrpcResponse::new(ListRegisteredPluginsResponse {
            instances,
        }))
    }

    /// Hook 治理替身：按 operation 解开载荷，记下摘要，回固定答复；`hook-missing` 回 NOT_FOUND，
    /// `hook-foreign` 回 PERMISSION_DENIED（真实 Capability 核对配置租户后的答复）。
    async fn administer(
        &self,
        request: GrpcRequest<GenericRequest>,
    ) -> Result<GrpcResponse<GenericResponse>, Status> {
        let outer = request.get_ref().clone();
        // Capability decides the code from the operation before it dispatches anything; an
        // unknown operation is gated with the read code and then refused.
        let code = if HOOK_WRITE_OPERATIONS.contains(&outer.operation.as_str()) {
            HOOKS_WRITE
        } else {
            HOOKS_READ
        };
        if let Err(status) = self.gate(&request, &[code]) {
            self.recorder.record(
                "Administer",
                &request,
                format!("refused {}", outer.operation),
            );
            return Err(status);
        }
        let hook_failure = |hook_id: &str| match hook_id {
            "hook-missing" => Some(Status::not_found("hook config not found id-secret-1")),
            "hook-foreign" => Some(Status::permission_denied("tenant id-secret-1")),
            _ => None,
        };
        let (summary, response) = match outer.operation.as_str() {
            "flare.extension.v1.hook_config.list" => {
                let r: ListHookConfigsRequest = decode(&outer)?;
                (
                    format!(
                        "list tenant_id={} hook_type={} enabled_only={} limit={}",
                        r.tenant_id,
                        r.hook_type,
                        r.enabled_only,
                        r.pagination.as_ref().map(|p| p.limit).unwrap_or_default()
                    ),
                    pack(
                        "ListHookConfigsResponse",
                        &ListHookConfigsResponse {
                            configs: vec![
                                hook_config("7", "dlp", true),
                                hook_config("8", "archive", false),
                            ],
                            pagination: Some(flare_proto::common::Pagination {
                                limit: r.pagination.map(|p| p.limit).unwrap_or_default(),
                                has_more: false,
                                total_size: 2,
                                ..Default::default()
                            }),
                        },
                    ),
                )
            }
            "flare.extension.v1.hook_config.create" => {
                let r: CreateHookConfigRequest = decode(&outer)?;
                (
                    format!(
                        "create tenant_id={} name={} hook_type={} priority={} secret_set={}",
                        r.tenant_id,
                        r.name,
                        r.hook_type,
                        r.priority,
                        r.transport.as_ref().is_some_and(|t| !t.secret.is_empty())
                    ),
                    pack(
                        "CreateHookConfigResponse",
                        &CreateHookConfigResponse {
                            config: Some(hook_config("9", &r.name, true)),
                        },
                    ),
                )
            }
            "flare.extension.v1.hook_config.get" => {
                let r: GetHookConfigRequest = decode(&outer)?;
                if let Some(status) = hook_failure(&r.hook_id) {
                    return Err(status);
                }
                (
                    format!("get hook_id={}", r.hook_id),
                    pack(
                        "GetHookConfigResponse",
                        &GetHookConfigResponse {
                            config: Some(hook_config(&r.hook_id, "dlp", true)),
                        },
                    ),
                )
            }
            "flare.extension.v1.hook_config.update" => {
                let r: UpdateHookConfigRequest = decode(&outer)?;
                if let Some(status) = hook_failure(&r.hook_id) {
                    return Err(status);
                }
                (
                    format!(
                        "update hook_id={} name={} priority={}",
                        r.hook_id, r.name, r.priority
                    ),
                    pack(
                        "UpdateHookConfigResponse",
                        &UpdateHookConfigResponse {
                            config: Some(hook_config(&r.hook_id, &r.name, true)),
                        },
                    ),
                )
            }
            "flare.extension.v1.hook_config.delete" => {
                let r: DeleteHookConfigRequest = decode(&outer)?;
                if let Some(status) = hook_failure(&r.hook_id) {
                    return Err(status);
                }
                (
                    format!("delete hook_id={}", r.hook_id),
                    pack(
                        "DeleteHookConfigResponse",
                        &DeleteHookConfigResponse { success: true },
                    ),
                )
            }
            "flare.extension.v1.hook_config.set_status" => {
                let r: SetHookStatusRequest = decode(&outer)?;
                if let Some(status) = hook_failure(&r.hook_id) {
                    return Err(status);
                }
                (
                    format!("set_status hook_id={} enabled={}", r.hook_id, r.enabled),
                    pack(
                        "SetHookStatusResponse",
                        &SetHookStatusResponse { success: true },
                    ),
                )
            }
            "flare.extension.v1.hook_config.get_statistics" => {
                let r: GetHookStatisticsRequest = decode(&outer)?;
                if let Some(status) = hook_failure(&r.hook_id) {
                    return Err(status);
                }
                (
                    format!(
                        "get_statistics hook_id={} start={:?} end={:?}",
                        r.hook_id,
                        r.time_range.as_ref().and_then(|t| t.start_time),
                        r.time_range.as_ref().and_then(|t| t.end_time)
                    ),
                    pack(
                        "GetHookStatisticsResponse",
                        &GetHookStatisticsResponse {
                            statistics: Some(HookStatistics {
                                hook_id: r.hook_id,
                                total_executions: 10,
                                success_count: 9,
                                failure_count: 1,
                                avg_latency_ms: 12.5,
                                p99_latency_ms: 40.0,
                                rate_limit_count: 0,
                                circuit_break_count: 0,
                                error_count_by_code: std::collections::HashMap::from([(
                                    "timeout".to_string(),
                                    1,
                                )]),
                            }),
                        },
                    ),
                )
            }
            "flare.extension.v1.hook_config.query_executions" => {
                let r: QueryHookExecutionsRequest = decode(&outer)?;
                if let Some(status) = hook_failure(&r.hook_id) {
                    return Err(status);
                }
                (
                    format!(
                        "query_executions hook_id={} message_id={} success_only={} limit={}",
                        r.hook_id,
                        r.message_id,
                        r.success_only,
                        r.pagination.as_ref().map(|p| p.limit).unwrap_or_default()
                    ),
                    pack(
                        "QueryHookExecutionsResponse",
                        &QueryHookExecutionsResponse {
                            executions: vec![HookExecution {
                                execution_id: "e1".to_string(),
                                hook_id: r.hook_id,
                                message_id: "m1".to_string(),
                                success: true,
                                latency_ms: 12,
                                error_code: String::new(),
                                error_message: String::new(),
                                executed_at: Some(at(1_700_000_000)),
                            }],
                            pagination: Some(flare_proto::common::Pagination {
                                has_more: false,
                                ..Default::default()
                            }),
                        },
                    ),
                )
            }
            other => {
                return Err(Status::unimplemented(format!(
                    "unknown administer operation: {other}"
                )));
            }
        };
        self.recorder.record("Administer", &request, summary);
        Ok(GrpcResponse::new(GenericResponse {
            request_id: outer.request_id,
            ..response
        }))
    }
}

/// 起一个只绑 127.0.0.1 的 tonic 服务，同一端口挂五个替身服务。`verifier` 是下游持有的 Cell
/// 公钥集（与网关同一份），替身用它验转发来的 Staff 上下文。
pub async fn spawn_fakes(verifier: SignedContextVerifier) -> (std::net::SocketAddr, Recorder) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    assert!(address.ip().is_loopback());
    let fakes = Fakes {
        recorder: Recorder::default(),
        verifier,
    };
    let recorder = fakes.recorder.clone();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ConversationAdminServiceServer::new(fakes.clone()))
            .add_service(OnlineServiceServer::new(fakes.clone()))
            .add_service(OnlineAdminServiceServer::new(fakes.clone()))
            .add_service(CapabilityServiceServer::new(fakes.clone()))
            .add_service(CapabilityAdminServiceServer::new(fakes))
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
    );
    (address, recorder)
}

fn pem_pair(seed: u8) -> (String, String) {
    let key = SigningKey::from_bytes(&[seed; 32]);
    (
        key.to_pkcs8_pem(Default::default()).unwrap().to_string(),
        key.verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap(),
    )
}

/// The Social gateway admin partition, as the signer of the staff context: it holds the Cell
/// key registered as `kid=gateway`. The verifier the Admin Gateway (and every stand-in
/// downstream) gets is built by the production constructor from a key-set spec that also holds
/// a hook key, so a Staff context signed with the wrong component key is refused by kid.
pub struct Gateway {
    signer: SignedContextSigner,
    hook_signer: SignedContextSigner,
    pub verifier: SignedContextVerifier,
}

impl Gateway {
    pub fn new() -> Self {
        Self::new_with_seed(73)
    }

    /// Another Cell key pair: a context it signs must be refused by the gateway's verifier.
    pub fn new_with_seed(seed: u8) -> Self {
        let (gateway_private, gateway_public) = pem_pair(seed);
        let (hook_private, hook_public) = pem_pair(seed.wrapping_add(100));
        let keys_spec = serde_json::to_string(&std::collections::BTreeMap::from([
            (kid::GATEWAY, gateway_public.as_str()),
            (kid::HOOK, hook_public.as_str()),
        ]))
        .unwrap();
        Self {
            signer: SignedContextSigner::from_private_key(&gateway_private)
                .unwrap()
                .with_kid(kid::GATEWAY)
                .unwrap(),
            hook_signer: SignedContextSigner::from_private_key(&hook_private)
                .unwrap()
                .with_kid(kid::HOOK)
                .unwrap(),
            verifier: admin_staff_verifier_from_sources(
                "admin-gateway",
                Some(keys_spec.as_str()),
                None,
            )
            .unwrap(),
        }
    }

    fn identity(subject: &str, tenant: &str, permission: &str) -> SignedContextIdentity {
        SignedContextIdentity::new(SignedActorType::Staff, subject, tenant, "req-staff-1")
            .with_staff_grant(Some("org-1".to_string()), Some(permission.to_string()), 3)
            .with_trace_id("trace-staff-1")
    }

    /// An organization staff context for `tenant` carrying `permission`.
    pub fn staff_token(&self, subject: &str, tenant: &str, permission: &str) -> String {
        self.signer
            .sign(&Self::identity(subject, tenant, permission))
            .unwrap()
    }

    /// A platform staff context (no organization) for `tenant` carrying `permission`.
    pub fn platform_staff_token(&self, subject: &str, tenant: &str, permission: &str) -> String {
        self.signer
            .sign(
                &SignedContextIdentity::new(SignedActorType::Staff, subject, tenant, "req-staff-1")
                    .with_staff_grant(None, Some(permission.to_string()), 3)
                    .with_trace_id("trace-staff-1"),
            )
            .unwrap()
    }

    /// The staff context for one admin route: the route's permission code, as the admin
    /// partition would sign it.
    pub fn token_for(&self, method: &str, path: &str) -> String {
        self.token_for_tenant(method, path, TENANT)
    }

    pub fn token_for_tenant(&self, method: &str, path: &str, tenant: &str) -> String {
        let relative = path
            .strip_prefix("/api/v1/admin")
            .unwrap_or(path)
            .split('?')
            .next()
            .unwrap();
        let (route, permission) = core_admin::resolve_with_permission(method, relative)
            .unwrap_or_else(|| panic!("{method} {relative} is not a Core admin route"));
        if route.platform_only {
            self.platform_staff_token(EMPLOYEE, tenant, &permission)
        } else {
            self.staff_token(EMPLOYEE, tenant, &permission)
        }
    }

    /// A Staff context signed with the hook key: valid signature, wrong component.
    pub fn hook_signed_staff_token(&self, tenant: &str, permission: &str) -> String {
        self.hook_signer
            .sign(&Self::identity(EMPLOYEE, tenant, permission))
            .unwrap()
    }

    /// An organization staff context for `tenant` with no permission claim at all: it verifies,
    /// but no route's code matches it.
    pub fn staff_token_without_permission(&self, subject: &str, tenant: &str) -> String {
        self.signer
            .sign(
                &SignedContextIdentity::new(SignedActorType::Staff, subject, tenant, "req-staff-1")
                    .with_staff_grant(Some("org-1".to_string()), None, 3)
                    .with_trace_id("trace-staff-1"),
            )
            .unwrap()
    }

    /// A non-staff context signed with the gateway key.
    pub fn service_token(&self, tenant: &str) -> String {
        self.signer
            .sign(&SignedContextIdentity::new(
                SignedActorType::Service,
                "flare-social-bridge",
                tenant,
                "req-service-1",
            ))
            .unwrap()
    }
}

impl Default for Gateway {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Harness {
    pub router: Router,
    pub recorder: Recorder,
    pub gateway: Gateway,
    pub downstream: std::net::SocketAddr,
}

/// `channel` 接 Conversation / Online 替身，`media` 接 Media 替身，其余下游接 `unused`。
pub fn clients(channel: Channel, media: Channel, unused: Channel) -> AdminGrpcClients {
    AdminGrpcClients {
        media: Arc::new(tokio::sync::Mutex::new(
            MediaServiceClientWrapper::from_channel(media, None),
        )),
        conversation_read: Arc::new(tokio::sync::Mutex::new(
            ConversationReadServiceClientWrapper::from_channel(channel.clone()),
        )),
        online: Arc::new(tokio::sync::Mutex::new(
            OnlineServiceClientWrapper::from_channel(channel),
        )),
        storage_reader: Arc::new(tokio::sync::Mutex::new(
            StorageReaderServiceClientWrapper::from_channel(unused),
        )),
        request_timeout: None,
    }
}

/// `capability=false` 模拟 `FLARE_ADMIN_GATEWAY_GRPC_CAPABILITY_SERVICE_URL=""`。
pub async fn harness(capability: bool) -> Harness {
    let gateway = Gateway::new();
    let (address, recorder) = spawn_fakes(gateway.verifier.clone()).await;
    let channel = Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_lazy();
    // 与管理员路由无关的下游指向一个不会被调用的环回端口。
    let unused = Channel::from_static("http://127.0.0.1:9").connect_lazy();
    let capability = capability.then(|| AdminCapabilityDownstream {
        config: CapabilityDownstreamConfig {
            service_url: format!("http://{address}"),
            static_fallback: String::new(),
        },
        client: CapabilityAdminClientWrapper::from_channel(channel.clone()),
    });
    let router = create_admin_router(
        Arc::new(clients(channel, unused.clone(), unused)),
        capability,
    )
    .layer(axum::Extension(Arc::new(gateway.verifier.clone())))
    .layer(axum::Extension(Arc::new(AuditEffects::disabled(
        "admin-gateway",
    ))));
    Harness {
        router,
        recorder,
        gateway,
        downstream: address,
    }
}

/// ADM-01 路由：(方法, 路径)。写操作是三个 POST。
pub const OPS_ROUTES: &[(&str, &str)] = &[
    ("GET", "/api/v1/admin/conversations"),
    ("GET", "/api/v1/admin/conversations/c1"),
    ("GET", "/api/v1/admin/conversations/c1/participants"),
    ("GET", "/api/v1/admin/users/u1/devices"),
    ("GET", "/api/v1/admin/users/u1/presence"),
    ("POST", "/api/v1/admin/users/u1/kick"),
    ("POST", "/api/v1/admin/users/u1/devices/d1/kick"),
    ("POST", "/api/v1/admin/tenant/kick-all"),
    ("GET", "/api/v1/admin/tenant/runtime"),
    ("GET", "/api/v1/admin/capabilities/tenant"),
    ("GET", "/api/v1/admin/plugins"),
];

/// R3-2 新增的 Capability 路由：目录、能力开关、插件登记与 Hook 治理（方法, 路径, JSON 请求体）。
pub fn capability_routes() -> Vec<(&'static str, &'static str, Option<&'static str>)> {
    vec![
        ("GET", "/api/v1/admin/capabilities/catalog", None),
        (
            "PUT",
            "/api/v1/admin/capabilities/tenant/rtc.call",
            Some(r#"{"enabled":false}"#),
        ),
        (
            "POST",
            "/api/v1/admin/plugins/register",
            Some(
                r#"{"plugin_id":"plugin-c","capability_id":"rtc.call","grpc_authority":"http://sfu:9","declared_operations":["rtc.call"],"plugin_version":"1.0.0","api_version":"1.0","manifest_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#,
            ),
        ),
        (
            "POST",
            "/api/v1/admin/plugins/deregister",
            Some(r#"{"plugin_id":"plugin-c"}"#),
        ),
        ("GET", "/api/v1/admin/hooks", None),
        (
            "POST",
            "/api/v1/admin/hooks",
            Some(r#"{"name":"dlp","hook_type":"message_pre_send","priority":10}"#),
        ),
        ("GET", "/api/v1/admin/hooks/7", None),
        (
            "PUT",
            "/api/v1/admin/hooks/7",
            Some(r#"{"name":"dlp-2","priority":20}"#),
        ),
        ("DELETE", "/api/v1/admin/hooks/7", None),
        (
            "PUT",
            "/api/v1/admin/hooks/7/status",
            Some(r#"{"enabled":false}"#),
        ),
        ("GET", "/api/v1/admin/hooks/7/statistics", None),
        ("GET", "/api/v1/admin/hooks/7/executions", None),
    ]
}

pub const WRITE_HEADERS: &[(&str, &str)] = &[
    ("x-audit-reason", "incident-42 containment"),
    ("idempotency-key", "idem-1"),
];

/// 写方法（按共享路由表：GET 以外都是写，两个只读查询 POST 除外）。
pub fn is_write(method: &str, path: &str) -> bool {
    let relative = path
        .strip_prefix("/api/v1/admin")
        .unwrap_or(path)
        .split('?')
        .next()
        .unwrap();
    core_admin::resolve(method, relative).is_some_and(|route| route.write)
}

/// 带 Staff 上下文的请求构造器；`extra` 追加其他头，`body` 为 JSON 请求体。
pub fn request_with_body(
    method: &str,
    path: &str,
    token: Option<&str>,
    extra: &[(&str, &str)],
    body: Option<&str>,
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(token) = token {
        builder = builder.header("x-flare-ctx", token);
    }
    for (name, value) in extra {
        builder = builder.header(*name, *value);
    }
    match body {
        Some(body) => builder
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    }
}

/// 带 Staff 上下文的请求构造器；`extra` 追加其他头。
pub fn request(
    method: &str,
    path: &str,
    token: Option<&str>,
    extra: &[(&str, &str)],
) -> Request<Body> {
    request_with_body(method, path, token, extra, None)
}

/// 管理员请求：按路由签对应权限码的 Staff 上下文，写路由自动带齐审计头。
pub fn admin_request(method: &str, path: &str, gateway: &Gateway) -> Request<Body> {
    admin_request_with_body(method, path, gateway, None)
}

pub fn admin_request_with_body(
    method: &str,
    path: &str,
    gateway: &Gateway,
    body: Option<&str>,
) -> Request<Body> {
    let extra = if is_write(method, path) {
        WRITE_HEADERS
    } else {
        &[]
    };
    let token = gateway.token_for(method, path);
    request_with_body(method, path, Some(&token), extra, body)
}

pub async fn send(router: &Router, request: Request<Body>) -> (StatusCode, serde_json::Value) {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json = if body.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&body).unwrap_or_else(|_| {
            serde_json::Value::String(String::from_utf8_lossy(&body).into_owned())
        })
    };
    (status, json)
}

/// `data` 对象的字段名集合（排好序），用于钉住响应形状。
pub fn keys(value: &serde_json::Value) -> Vec<String> {
    let mut keys: Vec<String> = value
        .as_object()
        .unwrap_or_else(|| panic!("expected an object, got {value}"))
        .keys()
        .cloned()
        .collect();
    keys.sort();
    keys
}

pub fn sorted(fields: &[&str]) -> Vec<String> {
    let mut fields: Vec<String> = fields.iter().map(|f| f.to_string()).collect();
    fields.sort();
    fields
}

/// 网关只把已验过的 Staff 上下文原样交给下游，不带 `authorization`，也不带任何 `x-*` 身份元数据
/// （消息检索路由另带请求号与追踪号，见 admin_messages_contract）。转发的上下文用网关的验签器
/// 验：员工、应用与权限码就是网关 admin 分区签进去的。
pub fn assert_forwarded_only_the_staff_context(
    call: &Call,
    gateway: &Gateway,
    tenant: &str,
    permission: &str,
) {
    assert_eq!(call.signed_context.len(), 1, "{}", call.method);
    let claims = gateway
        .verifier
        .verify(&call.signed_context[0])
        .unwrap_or_else(|_| panic!("{} forwarded a context that does not verify", call.method));
    assert_eq!(claims.actor_type, SignedActorType::Staff, "{}", call.method);
    assert_eq!(claims.actor_id, EMPLOYEE, "{}", call.method);
    assert_eq!(claims.tenant_id, tenant, "{}", call.method);
    assert_eq!(
        claims.permission.as_deref(),
        Some(permission),
        "{}",
        call.method
    );
    assert_eq!(claims.kid, kid::GATEWAY, "{}", call.method);
    assert!(
        call.authorization.is_empty(),
        "{} received a bearer {:?}",
        call.method,
        call.authorization
    );
    assert!(
        call.x_metadata.is_empty(),
        "{} received unsigned identity metadata {:?}",
        call.method,
        call.x_metadata
    );
}

/// [`assert_forwarded_only_the_staff_context`] for the route the request went to: the
/// forwarded permission is that route's code.
pub fn assert_forwarded_for_route(call: &Call, gateway: &Gateway, method: &str, path: &str) {
    let relative = path
        .strip_prefix("/api/v1/admin")
        .unwrap_or(path)
        .split('?')
        .next()
        .unwrap();
    let (_, permission) = core_admin::resolve_with_permission(method, relative).unwrap();
    assert_forwarded_only_the_staff_context(call, gateway, TENANT, &permission);
}
