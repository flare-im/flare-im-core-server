//! Admin Gateway 契约测试的公共夹具：Console 签发的 admin 令牌，以及只监听 127.0.0.1
//! 的替身 tonic 服务（Conversation / Online / Capability 的管理员接口）。
//!
//! 替身按输入给出固定回答，并记下每次调用收到的 `authorization` 与所有 `x-*` 元数据，
//! 用来断言网关只原样转发已验过的 admin 令牌、不带任何未签名的身份头。
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
    DeregisterPluginEndpointRequest, DeregisterPluginEndpointResponse, DispatchCapabilityRequest,
    DispatchCapabilityResponse, GenericRequest, GenericResponse, GetTenantRuntimeRequest,
    GetTenantRuntimeResponse, GrantUserCapabilityRequest, GrantUserCapabilityResponse,
    ListCapabilitiesRequest, ListCapabilitiesResponse, ListRegisteredPluginsRequest,
    ListRegisteredPluginsResponse, ListTenantCapabilitySwitchesRequest,
    ListTenantCapabilitySwitchesResponse, ListUserCapabilitiesRequest,
    ListUserCapabilitiesResponse, RegisterPluginEndpointRequest, RegisterPluginEndpointResponse,
    RegisteredPluginInstance, RevokeUserCapabilityRequest, RevokeUserCapabilityResponse,
    SetTenantCapabilitySwitchRequest, SetTenantCapabilitySwitchResponse, TenantCapabilitySwitch,
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
use flare_im_service_kit::clients::{
    CapabilityAdminClientWrapper, CapabilityDownstreamConfig,
    ConversationManageServiceClientWrapper, ConversationReadServiceClientWrapper, GrpcClients,
    MediaServiceClientWrapper, MessageActionServiceClientWrapper, MessageEventServiceClientWrapper,
    MessageSendServiceClientWrapper, OnlineServiceClientWrapper, StorageReaderServiceClientWrapper,
};
use flare_im_service_kit::control_auth::{
    CONSOLE_ISSUER, CORE_ADMIN_AUDIENCE, ConsoleControlVerifier,
};
use flare_proto::common::{
    ConversationDetail, ConversationLifecycleState, ConversationParticipant,
    ConversationVisibility, DevicePriority,
};
use tonic::transport::Channel;
use tonic::{Request as GrpcRequest, Response as GrpcResponse, Status};
use tower::ServiceExt;

pub const EMPLOYEE: &str = "employee-1";
pub const TENANT: &str = "tenant-a";
pub const OTHER_TENANT: &str = "tenant-b";

/// 一次下游调用：方法名、收到的 `authorization`、所有 `x-*` 元数据键、请求摘要。
#[derive(Debug, Clone)]
pub struct Call {
    pub method: &'static str,
    pub authorization: Vec<String>,
    pub x_metadata: Vec<String>,
    pub request: String,
}

#[derive(Clone, Default)]
pub struct Recorder(Arc<Mutex<Vec<Call>>>);

impl Recorder {
    fn record<T>(&self, method: &'static str, request: &GrpcRequest<T>, summary: String) {
        let metadata = request.metadata();
        let authorization = metadata
            .get_all("authorization")
            .iter()
            .map(|value| value.to_str().unwrap_or("<non-ascii>").to_string())
            .collect();
        let x_metadata = metadata
            .keys()
            .map(|key| match key {
                tonic::metadata::KeyRef::Ascii(key) => key.as_str().to_string(),
                tonic::metadata::KeyRef::Binary(key) => key.as_str().to_string(),
            })
            .filter(|key| key.starts_with("x-"))
            .collect();
        self.0.lock().unwrap().push(Call {
            method,
            authorization,
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
}

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
        Ok(GrpcResponse::new(KickTenantResponse { kicked: 3 }))
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

#[tonic::async_trait]
impl CapabilityService for Fakes {
    async fn list_capabilities(
        &self,
        _: GrpcRequest<ListCapabilitiesRequest>,
    ) -> Result<GrpcResponse<ListCapabilitiesResponse>, Status> {
        unimplemented()
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
        _: GrpcRequest<SetTenantCapabilitySwitchRequest>,
    ) -> Result<GrpcResponse<SetTenantCapabilitySwitchResponse>, Status> {
        unimplemented()
    }
    async fn register_plugin_endpoint(
        &self,
        _: GrpcRequest<RegisterPluginEndpointRequest>,
    ) -> Result<GrpcResponse<RegisterPluginEndpointResponse>, Status> {
        unimplemented()
    }
    async fn deregister_plugin_endpoint(
        &self,
        _: GrpcRequest<DeregisterPluginEndpointRequest>,
    ) -> Result<GrpcResponse<DeregisterPluginEndpointResponse>, Status> {
        unimplemented()
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
                seat_model: "per_user".to_string(),
                ..Default::default()
            })
            .collect();
        Ok(GrpcResponse::new(ListRegisteredPluginsResponse {
            instances,
        }))
    }

    async fn administer(
        &self,
        _: GrpcRequest<GenericRequest>,
    ) -> Result<GrpcResponse<GenericResponse>, Status> {
        unimplemented()
    }
}

/// 起一个只绑 127.0.0.1 的 tonic 服务，同一端口挂五个替身服务。
pub async fn spawn_fakes() -> (std::net::SocketAddr, Recorder) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    assert!(address.ip().is_loopback());
    let fakes = Fakes {
        recorder: Recorder::default(),
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

/// Console 私钥只在测试里：签 admin 令牌；网关只拿公钥。
pub struct Console {
    tokens: flare_server_core::TokenService,
    pub verifier: ConsoleControlVerifier,
}

impl Console {
    pub fn new() -> Self {
        Self::new_with_seed(73)
    }

    /// 另一把 Console 密钥：它签的令牌网关必须拒绝。
    pub fn new_with_seed(seed: u8) -> Self {
        let key = SigningKey::from_bytes(&[seed; 32]);
        let private = key.to_pkcs8_pem(Default::default()).unwrap();
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        Self {
            tokens: flare_server_core::TokenService::from_ed25519_private_key(
                &private,
                CONSOLE_ISSUER,
                60,
            )
            .unwrap(),
            verifier: ConsoleControlVerifier::from_public_key(&public).unwrap(),
        }
    }

    pub fn admin_token(&self, subject: &str, tenant: &str) -> String {
        self.tokens
            .generate_with_ed25519(subject, Some(tenant), "admin", CORE_ADMIN_AUDIENCE)
            .unwrap()
    }
}

pub struct Harness {
    pub router: Router,
    pub recorder: Recorder,
    pub console: Console,
    pub token: String,
    pub downstream: std::net::SocketAddr,
}

/// `channel` 接 Conversation / Online 替身，`media` 接 Media 替身，其余下游接 `unused`。
fn clients(channel: Channel, media: Channel, unused: Channel) -> GrpcClients {
    GrpcClients {
        media: Arc::new(tokio::sync::Mutex::new(
            MediaServiceClientWrapper::from_channel(media, None),
        )),
        message_ingest_send: Arc::new(tokio::sync::Mutex::new(
            MessageSendServiceClientWrapper::from_channel(unused.clone()),
        )),
        message_event: Arc::new(tokio::sync::Mutex::new(
            MessageEventServiceClientWrapper::from_channel(unused.clone()),
        )),
        message_action: Arc::new(tokio::sync::Mutex::new(
            MessageActionServiceClientWrapper::from_channel(unused.clone()),
        )),
        conversation_read: Arc::new(tokio::sync::Mutex::new(
            ConversationReadServiceClientWrapper::from_channel(channel.clone()),
        )),
        conversation_manage: Arc::new(tokio::sync::Mutex::new(
            ConversationManageServiceClientWrapper::from_channel(unused.clone()),
        )),
        online: Arc::new(tokio::sync::Mutex::new(
            OnlineServiceClientWrapper::from_channel(channel),
        )),
        storage_reader: Arc::new(tokio::sync::Mutex::new(
            StorageReaderServiceClientWrapper::from_channel(unused),
        )),
    }
}

/// `capability=false` 模拟 `FLARE_ADMIN_GATEWAY_GRPC_CAPABILITY_SERVICE_URL=""`。
pub async fn harness(capability: bool) -> Harness {
    let (address, recorder) = spawn_fakes().await;
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
    let console = Console::new();
    let router = create_admin_router(
        Arc::new(clients(channel, unused.clone(), unused)),
        capability,
    )
    .layer(axum::Extension(Arc::new(console.verifier.clone())));
    let token = console.admin_token(EMPLOYEE, TENANT);
    Harness {
        router,
        recorder,
        console,
        token,
        downstream: address,
    }
}

/// 新增的 ADM-01 路由：(方法, 路径)。写操作是三个 POST。
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

pub const WRITE_HEADERS: &[(&str, &str)] = &[
    ("x-actor-id", EMPLOYEE),
    ("x-audit-reason", "incident-42 containment"),
    ("idempotency-key", "idem-1"),
];

/// 带令牌与租户头的请求构造器；`extra` 追加其他头。
pub fn request(
    method: &str,
    path: &str,
    token: Option<&str>,
    tenant: Option<&str>,
    extra: &[(&str, &str)],
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    if let Some(tenant) = tenant {
        builder = builder.header("x-tenant-id", tenant);
    }
    for (name, value) in extra {
        builder = builder.header(*name, *value);
    }
    builder.body(Body::empty()).unwrap()
}

/// 管理员请求：写路由自动带齐审计头。
pub fn admin_request(method: &str, path: &str, token: &str) -> Request<Body> {
    let extra = if method == "POST" { WRITE_HEADERS } else { &[] };
    request(method, path, Some(token), Some(TENANT), extra)
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

/// 网关只把已验过的 admin 令牌原样交给下游，不带任何 `x-*` 身份元数据。
pub fn assert_forwarded_only_the_assertion(call: &Call, token: &str) {
    assert_eq!(
        call.authorization,
        vec![format!("Bearer {token}")],
        "{}",
        call.method
    );
    assert!(
        call.x_metadata.is_empty(),
        "{} received unsigned identity metadata {:?}",
        call.method,
        call.x_metadata
    );
}
