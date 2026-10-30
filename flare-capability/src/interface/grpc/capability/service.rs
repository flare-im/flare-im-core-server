//! `flare.capability.v1.CapabilityService` gRPC 实现（生产护栏：超时、限流、Staff 上下文、审计）。
//!
//! 管理 RPC 只认 admin-gateway 转发的 Staff `x-flare-ctx`（网关 admin 分区按 `kid=gateway` 签），
//! 按 RPC 族复核签名里的权限码：目录与用户授权读 `core:capabilities:read`，授予/吊销/租户开关
//! `core:capabilities:write`，插件簿读 `core:plugins:read`，插件登记与注销 `core:plugins:write`
//! （仅平台员工），Hook 治理 `core:hooks:read` / `core:hooks:write`。插件登记、注销与列表另接受
//! Cell 签的 Service 上下文（插件宿主自注册），其签名租户须等于请求租户。
//! 登记/注销还接受平台签发、完整请求绑定的 `x-flare-plugin-registration`；
//! 此专用凭据不产生 Cell 上下文，也不用于列表、管理、Dispatch 或资源回调。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::Utc;
use flare_grpc_proto::capability::capability_service_server::CapabilityService;
use flare_grpc_proto::capability::extension_plugin_client::ExtensionPluginClient;
use flare_grpc_proto::capability::{
    CapabilityDescriptor as ProtoDescriptor, CapabilityDispatchResult as ProtoDispatchResult,
    DeregisterPluginEndpointRequest, DeregisterPluginEndpointResponse, DispatchCapabilityRequest,
    DispatchCapabilityResponse, GenericRequest, GenericResponse, GrantUserCapabilityRequest,
    GrantUserCapabilityResponse, ListCapabilitiesRequest, ListCapabilitiesResponse,
    ListRegisteredPluginsRequest, ListRegisteredPluginsResponse, ListUserCapabilitiesRequest,
    ListUserCapabilitiesResponse, RegisterPluginEndpointRequest, RegisterPluginEndpointResponse,
    RegisteredPluginInstance, RevokeUserCapabilityRequest, RevokeUserCapabilityResponse,
    SetTenantCapabilitySwitchRequest, SetTenantCapabilitySwitchResponse,
    UserCapabilityGrant as ProtoGrant,
};
use flare_im_contracts::utils::normalize_tenant_id;
use flare_im_service_kit::audit_effects::durable::StaffAudit;
use flare_im_service_kit::staff_context::{VerifiedStaff, require_staff};
use flare_server_core::auth::core_admin::permission::{
    CAPABILITIES_READ, CAPABILITIES_WRITE, HOOKS_READ, HOOKS_WRITE, PLUGINS_READ, PLUGINS_WRITE,
};
use flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata;
use flare_server_core::{SignedActorType, SignedContextVerifier, VerifiedContext};
use prost_types::Timestamp;
use tonic::{Request, Response, Status};

use super::administer::{
    HookAdministrator, READ_OPERATIONS, WRITE_OPERATIONS, dispatch_hook_administer,
};
use crate::application::handler::admin_dispatch::{
    AdminCall, AdminEvidence, AdminIntent, AdminOutcome, AdminPluginRequest, admin_evidence_action,
    dispatch_admin,
};
use crate::application::handler::{VerifiedDispatchOrigin, dispatch_capability_command};
use crate::application::queries::list_registered_capabilities;
use crate::domain::capability::PluginHealthProbe;
use crate::domain::capability::{CapabilityDispatchCommand, CapabilityPolicyBackend};
use crate::domain::model::{PluginEndpoint, validate_plugin_endpoint};
use crate::infrastructure::capability::plugin_channel::resolve_plugin_channel;
use crate::infrastructure::capability::plugin_contract::LABEL_HEALTH_PROTOCOL;
use crate::infrastructure::capability::{
    CapabilityExtensionRegistry, DispatchRateLimiter, PluginRouteBook,
};
use crate::infrastructure::config::CapabilityRuntimeConfig;
use crate::infrastructure::persistence::PostgresCapabilityAuditLog;
use crate::interface::grpc::hooks::HookServiceServer;
use crate::interface::grpc::shared::helpers::{capability_error_to_status, trace_id_from_request};

/// 可观测计数（后续可对接 Prometheus / OTel）。
#[derive(Debug, Default)]
pub struct CapabilityInvocationMetrics {
    dispatch_ok: AtomicU64,
    dispatch_err: AtomicU64,
    dispatch_deadline: AtomicU64,
    dispatch_rate_limited: AtomicU64,
    policy_grant: AtomicU64,
    policy_revoke: AtomicU64,
    policy_tenant_switch: AtomicU64,
}

impl CapabilityInvocationMetrics {
    pub fn snapshot(&self) -> CapabilityMetricsSnapshot {
        CapabilityMetricsSnapshot {
            dispatch_ok: self.dispatch_ok.load(Ordering::Relaxed),
            dispatch_err: self.dispatch_err.load(Ordering::Relaxed),
            dispatch_deadline: self.dispatch_deadline.load(Ordering::Relaxed),
            dispatch_rate_limited: self.dispatch_rate_limited.load(Ordering::Relaxed),
            policy_grant: self.policy_grant.load(Ordering::Relaxed),
            policy_revoke: self.policy_revoke.load(Ordering::Relaxed),
            policy_tenant_switch: self.policy_tenant_switch.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CapabilityMetricsSnapshot {
    pub dispatch_ok: u64,
    pub dispatch_err: u64,
    pub dispatch_deadline: u64,
    pub dispatch_rate_limited: u64,
    pub policy_grant: u64,
    pub policy_revoke: u64,
    pub policy_tenant_switch: u64,
}

/// Capability 控制面 gRPC：能力目录 / 策略 / Dispatch / 插件路由 / Hook 治理（`Administer`）。
pub struct CapabilityGrpcServer {
    registry: CapabilityExtensionRegistry,
    policy: Arc<dyn CapabilityPolicyBackend>,
    runtime: Arc<CapabilityRuntimeConfig>,
    audit: Option<Arc<PostgresCapabilityAuditLog>>,
    rate_limiter: Option<Arc<DispatchRateLimiter>>,
    metrics: Arc<CapabilityInvocationMetrics>,
    /// Hook 配置等治理命令（CQRS 写模型）；无 DB 时不挂载。
    hook_governance: Option<Arc<HookServiceServer>>,
    /// 扩展插件 endpoint 登记（开发期进程内簿；可换持久化）。
    plugin_routes: Arc<PluginRouteBook>,
    /// Dispatch requires cell-signed identity (any component key); plugin self-registration
    /// verifies its Service context with the same key set.
    dispatch_verifier: Option<SignedContextVerifier>,
    /// The same key set with Staff pinned to `kid=gateway`: the admin RPCs.
    staff_verifier: Option<SignedContextVerifier>,
    pub(super) registration_verifier: Option<flare_server_core::TokenService>,
}

impl CapabilityGrpcServer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        registry: CapabilityExtensionRegistry,
        policy: Arc<dyn CapabilityPolicyBackend>,
        runtime: Arc<CapabilityRuntimeConfig>,
        audit: Option<Arc<PostgresCapabilityAuditLog>>,
        rate_limiter: Option<Arc<DispatchRateLimiter>>,
        metrics: Arc<CapabilityInvocationMetrics>,
        hook_governance: Option<Arc<HookServiceServer>>,
        plugin_routes: Arc<PluginRouteBook>,
        dispatch_verifier: Option<SignedContextVerifier>,
        staff_verifier: Option<SignedContextVerifier>,
    ) -> Self {
        let server = Self {
            registry,
            policy,
            runtime,
            audit,
            rate_limiter,
            metrics,
            hook_governance,
            plugin_routes,
            dispatch_verifier,
            staff_verifier,
            registration_verifier: None,
        };
        server.spawn_plugin_health_checker();
        server
    }

    pub fn metrics(&self) -> CapabilityMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// The admin gate: the forwarded Staff context with `permission`; without a key set every
    /// admin RPC is UNAUTHENTICATED.
    /// `dispatch_admin` (`flare.plugin.v1.admin.query` / `.dispatch`): the payload is
    /// `PayloadJson` `{"pluginId","operation","input","idempotencyKey"?}`; the Staff context must
    /// carry the `plugin:<pluginId>:<permission>` code the operation declares, for one concrete
    /// application. The answer is `PayloadJson`
    /// `{"pluginId","operation","requestId","success","data","errorCode"?}`; a plugin rejection is
    /// `success: false`, not an RPC error.
    async fn administer_plugin(
        &self,
        request: Request<GenericRequest>,
        call: AdminCall,
    ) -> Result<Response<GenericResponse>, Status> {
        let admin = request
            .get_ref()
            .payload
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("payload required"))
            .and_then(|payload| {
                AdminPluginRequest::parse(&payload.value).map_err(Status::invalid_argument)
            })?;
        let staff = self.plugin_staff(&request, &admin)?;
        let trace = trace_id_from_request(&request);
        // Audited like every other governance RPC: inside the Staff scope, the durable evidence
        // (`plugin_admin`, `<pluginId>:<operation>`) is an intent written before the plugin is
        // called (no evidence store, or a failed write: the plugin is not called) and the outcome
        // after it (`*_succeeded`, `*_refused`, `*_unknown`); every attempt is also logged in
        // capability_audit_log. Reads are audited too: an admin page may show personal data (a
        // poll's voter list).
        let plugin_id = admin.plugin_id.clone();
        let operation = admin.operation.clone();
        let effect = match call {
            AdminCall::Query => "query",
            AdminCall::Dispatch => "dispatch",
        };
        let evidence = StaffEffectEvidence {
            audit: self.audit.as_deref(),
            actor_id: &staff.actor_id,
            plugin_id: &plugin_id,
            operation: &operation,
            request_id: &staff.request_id,
            trace_id: trace.as_deref(),
        };
        let outcome = dispatch_admin(
            &staff,
            &self.plugin_routes,
            call,
            admin,
            self.runtime.plugin_call_timeout,
            &evidence,
        )
        .staff_audit(&staff.ctx, "flare-capability")
        .await;
        if let Some(audit) = &self.audit {
            let detail = match &outcome {
                Ok(result) => serde_json::json!({
                    "pluginId": plugin_id,
                    "call": effect,
                    "requestId": result.request_id,
                    "success": result.success,
                    "errorCode": result.error_code,
                    "auditTarget": result.audit_target,
                    "reason": staff.reason,
                }),
                Err(error) => serde_json::json!({
                    "pluginId": plugin_id,
                    "call": effect,
                    "requestId": staff.request_id,
                    "success": false,
                    "error": error.plugin_code().unwrap_or("CAPABILITY_ERROR"),
                    "reason": staff.reason,
                }),
            };
            audit
                .record_policy_event(
                    "plugin_admin",
                    &staff.tenant_id,
                    Some(staff.actor_id.as_str()),
                    None,
                    Some(operation.as_str()),
                    detail,
                    trace.as_deref(),
                )
                .await;
        }
        let result = outcome.map_err(capability_error_to_status)?;
        let value = serde_json::to_vec(&result)
            .map_err(|_| Status::internal("plugin admin result encoding"))?;
        Ok(Response::new(GenericResponse {
            ok: true,
            request_id: result.request_id,
            payload: Some(prost_types::Any {
                type_url: "type.googleapis.com/flare.capability.v1.PayloadJson".into(),
                value,
            }),
            error_code: String::new(),
            error_message: String::new(),
        }))
    }

    /// The forwarded Staff context of a plugin admin call: its permission must be one of the
    /// plugin's staff codes (`plugin:<pluginId>:<permission>`); `dispatch_admin` then requires the
    /// exact code the operation declares, which needs the tenant's manifest.
    fn plugin_staff<T>(
        &self,
        request: &Request<T>,
        admin: &AdminPluginRequest,
    ) -> Result<VerifiedStaff, Status> {
        let verifier = self
            .staff_verifier
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("staff context verifier not configured"))?;
        let ctx = verify_signed_context_metadata(request.metadata(), verifier)?;
        let claims = flare_im_service_kit::staff_context::staff_claims_of(&ctx)
            .ok_or_else(|| Status::permission_denied("staff context required"))?
            .clone();
        if !claims
            .permission
            .as_deref()
            .is_some_and(|code| admin.is_plugin_permission(code))
        {
            return Err(Status::permission_denied(
                "staff permission is not a permission of this plugin",
            ));
        }
        VerifiedStaff::from_verified(&claims, ctx)
    }

    fn staff<T>(&self, request: &Request<T>, permission: &str) -> Result<VerifiedStaff, Status> {
        let verifier = self
            .staff_verifier
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("staff context verifier not configured"))?;
        require_staff(request, verifier, permission)
    }

    /// Plugin route-book RPCs: platform staff with `permission`, or a Cell-signed `Service`
    /// context (a plugin host registering itself) whose signed tenant is `tenant_id`. The two
    /// are told apart by the verified actor type; the key set is shared.
    pub(super) fn plugin_route_actor<T>(
        &self,
        request: &Request<T>,
        permission: &str,
        tenant_id: &str,
    ) -> Result<PluginRouteActor, Status> {
        let verifier = self
            .dispatch_verifier
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("signed context verifier not configured"))?;
        let ctx = verify_signed_context_metadata(request.metadata(), verifier)?;
        let claims = ctx
            .get_data::<VerifiedContext>()
            .and_then(VerifiedContext::claims)
            .ok_or_else(|| Status::unauthenticated("signed internal context required"))?;
        if claims.actor_type == SignedActorType::Service {
            if claims.tenant_id != tenant_id {
                return Err(Status::permission_denied(
                    "plugin service context tenant mismatch",
                ));
            }
            return Ok(PluginRouteActor::Service {
                plugin_id: claims.actor_id.clone(),
                request_id: claims.request_id.clone(),
            });
        }
        let staff = self.staff(request, permission)?;
        if permission == PLUGINS_WRITE && !staff.is_platform_staff() {
            return Err(Status::permission_denied(
                "plugin registration is for platform staff only",
            ));
        }
        staff.require_tenant(tenant_id)?;
        Ok(PluginRouteActor::Staff(staff))
    }

    pub fn runtime_config(&self) -> Arc<CapabilityRuntimeConfig> {
        Arc::clone(&self.runtime)
    }

    fn spawn_plugin_health_checker(&self) {
        let routes = Arc::clone(&self.plugin_routes);
        let runtime = Arc::clone(&self.runtime);
        let registry = self.registry.clone();
        tokio::spawn(async move {
            // 探针表在装配后即固定，取一次即可；每轮再取会让健康循环
            // 反复抢注册表的读锁。
            let probes = registry.health_probes().await;
            let mut ticker = tokio::time::interval(runtime.plugin_health_check_interval);
            loop {
                ticker.tick().await;
                let snapshots = routes.list_snapshots();
                let mut healthy = Vec::new();
                for snapshot in snapshots {
                    let result = check_plugin_health(
                        snapshot.instance.grpc_authority.as_str(),
                        snapshot
                            .instance
                            .labels
                            .get(LABEL_HEALTH_PROTOCOL)
                            .map(String::as_str),
                        &probes,
                        runtime.plugin_call_timeout,
                    )
                    .await;
                    match result {
                        Ok(()) => {
                            routes.mark_health(
                                snapshot.tenant_id.as_str(),
                                snapshot.instance.plugin_id.as_str(),
                                true,
                                None,
                            );
                            healthy.push((snapshot.tenant_id, snapshot.instance.plugin_id));
                        }
                        Err(err) => routes.mark_health(
                            snapshot.tenant_id.as_str(),
                            snapshot.instance.plugin_id.as_str(),
                            false,
                            Some(err),
                        ),
                    }
                }
                // One statement per round for every registration that passed; the TTL cleanup
                // reads `last_healthy_at`. A storage failure only delays that bookkeeping.
                if let Err(error) = routes.persist_health(&healthy).await {
                    tracing::warn!(%error, "recording plugin health in capability_plugin_endpoints failed");
                }
            }
        });
    }
}

fn domain_descriptor_to_proto(
    d: crate::domain::capability::CapabilityDescriptor,
) -> ProtoDescriptor {
    ProtoDescriptor {
        capability_id: d.capability_id,
        plugin_id: d.plugin_id,
        version: d.version,
        scope: d.scope,
        visibility: d.visibility,
        permissions: d.permissions,
        message_types: d.message_types,
        timeout_ms: d.timeout_ms,
        description: d.description,
    }
}

fn ts_from_chrono(dt: chrono::DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

fn require_canonical_dispatch_tenant_id(tenant_id: &str) -> Result<String, Status> {
    let normalized = normalize_tenant_id(tenant_id);
    if normalized != tenant_id {
        return Err(Status::invalid_argument("tenant_id must be canonical"));
    }
    Ok(normalized)
}

/// 按插件**声明**的健康协议做检查。
///
/// 这里曾经内嵌着某个具体协议的客户端与一段 `if capability_id == ...`。
/// 选择依据改成插件声明的标签之后，实现仍然留在通用路径里 —— 加第二种
/// 特殊探活语义的插件时，还是只能回到这个文件加分支。
///
/// 现在通用侧只做两件事：按声明的协议名找已注册探针，找不到就用通用协议。
/// 探针由组合根注册，通用侧对任何具体协议一无所知。
async fn check_plugin_health(
    grpc_authority: &str,
    health_protocol: Option<&str>,
    probes: &[Arc<dyn PluginHealthProbe>],
    timeout: std::time::Duration,
) -> Result<(), String> {
    if let Some(protocol) = health_protocol
        && let Some(probe) = probes.iter().find(|p| p.protocol() == protocol)
    {
        return probe.probe(grpc_authority, timeout).await;
    }

    // 声明了协议却没有对应探针：说明部署缺少该 kind 的装配。
    // 这时**不能**静默走通用协议 —— 那会把「装配缺失」伪装成「健康」。
    if let Some(protocol) = health_protocol {
        return Err(format!(
            "declared health_protocol {protocol} has no registered probe; \
             deployment is missing this plugin kind's wiring"
        ));
    }

    let channel = tokio::time::timeout(timeout, resolve_plugin_channel(grpc_authority))
        .await
        .map_err(|_| format!("plugin channel resolve timeout: {grpc_authority}"))??;
    let mut client = ExtensionPluginClient::new(channel);
    let request = Request::new(GenericRequest {
        operation: "flare.capability.v1.health_check".to_string(),
        metadata: std::collections::HashMap::new(),
        payload: None,
        request_id: uuid::Uuid::new_v4().to_string(),
    });

    let response = tokio::time::timeout(timeout, client.call(request))
        .await
        .map_err(|_| "plugin health_check timeout".to_string())?
        .map_err(|e| e.to_string())?
        .into_inner();
    if response.ok {
        Ok(())
    } else {
        Err(format!(
            "plugin health_check returned error: {} {}",
            response.error_code, response.error_message
        ))
    }
}

/// Who may touch the plugin route book: platform staff, or the plugin host's own service identity.
#[derive(Debug, Clone)]
pub(super) enum PluginRouteActor {
    Staff(VerifiedStaff),
    Service {
        plugin_id: String,
        request_id: String,
    },
}

impl PluginRouteActor {
    fn id(&self) -> &str {
        match self {
            Self::Staff(staff) => &staff.actor_id,
            Self::Service { plugin_id, .. } => plugin_id,
        }
    }
    fn require_registration_binding(
        &self,
        plugin_id: &str,
        request_id: &str,
    ) -> Result<(), Status> {
        if let Self::Service {
            plugin_id: signed_plugin,
            request_id: signed_request,
        } = self
            && (signed_plugin != plugin_id || request_id.is_empty() || signed_request != request_id)
        {
            return Err(Status::permission_denied(
                "plugin registration identity does not match signed context",
            ));
        }
        Ok(())
    }
    async fn run<F: std::future::Future>(&self, work: F) -> F::Output {
        match self {
            Self::Staff(staff) => work.staff_audit(&staff.ctx, "flare-capability").await,
            Self::Service { .. } => work.await,
        }
    }
}

#[tonic::async_trait]
impl CapabilityService for CapabilityGrpcServer {
    async fn list_capabilities(
        &self,
        request: Request<ListCapabilitiesRequest>,
    ) -> Result<Response<ListCapabilitiesResponse>, Status> {
        let _staff = self.staff(&request, CAPABILITIES_READ)?;
        let capabilities = list_registered_capabilities()
            .into_iter()
            .map(domain_descriptor_to_proto)
            .collect();
        Ok(Response::new(ListCapabilitiesResponse { capabilities }))
    }

    async fn list_user_capabilities(
        &self,
        request: Request<ListUserCapabilitiesRequest>,
    ) -> Result<Response<ListUserCapabilitiesResponse>, Status> {
        let staff = self.staff(&request, CAPABILITIES_READ)?;
        let r = request.into_inner();
        staff.require_tenant(&r.tenant_id)?;
        if r.tenant_id.is_empty() || r.user_id.is_empty() {
            return Err(Status::invalid_argument("tenant_id and user_id required"));
        }
        let domain_grants = self
            .policy
            .list_user_grants(&r.tenant_id, &r.user_id)
            .await
            .map_err(capability_error_to_status)?;
        let grants: Vec<ProtoGrant> = domain_grants
            .into_iter()
            .map(|g| ProtoGrant {
                tenant_id: g.tenant_id,
                user_id: g.user_id,
                capability_id: g.capability_id,
                granted_at: Some(ts_from_chrono(g.granted_at)),
                expires_at: g.expires_at.map(ts_from_chrono),
                plan_code: g.plan_code.unwrap_or_default(),
                source: g.source.unwrap_or_default(),
            })
            .collect();
        Ok(Response::new(ListUserCapabilitiesResponse { grants }))
    }

    async fn dispatch(
        &self,
        request: Request<DispatchCapabilityRequest>,
    ) -> Result<Response<DispatchCapabilityResponse>, Status> {
        let verifier = self.dispatch_verifier.as_ref().ok_or_else(|| {
            Status::unauthenticated("signed dispatch context verifier not configured")
        })?;
        let origin = VerifiedDispatchOrigin::verify(request.metadata(), verifier)?;
        let r = request.into_inner();

        // Bind the body before policy lookup or plugin routing; normalization or
        // defaults must not turn a caller-supplied tenant/user into signed identity.
        origin.require_user_binding(&r.tenant_id, &r.user_id, &r.request_id)?;
        let normalized_tenant_id = require_canonical_dispatch_tenant_id(&r.tenant_id)?;
        let ctx = origin.into_context();

        if r.capability_id.is_empty() {
            return Err(Status::invalid_argument("capability_id required"));
        }
        if r.message_id.len() > 256 || r.message_id.chars().any(char::is_control) {
            return Err(Status::invalid_argument("invalid message_id"));
        }
        // 规范写法检查已拒绝空租户，这里的租户必然非空，日志直接记它，不再有 "0" 兜底（W13-D）。
        tracing::trace!(
            capability_id = %r.capability_id,
            tenant_id = %normalized_tenant_id,
            "capability.dispatch"
        );

        let payload_raw = r.payload_json.as_str();
        if payload_raw.len() > self.runtime.max_payload_json_bytes {
            self.metrics.dispatch_err.fetch_add(1, Ordering::Relaxed);
            return Err(Status::invalid_argument(format!(
                "payload_json exceeds max {} bytes",
                self.runtime.max_payload_json_bytes
            )));
        }

        let user_id = if r.user_id.is_empty() {
            None
        } else {
            Some(r.user_id.as_str())
        };

        if let Some(ref lim) = self.rate_limiter {
            // 限流键的租户就是已通过规范写法检查的租户（必然非空），不再有 "_" 占位（W14-D）。
            let u = user_id.unwrap_or("_");
            if !lim.check_and_record(&normalized_tenant_id, u) {
                self.metrics
                    .dispatch_rate_limited
                    .fetch_add(1, Ordering::Relaxed);
                return Err(Status::resource_exhausted(
                    "capability dispatch rate limit exceeded for this tenant and user (per minute)",
                ));
            }
        }

        let payload = if r.payload_json.trim().is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_str(&r.payload_json)
                .map_err(|e| Status::invalid_argument(format!("payload_json: {e}")))?
        };
        let cmd = CapabilityDispatchCommand {
            capability_id: r.capability_id,
            tenant_id: Some(normalized_tenant_id),
            user_id: user_id.map(str::to_string),
            conversation_id: if r.conversation_id.is_empty() {
                None
            } else {
                Some(r.conversation_id)
            },
            payload: Some(payload),
            request_id: if r.request_id.is_empty() {
                None
            } else {
                Some(r.request_id)
            },
            message_id: (!r.message_id.is_empty()).then_some(r.message_id),
        };

        let plugin_health_stale = self.runtime.plugin_health_check_interval.saturating_mul(2);
        let fut = dispatch_capability_command(
            &ctx,
            &self.registry,
            &self.plugin_routes,
            &self.policy,
            self.runtime.plugin_call_timeout,
            plugin_health_stale,
            &cmd,
        );
        let out = match tokio::time::timeout(self.runtime.dispatch_timeout, fut).await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                self.metrics.dispatch_err.fetch_add(1, Ordering::Relaxed);
                return Err(capability_error_to_status(e));
            }
            Err(_) => {
                self.metrics
                    .dispatch_deadline
                    .fetch_add(1, Ordering::Relaxed);
                return Err(Status::deadline_exceeded(format!(
                    "capability dispatch exceeded timeout {:?}",
                    self.runtime.dispatch_timeout
                )));
            }
        };

        self.metrics.dispatch_ok.fetch_add(1, Ordering::Relaxed);
        let result_json = serde_json::to_string(&out.data).unwrap_or_else(|_| "{}".into());
        let result = ProtoDispatchResult {
            request_id: out.request_id,
            success: out.success,
            plugin_id: out.plugin_id,
            capability_id: out.capability_id,
            result_json,
            error_message: out.error.unwrap_or_default(),
        };
        Ok(Response::new(DispatchCapabilityResponse {
            result: Some(result),
        }))
    }

    async fn grant_user_capability(
        &self,
        request: Request<GrantUserCapabilityRequest>,
    ) -> Result<Response<GrantUserCapabilityResponse>, Status> {
        let staff = self.staff(&request, CAPABILITIES_WRITE)?;
        let trace = staff
            .trace_id
            .clone()
            .or_else(|| trace_id_from_request(&request));
        let r = request.into_inner();
        staff.require_tenant(&r.tenant_id)?;
        if r.tenant_id.is_empty() || r.user_id.is_empty() || r.capability_id.is_empty() {
            return Err(Status::invalid_argument(
                "tenant_id, user_id, capability_id required",
            ));
        }
        let expires_at = if r.expires_at_rfc3339.trim().is_empty() {
            None
        } else {
            Some(
                chrono::DateTime::parse_from_rfc3339(&r.expires_at_rfc3339)
                    .map_err(|e| Status::invalid_argument(format!("expires_at_rfc3339: {e}")))?
                    .with_timezone(&Utc),
            )
        };
        self.policy
            .grant_user_capability(
                &r.tenant_id,
                &r.user_id,
                &r.capability_id,
                expires_at,
                if r.plan_code.is_empty() {
                    None
                } else {
                    Some(r.plan_code.clone())
                },
                if r.source.is_empty() {
                    None
                } else {
                    Some(r.source.clone())
                },
            )
            .staff_audit(&staff.ctx, "flare-capability")
            .await
            .map_err(capability_error_to_status)?;

        self.metrics.policy_grant.fetch_add(1, Ordering::Relaxed);
        if let Some(ref a) = self.audit {
            let detail = serde_json::json!({
                "plan_code": r.plan_code,
                "source": r.source,
                "expires_at_rfc3339": r.expires_at_rfc3339,
            });
            a.record_policy_event(
                "grant",
                &r.tenant_id,
                Some(staff.actor_id.as_str()),
                Some(r.user_id.as_str()),
                Some(r.capability_id.as_str()),
                detail,
                trace.as_deref(),
            )
            .await;
        }

        Ok(Response::new(GrantUserCapabilityResponse {
            message: "granted".into(),
        }))
    }

    async fn revoke_user_capability(
        &self,
        request: Request<RevokeUserCapabilityRequest>,
    ) -> Result<Response<RevokeUserCapabilityResponse>, Status> {
        let staff = self.staff(&request, CAPABILITIES_WRITE)?;
        let trace = staff
            .trace_id
            .clone()
            .or_else(|| trace_id_from_request(&request));
        let r = request.into_inner();
        staff.require_tenant(&r.tenant_id)?;
        if r.tenant_id.is_empty() || r.user_id.is_empty() || r.capability_id.is_empty() {
            return Err(Status::invalid_argument(
                "tenant_id, user_id, capability_id required",
            ));
        }
        self.policy
            .revoke_user_capability(&r.tenant_id, &r.user_id, &r.capability_id)
            .staff_audit(&staff.ctx, "flare-capability")
            .await
            .map_err(capability_error_to_status)?;

        self.metrics.policy_revoke.fetch_add(1, Ordering::Relaxed);
        if let Some(ref a) = self.audit {
            a.record_policy_event(
                "revoke",
                &r.tenant_id,
                Some(staff.actor_id.as_str()),
                Some(r.user_id.as_str()),
                Some(r.capability_id.as_str()),
                serde_json::json!({}),
                trace.as_deref(),
            )
            .await;
        }

        Ok(Response::new(RevokeUserCapabilityResponse {
            message: "revoked".into(),
        }))
    }

    async fn set_tenant_capability_switch(
        &self,
        request: Request<SetTenantCapabilitySwitchRequest>,
    ) -> Result<Response<SetTenantCapabilitySwitchResponse>, Status> {
        let staff = self.staff(&request, CAPABILITIES_WRITE)?;
        let trace = staff
            .trace_id
            .clone()
            .or_else(|| trace_id_from_request(&request));
        let r = request.into_inner();
        staff.require_tenant(&r.tenant_id)?;
        if r.tenant_id.is_empty() || r.capability_id.is_empty() {
            return Err(Status::invalid_argument(
                "tenant_id, capability_id required",
            ));
        }
        self.policy
            .set_tenant_capability(&r.tenant_id, &r.capability_id, r.enabled)
            .staff_audit(&staff.ctx, "flare-capability")
            .await
            .map_err(capability_error_to_status)?;

        self.metrics
            .policy_tenant_switch
            .fetch_add(1, Ordering::Relaxed);
        if let Some(ref a) = self.audit {
            a.record_policy_event(
                "tenant_switch",
                &r.tenant_id,
                Some(staff.actor_id.as_str()),
                None,
                Some(r.capability_id.as_str()),
                serde_json::json!({ "enabled": r.enabled }),
                trace.as_deref(),
            )
            .await;
        }

        Ok(Response::new(SetTenantCapabilitySwitchResponse {
            message: "tenant capability switch updated".into(),
        }))
    }

    async fn register_plugin_endpoint(
        &self,
        request: Request<RegisterPluginEndpointRequest>,
    ) -> Result<Response<RegisterPluginEndpointResponse>, Status> {
        let actor = self.registration_actor(
            &request,
            &super::registration::register_intent(request.get_ref()),
        )?;
        let r = request.into_inner();
        actor.require_registration_binding(&r.plugin_id, &r.request_id)?;
        if r.tenant_id.is_empty() || r.plugin_id.is_empty() || r.capability_id.is_empty() {
            return Err(Status::invalid_argument(
                "tenant_id, plugin_id, capability_id required",
            ));
        }
        if r.grpc_authority.is_empty() {
            return Err(Status::invalid_argument("grpc_authority required"));
        }
        // 声明是注册的必要条件；配置发现不能代替可信目录/授权流程。
        let endpoint = PluginEndpoint {
            tenant_id: r.tenant_id,
            instance: RegisteredPluginInstance {
                plugin_id: r.plugin_id,
                capability_id: r.capability_id,
                grpc_authority: r.grpc_authority,
                labels: r.labels,
                plugin_version: r.plugin_version,
                api_version: r.api_version,
                manifest_sha256: r.manifest_sha256,
                declared_operations: r.declared_operations,
                unverified: false,
            },
        };
        // 先校验再记日志或回显：plugin_id、capability_id 与声明的每一项来自 Admin 接口和插件自报，
        // 校验过的才是标识（见 `is_plugin_identifier`），带换行的值伪造不了日志行。
        validate_plugin_endpoint(&endpoint).map_err(Status::invalid_argument)?;
        let instance = &endpoint.instance;
        flare_im_capability_core::plugin::registration::validate_declaration(
            &instance.plugin_version,
            &instance.api_version,
            &instance.manifest_sha256,
            &instance.declared_operations,
            &instance.capability_id,
        )
        .map_err(Status::invalid_argument)?;
        tracing::debug!(registered_by = actor.id(), plugin_id = %instance.plugin_id, "plugin declaration validated");
        // 有库时先写 capability_plugin_endpoints，写成功才进本进程的读模型；其他实例重读后收敛。
        actor
            .run(
                self.plugin_routes
                    .register(endpoint.tenant_id.as_str(), endpoint.instance),
            )
            .await
            .map_err(|_| Status::unavailable("plugin registration storage is unavailable"))?;
        Ok(Response::new(RegisterPluginEndpointResponse {
            accepted: true,
            message: "registered".into(),
        }))
    }

    async fn deregister_plugin_endpoint(
        &self,
        request: Request<DeregisterPluginEndpointRequest>,
    ) -> Result<Response<DeregisterPluginEndpointResponse>, Status> {
        let actor = self.registration_actor(
            &request,
            &super::registration::deregister_intent(request.get_ref()),
        )?;
        let r = request.into_inner();
        actor.require_registration_binding(&r.plugin_id, &r.request_id)?;
        if r.tenant_id.is_empty() || r.plugin_id.is_empty() {
            return Err(Status::invalid_argument("tenant_id, plugin_id required"));
        }
        let removed = actor
            .run(
                self.plugin_routes
                    .deregister(r.tenant_id.as_str(), r.plugin_id.as_str()),
            )
            .await
            .map_err(|_| Status::unavailable("plugin registration storage is unavailable"))?;
        Ok(Response::new(DeregisterPluginEndpointResponse {
            accepted: removed,
        }))
    }

    async fn list_registered_plugins(
        &self,
        request: Request<ListRegisteredPluginsRequest>,
    ) -> Result<Response<ListRegisteredPluginsResponse>, Status> {
        let actor =
            self.plugin_route_actor(&request, PLUGINS_READ, &request.get_ref().tenant_id)?;
        let r = request.into_inner();
        if r.tenant_id.is_empty() {
            return Err(Status::invalid_argument("tenant_id required"));
        }
        let mut instances = self
            .plugin_routes
            .list_filtered(r.tenant_id.as_str(), r.capability_id.as_str());
        if let PluginRouteActor::Service { plugin_id, .. } = actor {
            instances.retain(|instance| instance.plugin_id == plugin_id);
        }
        Ok(Response::new(ListRegisteredPluginsResponse { instances }))
    }

    async fn administer(
        &self,
        request: Request<GenericRequest>,
    ) -> Result<Response<GenericResponse>, Status> {
        // The operation decides the code before anything is dispatched: reads need
        // core:hooks:read, writes core:hooks:write; an unknown operation is refused with the
        // read code so it cannot probe for a weaker gate.
        let operation = request.get_ref().operation.as_str();
        if let Some(call) = AdminCall::from_operation(operation) {
            return self.administer_plugin(request, call).await;
        }
        let permission = if WRITE_OPERATIONS.contains(&operation) {
            HOOKS_WRITE
        } else if READ_OPERATIONS.contains(&operation) {
            HOOKS_READ
        } else {
            self.staff(&request, HOOKS_READ)?;
            return Err(Status::unimplemented(format!(
                "unknown administer operation: {operation}"
            )));
        };
        let staff = self.staff(&request, permission)?;
        let Some(ref gov) = self.hook_governance else {
            return Err(Status::failed_precondition(
                "hook governance disabled: configure database_url for HookService",
            ));
        };
        dispatch_hook_administer(
            gov.as_ref(),
            request.into_inner(),
            &HookAdministrator::from_staff(&staff),
        )
        .await
    }
}

/// [`AdminEvidence`] on the durable audit outbox (`record_staff_effect`, inside the Staff scope);
/// the intent also keeps the stated reason and the audited target in `capability_audit_log`, in
/// the same transaction. Without an audit store no admin call reaches a plugin.
struct StaffEffectEvidence<'a> {
    audit: Option<&'a PostgresCapabilityAuditLog>,
    actor_id: &'a str,
    plugin_id: &'a str,
    operation: &'a str,
    request_id: &'a str,
    trace_id: Option<&'a str>,
}

#[async_trait::async_trait]
impl AdminEvidence for StaffEffectEvidence<'_> {
    async fn intent(
        &self,
        tenant_id: &str,
        aggregate_id: &str,
        call: AdminCall,
        intent: AdminIntent<'_>,
    ) -> std::result::Result<(), String> {
        let audit = self
            .audit
            .ok_or_else(|| "plugin admin audit store is not configured".to_owned())?;
        let action = admin_evidence_action(call, None);
        let detail = serde_json::json!({
            "pluginId": self.plugin_id,
            "call": action,
            "requestId": self.request_id,
            "reason": intent.reason,
            "auditTarget": intent.audit_target,
        });
        audit
            .record_staff_intent(
                tenant_id,
                "plugin_admin",
                aggregate_id,
                action,
                self.actor_id,
                self.operation,
                detail,
                self.trace_id,
            )
            .await
            .map_err(|error| error.to_string())
    }

    async fn outcome(
        &self,
        tenant_id: &str,
        aggregate_id: &str,
        call: AdminCall,
        outcome: AdminOutcome,
    ) -> std::result::Result<(), String> {
        self.record(
            tenant_id,
            aggregate_id,
            admin_evidence_action(call, Some(outcome)),
        )
        .await
    }
}

impl StaffEffectEvidence<'_> {
    async fn record(
        &self,
        tenant_id: &str,
        aggregate_id: &str,
        action: &'static str,
    ) -> std::result::Result<(), String> {
        let audit = self
            .audit
            .ok_or_else(|| "plugin admin audit store is not configured".to_owned())?;
        audit
            .record_staff_effect(tenant_id, "plugin_admin", aggregate_id, action)
            .await
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod dispatch_identity_tests {
    use super::require_canonical_dispatch_tenant_id;

    #[test]
    fn dispatch_rejects_tenant_aliases_that_would_change_signed_identity() {
        assert_eq!(require_canonical_dispatch_tenant_id("0").unwrap(), "0");
        assert_eq!(
            require_canonical_dispatch_tenant_id("tenant-a").unwrap(),
            "tenant-a"
        );
        for alias in ["", "default", " tenant-a", "tenant-a "] {
            assert!(require_canonical_dispatch_tenant_id(alias).is_err());
        }
    }
}

#[cfg(test)]
mod plugin_route_book_tests {
    //! 插件簿三个 RPC 的两条身份分支：网关签的平台员工 Staff 上下文，与 Cell 签的插件宿主
    //! Service 上下文；负向：缺上下文、hook 钥签的 Staff、组织员工登记、权限码不符、跨租户。

    use std::time::Duration;

    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_server_core::auth::{KeySet, kid};
    use flare_server_core::{SignedContextIdentity, SignedContextSigner};
    use tonic::Code;

    use super::*;
    use crate::infrastructure::capability::InMemoryCapabilityGrants;

    const GATEWAY_KEY: [u8; 32] = [47; 32];
    const HOOK_KEY: [u8; 32] = [48; 32];

    fn pem(seed: [u8; 32]) -> (String, String) {
        let key = SigningKey::from_bytes(&seed);
        (
            key.to_pkcs8_pem(Default::default()).unwrap().to_string(),
            key.verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap(),
        )
    }

    fn signer(seed: [u8; 32], key_id: &str) -> SignedContextSigner {
        SignedContextSigner::from_private_key(&pem(seed).0)
            .unwrap()
            .with_kid(key_id)
            .unwrap()
    }

    fn verifier() -> SignedContextVerifier {
        let mut keys = KeySet::empty();
        keys.insert(kid::GATEWAY, &pem(GATEWAY_KEY).1).unwrap();
        keys.insert(kid::HOOK, &pem(HOOK_KEY).1).unwrap();
        SignedContextVerifier::from_key_set(keys)
    }

    fn plugin(plugin_id: &str) -> RegisteredPluginInstance {
        RegisteredPluginInstance {
            plugin_id: plugin_id.to_string(),
            capability_id: "example.poll".to_string(),
            grpc_authority: "http://127.0.0.1:9".to_string(),
            plugin_version: "1.0.0".to_string(),
            api_version: "1.0".to_string(),
            manifest_sha256: "a".repeat(64),
            ..Default::default()
        }
    }

    fn server() -> CapabilityGrpcServer {
        let routes = Arc::new(PluginRouteBook::new());
        routes.upsert("tenant-a", plugin("plugin-a"));
        routes.upsert("tenant-b", plugin("plugin-b"));
        let keys = verifier();
        CapabilityGrpcServer::new(
            CapabilityExtensionRegistry::new(),
            Arc::new(InMemoryCapabilityGrants::new()),
            Arc::new(CapabilityRuntimeConfig {
                max_payload_json_bytes: 1024,
                dispatch_timeout: Duration::from_secs(1),
                dispatch_max_per_minute: None,
                plugin_health_check_interval: Duration::from_secs(3600),
                plugin_call_timeout: Duration::from_millis(50),
                plugin_route_refresh_interval: Duration::from_secs(10),
                plugin_route_ttl: Duration::from_secs(86_400),
            }),
            None,
            None,
            Arc::new(CapabilityInvocationMetrics::default()),
            None,
            routes,
            Some(keys.clone()),
            Some(keys.require_staff_kid([kid::GATEWAY])),
        )
    }

    fn staff_token(
        seed: [u8; 32],
        key_id: &str,
        tenant: &str,
        permission: &str,
        org: Option<&str>,
    ) -> String {
        signer(seed, key_id)
            .sign(
                &SignedContextIdentity::new(SignedActorType::Staff, "employee-1", tenant, "req-1")
                    .with_staff_grant(org.map(str::to_owned), Some(permission.into()), 2),
            )
            .unwrap()
    }

    fn service_token(tenant: &str, plugin_id: &str) -> String {
        signer(GATEWAY_KEY, kid::GATEWAY)
            .sign(&SignedContextIdentity::new(
                SignedActorType::Service,
                plugin_id,
                tenant,
                "req-2",
            ))
            .unwrap()
    }

    #[tokio::test]
    async fn delegated_registration_binds_body_and_cannot_become_a_cell_or_admin_credential() {
        use flare_im_capability_core::plugin::workload::*;
        use flare_server_core::TokenService;
        let (private, public) = pem([83; 32]);
        let issuer = TokenService::from_ed25519_private_key(
            &private,
            REGISTRATION_ISSUER,
            REGISTRATION_TTL_SECS,
        )
        .unwrap();
        let registration_verifier =
            TokenService::from_ed25519_public_key(&public, REGISTRATION_ISSUER).unwrap();
        let server = server().with_registration_verifier(registration_verifier);
        let body = register("unused", "tenant-a", "plugin-c").into_inner();
        let intent = super::super::registration::register_intent(&body);
        let token = issuer
            .generate_with_ed25519(
                &intent.plugin_id,
                Some(&intent.tenant_id),
                REGISTRATION_TOKEN_USE,
                &intent.audience().unwrap(),
            )
            .unwrap();
        let delegated = |body| {
            let mut req = Request::new(body);
            req.metadata_mut()
                .insert(REGISTRATION_HEADER, token.parse().unwrap());
            req
        };
        assert!(
            server
                .register_plugin_endpoint(delegated(body.clone()))
                .await
                .unwrap()
                .get_ref()
                .accepted
        );
        // Exact retries are idempotent, never a grant or arbitrary re-signing capability.
        assert!(
            server
                .register_plugin_endpoint(delegated(body.clone()))
                .await
                .unwrap()
                .get_ref()
                .accepted
        );
        for field in 0..7 {
            let mut changed = body.clone();
            match field {
                0 => changed.tenant_id = "tenant-b".into(),
                1 => changed.plugin_id = "plugin-d".into(),
                2 => changed.request_id = "another".into(),
                3 => changed.grpc_authority = "http://127.0.0.1:10".into(),
                4 => changed.manifest_sha256 = "b".repeat(64),
                5 => changed.plugin_version = "2.0.0".into(),
                _ => {
                    changed
                        .labels
                        .insert("health_protocol".into(), "other".into());
                }
            }
            assert!(
                server
                    .register_plugin_endpoint(delegated(changed))
                    .await
                    .is_err()
            );
        }
        for (subject, tenant, purpose, ttl) in [
            ("other-plugin", "tenant-a", REGISTRATION_TOKEN_USE, 60),
            ("plugin-c", "tenant-b", REGISTRATION_TOKEN_USE, 60),
            ("plugin-c", "tenant-a", "control", 60),
            ("plugin-c", "tenant-a", REGISTRATION_TOKEN_USE, 3600),
        ] {
            let token = TokenService::from_ed25519_private_key(&private, REGISTRATION_ISSUER, ttl)
                .unwrap()
                .generate_with_ed25519(subject, Some(tenant), purpose, &intent.audience().unwrap())
                .unwrap();
            let mut req = Request::new(body.clone());
            req.metadata_mut()
                .insert(REGISTRATION_HEADER, token.parse().unwrap());
            assert!(server.register_plugin_endpoint(req).await.is_err());
        }
        let mut duplicate = delegated(body.clone());
        duplicate
            .metadata_mut()
            .append(REGISTRATION_HEADER, token.parse().unwrap());
        assert!(server.register_plugin_endpoint(duplicate).await.is_err());
        let mut ambiguous = delegated(body.clone());
        ambiguous.metadata_mut().insert(
            "x-flare-ctx",
            service_token("tenant-a", "plugin-c").parse().unwrap(),
        );
        assert!(server.register_plugin_endpoint(ambiguous).await.is_err());
        let mut listing = request(None, "tenant-a");
        listing
            .metadata_mut()
            .insert(REGISTRATION_HEADER, token.parse().unwrap());
        assert!(server.list_registered_plugins(listing).await.is_err());
        assert!(verifier().verify(&token).is_err());
        let withdraw = DeregisterPluginEndpointRequest {
            tenant_id: body.tenant_id,
            plugin_id: body.plugin_id,
            request_id: body.request_id,
        };
        let mut req = Request::new(withdraw.clone());
        req.metadata_mut()
            .insert(REGISTRATION_HEADER, token.parse().unwrap());
        assert!(server.deregister_plugin_endpoint(req).await.is_err());
        let intent = super::super::registration::deregister_intent(&withdraw);
        let token = issuer
            .generate_with_ed25519(
                &intent.plugin_id,
                Some(&intent.tenant_id),
                REGISTRATION_TOKEN_USE,
                &intent.audience().unwrap(),
            )
            .unwrap();
        let mut req = Request::new(withdraw);
        req.metadata_mut()
            .insert(REGISTRATION_HEADER, token.parse().unwrap());
        assert!(
            server
                .deregister_plugin_endpoint(req)
                .await
                .unwrap()
                .get_ref()
                .accepted
        );
    }

    fn request(token: Option<&str>, tenant_id: &str) -> Request<ListRegisteredPluginsRequest> {
        let mut request = Request::new(ListRegisteredPluginsRequest {
            tenant_id: tenant_id.to_string(),
            capability_id: String::new(),
        });
        if let Some(token) = token {
            request
                .metadata_mut()
                .insert("x-flare-ctx", token.parse().unwrap());
        }
        request
    }

    async fn listed(
        server: &CapabilityGrpcServer,
        token: Option<&str>,
        tenant_id: &str,
    ) -> Result<Vec<String>, Code> {
        server
            .list_registered_plugins(request(token, tenant_id))
            .await
            .map(|response| {
                response
                    .into_inner()
                    .instances
                    .into_iter()
                    .map(|instance| instance.plugin_id)
                    .collect()
            })
            .map_err(|status| status.code())
    }

    fn register(
        token: &str,
        tenant_id: &str,
        plugin_id: &str,
    ) -> Request<RegisterPluginEndpointRequest> {
        let mut request = Request::new(RegisterPluginEndpointRequest {
            tenant_id: tenant_id.to_string(),
            plugin_id: plugin_id.to_string(),
            capability_id: "example.poll".to_string(),
            grpc_authority: "http://127.0.0.1:9".to_string(),
            declared_operations: vec!["example.poll".to_string()],
            request_id: "req-2".to_string(),
            plugin_version: "1.0.0".to_string(),
            api_version: "1.0".to_string(),
            manifest_sha256: "a".repeat(64),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-flare-ctx", token.parse().unwrap());
        request
    }

    #[tokio::test]
    async fn plugin_listing_accepts_platform_staff_org_staff_and_the_plugin_service() {
        let server = server();
        let platform = staff_token(GATEWAY_KEY, kid::GATEWAY, "tenant-a", PLUGINS_READ, None);
        let org = staff_token(
            GATEWAY_KEY,
            kid::GATEWAY,
            "tenant-a",
            PLUGINS_READ,
            Some("org-1"),
        );
        let service = service_token("tenant-a", "plugin-a");
        for token in [&platform, &org, &service] {
            assert_eq!(
                listed(&server, Some(token), "tenant-a").await,
                Ok(vec!["plugin-a".to_string()])
            );
            // 跨租户：请求租户不是签名租户。
            assert_eq!(
                listed(&server, Some(token), "tenant-b").await,
                Err(Code::PermissionDenied)
            );
        }
    }

    #[tokio::test]
    async fn plugin_listing_rejects_missing_wrong_kid_and_wrong_permission() {
        let server = server();
        assert_eq!(
            listed(&server, None, "tenant-a").await,
            Err(Code::Unauthenticated)
        );
        let forged = staff_token(HOOK_KEY, kid::HOOK, "tenant-a", PLUGINS_READ, None);
        assert_eq!(
            listed(&server, Some(&forged), "tenant-a").await,
            Err(Code::Unauthenticated)
        );
        let wrong = staff_token(
            GATEWAY_KEY,
            kid::GATEWAY,
            "tenant-a",
            CAPABILITIES_READ,
            None,
        );
        assert_eq!(
            listed(&server, Some(&wrong), "tenant-a").await,
            Err(Code::PermissionDenied)
        );
        let wildcard = staff_token(GATEWAY_KEY, kid::GATEWAY, "*", PLUGINS_READ, None);
        assert_eq!(
            listed(&server, Some(&wildcard), "tenant-a").await,
            Err(Code::PermissionDenied)
        );
    }

    #[tokio::test]
    async fn plugin_registration_is_platform_staff_or_the_plugin_service_itself() {
        let server = server();
        // 组织员工即使持写码也不能登记。
        let org = staff_token(
            GATEWAY_KEY,
            kid::GATEWAY,
            "tenant-a",
            PLUGINS_WRITE,
            Some("org-1"),
        );
        assert_eq!(
            server
                .register_plugin_endpoint(register(&org, "tenant-a", "plugin-c"))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        // 读码不能登记。
        let read = staff_token(GATEWAY_KEY, kid::GATEWAY, "tenant-a", PLUGINS_READ, None);
        assert_eq!(
            server
                .register_plugin_endpoint(register(&read, "tenant-a", "plugin-c"))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        // 平台员工登记自己签名里的应用。
        let platform = staff_token(GATEWAY_KEY, kid::GATEWAY, "tenant-a", PLUGINS_WRITE, None);
        assert!(
            server
                .register_plugin_endpoint(register(&platform, "tenant-a", "plugin-c"))
                .await
                .unwrap()
                .into_inner()
                .accepted
        );
        assert_eq!(
            server
                .register_plugin_endpoint(register(&platform, "tenant-b", "plugin-c"))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        // 插件宿主以自己的 Service 上下文自注册，租户须一致。
        let service = service_token("tenant-b", "plugin-d");
        assert!(
            server
                .register_plugin_endpoint(register(&service, "tenant-b", "plugin-d"))
                .await
                .unwrap()
                .into_inner()
                .accepted
        );
        assert_eq!(
            server
                .register_plugin_endpoint(register(&service, "tenant-a", "plugin-d"))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        let mut ids = listed(&server, Some(&service), "tenant-b").await.unwrap();
        ids.sort();
        assert_eq!(ids, ["plugin-d"]);
        assert_eq!(
            server
                .register_plugin_endpoint(register(&service, "tenant-b", "plugin-b"))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        let mut mismatched_request = register(&service, "tenant-b", "plugin-d");
        mismatched_request.get_mut().request_id = "another-request".into();
        assert_eq!(
            server
                .register_plugin_endpoint(mismatched_request)
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );

        // Registrations the table would refuse are INVALID_ARGUMENT before anything is stored:
        // a wildcard tenant, an over-long plugin id.
        let wildcard = service_token("*", "plugin-e");
        assert_eq!(
            server
                .register_plugin_endpoint(register(&wildcard, "*", "plugin-e"))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
        assert_eq!(
            server
                .register_plugin_endpoint(register(&platform, "tenant-a", &"p".repeat(129)))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }

    /// plugin_id、capability_id 与声明的每一项须是标识（ASCII 字母、数字、`_ - . :`），在记日志
    /// 之前校验，错误中不回显可伪造日志行的原始值。
    #[tokio::test]
    async fn plugin_registration_refuses_non_identifiers_before_logging_them() {
        let server = server();
        let platform = staff_token(GATEWAY_KEY, kid::GATEWAY, "tenant-a", PLUGINS_WRITE, None);
        let unverified = |plugin_id: &str, capability_id: &str| {
            let mut request = register(&platform, "tenant-a", plugin_id);
            request.get_mut().capability_id = capability_id.to_string();
            request.get_mut().declared_operations.clear();
            request
        };
        let capture = crate::test_logs::LogCapture::start(tracing::Level::WARN);
        for request in [
            unverified("p1\nWARN forged line", "example.poll"),
            unverified("plugin-c", "example.poll\r\nWARN forged line"),
            unverified("plugin c", "example.poll"),
            unverified("plugin-c", "example/poll"),
        ] {
            let status = server.register_plugin_endpoint(request).await.unwrap_err();
            assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
            assert!(!status.message().contains("forged"), "{status:?}");
        }
        let mut operation = register(&platform, "tenant-a", "plugin-c");
        operation.get_mut().declared_operations = vec![
            "example.poll".to_string(),
            "example.poll\nWARN forged line".to_string(),
        ];
        let status = server
            .register_plugin_endpoint(operation)
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
        assert!(
            status.message().contains("declared_operations"),
            "{status:?}"
        );
        // 标识合法也不能省略 operation 声明。
        assert_eq!(
            server
                .register_plugin_endpoint(unverified("plugin-c", "example.poll"))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
        let output = capture.finish();
        assert!(!output.contains("forged"), "{output}");

        let read = staff_token(GATEWAY_KEY, kid::GATEWAY, "tenant-a", PLUGINS_READ, None);
        assert_eq!(
            listed(&server, Some(&read), "tenant-a").await.unwrap(),
            ["plugin-a"]
        );
    }

    #[tokio::test]
    async fn administer_checks_the_operation_code_before_dispatching() {
        let server = server();
        let administer = |token: &str, operation: &str| {
            let mut request = Request::new(GenericRequest {
                operation: operation.to_string(),
                metadata: Default::default(),
                payload: Some(prost_types::Any::default()),
                request_id: "req-1".to_string(),
            });
            request
                .metadata_mut()
                .insert("x-flare-ctx", token.parse().unwrap());
            request
        };
        let read = staff_token(
            GATEWAY_KEY,
            kid::GATEWAY,
            "tenant-a",
            HOOKS_READ,
            Some("org-1"),
        );
        let write = staff_token(
            GATEWAY_KEY,
            kid::GATEWAY,
            "tenant-a",
            HOOKS_WRITE,
            Some("org-1"),
        );
        // 读码调写操作、写码调读操作：权限码不符；没有治理服务时通过了鉴权的调用回 FAILED_PRECONDITION。
        assert_eq!(
            server
                .administer(administer(&read, "flare.extension.v1.hook_config.create"))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            server
                .administer(administer(&write, "flare.extension.v1.hook_config.list"))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            server
                .administer(administer(&read, "flare.extension.v1.hook_config.list"))
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            server
                .administer(administer(
                    &write,
                    "flare.extension.v1.hook_config.set_status"
                ))
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        // 未知操作先验身份再拒绝。
        assert_eq!(
            server
                .administer(administer(&read, "flare.extension.v1.hook_config.explode"))
                .await
                .unwrap_err()
                .code(),
            Code::Unimplemented
        );
        assert_eq!(
            server
                .administer(Request::new(GenericRequest::default()))
                .await
                .unwrap_err()
                .code(),
            Code::Unauthenticated
        );
    }
}
