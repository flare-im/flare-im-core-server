//! `flare.capability.v1.CapabilityService` gRPC 实现（生产护栏：超时、限流、Staff 上下文、审计）。
//!
//! 管理 RPC 只认 admin-gateway 转发的 Staff `x-flare-ctx`（网关 admin 分区按 `kid=gateway` 签），
//! 按 RPC 族复核签名里的权限码：目录与用户授权读 `core:capabilities:read`，授予/吊销/租户开关
//! `core:capabilities:write`，插件簿读 `core:plugins:read`，插件登记与注销 `core:plugins:write`
//! （仅平台员工），Hook 治理 `core:hooks:read` / `core:hooks:write`。插件登记、注销与列表另接受
//! Cell 签的 Service 上下文（插件宿主自注册），其签名租户须等于请求租户。

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
use crate::application::handler::{VerifiedDispatchOrigin, dispatch_capability_command};
use crate::application::queries::list_registered_capabilities;
use crate::domain::capability::PluginHealthProbe;
use crate::domain::capability::{CapabilityDispatchCommand, CapabilityPolicyBackend};
use crate::domain::model::{PluginEndpoint, validate_plugin_endpoint};
use crate::infrastructure::capability::plugin_channel::resolve_plugin_channel;
use crate::infrastructure::capability::plugin_contract::{
    HEALTH_PROTOCOL_SFU_CONTROL, LABEL_HEALTH_PROTOCOL,
};
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
        };
        server.register_discovered_plugin_endpoints();
        server.spawn_plugin_health_checker();
        server
    }

    pub fn metrics(&self) -> CapabilityMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// The admin gate: the forwarded Staff context with `permission`; without a key set every
    /// admin RPC is UNAUTHENTICATED.
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
    fn plugin_route_actor<T>(
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
            return Ok(PluginRouteActor::Service(claims.actor_id.clone()));
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

    fn register_discovered_plugin_endpoints(&self) {
        let endpoints = self.runtime.media_control_endpoints();
        if endpoints.is_empty() {
            tracing::warn!(
                "no media control endpoints discovered from capability_runtime.plugin_discovery_endpoints"
            );
        }
        for ep in endpoints {
            // 发现来的媒体控制端点同样要声明健康协议 —— 它们是被
            // `media_control_endpoints()` 按能力 id 选出来的，配置里未必写了标签；
            // 漏声明会让健康检查静默退化成通用协议，丢掉 draining 语义。
            // 用 entry().or_insert 而非直接插入：配置里显式写了的以配置为准。
            let mut labels = ep.labels.clone();
            labels
                .entry(LABEL_HEALTH_PROTOCOL.to_string())
                .or_insert_with(|| HEALTH_PROTOCOL_SFU_CONTROL.to_string());

            let instance = RegisteredPluginInstance {
                plugin_id: ep.plugin_id.clone(),
                capability_id: ep.capability_id.clone(),
                grpc_authority: ep.discovery_route_authority(),
                labels,
                // 配置发现来的端点没有清单信息：如实标为 unverified，
                // 而不是伪造一份声明——否则「已验证」这个状态就失去意义了。
                plugin_version: String::new(),
                api_version: String::new(),
                manifest_sha256: String::new(),
                declared_operations: Vec::new(),
                unverified: true,
                // 配置发现来的端点没有清单，也就无从得知计费单位：沿用旧语义。
                seat_model: String::new(),
            };
            self.plugin_routes.upsert(ep.tenant_id.as_str(), instance);
            tracing::trace!(
                tenant_id = %ep.tenant_id,
                plugin_id = %ep.plugin_id,
                capability_id = %ep.capability_id,
                service_name = %ep.service_name,
                "auto-registered discovered capability plugin endpoint"
            );
        }
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
enum PluginRouteActor {
    Staff(VerifiedStaff),
    Service(String),
}

impl PluginRouteActor {
    fn id(&self) -> &str {
        match self {
            Self::Staff(staff) => &staff.actor_id,
            Self::Service(id) => id,
        }
    }
    async fn run<F: std::future::Future>(&self, work: F) -> F::Output {
        match self {
            Self::Staff(staff) => work.staff_audit(&staff.ctx, "flare-capability").await,
            Self::Service(_) => work.await,
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
        let actor =
            self.plugin_route_actor(&request, PLUGINS_WRITE, &request.get_ref().tenant_id)?;
        let r = request.into_inner();
        if r.tenant_id.is_empty() || r.plugin_id.is_empty() || r.capability_id.is_empty() {
            return Err(Status::invalid_argument(
                "tenant_id, plugin_id, capability_id required",
            ));
        }
        if r.grpc_authority.is_empty() {
            return Err(Status::invalid_argument("grpc_authority required"));
        }
        // 注册契约 v2：声明缺失时降级为 unverified 而不是拒绝。
        //
        // 这里是兼容窗口的关键位置——要求必填的话，核心升级的瞬间所有已部署
        // 插件都会注册失败。降级 + 告警，让新旧插件能在同一集群共存。
        let unverified = r.declared_operations.is_empty();
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
                unverified,
                seat_model: r.seat_model,
            },
        };
        // 先校验再记日志或回显：plugin_id、capability_id 与声明的每一项来自 Admin 接口和插件自报，
        // 校验过的才是标识（见 `is_plugin_identifier`），带换行的值伪造不了日志行。
        validate_plugin_endpoint(&endpoint).map_err(Status::invalid_argument)?;
        let instance = &endpoint.instance;
        if unverified {
            tracing::warn!(
                tenant_id = %endpoint.tenant_id,
                plugin_id = %instance.plugin_id,
                capability_id = %instance.capability_id,
                registered_by = actor.id(),
                "plugin registered without declared_operations;                  running unverified — declaration cannot be enforced"
            );
        } else if !instance
            .declared_operations
            .iter()
            .any(|op| op == &instance.capability_id)
        {
            // 声明了却不包含自己注册的 capability_id —— 这是清单与注册对不上，
            // 属于配置错误，早拒早发现，而不是等第一次调用时报 unknown op。
            return Err(Status::invalid_argument(format!(
                "capability_id {} not present in declared_operations {:?}",
                instance.capability_id, instance.declared_operations
            )));
        }
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
            message: if unverified {
                "registered (unverified)"
            } else {
                "registered"
            }
            .into(),
        }))
    }

    async fn deregister_plugin_endpoint(
        &self,
        request: Request<DeregisterPluginEndpointRequest>,
    ) -> Result<Response<DeregisterPluginEndpointResponse>, Status> {
        let actor =
            self.plugin_route_actor(&request, PLUGINS_WRITE, &request.get_ref().tenant_id)?;
        let r = request.into_inner();
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
        let _actor =
            self.plugin_route_actor(&request, PLUGINS_READ, &request.get_ref().tenant_id)?;
        let r = request.into_inner();
        if r.tenant_id.is_empty() {
            return Err(Status::invalid_argument("tenant_id required"));
        }
        let instances = self
            .plugin_routes
            .list_filtered(r.tenant_id.as_str(), r.capability_id.as_str());
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
            capability_id: "rtc.call".to_string(),
            grpc_authority: "http://127.0.0.1:9".to_string(),
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
                plugin_discovery_endpoints: Vec::new(),
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

    fn service_token(tenant: &str) -> String {
        signer(GATEWAY_KEY, kid::GATEWAY)
            .sign(&SignedContextIdentity::new(
                SignedActorType::Service,
                "flare-plugin-host",
                tenant,
                "req-2",
            ))
            .unwrap()
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
            capability_id: "rtc.call".to_string(),
            grpc_authority: "http://127.0.0.1:9".to_string(),
            declared_operations: vec!["rtc.call".to_string()],
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
        let service = service_token("tenant-a");
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
        let service = service_token("tenant-b");
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
        assert_eq!(ids, ["plugin-b", "plugin-d"]);

        // Registrations the table would refuse are INVALID_ARGUMENT before anything is stored:
        // a wildcard tenant, an over-long plugin id.
        let wildcard = service_token("*");
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
                .register_plugin_endpoint(register(&service, "tenant-b", &"p".repeat(129)))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }

    /// plugin_id、capability_id 与声明的每一项须是标识（ASCII 字母、数字、`_ - . :`），在记日志
    /// 之前校验：不带声明的登记会记一条 unverified 告警，带换行的值若先被记下就能伪造日志行。
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
            unverified("p1\nWARN forged line", "rtc.call"),
            unverified("plugin-c", "rtc.call\r\nWARN forged line"),
            unverified("plugin c", "rtc.call"),
            unverified("plugin-c", "rtc/call"),
        ] {
            let status = server.register_plugin_endpoint(request).await.unwrap_err();
            assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
            assert!(!status.message().contains("forged"), "{status:?}");
        }
        let mut operation = register(&platform, "tenant-a", "plugin-c");
        operation.get_mut().declared_operations = vec![
            "rtc.call".to_string(),
            "rtc.call\nWARN forged line".to_string(),
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
        // 合规的不带声明的登记照常受理，并记一条告警。
        assert!(
            server
                .register_plugin_endpoint(unverified("plugin-c", "rtc.call"))
                .await
                .unwrap()
                .into_inner()
                .accepted
        );
        let output = capture.finish();
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 1, "{output}");
        assert!(
            lines[0].contains("without declared_operations") && lines[0].contains("plugin-c"),
            "{output}"
        );
        assert!(!output.contains("forged"), "{output}");

        let read = staff_token(GATEWAY_KEY, kid::GATEWAY, "tenant-a", PLUGINS_READ, None);
        assert_eq!(
            listed(&server, Some(&read), "tenant-a").await.unwrap(),
            ["plugin-a", "plugin-c"]
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
