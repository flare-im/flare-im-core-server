//! `flare.capability.v1.CapabilityService` gRPC 实现（生产护栏：超时、限流、控制令牌、审计）。

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
use flare_im_service_kit::control_auth::{
    ConsoleControlVerifier, ControlActorKind, VerifiedControlActor, require_grpc_control_or_admin,
};
use flare_server_core::SignedContextVerifier;
use prost_types::Timestamp;
use tonic::{Request, Response, Status};

use super::administer::dispatch_hook_administer;
use crate::application::handler::{VerifiedDispatchOrigin, dispatch_capability_command};
use crate::application::queries::list_registered_capabilities;
use crate::domain::capability::PluginHealthProbe;
use crate::domain::capability::{CapabilityDispatchCommand, CapabilityPolicyBackend};
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
use crate::interface::grpc::shared::helpers::{
    capability_error_to_status, require_capability_policy_admin, trace_id_from_request,
};

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
    /// Dispatch alone requires cell-signed identity; other RPCs keep their own auth contract.
    dispatch_verifier: Option<SignedContextVerifier>,
    control_verifier: Option<ConsoleControlVerifier>,
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
        control_verifier: Option<ConsoleControlVerifier>,
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
            control_verifier,
        };
        server.register_discovered_plugin_endpoints();
        server.spawn_plugin_health_checker();
        server
    }

    pub fn metrics(&self) -> CapabilityMetricsSnapshot {
        self.metrics.snapshot()
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
                        Ok(()) => routes.mark_health(
                            snapshot.tenant_id.as_str(),
                            snapshot.instance.plugin_id.as_str(),
                            true,
                            None,
                        ),
                        Err(err) => routes.mark_health(
                            snapshot.tenant_id.as_str(),
                            snapshot.instance.plugin_id.as_str(),
                            false,
                            Some(err),
                        ),
                    }
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

#[tonic::async_trait]
impl CapabilityService for CapabilityGrpcServer {
    async fn list_capabilities(
        &self,
        request: Request<ListCapabilitiesRequest>,
    ) -> Result<Response<ListCapabilitiesResponse>, Status> {
        let _actor = require_capability_policy_admin(&request, self.control_verifier.as_ref())?;
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
        let actor = require_capability_policy_admin(&request, self.control_verifier.as_ref())?;
        let r = request.into_inner();
        actor.require_tenant(&r.tenant_id)?;
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
        let tenant_id = Some(normalized_tenant_id);

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
            let t = tenant_id.as_deref().unwrap_or("_");
            let u = user_id.unwrap_or("_");
            if !lim.check_and_record(t, u) {
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
            tenant_id,
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
        let actor = require_capability_policy_admin(&request, self.control_verifier.as_ref())?;
        let trace = trace_id_from_request(&request);
        let r = request.into_inner();
        actor.require_tenant(&r.tenant_id)?;
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
                Some(actor.subject.as_str()),
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
        let actor = require_capability_policy_admin(&request, self.control_verifier.as_ref())?;
        let trace = trace_id_from_request(&request);
        let r = request.into_inner();
        actor.require_tenant(&r.tenant_id)?;
        if r.tenant_id.is_empty() || r.user_id.is_empty() || r.capability_id.is_empty() {
            return Err(Status::invalid_argument(
                "tenant_id, user_id, capability_id required",
            ));
        }
        self.policy
            .revoke_user_capability(&r.tenant_id, &r.user_id, &r.capability_id)
            .await
            .map_err(capability_error_to_status)?;

        self.metrics.policy_revoke.fetch_add(1, Ordering::Relaxed);
        if let Some(ref a) = self.audit {
            a.record_policy_event(
                "revoke",
                &r.tenant_id,
                Some(actor.subject.as_str()),
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
        let actor = require_capability_policy_admin(&request, self.control_verifier.as_ref())?;
        let trace = trace_id_from_request(&request);
        let r = request.into_inner();
        actor.require_tenant(&r.tenant_id)?;
        if r.tenant_id.is_empty() || r.capability_id.is_empty() {
            return Err(Status::invalid_argument(
                "tenant_id, capability_id required",
            ));
        }
        self.policy
            .set_tenant_capability(&r.tenant_id, &r.capability_id, r.enabled)
            .await
            .map_err(capability_error_to_status)?;

        self.metrics
            .policy_tenant_switch
            .fetch_add(1, Ordering::Relaxed);
        if let Some(ref a) = self.audit {
            a.record_policy_event(
                "tenant_switch",
                &r.tenant_id,
                Some(actor.subject.as_str()),
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
        let actor = require_capability_policy_admin(&request, self.control_verifier.as_ref())?;
        let r = request.into_inner();
        actor.require_tenant(&r.tenant_id)?;
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
        let declared_operations = r.declared_operations;
        let unverified = declared_operations.is_empty();
        if unverified {
            tracing::warn!(
                tenant_id = %r.tenant_id,
                plugin_id = %r.plugin_id,
                capability_id = %r.capability_id,
                "plugin registered without declared_operations;                  running unverified — declaration cannot be enforced"
            );
        } else if !declared_operations.iter().any(|op| op == &r.capability_id) {
            // 声明了却不包含自己注册的 capability_id —— 这是清单与注册对不上，
            // 属于配置错误，早拒早发现，而不是等第一次调用时报 unknown op。
            return Err(Status::invalid_argument(format!(
                "capability_id {} not present in declared_operations {:?}",
                r.capability_id, declared_operations
            )));
        }

        let instance = RegisteredPluginInstance {
            plugin_id: r.plugin_id,
            capability_id: r.capability_id,
            grpc_authority: r.grpc_authority,
            labels: r.labels,
            plugin_version: r.plugin_version,
            api_version: r.api_version,
            manifest_sha256: r.manifest_sha256,
            declared_operations,
            unverified,
            seat_model: r.seat_model,
        };
        self.plugin_routes.upsert(r.tenant_id.as_str(), instance);
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
        let actor = require_capability_policy_admin(&request, self.control_verifier.as_ref())?;
        let r = request.into_inner();
        actor.require_tenant(&r.tenant_id)?;
        if r.tenant_id.is_empty() || r.plugin_id.is_empty() {
            return Err(Status::invalid_argument("tenant_id, plugin_id required"));
        }
        let removed = self
            .plugin_routes
            .remove(r.tenant_id.as_str(), r.plugin_id.as_str());
        Ok(Response::new(DeregisterPluginEndpointResponse {
            accepted: removed,
        }))
    }

    async fn list_registered_plugins(
        &self,
        request: Request<ListRegisteredPluginsRequest>,
    ) -> Result<Response<ListRegisteredPluginsResponse>, Status> {
        let actor = plugin_listing_actor(&request, self.control_verifier.as_ref())?;
        let r = request.into_inner();
        actor.require_tenant(&r.tenant_id)?;
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
        let actor = require_capability_policy_admin(&request, self.control_verifier.as_ref())?;
        actor.require_kind(ControlActorKind::Employee)?;
        let Some(ref gov) = self.hook_governance else {
            return Err(Status::failed_precondition(
                "hook governance disabled: configure database_url for HookService",
            ));
        };
        dispatch_hook_administer(gov.as_ref(), request.into_inner(), &actor).await
    }
}

/// `ListRegisteredPlugins` 只读：Console 直连的 control 令牌，或 admin-gateway 原样转发的
/// admin 令牌（ADM-01 `GET /api/v1/admin/plugins`）。两条分支都只允许员工，请求租户仍须
/// 等于令牌租户；登记与注销插件（写）继续只认 control 令牌。
fn plugin_listing_actor<T>(
    request: &Request<T>,
    verifier: Option<&ConsoleControlVerifier>,
) -> Result<VerifiedControlActor, Status> {
    let (actor, _channel) = require_grpc_control_or_admin(request, verifier)?;
    actor.require_kind(ControlActorKind::Employee)?;
    Ok(actor)
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
mod plugin_listing_tests {
    //! `ListRegisteredPlugins` 的两条令牌分支：缺令牌、错误受众、跨租户、非员工四类负向用例，
    //! 外加 control 与 admin 两个正向对照；只返回请求租户（即令牌租户）下的实例。

    use std::time::Duration;

    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_im_service_kit::control_auth::{
        CONSOLE_ISSUER, CORE_ADMIN_AUDIENCE, CORE_CONTROL_AUDIENCE, PROJECTION_WORKER_SUBJECT,
    };
    use tonic::Code;

    use super::*;
    use crate::infrastructure::capability::InMemoryCapabilityGrants;

    fn signer() -> (flare_server_core::TokenService, ConsoleControlVerifier) {
        let key = SigningKey::from_bytes(&[47; 32]);
        let private = key.to_pkcs8_pem(Default::default()).unwrap();
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        (
            flare_server_core::TokenService::from_ed25519_private_key(&private, CONSOLE_ISSUER, 60)
                .unwrap(),
            ConsoleControlVerifier::from_public_key(&public).unwrap(),
        )
    }

    fn plugin(plugin_id: &str) -> RegisteredPluginInstance {
        RegisteredPluginInstance {
            plugin_id: plugin_id.to_string(),
            capability_id: "rtc.call".to_string(),
            grpc_authority: "http://127.0.0.1:9".to_string(),
            ..Default::default()
        }
    }

    fn server(verifier: ConsoleControlVerifier) -> CapabilityGrpcServer {
        let routes = Arc::new(PluginRouteBook::new());
        routes.upsert("tenant-a", plugin("plugin-a"));
        routes.upsert("tenant-b", plugin("plugin-b"));
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
            }),
            None,
            None,
            Arc::new(CapabilityInvocationMetrics::default()),
            None,
            routes,
            None,
            Some(verifier),
        )
    }

    fn request(token: Option<&str>, tenant_id: &str) -> Request<ListRegisteredPluginsRequest> {
        let mut request = Request::new(ListRegisteredPluginsRequest {
            tenant_id: tenant_id.to_string(),
            capability_id: String::new(),
        });
        if let Some(token) = token {
            request
                .metadata_mut()
                .insert("authorization", format!("Bearer {token}").parse().unwrap());
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

    #[tokio::test]
    async fn plugin_listing_accepts_control_and_forwarded_admin_within_the_token_tenant() {
        let (tokens, verifier) = signer();
        let server = server(verifier);
        let admin = tokens
            .generate_with_ed25519("employee-1", Some("tenant-a"), "admin", CORE_ADMIN_AUDIENCE)
            .unwrap();
        let control = tokens
            .generate_with_ed25519(
                "employee-1",
                Some("tenant-a"),
                "control",
                CORE_CONTROL_AUDIENCE,
            )
            .unwrap();

        for token in [&admin, &control] {
            assert_eq!(
                listed(&server, Some(token), "tenant-a").await,
                Ok(vec!["plugin-a".to_string()])
            );
            // 跨租户：请求租户不是令牌租户。
            assert_eq!(
                listed(&server, Some(token), "tenant-b").await,
                Err(Code::PermissionDenied)
            );
        }
    }

    #[tokio::test]
    async fn plugin_listing_rejects_missing_wrong_audience_and_non_employee_tokens() {
        let (tokens, verifier) = signer();
        let server = server(verifier);
        assert_eq!(
            listed(&server, None, "tenant-a").await,
            Err(Code::Unauthenticated)
        );
        // admin 令牌配 control 受众（或反过来）都不是任何一条分支的合法组合。
        let crossed = tokens
            .generate_with_ed25519(
                "employee-1",
                Some("tenant-a"),
                "admin",
                CORE_CONTROL_AUDIENCE,
            )
            .unwrap();
        assert_eq!(
            listed(&server, Some(&crossed), "tenant-a").await,
            Err(Code::Unauthenticated)
        );
        // 通配租户的 admin 令牌不接受。
        let wildcard = tokens
            .generate_with_ed25519("employee-1", Some("*"), "admin", CORE_ADMIN_AUDIENCE)
            .unwrap();
        assert!(listed(&server, Some(&wildcard), "tenant-a").await.is_err());
        // 投影 worker 不是员工：control 分支按员工拒绝，admin 分支直接拒绝。
        for (token_use, audience) in [
            ("control", CORE_CONTROL_AUDIENCE),
            ("admin", CORE_ADMIN_AUDIENCE),
        ] {
            let worker = tokens
                .generate_with_ed25519(
                    PROJECTION_WORKER_SUBJECT,
                    Some("tenant-a"),
                    token_use,
                    audience,
                )
                .unwrap();
            assert_eq!(
                listed(&server, Some(&worker), "tenant-a").await,
                Err(Code::PermissionDenied),
                "{token_use}"
            );
        }
    }
}
