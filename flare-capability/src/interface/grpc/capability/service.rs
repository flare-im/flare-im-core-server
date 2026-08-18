//! `flare.capability.v1.CapabilityService` gRPC 实现（生产护栏：超时、限流、管理密钥、审计）。

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
use flare_grpc_proto::sfu_control::HealthCheckRequest;
use flare_grpc_proto::sfu_control::sfu_control_client::SfuControlClient;
use flare_im_contracts::utils::normalize_tenant_id;
use prost_types::Timestamp;
use tonic::{Request, Response, Status};

use super::administer::dispatch_hook_administer;
use crate::application::handler::dispatch_capability_command;
use crate::application::queries::list_registered_capabilities;
use crate::domain::capability::{CapabilityDispatchCommand, CapabilityPolicyBackend};
use crate::infrastructure::capability::plugin_channel::resolve_plugin_channel;
use crate::infrastructure::capability::{
    CapabilityExtensionRegistry, DispatchRateLimiter, PluginRouteBook,
};
use crate::infrastructure::config::CapabilityRuntimeConfig;
use crate::infrastructure::persistence::PostgresCapabilityAuditLog;
use crate::interface::grpc::hooks::HookServiceServer;
use crate::interface::grpc::shared::helpers::{
    actor_id_from_request, capability_error_to_status, ctx_allow_missing,
    require_capability_policy_admin, trace_id_from_request,
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
            let instance = RegisteredPluginInstance {
                plugin_id: ep.plugin_id.clone(),
                capability_id: ep.capability_id.clone(),
                grpc_authority: ep.discovery_route_authority(),
                labels: ep.labels.clone(),
                // 配置发现来的端点没有清单信息：如实标为 unverified，
                // 而不是伪造一份声明——否则「已验证」这个状态就失去意义了。
                plugin_version: String::new(),
                api_version: String::new(),
                manifest_sha256: String::new(),
                declared_operations: Vec::new(),
                unverified: true,
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
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(runtime.plugin_health_check_interval);
            loop {
                ticker.tick().await;
                let snapshots = routes.list_snapshots();
                for snapshot in snapshots {
                    let result = check_plugin_health(
                        snapshot.instance.grpc_authority.as_str(),
                        snapshot.instance.capability_id.as_str(),
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

async fn check_plugin_health(
    grpc_authority: &str,
    capability_id: &str,
    timeout: std::time::Duration,
) -> Result<(), String> {
    let channel = tokio::time::timeout(timeout, resolve_plugin_channel(grpc_authority))
        .await
        .map_err(|_| format!("plugin channel resolve timeout: {grpc_authority}"))??;

    if capability_id == "rtc.media.control" {
        let mut client = SfuControlClient::new(channel);
        let request = Request::new(HealthCheckRequest {});
        let response = tokio::time::timeout(timeout, client.health_check(request))
            .await
            .map_err(|_| "sfu health_check timeout".to_string())?
            .map_err(|e| e.to_string())?
            .into_inner();
        if response.draining {
            Err("sfu health_check: instance is draining".to_string())
        } else {
            Ok(())
        }
    } else {
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
}

#[tonic::async_trait]
impl CapabilityService for CapabilityGrpcServer {
    async fn list_capabilities(
        &self,
        _request: Request<ListCapabilitiesRequest>,
    ) -> Result<Response<ListCapabilitiesResponse>, Status> {
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
        let r = request.into_inner();
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
        let ctx = ctx_allow_missing(&request);
        let r = request.into_inner();

        if r.capability_id.is_empty() {
            return Err(Status::invalid_argument("capability_id required"));
        }
        let tenant_id = if r.tenant_id.trim().is_empty() {
            None
        } else {
            Some(normalize_tenant_id(&r.tenant_id))
        };
        let tenant_for_log = tenant_id.as_deref().unwrap_or("0");
        tracing::trace!(
            capability_id = %r.capability_id,
            tenant_id = %tenant_for_log,
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
        require_capability_policy_admin(&request, self.runtime.as_ref())?;
        let trace = trace_id_from_request(&request);
        let actor = actor_id_from_request(&request);
        let r = request.into_inner();
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
                actor.as_deref(),
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
        require_capability_policy_admin(&request, self.runtime.as_ref())?;
        let trace = trace_id_from_request(&request);
        let actor = actor_id_from_request(&request);
        let r = request.into_inner();
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
                actor.as_deref(),
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
        require_capability_policy_admin(&request, self.runtime.as_ref())?;
        let trace = trace_id_from_request(&request);
        let actor = actor_id_from_request(&request);
        let r = request.into_inner();
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
                actor.as_deref(),
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
        require_capability_policy_admin(&request, self.runtime.as_ref())?;
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
        require_capability_policy_admin(&request, self.runtime.as_ref())?;
        let r = request.into_inner();
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
        require_capability_policy_admin(&request, self.runtime.as_ref())?;
        let Some(ref gov) = self.hook_governance else {
            return Err(Status::failed_precondition(
                "hook governance disabled: configure database_url for HookService",
            ));
        };
        dispatch_hook_administer(gov.as_ref(), request.into_inner()).await
    }
}
