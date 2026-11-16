//! `flare.capability.v1.CapabilityService` gRPC 实现（生产护栏：超时、限流、Staff 上下文、审计）。
//!
//! 管理 RPC 只认 admin-gateway 转发的 Staff `x-flare-ctx`（网关 admin 分区按 `kid=gateway` 签），
//! 按 RPC 族复核签名里的权限码：目录读 `core:capabilities:read`，租户开关
//! `core:capabilities:write`，插件诊断读 `core:plugins:read`，
//! Hook 治理 `core:hooks:read` / `core:hooks:write` / `core:hooks:platform:write`。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use flare_grpc_proto::capability::capability_service_server::CapabilityService;
use flare_grpc_proto::capability::{
    CapabilityDescriptor as ProtoDescriptor, CapabilityDispatchResult as ProtoDispatchResult,
    DispatchCapabilityRequest, DispatchCapabilityResponse, GenericRequest, GenericResponse,
    ListCapabilitiesRequest, ListCapabilitiesResponse, ListRegisteredPluginsRequest,
    ListRegisteredPluginsResponse, SetTenantCapabilitySwitchRequest,
    SetTenantCapabilitySwitchResponse,
};
use flare_im_contracts::utils::normalize_tenant_id;
use flare_im_service_kit::audit_effects::durable::StaffAudit;
use flare_im_service_kit::staff_context::{VerifiedStaff, require_staff, require_staff_any};
use flare_server_core::SignedContextVerifier;
use flare_server_core::auth::core_admin::permission::{
    CAPABILITIES_READ, CAPABILITIES_WRITE, HOOKS_PLATFORM_WRITE, HOOKS_READ, HOOKS_WRITE,
    PLUGINS_READ,
};
use flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata;
use tonic::{Request, Response, Status};

use super::administer::{
    HookAdministrator, PLATFORM_WRITE_OPERATIONS, READ_OPERATIONS, WRITE_OPERATIONS,
    dispatch_hook_administer,
};
use crate::application::handler::admin_dispatch::{
    AdminCall, AdminEvidence, AdminIntent, AdminOutcome, AdminPluginRequest, admin_evidence_action,
    dispatch_admin,
};
use crate::application::handler::{VerifiedDispatchOrigin, dispatch_capability_command};
use crate::application::queries::list_registered_capabilities;
use crate::domain::capability::{CapabilityDispatchCommand, CapabilityPolicyBackend};
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
    policy_tenant_switch: AtomicU64,
}

impl CapabilityInvocationMetrics {
    pub fn snapshot(&self) -> CapabilityMetricsSnapshot {
        CapabilityMetricsSnapshot {
            dispatch_ok: self.dispatch_ok.load(Ordering::Relaxed),
            dispatch_err: self.dispatch_err.load(Ordering::Relaxed),
            dispatch_deadline: self.dispatch_deadline.load(Ordering::Relaxed),
            dispatch_rate_limited: self.dispatch_rate_limited.load(Ordering::Relaxed),
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
    pub policy_tenant_switch: u64,
}

/// Capability 控制面 gRPC：能力目录 / 策略 / Dispatch / 插件路由 / Hook 治理（`Administer`）。
pub struct CapabilityGrpcServer {
    registry: CapabilityExtensionRegistry,
    policy: Arc<dyn CapabilityPolicyBackend>,
    runtime: Arc<CapabilityRuntimeConfig>,
    pub(super) audit: Option<Arc<PostgresCapabilityAuditLog>>,
    rate_limiter: Option<Arc<DispatchRateLimiter>>,
    metrics: Arc<CapabilityInvocationMetrics>,
    /// Hook 配置等治理命令（CQRS 写模型）；无 DB 时不挂载。
    hook_governance: Option<Arc<HookServiceServer>>,
    /// 远端插件调用的装配：授权快照、票据签名、卡片与共享状态。
    plugin_routes: Arc<PluginRouteBook>,
    /// Dispatch requires cell-signed identity (any component key).
    dispatch_verifier: Option<SignedContextVerifier>,
    /// The same key set with Staff pinned to `kid=gateway`: the admin RPCs.
    pub(super) staff_verifier: Option<SignedContextVerifier>,
    /// The plugin registry operations of `Administer`; without a database they are refused.
    pub(super) plugin_registry: Option<Arc<super::plugin_registry::PluginRegistryApi>>,
    /// Verifies the platform worker on those operations, the same assertion as the projection.
    pub(super) platform_verifier:
        Option<flare_im_service_kit::control_auth::PlatformAssertionVerifier>,
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
        Self {
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
            plugin_registry: None,
            platform_verifier: None,
        }
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
    /// exact code the operation declares, which needs the tenant's manifest. Like every other admin
    /// gate (`verify_staff_metadata`), it carries the one bounded `x-flare-staff-reauth` proof the
    /// admin partition forwarded, for the plugin to check; a malformed or repeated one is
    /// UNAUTHENTICATED.
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
        VerifiedStaff::from_verified(&claims, ctx)?
            .with_reauthentication_metadata(request.metadata())
    }

    fn staff<T>(&self, request: &Request<T>, permission: &str) -> Result<VerifiedStaff, Status> {
        let verifier = self
            .staff_verifier
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("staff context verifier not configured"))?;
        require_staff(request, verifier, permission)
    }

    pub fn runtime_config(&self) -> Arc<CapabilityRuntimeConfig> {
        Arc::clone(&self.runtime)
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

fn require_canonical_dispatch_tenant_id(tenant_id: &str) -> Result<String, Status> {
    let normalized = normalize_tenant_id(tenant_id);
    if normalized != tenant_id {
        return Err(Status::invalid_argument("tenant_id must be canonical"));
    }
    Ok(normalized)
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

        let fut = dispatch_capability_command(
            &ctx,
            &self.registry,
            &self.plugin_routes,
            &self.policy,
            self.runtime.plugin_call_timeout,
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

    /// Diagnostics: the plugins Core may call for this application, as its authorized snapshot
    /// says. Nothing writes this view; a deployment reaches it through the registry and a grant.
    async fn list_registered_plugins(
        &self,
        request: Request<ListRegisteredPluginsRequest>,
    ) -> Result<Response<ListRegisteredPluginsResponse>, Status> {
        let staff = self.staff(&request, PLUGINS_READ)?;
        let r = request.into_inner();
        staff.require_tenant(&r.tenant_id)?;
        if r.tenant_id.is_empty() {
            return Err(Status::invalid_argument("tenant_id required"));
        }
        let instances = self
            .plugin_routes
            .instances(r.tenant_id.as_str(), r.capability_id.as_str())
            .await
            .map_err(|_| Status::unavailable("plugin authorization is unavailable"))?;
        Ok(Response::new(ListRegisteredPluginsResponse { instances }))
    }

    async fn administer(
        &self,
        request: Request<GenericRequest>,
    ) -> Result<Response<GenericResponse>, Status> {
        // The operation decides the code before anything is dispatched: reads need
        // core:hooks:read, writes core:hooks:write or core:hooks:platform:write (the dispatch
        // holds each to its scope), a test call the platform code; an unknown operation is
        // refused with the read code so it cannot probe for a weaker gate.
        let operation = request.get_ref().operation.as_str();
        if let Some(call) = AdminCall::from_operation(operation) {
            return self.administer_plugin(request, call).await;
        }
        if let Some(spec) = super::plugin_registry::spec_of(operation) {
            return self.administer_registry(request, spec).await;
        }
        let codes: &[&str] = if WRITE_OPERATIONS.contains(&operation) {
            &[HOOKS_WRITE, HOOKS_PLATFORM_WRITE]
        } else if PLATFORM_WRITE_OPERATIONS.contains(&operation) {
            &[HOOKS_PLATFORM_WRITE]
        } else if READ_OPERATIONS.contains(&operation) {
            &[HOOKS_READ]
        } else {
            self.staff(&request, HOOKS_READ)?;
            return Err(Status::unimplemented(format!(
                "unknown administer operation: {operation}"
            )));
        };
        let verifier = self
            .staff_verifier
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("staff context verifier not configured"))?;
        let staff = require_staff_any(&request, verifier, codes)?;
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
#[path = "plugin_admin_reauth_tests.rs"]
mod plugin_admin_reauth_tests;

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
