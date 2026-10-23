//! `dispatch_admin`: a tenant staff member invokes an admin operation of a plugin from the console
//! (console → gateway admin partition → admin-gateway → `CapabilityService.Administer`).
//!
//! The Staff context is verified upstream for one concrete application with a permission code of
//! this plugin, and carries the reason the staff member stated when they gave one. Here the
//! operation must be one of the plugin's admin operations (the manifest's `admin.operations`,
//! `PluginManifest::admin_operations`) — `query` operations through [`AdminCall::Query`],
//! `command` operations through [`AdminCall::Dispatch`] — the staff code must be exactly the one
//! the operation declares (`PluginManifest::admin_permission_for`:
//! `plugin:<pluginId>:<permission>`), an operation declaring `requiresReason` needs the stated
//! reason, and the operation must belong to the enabled, approved grant of the local projection.
//! Core writes the intent evidence (with the reason and the audited target), signs a staff ticket
//! ([`PluginCallSigner::sign_staff`], with the reason) and calls the plugin like a user call,
//! without a conversation. A successful action may return `data.sharedState`; it is published
//! tenant-wide (staff closing a poll ends it on every card).
//!
//! An action the console sent with an `Idempotency-Key` carries the command id of the staff
//! member's intent in its input (`commandId`, [`staff_command_id`]): every attempt of one console
//! action has a new request id but the same command id, and the plugin keys staff commands by it,
//! so a retry after an unknown outcome runs the command once. Every admin command takes one
//! (the platform refuses to publish one that does not).
use std::sync::Arc;
use std::time::Duration;

use flare_im_capability_core::plugin::authorization::PluginInvocation;
use flare_im_capability_core::plugin::invocation::{
    MAX_PERMISSION_CODE_BYTES, PluginStaffCallClaims, plugin_permission_target,
};
use flare_im_capability_core::plugin::manifest::OperationMode;
use flare_im_capability_core::plugin::resource::HostCapability;
use flare_im_capability_core::plugin::state::SharedState;
use flare_im_service_kit::staff_context::VerifiedStaff;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::plugin_state::{PublishScope, PublishSource};
use super::remote_dispatch::{PluginCall, call_plugin};
use crate::domain::capability::{CapabilityError, Result};
use crate::infrastructure::capability::PluginRouteBook;

/// `Administer` operation: call an admin `query` operation.
pub const PLUGIN_ADMIN_QUERY_OPERATION: &str = "flare.plugin.v1.admin.query";
/// `Administer` operation: call an admin `command` operation.
pub const PLUGIN_ADMIN_DISPATCH_OPERATION: &str = "flare.plugin.v1.admin.dispatch";
/// Request body bound (the `PayloadJson` value).
pub const MAX_ADMIN_REQUEST_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminCall {
    Query,
    Dispatch,
}
impl AdminCall {
    pub fn from_operation(operation: &str) -> Option<Self> {
        match operation {
            PLUGIN_ADMIN_QUERY_OPERATION => Some(Self::Query),
            PLUGIN_ADMIN_DISPATCH_OPERATION => Some(Self::Dispatch),
            _ => None,
        }
    }
}

/// `{"pluginId", "operation", "input"?, "idempotencyKey"?}`; `input` is an object (default `{}`);
/// `idempotencyKey` is the console's key of an action (1..=128 bytes of ASCII without control
/// characters).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AdminPluginRequest {
    pub plugin_id: String,
    pub operation: String,
    #[serde(default = "empty_object")]
    pub input: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

/// The command id of the intent `idempotency_key` names for staff member `staff_id`:
/// `intent-<sha256 hex>` of the two, the same on every attempt and never the same for two staff
/// members or two keys. It fits a plugin command id (`[A-Za-z0-9_:-]{1,128}`).
pub fn staff_command_id(staff_id: &str, idempotency_key: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"flare.plugin.admin.command\0");
    digest.update(staff_id.as_bytes());
    digest.update(b"\0");
    digest.update(idempotency_key.as_bytes());
    format!("intent-{}", hex::encode(digest.finalize()))
}
fn valid_idempotency_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 128
        && key.bytes().all(|b| b.is_ascii() && !b.is_ascii_control())
}
fn empty_object() -> Value {
    Value::Object(Default::default())
}
/// A plugin id that can carry a staff permission code (`plugin:<pluginId>:<permission>` of at
/// most [`MAX_PERMISSION_CODE_BYTES`], with a permission id of at least one byte).
fn valid_plugin_id(plugin_id: &str) -> bool {
    !plugin_id.is_empty()
        && plugin_id.len() < MAX_PERMISSION_CODE_BYTES - "plugin::".len()
        && !plugin_id.contains(':')
        && !plugin_id.chars().any(char::is_control)
}
impl AdminPluginRequest {
    pub fn parse(bytes: &[u8]) -> std::result::Result<Self, &'static str> {
        if bytes.len() > MAX_ADMIN_REQUEST_BYTES {
            return Err("plugin admin request too large");
        }
        let request: Self =
            serde_json::from_slice(bytes).map_err(|_| "invalid plugin admin request")?;
        if !request.input.is_object()
            || request.operation.is_empty()
            || request.operation.len() > 128
            || !valid_plugin_id(&request.plugin_id)
            || request
                .idempotency_key
                .as_deref()
                .is_some_and(|key| !valid_idempotency_key(key))
        {
            return Err("invalid plugin admin request");
        }
        Ok(request)
    }
    /// Whether `permission` is a staff permission code of this request's plugin
    /// (`plugin:<pluginId>:<permission>`). Which one the operation needs is in the manifest
    /// (`PluginManifest::admin_permission_for`); [`dispatch_admin`] checks that exact code.
    pub fn is_plugin_permission(&self, permission: &str) -> bool {
        plugin_permission_target(permission)
            .is_some_and(|(plugin_id, _)| plugin_id == self.plugin_id)
    }
}

/// How one admin call ended, for its durable evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminOutcome {
    /// The plugin ran the operation (`success: true`).
    Succeeded,
    /// The plugin refused it (`success: false` with its code): nothing was changed.
    Refused,
    /// No answer (timeout, transport): the plugin may or may not have run it.
    Unknown,
}

/// What the intent of an admin call records besides its action: the reason the staff member
/// stated and the audited target the operation declares (`auditTarget`, read from the input).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdminIntent<'a> {
    pub reason: Option<&'a str>,
    pub audit_target: Option<&'a str>,
}

/// The durable Staff evidence of a plugin admin call (`core_audit_outbox`, aggregate
/// `plugin_admin`, id `<pluginId>:<operation>`; the Staff request id ties the two rows together):
/// the intent — with [`AdminIntent`] — is written after the call is authorized and before the
/// plugin is called — when it cannot be written the plugin is not called (fail closed) — and the
/// outcome once the plugin answered (or did not). A plugin refusal is recorded as such, never as
/// an effect.
#[async_trait::async_trait]
pub trait AdminEvidence: Send + Sync {
    async fn intent(
        &self,
        tenant_id: &str,
        aggregate_id: &str,
        call: AdminCall,
        intent: AdminIntent<'_>,
    ) -> std::result::Result<(), String>;

    async fn outcome(
        &self,
        tenant_id: &str,
        aggregate_id: &str,
        call: AdminCall,
        outcome: AdminOutcome,
    ) -> std::result::Result<(), String>;
}

/// The evidence action of a call: `query_intent`, `dispatch_succeeded`, ...
pub fn admin_evidence_action(call: AdminCall, outcome: Option<AdminOutcome>) -> &'static str {
    match (call, outcome) {
        (AdminCall::Query, None) => "query_intent",
        (AdminCall::Query, Some(AdminOutcome::Succeeded)) => "query_succeeded",
        (AdminCall::Query, Some(AdminOutcome::Refused)) => "query_refused",
        (AdminCall::Query, Some(AdminOutcome::Unknown)) => "query_unknown",
        (AdminCall::Dispatch, None) => "dispatch_intent",
        (AdminCall::Dispatch, Some(AdminOutcome::Succeeded)) => "dispatch_succeeded",
        (AdminCall::Dispatch, Some(AdminOutcome::Refused)) => "dispatch_refused",
        (AdminCall::Dispatch, Some(AdminOutcome::Unknown)) => "dispatch_unknown",
    }
}

/// `{"pluginId", "operation", "requestId", "success", "data", "errorCode"?}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminPluginResult {
    pub plugin_id: String,
    pub operation: String,
    pub request_id: String,
    pub success: bool,
    pub data: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    /// The audited target the operation declares (`admin.operations[].auditTarget`, read from the
    /// input): for Core's audit record only, never part of the answer.
    #[serde(skip)]
    pub audit_target: Option<String>,
}

fn denied(reason: &str) -> CapabilityError {
    CapabilityError::PolicyDenied(format!("plugin admin call denied: {reason}"))
}

/// Admin calls are not stopped by the emergency switch (`ensure_not_suspended`): staff use them to
/// inspect and repair a plugin's data while its user operations are stopped. They still need the
/// plugin enabled in the projection and every call leaves durable evidence ([`AdminEvidence`]).
pub async fn dispatch_admin(
    staff: &VerifiedStaff,
    routes: &Arc<PluginRouteBook>,
    call: AdminCall,
    mut request: AdminPluginRequest,
    timeout: Duration,
    evidence: &dyn AdminEvidence,
) -> Result<AdminPluginResult> {
    if !staff
        .permission
        .as_deref()
        .is_some_and(|code| request.is_plugin_permission(code))
    {
        return Err(denied(
            "staff permission is not a permission of this plugin",
        ));
    }
    let tenant = staff.tenant_id.as_str();
    let now = chrono::Utc::now().timestamp_millis();
    let snapshot = routes
        .authorization
        .as_ref()
        .ok_or_else(|| denied("plugin authorization unavailable"))?
        .plugin_snapshot(tenant)
        .await
        .map_err(|_| denied("plugin authorization unavailable"))?
        .ok_or_else(|| denied("no plugin projection for this application"))?;
    snapshot
        .validate(now)
        .map_err(|_| denied("plugin projection expired"))?;
    let projected = snapshot
        .plugins
        .iter()
        .find(|p| {
            p.grant.plugin_id == request.plugin_id
                && p.grant.enabled
                && !p.grant.revoked
                && now >= p.grant.valid_from_ms
                && now < p.grant.valid_until_ms
        })
        .ok_or_else(|| denied("plugin is not enabled for this application"))?;
    let manifest = projected
        .manifest()
        .map_err(|_| denied("plugin projection invalid"))?;
    // An admin operation is a read or an action by its declared mode.
    let declared = manifest
        .admin
        .as_ref()
        .and_then(|admin| admin.operation(&request.operation))
        .filter(|op| match call {
            AdminCall::Query => op.mode == OperationMode::Query,
            AdminCall::Dispatch => op.mode == OperationMode::Command,
        })
        .ok_or_else(|| denied("operation is not an admin operation of this kind"))?;
    let audit_target: Option<String> = declared
        .audit_target
        .as_deref()
        .and_then(|pointer| request.input.pointer(pointer))
        .map(|value| match value {
            Value::String(text) => text.chars().take(256).collect(),
            other => other.to_string().chars().take(256).collect(),
        });
    // The staff member holds exactly the permission the operation declares
    // (`admin.operations[].permission`).
    let permission = manifest
        .admin_permission_for(&request.operation)
        .ok_or_else(|| denied("operation declares no staff permission"))?;
    if staff.permission.as_deref() != Some(permission.as_str()) {
        return Err(denied(
            "staff permission is not the one this operation needs",
        ));
    }
    // A change the manifest asks a reason for comes with the one the staff member stated (the
    // admin partition signed it into the Staff context).
    if declared.requires_reason && staff.reason.is_none() {
        return Err(denied("operation requires the staff member's reason"));
    }
    let operation = manifest
        .operation(&request.operation)
        .ok_or_else(|| denied("operation not declared"))?;
    projected
        .authorize(&PluginInvocation {
            tenant_id: tenant,
            actor_id: &staff.actor_id,
            operation: &request.operation,
            actor_permissions: &operation.permission_ids.iter().cloned().collect(),
            platform_enabled: true,
            now_ms: now,
        })
        .map_err(|_| denied("operation not approved for this application"))?;
    let timeout = timeout.min(Duration::from_millis(u64::from(operation.timeout_ms)));
    // One intent, one command: the command id comes from the staff member and the console's
    // key, whatever the input said.
    if call == AdminCall::Dispatch
        && let Some(key) = request.idempotency_key.as_deref()
    {
        let command_id = staff_command_id(&staff.actor_id, key);
        request
            .input
            .as_object_mut()
            .ok_or_else(|| denied("operation input is not an object"))?
            .insert("commandId".into(), Value::String(command_id));
    }
    let claims = PluginStaffCallClaims {
        tenant_id: tenant.to_owned(),
        plugin_id: projected.grant.plugin_id.clone(),
        plugin_version: projected.grant.plugin_version.clone(),
        manifest_digest: projected.grant.manifest_digest.clone(),
        deployment_id: projected.deployment.deployment_id.clone(),
        grant_id: projected.grant.grant_id.clone(),
        grant_revision: projected.grant.revision,
        configuration_digest: projected.configuration_digest.clone(),
        staff_id: staff.actor_id.clone(),
        org_id: staff.org_id.clone(),
        permission,
        operation: request.operation.clone(),
        request_id: staff.request_id.clone(),
        reason: staff.reason.clone(),
        issued_at_ms: now,
        expires_at_ms: now
            .saturating_add(timeout.as_millis().min(120_000) as i64)
            .min(projected.grant.valid_until_ms),
    };
    let ticket = routes
        .invocation_signer
        .as_ref()
        .ok_or_else(|| denied("plugin invocation signer absent"))?
        .sign_staff(&claims, now)
        .map_err(|_| denied("staff ticket rejected"))?;
    let trace_id = staff.trace_id.clone().unwrap_or_default();
    let aggregate_id = format!("{}:{}", request.plugin_id, request.operation);
    let intent = AdminIntent {
        reason: staff.reason.as_deref(),
        audit_target: audit_target.as_deref(),
    };
    evidence
        .intent(tenant, &aggregate_id, call, intent)
        .await
        .map_err(|error| {
            tracing::error!(%error, tenant_id = tenant, %aggregate_id, "plugin admin intent evidence not recorded; the plugin is not called");
            CapabilityError::System("plugin admin audit unavailable".into())
        })?;
    let answer = tokio::time::timeout(
        timeout,
        call_plugin(
            PluginCall {
                grpc_authority: &projected.deployment.grpc_authority,
                plugin_id: &projected.grant.plugin_id,
                operation: &request.operation,
                tenant_id: tenant,
                user_id: None,
                request_id: &staff.request_id,
                trace_id: &trace_id,
                payload: &request.input,
                ticket: &ticket,
            },
            timeout,
        ),
    )
    .await
    .map_err(|_| CapabilityError::Timeout("plugin admin call deadline exceeded".into()))
    .and_then(|answer| answer);
    let outcome = match &answer {
        Ok(result) if result.success => AdminOutcome::Succeeded,
        Ok(_) => AdminOutcome::Refused,
        Err(_) => AdminOutcome::Unknown,
    };
    if let Err(error) = evidence.outcome(tenant, &aggregate_id, call, outcome).await {
        tracing::error!(%error, tenant_id = tenant, %aggregate_id, ?outcome, "plugin admin outcome evidence not recorded");
    }
    let result = answer?;
    if result.success
        && call == AdminCall::Dispatch
        && operation
            .host_capabilities
            .contains(&HostCapability::EntityStatePublish)
    {
        publish_admin_result(routes, tenant, &projected.grant.plugin_id, &result.data);
    }
    let error_code = (!result.success)
        .then(|| {
            result
                .data
                .get("errorCode")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .flatten();
    Ok(AdminPluginResult {
        plugin_id: result.plugin_id,
        operation: result.capability_id,
        request_id: result.request_id,
        success: result.success,
        data: result.data,
        error_code,
        audit_target,
    })
}

fn publish_admin_result(routes: &PluginRouteBook, tenant: &str, plugin_id: &str, data: &Value) {
    let state = match SharedState::from_result(data) {
        Ok(Some(state)) => state,
        Ok(None) => return,
        Err(_) => {
            tracing::warn!(
                tenant,
                plugin_id,
                "plugin admin result has a malformed sharedState; not published"
            );
            return;
        }
    };
    let Some(publisher) = routes.state_publisher.clone() else {
        tracing::debug!(
            tenant,
            plugin_id,
            "plugin shared state publishing is not configured"
        );
        return;
    };
    let (tenant, plugin_id) = (tenant.to_owned(), plugin_id.to_owned());
    tokio::spawn(async move {
        if let Err(error) = publisher
            .submit(
                &tenant,
                &plugin_id,
                PublishScope::Tenant,
                state,
                PublishSource::Invocation,
            )
            .await
        {
            tracing::warn!(%error, %tenant, %plugin_id, "plugin admin result sharedState not published");
        }
    });
}

#[cfg(test)]
#[path = "admin_dispatch_tests.rs"]
mod tests;
