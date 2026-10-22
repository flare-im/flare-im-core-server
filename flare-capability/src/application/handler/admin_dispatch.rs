//! `dispatch_admin`: a tenant staff member invokes an admin-page operation of a plugin from the
//! console (console → gateway admin partition → admin-gateway → `CapabilityService.Administer`).
//!
//! The Staff context is verified upstream with the permission `plugin:<pluginId>:admin`
//! ([`plugin_admin_permission`]) for one concrete application. Here the operation must belong to
//! the plugin's admin pages ([`PluginManifest::admin_operations`]) — reads (rows sources, detail
//! operations) through [`AdminCall::Query`], actions through [`AdminCall::Dispatch`] — and to the
//! enabled, approved grant of the local projection. Core then signs a staff ticket
//! ([`PluginCallSigner::sign_staff`]) and calls the plugin like a user call, without a
//! conversation. A successful action may return `data.sharedState`; it is published tenant-wide
//! (staff closing a poll ends it on every card).
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use flare_im_capability_core::plugin::PluginManifest;
use flare_im_capability_core::plugin::authorization::PluginInvocation;
use flare_im_capability_core::plugin::invocation::{
    PluginStaffCallClaims, plugin_admin_permission,
};
use flare_im_capability_core::plugin::manifest::UiContributionKind;
use flare_im_capability_core::plugin::resource::HostCapability;
use flare_im_capability_core::plugin::state::SharedState;
use flare_im_capability_core::plugin::ui::{UiActionTarget, UiBlock, UiDocument};
use flare_im_service_kit::staff_context::VerifiedStaff;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::plugin_state::{PublishScope, PublishSource};
use super::remote_dispatch::{PluginCall, call_plugin};
use crate::domain::capability::{CapabilityError, Result};
use crate::infrastructure::capability::PluginRouteBook;

/// `Administer` operation: read an admin page's data (rows source or detail operation).
pub const PLUGIN_ADMIN_QUERY_OPERATION: &str = "flare.plugin.v1.admin.query";
/// `Administer` operation: run an admin page action.
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

/// `{"pluginId", "operation", "input"?}`; `input` is an object (default `{}`).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AdminPluginRequest {
    pub plugin_id: String,
    pub operation: String,
    #[serde(default = "empty_object")]
    pub input: Value,
}
fn empty_object() -> Value {
    Value::Object(Default::default())
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
            || plugin_admin_permission(&request.plugin_id).is_none()
        {
            return Err("invalid plugin admin request");
        }
        Ok(request)
    }
    /// The staff permission this request needs: `plugin:<pluginId>:admin`.
    pub fn permission(&self) -> String {
        plugin_admin_permission(&self.plugin_id).unwrap_or_default()
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
}

fn admin_documents(manifest: &PluginManifest) -> impl Iterator<Item = &UiDocument> {
    manifest
        .client_extensions
        .contributions
        .iter()
        .filter(|c| c.kind == UiContributionKind::AdminPage)
        .flat_map(|c| std::iter::once(&c.schema).chain(c.detail.as_ref().map(|d| &d.document)))
}

/// Admin-page reads: rows sources and detail operations.
pub fn admin_sources(manifest: &PluginManifest) -> BTreeSet<String> {
    let mut sources: BTreeSet<String> = manifest
        .client_extensions
        .contributions
        .iter()
        .filter(|c| c.kind == UiContributionKind::AdminPage)
        .filter_map(|c| c.detail.as_ref().map(|d| d.operation.clone()))
        .collect();
    for document in admin_documents(manifest) {
        sources.extend(document.blocks.iter().filter_map(|block| match block {
            UiBlock::Rows(rows) => Some(rows.source.clone()),
            _ => None,
        }));
    }
    sources
}

/// Admin-page actions: the operations their actions target.
pub fn admin_actions(manifest: &PluginManifest) -> BTreeSet<String> {
    admin_documents(manifest)
        .flat_map(|document| document.actions.iter())
        .filter_map(|action| match &action.target {
            UiActionTarget::Operation { operation } => Some(operation.clone()),
            _ => None,
        })
        .collect()
}

fn denied(reason: &str) -> CapabilityError {
    CapabilityError::PolicyDenied(format!("plugin admin call denied: {reason}"))
}

pub async fn dispatch_admin(
    staff: &VerifiedStaff,
    routes: &Arc<PluginRouteBook>,
    call: AdminCall,
    request: AdminPluginRequest,
    timeout: Duration,
) -> Result<AdminPluginResult> {
    let permission = request.permission();
    if permission.is_empty() || staff.permission.as_deref() != Some(permission.as_str()) {
        return Err(denied("staff permission is not this plugin's admin code"));
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
    let allowed = match call {
        AdminCall::Query => admin_sources(&manifest),
        AdminCall::Dispatch => admin_actions(&manifest),
    };
    if !allowed.contains(&request.operation)
        || !manifest.admin_operations().contains(&request.operation)
    {
        return Err(denied(
            "operation is not an admin-page operation of this kind",
        ));
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
    let result = tokio::time::timeout(
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
    .map_err(|_| CapabilityError::Timeout("plugin admin call deadline exceeded".into()))??;
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
