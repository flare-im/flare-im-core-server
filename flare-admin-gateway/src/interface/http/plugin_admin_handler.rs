//! Plugin admin pages (`/plugin-admin/{plugin_id}/query|dispatch`): the console renders a plugin's
//! admin-page contribution and invokes its operations through Capability `dispatch_admin`
//! (`CapabilityService.Administer`, `flare.plugin.v1.admin.query` / `.dispatch`).
//!
//! The admin partition signs `plugin:<plugin_id>:admin` for these routes (the shared route table's
//! per-plugin code, `core_admin::route_permission`) and the middleware has checked it. Capability
//! checks it again, keeps reads to the admin pages' rows sources and detail operations and actions to
//! their action targets, signs a staff ticket and calls the plugin. A plugin rejection is a 200
//! with `success: false` and its `error_code`.
use axum::{Json, extract::Extension, http::HeaderMap};
use flare_im_service_kit::clients::AdminGrpcClients;
use flare_server_core::{
    AuthenticatedPrincipal,
    error::{ErrorBuilder, ErrorCode},
    http::{ApiResponse, HttpApiError, Result},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use utoipa::ToSchema;

use super::admin_auth_middleware::VerifiedStaffContext;
use super::admin_extract::{AdminJson, AdminPath};
use super::audit_target::AuditTarget;
use super::ops_handler::{AdminCapability, admin_rpc_error, audit_write, verified_tenant};

/// Capability `Administer` operations (`flare-capability` `admin_dispatch`).
const QUERY_OPERATION: &str = "flare.plugin.v1.admin.query";
const DISPATCH_OPERATION: &str = "flare.plugin.v1.admin.dispatch";
/// Request body bound, the same as Capability's.
const MAX_INPUT_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AdminPluginCallHttpRequest {
    /// Wire operation of the plugin, for example `flare.poll.list`.
    pub operation: String,
    /// Operation input (an object): rows filters and `cursor`, or the entity id of an action.
    #[serde(default)]
    #[schema(value_type = Object)]
    pub input: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AdminPluginCallHttp {
    pub tenant_id: String,
    pub plugin_id: String,
    pub operation: String,
    pub request_id: String,
    /// `false` when the plugin rejected the call; see `error_code`.
    pub success: bool,
    /// The plugin's result `data` (rows, detail data, the action outcome).
    #[schema(value_type = Object)]
    pub data: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
}

/// Capability's answer (`PayloadJson`).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CapabilityAnswer {
    plugin_id: String,
    operation: String,
    request_id: String,
    success: bool,
    data: Value,
    #[serde(default)]
    error_code: Option<String>,
}

fn bad_request(reason: &'static str, message: &str) -> HttpApiError {
    HttpApiError::bad_request(reason, message.to_string())
}

/// The Capability request body: `{"pluginId","operation","input"}`.
fn capability_body(plugin_id: &str, request: AdminPluginCallHttpRequest) -> Result<Vec<u8>> {
    if flare_plugin_protocol::invocation::plugin_admin_permission(plugin_id).is_none() {
        return Err(bad_request(
            "ADMIN_PLUGIN_FIELD_INVALID",
            "plugin_id cannot carry an admin permission code",
        ));
    }
    let operation = request.operation.trim();
    if operation.is_empty() || operation.len() > 128 {
        return Err(bad_request(
            "ADMIN_PLUGIN_FIELD_INVALID",
            "operation must be 1 to 128 bytes",
        ));
    }
    let input = request
        .input
        .unwrap_or_else(|| Value::Object(Default::default()));
    if !input.is_object() {
        return Err(bad_request(
            "ADMIN_PLUGIN_FIELD_INVALID",
            "input must be an object",
        ));
    }
    let body = serde_json::to_vec(&serde_json::json!({
        "pluginId": plugin_id,
        "operation": operation,
        "input": input,
    }))
    .map_err(|_| bad_request("ADMIN_PLUGIN_FIELD_INVALID", "input does not encode"))?;
    if body.len() > MAX_INPUT_BYTES {
        return Err(bad_request(
            "ADMIN_PLUGIN_INPUT_TOO_LARGE",
            "input exceeds 16 KiB",
        ));
    }
    Ok(body)
}

#[allow(clippy::too_many_arguments)]
async fn call(
    clients: &AdminGrpcClients,
    capability: &AdminCapability,
    staff: &VerifiedStaffContext,
    tenant_id: String,
    plugin_id: String,
    operation: &'static str,
    request: AdminPluginCallHttpRequest,
    failure_reason: &'static str,
) -> Result<AdminPluginCallHttp> {
    let body = capability_body(&plugin_id, request)?;
    let mut client = capability.client().await?;
    let response = clients
        .bounded(client.administer_json(staff.token(), operation, body, staff.request_id()))
        .await
        .map_err(|err| admin_rpc_error(err, failure_reason))?;
    let answer = response
        .payload
        .filter(|_| response.ok)
        .and_then(|payload| serde_json::from_slice::<CapabilityAnswer>(&payload.value).ok())
        .filter(|answer| answer.plugin_id == plugin_id)
        .ok_or_else(|| {
            HttpApiError(
                ErrorBuilder::new(ErrorCode::HttpInternalServerError, failure_reason).build_error(),
            )
        })?;
    Ok(AdminPluginCallHttp {
        tenant_id,
        plugin_id: answer.plugin_id,
        operation: answer.operation,
        request_id: answer.request_id,
        success: answer.success,
        data: answer.data,
        error_code: answer.error_code,
    })
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/plugin-admin/{plugin_id}/query",
    tag = "Admin",
    request_body = AdminPluginCallHttpRequest,
    params(
        ("plugin_id" = String, Path, description = "Plugin id; the signed permission must be plugin:<plugin_id>:admin"),
    ),
    responses(
        (status = 200, description = "The plugin answered a read of its admin page (a rows source or a detail operation); `success=false` with `error_code` when the plugin rejected it", body = ApiResponse<AdminPluginCallHttp>),
        (status = 400, description = "plugin_id too long for its permission code, operation blank or over 128 bytes, input not an object (ADMIN_PLUGIN_FIELD_INVALID), input over 16 KiB (ADMIN_PLUGIN_INPUT_TOO_LARGE), or Capability refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not plugin:<plugin_id>:admin (ADMIN_PERMISSION_DENIED), or Capability refused: the plugin is not enabled for the application, or the operation is not a read of its admin pages (FORBIDDEN)"),
        (status = 500, description = "Capability downstream failed (ADMIN_PLUGIN_QUERY_FAILED); its error text is not returned"),
        (status = 502, description = "Capability downstream refused the forwarded staff context as unauthenticated, a deployment error (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Capability downstream is disabled (ADMIN_CAPABILITY_DISABLED) or unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Capability downstream or the plugin did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
pub async fn query_plugin_admin(
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    AdminPath(plugin_id): AdminPath<String>,
    AdminJson(request): AdminJson<AdminPluginCallHttpRequest>,
) -> Result<Json<ApiResponse<AdminPluginCallHttp>>> {
    let tenant_id = verified_tenant(&principal)?;
    let answer = call(
        &clients,
        &capability,
        &staff,
        tenant_id,
        plugin_id,
        QUERY_OPERATION,
        request,
        "ADMIN_PLUGIN_QUERY_FAILED",
    )
    .await?;
    Ok(Json(ApiResponse::success(answer)))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/plugin-admin/{plugin_id}/dispatch",
    tag = "Admin",
    request_body = AdminPluginCallHttpRequest,
    params(
        ("plugin_id" = String, Path, description = "Plugin id; the signed permission must be plugin:<plugin_id>:admin"),
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8 (encodeURIComponent; plain ASCII may be sent as is), at most 512 bytes decoded and 1536 bytes as sent; recorded decoded in the audit log"),
        ("idempotency-key" = Option<String>, Header, description = "Required unless x-request-id is present; correlation only, not deduplicated"),
        ("x-request-id" = Option<String>, Header, description = "Required unless idempotency-key is present"),
    ),
    responses(
        (status = 200, description = "The plugin ran an action of its admin page; `success=false` with `error_code` when the plugin rejected it", body = ApiResponse<AdminPluginCallHttp>),
        (status = 400, description = "plugin_id too long for its permission code, operation blank or over 128 bytes, input not an object (ADMIN_PLUGIN_FIELD_INVALID), input over 16 KiB (ADMIN_PLUGIN_INPUT_TOO_LARGE), or Capability refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not plugin:<plugin_id>:admin (ADMIN_PERMISSION_DENIED), audit reason / idempotency header missing (FORBIDDEN), or Capability refused: the plugin is not enabled for the application, or the operation is not an action of its admin pages (FORBIDDEN)"),
        (status = 500, description = "Capability downstream failed (ADMIN_PLUGIN_DISPATCH_FAILED); its error text is not returned"),
        (status = 502, description = "Capability downstream refused the forwarded staff context as unauthenticated, a deployment error (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Capability downstream is disabled (ADMIN_CAPABILITY_DISABLED) or unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Capability downstream or the plugin did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
// Every argument is an axum extractor; the handler has no state of its own.
#[allow(clippy::too_many_arguments)]
pub async fn dispatch_plugin_admin(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    AdminPath(plugin_id): AdminPath<String>,
    AdminJson(request): AdminJson<AdminPluginCallHttpRequest>,
) -> Result<Json<ApiResponse<AdminPluginCallHttp>>> {
    let tenant_id = verified_tenant(&principal)?;
    audit_write(
        "plugins.admin.dispatch",
        &principal,
        &headers,
        &AuditTarget::id(&plugin_id),
    );
    let answer = call(
        &clients,
        &capability,
        &staff,
        tenant_id,
        plugin_id,
        DISPATCH_OPERATION,
        request,
        "ADMIN_PLUGIN_DISPATCH_FAILED",
    )
    .await?;
    Ok(Json(ApiResponse::success(answer)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(operation: &str, input: Option<Value>) -> AdminPluginCallHttpRequest {
        AdminPluginCallHttpRequest {
            operation: operation.into(),
            input,
        }
    }

    #[test]
    fn the_capability_body_names_the_path_plugin_and_an_object_input() {
        let body = capability_body("flare.poll", request(" flare.poll.list ", None)).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            serde_json::json!({"pluginId": "flare.poll", "operation": "flare.poll.list", "input": {}})
        );
        for bad in [
            capability_body("flare.poll", request("", None)),
            capability_body("flare.poll", request("x", Some(serde_json::json!([1])))),
            capability_body(&"p".repeat(116), request("x", None)),
            capability_body(
                "flare.poll",
                request(
                    "x",
                    Some(serde_json::json!({"q": "x".repeat(MAX_INPUT_BYTES)})),
                ),
            ),
        ] {
            assert!(bad.is_err());
        }
    }
}
