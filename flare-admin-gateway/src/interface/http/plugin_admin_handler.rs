//! Plugin admin operations (`/plugin-admin/{plugin_id}/{permission}/query|dispatch`): the console
//! renders the pages of a plugin's manifest `admin` section and invokes its admin operations
//! through Capability `dispatch_admin` (`CapabilityService.Administer`,
//! `flare.plugin.v1.admin.query` / `.dispatch`).
//!
//! The admin partition signs `plugin:<plugin_id>:<permission>` for these routes (the shared route
//! table's per-plugin code, `core_admin::route_permission`) and the middleware has checked it,
//! with the operator's stated reason in the same signed context. Capability checks that the
//! operation is an admin operation of that mode declaring exactly this permission, that an
//! operation declaring `requiresReason` comes with the reason, signs a staff ticket and calls the
//! plugin. A plugin rejection is a 200 with `success: false` and its `error_code`.
//!
//! A dispatch passes the console's `idempotency-key` on to Capability, which derives the command
//! id of the staff member's intent from it: a retry of one action after an unknown outcome is a
//! new request carrying the same command id, and the plugin runs the command once.
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

/// The Capability request body: `{"pluginId","operation","input","idempotencyKey"?}`.
fn capability_body(
    plugin_id: &str,
    permission: &str,
    request: AdminPluginCallHttpRequest,
    idempotency_key: Option<&str>,
) -> Result<Vec<u8>> {
    if flare_plugin_protocol::invocation::plugin_permission(plugin_id, permission).is_none() {
        return Err(bad_request(
            "ADMIN_PLUGIN_FIELD_INVALID",
            "plugin_id and permission cannot carry a permission code",
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
    let mut body = serde_json::json!({
        "pluginId": plugin_id,
        "operation": operation,
        "input": input,
    });
    if let Some(key) = idempotency_key {
        body["idempotencyKey"] = Value::String(key.to_owned());
    }
    let body = serde_json::to_vec(&body)
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
    (plugin_id, permission): (String, String),
    operation: &'static str,
    request: AdminPluginCallHttpRequest,
    idempotency_key: Option<&str>,
    failure_reason: &'static str,
) -> Result<AdminPluginCallHttp> {
    let body = capability_body(&plugin_id, &permission, request, idempotency_key)?;
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
    path = "/api/v1/admin/plugin-admin/{plugin_id}/{permission}/query",
    tag = "Admin",
    request_body = AdminPluginCallHttpRequest,
    params(
        ("plugin_id" = String, Path, description = "Plugin id"),
        ("permission" = String, Path, description = "The staff permission the operation declares (admin.permissions id); the signed permission must be plugin:<plugin_id>:<permission>"),
    ),
    responses(
        (status = 200, description = "The plugin answered one of its admin query operations; `success=false` with `error_code` when the plugin rejected it", body = ApiResponse<AdminPluginCallHttp>),
        (status = 400, description = "plugin_id and permission too long for their permission code, operation blank or over 128 bytes, input not an object (ADMIN_PLUGIN_FIELD_INVALID), input over 16 KiB (ADMIN_PLUGIN_INPUT_TOO_LARGE), or Capability refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not plugin:<plugin_id>:<permission> (ADMIN_PERMISSION_DENIED), or Capability refused: the plugin is not enabled for the application, or the operation is not an admin query declaring this permission (FORBIDDEN)"),
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
    AdminPath(path): AdminPath<(String, String)>,
    AdminJson(request): AdminJson<AdminPluginCallHttpRequest>,
) -> Result<Json<ApiResponse<AdminPluginCallHttp>>> {
    let tenant_id = verified_tenant(&principal)?;
    let answer = call(
        &clients,
        &capability,
        &staff,
        tenant_id,
        path,
        QUERY_OPERATION,
        request,
        None,
        "ADMIN_PLUGIN_QUERY_FAILED",
    )
    .await?;
    Ok(Json(ApiResponse::success(answer)))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/plugin-admin/{plugin_id}/{permission}/dispatch",
    tag = "Admin",
    request_body = AdminPluginCallHttpRequest,
    params(
        ("plugin_id" = String, Path, description = "Plugin id"),
        ("permission" = String, Path, description = "The staff permission the operation declares (admin.permissions id); the signed permission must be plugin:<plugin_id>:<permission>"),
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8 (encodeURIComponent; plain ASCII may be sent as is), at most 512 bytes decoded and 1536 bytes as sent; recorded decoded in the audit log; the plugin receives the one the admin partition signed into the staff context"),
        ("idempotency-key" = Option<String>, Header, description = "Required unless x-request-id is present; the command id of the staff member's intent is derived from it, so a retry under the same key runs the plugin command once"),
        ("x-request-id" = Option<String>, Header, description = "Required unless idempotency-key is present"),
    ),
    responses(
        (status = 200, description = "The plugin ran one of its admin command operations; `success=false` with `error_code` when the plugin rejected it", body = ApiResponse<AdminPluginCallHttp>),
        (status = 400, description = "plugin_id and permission too long for their permission code, operation blank or over 128 bytes, input not an object (ADMIN_PLUGIN_FIELD_INVALID), input over 16 KiB (ADMIN_PLUGIN_INPUT_TOO_LARGE), or Capability refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not plugin:<plugin_id>:<permission> (ADMIN_PERMISSION_DENIED), audit reason / idempotency header missing (FORBIDDEN), or Capability refused: the plugin is not enabled for the application, the operation is not an admin command declaring this permission, or it requires a reason the staff context does not carry (FORBIDDEN)"),
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
    AdminPath(path): AdminPath<(String, String)>,
    AdminJson(request): AdminJson<AdminPluginCallHttpRequest>,
) -> Result<Json<ApiResponse<AdminPluginCallHttp>>> {
    let tenant_id = verified_tenant(&principal)?;
    audit_write(
        "plugins.admin.dispatch",
        &principal,
        &headers,
        &AuditTarget::id(&path.0),
    );
    let answer = call(
        &clients,
        &capability,
        &staff,
        tenant_id,
        path,
        DISPATCH_OPERATION,
        request,
        idempotency_key(&headers),
        "ADMIN_PLUGIN_DISPATCH_FAILED",
    )
    .await?;
    Ok(Json(ApiResponse::success(answer)))
}

/// The console's key of a dispatch (the admin middleware bounded it to 128 bytes).
fn idempotency_key(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(super::admin_contract::IDEMPOTENCY_KEY_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|key| !key.is_empty())
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
        let body = capability_body(
            "flare.poll",
            "read",
            request(" flare.poll.list ", None),
            None,
        )
        .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            serde_json::json!({"pluginId": "flare.poll", "operation": "flare.poll.list", "input": {}})
        );
        for bad in [
            capability_body("flare.poll", "read", request("", None), None),
            capability_body(
                "flare.poll",
                "read",
                request("x", Some(serde_json::json!([1]))),
                None,
            ),
            capability_body(&"p".repeat(117), "read", request("x", None), None),
            capability_body("flare.poll", "a:b", request("x", None), None),
            capability_body(
                "flare.poll",
                "read",
                request(
                    "x",
                    Some(serde_json::json!({"q": "x".repeat(MAX_INPUT_BYTES)})),
                ),
                None,
            ),
        ] {
            assert!(bad.is_err());
        }
        // A dispatch's key goes on for Capability to derive the command id from.
        let body = capability_body(
            "flare.poll",
            "manage",
            request(
                "flare.poll.admin.close",
                Some(serde_json::json!({"pollId": "p"})),
            ),
            Some("close-7"),
        )
        .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap()["idempotencyKey"],
            "close-7"
        );
    }
}
