//! The plugin registry (`/platform/plugin-registry/**`, `/plugin-registry/**`), through Capability
//! `Administer`. A deployment without the platform maintains its registry here; the platform itself
//! calls Capability directly, because its routes are not signed with `core:plugins:*`.
//!
//! Answers are Capability's JSON unchanged. Capability checks scope and permission again.
use axum::{Json, body::Bytes, extract::Extension, http::HeaderMap};
use flare_im_service_kit::clients::AdminGrpcClients;
use flare_server_core::{
    AuthenticatedPrincipal,
    error::{ErrorBuilder, ErrorCode},
    http::{ApiResponse, HttpApiError, Result},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use utoipa::{IntoParams, ToSchema};

use super::admin_auth_middleware::VerifiedStaffContext;
use super::admin_extract::{AdminJson, AdminPath, AdminQuery};
use super::audit_target::AuditTarget;
use super::ops_handler::{AdminCapability, admin_rpc_error, audit_write, verified_tenant};

const VERSION_LIST: &str = "flare.extension.v1.plugin_version.list";
const VERSION_GET: &str = "flare.extension.v1.plugin_version.get";
const VERSION_REGISTER: &str = "flare.extension.v1.plugin_version.register";
const VERSION_SET_STATE: &str = "flare.extension.v1.plugin_version.set_state";
const DEPLOYMENT_LIST: &str = "flare.extension.v1.plugin_deployment.list";
const DEPLOYMENT_REGISTER: &str = "flare.extension.v1.plugin_deployment.register";
const STATUS_GET: &str = "flare.extension.v1.plugin_app_status.get";
const CONFIG_GET: &str = "flare.extension.v1.plugin_config.get";
const CONFIG_PUT: &str = "flare.extension.v1.plugin_config.put";
const DEV_REGISTER: &str = "flare.extension.v1.plugin_dev.register";
const DEV_FLAG_APP: &str = "flare.extension.v1.plugin_dev.flag_app";
const FAILED: &str = "ADMIN_PLUGIN_REGISTRY_FAILED";
/// A package, the size the platform's upload route takes.
pub(super) const MAX_PACKAGE_BYTES: usize = 32 * 1024 * 1024;
/// A manifest of at most 1 MiB, as a JSON string: escaping can double it.
pub(super) const MAX_DEV_REGISTRATION_BYTES: usize = 3 * 1024 * 1024;

/// Capability's JSON answer.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct AdminPluginRegistryHttp(#[schema(value_type = Object)] pub Value);

#[derive(Debug, Clone, Deserialize, Serialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct AdminPluginVersionQuery {
    pub plugin_id: Option<String>,
    pub query: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct AdminPluginDeploymentQuery {
    pub plugin_id: Option<String>,
    pub plugin_version: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AdminPluginVersionStateHttpRequest {
    pub published: Option<bool>,
    pub revoked: Option<bool>,
    pub suspended: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AdminPluginDeploymentHttpRequest {
    pub deployment_id: String,
    pub plugin_id: String,
    pub plugin_version: String,
    pub grpc_authority: String,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AdminPluginConfigHttpRequest {
    pub plugin_id: String,
    pub plugin_version: String,
    pub manifest_digest: String,
    pub revision: i64,
    pub enabled: bool,
    pub configured: bool,
    #[schema(value_type = Object)]
    pub configuration: Value,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AdminPluginDevHttpRequest {
    pub app_id: String,
    /// The bytes of `manifest.json`, unsigned.
    pub manifest_json: String,
    pub deployment_id: String,
    pub grpc_authority: String,
    #[schema(value_type = Option<Object>)]
    pub configuration: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AdminPluginDevAppHttpRequest {
    pub development: bool,
}

async fn call(
    clients: &AdminGrpcClients,
    capability: &AdminCapability,
    staff: &VerifiedStaffContext,
    operation: &'static str,
    payload: Vec<u8>,
) -> Result<Json<ApiResponse<AdminPluginRegistryHttp>>> {
    let mut client = capability.client().await?;
    let response = clients
        .bounded(client.administer_json(staff.staff(), operation, payload, staff.request_id()))
        .await
        .map_err(|err| admin_rpc_error(err, FAILED))?;
    let answer = response
        .payload
        .filter(|_| response.ok)
        .and_then(|payload| serde_json::from_slice::<Value>(&payload.value).ok())
        .ok_or_else(|| {
            HttpApiError(
                ErrorBuilder::new(ErrorCode::HttpInternalServerError, FAILED).build_error(),
            )
        })?;
    Ok(Json(ApiResponse::success(AdminPluginRegistryHttp(answer))))
}

fn body(value: Value) -> Result<Vec<u8>> {
    serde_json::to_vec(&value)
        .map_err(|_| HttpApiError::bad_request(FAILED, "request does not encode".to_string()))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/platform/plugin-registry/versions",
    tag = "Admin",
    params(AdminPluginVersionQuery),
    responses(
        (status = 200, description = "Registered versions, without manifests", body = ApiResponse<AdminPluginRegistryHttp>),
        (status = 403, description = "Signed permission is not core:plugins:read, or the caller is not platform staff"),
    ),
)]
pub async fn list_versions(
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    AdminQuery(query): AdminQuery<AdminPluginVersionQuery>,
) -> Result<Json<ApiResponse<AdminPluginRegistryHttp>>> {
    let payload = body(json!({
        "pluginId": query.plugin_id,
        "query": query.query,
        "limit": query.limit,
        "offset": query.offset,
    }))?;
    call(&clients, &capability, &staff, VERSION_LIST, payload).await
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/platform/plugin-registry/versions/{plugin_id}/{version}",
    tag = "Admin",
    params(
        ("plugin_id" = String, Path, description = "Plugin id"),
        ("version" = String, Path, description = "Plugin version"),
    ),
    responses(
        (status = 200, description = "The version with its signed manifest and assets", body = ApiResponse<AdminPluginRegistryHttp>),
        (status = 403, description = "Signed permission is not core:plugins:read, or the caller is not platform staff"),
        (status = 404, description = "No such version"),
    ),
)]
pub async fn get_version(
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    AdminPath((plugin_id, version)): AdminPath<(String, String)>,
) -> Result<Json<ApiResponse<AdminPluginRegistryHttp>>> {
    let payload = body(json!({"pluginId": plugin_id, "pluginVersion": version}))?;
    call(&clients, &capability, &staff, VERSION_GET, payload).await
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/platform/plugin-registry/versions",
    tag = "Admin",
    request_body(content = Vec<u8>, content_type = "application/octet-stream", description = "A .fpkg package, at most 32 MiB"),
    params(
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8"),
    ),
    responses(
        (status = 200, description = "The package verified and stored; `created` is false when the same digest was already there", body = ApiResponse<AdminPluginRegistryHttp>),
        (status = 400, description = "Invalid package or signature (BAD_REQUEST)"),
        (status = 403, description = "Signed permission is not core:plugins:platform:write, or the caller is not platform staff"),
    ),
)]
pub async fn register_version(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    package: Bytes,
) -> Result<Json<ApiResponse<AdminPluginRegistryHttp>>> {
    audit_write(
        "plugins.registry.version.register",
        &principal,
        &headers,
        &AuditTarget::id("package"),
    );
    call(
        &clients,
        &capability,
        &staff,
        VERSION_REGISTER,
        package.to_vec(),
    )
    .await
}

#[utoipa::path(
    put,
    path = "/api/v1/admin/platform/plugin-registry/versions/{plugin_id}/{version}/state",
    tag = "Admin",
    request_body = AdminPluginVersionStateHttpRequest,
    params(
        ("plugin_id" = String, Path, description = "Plugin id"),
        ("version" = String, Path, description = "Plugin version"),
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8"),
    ),
    responses(
        (status = 200, description = "The version after the change", body = ApiResponse<AdminPluginRegistryHttp>),
        (status = 403, description = "Signed permission is not core:plugins:platform:write, or the caller is not platform staff"),
        (status = 404, description = "No such version"),
    ),
)]
pub async fn set_version_state(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    AdminPath((plugin_id, version)): AdminPath<(String, String)>,
    AdminJson(request): AdminJson<AdminPluginVersionStateHttpRequest>,
) -> Result<Json<ApiResponse<AdminPluginRegistryHttp>>> {
    audit_write(
        "plugins.registry.version.state",
        &principal,
        &headers,
        &AuditTarget::id(&plugin_id),
    );
    let payload = body(json!({
        "pluginId": plugin_id,
        "pluginVersion": version,
        "published": request.published,
        "revoked": request.revoked,
        "suspended": request.suspended,
    }))?;
    call(&clients, &capability, &staff, VERSION_SET_STATE, payload).await
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/platform/plugin-registry/deployments",
    tag = "Admin",
    params(AdminPluginDeploymentQuery),
    responses(
        (status = 200, description = "Deployments and their health", body = ApiResponse<AdminPluginRegistryHttp>),
        (status = 403, description = "Signed permission is not core:plugins:read, or the caller is not platform staff"),
    ),
)]
pub async fn list_deployments(
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    AdminQuery(query): AdminQuery<AdminPluginDeploymentQuery>,
) -> Result<Json<ApiResponse<AdminPluginRegistryHttp>>> {
    let payload = body(json!({
        "pluginId": query.plugin_id,
        "pluginVersion": query.plugin_version,
        "limit": query.limit,
    }))?;
    call(&clients, &capability, &staff, DEPLOYMENT_LIST, payload).await
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/platform/plugin-registry/deployments",
    tag = "Admin",
    request_body = AdminPluginDeploymentHttpRequest,
    params(
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8"),
    ),
    responses(
        (status = 200, description = "The deployment", body = ApiResponse<AdminPluginRegistryHttp>),
        (status = 400, description = "The binding does not match the signed artifact (BAD_REQUEST)"),
        (status = 403, description = "Signed permission is not core:plugins:platform:write, or the caller is not platform staff"),
    ),
)]
pub async fn register_deployment(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    AdminJson(request): AdminJson<AdminPluginDeploymentHttpRequest>,
) -> Result<Json<ApiResponse<AdminPluginRegistryHttp>>> {
    audit_write(
        "plugins.registry.deployment.register",
        &principal,
        &headers,
        &AuditTarget::id(&request.deployment_id),
    );
    let payload = body(json!({
        "deploymentId": request.deployment_id,
        "pluginId": request.plugin_id,
        "pluginVersion": request.plugin_version,
        "grpcAuthority": request.grpc_authority,
    }))?;
    call(&clients, &capability, &staff, DEPLOYMENT_REGISTER, payload).await
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/plugin-registry/status",
    tag = "Admin",
    responses(
        (status = 200, description = "Desired and observed state of every binding of the staff application", body = ApiResponse<AdminPluginRegistryHttp>),
        (status = 403, description = "Signed permission is not core:plugins:read"),
    ),
)]
pub async fn app_status(
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<ApiResponse<AdminPluginRegistryHttp>>> {
    let payload = body(json!({"appId": verified_tenant(&principal)?}))?;
    call(&clients, &capability, &staff, STATUS_GET, payload).await
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/plugin-registry/bindings/{binding_id}/config",
    tag = "Admin",
    params(("binding_id" = String, Path, description = "Binding id")),
    responses(
        (status = 200, description = "The binding's state and configuration", body = ApiResponse<AdminPluginRegistryHttp>),
        (status = 403, description = "Signed permission is not core:plugins:read"),
        (status = 404, description = "No such binding"),
    ),
)]
pub async fn get_config(
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    AdminPath(binding_id): AdminPath<String>,
) -> Result<Json<ApiResponse<AdminPluginRegistryHttp>>> {
    let payload = body(json!({
        "appId": verified_tenant(&principal)?,
        "bindingId": binding_id,
    }))?;
    call(&clients, &capability, &staff, CONFIG_GET, payload).await
}

#[utoipa::path(
    put,
    path = "/api/v1/admin/plugin-registry/bindings/{binding_id}/config",
    tag = "Admin",
    request_body = AdminPluginConfigHttpRequest,
    params(
        ("binding_id" = String, Path, description = "Binding id"),
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8"),
    ),
    responses(
        (status = 200, description = "The binding after the write", body = ApiResponse<AdminPluginRegistryHttp>),
        (status = 400, description = "Invalid configuration or unknown release (BAD_REQUEST)"),
        (status = 403, description = "Signed permission is not core:plugins:write"),
    ),
)]
pub async fn put_config(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    AdminPath(binding_id): AdminPath<String>,
    AdminJson(request): AdminJson<AdminPluginConfigHttpRequest>,
) -> Result<Json<ApiResponse<AdminPluginRegistryHttp>>> {
    let app_id = verified_tenant(&principal)?;
    audit_write(
        "plugins.registry.config.put",
        &principal,
        &headers,
        &AuditTarget::id(&binding_id),
    );
    let payload = body(json!({
        "appId": app_id,
        "bindingId": binding_id,
        "pluginId": request.plugin_id,
        "pluginVersion": request.plugin_version,
        "manifestDigest": request.manifest_digest,
        "revision": request.revision,
        "enabled": request.enabled,
        "configured": request.configured,
        "configuration": request.configuration,
    }))?;
    call(&clients, &capability, &staff, CONFIG_PUT, payload).await
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/plugin-registry/dev",
    tag = "Admin",
    request_body = AdminPluginDevHttpRequest,
    params(
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8"),
    ),
    responses(
        (status = 200, description = "The binding; `changed` is false when nothing differed", body = ApiResponse<AdminPluginRegistryHttp>),
        (status = 400, description = "Invalid manifest, deployment or configuration (BAD_REQUEST)"),
        (status = 403, description = "Development mode is off, the application is not flagged, or the signed permission is not core:plugins:write for it"),
    ),
)]
pub async fn register_development(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    AdminJson(request): AdminJson<AdminPluginDevHttpRequest>,
) -> Result<Json<ApiResponse<AdminPluginRegistryHttp>>> {
    audit_write(
        "plugins.registry.dev.register",
        &principal,
        &headers,
        &AuditTarget::id(&request.deployment_id),
    );
    let mut payload = json!({
        "appId": request.app_id,
        "manifestJson": request.manifest_json,
        "deploymentId": request.deployment_id,
        "grpcAuthority": request.grpc_authority,
    });
    if let Some(configuration) = request.configuration {
        payload["configuration"] = configuration;
    }
    call(&clients, &capability, &staff, DEV_REGISTER, body(payload)?).await
}

#[utoipa::path(
    put,
    path = "/api/v1/admin/platform/plugin-registry/dev-apps/{app_id}",
    tag = "Admin",
    request_body = AdminPluginDevAppHttpRequest,
    params(
        ("app_id" = String, Path, description = "Application id"),
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8"),
    ),
    responses(
        (status = 200, description = "The flag is set; clearing it removes the application's development bindings", body = ApiResponse<AdminPluginRegistryHttp>),
        (status = 403, description = "Development mode is off, or the signed permission is not core:plugins:platform:write"),
    ),
)]
pub async fn flag_development_app(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    AdminPath(app_id): AdminPath<String>,
    AdminJson(request): AdminJson<AdminPluginDevAppHttpRequest>,
) -> Result<Json<ApiResponse<AdminPluginRegistryHttp>>> {
    audit_write(
        "plugins.registry.dev.flag_app",
        &principal,
        &headers,
        &AuditTarget::id(&app_id),
    );
    let payload = body(json!({"appId": app_id, "development": request.development}))?;
    call(&clients, &capability, &staff, DEV_FLAG_APP, payload).await
}
