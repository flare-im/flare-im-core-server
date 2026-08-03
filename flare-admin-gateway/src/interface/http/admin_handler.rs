use axum::{
    Json,
    extract::{Extension, Json as RequestJson, Path, Query},
    http::HeaderMap,
};
use flare_core_base::context::keys;
use flare_server_core::{
    AuthenticatedPrincipal,
    auth::AuthProviderMode,
    context::Ctx,
    http::{ApiResponse, ContextFromHeaders, HttpApiError as GatewayError, Result},
};
use serde::Serialize;

use super::admin_contract::{APP_ID_HEADER, AdminCapabilitiesResponse, build_admin_capabilities};
use crate::application::admin_messages::{
    AdminMessageDetailHttpResponse, AdminMessageEventsHttpResponse,
    AdminMessageEventsQueryHttpRequest, AdminMessageExportHttpRequest,
    AdminMessageExportHttpResponse, AdminMessageQueryHttpRequest, AdminMessageQueryHttpResponse,
    AdminMessageWriteLedgerHttpResponse, AdminMessageWriteLedgerQueryHttpRequest,
    admin_message_detail_response, admin_message_events_response, admin_message_export_response,
    admin_message_query_response, admin_message_write_ledger_response,
    build_storage_get_message_request, build_storage_message_events_request,
    build_storage_message_export_request, build_storage_search_request,
    build_storage_write_ledger_request,
};
use flare_im_service_kit::{clients::GrpcClients, gateway::GatewaySettings};
use std::sync::Arc;

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AdminAuthCheckResponse {
    pub user_id: String,
    pub tenant_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor_id: Option<String>,
    pub request_id: String,
    pub trace_id: String,
    pub scopes: Vec<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AdminGatewayHealthResponse {
    pub service: String,
    pub status: String,
    pub boundary: String,
    pub admin_api_version: String,
    pub route_count: usize,
    pub upstream_count: usize,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AdminGatewayUpstreamsResponse {
    pub upstreams: Vec<AdminGatewayUpstream>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AdminGatewayUpstream {
    pub name: String,
    pub route: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub static_fallback: Option<String>,
    pub connect_timeout_secs: u64,
    pub request_timeout_secs: u64,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AdminGatewayRoutesResponse {
    pub routes: Vec<AdminGatewayRoute>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AdminGatewayRoute {
    pub method: String,
    pub path: String,
    pub category: String,
    pub auth_required: bool,
    pub admin_required: bool,
    pub write: bool,
    pub downstream: String,
    pub status: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AdminGatewayConfigSnapshot {
    pub server: AdminServerConfigSnapshot,
    pub grpc: AdminGrpcConfigSnapshot,
    pub auth: AdminAuthConfigSnapshot,
    pub rate_limit: AdminRateLimitConfigSnapshot,
    pub tracing: AdminTracingConfigSnapshot,
    pub redacted_fields: Vec<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AdminServerConfigSnapshot {
    pub bind: String,
    pub port: u16,
    pub timeout_secs: u64,
    pub max_body_size: usize,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AdminGrpcConfigSnapshot {
    pub media_service_url: String,
    pub message_ingest_service_url: String,
    pub message_orchestrator_service_url: String,
    pub conversation_service_url: String,
    pub online_service_url: String,
    pub storage_reader_service_url: String,
    pub media_static_fallback: Option<String>,
    pub message_ingest_static_fallback: Option<String>,
    pub message_orchestrator_static_fallback: Option<String>,
    pub conversation_static_fallback: Option<String>,
    pub online_static_fallback: Option<String>,
    pub storage_reader_static_fallback: Option<String>,
    pub connect_timeout_secs: u64,
    pub request_timeout_secs: u64,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AdminAuthConfigSnapshot {
    pub mode: String,
    pub hook_url: Option<String>,
    pub hook_timeout_ms: u64,
    pub hook_secret_header: String,
    pub hook_secret_configured: Option<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AdminRateLimitConfigSnapshot {
    pub enabled: bool,
    pub requests_per_second: u32,
    pub burst_capacity: u32,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AdminTracingConfigSnapshot {
    pub enabled: bool,
    pub service_name: String,
    pub sample_rate: f64,
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/auth/check",
    tag = "Admin",
    responses(
        (status = 200, description = "Admin token is valid", body = ApiResponse<AdminAuthCheckResponse>),
        (status = 401, description = "Token is missing or invalid"),
        (status = 403, description = "Admin scope or admin context is missing"),
    ),
)]
pub async fn admin_auth_check(
    headers: HeaderMap,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Json<ApiResponse<AdminAuthCheckResponse>> {
    let ctx = Ctx::from_headers(&headers);
    let response = AdminAuthCheckResponse {
        user_id: principal.user_id,
        tenant_id: ctx.tenant_id().unwrap_or_default().to_string(),
        app_id: header_value(&headers, APP_ID_HEADER).or(principal.app_id),
        actor_id: header_value(&headers, keys::ACTOR_ID),
        request_id: ctx.request_id().to_string(),
        trace_id: ctx.trace_id().to_string(),
        scopes: principal.scopes,
    };

    Json(ApiResponse::success(response))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/capabilities",
    tag = "Admin",
    responses(
        (status = 200, description = "Admin API capabilities", body = ApiResponse<AdminCapabilitiesResponse>),
        (status = 401, description = "Token is missing or invalid"),
        (status = 403, description = "Admin scope or admin context is missing"),
    ),
)]
pub async fn admin_capabilities() -> Json<ApiResponse<AdminCapabilitiesResponse>> {
    Json(ApiResponse::success(build_admin_capabilities()))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/gateway/health",
    tag = "Admin",
    responses(
        (status = 200, description = "Gateway management health", body = ApiResponse<AdminGatewayHealthResponse>),
        (status = 401, description = "Token is missing or invalid"),
        (status = 403, description = "Admin scope or admin context is missing"),
    ),
)]
pub async fn admin_gateway_health() -> Json<ApiResponse<AdminGatewayHealthResponse>> {
    Json(ApiResponse::success(build_gateway_health()))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/gateway/upstreams",
    tag = "Admin",
    responses(
        (status = 200, description = "Configured downstream gRPC upstreams", body = ApiResponse<AdminGatewayUpstreamsResponse>),
        (status = 401, description = "Token is missing or invalid"),
        (status = 403, description = "Admin scope or admin context is missing"),
    ),
)]
pub async fn admin_gateway_upstreams(
    Extension(settings): Extension<GatewaySettings>,
) -> Json<ApiResponse<AdminGatewayUpstreamsResponse>> {
    Json(ApiResponse::success(build_gateway_upstreams(&settings)))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/gateway/routes",
    tag = "Admin",
    responses(
        (status = 200, description = "Gateway route inventory", body = ApiResponse<AdminGatewayRoutesResponse>),
        (status = 401, description = "Token is missing or invalid"),
        (status = 403, description = "Admin scope or admin context is missing"),
    ),
)]
pub async fn admin_gateway_routes() -> Json<ApiResponse<AdminGatewayRoutesResponse>> {
    Json(ApiResponse::success(build_gateway_routes()))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/gateway/config",
    tag = "Admin",
    responses(
        (status = 200, description = "Redacted gateway configuration snapshot", body = ApiResponse<AdminGatewayConfigSnapshot>),
        (status = 401, description = "Token is missing or invalid"),
        (status = 403, description = "Admin scope or admin context is missing"),
    ),
)]
pub async fn admin_gateway_config(
    Extension(settings): Extension<GatewaySettings>,
) -> Json<ApiResponse<AdminGatewayConfigSnapshot>> {
    Json(ApiResponse::success(build_gateway_config_snapshot(
        &settings,
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/messages/query",
    tag = "Admin",
    request_body = AdminMessageQueryHttpRequest,
    responses(
        (status = 200, description = "Admin message query result", body = ApiResponse<AdminMessageQueryHttpResponse>),
        (status = 400, description = "Query is unbounded or invalid"),
        (status = 401, description = "Token is missing or invalid"),
        (status = 403, description = "Admin scope or admin context is missing"),
        (status = 503, description = "Storage reader is unavailable"),
    ),
)]
pub async fn admin_query_messages(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    RequestJson(req): RequestJson<AdminMessageQueryHttpRequest>,
) -> Result<Json<ApiResponse<AdminMessageQueryHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let storage_request = build_storage_search_request(&req)
        .map_err(|err| GatewayError::bad_request(err.code, err.message))?;
    let requested_limit = storage_request
        .pagination
        .as_ref()
        .map(|pagination| pagination.limit)
        .unwrap_or_default();

    let mut storage = clients.storage_reader.lock().await;
    let storage_response = storage
        .search_messages_with_ctx(&ctx, storage_request)
        .await
        .map_err(|err| GatewayError::internal("ADMIN_MESSAGE_QUERY_FAILED", err.to_string()))?;

    Ok(Json(ApiResponse::success(admin_message_query_response(
        storage_response,
        requested_limit,
    ))))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/messages/{message_id}",
    tag = "Admin",
    params(
        ("message_id" = String, Path, description = "Server message id"),
    ),
    responses(
        (status = 200, description = "Admin message detail", body = ApiResponse<AdminMessageDetailHttpResponse>),
        (status = 400, description = "Message id is missing"),
        (status = 401, description = "Token is missing or invalid"),
        (status = 403, description = "Admin scope or admin context is missing"),
        (status = 404, description = "Message is not found"),
        (status = 503, description = "Storage reader is unavailable"),
    ),
)]
pub async fn admin_get_message(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Path(message_id): Path<String>,
) -> Result<Json<ApiResponse<AdminMessageDetailHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let storage_request = build_storage_get_message_request(&message_id)
        .map_err(|err| GatewayError::bad_request(err.code, err.message))?;

    let mut storage = clients.storage_reader.lock().await;
    let storage_response = storage
        .get_message_with_ctx(&ctx, storage_request)
        .await
        .map_err(|err| GatewayError::internal("ADMIN_MESSAGE_DETAIL_FAILED", err.to_string()))?;
    let response = admin_message_detail_response(storage_response)
        .ok_or_else(|| GatewayError::not_found("message not found"))?;

    Ok(Json(ApiResponse::success(response)))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/messages/{message_id}/events",
    tag = "Admin",
    params(
        ("message_id" = String, Path, description = "Server message id"),
        ("event_types" = Option<String>, Query, description = "Comma-separated event type integers"),
        ("cursor" = Option<String>, Query, description = "Pagination cursor"),
        ("limit" = Option<i32>, Query, description = "Page size, capped by gateway"),
    ),
    responses(
        (status = 200, description = "Admin message event chain", body = ApiResponse<AdminMessageEventsHttpResponse>),
        (status = 400, description = "Message id or event type query is invalid"),
        (status = 401, description = "Token is missing or invalid"),
        (status = 403, description = "Admin scope or admin context is missing"),
        (status = 503, description = "Storage reader is unavailable"),
    ),
)]
pub async fn admin_query_message_events(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Path(message_id): Path<String>,
    Query(query): Query<AdminMessageEventsQueryHttpRequest>,
) -> Result<Json<ApiResponse<AdminMessageEventsHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let storage_request = build_storage_message_events_request(&message_id, &query)
        .map_err(|err| GatewayError::bad_request(err.code, err.message))?;
    let requested_limit = storage_request
        .pagination
        .as_ref()
        .map(|pagination| pagination.limit)
        .unwrap_or_default();

    let mut storage = clients.storage_reader.lock().await;
    let storage_response = storage
        .query_message_events_with_ctx(&ctx, storage_request)
        .await
        .map_err(|err| GatewayError::internal("ADMIN_MESSAGE_EVENTS_FAILED", err.to_string()))?;

    Ok(Json(ApiResponse::success(admin_message_events_response(
        storage_response,
        requested_limit,
    ))))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/messages/write-ledger/query",
    tag = "Admin",
    request_body = AdminMessageWriteLedgerQueryHttpRequest,
    responses(
        (status = 200, description = "Admin message write ledger query result", body = ApiResponse<AdminMessageWriteLedgerHttpResponse>),
        (status = 400, description = "Ledger query is unbounded or invalid"),
        (status = 401, description = "Token is missing or invalid"),
        (status = 403, description = "Admin scope or admin context is missing"),
        (status = 503, description = "Storage reader is unavailable"),
    ),
)]
pub async fn admin_query_message_write_ledger(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    RequestJson(req): RequestJson<AdminMessageWriteLedgerQueryHttpRequest>,
) -> Result<Json<ApiResponse<AdminMessageWriteLedgerHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let storage_request = build_storage_write_ledger_request(&req)
        .map_err(|err| GatewayError::bad_request(err.code, err.message))?;
    let requested_limit = storage_request
        .pagination
        .as_ref()
        .map(|pagination| pagination.limit)
        .unwrap_or_default();

    let mut storage = clients.storage_reader.lock().await;
    let storage_response = storage
        .query_message_write_ledger_with_ctx(&ctx, storage_request)
        .await
        .map_err(|err| {
            GatewayError::internal("ADMIN_MESSAGE_WRITE_LEDGER_QUERY_FAILED", err.to_string())
        })?;

    Ok(Json(ApiResponse::success(
        admin_message_write_ledger_response(storage_response, requested_limit),
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/messages/export",
    tag = "Admin",
    request_body = AdminMessageExportHttpRequest,
    responses(
        (status = 200, description = "Admin message export task accepted", body = ApiResponse<AdminMessageExportHttpResponse>),
        (status = 400, description = "Export request is unbounded or invalid"),
        (status = 401, description = "Token is missing or invalid"),
        (status = 403, description = "Admin scope or admin context is missing"),
        (status = 503, description = "Storage reader is unavailable"),
    ),
)]
pub async fn admin_export_messages(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    RequestJson(req): RequestJson<AdminMessageExportHttpRequest>,
) -> Result<Json<ApiResponse<AdminMessageExportHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let storage_request = build_storage_message_export_request(&req)
        .map_err(|err| GatewayError::bad_request(err.code, err.message))?;

    let mut storage = clients.storage_reader.lock().await;
    let storage_response = storage
        .export_messages_with_ctx(&ctx, storage_request)
        .await
        .map_err(|err| GatewayError::internal("ADMIN_MESSAGE_EXPORT_FAILED", err.to_string()))?;

    Ok(Json(ApiResponse::success(admin_message_export_response(
        storage_response,
    ))))
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn build_gateway_health() -> AdminGatewayHealthResponse {
    AdminGatewayHealthResponse {
        service: "flare-admin-gateway".to_string(),
        status: "ok".to_string(),
        boundary: "internal_admin_api_only_no_admin_console".to_string(),
        admin_api_version: "v1".to_string(),
        route_count: build_gateway_routes().routes.len(),
        upstream_count: 6,
    }
}

fn build_gateway_upstreams(settings: &GatewaySettings) -> AdminGatewayUpstreamsResponse {
    let grpc = &settings.grpc;
    AdminGatewayUpstreamsResponse {
        upstreams: vec![
            gateway_upstream(
                "media",
                &grpc.media_service_url,
                &grpc.media_static_fallback,
                grpc.connect_timeout_secs,
                grpc.request_timeout_secs,
            ),
            gateway_upstream(
                "message-ingest",
                &grpc.message_ingest_service_url,
                &grpc.message_ingest_static_fallback,
                grpc.connect_timeout_secs,
                grpc.request_timeout_secs,
            ),
            gateway_upstream(
                "message-orchestrator",
                &grpc.message_orchestrator_service_url,
                &grpc.message_orchestrator_static_fallback,
                grpc.connect_timeout_secs,
                grpc.request_timeout_secs,
            ),
            gateway_upstream(
                "conversation",
                &grpc.conversation_service_url,
                &grpc.conversation_static_fallback,
                grpc.connect_timeout_secs,
                grpc.request_timeout_secs,
            ),
            gateway_upstream(
                "signaling-online",
                &grpc.online_service_url,
                &grpc.online_static_fallback,
                grpc.connect_timeout_secs,
                grpc.request_timeout_secs,
            ),
            gateway_upstream(
                "storage-reader",
                &grpc.storage_reader_service_url,
                &grpc.storage_reader_static_fallback,
                grpc.connect_timeout_secs,
                grpc.request_timeout_secs,
            ),
        ],
    }
}

fn gateway_upstream(
    name: &str,
    route: &str,
    fallback: &str,
    connect_timeout_secs: u64,
    request_timeout_secs: u64,
) -> AdminGatewayUpstream {
    AdminGatewayUpstream {
        name: name.to_string(),
        route: route.to_string(),
        static_fallback: optional_string(fallback),
        connect_timeout_secs,
        request_timeout_secs,
    }
}

fn build_gateway_routes() -> AdminGatewayRoutesResponse {
    AdminGatewayRoutesResponse {
        routes: vec![
            route(
                "GET",
                "/api/v1/admin/auth/check",
                "admin",
                true,
                true,
                false,
                "gateway",
                "available",
            ),
            route(
                "GET",
                "/api/v1/admin/capabilities",
                "admin",
                true,
                true,
                false,
                "gateway",
                "available",
            ),
            route(
                "GET",
                "/api/v1/admin/gateway/health",
                "admin",
                true,
                true,
                false,
                "gateway",
                "available",
            ),
            route(
                "GET",
                "/api/v1/admin/gateway/upstreams",
                "admin",
                true,
                true,
                false,
                "gateway",
                "available",
            ),
            route(
                "GET",
                "/api/v1/admin/gateway/routes",
                "admin",
                true,
                true,
                false,
                "gateway",
                "available",
            ),
            route(
                "GET",
                "/api/v1/admin/gateway/config",
                "admin",
                true,
                true,
                false,
                "gateway",
                "available",
            ),
            route(
                "POST",
                "/api/v1/admin/messages/query",
                "admin-message-query",
                true,
                true,
                false,
                "storage-reader",
                "available",
            ),
            route(
                "GET",
                "/api/v1/admin/messages/{message_id}",
                "admin-message-detail",
                true,
                true,
                false,
                "storage-reader",
                "available",
            ),
            route(
                "GET",
                "/api/v1/admin/messages/{message_id}/events",
                "admin-message-events",
                true,
                true,
                false,
                "storage-reader",
                "available",
            ),
            route(
                "POST",
                "/api/v1/admin/messages/write-ledger/query",
                "admin-message-write-ledger",
                true,
                true,
                false,
                "storage-reader",
                "available",
            ),
            route(
                "POST",
                "/api/v1/admin/messages/export",
                "admin-message-export",
                true,
                true,
                true,
                "storage-reader",
                "available",
            ),
        ],
    }
}

#[allow(clippy::too_many_arguments)]
fn route(
    method: &str,
    path: &str,
    category: &str,
    auth_required: bool,
    admin_required: bool,
    write: bool,
    downstream: &str,
    status: &str,
) -> AdminGatewayRoute {
    AdminGatewayRoute {
        method: method.to_string(),
        path: path.to_string(),
        category: category.to_string(),
        auth_required,
        admin_required,
        write,
        downstream: downstream.to_string(),
        status: status.to_string(),
    }
}

fn build_gateway_config_snapshot(settings: &GatewaySettings) -> AdminGatewayConfigSnapshot {
    AdminGatewayConfigSnapshot {
        server: AdminServerConfigSnapshot {
            bind: settings.server.bind.clone(),
            port: settings.server.port,
            timeout_secs: settings.server.timeout_secs,
            max_body_size: settings.server.max_body_size,
        },
        grpc: AdminGrpcConfigSnapshot {
            media_service_url: settings.grpc.media_service_url.clone(),
            message_ingest_service_url: settings.grpc.message_ingest_service_url.clone(),
            message_orchestrator_service_url: settings
                .grpc
                .message_orchestrator_service_url
                .clone(),
            conversation_service_url: settings.grpc.conversation_service_url.clone(),
            online_service_url: settings.grpc.online_service_url.clone(),
            storage_reader_service_url: settings.grpc.storage_reader_service_url.clone(),
            media_static_fallback: optional_string(&settings.grpc.media_static_fallback),
            message_ingest_static_fallback: optional_string(
                &settings.grpc.message_ingest_static_fallback,
            ),
            message_orchestrator_static_fallback: optional_string(
                &settings.grpc.message_orchestrator_static_fallback,
            ),
            conversation_static_fallback: optional_string(
                &settings.grpc.conversation_static_fallback,
            ),
            online_static_fallback: optional_string(&settings.grpc.online_static_fallback),
            storage_reader_static_fallback: optional_string(
                &settings.grpc.storage_reader_static_fallback,
            ),
            connect_timeout_secs: settings.grpc.connect_timeout_secs,
            request_timeout_secs: settings.grpc.request_timeout_secs,
        },
        auth: AdminAuthConfigSnapshot {
            mode: auth_mode_label(settings.auth.mode).to_string(),
            hook_url: settings.auth.hook_url.clone(),
            hook_timeout_ms: settings.auth.hook_timeout_ms,
            hook_secret_header: settings.auth.hook_secret_header.clone(),
            hook_secret_configured: settings
                .auth
                .hook_secret
                .as_deref()
                .filter(|secret| !secret.trim().is_empty())
                .map(|_| "<redacted>".to_string()),
        },
        rate_limit: AdminRateLimitConfigSnapshot {
            enabled: settings.rate_limit.enabled,
            requests_per_second: settings.rate_limit.requests_per_second,
            burst_capacity: settings.rate_limit.burst_capacity,
        },
        tracing: AdminTracingConfigSnapshot {
            enabled: settings.tracing.enabled,
            service_name: settings.tracing.service_name.clone(),
            sample_rate: settings.tracing.sample_rate,
        },
        redacted_fields: vec!["auth.hook_secret".to_string()],
    }
}

fn auth_mode_label(mode: AuthProviderMode) -> &'static str {
    match mode {
        AuthProviderMode::CoreJwt => "core_jwt",
        AuthProviderMode::HttpHook => "http_hook",
    }
}

fn optional_string(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::super::admin_contract::{
        EnterprisePolicyAuthority, EnterprisePolicyStatus, EnterpriseProtectedOperation,
        RetentionEnforcementAnchor,
    };
    use super::*;
    use flare_im_service_kit::gateway::{
        GatewayGrpcConfig, GatewaySettings, RateLimitConfig, ServerConfig, TracingConfig,
    };
    use flare_server_core::auth::{AuthProviderConfig, AuthProviderMode};

    #[test]
    fn admin_capabilities_expose_api_boundaries_without_admin_console() {
        let capabilities = build_admin_capabilities();

        assert_eq!(capabilities.service, "flare-admin-gateway");
        assert!(
            capabilities
                .required_scopes
                .iter()
                .any(|scope| scope == "admin_gateway:admin")
        );
        assert!(
            capabilities
                .endpoints
                .iter()
                .any(|endpoint| endpoint.path == "/api/v1/admin/auth/check")
        );
        assert_eq!(
            capabilities.organization_policy.status,
            EnterprisePolicyStatus::ExternalPolicyRequired
        );
        assert_eq!(
            capabilities.organization_policy.authority,
            EnterprisePolicyAuthority::BusinessAdminIdentityProvider
        );
        assert_eq!(
            capabilities.data_residency_policy.tenant_routing_key,
            keys::TENANT_ID
        );
        assert!(
            capabilities
                .retention_legal_policy
                .enforcement_anchors
                .contains(&RetentionEnforcementAnchor::CapabilityAuditLog)
        );
        assert!(
            capabilities
                .data_residency_policy
                .protected_operations
                .contains(&EnterpriseProtectedOperation::MessageExport)
        );
    }

    #[test]
    fn admin_upstreams_are_built_from_gateway_grpc_config() {
        let settings = test_settings();

        let response = build_gateway_upstreams(&settings);

        assert_eq!(response.upstreams.len(), 6);
        assert!(
            response
                .upstreams
                .iter()
                .any(|upstream| upstream.name == "message-ingest"
                    && upstream.route == "discovery://flare-message-ingest")
        );
        assert!(
            response
                .upstreams
                .iter()
                .any(|upstream| upstream.name == "message-orchestrator"
                    && upstream.route == "discovery://flare-orchestrator")
        );
    }

    #[test]
    fn admin_config_snapshot_redacts_auth_secret() {
        let settings = test_settings();

        let snapshot = build_gateway_config_snapshot(&settings);

        assert_eq!(snapshot.auth.mode, "http_hook");
        assert_eq!(
            snapshot.auth.hook_secret_configured.as_deref(),
            Some("<redacted>")
        );
        assert!(
            snapshot
                .redacted_fields
                .iter()
                .any(|field| field == "auth.hook_secret")
        );
    }

    #[test]
    fn admin_routes_mark_management_routes_as_admin_only() {
        let response = build_gateway_routes();

        assert!(
            response
                .routes
                .iter()
                .any(|route| route.path == "/api/v1/admin/gateway/config"
                    && route.admin_required
                    && !route.write)
        );
    }

    fn test_settings() -> GatewaySettings {
        GatewaySettings {
            server: ServerConfig {
                bind: "0.0.0.0".to_string(),
                port: 50050,
                timeout_secs: 30,
                max_body_size: 16 * 1024 * 1024,
            },
            grpc: GatewayGrpcConfig {
                media_service_url: "discovery://flare-media".to_string(),
                message_ingest_service_url: "discovery://flare-message-ingest".to_string(),
                message_orchestrator_service_url: "discovery://flare-orchestrator".to_string(),
                conversation_service_url: "discovery://flare-conversation".to_string(),
                online_service_url: "discovery://flare-signaling-online".to_string(),
                storage_reader_service_url: "discovery://flare-storage-reader".to_string(),
                media_static_fallback: "http://127.0.0.1:60081".to_string(),
                message_ingest_static_fallback: "http://127.0.0.1:50182".to_string(),
                message_orchestrator_static_fallback: "http://127.0.0.1:50181".to_string(),
                conversation_static_fallback: "http://127.0.0.1:50090".to_string(),
                online_static_fallback: "http://127.0.0.1:50061".to_string(),
                storage_reader_static_fallback: "http://127.0.0.1:60083".to_string(),
                connect_timeout_secs: 5,
                request_timeout_secs: 10,
            },
            auth: AuthProviderConfig {
                mode: AuthProviderMode::HttpHook,
                hook_url: Some("https://auth.example.com/validate".to_string()),
                hook_timeout_ms: 800,
                hook_secret_header: "x-flare-auth-hook-secret".to_string(),
                hook_secret: Some("secret-value".to_string()),
            },
            rate_limit: RateLimitConfig {
                enabled: true,
                requests_per_second: 1000,
                burst_capacity: 2000,
            },
            tracing: TracingConfig {
                enabled: true,
                service_name: "flare-gateway".to_string(),
                sample_rate: 1.0,
            },
        }
    }
}
