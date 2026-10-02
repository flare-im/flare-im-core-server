use axum::{Json, extract::Extension, http::HeaderMap};
use flare_core_base::context::keys;
use flare_server_core::{
    AuthenticatedPrincipal,
    context::Ctx,
    http::{ApiResponse, ContextFromHeaders, HttpApiError as GatewayError, Result},
};
use serde::Serialize;

use super::admin_auth_middleware::VerifiedAdminAssertion;
use super::admin_contract::{APP_ID_HEADER, AdminCapabilitiesResponse, build_admin_capabilities};
use super::admin_extract::{AdminJson, AdminPath, AdminQuery};
use super::ops_handler::{AdminCapability, admin_rpc_error};
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
use flare_im_service_kit::{
    clients::{CapabilityDownstreamConfig, GrpcClients, StorageReaderServiceClientWrapper},
    gateway::GatewaySettings,
};
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
    /// Admin Gateway 专用的可选下游；关闭时为 `null`。
    pub capability_service_url: Option<String>,
    pub capability_static_fallback: Option<String>,
    pub connect_timeout_secs: u64,
    pub request_timeout_secs: u64,
}

/// Admin Gateway 实际使用的认证方式。
///
/// 管理请求只经 `FLARE_CONSOLE_PUBLIC_KEY` 验 Console 签发的 admin 断言（EdDSA，issuer
/// `flare-im-console`，audience `flare-core-admin`，`token_use=admin`，有效期不超过 60 秒）。
/// `FLARE_ADMIN_GATEWAY_AUTH_*`（`core_jwt` / `http_hook`）在 Admin 作用域下不读取（出现时启动
/// 日志 WARN），快照也不转述它们。四个 `hook_*` 字段只为保持 Control 已登记的响应结构，
/// 取值固定为「没有认证 hook」。
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AdminAuthConfigSnapshot {
    /// 固定为 `console_ed25519`。
    pub mode: String,
    /// 不调用认证 hook，固定为 `null`。
    pub hook_url: Option<String>,
    /// 不调用认证 hook，固定为 `0`。
    pub hook_timeout_ms: u64,
    /// 不调用认证 hook，固定为空串。
    pub hook_secret_header: String,
    /// 不调用认证 hook，固定为 `null`。
    pub hook_secret_configured: Option<String>,
}

/// `gateway/config` 报告的认证方式：Console Ed25519 公钥验签。
const ADMIN_AUTH_MODE: &str = "console_ed25519";

/// Admin Gateway 的请求限流：它不做限流，所以固定报告关闭。
///
/// `FLARE_ADMIN_GATEWAY_RATE_LIMIT_*` 在 Admin 作用域下不读取（出现时启动日志 WARN）；三个
/// 字段只为保持 Control 已登记的响应结构。
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AdminRateLimitConfigSnapshot {
    /// 固定为 `false`。
    pub enabled: bool,
    /// 不限流，固定为 `0`。
    pub requests_per_second: u32,
    /// 不限流，固定为 `0`。
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
pub async fn admin_gateway_health(
    Extension(capability): Extension<AdminCapability>,
) -> Json<ApiResponse<AdminGatewayHealthResponse>> {
    Json(ApiResponse::success(build_gateway_health(
        capability.config.as_ref(),
    )))
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
    Extension(capability): Extension<AdminCapability>,
) -> Json<ApiResponse<AdminGatewayUpstreamsResponse>> {
    Json(ApiResponse::success(build_gateway_upstreams(
        &settings,
        capability.config.as_ref(),
    )))
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
    Extension(capability): Extension<AdminCapability>,
) -> Json<ApiResponse<AdminGatewayConfigSnapshot>> {
    Json(ApiResponse::success(build_gateway_config_snapshot(
        &settings,
        capability.config.as_ref(),
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/messages/query",
    tag = "Admin",
    request_body = AdminMessageQueryHttpRequest,
    responses(
        (status = 200, description = "Admin message query result", body = ApiResponse<AdminMessageQueryHttpResponse>),
        (status = 400, description = "No indexed filter and no time range (ADMIN_MESSAGE_QUERY_FILTER_REQUIRED), or the storage reader refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Token is missing or invalid"),
        (status = 403, description = "Admin scope or admin context is missing (FORBIDDEN), or the storage reader refused the tenant scope (FORBIDDEN)"),
        (status = 500, description = "Storage reader failed (ADMIN_MESSAGE_QUERY_FAILED); its error text is not returned"),
        (status = 502, description = "Storage reader refused the gateway as unauthenticated, a deployment error (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Storage reader is unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Storage reader did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
pub async fn admin_query_messages(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Extension(assertion): Extension<VerifiedAdminAssertion>,
    AdminJson(req): AdminJson<AdminMessageQueryHttpRequest>,
) -> Result<Json<ApiResponse<AdminMessageQueryHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let storage_request = build_storage_search_request(&req)
        .map_err(|err| GatewayError::bad_request(err.code, err.message))?;
    let requested_limit = storage_request
        .pagination
        .as_ref()
        .map(|pagination| pagination.limit)
        .unwrap_or_default();

    let mut storage = storage_reader(&clients).await;
    let storage_response = storage
        .search_messages_as_admin(assertion.token(), &ctx, storage_request)
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_MESSAGE_QUERY_FAILED"))?;

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
        (status = 400, description = "Message id is blank (ADMIN_MESSAGE_ID_REQUIRED), or the storage reader refused it (BAD_REQUEST)"),
        (status = 401, description = "Token is missing or invalid"),
        (status = 403, description = "Admin scope or admin context is missing (FORBIDDEN), or the storage reader refused the tenant scope (FORBIDDEN)"),
        (status = 404, description = "Message is not found in the token tenant (NOT_FOUND)"),
        (status = 500, description = "Storage reader failed (ADMIN_MESSAGE_DETAIL_FAILED); its error text is not returned"),
        (status = 502, description = "Storage reader refused the gateway as unauthenticated, a deployment error (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Storage reader is unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Storage reader did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
pub async fn admin_get_message(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Extension(assertion): Extension<VerifiedAdminAssertion>,
    AdminPath(message_id): AdminPath<String>,
) -> Result<Json<ApiResponse<AdminMessageDetailHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let storage_request = build_storage_get_message_request(&message_id)
        .map_err(|err| GatewayError::bad_request(err.code, err.message))?;

    let mut storage = storage_reader(&clients).await;
    let storage_response = storage
        .get_message_as_admin(assertion.token(), &ctx, storage_request)
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_MESSAGE_DETAIL_FAILED"))?;
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
        (status = 400, description = "Message id is blank (ADMIN_MESSAGE_ID_REQUIRED), an event_types item is not a positive integer (ADMIN_MESSAGE_EVENT_TYPE_INVALID), or the storage reader refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Token is missing or invalid"),
        (status = 403, description = "Admin scope or admin context is missing (FORBIDDEN), or the storage reader refused the tenant scope (FORBIDDEN)"),
        (status = 500, description = "Storage reader failed (ADMIN_MESSAGE_EVENTS_FAILED); its error text is not returned"),
        (status = 502, description = "Storage reader refused the gateway as unauthenticated, a deployment error (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Storage reader is unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Storage reader did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
pub async fn admin_query_message_events(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Extension(assertion): Extension<VerifiedAdminAssertion>,
    AdminPath(message_id): AdminPath<String>,
    AdminQuery(query): AdminQuery<AdminMessageEventsQueryHttpRequest>,
) -> Result<Json<ApiResponse<AdminMessageEventsHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let storage_request = build_storage_message_events_request(&message_id, &query)
        .map_err(|err| GatewayError::bad_request(err.code, err.message))?;
    let requested_limit = storage_request
        .pagination
        .as_ref()
        .map(|pagination| pagination.limit)
        .unwrap_or_default();

    let mut storage = storage_reader(&clients).await;
    let storage_response = storage
        .query_message_events_as_admin(assertion.token(), &ctx, storage_request)
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_MESSAGE_EVENTS_FAILED"))?;

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
        (status = 400, description = "No filter and no updated time range (ADMIN_MESSAGE_WRITE_LEDGER_FILTER_REQUIRED), updated_before earlier than updated_after (ADMIN_MESSAGE_WRITE_LEDGER_TIME_RANGE_INVALID), or the storage reader refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Token is missing or invalid"),
        (status = 403, description = "Admin scope or admin context is missing (FORBIDDEN), or the storage reader refused the tenant scope (FORBIDDEN)"),
        (status = 500, description = "Storage reader failed (ADMIN_MESSAGE_WRITE_LEDGER_QUERY_FAILED); its error text is not returned"),
        (status = 502, description = "Storage reader refused the gateway as unauthenticated, a deployment error (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Storage reader is unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Storage reader did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
pub async fn admin_query_message_write_ledger(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Extension(assertion): Extension<VerifiedAdminAssertion>,
    AdminJson(req): AdminJson<AdminMessageWriteLedgerQueryHttpRequest>,
) -> Result<Json<ApiResponse<AdminMessageWriteLedgerHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let storage_request = build_storage_write_ledger_request(&req)
        .map_err(|err| GatewayError::bad_request(err.code, err.message))?;
    let requested_limit = storage_request
        .pagination
        .as_ref()
        .map(|pagination| pagination.limit)
        .unwrap_or_default();

    let mut storage = storage_reader(&clients).await;
    let storage_response = storage
        .query_message_write_ledger_as_admin(assertion.token(), &ctx, storage_request)
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_MESSAGE_WRITE_LEDGER_QUERY_FAILED"))?;

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
        (status = 400, description = "Blank conversation_id (ADMIN_MESSAGE_EXPORT_CONVERSATION_REQUIRED), start_time or end_time missing (ADMIN_MESSAGE_EXPORT_TIME_RANGE_REQUIRED), end_time not after start_time (ADMIN_MESSAGE_EXPORT_TIME_RANGE_INVALID), or the storage reader refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Token is missing or invalid"),
        (status = 403, description = "Admin scope or admin context is missing (FORBIDDEN), or the storage reader refused the tenant scope (FORBIDDEN)"),
        (status = 500, description = "Storage reader failed (ADMIN_MESSAGE_EXPORT_FAILED); its error text is not returned"),
        (status = 502, description = "Storage reader refused the gateway as unauthenticated, a deployment error (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Storage reader is unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Storage reader did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
pub async fn admin_export_messages(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Extension(assertion): Extension<VerifiedAdminAssertion>,
    AdminJson(req): AdminJson<AdminMessageExportHttpRequest>,
) -> Result<Json<ApiResponse<AdminMessageExportHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let storage_request = build_storage_message_export_request(&req)
        .map_err(|err| GatewayError::bad_request(err.code, err.message))?;

    let mut storage = storage_reader(&clients).await;
    let storage_response = storage
        .export_messages_as_admin(assertion.token(), &ctx, storage_request)
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_MESSAGE_EXPORT_FAILED"))?;

    Ok(Json(ApiResponse::success(admin_message_export_response(
        storage_response,
    ))))
}

/// Storage Reader 客户端的一份克隆：只在取克隆时占锁，调用期间不占，一次慢检索不会让其他
/// 管理检索排队等它。下游只认转发的 admin 令牌（Storage Reader 按令牌租户检索），不带未签名
/// 的身份元数据。
async fn storage_reader(clients: &GrpcClients) -> StorageReaderServiceClientWrapper {
    clients.storage_reader.lock().await.clone()
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

/// 6 个固定下游，外加启用时的 Capability。
fn build_gateway_health(
    capability: Option<&CapabilityDownstreamConfig>,
) -> AdminGatewayHealthResponse {
    AdminGatewayHealthResponse {
        service: "flare-admin-gateway".to_string(),
        status: "ok".to_string(),
        boundary: "internal_admin_api_only_no_admin_console".to_string(),
        admin_api_version: "v1".to_string(),
        route_count: build_gateway_routes().routes.len(),
        upstream_count: 6 + usize::from(capability.is_some()),
    }
}

fn build_gateway_upstreams(
    settings: &GatewaySettings,
    capability: Option<&CapabilityDownstreamConfig>,
) -> AdminGatewayUpstreamsResponse {
    let grpc = &settings.grpc;
    let mut upstreams = vec![
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
    ];
    if let Some(capability) = capability {
        upstreams.push(gateway_upstream(
            "capability",
            &capability.service_url,
            &capability.static_fallback,
            grpc.connect_timeout_secs,
            grpc.request_timeout_secs,
        ));
    }
    AdminGatewayUpstreamsResponse { upstreams }
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
            route(
                "GET",
                "/api/v1/admin/medias/bucket",
                "admin-media",
                true,
                true,
                false,
                "media",
                "available",
            ),
            route(
                "POST",
                "/api/v1/admin/medias/cleanup-orphaned-assets",
                "admin-media",
                true,
                true,
                true,
                "media",
                "available",
            ),
            route(
                "GET",
                "/api/v1/admin/medias/objects",
                "admin-media",
                true,
                true,
                false,
                "media",
                "not_implemented",
            ),
            route(
                "POST",
                "/api/v1/admin/medias/object-acl",
                "admin-media",
                true,
                true,
                true,
                "media",
                "not_implemented",
            ),
            ops_route(
                "GET",
                "/api/v1/admin/conversations",
                "admin-conversation",
                "conversation",
            ),
            ops_route(
                "GET",
                "/api/v1/admin/conversations/{conversation_id}",
                "admin-conversation",
                "conversation",
            ),
            ops_route(
                "GET",
                "/api/v1/admin/conversations/{conversation_id}/participants",
                "admin-conversation",
                "conversation",
            ),
            ops_route(
                "GET",
                "/api/v1/admin/users/{user_id}/devices",
                "admin-session",
                "signaling-online",
            ),
            ops_route(
                "GET",
                "/api/v1/admin/users/{user_id}/presence",
                "admin-session",
                "signaling-online",
            ),
            ops_route(
                "POST",
                "/api/v1/admin/users/{user_id}/kick",
                "admin-session",
                "signaling-online",
            ),
            ops_route(
                "POST",
                "/api/v1/admin/users/{user_id}/devices/{device_id}/kick",
                "admin-session",
                "signaling-online",
            ),
            ops_route(
                "POST",
                "/api/v1/admin/tenant/kick-all",
                "admin-session",
                "signaling-online",
            ),
            ops_route(
                "GET",
                "/api/v1/admin/tenant/runtime",
                "admin-tenant",
                "capability",
            ),
            ops_route(
                "GET",
                "/api/v1/admin/capabilities/tenant",
                "admin-capability",
                "capability",
            ),
            ops_route(
                "GET",
                "/api/v1/admin/plugins",
                "admin-capability",
                "capability",
            ),
        ],
    }
}

/// ADM-01 路由：POST 即写操作（踢线），其余只读。
fn ops_route(method: &str, path: &str, category: &str, downstream: &str) -> AdminGatewayRoute {
    route(
        method,
        path,
        category,
        true,
        true,
        method == "POST",
        downstream,
        "available",
    )
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

fn build_gateway_config_snapshot(
    settings: &GatewaySettings,
    capability: Option<&CapabilityDownstreamConfig>,
) -> AdminGatewayConfigSnapshot {
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
            capability_service_url: capability.map(|c| c.service_url.clone()),
            capability_static_fallback: capability
                .and_then(|c| optional_string(&c.static_fallback)),
            connect_timeout_secs: settings.grpc.connect_timeout_secs,
            request_timeout_secs: settings.grpc.request_timeout_secs,
        },
        // 认证方式由 admin_auth_middleware 决定，与 settings.auth 无关。
        auth: AdminAuthConfigSnapshot {
            mode: ADMIN_AUTH_MODE.to_string(),
            hook_url: None,
            hook_timeout_ms: 0,
            hook_secret_header: String::new(),
            hook_secret_configured: None,
        },
        // 网关没有限流层：不论设置如何都如实报告「未限流」。
        rate_limit: AdminRateLimitConfigSnapshot {
            enabled: false,
            requests_per_second: 0,
            burst_capacity: 0,
        },
        tracing: AdminTracingConfigSnapshot {
            enabled: settings.tracing.enabled,
            service_name: settings.tracing.service_name.clone(),
            sample_rate: settings.tracing.sample_rate,
        },
        // 快照不含任何密钥材料，没有需要打码的字段。
        redacted_fields: Vec::new(),
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

        let response = build_gateway_upstreams(&settings, None);

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
    fn admin_config_snapshot_reports_console_ed25519_whatever_the_unused_auth_settings() {
        // 网关设置里的 auth 是 API Gateway 那套（这里故意配成 http_hook 带 URL 与密钥），
        // Admin Gateway 不用它，快照也不能转述它。
        let hook = test_settings();
        let mut core_jwt = test_settings();
        core_jwt.auth = AuthProviderConfig::default();
        assert_eq!(core_jwt.auth.mode, AuthProviderMode::CoreJwt);

        for settings in [&hook, &core_jwt] {
            let snapshot = build_gateway_config_snapshot(settings, None);
            assert_eq!(snapshot.auth.mode, "console_ed25519");
            assert_eq!(snapshot.auth.hook_url, None);
            assert_eq!(snapshot.auth.hook_timeout_ms, 0);
            assert_eq!(snapshot.auth.hook_secret_header, "");
            assert_eq!(snapshot.auth.hook_secret_configured, None);
            assert!(snapshot.redacted_fields.is_empty());
        }
    }

    #[test]
    fn admin_config_snapshot_reports_no_rate_limit_whatever_the_settings() {
        // Admin Gateway 没有限流层：设置里开着 1000/2000 也不能报成在限流。
        let settings = test_settings();
        assert!(settings.rate_limit.enabled);
        let snapshot = build_gateway_config_snapshot(&settings, None);
        assert!(!snapshot.rate_limit.enabled);
        assert_eq!(snapshot.rate_limit.requests_per_second, 0);
        assert_eq!(snapshot.rate_limit.burst_capacity, 0);
    }

    #[test]
    fn capability_upstream_is_listed_only_when_enabled() {
        let settings = test_settings();
        let capability = CapabilityDownstreamConfig {
            service_url: "discovery://flare-capability".to_string(),
            static_fallback: "http://127.0.0.1:50110".to_string(),
        };

        let response = build_gateway_upstreams(&settings, Some(&capability));
        assert_eq!(response.upstreams.len(), 7);
        let upstream = response.upstreams.last().unwrap();
        assert_eq!(upstream.name, "capability");
        assert_eq!(
            upstream.static_fallback.as_deref(),
            Some("http://127.0.0.1:50110")
        );
        assert_eq!(build_gateway_health(Some(&capability)).upstream_count, 7);
        assert_eq!(build_gateway_health(None).upstream_count, 6);
        let snapshot = build_gateway_config_snapshot(&settings, Some(&capability));
        assert_eq!(
            snapshot.grpc.capability_service_url.as_deref(),
            Some("discovery://flare-capability")
        );
        assert_eq!(
            build_gateway_config_snapshot(&settings, None)
                .grpc
                .capability_service_url,
            None
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
                ..AuthProviderConfig::default()
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
