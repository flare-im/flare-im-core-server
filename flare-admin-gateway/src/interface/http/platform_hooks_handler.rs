//! 平台 Hook（`/platform/hooks/**`，业务网关 admin 分区的 `/admin/v1/hooks/**`）：经 Capability 的
//! `CapabilityService.Administer` 读写 `tenant_id` 为空的配置，读部署摘要和点位目录，调用一次目标做
//! 测试。
//!
//! 全部只给平台员工。读要求 `core:hooks:read`，写要求 `core:hooks:platform:write`；Capability 再核对
//! 签名里的范围。按 hook_id 的操作和应用路由走同一份实现，范围由 Capability 从配置行判断。
use axum::{Json, extract::Extension, http::HeaderMap};
use flare_grpc_proto::capability::{
    CreateHookConfigResponse, ListHookConfigsResponse, ListHookPointsRequest,
    ListHookPointsResponse, ListHookProfilesRequest, ListHookProfilesResponse,
    TestHookConfigResponse,
};
use flare_im_core_service_kit::clients::AdminGrpcClients;
use flare_server_core::{
    AuthenticatedPrincipal,
    http::{ApiResponse, HttpApiError, Result},
};
use std::sync::Arc;

use super::admin_auth_middleware::VerifiedStaffContext;
use super::admin_extract::{AdminJson, AdminPath, AdminQuery};
use super::audit_target::AuditTarget;
use super::hooks_handler::{self, administer, bad_request};
use super::ops_handler::{AdminCapability, audit_write};
use crate::application::admin_hooks::{
    AdminHookCreateHttpRequest, AdminHookDeleteHttp, AdminHookDetailHttp,
    AdminHookExecutionsPageHttp, AdminHookExecutionsQuery, AdminHookListQuery, AdminHookPointsHttp,
    AdminHookProfilesHttp, AdminHookStatisticsHttp, AdminHookStatisticsQuery, AdminHookStatusHttp,
    AdminHookStatusHttpRequest, AdminHookTestHttp, AdminHookTestHttpRequest,
    AdminHookUpdateHttpRequest, AdminHooksPageHttp, PLATFORM_TENANT, build_create_request,
    build_list_request, build_test_request, hook_http, hooks_page_http, operation, points_http,
    profiles_http, test_http,
};

#[utoipa::path(
    get,
    path = "/api/v1/admin/platform/hooks",
    tag = "Admin",
    params(
        ("hook_type" = Option<String>, Query, description = "Only hooks of this type, for example pre_send"),
        ("enabled_only" = Option<bool>, Query, description = "Only enabled hooks"),
        ("limit" = Option<i32>, Query, description = "Page size, default 100, capped at 1000"),
    ),
    responses(
        (status = 200, description = "Platform hooks", body = ApiResponse<AdminHooksPageHttp>),
        (status = 400, description = "Limit below 1 (ADMIN_PAGE_LIMIT_INVALID)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:read (ADMIN_PERMISSION_DENIED), or the caller is not platform staff (ADMIN_SCOPE_DENIED)"),
        (status = 500, description = "Capability downstream failed (ADMIN_HOOK_LIST_FAILED); its error text is not returned"),
        (status = 503, description = "Capability downstream is disabled (ADMIN_CAPABILITY_DISABLED) or unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Capability downstream did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
pub async fn list_platform_hooks(
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    AdminQuery(query): AdminQuery<AdminHookListQuery>,
) -> Result<Json<ApiResponse<AdminHooksPageHttp>>> {
    let (request, limit) = build_list_request(PLATFORM_TENANT, &query)
        .map_err(|err| bad_request(err.code, err.message))?;
    let mut client = capability.client().await?;
    let response: ListHookConfigsResponse = administer(
        &clients,
        &mut client,
        &staff,
        operation::LIST,
        "ListHookConfigsRequest",
        &request,
        "ADMIN_HOOK_LIST_FAILED",
    )
    .await?;
    Ok(Json(ApiResponse::success(hooks_page_http(response, limit))))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/platform/hooks",
    tag = "Admin",
    request_body = AdminHookCreateHttpRequest,
    params(
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8 (encodeURIComponent; plain ASCII may be sent as is), at most 512 bytes decoded and 1536 bytes as sent; recorded decoded in the audit log"),
        ("idempotency-key" = Option<String>, Header, description = "Required unless x-request-id is present; correlation only, not deduplicated"),
        ("x-request-id" = Option<String>, Header, description = "Required unless idempotency-key is present"),
    ),
    responses(
        (status = 200, description = "The platform hook was created", body = ApiResponse<AdminHookDetailHttp>),
        (status = 400, description = "Blank name (ADMIN_HOOK_NAME_REQUIRED) or hook_type (ADMIN_HOOK_TYPE_REQUIRED), or Capability refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:platform:write (ADMIN_PERMISSION_DENIED), the caller is not platform staff (ADMIN_SCOPE_DENIED), or audit reason / idempotency header missing (FORBIDDEN)"),
        (status = 500, description = "Capability downstream failed, or the name is taken (ADMIN_HOOK_CREATE_FAILED); its error text is not returned"),
        (status = 503, description = "Capability downstream is disabled (ADMIN_CAPABILITY_DISABLED) or unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Capability downstream did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
pub async fn create_platform_hook(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    AdminJson(request): AdminJson<AdminHookCreateHttpRequest>,
) -> Result<Json<ApiResponse<AdminHookDetailHttp>>> {
    let proto = build_create_request(PLATFORM_TENANT, request)
        .map_err(|err| bad_request(err.code, err.message))?;
    audit_write(
        "hooks.create",
        &principal,
        &headers,
        &AuditTarget::id(&proto.name),
    );
    let mut client = capability.client().await?;
    let response: CreateHookConfigResponse = administer(
        &clients,
        &mut client,
        &staff,
        operation::CREATE,
        "CreateHookConfigRequest",
        &proto,
        "ADMIN_HOOK_CREATE_FAILED",
    )
    .await?;
    let config = response
        .config
        .ok_or_else(|| HttpApiError::internal("ADMIN_HOOK_CREATE_FAILED", ""))?;
    Ok(Json(ApiResponse::success(AdminHookDetailHttp {
        hook: hook_http(config),
    })))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/platform/hooks/points",
    tag = "Admin",
    responses(
        (status = 200, description = "The hook point catalog", body = ApiResponse<AdminHookPointsHttp>),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:read (ADMIN_PERMISSION_DENIED), or the caller is not platform staff (ADMIN_SCOPE_DENIED)"),
        (status = 500, description = "Capability downstream failed (ADMIN_HOOK_POINTS_FAILED)"),
        (status = 503, description = "Capability downstream is disabled (ADMIN_CAPABILITY_DISABLED) or unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Capability downstream did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
pub async fn list_hook_points(
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
) -> Result<Json<ApiResponse<AdminHookPointsHttp>>> {
    let mut client = capability.client().await?;
    let response: ListHookPointsResponse = administer(
        &clients,
        &mut client,
        &staff,
        operation::POINT_LIST,
        "ListHookPointsRequest",
        &ListHookPointsRequest {},
        "ADMIN_HOOK_POINTS_FAILED",
    )
    .await?;
    Ok(Json(ApiResponse::success(points_http(response))))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/platform/hooks/profiles",
    tag = "Admin",
    responses(
        (status = 200, description = "Deployment hooks, one entry per service", body = ApiResponse<AdminHookProfilesHttp>),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:read (ADMIN_PERMISSION_DENIED), or the caller is not platform staff (ADMIN_SCOPE_DENIED)"),
        (status = 500, description = "Capability downstream failed (ADMIN_HOOK_PROFILES_FAILED)"),
        (status = 503, description = "Capability downstream is disabled (ADMIN_CAPABILITY_DISABLED) or unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Capability downstream did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
pub async fn list_hook_profiles(
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
) -> Result<Json<ApiResponse<AdminHookProfilesHttp>>> {
    let mut client = capability.client().await?;
    let response: ListHookProfilesResponse = administer(
        &clients,
        &mut client,
        &staff,
        operation::PROFILE_LIST,
        "ListHookProfilesRequest",
        &ListHookProfilesRequest {},
        "ADMIN_HOOK_PROFILES_FAILED",
    )
    .await?;
    Ok(Json(ApiResponse::success(profiles_http(response))))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/platform/hooks/test",
    tag = "Admin",
    request_body = AdminHookTestHttpRequest,
    params(
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8 (encodeURIComponent; plain ASCII may be sent as is), at most 512 bytes decoded and 1536 bytes as sent; recorded decoded in the audit log"),
        ("idempotency-key" = Option<String>, Header, description = "Required unless x-request-id is present; correlation only, not deduplicated"),
        ("x-request-id" = Option<String>, Header, description = "Required unless idempotency-key is present"),
    ),
    responses(
        (status = 200, description = "The target was called once with a sample message; `outcome` is what it answered, a failed call included", body = ApiResponse<AdminHookTestHttp>),
        (status = 400, description = "Neither hook_id nor transport (ADMIN_HOOK_TARGET_REQUIRED), or Capability refused the target (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:platform:write (ADMIN_PERMISSION_DENIED), the caller is not platform staff (ADMIN_SCOPE_DENIED), or audit reason / idempotency header missing (FORBIDDEN)"),
        (status = 404, description = "No such hook (NOT_FOUND)"),
        (status = 500, description = "Capability downstream failed (ADMIN_HOOK_TEST_FAILED); its error text is not returned"),
        (status = 503, description = "Capability downstream is disabled (ADMIN_CAPABILITY_DISABLED) or unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Capability downstream did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
pub async fn test_hook(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    AdminJson(request): AdminJson<AdminHookTestHttpRequest>,
) -> Result<Json<ApiResponse<AdminHookTestHttp>>> {
    let proto = build_test_request(request).map_err(|err| bad_request(err.code, err.message))?;
    audit_write(
        "hooks.test",
        &principal,
        &headers,
        &AuditTarget::id(&proto.hook_id),
    );
    let mut client = capability.client().await?;
    let response: TestHookConfigResponse = administer(
        &clients,
        &mut client,
        &staff,
        operation::TEST,
        "TestHookConfigRequest",
        &proto,
        "ADMIN_HOOK_TEST_FAILED",
    )
    .await?;
    Ok(Json(ApiResponse::success(test_http(response))))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/platform/hooks/{hook_id}",
    tag = "Admin",
    params(("hook_id" = String, Path, description = "Hook id, or hook_type:name")),
    responses(
        (status = 200, description = "The platform hook", body = ApiResponse<AdminHookDetailHttp>),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:read (ADMIN_PERMISSION_DENIED), the caller is not platform staff (ADMIN_SCOPE_DENIED), or the hook belongs to an application (FORBIDDEN)"),
        (status = 404, description = "No such hook (NOT_FOUND)"),
    ),
)]
pub async fn get_platform_hook(
    clients: Extension<Arc<AdminGrpcClients>>,
    capability: Extension<AdminCapability>,
    staff: Extension<VerifiedStaffContext>,
    hook_id: AdminPath<String>,
) -> Result<Json<ApiResponse<AdminHookDetailHttp>>> {
    hooks_handler::get_hook(clients, capability, staff, hook_id).await
}

#[utoipa::path(
    put,
    path = "/api/v1/admin/platform/hooks/{hook_id}",
    tag = "Admin",
    request_body = AdminHookUpdateHttpRequest,
    params(
        ("hook_id" = String, Path, description = "Hook id, or hook_type:name"),
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8 (encodeURIComponent; plain ASCII may be sent as is), at most 512 bytes decoded and 1536 bytes as sent; recorded decoded in the audit log"),
        ("idempotency-key" = Option<String>, Header, description = "Required unless x-request-id is present; correlation only, not deduplicated"),
        ("x-request-id" = Option<String>, Header, description = "Required unless idempotency-key is present"),
    ),
    responses(
        (status = 200, description = "The platform hook after the update", body = ApiResponse<AdminHookDetailHttp>),
        (status = 400, description = "Capability refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:platform:write (ADMIN_PERMISSION_DENIED), the caller is not platform staff (ADMIN_SCOPE_DENIED), audit reason / idempotency header missing (FORBIDDEN), or the hook belongs to an application (FORBIDDEN)"),
        (status = 404, description = "No such hook (NOT_FOUND)"),
    ),
)]
// Every argument is an axum extractor; the handler has no state of its own.
#[allow(clippy::too_many_arguments)]
pub async fn update_platform_hook(
    headers: HeaderMap,
    clients: Extension<Arc<AdminGrpcClients>>,
    capability: Extension<AdminCapability>,
    staff: Extension<VerifiedStaffContext>,
    principal: Extension<AuthenticatedPrincipal>,
    hook_id: AdminPath<String>,
    request: AdminJson<AdminHookUpdateHttpRequest>,
) -> Result<Json<ApiResponse<AdminHookDetailHttp>>> {
    hooks_handler::update_hook(
        headers, clients, capability, staff, principal, hook_id, request,
    )
    .await
}

#[utoipa::path(
    delete,
    path = "/api/v1/admin/platform/hooks/{hook_id}",
    tag = "Admin",
    params(
        ("hook_id" = String, Path, description = "Hook id, or hook_type:name"),
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8 (encodeURIComponent; plain ASCII may be sent as is), at most 512 bytes decoded and 1536 bytes as sent; recorded decoded in the audit log"),
        ("idempotency-key" = Option<String>, Header, description = "Required unless x-request-id is present; correlation only, not deduplicated"),
        ("x-request-id" = Option<String>, Header, description = "Required unless idempotency-key is present"),
    ),
    responses(
        (status = 200, description = "The platform hook was deleted", body = ApiResponse<AdminHookDeleteHttp>),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:platform:write (ADMIN_PERMISSION_DENIED), the caller is not platform staff (ADMIN_SCOPE_DENIED), audit reason / idempotency header missing (FORBIDDEN), or the hook belongs to an application (FORBIDDEN)"),
        (status = 404, description = "No such hook (NOT_FOUND)"),
    ),
)]
pub async fn delete_platform_hook(
    headers: HeaderMap,
    clients: Extension<Arc<AdminGrpcClients>>,
    capability: Extension<AdminCapability>,
    staff: Extension<VerifiedStaffContext>,
    principal: Extension<AuthenticatedPrincipal>,
    hook_id: AdminPath<String>,
) -> Result<Json<ApiResponse<AdminHookDeleteHttp>>> {
    hooks_handler::delete_hook(headers, clients, capability, staff, principal, hook_id).await
}

#[utoipa::path(
    put,
    path = "/api/v1/admin/platform/hooks/{hook_id}/status",
    tag = "Admin",
    request_body = AdminHookStatusHttpRequest,
    params(
        ("hook_id" = String, Path, description = "Hook id, or hook_type:name"),
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8 (encodeURIComponent; plain ASCII may be sent as is), at most 512 bytes decoded and 1536 bytes as sent; recorded decoded in the audit log"),
        ("idempotency-key" = Option<String>, Header, description = "Required unless x-request-id is present; correlation only, not deduplicated"),
        ("x-request-id" = Option<String>, Header, description = "Required unless idempotency-key is present"),
    ),
    responses(
        (status = 200, description = "The platform hook's status", body = ApiResponse<AdminHookStatusHttp>),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:platform:write (ADMIN_PERMISSION_DENIED), the caller is not platform staff (ADMIN_SCOPE_DENIED), audit reason / idempotency header missing (FORBIDDEN), or the hook belongs to an application (FORBIDDEN)"),
        (status = 404, description = "No such hook (NOT_FOUND)"),
    ),
)]
// Every argument is an axum extractor; the handler has no state of its own.
#[allow(clippy::too_many_arguments)]
pub async fn set_platform_hook_status(
    headers: HeaderMap,
    clients: Extension<Arc<AdminGrpcClients>>,
    capability: Extension<AdminCapability>,
    staff: Extension<VerifiedStaffContext>,
    principal: Extension<AuthenticatedPrincipal>,
    hook_id: AdminPath<String>,
    request: AdminJson<AdminHookStatusHttpRequest>,
) -> Result<Json<ApiResponse<AdminHookStatusHttp>>> {
    hooks_handler::set_hook_status(
        headers, clients, capability, staff, principal, hook_id, request,
    )
    .await
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/platform/hooks/{hook_id}/statistics",
    tag = "Admin",
    params(
        ("hook_id" = String, Path, description = "Hook id or hook_type:name; a deployment hook only by hook_type:name"),
        ("start_time" = Option<i64>, Query, description = "Unix milliseconds; both omitted means the last 24 hours"),
        ("end_time" = Option<i64>, Query, description = "Unix milliseconds"),
    ),
    responses(
        (status = 200, description = "Execution statistics across all applications", body = ApiResponse<AdminHookStatisticsHttp>),
        (status = 400, description = "end_time before start_time (ADMIN_HOOK_TIME_RANGE_INVALID)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:read (ADMIN_PERMISSION_DENIED), the caller is not platform staff (ADMIN_SCOPE_DENIED), or the hook belongs to an application (FORBIDDEN)"),
        (status = 404, description = "No such hook (NOT_FOUND)"),
    ),
)]
pub async fn get_platform_hook_statistics(
    clients: Extension<Arc<AdminGrpcClients>>,
    capability: Extension<AdminCapability>,
    staff: Extension<VerifiedStaffContext>,
    hook_id: AdminPath<String>,
    query: AdminQuery<AdminHookStatisticsQuery>,
) -> Result<Json<ApiResponse<AdminHookStatisticsHttp>>> {
    hooks_handler::get_hook_statistics(clients, capability, staff, hook_id, query).await
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/platform/hooks/{hook_id}/executions",
    tag = "Admin",
    params(
        ("hook_id" = String, Path, description = "Hook id or hook_type:name; a deployment hook only by hook_type:name"),
        ("message_id" = Option<String>, Query, description = "Only this message"),
        ("success_only" = Option<bool>, Query, description = "Only successful executions"),
        ("start_time" = Option<i64>, Query, description = "Unix milliseconds"),
        ("end_time" = Option<i64>, Query, description = "Unix milliseconds"),
        ("limit" = Option<i32>, Query, description = "Page size, default 100, capped at 1000"),
    ),
    responses(
        (status = 200, description = "Executions across all applications, newest first", body = ApiResponse<AdminHookExecutionsPageHttp>),
        (status = 400, description = "Limit below 1 (ADMIN_PAGE_LIMIT_INVALID), or end_time before start_time (ADMIN_HOOK_TIME_RANGE_INVALID)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:read (ADMIN_PERMISSION_DENIED), the caller is not platform staff (ADMIN_SCOPE_DENIED), or the hook belongs to an application (FORBIDDEN)"),
        (status = 404, description = "No such hook (NOT_FOUND)"),
    ),
)]
pub async fn query_platform_hook_executions(
    clients: Extension<Arc<AdminGrpcClients>>,
    capability: Extension<AdminCapability>,
    staff: Extension<VerifiedStaffContext>,
    hook_id: AdminPath<String>,
    query: AdminQuery<AdminHookExecutionsQuery>,
) -> Result<Json<ApiResponse<AdminHookExecutionsPageHttp>>> {
    hooks_handler::query_hook_executions(clients, capability, staff, hook_id, query).await
}
