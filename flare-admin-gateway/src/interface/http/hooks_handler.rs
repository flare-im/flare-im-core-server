//! Hook 治理路由（`/hooks/**`）：经 Capability 的 `CapabilityService.Administer`
//! （`flare.extension.v1.hook_config.*`）读写 Hook 配置、查看统计与执行记录。
//!
//! 只经 `admin_auth_middleware` 可达，租户只取自 Staff 上下文：创建与列表请求的 `tenant_id`
//! 由网关填入；按 hook_id 的操作由 Capability 先核对该配置属于签名里的租户。读要求
//! `core:hooks:read`，写要求 `core:hooks:write`，Capability 按 operation 复核。写操作成功后发
//! `flare.audit.effect.v1`（聚合 `hook_config`）。
use axum::{Json, extract::Extension, http::HeaderMap};
use flare_grpc_proto::capability::{
    CreateHookConfigResponse, DeleteHookConfigRequest, DeleteHookConfigResponse, GenericResponse,
    GetHookConfigRequest, GetHookConfigResponse, GetHookStatisticsResponse,
    ListHookConfigsResponse, QueryHookExecutionsResponse, SetHookStatusResponse,
    UpdateHookConfigResponse,
};
use flare_im_service_kit::clients::{AdminGrpcClients, CapabilityAdminClientWrapper};
use flare_server_core::{
    AuthenticatedPrincipal,
    error::{ErrorBuilder, ErrorCode},
    http::{ApiResponse, HttpApiError, Result},
};
use prost::Message;
use std::sync::Arc;

use super::admin_auth_middleware::VerifiedStaffContext;
use super::admin_extract::{AdminJson, AdminPath, AdminQuery};
use super::audit_target::AuditTarget;
use super::ops_handler::{AdminCapability, admin_rpc_error, audit_write, verified_tenant};
use crate::application::admin_hooks::{
    AdminHookCreateHttpRequest, AdminHookDeleteHttp, AdminHookDetailHttp,
    AdminHookExecutionsPageHttp, AdminHookExecutionsQuery, AdminHookListQuery,
    AdminHookStatisticsHttp, AdminHookStatisticsQuery, AdminHookStatusHttp,
    AdminHookStatusHttpRequest, AdminHookUpdateHttpRequest, AdminHooksPageHttp,
    build_create_request, build_executions_request, build_list_request, build_statistics_request,
    build_status_request, build_update_request, delete_http, executions_page_http, hook_http,
    hooks_page_http, operation, statistics_http, status_http,
};

/// `GenericResponse` 不是成功，或载荷缺失、解不开：Capability 的治理服务没有给出它承诺的
/// 应答，按路由固定原因回 500；它的错误文案不回显。
fn decode_payload<R: Message + Default>(
    response: GenericResponse,
    failure_reason: &'static str,
) -> Result<R> {
    if !response.ok {
        tracing::warn!(
            error_code = %response.error_code,
            failure_reason,
            "hook administer returned an error payload"
        );
        return Err(HttpApiError(
            ErrorBuilder::new(ErrorCode::HttpInternalServerError, failure_reason).build_error(),
        ));
    }
    let payload = response.payload.ok_or_else(|| {
        HttpApiError::internal(failure_reason, "hook administer answered without a payload")
    })?;
    R::decode(payload.value.as_slice()).map_err(|_| {
        HttpApiError::internal(failure_reason, "hook administer payload does not decode")
    })
}

async fn administer<P: Message, R: Message + Default>(
    clients: &AdminGrpcClients,
    client: &mut CapabilityAdminClientWrapper,
    staff: &VerifiedStaffContext,
    op: &str,
    request_type: &str,
    payload: &P,
    failure_reason: &'static str,
) -> Result<R> {
    let response = clients
        .bounded(client.administer(staff.token(), op, request_type, payload, staff.request_id()))
        .await
        .map_err(|err| admin_rpc_error(err, failure_reason))?;
    decode_payload(response, failure_reason)
}

fn bad_request(code: &'static str, message: String) -> HttpApiError {
    HttpApiError::bad_request(code, message)
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/hooks",
    tag = "Admin",
    params(
        ("hook_type" = Option<String>, Query, description = "Only hooks of this type, for example message_pre_send"),
        ("enabled_only" = Option<bool>, Query, description = "Only enabled hooks"),
        ("limit" = Option<i32>, Query, description = "Page size, default 100, capped at 1000"),
    ),
    responses(
        (status = 200, description = "Hook configurations of the staff application", body = ApiResponse<AdminHooksPageHttp>),
        (status = 400, description = "Limit below 1 (ADMIN_PAGE_LIMIT_INVALID), or Capability refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:read (ADMIN_PERMISSION_DENIED), or Capability refused the application (FORBIDDEN)"),
        (status = 500, description = "Capability downstream failed or hook governance is disabled there (ADMIN_HOOK_LIST_FAILED); its error text is not returned"),
        (status = 502, description = "Capability downstream refused the forwarded staff context as unauthenticated, a deployment error (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Capability downstream is disabled (ADMIN_CAPABILITY_DISABLED) or unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Capability downstream did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
pub async fn list_hooks(
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    AdminQuery(query): AdminQuery<AdminHookListQuery>,
) -> Result<Json<ApiResponse<AdminHooksPageHttp>>> {
    let tenant_id = verified_tenant(&principal)?;
    let (request, limit) =
        build_list_request(&tenant_id, &query).map_err(|err| bad_request(err.code, err.message))?;
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
    path = "/api/v1/admin/hooks",
    tag = "Admin",
    request_body = AdminHookCreateHttpRequest,
    params(
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8 (encodeURIComponent; plain ASCII may be sent as is), at most 512 bytes decoded and 1536 bytes as sent; recorded decoded in the audit log"),
        ("idempotency-key" = Option<String>, Header, description = "Required unless x-request-id is present; correlation only, not deduplicated"),
        ("x-request-id" = Option<String>, Header, description = "Required unless idempotency-key is present"),
    ),
    responses(
        (status = 200, description = "The hook was created in the staff application", body = ApiResponse<AdminHookDetailHttp>),
        (status = 400, description = "Blank name (ADMIN_HOOK_NAME_REQUIRED) or hook_type (ADMIN_HOOK_TYPE_REQUIRED), an unknown hook point (ADMIN_HOOK_POINT_INVALID), an extension that is not base64 (ADMIN_HOOK_EXTENSION_INVALID), or Capability refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:write (ADMIN_PERMISSION_DENIED), audit reason / idempotency header missing (FORBIDDEN), or Capability refused the application (FORBIDDEN)"),
        (status = 500, description = "Capability downstream failed or hook governance is disabled there (ADMIN_HOOK_CREATE_FAILED); its error text is not returned"),
        (status = 502, description = "Capability downstream refused the forwarded staff context as unauthenticated, a deployment error (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Capability downstream is disabled (ADMIN_CAPABILITY_DISABLED) or unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Capability downstream did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
pub async fn create_hook(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    AdminJson(request): AdminJson<AdminHookCreateHttpRequest>,
) -> Result<Json<ApiResponse<AdminHookDetailHttp>>> {
    let tenant_id = verified_tenant(&principal)?;
    let proto = build_create_request(&tenant_id, request)
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
    let hook = hook_http(config);
    Ok(Json(ApiResponse::success(AdminHookDetailHttp { hook })))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/hooks/{hook_id}",
    tag = "Admin",
    params(("hook_id" = String, Path, description = "Hook id, or hook_type:name")),
    responses(
        (status = 200, description = "The hook configuration", body = ApiResponse<AdminHookDetailHttp>),
        (status = 400, description = "Capability refused the hook id (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:read (ADMIN_PERMISSION_DENIED), or the hook belongs to another application (FORBIDDEN)"),
        (status = 404, description = "No such hook (NOT_FOUND)"),
        (status = 500, description = "Capability downstream failed or hook governance is disabled there (ADMIN_HOOK_DETAIL_FAILED); its error text is not returned"),
        (status = 502, description = "Capability downstream refused the forwarded staff context as unauthenticated, a deployment error (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Capability downstream is disabled (ADMIN_CAPABILITY_DISABLED) or unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Capability downstream did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
pub async fn get_hook(
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    AdminPath(hook_id): AdminPath<String>,
) -> Result<Json<ApiResponse<AdminHookDetailHttp>>> {
    let mut client = capability.client().await?;
    let response: GetHookConfigResponse = administer(
        &clients,
        &mut client,
        &staff,
        operation::GET,
        "GetHookConfigRequest",
        &GetHookConfigRequest { hook_id },
        "ADMIN_HOOK_DETAIL_FAILED",
    )
    .await?;
    let config = response
        .config
        .ok_or_else(|| HttpApiError::not_found("hook not found"))?;
    Ok(Json(ApiResponse::success(AdminHookDetailHttp {
        hook: hook_http(config),
    })))
}

#[utoipa::path(
    put,
    path = "/api/v1/admin/hooks/{hook_id}",
    tag = "Admin",
    request_body = AdminHookUpdateHttpRequest,
    params(
        ("hook_id" = String, Path, description = "Hook id, or hook_type:name"),
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8 (encodeURIComponent; plain ASCII may be sent as is), at most 512 bytes decoded and 1536 bytes as sent; recorded decoded in the audit log"),
        ("idempotency-key" = Option<String>, Header, description = "Required unless x-request-id is present; correlation only, not deduplicated"),
        ("x-request-id" = Option<String>, Header, description = "Required unless idempotency-key is present"),
    ),
    responses(
        (status = 200, description = "The hook configuration after the update", body = ApiResponse<AdminHookDetailHttp>),
        (status = 400, description = "An unknown hook point (ADMIN_HOOK_POINT_INVALID), an extension that is not base64 (ADMIN_HOOK_EXTENSION_INVALID), or Capability refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:write (ADMIN_PERMISSION_DENIED), audit reason / idempotency header missing (FORBIDDEN), or the hook belongs to another application (FORBIDDEN)"),
        (status = 404, description = "No such hook (NOT_FOUND)"),
        (status = 500, description = "Capability downstream failed or hook governance is disabled there (ADMIN_HOOK_UPDATE_FAILED); its error text is not returned"),
        (status = 502, description = "Capability downstream refused the forwarded staff context as unauthenticated, a deployment error (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Capability downstream is disabled (ADMIN_CAPABILITY_DISABLED) or unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Capability downstream did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
// Every argument is an axum extractor; the handler has no state of its own.
#[allow(clippy::too_many_arguments)]
pub async fn update_hook(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    AdminPath(hook_id): AdminPath<String>,
    AdminJson(request): AdminJson<AdminHookUpdateHttpRequest>,
) -> Result<Json<ApiResponse<AdminHookDetailHttp>>> {
    let proto = build_update_request(&hook_id, request)
        .map_err(|err| bad_request(err.code, err.message))?;
    audit_write(
        "hooks.update",
        &principal,
        &headers,
        &AuditTarget::id(&hook_id),
    );
    let mut client = capability.client().await?;
    let response: UpdateHookConfigResponse = administer(
        &clients,
        &mut client,
        &staff,
        operation::UPDATE,
        "UpdateHookConfigRequest",
        &proto,
        "ADMIN_HOOK_UPDATE_FAILED",
    )
    .await?;
    let config = response
        .config
        .ok_or_else(|| HttpApiError::internal("ADMIN_HOOK_UPDATE_FAILED", ""))?;
    Ok(Json(ApiResponse::success(AdminHookDetailHttp {
        hook: hook_http(config),
    })))
}

#[utoipa::path(
    delete,
    path = "/api/v1/admin/hooks/{hook_id}",
    tag = "Admin",
    params(
        ("hook_id" = String, Path, description = "Hook id, or hook_type:name"),
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8 (encodeURIComponent; plain ASCII may be sent as is), at most 512 bytes decoded and 1536 bytes as sent; recorded decoded in the audit log"),
        ("idempotency-key" = Option<String>, Header, description = "Required unless x-request-id is present; correlation only, not deduplicated"),
        ("x-request-id" = Option<String>, Header, description = "Required unless idempotency-key is present"),
    ),
    responses(
        (status = 200, description = "The hook was deleted (`deleted=false` when Capability reports it was not)", body = ApiResponse<AdminHookDeleteHttp>),
        (status = 400, description = "Capability refused the hook id (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:write (ADMIN_PERMISSION_DENIED), audit reason / idempotency header missing (FORBIDDEN), or the hook belongs to another application (FORBIDDEN)"),
        (status = 404, description = "No such hook (NOT_FOUND)"),
        (status = 500, description = "Capability downstream failed or hook governance is disabled there (ADMIN_HOOK_DELETE_FAILED); its error text is not returned"),
        (status = 502, description = "Capability downstream refused the forwarded staff context as unauthenticated, a deployment error (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Capability downstream is disabled (ADMIN_CAPABILITY_DISABLED) or unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Capability downstream did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
pub async fn delete_hook(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    AdminPath(hook_id): AdminPath<String>,
) -> Result<Json<ApiResponse<AdminHookDeleteHttp>>> {
    audit_write(
        "hooks.delete",
        &principal,
        &headers,
        &AuditTarget::id(&hook_id),
    );
    let mut client = capability.client().await?;
    let response: DeleteHookConfigResponse = administer(
        &clients,
        &mut client,
        &staff,
        operation::DELETE,
        "DeleteHookConfigRequest",
        &DeleteHookConfigRequest {
            hook_id: hook_id.clone(),
        },
        "ADMIN_HOOK_DELETE_FAILED",
    )
    .await?;
    Ok(Json(ApiResponse::success(delete_http(hook_id, response))))
}

#[utoipa::path(
    put,
    path = "/api/v1/admin/hooks/{hook_id}/status",
    tag = "Admin",
    request_body = AdminHookStatusHttpRequest,
    params(
        ("hook_id" = String, Path, description = "Hook id, or hook_type:name"),
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8 (encodeURIComponent; plain ASCII may be sent as is), at most 512 bytes decoded and 1536 bytes as sent; recorded decoded in the audit log"),
        ("idempotency-key" = Option<String>, Header, description = "Required unless x-request-id is present; correlation only, not deduplicated"),
        ("x-request-id" = Option<String>, Header, description = "Required unless idempotency-key is present"),
    ),
    responses(
        (status = 200, description = "The hook was enabled or disabled", body = ApiResponse<AdminHookStatusHttp>),
        (status = 400, description = "Capability refused the hook id (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:write (ADMIN_PERMISSION_DENIED), audit reason / idempotency header missing (FORBIDDEN), or the hook belongs to another application (FORBIDDEN)"),
        (status = 404, description = "No such hook (NOT_FOUND)"),
        (status = 500, description = "Capability downstream failed or hook governance is disabled there (ADMIN_HOOK_STATUS_FAILED); its error text is not returned"),
        (status = 502, description = "Capability downstream refused the forwarded staff context as unauthenticated, a deployment error (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Capability downstream is disabled (ADMIN_CAPABILITY_DISABLED) or unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Capability downstream did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
// Every argument is an axum extractor; the handler has no state of its own.
#[allow(clippy::too_many_arguments)]
pub async fn set_hook_status(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    AdminPath(hook_id): AdminPath<String>,
    AdminJson(request): AdminJson<AdminHookStatusHttpRequest>,
) -> Result<Json<ApiResponse<AdminHookStatusHttp>>> {
    let enabled = request.enabled;
    audit_write(
        "hooks.set_status",
        &principal,
        &headers,
        &AuditTarget::id(&hook_id),
    );
    let mut client = capability.client().await?;
    let response: SetHookStatusResponse = administer(
        &clients,
        &mut client,
        &staff,
        operation::SET_STATUS,
        "SetHookStatusRequest",
        &build_status_request(&hook_id, request),
        "ADMIN_HOOK_STATUS_FAILED",
    )
    .await?;
    Ok(Json(ApiResponse::success(status_http(
        hook_id, enabled, response,
    ))))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/hooks/{hook_id}/statistics",
    tag = "Admin",
    params(
        ("hook_id" = String, Path, description = "Hook id, or hook_type:name"),
        ("start_time" = Option<i64>, Query, description = "Unix milliseconds; with end_time absent too, Capability takes the last 24 hours"),
        ("end_time" = Option<i64>, Query, description = "Unix milliseconds"),
    ),
    responses(
        (status = 200, description = "Execution statistics of the hook in the range", body = ApiResponse<AdminHookStatisticsHttp>),
        (status = 400, description = "end_time earlier than start_time (ADMIN_HOOK_TIME_RANGE_INVALID), or Capability refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:read (ADMIN_PERMISSION_DENIED), or the hook belongs to another application (FORBIDDEN)"),
        (status = 404, description = "No such hook (NOT_FOUND)"),
        (status = 500, description = "Capability downstream failed or hook governance is disabled there (ADMIN_HOOK_STATISTICS_FAILED); its error text is not returned"),
        (status = 502, description = "Capability downstream refused the forwarded staff context as unauthenticated, a deployment error (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Capability downstream is disabled (ADMIN_CAPABILITY_DISABLED) or unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Capability downstream did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
pub async fn get_hook_statistics(
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    AdminPath(hook_id): AdminPath<String>,
    AdminQuery(query): AdminQuery<AdminHookStatisticsQuery>,
) -> Result<Json<ApiResponse<AdminHookStatisticsHttp>>> {
    let proto = build_statistics_request(&hook_id, &query)
        .map_err(|err| bad_request(err.code, err.message))?;
    let mut client = capability.client().await?;
    let response: GetHookStatisticsResponse = administer(
        &clients,
        &mut client,
        &staff,
        operation::GET_STATISTICS,
        "GetHookStatisticsRequest",
        &proto,
        "ADMIN_HOOK_STATISTICS_FAILED",
    )
    .await?;
    Ok(Json(ApiResponse::success(statistics_http(
        hook_id,
        response.statistics,
    ))))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/hooks/{hook_id}/executions",
    tag = "Admin",
    params(
        ("hook_id" = String, Path, description = "Hook id, or hook_type:name"),
        ("message_id" = Option<String>, Query, description = "Only executions for this message"),
        ("success_only" = Option<bool>, Query, description = "Only successful executions"),
        ("start_time" = Option<i64>, Query, description = "Unix milliseconds"),
        ("end_time" = Option<i64>, Query, description = "Unix milliseconds"),
        ("limit" = Option<i32>, Query, description = "Page size, default 100, capped at 1000"),
    ),
    responses(
        (status = 200, description = "Recent executions of the hook, newest first", body = ApiResponse<AdminHookExecutionsPageHttp>),
        (status = 400, description = "Limit below 1 (ADMIN_PAGE_LIMIT_INVALID), end_time earlier than start_time (ADMIN_HOOK_TIME_RANGE_INVALID), or Capability refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:hooks:read (ADMIN_PERMISSION_DENIED), or the hook belongs to another application (FORBIDDEN)"),
        (status = 404, description = "No such hook (NOT_FOUND)"),
        (status = 500, description = "Capability downstream failed or hook governance is disabled there (ADMIN_HOOK_EXECUTIONS_FAILED); its error text is not returned"),
        (status = 502, description = "Capability downstream refused the forwarded staff context as unauthenticated, a deployment error (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Capability downstream is disabled (ADMIN_CAPABILITY_DISABLED) or unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Capability downstream did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ),
)]
pub async fn query_hook_executions(
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Extension(capability): Extension<AdminCapability>,
    Extension(staff): Extension<VerifiedStaffContext>,
    AdminPath(hook_id): AdminPath<String>,
    AdminQuery(query): AdminQuery<AdminHookExecutionsQuery>,
) -> Result<Json<ApiResponse<AdminHookExecutionsPageHttp>>> {
    let (proto, limit) = build_executions_request(&hook_id, &query)
        .map_err(|err| bad_request(err.code, err.message))?;
    let mut client = capability.client().await?;
    let response: QueryHookExecutionsResponse = administer(
        &clients,
        &mut client,
        &staff,
        operation::QUERY_EXECUTIONS,
        "QueryHookExecutionsRequest",
        &proto,
        "ADMIN_HOOK_EXECUTIONS_FAILED",
    )
    .await?;
    Ok(Json(ApiResponse::success(executions_page_http(
        hook_id, response, limit,
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;

    #[test]
    fn administer_payloads_must_be_ok_present_and_decodable() {
        let good = GenericResponse {
            ok: true,
            request_id: "req-1".into(),
            payload: Some(prost_types::Any {
                type_url: "type.googleapis.com/flare.capability.v1.DeleteHookConfigResponse".into(),
                value: DeleteHookConfigResponse { success: true }.encode_to_vec(),
            }),
            error_code: String::new(),
            error_message: String::new(),
        };
        let decoded: DeleteHookConfigResponse = decode_payload(good, "ADMIN_TEST_FAILED").unwrap();
        assert!(decoded.success);

        let failed = GenericResponse {
            ok: false,
            request_id: "req-1".into(),
            payload: None,
            error_code: "boom".into(),
            error_message: "detail id-123".into(),
        };
        let error =
            decode_payload::<DeleteHookConfigResponse>(failed, "ADMIN_TEST_FAILED").unwrap_err();
        assert!(!error.to_string().contains("id-123"));
        assert_eq!(error.into_response().status().as_u16(), 500);

        let missing = GenericResponse {
            ok: true,
            request_id: "req-1".into(),
            payload: None,
            error_code: String::new(),
            error_message: String::new(),
        };
        assert!(decode_payload::<DeleteHookConfigResponse>(missing, "ADMIN_TEST_FAILED").is_err());
    }
}
