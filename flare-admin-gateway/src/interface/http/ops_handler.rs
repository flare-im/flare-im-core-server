//! ADM-01 管理员接口：会话与成员、在线设备与状态、踢线、租户运行时、能力开关与插件。
//!
//! 只经 `admin_auth_middleware` 可达。下游调用只带中间件刚验过的 admin 令牌原样转发
//! （[`VerifiedAdminAssertion`]），不带任何未签名的身份头；租户只取自令牌。写操作
//! （三个踢线入口）的审计原因与幂等键已由中间件强制，这里只记审计日志；会话成员、用户设备、
//! 在线状态三条敏感读也记一条审计日志。
use axum::{
    Json,
    extract::{Extension, Path, Query},
    http::HeaderMap,
};
use flare_core_base::context::keys;
use flare_grpc_proto::conversation::{
    AdminGetConversationRequest, AdminListConversationsRequest, AdminListParticipantsRequest,
};
use flare_grpc_proto::signaling::online::{
    AdminGetUserPresenceRequest, AdminListUserDevicesRequest, KickDeviceRequest, KickTenantRequest,
};
use flare_im_service_kit::clients::{
    CapabilityAdminClientWrapper, CapabilityDownstreamConfig, GrpcClients,
};
use flare_server_core::{
    AuthenticatedPrincipal,
    error::{ErrorBuilder, ErrorCode, FlareError},
    http::{ApiResponse, HttpApiError, Result},
};
use std::sync::Arc;

use super::admin_auth_middleware::VerifiedAdminAssertion;
use super::admin_contract::{AUDIT_REASON_HEADER, IDEMPOTENCY_KEY_HEADER};
use crate::application::admin_ops::{
    AdminConversationDetailHttp, AdminConversationListQuery, AdminConversationPageHttp,
    AdminDeviceKickHttp, AdminParticipantListQuery, AdminParticipantPageHttp, AdminPluginListQuery,
    AdminPluginsHttp, AdminTenantCapabilitiesHttp, AdminTenantKickHttp, AdminTenantRuntimeHttp,
    AdminUserDevicesHttp, AdminUserKickHttp, AdminUserPresenceHttp, DEFAULT_CONVERSATION_PAGE,
    DEFAULT_PARTICIPANT_PAGE, MAX_CONVERSATION_PAGE, MAX_PARTICIPANT_PAGE, conversation_http,
    conversation_page_http, page_limit, participant_page_http, plugins_http,
    tenant_capabilities_http, tenant_runtime_http, user_devices_http, user_presence_http,
};

/// Admin Gateway 的可选 Capability 下游（租户运行时、能力开关、插件）。
///
/// `FLARE_ADMIN_GATEWAY_GRPC_CAPABILITY_SERVICE_URL` 设为空串时为 `None`，三条 Capability
/// 路由回 503，其余路由不受影响。
pub struct AdminCapabilityDownstream {
    pub config: CapabilityDownstreamConfig,
    pub client: CapabilityAdminClientWrapper,
}

/// 路由层扩展：客户端在锁里只取一份克隆，调用期间不占锁。
#[derive(Clone)]
pub(crate) struct AdminCapability {
    pub(crate) config: Option<CapabilityDownstreamConfig>,
    client: Option<Arc<tokio::sync::Mutex<CapabilityAdminClientWrapper>>>,
}

impl AdminCapability {
    pub(crate) fn new(downstream: Option<AdminCapabilityDownstream>) -> Self {
        match downstream {
            Some(downstream) => Self {
                config: Some(downstream.config),
                client: Some(Arc::new(tokio::sync::Mutex::new(downstream.client))),
            },
            None => Self {
                config: None,
                client: None,
            },
        }
    }

    async fn client(&self) -> Result<CapabilityAdminClientWrapper> {
        match &self.client {
            Some(client) => Ok(client.lock().await.clone()),
            None => Err(HttpApiError(
                ErrorBuilder::new(
                    ErrorCode::HttpServiceUnavailable,
                    "ADMIN_CAPABILITY_DISABLED",
                )
                .details("capability downstream is not configured for admin-gateway")
                .build_error(),
            )),
        }
    }
}

/// 下游拒绝 → 类型化 HTTP 错误；不回显下游文案，也不带请求里的标识。媒资路由同样用它。
///
/// 下游对已由网关验过的 admin 令牌回 UNAUTHENTICATED，只可能是部署问题（例如下游缺
/// `FLARE_CONSOLE_PUBLIC_KEY`；Media 则是 Cell 签名密钥与公钥不配对），因此报 502 而不是让
/// 调用方去重新登录。
pub(super) fn admin_rpc_error(err: FlareError, failure_reason: &'static str) -> HttpApiError {
    let (code, reason) = match err.code() {
        Some(ErrorCode::PermissionDenied) => (ErrorCode::HttpForbidden, "FORBIDDEN"),
        Some(ErrorCode::UserNotFound | ErrorCode::MessageNotFound) => {
            (ErrorCode::HttpNotFound, "NOT_FOUND")
        }
        Some(ErrorCode::InvalidParameter | ErrorCode::BadRequest) => {
            (ErrorCode::HttpBadRequest, "BAD_REQUEST")
        }
        Some(
            ErrorCode::ServiceUnavailable | ErrorCode::NetworkError | ErrorCode::OperationTimeout,
        ) => (
            ErrorCode::HttpServiceUnavailable,
            "ADMIN_UPSTREAM_UNAVAILABLE",
        ),
        Some(ErrorCode::AuthenticationFailed | ErrorCode::AuthenticationRequired) => (
            ErrorCode::HttpBadGateway,
            "ADMIN_UPSTREAM_REJECTED_ASSERTION",
        ),
        _ => {
            tracing::warn!(error = %err, failure_reason, "admin downstream call failed");
            return HttpApiError(
                ErrorBuilder::new(ErrorCode::HttpInternalServerError, failure_reason).build_error(),
            );
        }
    };
    tracing::debug!(error = %err, failure_reason, "admin downstream call rejected");
    HttpApiError(ErrorBuilder::new(code, reason).build_error())
}

fn is_not_found(err: &FlareError) -> bool {
    matches!(
        err.code(),
        Some(ErrorCode::UserNotFound | ErrorCode::MessageNotFound)
    )
}

fn bad_request(code: &'static str, message: String) -> HttpApiError {
    HttpApiError::bad_request(code, message)
}

/// 中间件放进扩展的已验证租户；缺失即未经中间件，按 403 拒绝。
pub(super) fn verified_tenant(principal: &AuthenticatedPrincipal) -> Result<String> {
    principal
        .tenant_id
        .clone()
        .filter(|tenant| !tenant.is_empty() && tenant != "*")
        .ok_or_else(|| {
            HttpApiError(
                ErrorBuilder::new(ErrorCode::HttpForbidden, "ADMIN_IDENTITY_REQUIRED")
                    .build_error(),
            )
        })
}

fn header(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .unwrap_or_default()
        .to_string()
}

/// 写操作审计：谁、哪个租户、哪条入口、原因、请求号/幂等键。中间件已保证原因与请求号存在。
///
/// 幂等键只用于关联日志，网关不保存、不去重：同一键重放会再执行一次。
pub(super) fn audit_write(
    route: &'static str,
    principal: &AuthenticatedPrincipal,
    headers: &HeaderMap,
    target: &str,
) -> String {
    let reason = header(headers, AUDIT_REASON_HEADER);
    tracing::info!(
        target: "admin_audit",
        access = "write",
        route,
        actor_id = %principal.user_id,
        tenant_id = %principal.tenant_id.as_deref().unwrap_or_default(),
        target,
        audit_reason = %reason,
        request_id = %header(headers, keys::REQUEST_ID),
        idempotency_key = %header(headers, IDEMPOTENCY_KEY_HEADER),
        "admin write accepted"
    );
    reason
}

/// 敏感读审计：会话成员、用户设备、在线状态能看出某人在哪些会话、用什么设备、何时在线，
/// 因此与写操作记同一种 `admin_audit` 事件（`access=read`）；其余管理读不记。读不强制审计
/// 原因，调用方带了 `x-audit-reason` 就一并记下。
///
/// Core 不落审计表：写与敏感读的持久审计行都归调用方 Control（见 docs/admin-api.md
/// 「Audit」）。Control 目前只为非 GET 请求落行，接这三条读的 Console 页面需要自己补审计行。
fn audit_sensitive_read(
    route: &'static str,
    principal: &AuthenticatedPrincipal,
    headers: &HeaderMap,
    target: &str,
) {
    tracing::info!(
        target: "admin_audit",
        access = "read",
        route,
        actor_id = %principal.user_id,
        tenant_id = %principal.tenant_id.as_deref().unwrap_or_default(),
        target,
        audit_reason = %header(headers, AUDIT_REASON_HEADER),
        request_id = %header(headers, keys::REQUEST_ID),
        "admin sensitive read accepted"
    );
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/conversations",
    tag = "Admin",
    params(
        ("cursor" = Option<String>, Query, description = "Opaque cursor from the previous page"),
        ("limit" = Option<i32>, Query, description = "Page size, default 50, capped at 200"),
        ("include_deleted" = Option<bool>, Query, description = "Include conversations in the deleted lifecycle state"),
    ),
    responses(
        (status = 200, description = "Conversations of the token tenant, newest update first", body = ApiResponse<AdminConversationPageHttp>),
        (status = 400, description = "Limit or cursor is invalid"),
        (status = 401, description = "Admin token is missing or invalid"),
        (status = 403, description = "Tenant header does not match the token"),
        (status = 503, description = "Conversation service is unavailable"),
    ),
)]
pub async fn list_conversations(
    Extension(clients): Extension<Arc<GrpcClients>>,
    Extension(assertion): Extension<VerifiedAdminAssertion>,
    Query(query): Query<AdminConversationListQuery>,
) -> Result<Json<ApiResponse<AdminConversationPageHttp>>> {
    let limit = page_limit(
        query.limit,
        DEFAULT_CONVERSATION_PAGE,
        MAX_CONVERSATION_PAGE,
    )
    .map_err(|err| bad_request(err.code, err.message))?;
    let mut conversation = clients.conversation_read.lock().await.clone();
    let response = conversation
        .admin_list_conversations(
            assertion.token(),
            AdminListConversationsRequest {
                cursor: query.cursor.unwrap_or_default(),
                limit,
                include_deleted: query.include_deleted.unwrap_or(false),
            },
        )
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_CONVERSATION_LIST_FAILED"))?;
    Ok(Json(ApiResponse::success(conversation_page_http(
        response, limit,
    ))))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/conversations/{conversation_id}",
    tag = "Admin",
    params(("conversation_id" = String, Path, description = "Conversation id")),
    responses(
        (status = 200, description = "Conversation detail without the member list", body = ApiResponse<AdminConversationDetailHttp>),
        (status = 401, description = "Admin token is missing or invalid"),
        (status = 403, description = "Tenant header does not match the token"),
        (status = 404, description = "No such conversation in the token tenant"),
        (status = 503, description = "Conversation service is unavailable"),
    ),
)]
pub async fn get_conversation(
    Extension(clients): Extension<Arc<GrpcClients>>,
    Extension(assertion): Extension<VerifiedAdminAssertion>,
    Path(conversation_id): Path<String>,
) -> Result<Json<ApiResponse<AdminConversationDetailHttp>>> {
    let mut conversation = clients.conversation_read.lock().await.clone();
    let response = conversation
        .admin_get_conversation(
            assertion.token(),
            AdminGetConversationRequest { conversation_id },
        )
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_CONVERSATION_DETAIL_FAILED"))?;
    let detail = response
        .detail
        .ok_or_else(|| HttpApiError::internal("ADMIN_CONVERSATION_DETAIL_FAILED", ""))?;
    Ok(Json(ApiResponse::success(AdminConversationDetailHttp {
        conversation: conversation_http(detail),
    })))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/conversations/{conversation_id}/participants",
    tag = "Admin",
    params(
        ("conversation_id" = String, Path, description = "Conversation id"),
        ("cursor" = Option<String>, Query, description = "Opaque cursor from the previous page"),
        ("limit" = Option<i32>, Query, description = "Page size, default 200, capped at 500"),
        ("include_removed" = Option<bool>, Query, description = "Include members who left or were removed"),
    ),
    responses(
        (status = 200, description = "Members of the conversation, oldest join first", body = ApiResponse<AdminParticipantPageHttp>),
        (status = 400, description = "Limit or cursor is invalid"),
        (status = 401, description = "Admin token is missing or invalid"),
        (status = 403, description = "Tenant header does not match the token"),
        (status = 404, description = "No such conversation in the token tenant"),
        (status = 503, description = "Conversation service is unavailable"),
    ),
)]
pub async fn list_participants(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Extension(assertion): Extension<VerifiedAdminAssertion>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(conversation_id): Path<String>,
    Query(query): Query<AdminParticipantListQuery>,
) -> Result<Json<ApiResponse<AdminParticipantPageHttp>>> {
    let limit = page_limit(query.limit, DEFAULT_PARTICIPANT_PAGE, MAX_PARTICIPANT_PAGE)
        .map_err(|err| bad_request(err.code, err.message))?;
    audit_sensitive_read(
        "conversations.participants",
        &principal,
        &headers,
        &conversation_id,
    );
    let mut conversation = clients.conversation_read.lock().await.clone();
    let response = conversation
        .admin_list_participants(
            assertion.token(),
            AdminListParticipantsRequest {
                conversation_id,
                cursor: query.cursor.unwrap_or_default(),
                limit,
                include_removed: query.include_removed.unwrap_or(false),
            },
        )
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_PARTICIPANT_LIST_FAILED"))?;
    Ok(Json(ApiResponse::success(participant_page_http(
        response, limit,
    ))))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/users/{user_id}/devices",
    tag = "Admin",
    params(("user_id" = String, Path, description = "User id in the token tenant")),
    responses(
        (status = 200, description = "Online devices of the user", body = ApiResponse<AdminUserDevicesHttp>),
        (status = 400, description = "User id is invalid"),
        (status = 401, description = "Admin token is missing or invalid"),
        (status = 403, description = "Tenant header does not match the token"),
        (status = 503, description = "Online service is unavailable"),
    ),
)]
pub async fn list_user_devices(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Extension(assertion): Extension<VerifiedAdminAssertion>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(user_id): Path<String>,
) -> Result<Json<ApiResponse<AdminUserDevicesHttp>>> {
    audit_sensitive_read("users.devices", &principal, &headers, &user_id);
    let mut online = clients.online.lock().await.clone();
    let response = online
        .admin_list_user_devices(
            assertion.token(),
            AdminListUserDevicesRequest {
                user_id: user_id.clone(),
            },
        )
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_USER_DEVICES_FAILED"))?;
    Ok(Json(ApiResponse::success(user_devices_http(
        user_id,
        response.devices,
    ))))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/users/{user_id}/presence",
    tag = "Admin",
    params(("user_id" = String, Path, description = "User id in the token tenant")),
    responses(
        (status = 200, description = "Presence of the user; a user never seen online reads as offline", body = ApiResponse<AdminUserPresenceHttp>),
        (status = 400, description = "User id is invalid"),
        (status = 401, description = "Admin token is missing or invalid"),
        (status = 403, description = "Tenant header does not match the token"),
        (status = 503, description = "Online service is unavailable"),
    ),
)]
pub async fn get_user_presence(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Extension(assertion): Extension<VerifiedAdminAssertion>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(user_id): Path<String>,
) -> Result<Json<ApiResponse<AdminUserPresenceHttp>>> {
    audit_sensitive_read("users.presence", &principal, &headers, &user_id);
    let mut online = clients.online.lock().await.clone();
    let response = online
        .admin_get_user_presence(
            assertion.token(),
            AdminGetUserPresenceRequest {
                user_id: user_id.clone(),
            },
        )
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_USER_PRESENCE_FAILED"))?;
    Ok(Json(ApiResponse::success(user_presence_http(
        user_id,
        response.presence,
    ))))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/users/{user_id}/kick",
    tag = "Admin",
    params(
        ("user_id" = String, Path, description = "User id in the token tenant"),
        ("x-audit-reason" = String, Header, description = "Operator reason, recorded in the audit log and passed to Online"),
        ("idempotency-key" = Option<String>, Header, description = "Required unless x-request-id is present; correlation only, not deduplicated"),
        ("x-request-id" = Option<String>, Header, description = "Required unless idempotency-key is present"),
    ),
    responses(
        (status = 200, description = "Every device the user had online was kicked; devices that went offline in between are listed separately", body = ApiResponse<AdminUserKickHttp>),
        (status = 400, description = "User id is invalid"),
        (status = 401, description = "Admin token is missing or invalid"),
        (status = 403, description = "Tenant header mismatch, or audit reason / idempotency header missing"),
        (status = 503, description = "Online service is unavailable"),
    ),
)]
pub async fn kick_user(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Extension(assertion): Extension<VerifiedAdminAssertion>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(user_id): Path<String>,
) -> Result<Json<ApiResponse<AdminUserKickHttp>>> {
    let reason = audit_write("users.kick", &principal, &headers, &user_id);
    let mut online = clients.online.lock().await.clone();
    let devices = online
        .admin_list_user_devices(
            assertion.token(),
            AdminListUserDevicesRequest {
                user_id: user_id.clone(),
            },
        )
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_USER_KICK_FAILED"))?
        .devices;
    // 逐台踢：任何一台失败即停止并报错。已踢下线的设备不回滚（踢线无法撤销），也不会再
    // 出现在下一次列表里，所以同一请求重试是安全的。
    let mut kicked = Vec::new();
    let mut already_offline = Vec::new();
    for device in devices {
        let outcome = online
            .kick_device(
                assertion.token(),
                KickDeviceRequest {
                    user_id: user_id.clone(),
                    device_id: device.device_id.clone(),
                    reason: reason.clone(),
                },
            )
            .await;
        match outcome {
            Ok(response) if response.success => kicked.push(device.device_id),
            Ok(_) => already_offline.push(device.device_id),
            Err(err) if is_not_found(&err) => already_offline.push(device.device_id),
            Err(err) => return Err(admin_rpc_error(err, "ADMIN_USER_KICK_FAILED")),
        }
    }
    Ok(Json(ApiResponse::success(AdminUserKickHttp {
        user_id,
        kicked,
        already_offline,
    })))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/users/{user_id}/devices/{device_id}/kick",
    tag = "Admin",
    params(
        ("user_id" = String, Path, description = "User id in the token tenant"),
        ("device_id" = String, Path, description = "Device id of that user"),
        ("x-audit-reason" = String, Header, description = "Operator reason, recorded in the audit log and passed to Online"),
        ("idempotency-key" = Option<String>, Header, description = "Required unless x-request-id is present; correlation only, not deduplicated"),
        ("x-request-id" = Option<String>, Header, description = "Required unless idempotency-key is present"),
    ),
    responses(
        (status = 200, description = "Device was kicked", body = ApiResponse<AdminDeviceKickHttp>),
        (status = 400, description = "User or device id is invalid"),
        (status = 401, description = "Admin token is missing or invalid"),
        (status = 403, description = "Tenant header mismatch, audit reason / idempotency header missing, or the device session belongs to another tenant"),
        (status = 404, description = "Device is not online"),
        (status = 503, description = "Online service is unavailable"),
    ),
)]
pub async fn kick_device(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Extension(assertion): Extension<VerifiedAdminAssertion>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path((user_id, device_id)): Path<(String, String)>,
) -> Result<Json<ApiResponse<AdminDeviceKickHttp>>> {
    let target = format!("{user_id}/{device_id}");
    let reason = audit_write("devices.kick", &principal, &headers, &target);
    let mut online = clients.online.lock().await.clone();
    let response = online
        .kick_device(
            assertion.token(),
            KickDeviceRequest {
                user_id: user_id.clone(),
                device_id: device_id.clone(),
                reason,
            },
        )
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_DEVICE_KICK_FAILED"))?;
    Ok(Json(ApiResponse::success(AdminDeviceKickHttp {
        user_id,
        device_id,
        kicked: response.success,
    })))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/tenant/kick-all",
    tag = "Admin",
    params(
        ("x-audit-reason" = String, Header, description = "Operator reason, recorded in the audit log and passed to Online"),
        ("idempotency-key" = Option<String>, Header, description = "Required unless x-request-id is present; correlation only, not deduplicated"),
        ("x-request-id" = Option<String>, Header, description = "Required unless idempotency-key is present"),
    ),
    responses(
        (status = 200, description = "Every connection of the token tenant was kicked; clients receive KICKED with TENANT_UNAVAILABLE", body = ApiResponse<AdminTenantKickHttp>),
        (status = 401, description = "Admin token is missing or invalid"),
        (status = 403, description = "Tenant header mismatch, or audit reason / idempotency header missing"),
        (status = 503, description = "Online service is unavailable"),
    ),
)]
pub async fn kick_tenant(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Extension(assertion): Extension<VerifiedAdminAssertion>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<ApiResponse<AdminTenantKickHttp>>> {
    let tenant_id = verified_tenant(&principal)?;
    let reason = audit_write("tenant.kick_all", &principal, &headers, &tenant_id);
    let mut online = clients.online.lock().await.clone();
    let response = online
        .kick_tenant(
            assertion.token(),
            KickTenantRequest {
                tenant_id: tenant_id.clone(),
                reason,
            },
        )
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_TENANT_KICK_FAILED"))?;
    Ok(Json(ApiResponse::success(AdminTenantKickHttp {
        tenant_id,
        kicked: response.kicked,
    })))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/tenant/runtime",
    tag = "Admin",
    responses(
        (status = 200, description = "Tenant projection held by Capability (status, quota, settings, version)", body = ApiResponse<AdminTenantRuntimeHttp>),
        (status = 401, description = "Admin token is missing or invalid"),
        (status = 403, description = "Tenant header does not match the token"),
        (status = 503, description = "Capability downstream is disabled or unavailable"),
    ),
)]
pub async fn get_tenant_runtime(
    Extension(capability): Extension<AdminCapability>,
    Extension(assertion): Extension<VerifiedAdminAssertion>,
) -> Result<Json<ApiResponse<AdminTenantRuntimeHttp>>> {
    let mut client = capability.client().await?;
    let response = client
        .get_tenant_runtime(assertion.token())
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_TENANT_RUNTIME_FAILED"))?;
    Ok(Json(ApiResponse::success(tenant_runtime_http(response))))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/capabilities/tenant",
    tag = "Admin",
    responses(
        (status = 200, description = "Capability switches set for the token tenant", body = ApiResponse<AdminTenantCapabilitiesHttp>),
        (status = 401, description = "Admin token is missing or invalid"),
        (status = 403, description = "Tenant header does not match the token"),
        (status = 503, description = "Capability downstream is disabled or unavailable"),
    ),
)]
pub async fn list_tenant_capabilities(
    Extension(capability): Extension<AdminCapability>,
    Extension(assertion): Extension<VerifiedAdminAssertion>,
) -> Result<Json<ApiResponse<AdminTenantCapabilitiesHttp>>> {
    let mut client = capability.client().await?;
    let response = client
        .list_tenant_capability_switches(assertion.token())
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_TENANT_CAPABILITIES_FAILED"))?;
    Ok(Json(ApiResponse::success(tenant_capabilities_http(
        response,
    ))))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/plugins",
    tag = "Admin",
    params(("capability_id" = Option<String>, Query, description = "Only instances serving this capability")),
    responses(
        (status = 200, description = "Extension plugin instances registered for the token tenant", body = ApiResponse<AdminPluginsHttp>),
        (status = 401, description = "Admin token is missing or invalid"),
        (status = 403, description = "Tenant header does not match the token"),
        (status = 503, description = "Capability downstream is disabled or unavailable"),
    ),
)]
pub async fn list_plugins(
    Extension(capability): Extension<AdminCapability>,
    Extension(assertion): Extension<VerifiedAdminAssertion>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(query): Query<AdminPluginListQuery>,
) -> Result<Json<ApiResponse<AdminPluginsHttp>>> {
    let tenant_id = verified_tenant(&principal)?;
    let mut client = capability.client().await?;
    let response = client
        .list_registered_plugins(
            assertion.token(),
            &tenant_id,
            query.capability_id.as_deref().unwrap_or_default(),
        )
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_PLUGIN_LIST_FAILED"))?;
    Ok(Json(ApiResponse::success(plugins_http(
        tenant_id, response,
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;

    fn status_of(err: HttpApiError) -> u16 {
        err.into_response().status().as_u16()
    }

    /// 经 gRPC 往返后的真实形状：下游的 Status 由客户端还原成 FlareError。
    fn through_grpc(code: tonic::Code) -> FlareError {
        FlareError::from(tonic::Status::new(code, "downstream detail with id-123"))
    }

    #[test]
    fn downstream_rejections_map_to_typed_statuses_without_echoing_detail() {
        for (code, status) in [
            (tonic::Code::PermissionDenied, 403),
            (tonic::Code::NotFound, 404),
            (tonic::Code::InvalidArgument, 400),
            (tonic::Code::Unavailable, 503),
            (tonic::Code::DeadlineExceeded, 503),
            (tonic::Code::Unauthenticated, 502),
            (tonic::Code::Internal, 500),
            (tonic::Code::FailedPrecondition, 500),
        ] {
            let err = admin_rpc_error(through_grpc(code), "ADMIN_TEST_FAILED");
            assert!(!err.to_string().contains("id-123"), "{code:?}: {err}");
            assert_eq!(status_of(err), status, "{code:?}");
        }
    }

    #[test]
    fn not_found_is_recognised_after_the_grpc_round_trip() {
        assert!(is_not_found(&through_grpc(tonic::Code::NotFound)));
        assert!(!is_not_found(&through_grpc(tonic::Code::PermissionDenied)));
    }
}
