//! 设备推送令牌：`PUT` / `DELETE /api/v1/devices/{device_id}/push-token`。
//!
//! 用户与租户只取自已验证的 IM 令牌主体；网关按该身份现签一枚 User 内部上下文，
//! 调用 Push Proxy 的 `RegisterDevicePushToken` / `UnregisterDevicePushToken`。
//! 请求体、查询串和请求头都不能指定 `user_id` / `tenant_id`：体与查询串拒收未知字段，
//! 身份头在鉴权中间件里已被令牌主体覆盖，Push Proxy 侧的签名层也会丢弃它们。

use std::time::Duration;

use axum::{
    Json,
    extract::{
        Extension, Path, Query,
        rejection::{JsonRejection, QueryRejection},
    },
    http::HeaderMap,
};
use flare_grpc_proto::push::push_service_client::PushServiceClient;
use flare_grpc_proto::push::{
    DevicePushProvider, RegisterDevicePushTokenRequest, UnregisterDevicePushTokenRequest,
};
use flare_server_core::{
    AuthenticatedPrincipal, SignedActorType, SignedContextIdentity,
    context::Ctx,
    error::{ErrorBuilder, ErrorCode},
    http::{ApiResponse, ContextFromHeaders, HttpApiError as GatewayError, Result},
};
use serde::{Deserialize, Serialize};
use tonic::Code;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, instrument, warn};

use super::MessageContextSigner;

/// single 拓扑（supervise-core.toml）下 Push Proxy 与网关同容器的监听地址。
const DEFAULT_PUSH_PROXY_URL: &str = "http://127.0.0.1:50091";

const MAX_DEVICE_ID_LEN: usize = 128;
const MAX_PLATFORM_LEN: usize = 32;
const MAX_PUSH_TOKEN_LEN: usize = 4096;

/// Push Proxy 设备令牌 RPC 的客户端。通道惰性建立：Push Proxy 暂不可达时网关照常启动，
/// 该接口按请求返回 503。
#[derive(Clone)]
pub(crate) struct PushTokenClient(PushServiceClient<Channel>);

/// Push Proxy 的静态 gRPC 地址：`FLARE_API_GATEWAY_GRPC_PUSH_PROXY_URL`，
/// 未配置或为空时取 single 拓扑地址；Push Proxy 不在同一容器的拓扑必须显式配置。
pub(crate) fn push_proxy_url_from_env() -> String {
    resolve_push_proxy_url(std::env::var("FLARE_API_GATEWAY_GRPC_PUSH_PROXY_URL").ok())
}

fn resolve_push_proxy_url(configured: Option<String>) -> String {
    configured
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_PUSH_PROXY_URL.to_string())
}

impl PushTokenClient {
    pub(crate) fn connect_lazy(
        url: &str,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> std::result::Result<Self, String> {
        let url = url.trim();
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(
                "FLARE_API_GATEWAY_GRPC_PUSH_PROXY_URL must be an http:// or https:// gRPC address"
                    .to_string(),
            );
        }
        let endpoint = Endpoint::from_shared(url.to_string())
            .map_err(|_| "FLARE_API_GATEWAY_GRPC_PUSH_PROXY_URL is not a valid URI".to_string())?
            .connect_timeout(connect_timeout)
            .timeout(request_timeout);
        Ok(Self::from_channel(endpoint.connect_lazy()))
    }

    pub(crate) fn from_channel(channel: Channel) -> Self {
        Self(PushServiceClient::new(channel))
    }
}

/// 注册/更新当前用户某台设备的推送令牌。
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RegisterPushTokenHttpRequest {
    /// 设备平台，例如 `ios`、`android`、`harmony`
    pub platform: String,
    /// 推送通道，当前只支持 `getui`
    pub provider: String,
    /// 推送通道为该设备下发的令牌
    pub token: String,
}

/// 注销推送令牌的查询参数。
#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct UnregisterPushTokenQuery {
    /// 推送通道，当前只支持 `getui`
    pub provider: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct PushTokenHttpResponse {
    pub success: bool,
}

/// 已验证的调用方：用户、规范化租户与现签的内部上下文令牌。
struct PushTokenCaller {
    ctx: Ctx,
    user_id: String,
    signed_context: String,
}

impl PushTokenCaller {
    fn request<T>(&self, payload: T) -> Result<tonic::Request<T>> {
        let mut request = flare_server_core::request_with_context(payload, &self.ctx);
        flare_server_core::set_signed_context_token(&mut request, &self.signed_context).map_err(
            |_| GatewayError::internal("PUSH_SIGNING_FAILED", "internal identity signing failed"),
        )?;
        Ok(request)
    }
}

/// 从令牌主体构造调用方；任一绑定缺失都在发出 RPC 之前拒绝。
fn push_token_caller(
    headers: &HeaderMap,
    principal: &AuthenticatedPrincipal,
    signer: &MessageContextSigner,
    device_id: &str,
) -> Result<PushTokenCaller> {
    let signer = signer.0.as_ref().ok_or_else(|| {
        GatewayError::internal(
            "PUSH_SIGNER_UNAVAILABLE",
            "internal identity signer not configured",
        )
    })?;
    // 先要求令牌自带非空租户，再做 default→0 规范化：空租户不能被规范化成租户 0。
    let tenant_id = principal
        .tenant_id
        .as_deref()
        .map(str::trim)
        .filter(|tenant| !tenant.is_empty())
        .map(flare_im_contracts::utils::normalize_tenant_id)
        .filter(|tenant| flare_im_contracts::domain::tenant::is_valid_tenant_id(tenant))
        .ok_or_else(|| forbidden("TENANT_REQUIRED", "a tenant-bound user token is required"))?;
    let user_id = principal.user_id.trim();
    if user_id.is_empty() || principal.has_gateway_admin_scope() {
        return Err(forbidden(
            "USER_REQUIRED",
            "an end-user token is required to manage device push tokens",
        ));
    }
    // 绑定了设备的令牌只能管理本设备的推送令牌。
    if let Some(bound) = principal
        .device_id
        .as_deref()
        .map(str::trim)
        .filter(|bound| !bound.is_empty())
        && bound != device_id
    {
        return Err(forbidden(
            "DEVICE_MISMATCH",
            "the token is bound to a different device",
        ));
    }

    let ctx = Ctx::from_headers(headers);
    let request_id = Some(ctx.request_id())
        .filter(|id| {
            !id.is_empty()
                && id.len() <= 128
                && id.trim() == *id
                && !id.chars().any(char::is_control)
        })
        .map(str::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let signed_context = signer
        .sign(
            &SignedContextIdentity::new(SignedActorType::User, user_id, tenant_id, request_id)
                .with_credential_id("api-gateway"),
        )
        .map_err(|_| {
            GatewayError::internal("PUSH_SIGNING_FAILED", "internal identity signing failed")
        })?;
    Ok(PushTokenCaller {
        ctx,
        user_id: user_id.to_string(),
        signed_context,
    })
}

fn forbidden(reason: &str, message: &str) -> GatewayError {
    GatewayError(
        ErrorBuilder::new(ErrorCode::HttpForbidden, reason)
            .details(message)
            .build_error(),
    )
}

fn bounded_text(value: &str, max_len: usize, allow: impl Fn(char) -> bool) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty() && value.len() <= max_len && value.chars().all(allow)).then_some(value)
}

fn device_id(raw: &str) -> Result<String> {
    bounded_text(raw, MAX_DEVICE_ID_LEN, |ch| !ch.is_control())
        .map(str::to_owned)
        .ok_or_else(|| GatewayError::bad_request("DEVICE_ID_INVALID", "device_id is invalid"))
}

fn platform(raw: &str) -> Result<String> {
    bounded_text(raw, MAX_PLATFORM_LEN, |ch| {
        ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'
    })
    .map(str::to_owned)
    .ok_or_else(|| GatewayError::bad_request("PLATFORM_INVALID", "platform is invalid"))
}

fn push_token(raw: &str) -> Result<String> {
    bounded_text(raw, MAX_PUSH_TOKEN_LEN, |ch| {
        !ch.is_control() && !ch.is_whitespace()
    })
    .map(str::to_owned)
    .ok_or_else(|| GatewayError::bad_request("PUSH_TOKEN_INVALID", "token is invalid"))
}

fn provider(raw: &str) -> Result<DevicePushProvider> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "getui" => Ok(DevicePushProvider::Getui),
        _ => Err(GatewayError::bad_request(
            "PUSH_PROVIDER_INVALID",
            "provider must be one of: getui",
        )),
    }
}

/// Push Proxy 的 gRPC 状态到 HTTP 信封。只有参数与权限类错误把下游说明带给调用方；
/// 其余只记日志。下游 `Unauthenticated` 表示网关自己的内部签名被拒（密钥不一致），
/// 不是调用方的 IM 令牌失效，因此映射为 502 而非 401，避免客户端据此反复刷新令牌。
fn push_proxy_error(status: tonic::Status) -> GatewayError {
    let (code, reason, expose) = match status.code() {
        Code::InvalidArgument => (ErrorCode::HttpBadRequest, "INVALID_ARGUMENT", true),
        Code::PermissionDenied => (ErrorCode::HttpForbidden, "PERMISSION_DENIED", true),
        Code::NotFound => (ErrorCode::HttpNotFound, "NOT_FOUND", true),
        Code::ResourceExhausted => (ErrorCode::HttpTooManyRequests, "RESOURCE_EXHAUSTED", false),
        Code::Unavailable => (
            ErrorCode::HttpServiceUnavailable,
            "UPSTREAM_UNAVAILABLE",
            false,
        ),
        Code::DeadlineExceeded => (ErrorCode::HttpGatewayTimeout, "UPSTREAM_TIMEOUT", false),
        _ => (ErrorCode::HttpBadGateway, "GRPC_ERROR", false),
    };
    warn!(
        grpc_code = ?status.code(),
        grpc_message = %status.message(),
        "push proxy rejected a device push token request"
    );
    let details = if expose { status.message() } else { "" };
    GatewayError(
        ErrorBuilder::new(code, reason)
            .details(details)
            .build_error(),
    )
}

/// 注册或更新当前用户某台设备的推送令牌
#[utoipa::path(
    put,
    path = "/api/v1/devices/{device_id}/push-token",
    tag = "Device",
    params(("device_id" = String, Path, description = "设备 ID；设备绑定的令牌只能是本设备")),
    request_body = RegisterPushTokenHttpRequest,
    responses(
        (status = 200, description = "成功", body = ApiResponse<PushTokenHttpResponse>),
        (status = 400, description = "参数错误或包含未知字段"),
        (status = 401, description = "缺少或无效的 IM 令牌"),
        (status = 403, description = "令牌未绑定租户、是管理员令牌或绑定了其他设备"),
        (status = 503, description = "Push Proxy 不可用"),
    )
)]
#[instrument(skip_all)]
pub async fn register_push_token(
    headers: HeaderMap,
    Extension(client): Extension<PushTokenClient>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(signer): Extension<MessageContextSigner>,
    Path(raw_device_id): Path<String>,
    body: std::result::Result<Json<RegisterPushTokenHttpRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<PushTokenHttpResponse>>> {
    let device_id = device_id(&raw_device_id)?;
    let caller = push_token_caller(&headers, &principal, &signer, &device_id)?;
    let Json(body) = body.map_err(|rejection| {
        GatewayError::bad_request("INVALID_REQUEST_BODY", rejection.body_text())
    })?;
    let request = RegisterDevicePushTokenRequest {
        user_id: caller.user_id.clone(),
        device_id,
        platform: platform(&body.platform)?,
        provider: provider(&body.provider)? as i32,
        token: push_token(&body.token)?,
    };
    debug!(
        trace_id = %caller.ctx.trace_id(),
        device_id = %request.device_id,
        platform = %request.platform,
        "RegisterDevicePushToken"
    );
    let mut client = client.0;
    let response = client
        .register_device_push_token(caller.request(request)?)
        .await
        .map_err(push_proxy_error)?
        .into_inner();
    Ok(Json(ApiResponse::success(PushTokenHttpResponse {
        success: response.success,
    })))
}

/// 注销当前用户某台设备在指定通道的推送令牌
#[utoipa::path(
    delete,
    path = "/api/v1/devices/{device_id}/push-token",
    tag = "Device",
    params(
        ("device_id" = String, Path, description = "设备 ID；设备绑定的令牌只能是本设备"),
        UnregisterPushTokenQuery,
    ),
    responses(
        (status = 200, description = "成功", body = ApiResponse<PushTokenHttpResponse>),
        (status = 400, description = "参数错误或包含未知参数"),
        (status = 401, description = "缺少或无效的 IM 令牌"),
        (status = 403, description = "令牌未绑定租户、是管理员令牌或绑定了其他设备"),
        (status = 503, description = "Push Proxy 不可用"),
    )
)]
#[instrument(skip_all)]
pub async fn unregister_push_token(
    headers: HeaderMap,
    Extension(client): Extension<PushTokenClient>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(signer): Extension<MessageContextSigner>,
    Path(raw_device_id): Path<String>,
    query: std::result::Result<Query<UnregisterPushTokenQuery>, QueryRejection>,
) -> Result<Json<ApiResponse<PushTokenHttpResponse>>> {
    let device_id = device_id(&raw_device_id)?;
    let caller = push_token_caller(&headers, &principal, &signer, &device_id)?;
    let Query(query) = query
        .map_err(|rejection| GatewayError::bad_request("INVALID_QUERY", rejection.body_text()))?;
    let request = UnregisterDevicePushTokenRequest {
        user_id: caller.user_id.clone(),
        device_id,
        provider: provider(&query.provider)? as i32,
    };
    debug!(
        trace_id = %caller.ctx.trace_id(),
        device_id = %request.device_id,
        "UnregisterDevicePushToken"
    );
    let mut client = client.0;
    let response = client
        .unregister_device_push_token(caller.request(request)?)
        .await
        .map_err(push_proxy_error)?
        .into_inner();
    Ok(Json(ApiResponse::success(PushTokenHttpResponse {
        success: response.success,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_accepts_only_known_channels() {
        assert_eq!(provider(" GeTui ").unwrap(), DevicePushProvider::Getui);
        for raw in ["", "apns", "fcm", "unspecified", "1"] {
            assert!(provider(raw).is_err(), "{raw}");
        }
    }

    #[test]
    fn field_validation_bounds_length_and_charset() {
        assert_eq!(platform(" ios ").unwrap(), "ios");
        assert!(platform("i os").is_err());
        assert!(platform(&"a".repeat(MAX_PLATFORM_LEN + 1)).is_err());
        assert_eq!(push_token(" abc:DEF-1_2 ").unwrap(), "abc:DEF-1_2");
        assert!(push_token("ab c").is_err());
        assert!(push_token(&"a".repeat(MAX_PUSH_TOKEN_LEN + 1)).is_err());
        assert!(device_id("  ").is_err());
        assert!(device_id("d\n1").is_err());
        assert!(device_id(&"d".repeat(MAX_DEVICE_ID_LEN + 1)).is_err());
    }

    #[test]
    fn downstream_identity_rejection_is_not_reported_as_caller_unauthenticated() {
        let status =
            |error: GatewayError| axum::response::IntoResponse::into_response(error).status();
        assert_eq!(
            status(push_proxy_error(tonic::Status::unauthenticated("bad sig"))),
            axum::http::StatusCode::BAD_GATEWAY
        );
        assert_eq!(
            status(push_proxy_error(tonic::Status::permission_denied("x"))),
            axum::http::StatusCode::FORBIDDEN
        );
        assert_eq!(
            status(push_proxy_error(tonic::Status::unavailable("x"))),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn unset_push_proxy_address_defaults_to_the_single_topology_listener() {
        assert_eq!(resolve_push_proxy_url(None), "http://127.0.0.1:50091");
        assert_eq!(
            resolve_push_proxy_url(Some("  ".into())),
            "http://127.0.0.1:50091"
        );
        assert_eq!(
            resolve_push_proxy_url(Some(" http://push-proxy:50091 ".into())),
            "http://push-proxy:50091"
        );
    }

    #[test]
    fn invalid_static_address_is_a_startup_error() {
        let timeout = Duration::from_secs(1);
        assert!(PushTokenClient::connect_lazy("127.0.0.1:50091", timeout, timeout).is_err());
        assert!(PushTokenClient::connect_lazy("", timeout, timeout).is_err());
    }
}
