//! Conditional push binding and an independently authenticated revoke-only capability.
//!
//! 用户与租户只取自已验证的 IM 令牌主体；网关按该身份现签一枚 User 内部上下文，
//! User binding/inspection and Service capability revocation are separate boundaries.
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
    DevicePushProvider, InspectDevicePushBindingRequest, RegisterDevicePushTokenRequest,
    RevokeDevicePushBindingRequest,
};
use flare_im_service_kit::discovery::{
    StaticChannelPolicy, invalidate_static_channel_on_transport_failure, resolve_grpc_channel,
    static_grpc_endpoint,
};
use flare_server_core::{
    AuthenticatedPrincipal, SignedActorType, SignedContextIdentity,
    context::Ctx,
    error::{ErrorBuilder, ErrorCode},
    http::{ApiResponse, ContextFromHeaders, HttpApiError as GatewayError, Result},
};
use serde::{Deserialize, Serialize};
use tonic::Code;
use tonic::transport::Channel;
use tracing::{debug, instrument, warn};

use super::MessageContextSigner;

/// single 拓扑（supervise-core.toml）下 Push Proxy 与网关同容器的监听地址。
const DEFAULT_PUSH_PROXY_URL: &str = "http://127.0.0.1:50091";

const MAX_DEVICE_ID_LEN: usize = 128;
const MAX_PLATFORM_LEN: usize = 32;
const MAX_PUSH_TOKEN_LEN: usize = 4096;

/// Push Proxy 设备令牌 RPC 的客户端。通道惰性建立：Push Proxy 暂不可达时网关照常启动，
/// 该接口按请求返回 503。
///
/// 静态地址的通道取自 service-kit 的静态通道缓存（建连上限 2 s 覆盖名字解析与握手、HTTP/2
/// 探活，见 [`StaticChannelPolicy`]），调用遇到传输层失败时丢弃，下一次请求重新解析、新建连接。
/// 以前的懒连接通道有建连上限、没有探活，对端消失却没有断开连接时要等 TCP 自己超时。
#[derive(Clone)]
pub(crate) struct PushTokenClient {
    target: PushTokenTarget,
    request_timeout: Option<Duration>,
}

#[derive(Clone)]
enum PushTokenTarget {
    /// `http(s)://` 静态地址（service-kit 静态通道缓存的键）。
    Static(String),
    /// 调用方给定的通道（测试注入）。
    #[cfg(test)]
    Channel(Channel),
}

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
        request_timeout: Duration,
    ) -> std::result::Result<Self, String> {
        let url = url.trim();
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(
                "FLARE_API_GATEWAY_GRPC_PUSH_PROXY_URL must be an http:// or https:// gRPC address"
                    .to_string(),
            );
        }
        static_grpc_endpoint(url, StaticChannelPolicy::DEFAULT)
            .map_err(|_| "FLARE_API_GATEWAY_GRPC_PUSH_PROXY_URL is not a valid URI".to_string())?;
        Ok(Self {
            target: PushTokenTarget::Static(url.to_string()),
            request_timeout: Some(request_timeout),
        })
    }

    #[cfg(test)]
    pub(crate) fn from_channel(channel: Channel) -> Self {
        Self {
            target: PushTokenTarget::Channel(channel),
            request_timeout: None,
        }
    }

    async fn client(&self) -> Result<PushServiceClient<Channel>> {
        match &self.target {
            PushTokenTarget::Static(url) => resolve_grpc_channel(url)
                .await
                .map(PushServiceClient::new)
                .map_err(|_| {
                    push_proxy_error(tonic::Status::unavailable("push proxy channel unavailable"))
                }),
            #[cfg(test)]
            PushTokenTarget::Channel(channel) => Ok(PushServiceClient::new(channel.clone())),
        }
    }

    /// 一次 Push Proxy 调用：带请求超时；传输层失败（连不上、连接被断、ping 超时）时丢弃
    /// 静态地址的通道，对端回的状态不动缓存。
    async fn call<T, F, Fut>(&self, rpc: F) -> std::result::Result<T, tonic::Status>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, tonic::Status>>,
    {
        let result = match self.request_timeout {
            Some(timeout) => tokio::time::timeout(timeout, rpc())
                .await
                .unwrap_or_else(|_| {
                    Err(tonic::Status::deadline_exceeded(
                        "push proxy request timed out",
                    ))
                }),
            None => rpc().await,
        };
        if let (Err(status), PushTokenTarget::Static(url)) = (&result, &self.target)
            && invalidate_static_channel_on_transport_failure(url, status)
        {
            debug!(code = ?status.code(), "push proxy channel dropped after transport failure");
        }
        result
    }
}

/// 注册/更新当前用户某台设备的推送令牌。
#[derive(Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RegisterPushTokenHttpRequest {
    /// 设备平台，例如 `ios`、`android`、`harmony`
    pub platform: String,
    /// 推送通道：`getui`、`fcm` 或 `apns`。
    pub provider: String,
    /// 推送通道为该设备下发的令牌
    pub token: String,
    pub binding_id: String,
    pub expected_version: String,
    pub expires_at_ms: i64,
}

/// 注销推送令牌的查询参数。
#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct InspectPushTokenQuery {
    /// 推送通道：`getui`、`fcm` 或 `apns`。
    pub provider: String,
    #[serde(default, rename = "bindingId")]
    pub binding_id: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PushTokenHttpResponse {
    pub success: bool,
    pub binding_id: String,
}

#[derive(Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RevokePushTokenRequest {
    pub revocation_token: String,
}
#[derive(Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PushBindingView {
    pub version: String,
    pub current_binding_id: String,
    pub state: String,
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
    if principal
        .device_id
        .as_deref()
        .map(str::trim)
        .filter(|bound| !bound.is_empty())
        != Some(device_id)
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
        "fcm" => Ok(DevicePushProvider::Fcm),
        "apns" => Ok(DevicePushProvider::Apns),
        _ => Err(GatewayError::bad_request(
            "PUSH_PROVIDER_INVALID",
            "provider must be one of: getui, fcm, apns",
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
        Code::FailedPrecondition | Code::AlreadyExists => {
            (ErrorCode::HttpConflict, "PUSH_BINDING_CONFLICT", false)
        }
        _ => (ErrorCode::HttpBadGateway, "GRPC_ERROR", false),
    };
    warn!(
        grpc_code = ?status.code(),
        "push proxy rejected a device push token request"
    );
    let details = if expose {
        "push registration rejected"
    } else {
        ""
    };
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
        let _ = rejection;
        GatewayError::bad_request("INVALID_REQUEST_BODY", "push binding body rejected")
    })?;
    let request = RegisterDevicePushTokenRequest {
        user_id: caller.user_id.clone(),
        device_id,
        platform: platform(&body.platform)?,
        provider: provider(&body.provider)? as i32,
        token: push_token(&body.token)?,
        binding_id: body.binding_id,
        expected_version: body.expected_version,
        expires_at_ms: body.expires_at_ms,
    };
    debug!(
        trace_id = %caller.ctx.trace_id(),
        device_id = %request.device_id,
        platform = %request.platform,
        "RegisterDevicePushToken"
    );
    let grpc_request = caller.request(request)?;
    let mut push_proxy = client.client().await?;
    let response = client
        .call(|| push_proxy.register_device_push_token(grpc_request))
        .await
        .map_err(push_proxy_error)?
        .into_inner();
    Ok(Json(ApiResponse::success(PushTokenHttpResponse {
        success: response.success,
        binding_id: response.binding_id,
    })))
}

/// Read back the exact binding after an unknown write; never returns an OS token.
#[utoipa::path(
    get,
    path = "/api/v1/devices/{device_id}/push-token",
    tag = "Device",
    params(
        ("device_id" = String, Path, description = "设备 ID；设备绑定的令牌只能是本设备"),
        InspectPushTokenQuery,
    ),
    responses(
        (status = 200, description = "成功", body = ApiResponse<PushBindingView>),
        (status = 400, description = "参数错误或包含未知参数"),
        (status = 401, description = "缺少或无效的 IM 令牌"),
        (status = 403, description = "令牌未绑定租户、是管理员令牌或绑定了其他设备"),
        (status = 503, description = "Push Proxy 不可用"),
    )
)]
#[instrument(skip_all)]
pub async fn inspect_push_token(
    headers: HeaderMap,
    Extension(client): Extension<PushTokenClient>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(signer): Extension<MessageContextSigner>,
    Path(raw_device_id): Path<String>,
    query: std::result::Result<Query<InspectPushTokenQuery>, QueryRejection>,
) -> Result<Json<ApiResponse<PushBindingView>>> {
    let device_id = device_id(&raw_device_id)?;
    let caller = push_token_caller(&headers, &principal, &signer, &device_id)?;
    let Query(query) = query
        .map_err(|_| GatewayError::bad_request("INVALID_QUERY", "push binding query rejected"))?;
    let request = InspectDevicePushBindingRequest {
        user_id: caller.user_id.clone(),
        device_id,
        provider: provider(&query.provider)? as i32,
        binding_id: query.binding_id,
    };
    debug!(
        trace_id = %caller.ctx.trace_id(),
        device_id = %request.device_id,
        "InspectDevicePushBinding"
    );
    let grpc_request = caller.request(request)?;
    let mut push_proxy = client.client().await?;
    let response = client
        .call(|| push_proxy.inspect_device_push_binding(grpc_request))
        .await
        .map_err(push_proxy_error)?
        .into_inner();
    Ok(Json(ApiResponse::success(PushBindingView {
        version: response.version,
        current_binding_id: response.current_binding_id,
        state: response.state,
    })))
}

/// No bearer required: possession of the random revoke-only secret can ONLY
/// retire that exact binding. It cannot inspect/register/send or grant authority.
#[utoipa::path(post,path="/api/v1/push/bindings/revoke",tag="Device",request_body=RevokePushTokenRequest,responses((status=200,body=ApiResponse<serde_json::Value>)))]
#[instrument(skip_all)]
pub async fn revoke_push_token(
    Extension(client): Extension<PushTokenClient>,
    Extension(signer): Extension<MessageContextSigner>,
    body: std::result::Result<Json<RevokePushTokenRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<serde_json::Value>>> {
    let Json(body) = body.map_err(|_| {
        GatewayError::bad_request("INVALID_REQUEST_BODY", "push revocation body rejected")
    })?;
    if !flare_im_contracts::domain::push_binding::binding_id_valid(&body.revocation_token) {
        return Err(GatewayError::bad_request(
            "INVALID_CAPABILITY",
            "push revocation capability rejected",
        ));
    }
    let signer = signer.0.as_ref().ok_or_else(|| {
        GatewayError::internal(
            "PUSH_SIGNER_UNAVAILABLE",
            "internal identity signer not configured",
        )
    })?;
    let signed = signer
        .sign(
            &SignedContextIdentity::new(
                SignedActorType::Service,
                "push-binding-revoker",
                "0",
                uuid::Uuid::new_v4().to_string(),
            )
            .with_credential_id("api-gateway"),
        )
        .map_err(|_| {
            GatewayError::internal("PUSH_SIGNING_FAILED", "internal identity signing failed")
        })?;
    let caller = PushTokenCaller {
        ctx: Ctx::default(),
        user_id: String::new(),
        signed_context: signed,
    };
    let req = caller.request(RevokeDevicePushBindingRequest {
        revocation_token: body.revocation_token,
    })?;
    let mut proxy = client.client().await?;
    let response = client
        .call(|| proxy.revoke_device_push_binding(req))
        .await
        .map_err(push_proxy_error)?
        .into_inner();
    Ok(Json(ApiResponse::success(
        serde_json::json!({"success":response.success}),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_accepts_only_known_channels() {
        assert_eq!(provider(" GeTui ").unwrap(), DevicePushProvider::Getui);
        assert_eq!(provider(" FCM ").unwrap(), DevicePushProvider::Fcm);
        assert_eq!(provider("apns").unwrap(), DevicePushProvider::Apns);
        for raw in ["", "firebase", "apple", "unspecified", "1"] {
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

    /// 网关拨的 Push Proxy 地址必须落在它的真实监听端口上：single 拓扑靠代码缺省（同容器），
    /// 多容器 stack compose 里网关与 Push Proxy 分属两个容器，必须显式指向 push-proxy 服务，
    /// 否则缺省的 127.0.0.1 指回网关自己，接口全部 503。
    #[test]
    fn deployment_topologies_point_the_gateway_at_the_push_proxy_listener() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let read = |path: &str| {
            std::fs::read_to_string(root.join(path))
                .unwrap_or_else(|error| panic!("read {path}: {error}"))
        };
        let mut section = "";
        let port = read("config/services/push-proxy.toml")
            .lines()
            .map(str::trim)
            .find_map(|line| {
                if line.starts_with('[') {
                    section = if line == "[services.push_proxy.server]" {
                        "server"
                    } else {
                        ""
                    };
                    return None;
                }
                let value = line.strip_prefix("port")?.trim().strip_prefix('=')?;
                (section == "server").then(|| value.trim().parse::<u16>().ok())?
            })
            .expect("push-proxy.toml declares [services.push_proxy.server] port");

        // single：supervise-core.toml 的 push-proxy 健康探针地址就是网关的缺省拨号地址。
        assert_eq!(DEFAULT_PUSH_PROXY_URL, format!("http://127.0.0.1:{port}"));
        let supervise = read("release/supervise-core.toml");
        let proxy_block = supervise
            .split("[[services]]")
            .find(|block| block.contains("name = \"push-proxy\""))
            .expect("supervise-core.toml runs push-proxy");
        assert!(
            proxy_block.contains(&format!("tcp_health = \"127.0.0.1:{port}\"")),
            "{proxy_block}"
        );

        // stack compose：api-gateway 服务块显式指向 push-proxy 服务。
        let stack = read("deploy/docker-compose.stack.yml");
        let service_block = |name: &str| {
            let header = format!("  {name}:");
            let mut lines = stack.lines().skip_while(|line| *line != header);
            lines.next()?;
            Some(
                lines
                    .take_while(|line| line.is_empty() || line.starts_with("    "))
                    .map(str::trim)
                    .collect::<Vec<_>>(),
            )
        };
        assert!(
            service_block("push-proxy").is_some(),
            "stack compose runs push-proxy in its own container"
        );
        let gateway = service_block("api-gateway").expect("stack compose runs api-gateway");
        let expected = format!("FLARE_API_GATEWAY_GRPC_PUSH_PROXY_URL: http://push-proxy:{port}");
        assert!(
            gateway.contains(&expected.as_str()),
            "api-gateway in deploy/docker-compose.stack.yml must set `{expected}`"
        );
    }

    #[test]
    fn invalid_static_address_is_a_startup_error() {
        let timeout = Duration::from_secs(1);
        assert!(PushTokenClient::connect_lazy("127.0.0.1:50091", timeout).is_err());
        assert!(PushTokenClient::connect_lazy("", timeout).is_err());
        assert!(PushTokenClient::connect_lazy("http://bad host:1", timeout).is_err());
    }

    /// 静态地址走 service-kit 的静态通道（建连上限、HTTP/2 探活），传输层失败后丢弃该地址的
    /// 通道；请求超时按 504 报。以前的懒连接通道有建连上限、没有探活，且永久复用。
    #[tokio::test]
    async fn static_push_proxy_uses_the_static_channel_and_drops_it_after_transport_failure() {
        use flare_im_service_kit::discovery::is_static_channel_cached;

        // 一个刚关掉的回环端口：连上会被拒绝。
        let closed = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        let url = format!("http://{closed}");
        let client = PushTokenClient::connect_lazy(&url, Duration::from_secs(2)).unwrap();
        let mut push_proxy = client.client().await.expect("lazy static channel");
        assert!(
            is_static_channel_cached(&url),
            "push proxy must use the service-kit static channel"
        );
        let request = tonic::Request::new(InspectDevicePushBindingRequest::default());
        let status = client
            .call(|| push_proxy.inspect_device_push_binding(request))
            .await
            .expect_err("nothing listens on a closed port");
        assert!(
            !is_static_channel_cached(&url),
            "transport failure must drop the cached static channel"
        );
        let response = axum::response::IntoResponse::into_response(push_proxy_error(status));
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );

        let client = PushTokenClient {
            request_timeout: Some(Duration::from_millis(50)),
            ..client
        };
        let status = client
            .call(|| async {
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok::<(), tonic::Status>(())
            })
            .await
            .expect_err("request timeout");
        assert_eq!(status.code(), Code::DeadlineExceeded);
        let response = axum::response::IntoResponse::into_response(push_proxy_error(status));
        assert_eq!(response.status(), axum::http::StatusCode::GATEWAY_TIMEOUT);
    }
}
