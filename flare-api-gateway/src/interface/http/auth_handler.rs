//! 接入 token 签发与刷新（`/api/v1/auth/tokens`）。
//!
//! 生命周期实现在 `flare_server_core::auth::issuer`，这里只做：入站鉴权（app 凭据 / 联调开关 /
//! bearer）、参数校验、错误映射。见 `flare-im-core/docs/AUTH-TOKEN-ISSUANCE.zh-CN.md`。

use std::sync::Arc;

use axum::{
    Json,
    extract::Extension,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument, warn};

use flare_im_service_kit::gateway::GatewaySettings;
use flare_im_service_kit::gateway_auth::{auth_error_response, extract_bearer_token};
use flare_server_core::TokenService;
use flare_server_core::auth::{AuthError, IssuedToken, TokenIssueRequest, TokenIssuer};
use flare_server_core::http::ApiResponse;

const GATEWAY_NAME: &str = "api-gateway";
pub const APP_ID_HEADER: &str = "x-app-id";
pub const APP_SECRET_HEADER: &str = "x-app-secret";

/// 网关持有的签发器；`http_hook` 模式未配 issue URL 时为 `None`（签发接口回 501）。
#[derive(Clone, Default)]
pub struct TokenIssuerHandle(pub Option<Arc<dyn TokenIssuer>>);

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct IssueTokenHttpRequest {
    pub user_id: String,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub device_id: Option<String>,
    /// 请求的有效期（秒）；超过网关 TTL 时按网关 TTL。
    #[serde(default)]
    pub ttl_secs: Option<u64>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct IssuedTokenHttp {
    pub token: String,
    pub expires_at: u64,
    pub user_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    /// 长效刷新令牌：接入令牌过期后凭它换新（发到 `/tokens/refresh` 的 Bearer 头），无需重登。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_expires_at: Option<u64>,
}

impl From<IssuedToken> for IssuedTokenHttp {
    fn from(value: IssuedToken) -> Self {
        Self {
            token: value.token,
            expires_at: value.expires_at,
            user_id: value.user_id,
            tenant_id: value.tenant_id,
            device_id: value.device_id,
            refresh_token: value.refresh_token,
            refresh_expires_at: value.refresh_expires_at,
        }
    }
}

/// 签发接入 token。
///
/// 调用方二选一：业务后端带 `x-app-id`/`x-app-secret`（服务端到服务端）；
/// 或网关开了 `AUTH_DEV_ISSUE`（联调，默认关）。
#[utoipa::path(
    post,
    path = "/api/v1/auth/tokens",
    tag = "Auth",
    request_body = IssueTokenHttpRequest,
    responses(
        (status = 200, description = "签发成功", body = ApiResponse<IssuedTokenHttp>),
        (status = 401, description = "缺少或错误的 app 凭据，且未开联调签发"),
        (status = 501, description = "签发已委托给业务认证系统（http_hook 未配 issue URL）"),
    )
)]
#[instrument(skip_all, fields(user_id = %request.user_id))]
pub async fn issue_token(
    Extension(settings): Extension<GatewaySettings>,
    Extension(issuer): Extension<TokenIssuerHandle>,
    headers: HeaderMap,
    Json(request): Json<IssueTokenHttpRequest>,
) -> Response {
    let Some(issuer) = issuer.0 else {
        return delegated_response();
    };
    if let Err(err) = authorize_issue(&settings, &headers) {
        return auth_error_response(err, GATEWAY_NAME);
    }
    if request.user_id.trim().is_empty() {
        return auth_error_response(
            AuthError::InvalidToken("userId is required".into()),
            GATEWAY_NAME,
        );
    }
    match issuer
        .issue(TokenIssueRequest {
            user_id: request.user_id,
            tenant_id: request.tenant_id,
            device_id: request.device_id,
            ttl_secs: request.ttl_secs,
        })
        .await
    {
        Ok(issued) => {
            info!(user_id = %issued.user_id, tenant_id = ?issued.tenant_id, "access token issued");
            Json(ApiResponse::success(IssuedTokenHttp::from(issued))).into_response()
        }
        Err(err) => auth_error_response(err, GATEWAY_NAME),
    }
}

/// 用当前 token 换一枚新 token（可已过期，但须在宽限期内）。
#[utoipa::path(
    post,
    path = "/api/v1/auth/tokens/refresh",
    tag = "Auth",
    responses(
        (status = 200, description = "刷新成功", body = ApiResponse<IssuedTokenHttp>),
        (status = 401, description = "token 非法、已撤销或超出刷新宽限"),
        (status = 501, description = "刷新已委托给业务认证系统"),
    )
)]
#[instrument(skip_all)]
pub async fn refresh_token(
    Extension(issuer): Extension<TokenIssuerHandle>,
    headers: HeaderMap,
) -> Response {
    let Some(issuer) = issuer.0 else {
        return delegated_response();
    };
    let current = match extract_bearer_token(&headers) {
        Ok(token) => token,
        Err(err) => return auth_error_response(err, GATEWAY_NAME),
    };
    match issuer.refresh(&current).await {
        Ok(issued) => {
            info!(user_id = %issued.user_id, "access token refreshed");
            Json(ApiResponse::success(IssuedTokenHttp::from(issued))).into_response()
        }
        Err(err) => auth_error_response(err, GATEWAY_NAME),
    }
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RevokeUserHttpRequest {
    pub user_id: String,
}

/// 强制注销某用户：撤销其全部令牌 + 广播踢人信号让各长连接网关立即关闭其活连接（撤销即断）。
///
/// 授权同签发：`x-app-id`/`x-app-secret`（业务后端）或联调开关。撤销位写入共享 token store，
/// 各网关建连时读到即拒；踢人信号经 redis pubsub 让在线连接立刻掉线。
#[utoipa::path(
    post,
    path = "/api/v1/auth/revoke",
    tag = "Auth",
    request_body = RevokeUserHttpRequest,
    responses(
        (status = 200, description = "已撤销并广播踢人"),
        (status = 401, description = "缺少或错误的 app 凭据，且未开联调"),
        (status = 501, description = "无本地 token store（http_hook 委托业务方，或未配 token_store）"),
    )
)]
#[instrument(skip_all, fields(user_id = %request.user_id))]
pub async fn revoke_user(
    Extension(settings): Extension<GatewaySettings>,
    Extension(token_service): Extension<Arc<TokenService>>,
    headers: HeaderMap,
    Json(request): Json<RevokeUserHttpRequest>,
) -> Response {
    if let Err(err) = authorize_issue(&settings, &headers) {
        return auth_error_response(err, GATEWAY_NAME);
    }
    let user_id = request.user_id.trim();
    if user_id.is_empty() {
        return auth_error_response(
            AuthError::InvalidToken("userId is required".into()),
            GATEWAY_NAME,
        );
    }
    if !token_service.has_store() {
        // 无撤销存储：撤销/踢人无处落地，明确告知（http_hook 形态由业务方负责）。
        return (
            StatusCode::NOT_IMPLEMENTED,
            Json(ApiResponse::<()>::error(
                StatusCode::NOT_IMPLEMENTED.as_u16() as i32,
                "REVOKE_UNAVAILABLE",
                "no token store configured; revocation requires services.api_gateway.token_store",
            )),
        )
            .into_response();
    }
    // 撤销窗口覆盖到刷新令牌有效期，确保被撤销的令牌在其可能存活的全程都被拒。
    let ttl = token_service.refresh_ttl();
    if let Err(err) = token_service.revoke_user(user_id, ttl) {
        warn!(%err, "revoke_user failed");
        return auth_error_response(
            AuthError::ProviderUnavailable(err.to_string()),
            GATEWAY_NAME,
        );
    }
    // 广播踢人信号（best-effort：失败不影响撤销本身，只是活连接要等自然超时/重连被拒）。
    if let Err(err) = token_service.publish_kick(user_id) {
        warn!(%err, "publish_kick failed; revocation still applied");
    }
    info!(user_id, "user revoked and kick signal broadcast");
    Json(ApiResponse::success(serde_json::json!({
        "revoked": true,
        "userId": user_id,
    })))
    .into_response()
}

/// 签发鉴权：app 凭据优先；没带凭据时看联调开关。
pub(crate) fn authorize_issue(settings: &GatewaySettings, headers: &HeaderMap) -> Result<(), AuthError> {
    let app_id = header_str(headers, APP_ID_HEADER);
    let app_secret = header_str(headers, APP_SECRET_HEADER);
    match (app_id, app_secret) {
        (Some(app_id), Some(app_secret)) => {
            let matched = settings.auth.app_credentials.iter().any(|cred| {
                cred.app_id == app_id
                    && constant_time_eq(cred.secret.as_bytes(), app_secret.as_bytes())
            });
            if matched {
                Ok(())
            } else {
                warn!(app_id = %app_id, "token issue rejected: unknown app credential");
                Err(AuthError::Forbidden("unknown app credential".into()))
            }
        }
        (Some(_), None) | (None, Some(_)) => Err(AuthError::InvalidToken(
            "both x-app-id and x-app-secret are required".into(),
        )),
        (None, None) if settings.auth.dev_issue => Ok(()),
        (None, None) => Err(AuthError::MissingToken),
    }
}

fn header_str(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

/// 凭据比对不能提前返回：比对时间随首个不同字节的位置变化会泄露密钥。
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn delegated_response() -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(ApiResponse::<IssuedTokenHttp>::error(
            StatusCode::NOT_IMPLEMENTED.as_u16() as i32,
            "TOKEN_ISSUE_DELEGATED",
            "token issuance is delegated to the business auth system (auth mode http_hook without AUTH_HOOK_ISSUE_URL)",
        )),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, body::Body, http::Request, routing::post};
    use flare_im_service_kit::gateway::GatewayEnvScope;
    use flare_server_core::auth::{CoreJwtTokenIssuer, TokenService};
    use std::collections::HashMap;
    use tower::ServiceExt;

    const SECRET: &str = "a-strong-shared-secret-with-more-than-32-bytes!";
    const APP_SECRET: &str = "business-backend-secret-16+";

    fn settings(extra: &[(&str, &str)]) -> GatewaySettings {
        let mut env: HashMap<String, String> = HashMap::new();
        env.insert("FLARE_API_GATEWAY_AUTH_APP_CREDENTIALS".into(), format!("console:{APP_SECRET}"));
        for (k, v) in extra {
            env.insert((*k).to_string(), (*v).to_string());
        }
        GatewaySettings::from_env_source(GatewayEnvScope::Api, |key| env.get(key).cloned()).unwrap()
    }

    fn service() -> Arc<TokenService> {
        Arc::new(TokenService::new(SECRET, "flare-im-core", 3600))
    }

    fn app(settings: GatewaySettings, issuer: Option<Arc<dyn TokenIssuer>>) -> Router {
        Router::new()
            .route("/tokens", post(issue_token))
            .route("/tokens/refresh", post(refresh_token))
            .layer(Extension(settings))
            .layer(Extension(TokenIssuerHandle(issuer)))
    }

    fn core_issuer() -> Option<Arc<dyn TokenIssuer>> {
        Some(Arc::new(CoreJwtTokenIssuer::new(service(), std::time::Duration::from_secs(60))))
    }

    async fn call(app: Router, req: Request<Body>) -> (StatusCode, serde_json::Value) {
        let response = app.oneshot(req).await.unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap();
        let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, json)
    }

    fn issue_req(headers: &[(&str, &str)]) -> Request<Body> {
        let mut builder = Request::builder()
            .method("POST")
            .uri("/tokens")
            .header("content-type", "application/json");
        for (k, v) in headers {
            builder = builder.header(*k, *v);
        }
        builder
            .body(Body::from(r#"{"userId":"hugo","tenantId":"0","deviceId":"ios-1"}"#))
            .unwrap()
    }

    #[tokio::test]
    async fn without_credentials_and_without_dev_issue_is_unauthorized() {
        let (status, _) = call(app(settings(&[]), core_issuer()), issue_req(&[])).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "默认不能凭 userId 白拿 token");
    }

    #[tokio::test]
    async fn dev_issue_issues_a_token_the_gateway_itself_accepts() {
        let cfg = settings(&[("FLARE_API_GATEWAY_AUTH_DEV_ISSUE", "true")]);
        let (status, json) = call(app(cfg, core_issuer()), issue_req(&[])).await;
        assert_eq!(status, StatusCode::OK, "{json}");
        let token = json["data"]["token"].as_str().unwrap();
        let claims = service().validate_token(token).expect("网关自己的校验器必须认这枚 token");
        assert_eq!(claims.sub, "hugo");
        assert_eq!(claims.tenant_id.as_deref(), Some("0"));
        assert_eq!(claims.device_id.as_deref(), Some("ios-1"));
        assert_eq!(json["data"]["userId"], "hugo");
    }

    #[tokio::test]
    async fn app_credential_grants_and_wrong_secret_is_forbidden() {
        let ok = call(
            app(settings(&[]), core_issuer()),
            issue_req(&[(APP_ID_HEADER, "console"), (APP_SECRET_HEADER, APP_SECRET)]),
        )
        .await;
        assert_eq!(ok.0, StatusCode::OK);
        let bad = call(
            app(settings(&[]), core_issuer()),
            issue_req(&[(APP_ID_HEADER, "console"), (APP_SECRET_HEADER, "wrong-secret-wrong-secret")]),
        )
        .await;
        assert_eq!(bad.0, StatusCode::FORBIDDEN);
        let half = call(app(settings(&[]), core_issuer()), issue_req(&[(APP_ID_HEADER, "console")])).await;
        assert_eq!(half.0, StatusCode::UNAUTHORIZED, "只带 app id 不带 secret 不算凭据");
    }

    #[tokio::test]
    async fn refresh_rotates_and_rejects_garbage() {
        let cfg = settings(&[("FLARE_API_GATEWAY_AUTH_DEV_ISSUE", "true")]);
        let (_, json) = call(app(cfg.clone(), core_issuer()), issue_req(&[])).await;
        let token = json["data"]["token"].as_str().unwrap().to_string();

        let req = Request::builder()
            .method("POST")
            .uri("/tokens/refresh")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let (status, json) = call(app(cfg.clone(), core_issuer()), req).await;
        assert_eq!(status, StatusCode::OK, "{json}");
        assert_ne!(json["data"]["token"].as_str().unwrap(), token);
        assert_eq!(json["data"]["userId"], "hugo");

        let req = Request::builder()
            .method("POST")
            .uri("/tokens/refresh")
            .header("authorization", "Bearer not-a-jwt")
            .body(Body::empty())
            .unwrap();
        let (status, _) = call(app(cfg, core_issuer()), req).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn without_an_issuer_the_endpoints_say_delegated() {
        let cfg = settings(&[("FLARE_API_GATEWAY_AUTH_DEV_ISSUE", "true")]);
        let (status, json) = call(app(cfg, None), issue_req(&[])).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert_eq!(json["reason"], "TOKEN_ISSUE_DELEGATED");
    }

    #[test]
    fn constant_time_eq_only_matches_identical_bytes() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
    }
}
