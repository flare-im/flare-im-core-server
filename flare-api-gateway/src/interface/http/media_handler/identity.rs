//! Media RPC 调用方身份：租户与用户只取自已验证的令牌主体，
//! 由 `MediaCaller` 在每次 RPC 前现签 Cell 内部上下文；不读取调用方自带的身份头。
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use flare_im_service_kit::clients::MediaCaller;
use flare_server_core::{
    AuthenticatedPrincipal,
    context::Ctx,
    error::{ErrorBuilder, ErrorCode},
    http::{ContextFromHeaders, HttpApiError as GatewayError},
};

use crate::interface::http::MessageContextSigner;

/// Media handler 的调用方提取器。缺签名器、缺租户/用户绑定或是管理员 scope 时直接拒绝，
/// 绝不向 Media 发出未签名的请求。
pub(crate) struct MediaIdentity(pub(crate) MediaCaller);

impl<S: Send + Sync> FromRequestParts<S> for MediaIdentity {
    type Rejection = GatewayError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let signer = parts
            .extensions
            .get::<MessageContextSigner>()
            .and_then(|signer| signer.0.clone())
            .ok_or_else(|| {
                GatewayError::internal(
                    "MEDIA_SIGNER_UNAVAILABLE",
                    "internal identity signer not configured",
                )
            })?;
        let principal = parts
            .extensions
            .get::<AuthenticatedPrincipal>()
            .ok_or_else(|| forbidden("media requires a tenant-bound user identity"))?;
        let user_id = principal.user_id.trim();
        let tenant_id = principal
            .tenant_id
            .as_deref()
            .filter(|tenant| !tenant.is_empty() && tenant.trim() == *tenant && *tenant != "*")
            .ok_or_else(|| forbidden("media requires a tenant-bound user identity"))?;
        if user_id.is_empty() || principal.has_gateway_admin_scope() {
            return Err(forbidden("media requires an end-user identity"));
        }
        // 租户保持令牌原值（不做 default→0 归一化），与已落库媒资的租户键一致。
        let ctx = Ctx::from_headers(&parts.headers);
        Ok(Self(MediaCaller::user(ctx, signer, tenant_id, user_id)))
    }
}

fn forbidden(message: &str) -> GatewayError {
    GatewayError(
        ErrorBuilder::new(ErrorCode::HttpForbidden, "MEDIA_IDENTITY_REQUIRED")
            .details(message)
            .build_error(),
    )
}
