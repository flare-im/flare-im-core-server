use std::sync::Arc;

use axum::{
    extract::{Extension, Request},
    middleware::Next,
    response::Response,
};
use flare_im_service_kit::gateway_auth::{
    TenantRuntimeGuard, auth_error_response, authenticate_http_request,
};
use flare_server_core::{AuthenticatedPrincipal, TokenValidator};

const GATEWAY_NAME: &str = "api-gateway";

pub async fn gateway_auth_middleware(
    Extension(validator): Extension<Arc<dyn TokenValidator>>,
    Extension(tenant_guard): Extension<TenantRuntimeGuard>,
    request: Request,
    next: Next,
) -> Response {
    match authenticate_http_request(&validator, request, GATEWAY_NAME).await {
        Ok(request) => {
            let tenant = request
                .extensions()
                .get::<AuthenticatedPrincipal>()
                .and_then(|principal| principal.tenant_id.as_deref());
            match tenant_guard.admit(tenant).await {
                Ok(()) => next.run(request).await,
                Err(err) => auth_error_response(err, GATEWAY_NAME),
            }
        }
        Err(err) => auth_error_response(err, GATEWAY_NAME),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        routing::get,
    };
    use flare_im_contracts::domain::tenant::{TenantQuota, TenantRuntimeSnapshot, TenantStatus};
    use flare_im_service_kit::tenant_runtime::{
        TenantAccessPolicy, TenantRuntimeCache, TenantRuntimeCacheOptions, TenantRuntimeSource,
    };
    use flare_server_core::{AuthError, AuthenticatedPrincipal, TokenValidationRequest};
    use tower::ServiceExt;

    struct AcceptToken;

    #[async_trait::async_trait]
    impl TokenValidator for AcceptToken {
        async fn validate(
            &self,
            _request: TokenValidationRequest,
        ) -> Result<AuthenticatedPrincipal, AuthError> {
            Ok(AuthenticatedPrincipal {
                user_id: "user-a".into(),
                tenant_id: Some("tenant-a".into()),
                device_id: None,
                app_id: None,
                expires_at: None,
                scopes: Vec::new(),
                metadata: Default::default(),
            })
        }
    }

    struct RuntimeTenant(TenantStatus);

    #[async_trait::async_trait]
    impl TenantRuntimeSource for RuntimeTenant {
        async fn load(&self, tenant_id: &str) -> Result<Option<TenantRuntimeSnapshot>, String> {
            Ok(Some(TenantRuntimeSnapshot {
                tenant_id: tenant_id.into(),
                status: self.0,
                quota: TenantQuota::default(),
                version: 1,
            }))
        }
    }

    async fn call(status: TenantStatus) -> (StatusCode, String) {
        let validator: Arc<dyn TokenValidator> = Arc::new(AcceptToken);
        let guard = TenantRuntimeGuard::new(
            TenantRuntimeCache::with_source(
                Arc::new(RuntimeTenant(status)),
                TenantRuntimeCacheOptions::default(),
            ),
            TenantAccessPolicy::Strict,
        );
        let app = Router::new()
            .route("/send", get(|| async { StatusCode::OK }))
            .route_layer(axum::middleware::from_fn(gateway_auth_middleware))
            .layer(Extension(validator))
            .layer(Extension(guard));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/send")
                    .header("authorization", "Bearer token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let reason = serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|body| body["reason"].as_str().map(str::to_owned))
            .unwrap_or_default();
        (status, reason)
    }

    /// 停用 / 删除中租户的请求答 403 `TENANT_UNAVAILABLE`，与 `/ws` 拒绝、踢线、摄入侧拒绝同一个码。
    #[tokio::test]
    async fn validated_access_token_cannot_call_http_api_after_tenant_suspension() {
        assert_eq!(call(TenantStatus::Active).await.0, StatusCode::OK);
        for status in [TenantStatus::Suspended, TenantStatus::Deleting] {
            assert_eq!(
                call(status).await,
                (StatusCode::FORBIDDEN, "TENANT_UNAVAILABLE".to_string()),
                "{status:?}"
            );
        }
    }
}
