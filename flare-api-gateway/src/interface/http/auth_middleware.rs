use std::sync::Arc;

use axum::{
    extract::{Extension, OriginalUri, Request},
    middleware::Next,
    response::Response,
};
use flare_im_service_kit::gateway_auth::{
    TenantRuntimeGuard, auth_error_response, authenticate_http_request,
};
use flare_im_usage::UsageRecorder;
use flare_server_core::{AuthenticatedPrincipal, TokenValidator};

const GATEWAY_NAME: &str = "api-gateway";

/// `api.calls` 的路由组维度：`/api/v1/<group>/...` 的分组段，封闭取值。
fn route_group(request: &Request) -> &'static str {
    let path = request
        .extensions()
        .get::<OriginalUri>()
        .map_or_else(|| request.uri().path(), |original| original.path());
    match path
        .strip_prefix("/api/v1/")
        .and_then(|rest| rest.split('/').next())
    {
        Some("medias") => "media",
        Some("messages") => "messages",
        Some("conversations") => "conversations",
        Some("presence") => "presence",
        Some("devices") => "devices",
        _ => "other",
    }
}

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
                Ok(()) => {
                    // 用量计数点（`api.calls`）：通过鉴权与租户闸门的请求按租户与路由组计一次。
                    // 记录器由启动层以 Extension 注入；单测里没有就不计。
                    if let (Some(tenant), Some(usage)) =
                        (tenant, request.extensions().get::<UsageRecorder>())
                    {
                        usage.api_call(tenant, route_group(&request));
                    }
                    next.run(request).await
                }
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

    /// 通过鉴权与租户闸门的请求按租户与路由组计 `api.calls`；被闸门拒绝的不计。
    #[tokio::test]
    async fn admitted_requests_are_counted_per_tenant_and_route_group() {
        let usage = UsageRecorder::unpublished("api-gateway-test");
        let app = |status| {
            let validator: Arc<dyn TokenValidator> = Arc::new(AcceptToken);
            let guard = TenantRuntimeGuard::new(
                TenantRuntimeCache::with_source(
                    Arc::new(RuntimeTenant(status)),
                    TenantRuntimeCacheOptions::default(),
                ),
                TenantAccessPolicy::Strict,
            );
            let messages = Router::new()
                .route("/send", get(|| async { StatusCode::OK }))
                .route_layer(axum::middleware::from_fn(gateway_auth_middleware));
            Router::new()
                .nest("/api/v1/messages", messages)
                .layer(Extension(validator))
                .layer(Extension(guard))
                .layer(Extension(usage.clone()))
        };
        let request = || {
            Request::builder()
                .uri("/api/v1/messages/send")
                .header("authorization", "Bearer token")
                .body(Body::empty())
                .unwrap()
        };
        for _ in 0..2 {
            let response = app(TenantStatus::Active).oneshot(request()).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
        let response = app(TenantStatus::Suspended)
            .oneshot(request())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let live = usage.live();
        assert_eq!(live.len(), 1);
        assert_eq!(
            (live[0].app.as_str(), live[0].meter, live[0].value),
            ("tenant-a", "api.calls", 2)
        );
        assert_eq!(live[0].dims, [("route_group", "messages")]);
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
