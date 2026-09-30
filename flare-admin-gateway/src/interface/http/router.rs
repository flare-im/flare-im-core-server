use axum::{
    Json, Router,
    http::header,
    middleware,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use std::sync::Arc;
use utoipa::OpenApi;

use super::admin_auth_middleware::admin_auth_middleware;
use super::admin_handler;
use super::media_handler;
use flare_im_service_kit::clients::GrpcClients;

#[allow(dead_code)]
#[derive(OpenApi)]
#[openapi(
    paths(
        admin_handler::admin_auth_check,
        admin_handler::admin_capabilities,
        admin_handler::admin_gateway_health,
        admin_handler::admin_gateway_upstreams,
        admin_handler::admin_gateway_routes,
        admin_handler::admin_gateway_config,
        admin_handler::admin_query_messages,
        admin_handler::admin_get_message,
        admin_handler::admin_query_message_events,
        admin_handler::admin_query_message_write_ledger,
        admin_handler::admin_export_messages,
        media_handler::cleanup_orphaned_assets,
        media_handler::set_object_acl,
        media_handler::list_objects,
        media_handler::describe_bucket,
    ),
    components(
        schemas(
            media_handler::ListObjectsQuery,
            media_handler::AdminMediaObjects,
            media_handler::AdminMediaObject,
            super::admin_contract::AdminCapabilitiesResponse,
            super::admin_contract::AdminRequiredHeaders,
            super::admin_contract::AdminEndpointDescriptor,
            super::admin_contract::EnterprisePolicyStatus,
            super::admin_contract::AdminOrganizationPolicyDescriptor,
            super::admin_contract::AdminDataResidencyPolicyDescriptor,
            super::admin_contract::AdminRetentionLegalPolicyDescriptor,
            super::admin_contract::EnterprisePolicyAuthority,
            super::admin_contract::OrganizationRoleSource,
            super::admin_contract::EnterpriseProtectedOperation,
            super::admin_contract::RetentionEnforcementAnchor,
            super::admin_handler::AdminAuthCheckResponse,
            super::admin_handler::AdminGatewayHealthResponse,
            super::admin_handler::AdminGatewayUpstreamsResponse,
            super::admin_handler::AdminGatewayUpstream,
            super::admin_handler::AdminGatewayRoutesResponse,
            super::admin_handler::AdminGatewayRoute,
            super::admin_handler::AdminGatewayConfigSnapshot,
            super::admin_handler::AdminServerConfigSnapshot,
            super::admin_handler::AdminGrpcConfigSnapshot,
            super::admin_handler::AdminAuthConfigSnapshot,
            super::admin_handler::AdminRateLimitConfigSnapshot,
            super::admin_handler::AdminTracingConfigSnapshot,
            crate::application::admin_messages::AdminMessageQueryHttpRequest,
            crate::application::admin_messages::AdminMessageQueryHttpResponse,
            crate::application::admin_messages::AdminMessageEventsQueryHttpRequest,
            crate::application::admin_messages::AdminMessageExportHttpRequest,
            crate::application::admin_messages::AdminMessageWriteLedgerQueryHttpRequest,
            crate::application::admin_messages::AdminMessageDetailHttpResponse,
            crate::application::admin_messages::AdminMessageEventsHttpResponse,
            crate::application::admin_messages::AdminMessageEventHttp,
            crate::application::admin_messages::AdminMessageExportHttpResponse,
            crate::application::admin_messages::AdminMessageWriteLedgerHttpResponse,
            crate::application::admin_messages::AdminMessageWriteLedgerEntryHttp,
            crate::application::admin_messages::AdminMessageHttp,
        )
    ),
    tags(
        (name = "Admin", description = "内网管理面 API 认证、安全入口和 typed facade"),
    )
)]
struct AdminApiDoc;

async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(AdminApiDoc::openapi())
}

// Swagger UI 5.3.1 的静态资源随二进制内嵌（Apache-2.0，许可与 NOTICE 见
// assets/swagger-ui/，取自 swagger-ui 官方发行包 dist/，未改动）。Admin Gateway
// 按约定部署在内网/VPN 之后，文档页不能依赖公网 CDN：离线环境会白屏，联网环境也会
// 把内网访问行为暴露给第三方。
const SWAGGER_UI_CSS: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/assets/swagger-ui/swagger-ui.css"
));
const SWAGGER_UI_BUNDLE_JS: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/assets/swagger-ui/swagger-ui-bundle.js"
));

async fn swagger_ui_html() -> Html<&'static str> {
    // validatorUrl 置空：Swagger UI 默认会让浏览器去 validator.swagger.io 取校验徽章，
    // 并把 OpenAPI 地址一并带出去。
    Html(
        r#"<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Flare Admin Gateway API Docs</title>
    <link rel="stylesheet" href="/swagger-ui/swagger-ui.css" />
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="/swagger-ui/swagger-ui-bundle.js"></script>
    <script>
      window.ui = SwaggerUIBundle({
        url: '/api-doc/openapi.json',
        dom_id: '#swagger-ui',
        validatorUrl: null
      });
    </script>
  </body>
</html>
"#,
    )
}

fn embedded_asset(content_type: &'static str, body: &'static [u8]) -> Response {
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "public, max-age=3600"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        body,
    )
        .into_response()
}

async fn swagger_ui_css() -> Response {
    embedded_asset("text/css; charset=utf-8", SWAGGER_UI_CSS)
}

async fn swagger_ui_bundle_js() -> Response {
    embedded_asset("text/javascript; charset=utf-8", SWAGGER_UI_BUNDLE_JS)
}

/// OpenAPI 文档与 Swagger UI 路由；全部资源来自本进程，不引用任何外部源。
fn api_docs_router() -> Router {
    Router::new()
        .route("/api-doc/openapi.json", get(openapi_json))
        .route("/swagger-ui", get(swagger_ui_html))
        .route("/swagger-ui/", get(swagger_ui_html))
        .route("/swagger-ui/swagger-ui.css", get(swagger_ui_css))
        .route(
            "/swagger-ui/swagger-ui-bundle.js",
            get(swagger_ui_bundle_js),
        )
}

pub fn create_admin_router(clients: Arc<GrpcClients>) -> Router {
    let admin_router = Router::new()
        .route(
            "/medias/cleanup-orphaned-assets",
            post(media_handler::cleanup_orphaned_assets),
        )
        .route("/medias/object-acl", post(media_handler::set_object_acl))
        .route("/medias/objects", get(media_handler::list_objects))
        .route("/medias/bucket", get(media_handler::describe_bucket))
        .route("/auth/check", get(admin_handler::admin_auth_check))
        .route("/capabilities", get(admin_handler::admin_capabilities))
        .route("/gateway/health", get(admin_handler::admin_gateway_health))
        .route(
            "/gateway/upstreams",
            get(admin_handler::admin_gateway_upstreams),
        )
        .route("/gateway/routes", get(admin_handler::admin_gateway_routes))
        .route("/gateway/config", get(admin_handler::admin_gateway_config))
        .route("/messages/query", post(admin_handler::admin_query_messages))
        .route(
            "/messages/export",
            post(admin_handler::admin_export_messages),
        )
        .route(
            "/messages/write-ledger/query",
            post(admin_handler::admin_query_message_write_ledger),
        )
        .route(
            "/messages/{message_id}",
            get(admin_handler::admin_get_message),
        )
        .route(
            "/messages/{message_id}/events",
            get(admin_handler::admin_query_message_events),
        )
        .layer(axum::Extension(clients))
        .route_layer(middleware::from_fn(admin_auth_middleware));

    compose_router(admin_router)
}

/// 顶层路由装配；与下游 gRPC 客户端无关，单测可直接用它验证文档页与健康检查。
fn compose_router(admin_router: Router) -> Router {
    Router::new()
        .nest("/api/v1/admin", admin_router)
        .merge(api_docs_router())
        .route("/health", get(|| async { "OK" }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    async fn get_path(path: &str) -> (StatusCode, Option<String>, Vec<u8>) {
        let admin_stub = Router::new().route("/auth/check", get(|| async { "stub" }));
        let response = compose_router(admin_stub)
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .map(|value| value.to_str().unwrap().to_owned());
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, content_type, body.to_vec())
    }

    /// 取出 HTML 中所有 `src="..."` / `href="..."` 的取值。
    fn referenced_urls(html: &str) -> Vec<String> {
        let mut urls = Vec::new();
        for attr in ["src=\"", "href=\""] {
            let mut rest = html;
            while let Some(start) = rest.find(attr) {
                rest = &rest[start + attr.len()..];
                let end = rest.find('"').unwrap();
                urls.push(rest[..end].to_owned());
                rest = &rest[end..];
            }
        }
        urls
    }

    #[tokio::test]
    async fn swagger_ui_bundle_is_served_from_embedded_asset() {
        let (status, content_type, body) = get_path("/swagger-ui/swagger-ui-bundle.js").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            content_type.as_deref(),
            Some("text/javascript; charset=utf-8")
        );
        assert_eq!(body, SWAGGER_UI_BUNDLE_JS);
        assert!(String::from_utf8_lossy(&body).contains("SwaggerUIBundle"));
    }

    #[tokio::test]
    async fn swagger_ui_css_is_served_from_embedded_asset() {
        let (status, content_type, body) = get_path("/swagger-ui/swagger-ui.css").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(content_type.as_deref(), Some("text/css; charset=utf-8"));
        assert!(!body.is_empty());
        assert_eq!(body, SWAGGER_UI_CSS);
    }

    #[tokio::test]
    async fn swagger_ui_html_references_only_local_resources() {
        for page in ["/swagger-ui", "/swagger-ui/"] {
            let (status, _, body) = get_path(page).await;
            assert_eq!(status, StatusCode::OK);
            let html = String::from_utf8(body).unwrap();

            assert!(!html.contains("unpkg.com"), "{page} still loads from unpkg");
            assert!(
                !html.contains("://"),
                "{page} references an external origin"
            );

            let urls = referenced_urls(&html);
            assert_eq!(
                urls.len(),
                2,
                "{page} should reference exactly the css and bundle: {urls:?}"
            );
            for url in urls {
                let (status, _, body) = get_path(&url).await;
                assert_eq!(status, StatusCode::OK, "{page} references missing {url}");
                assert!(!body.is_empty(), "{url} is empty");
            }
        }
    }

    #[tokio::test]
    async fn openapi_json_and_health_stay_reachable_next_to_swagger_ui() {
        let (status, content_type, _) = get_path("/api-doc/openapi.json").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(content_type.as_deref(), Some("application/json"));

        let (status, _, body) = get_path("/health").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"OK");
    }
}
