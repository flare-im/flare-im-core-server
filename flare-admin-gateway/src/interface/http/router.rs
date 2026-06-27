use axum::{
    Json, Router, middleware,
    response::Html,
    routing::{get, post},
};
use std::sync::Arc;
use utoipa::OpenApi;

use super::admin_auth_middleware::admin_auth_middleware;
use super::admin_handler;
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
    ),
    components(
        schemas(
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

async fn swagger_ui_html() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Flare Admin Gateway API Docs</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
      window.ui = SwaggerUIBundle({
        url: '/api-doc/openapi.json',
        dom_id: '#swagger-ui'
      });
    </script>
  </body>
</html>
"#,
    )
}

pub fn create_admin_router(clients: Arc<GrpcClients>) -> Router {
    let admin_router = Router::new()
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

    Router::new()
        .nest("/api/v1/admin", admin_router)
        .route("/api-doc/openapi.json", get(openapi_json))
        .route("/swagger-ui", get(swagger_ui_html))
        .route("/swagger-ui/", get(swagger_ui_html))
        .route("/health", get(|| async { "OK" }))
}
