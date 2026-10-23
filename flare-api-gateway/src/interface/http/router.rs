use axum::{
    Json, Router,
    http::header,
    middleware,
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post, put},
};
use std::sync::Arc;
use utoipa::OpenApi;

use super::auth_handler;
use super::auth_middleware::gateway_auth_middleware;
use super::conversation_handler;
use super::media_handler;
use super::message_handler;
use super::presence_handler;
use super::push_token_handler;
use super::read_receipt_handler;
use flare_im_service_kit::clients::GrpcClients;
use flare_server_core::http::HttpApiError;

/// OpenAPI 文档定义（由 utoipa 过程宏消费，编译器视为未构造）
#[allow(dead_code)]
#[derive(OpenApi)]
#[openapi(
    paths(
        media_handler::uploads::generate_upload_url,
        media_handler::uploads::upload_file,
        media_handler::uploads::initiate_multipart_upload,
        media_handler::uploads::upload_multipart_chunk,
        media_handler::uploads::complete_multipart_upload,
        media_handler::uploads::abort_multipart_upload,
        media_handler::uploads::initiate_direct_upload,
        media_handler::uploads::get_direct_upload_status,
        media_handler::uploads::presign_direct_upload_parts,
        media_handler::uploads::commit_direct_upload_parts,
        media_handler::uploads::complete_direct_upload,
        media_handler::uploads::abort_direct_upload,
        media_handler::files::get_file_url,
        media_handler::files::get_file_info,
        media_handler::files::delete_file,
        media_handler::references::create_reference,
        media_handler::references::delete_reference,
        media_handler::references::list_references,
        media_handler::processing::process_image,
        media_handler::processing::process_video,
        super::capability_handler::dispatch,
        message_handler::send_message,
        message_handler::execute_custom_event,
        message_handler::recall_message,
        read_receipt_handler::get_message_read_summaries,
        read_receipt_handler::list_message_readers,
        conversation_handler::list_conversations,
        conversation_handler::list_conversation_participants,
        conversation_handler::manage_participants,
        presence_handler::get_user_presence,
        presence_handler::list_user_devices,
        presence_handler::get_device,
        presence_handler::batch_get_user_presence,
        presence_handler::kick_device,
        presence_handler::logout_presence,
        push_token_handler::register_push_token,
        push_token_handler::inspect_push_token,
        push_token_handler::revoke_push_token,
        auth_handler::issue_token,
        auth_handler::refresh_token,
        auth_handler::revoke_user,
    ),
    components(
        schemas(
            crate::application::dto::UploadFileMetadataHttp,
            crate::application::dto::UploadFileHttpRequest,
            crate::application::dto::UploadFileHttpResponse,
            crate::application::dto::InitiateMultipartUploadHttpRequest,
            crate::application::dto::InitiateMultipartUploadHttpResponse,
            crate::application::dto::UploadMultipartChunkHttpRequest,
            crate::application::dto::UploadMultipartChunkHttpResponse,
            crate::application::dto::CompleteMultipartUploadHttpRequest,
            crate::application::dto::AbortMultipartUploadHttpRequest,
            crate::application::dto::AbortMultipartUploadHttpResponse,
            crate::application::dto::DirectUploadTransportKindHttp,
            crate::application::dto::InitiateDirectUploadHttpRequest,
            crate::application::dto::InitiateDirectUploadHttpResponse,
            crate::application::dto::GetDirectUploadStatusHttpRequest,
            crate::application::dto::GetDirectUploadStatusHttpResponse,
            crate::application::dto::UploadedPartInfoHttp,
            crate::application::dto::PresignDirectUploadPartsHttpRequest,
            crate::application::dto::PresignedUploadPartHttp,
            crate::application::dto::PresignDirectUploadPartsHttpResponse,
            crate::application::dto::CommitDirectUploadPartsHttpRequest,
            crate::application::dto::CommitDirectUploadPartsHttpResponse,
            crate::application::dto::CompleteDirectUploadHttpRequest,
            crate::application::dto::AbortDirectUploadHttpRequest,
            crate::application::dto::ImageOperationHttp,
            crate::application::dto::ProcessImageHttpRequest,
            crate::application::dto::ProcessImageHttpResponse,
            crate::application::dto::VideoOperationHttp,
            crate::application::dto::ProcessVideoHttpRequest,
            crate::application::dto::ProcessVideoHttpResponse,
            crate::application::dto::GetFileUrlHttpResponse,
            crate::application::dto::FileInfoHttpResponse,
            crate::application::dto::CreateReferenceHttpResponse,
            crate::application::dto::DeleteReferenceHttpResponse,
            crate::application::dto::MediaReferenceHttpResponse,
            crate::application::dto::ListReferencesHttpResponse,
            crate::application::dto::ListObjectsHttpResponse,
            super::capability_handler::DispatchRequest,
            super::capability_handler::DispatchResponse,
            crate::application::dto::SendMessageHttpRequest,
            crate::application::dto::SendMessageHttpResponse,
            crate::application::dto::ExecuteCustomEventHttpRequest,
            crate::application::dto::ExecuteCustomEventHttpResponse,
            crate::application::dto::RecallMessageHttpRequest,
            crate::application::dto::RecallMessageHttpResponse,
            super::read_receipt_handler::MessageReadSummariesHttpRequest,
            super::read_receipt_handler::MessageReadSummaryHttp,
            super::read_receipt_handler::MessageReadSummariesHttpResponse,
            super::read_receipt_handler::ListMessageReadersHttpResponse,
            super::conversation_handler::ListConversationsHttpRequest,
            super::conversation_handler::ConversationHttpResponse,
            super::conversation_handler::ListConversationsHttpResponse,
            super::conversation_handler::ConversationParticipantHttp,
            super::conversation_handler::ListConversationParticipantsHttpRequest,
            super::conversation_handler::ListConversationParticipantsHttpResponse,
            super::conversation_handler::ManageParticipantsHttpRequest,
            super::conversation_handler::ManageParticipantsHttpResponse,
            super::conversation_handler::ParticipantRoleUpdateHttp,
            super::presence_handler::UserPresenceHttp,
            super::presence_handler::DevicePresenceHttp,
            super::presence_handler::ConnectionQualityHttp,
            super::presence_handler::ListUserDevicesHttpResponse,
            super::presence_handler::BatchGetUserPresenceHttpRequest,
            super::presence_handler::BatchGetUserPresenceHttpResponse,
            super::presence_handler::KickDeviceHttpRequest,
            super::presence_handler::KickDeviceHttpResponse,
            super::presence_handler::LogoutPresenceHttpRequest,
            super::presence_handler::LogoutPresenceHttpResponse,
            super::push_token_handler::RegisterPushTokenHttpRequest,
            super::push_token_handler::PushTokenHttpResponse,
            super::push_token_handler::PushBindingView,
            super::push_token_handler::RevokePushTokenRequest,
            super::auth_handler::IssueTokenHttpRequest,
        auth_handler::RevokeUserHttpRequest,
            super::auth_handler::IssuedTokenHttp,
        )
    ),
    tags(
        (name = "Media", description = "媒体文件管理接口"),
        (name = "Message", description = "消息管理接口"),
        (name = "Presence", description = "在线状态接口"),
        (name = "Device", description = "设备推送令牌接口"),
    )
)]
struct ApiDoc;

async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}

// Swagger UI 5.3.1 的静态资源随二进制内嵌，文档页不依赖公网 CDN：私有化/离线部署
// 打开不会白屏，也不会把访问行为暴露给第三方。资源与 Apache-2.0 LICENSE/NOTICE 只在
// flare-admin-gateway/assets/swagger-ui/ 保存一份（取自 swagger-ui 官方 dist/，未改动），
// 两个网关共用同一份文件，升级时只改一处；该目录移动时这里会编译失败而不是静默漂移。
const SWAGGER_UI_CSS: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../flare-admin-gateway/assets/swagger-ui/swagger-ui.css"
));
const SWAGGER_UI_BUNDLE_JS: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../flare-admin-gateway/assets/swagger-ui/swagger-ui-bundle.js"
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
    <title>Flare API Gateway Docs</title>
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

/// 创建默认路由。
///
/// `flare-api-gateway` 只暴露 public/third-party API；Admin API 由
/// `flare-admin-gateway` 单独承载。
pub fn create_router(clients: Arc<GrpcClients>) -> Router {
    create_public_router(clients)
}

/// 创建 public API Gateway 路由。
pub fn create_public_router(clients: Arc<GrpcClients>) -> Router {
    // Media API 路由
    let media_router = Router::new()
        .route("/files/{file_id}", get(media_handler::serve_file))
        .route("/upload-url", post(media_handler::generate_upload_url))
        .route("/upload-file", post(media_handler::upload_file))
        .route(
            "/multipart/initiate",
            post(media_handler::initiate_multipart_upload),
        )
        .route(
            "/multipart/chunk",
            post(media_handler::upload_multipart_chunk),
        )
        .route(
            "/multipart/complete",
            post(media_handler::complete_multipart_upload),
        )
        .route(
            "/multipart/abort",
            post(media_handler::abort_multipart_upload),
        )
        .route(
            "/uploads/initiate",
            post(media_handler::initiate_direct_upload),
        )
        .route(
            "/uploads/status",
            get(media_handler::get_direct_upload_status),
        )
        .route(
            "/uploads/presign-parts",
            post(media_handler::presign_direct_upload_parts),
        )
        .route(
            "/uploads/commit-parts",
            post(media_handler::commit_direct_upload_parts),
        )
        .route(
            "/uploads/complete",
            post(media_handler::complete_direct_upload),
        )
        .route("/uploads/abort", post(media_handler::abort_direct_upload))
        .route("/file-url", post(media_handler::get_file_url))
        .route("/file-info", get(media_handler::get_file_info))
        .route("/file", delete(media_handler::delete_file))
        .route("/references", post(media_handler::create_reference))
        .route("/references", delete(media_handler::delete_reference))
        .route("/references", get(media_handler::list_references))
        .route("/process-image", post(media_handler::process_image))
        .route("/process-video", post(media_handler::process_video))
        .layer(axum::Extension(clients.clone()))
        .route_layer(middleware::from_fn(media_handler::require_media_identity))
        .route_layer(middleware::from_fn(gateway_auth_middleware));

    // Message API 路由
    let message_router = Router::new()
        .route("/send", post(message_handler::send_message))
        .route(
            "/events/custom",
            post(message_handler::execute_custom_event),
        )
        .route("/recall", post(message_handler::recall_message))
        .route(
            "/read-summaries",
            post(read_receipt_handler::get_message_read_summaries),
        )
        .route("/readers", get(read_receipt_handler::list_message_readers))
        .layer(axum::Extension(clients.clone()))
        .route_layer(middleware::from_fn(gateway_auth_middleware));

    // Conversation API 路由
    let conversation_router = Router::new()
        .route("/", get(conversation_handler::list_conversations))
        .route(
            "/participants",
            get(conversation_handler::list_conversation_participants),
        )
        .route(
            "/participants/manage",
            post(conversation_handler::manage_participants),
        )
        .layer(axum::Extension(clients.clone()))
        .route_layer(middleware::from_fn(gateway_auth_middleware));

    // Presence API 路由
    let presence_router = Router::new()
        .route("/users/{user_id}", get(presence_handler::get_user_presence))
        .route(
            "/users/{user_id}/devices",
            get(presence_handler::list_user_devices),
        )
        .route(
            "/users/batch",
            post(presence_handler::batch_get_user_presence),
        )
        .route("/devices/{device_id}", get(presence_handler::get_device))
        .route(
            "/devices/{device_id}/kick",
            post(presence_handler::kick_device),
        )
        .route("/logout", post(presence_handler::logout_presence))
        .layer(axum::Extension(clients.clone()))
        .route_layer(middleware::from_fn(gateway_auth_middleware));

    // Device API 路由：设备推送令牌。用户/租户只取自 IM 令牌，Push Proxy 客户端由启动层以
    // Extension 注入。
    let device_router = Router::new()
        .route(
            "/{device_id}/push-token",
            put(push_token_handler::register_push_token)
                .get(push_token_handler::inspect_push_token),
        )
        .route_layer(middleware::from_fn(gateway_auth_middleware));

    let capability_router = Router::new()
        .route("/dispatch", post(super::capability_handler::dispatch))
        .layer(axum::extract::DefaultBodyLimit::max(70 * 1024))
        .route_layer(middleware::from_fn(gateway_auth_middleware));

    // 主路由
    // 签发/刷新是"拿 token 之前"的接口，不能套 bearer 中间件；自己的鉴权（app 凭据 / 联调开关 /
    // 刷新用旧 token）在 handler 里做。生命周期实现不在网关（GATEWAY_SPEC 第 3 节）。
    let auth_router = Router::new()
        .route("/tokens", post(auth_handler::issue_token))
        .route("/tokens/refresh", post(auth_handler::refresh_token))
        .route("/revoke", post(auth_handler::revoke_user));

    let api_router = Router::new()
        .route(
            "/api/v1/push/bindings/revoke",
            post(push_token_handler::revoke_push_token)
                .layer(axum::extract::DefaultBodyLimit::max(512)),
        )
        .nest("/api/v1/auth", auth_router)
        .nest("/api/v1/capabilities", capability_router)
        .nest("/api/v1/medias", media_router)
        .nest("/api/v1/messages", message_router)
        .nest("/api/v1/conversations", conversation_router)
        .nest("/api/v1/presence", presence_router)
        .nest("/api/v1/devices", device_router);

    compose_router(api_router)
}

/// 对外 HTTP 的 CORS 策略：允许任意来源、方法与请求头，暴露全部应答头
/// （`Access-Control-Expose-Headers: *`）。浏览器因此能读到发送接口的 `x-client-msg-id`
/// （503 后带同一 id 重试要用它）、`retry-after` 等头。来源是 `*`、不带凭据，通配符对这类请求
/// 有效；改成显式列表时须保留 `x-client-msg-id`（见 `downstream_timeout_tests` 的跨源用例）。
/// CORS 只在这里做，Edge 原样透传。
pub(crate) fn public_cors_layer() -> tower_http::cors::CorsLayer {
    tower_http::cors::CorsLayer::permissive()
}

/// 顶层路由装配；与下游 gRPC 客户端无关，单测可直接用它验证文档页与健康检查。
fn compose_router(api_router: Router) -> Router {
    api_router
        .merge(api_docs_router())
        // 健康检查
        .route("/health", get(|| async { "OK" }))
        // 未知路由（含各分组前缀下的未知子路径）回与其它错误一致的 JSON 信封，
        // 调用方据此区分 Core 的 404 与 Edge/反代的 404。
        .fallback(route_not_found)
}

async fn route_not_found() -> HttpApiError {
    HttpApiError::not_found("route not found")
}

#[cfg(test)]
#[path = "router_media_tests.rs"]
mod media_tests;

#[cfg(test)]
#[path = "router_push_token_tests.rs"]
mod push_token_tests;

#[cfg(test)]
#[path = "router_message_send_tests.rs"]
mod message_send_tests;

#[cfg(test)]
#[path = "router_conversation_read_tests.rs"]
mod conversation_read_tests;

#[cfg(test)]
#[path = "router_downstream_timeout_tests.rs"]
mod downstream_timeout_tests;

#[cfg(test)]
#[path = "router_read_receipt_tests.rs"]
mod read_receipt_tests;

#[cfg(test)]
mod api_docs_tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    async fn get_path(path: &str) -> (StatusCode, Option<String>, Vec<u8>) {
        let api_stub = Router::new().route("/api/v1/presence/stub", get(|| async { "stub" }));
        let response = compose_router(api_stub)
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

            assert!(!html.contains("unpkg"), "{page} still loads from unpkg");
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
    async fn unknown_route_returns_json_not_found_envelope() {
        for path in ["/nope", "/api/v1/presence/nope", "/swagger-ui/nope.js"] {
            let (status, content_type, body) = get_path(path).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
            assert_eq!(content_type.as_deref(), Some("application/json"), "{path}");
            let json: serde_json::Value = serde_json::from_slice(&body)
                .unwrap_or_else(|_| panic!("{path}: 404 body is not JSON: {body:?}"));
            assert_eq!(json["code"], 404, "{path}: {json}");
            assert_eq!(json["reason"], "NOT_FOUND", "{path}: {json}");
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
