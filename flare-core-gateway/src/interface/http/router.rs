use axum::{
    Json, Router, middleware,
    response::Html,
    routing::{delete, get, post},
};
use std::sync::Arc;
use utoipa::OpenApi;

use super::auth_middleware::gateway_auth_middleware;
use super::conversation_handler;
use super::media_handler;
use super::message_handler;
use super::presence_handler;
use flare_im_core::clients::GrpcClients;

/// OpenAPI 文档定义（由 utoipa 过程宏消费，编译器视为未构造）
#[allow(dead_code)]
#[derive(OpenApi)]
#[openapi(
    paths(
        media_handler::generate_upload_url,
        media_handler::upload_file,
        media_handler::initiate_multipart_upload,
        media_handler::upload_multipart_chunk,
        media_handler::complete_multipart_upload,
        media_handler::abort_multipart_upload,
        media_handler::initiate_direct_upload,
        media_handler::get_direct_upload_status,
        media_handler::presign_direct_upload_parts,
        media_handler::commit_direct_upload_parts,
        media_handler::complete_direct_upload,
        media_handler::abort_direct_upload,
        media_handler::get_file_url,
        media_handler::get_file_info,
        media_handler::delete_file,
        media_handler::create_reference,
        media_handler::delete_reference,
        media_handler::list_references,
        media_handler::cleanup_orphaned_assets,
        media_handler::process_image,
        media_handler::process_video,
        media_handler::set_object_acl,
        media_handler::list_objects,
        media_handler::describe_bucket,
        message_handler::send_message,
        message_handler::recall_message,
        message_handler::mark_message_read,
        conversation_handler::list_conversations,
        conversation_handler::list_conversation_participants,
        conversation_handler::manage_participants,
        presence_handler::get_user_presence,
        presence_handler::batch_get_user_presence,
        presence_handler::logout_presence,
    ),
    components(
        schemas(
            super::media_handler::ListObjectsHttpRequest,
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
            crate::application::dto::SendMessageHttpRequest,
            crate::application::dto::SendMessageHttpResponse,
            crate::application::dto::RecallMessageHttpRequest,
            crate::application::dto::RecallMessageHttpResponse,
            crate::application::dto::MarkReadHttpRequest,
            crate::application::dto::MarkReadHttpResponse,
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
            super::presence_handler::BatchGetUserPresenceHttpRequest,
            super::presence_handler::BatchGetUserPresenceHttpResponse,
            super::presence_handler::LogoutPresenceHttpRequest,
            super::presence_handler::LogoutPresenceHttpResponse,
        )
    ),
    tags(
        (name = "Media", description = "媒体文件管理接口"),
        (name = "Message", description = "消息管理接口"),
        (name = "Presence", description = "在线状态接口"),
    )
)]
struct ApiDoc;

async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}

async fn swagger_ui_html() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Flare Core Gateway API Docs</title>
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

/// 创建默认路由。
///
/// `flare-core-gateway` 现在只暴露 public/third-party API；Admin API 由
/// `flare-admin-gateway` 单独承载。
pub fn create_router(clients: Arc<GrpcClients>) -> Router {
    create_public_router(clients)
}

/// 创建 public Core Gateway 路由。
pub fn create_public_router(clients: Arc<GrpcClients>) -> Router {
    let media_public_router = Router::new()
        .route("/files/{file_id}", get(media_handler::serve_file))
        .layer(axum::Extension(clients.clone()));

    // Media API 路由
    let media_router = Router::new()
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
        .route(
            "/cleanup-orphaned-assets",
            post(media_handler::cleanup_orphaned_assets),
        )
        .route("/process-image", post(media_handler::process_image))
        .route("/process-video", post(media_handler::process_video))
        .route("/object-acl", post(media_handler::set_object_acl))
        .route("/objects", get(media_handler::list_objects))
        .route("/bucket", get(media_handler::describe_bucket))
        .layer(axum::Extension(clients.clone()))
        .route_layer(middleware::from_fn(gateway_auth_middleware));

    // Message API 路由
    let message_router = Router::new()
        .route("/send", post(message_handler::send_message))
        .route("/recall", post(message_handler::recall_message))
        .route("/read", post(message_handler::mark_message_read))
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
            "/users/batch",
            post(presence_handler::batch_get_user_presence),
        )
        .route("/logout", post(presence_handler::logout_presence))
        .layer(axum::Extension(clients.clone()))
        .route_layer(middleware::from_fn(gateway_auth_middleware));

    // 主路由
    Router::new()
        .nest("/api/v1/medias", media_public_router.merge(media_router))
        .nest("/api/v1/messages", message_router)
        .nest("/api/v1/conversations", conversation_router)
        .nest("/api/v1/presence", presence_router)
        .route("/api-doc/openapi.json", get(openapi_json))
        .route("/swagger-ui", get(swagger_ui_html))
        .route("/swagger-ui/", get(swagger_ui_html))
        // 健康检查
        .route("/health", get(|| async { "OK" }))
}
