use axum::{
    routing::{get, post, delete},
    Router,
};
use std::sync::Arc;
use utoipa::OpenApi;

use crate::infrastructure::grpc::GrpcClients;
use super::handler;
use super::message_handler;
use super::conversation_handler;

/// OpenAPI 文档定义（由 utoipa 过程宏消费，编译器视为未构造）
#[allow(dead_code)]
#[derive(OpenApi)]
#[openapi(
    paths(
        handler::generate_upload_url,
        handler::get_file_url,
        handler::get_file_info,
        handler::delete_file,
        message_handler::send_message,
        message_handler::recall_message,
        message_handler::mark_message_read,
    ),
    components(
        schemas(
            super::response::ErrorResponse,
            super::response::GenerateUploadUrlHttpRequest,
            super::response::GenerateUploadUrlHttpResponse,
            super::response::GetFileUrlHttpRequest,
            super::response::GetFileUrlHttpResponse,
            super::response::GetFileInfoHttpRequest,
            super::response::FileInfoHttpResponse,
            super::response::DeleteFileHttpRequest,
            super::response::DeleteFileHttpResponse,
            super::response::SendMessageHttpRequest,
            super::response::SendMessageHttpResponse,
            super::response::RecallMessageHttpRequest,
            super::response::RecallMessageHttpResponse,
            super::response::MarkReadHttpRequest,
            super::response::MarkReadHttpResponse,
        )
    ),
    tags(
        (name = "Media", description = "媒体文件管理接口"),
        (name = "Message", description = "消息管理接口"),
    )
)]
struct ApiDoc;

/// 创建路由
pub fn create_router(clients: Arc<GrpcClients>) -> Router {
    // Media API 路由
    let media_router = Router::new()
        .route("/upload-url", post(handler::generate_upload_url))
        .route("/file-url", post(handler::get_file_url))
        .route("/file-info", get(handler::get_file_info))
        .route("/file", delete(handler::delete_file))
        .layer(axum::Extension(clients.clone()));

    // Message API 路由
    let message_router = Router::new()
        .route("/send", post(message_handler::send_message))
        .route("/recall", post(message_handler::recall_message))
        .route("/read", post(message_handler::mark_message_read))
        .layer(axum::Extension(clients.clone()));

    // Conversation API 路由
    let conversation_router = Router::new()
        .route("/", get(conversation_handler::list_conversations))
        .layer(axum::Extension(clients));

    // 主路由
    Router::new()
        .nest("/api/v1/medias", media_router)
        .nest("/api/v1/messages", message_router)
        .nest("/api/v1/conversations", conversation_router)
        // 健康检查
        .route("/health", get(|| async { "OK" }))
}
