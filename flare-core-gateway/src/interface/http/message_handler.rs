use axum::{
    extract::{Extension, Json},
    http::HeaderMap,
};
use std::sync::Arc;
use tracing::{debug, instrument};

use crate::application::dto::{
    MarkReadHttpRequest, MarkReadHttpResponse, RecallMessageHttpRequest, RecallMessageHttpResponse,
    SendMessageHttpRequest, SendMessageHttpResponse,
};
use crate::context::Ctx;
use crate::error::Result;
use crate::infrastructure::grpc::GrpcClients;
use flare_server_core::http::ApiResponse;

/// 发送消息
#[utoipa::path(
    post,
    path = "/api/v1/messages/send",
    tag = "Message",
    request_body = SendMessageHttpRequest,
    responses(
        (status = 200, description = "成功", body = ApiResponse<SendMessageHttpResponse>),
        (status = 400, description = "参数错误"),
    ),
)]
#[instrument(skip(headers, _clients))]
pub async fn send_message(
    headers: HeaderMap,
    Extension(_clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<SendMessageHttpRequest>,
) -> Result<Json<ApiResponse<SendMessageHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(
        trace_id = %ctx.trace_id,
        conversation_id = %req.conversation_id,
        message_type = req.message_type,
        "Sending message"
    );

    // TODO: 实现 gRPC 调用
    let response = SendMessageHttpResponse {
        server_msg_id: uuid::Uuid::new_v4().to_string(),
        seq: 1,
        success: true,
    };

    Ok(Json(ApiResponse::success(response)))
}

/// 撤回消息
#[utoipa::path(
    post,
    path = "/api/v1/messages/recall",
    tag = "Message",
    request_body = RecallMessageHttpRequest,
    responses(
        (status = 200, description = "成功", body = ApiResponse<RecallMessageHttpResponse>),
        (status = 400, description = "参数错误"),
    ),
)]
#[instrument(skip(headers, _clients))]
pub async fn recall_message(
    headers: HeaderMap,
    Extension(_clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<RecallMessageHttpRequest>,
) -> Result<Json<ApiResponse<RecallMessageHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(
        trace_id = %ctx.trace_id,
        conversation_id = %req.conversation_id,
        message_id = %req.message_id,
        "Recalling message"
    );

    // TODO: 实现 gRPC 调用
    let response = RecallMessageHttpResponse { success: true };

    Ok(Json(ApiResponse::success(response)))
}

/// 标记消息已读
#[utoipa::path(
    post,
    path = "/api/v1/messages/read",
    tag = "Message",
    request_body = MarkReadHttpRequest,
    responses(
        (status = 200, description = "成功", body = ApiResponse<MarkReadHttpResponse>),
        (status = 400, description = "参数错误"),
    ),
)]
#[instrument(skip(headers, _clients))]
pub async fn mark_message_read(
    headers: HeaderMap,
    Extension(_clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<MarkReadHttpRequest>,
) -> Result<Json<ApiResponse<MarkReadHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(
        trace_id = %ctx.trace_id,
        conversation_id = %req.conversation_id,
        message_id = %req.message_id,
        "Marking message as read"
    );

    // TODO: 实现 gRPC 调用
    let response = MarkReadHttpResponse { success: true };

    Ok(Json(ApiResponse::success(response)))
}
