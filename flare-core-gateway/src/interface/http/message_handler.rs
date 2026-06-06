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
use flare_grpc_proto::message::{MarkMessageReadRequest, RecallMessageRequest, SendMessageRequest};
use flare_im_core::clients::GrpcClients;
use flare_proto::common::{
    CustomContent, Message, MessageContent, MessageSource, MessageStatus, message_content,
};
use flare_server_core::{
    context::Ctx,
    http::{ApiResponse, ContextFromHeaders, HttpApiError as GatewayError, Result},
};

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
#[instrument(skip(headers, clients))]
pub async fn send_message(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<SendMessageHttpRequest>,
) -> Result<Json<ApiResponse<SendMessageHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(
        trace_id = %ctx.trace_id(),
        conversation_id = %req.conversation_id,
        message_type = req.message_type,
        "Sending message"
    );

    let sender_id = ctx
        .require_user_id()
        .map_err(|e| GatewayError::bad_request("USER_REQUIRED", e))?
        .to_string();
    let content = serde_json::to_vec(&req.content)?;
    let client_msg_id = if req.client_msg_id.trim().is_empty() {
        format!("http_{}", uuid::Uuid::new_v4())
    } else {
        req.client_msg_id.clone()
    };
    let message = Message {
        server_id: String::new(),
        conversation_id: req.conversation_id.clone(),
        client_msg_id,
        sender_id,
        source: MessageSource::User as i32,
        conversation_seq: 0,
        created_at: 0,
        conversation_type: req.conversation_type,
        message_type: req.message_type,
        message_seq: None,
        channel_id: req.channel_id,
        sender_name: req.sender_name,
        sender_avatar: req.sender_avatar,
        // HTTP 网关保留 JSON 透明代理能力；结构化客户端仍应通过 SDK 写入具体 MessageContent 变体。
        content: Some(MessageContent {
            content: Some(message_content::Content::Custom(CustomContent {
                r#type: "http.json".to_string(),
                payload: content,
                description: String::new(),
                attributes: std::collections::HashMap::new(),
            })),
        }),
        status: MessageStatus::Created as i32,
        retention_policy: None,
        retention_state: None,
        offline_push_info: None,
        attributes: std::collections::HashMap::new(),
        extensions: std::collections::HashMap::new(),
    };
    let grpc_req = SendMessageRequest {
        conversation_id: req.conversation_id,
        message: Some(message),
        sync: req.sync,
        svid: req.svid,
    };

    let mut message_client = clients.message_send.lock().await;
    let grpc_res = message_client
        .send_message_with_ctx(&ctx, grpc_req)
        .await
        .map_err(|err| GatewayError::internal("MESSAGE_SEND_FAILED", err.to_string()))?;
    let response = SendMessageHttpResponse {
        server_msg_id: grpc_res.server_msg_id,
        seq: grpc_res.conversation_seq,
        success: grpc_res.success,
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
#[instrument(skip(headers, clients))]
pub async fn recall_message(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<RecallMessageHttpRequest>,
) -> Result<Json<ApiResponse<RecallMessageHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(
        trace_id = %ctx.trace_id(),
        conversation_id = %req.conversation_id,
        message_id = %req.message_id,
        "Recalling message"
    );

    let grpc_req = RecallMessageRequest {
        message_id: req.message_id,
        reason: String::new(),
        recall_time_limit_seconds: 0,
        conversation_id: req.conversation_id,
    };
    let mut action_client = clients.message_action.lock().await;
    let grpc_res = action_client
        .recall_message_with_ctx(&ctx, grpc_req)
        .await
        .map_err(|err| GatewayError::internal("MESSAGE_RECALL_FAILED", err.to_string()))?;
    let response = RecallMessageHttpResponse {
        success: grpc_res.success,
    };

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
#[instrument(skip(headers, clients))]
pub async fn mark_message_read(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<MarkReadHttpRequest>,
) -> Result<Json<ApiResponse<MarkReadHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(
        trace_id = %ctx.trace_id(),
        conversation_id = %req.conversation_id,
        message_id = %req.message_id,
        "Marking message as read"
    );

    let grpc_req = MarkMessageReadRequest {
        message_id: req.message_id,
        read_at: None,
        conversation_id: req.conversation_id,
    };
    let mut action_client = clients.message_action.lock().await;
    let grpc_res = action_client
        .mark_message_read_with_ctx(&ctx, grpc_req)
        .await
        .map_err(|err| GatewayError::internal("MESSAGE_MARK_READ_FAILED", err.to_string()))?;
    let response = MarkReadHttpResponse {
        success: grpc_res.success,
    };

    Ok(Json(ApiResponse::success(response)))
}
