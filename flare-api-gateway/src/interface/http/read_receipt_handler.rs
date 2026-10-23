//! 群聊消息的已读摘要与已读名单（发送者查看「N 人未读 / 全部已读」与 ReadReceiptSheet）。
//!
//! 身份与读会话接口相同（`conversation_reader`）：只取令牌验出的用户与规范化租户，网关代签
//! User 签名上下文转给会话服务的 `MessageReadReceiptService`。会话服务按成员的已读水位计数，
//! 只回答调用者本人发送的消息；不是群聊或成员（未退出；隐藏了会话的也算）超过上限（500）时
//! `available=false`。
use axum::{
    extract::{Extension, Json},
    http::HeaderMap,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::instrument;

use super::MessageContextSigner;
use super::conversation_handler::conversation_reader;
use flare_grpc_proto::conversation::{
    GetMessageReadSummariesRequest, ListMessageReadersRequest, MessageReadSummary,
    MessageReaderState,
};
use flare_im_service_kit::clients::GrpcClients;
use flare_server_core::{
    AuthenticatedPrincipal,
    error::{ErrorBuilder, ErrorCode, FlareError},
    http::{ApiResponse, HttpApiError as GatewayError, Result},
};

/// 一批消息的已读摘要请求。
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct MessageReadSummariesHttpRequest {
    /// 群聊会话 ID
    pub conversation_id: String,
    /// 1..=50 个互不相同的服务端消息 ID（通常是可见页）
    #[serde(default)]
    pub server_msg_ids: Vec<String>,
}

/// 一条消息的已读摘要；total = read_count + unread_count。
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct MessageReadSummaryHttp {
    pub server_msg_id: String,
    /// 已读水位不低于消息 seq 的成员数
    pub read_count: u32,
    /// 其余能看到这条消息的成员数
    pub unread_count: u32,
    /// 除发送者外、能看到这条消息的成员数
    pub total: u32,
}

/// 一批消息的已读摘要响应。
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct MessageReadSummariesHttpResponse {
    pub conversation_id: String,
    /// false：不是群聊，或成员超过上限；summaries 为空，客户端不显示
    pub available: bool,
    /// 只含调用者本人发送、未撤回也未删除的消息，按请求顺序；其他 ID 不出现
    pub summaries: Vec<MessageReadSummaryHttp>,
}

/// 已读名单查询参数。
#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct ListMessageReadersHttpQuery {
    /// 群聊会话 ID
    pub conversation_id: String,
    /// 服务端消息 ID
    pub server_msg_id: String,
    /// `read` 或 `unread`
    pub state: String,
    /// 不透明游标，首页为空
    #[serde(default)]
    pub cursor: String,
    /// 0 = 50，最大 200
    #[serde(default)]
    pub limit: i32,
}

/// 已读名单响应：按 user_id 升序分页。
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ListMessageReadersHttpResponse {
    pub conversation_id: String,
    pub server_msg_id: String,
    /// false：不是群聊，或成员超过上限；user_ids 为空、summary 为 null
    pub available: bool,
    /// 两栏的计数
    pub summary: Option<MessageReadSummaryHttp>,
    pub user_ids: Vec<String>,
    pub next_cursor: String,
    pub has_more: bool,
}

fn summary_to_http(summary: MessageReadSummary) -> MessageReadSummaryHttp {
    MessageReadSummaryHttp {
        server_msg_id: summary.server_msg_id,
        read_count: summary.read_count,
        unread_count: summary.unread_count,
        total: summary.total,
    }
}

/// 会话服务的应答到 HTTP：参数错误 400，拒绝 403，不是成员、会话或消息不存在（名单接口里
/// 不是自己发的消息也是这一类）404，下游不可用或超时 503（可退避重试），其余 500，都不回显
/// 下游文本。
fn read_receipt_failure(err: FlareError) -> GatewayError {
    let (code, reason) = match err.code() {
        Some(ErrorCode::ServiceUnavailable | ErrorCode::OperationTimeout) => {
            tracing::warn!(error = %err, "read receipt downstream unavailable or timed out");
            (ErrorCode::HttpServiceUnavailable, "SERVICE_UNAVAILABLE")
        }
        Some(ErrorCode::InvalidParameter) => (ErrorCode::HttpBadRequest, "INVALID_PARAMETER"),
        Some(ErrorCode::PermissionDenied) => (ErrorCode::HttpForbidden, "FORBIDDEN"),
        // gRPC NOT_FOUND 转回 FlareError 时是 UserNotFound。
        Some(ErrorCode::UserNotFound | ErrorCode::MessageNotFound) => {
            (ErrorCode::HttpNotFound, "NOT_FOUND")
        }
        _ => {
            tracing::warn!(error = %err, "read receipt query failed");
            (
                ErrorCode::HttpInternalServerError,
                "MESSAGE_READ_RECEIPT_FAILED",
            )
        }
    };
    GatewayError(ErrorBuilder::new(code, reason).build_error())
}

/// 群聊里自己发送的一批消息的已读摘要
#[utoipa::path(
    post,
    path = "/api/v1/messages/read-summaries",
    tag = "Message",
    request_body = MessageReadSummariesHttpRequest,
    responses(
        (status = 200, description = "成功；available=false 时不显示已读摘要", body = ApiResponse<MessageReadSummariesHttpResponse>),
        (status = 400, description = "参数错误（INVALID_PARAMETER）：会话 ID 为空，或消息 ID 为空、重复、超过 50 个"),
        (status = 404, description = "调用者不是会话成员（已退出或从未加入），或会话不存在（NOT_FOUND）"),
        (status = 503, description = "下游不可用或超过请求超时（SERVICE_UNAVAILABLE），可退避重试"),
    ),
)]
#[instrument(skip(headers, clients, principal, signer, req))]
pub async fn get_message_read_summaries(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(signer): Extension<MessageContextSigner>,
    Json(req): Json<MessageReadSummariesHttpRequest>,
) -> Result<Json<ApiResponse<MessageReadSummariesHttpResponse>>> {
    let (ctx, token) = conversation_reader(&headers, &principal, &signer)?;
    let grpc_req = GetMessageReadSummariesRequest {
        conversation_id: req.conversation_id,
        server_msg_ids: req.server_msg_ids,
    };
    let response = clients
        .bounded(async {
            let mut client = clients.conversation_read.lock().await;
            client
                .get_message_read_summaries_with_ctx(&ctx, token.as_deref(), grpc_req)
                .await
        })
        .await
        .map_err(read_receipt_failure)?;
    Ok(Json(ApiResponse::success(
        MessageReadSummariesHttpResponse {
            conversation_id: response.conversation_id,
            available: response.available,
            summaries: response
                .summaries
                .into_iter()
                .map(summary_to_http)
                .collect(),
        },
    )))
}

/// 群聊里自己发送的一条消息的已读或未读成员
#[utoipa::path(
    get,
    path = "/api/v1/messages/readers",
    tag = "Message",
    params(ListMessageReadersHttpQuery),
    responses(
        (status = 200, description = "成功；available=false 时不显示名单", body = ApiResponse<ListMessageReadersHttpResponse>),
        (status = 400, description = "参数错误（INVALID_PARAMETER）：state 不是 read 或 unread，limit 超过 200"),
        (status = 404, description = "调用者不是会话成员，或会话不存在，或消息不在本会话、已撤回、已删除，或不是调用者发送的（NOT_FOUND，几种情形不区分）"),
        (status = 503, description = "下游不可用或超过请求超时（SERVICE_UNAVAILABLE），可退避重试"),
    ),
)]
#[instrument(skip(headers, clients, principal, signer, query))]
pub async fn list_message_readers(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(signer): Extension<MessageContextSigner>,
    axum::extract::Query(query): axum::extract::Query<ListMessageReadersHttpQuery>,
) -> Result<Json<ApiResponse<ListMessageReadersHttpResponse>>> {
    let state = match query.state.trim().to_ascii_lowercase().as_str() {
        "read" => MessageReaderState::Read,
        "unread" => MessageReaderState::Unread,
        _ => {
            return Err(GatewayError::bad_request(
                "INVALID_PARAMETER",
                "state must be read or unread",
            ));
        }
    };
    let (ctx, token) = conversation_reader(&headers, &principal, &signer)?;
    let grpc_req = ListMessageReadersRequest {
        conversation_id: query.conversation_id,
        server_msg_id: query.server_msg_id,
        state: state as i32,
        cursor: query.cursor,
        limit: query.limit,
    };
    let response = clients
        .bounded(async {
            let mut client = clients.conversation_read.lock().await;
            client
                .list_message_readers_with_ctx(&ctx, token.as_deref(), grpc_req)
                .await
        })
        .await
        .map_err(read_receipt_failure)?;
    Ok(Json(ApiResponse::success(ListMessageReadersHttpResponse {
        conversation_id: response.conversation_id,
        server_msg_id: response.server_msg_id,
        available: response.available,
        summary: response.summary.map(summary_to_http),
        user_ids: response.user_ids,
        next_cursor: response.next_cursor,
        has_more: response.has_more,
    })))
}
