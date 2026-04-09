use axum::{
    extract::{Extension, Json},
    http::HeaderMap,
};
use std::sync::Arc;
use tracing::{info, instrument};

use crate::context::Ctx;
use crate::error::Result;
use crate::infrastructure::grpc::GrpcClients;
use flare_server_core::http::ApiResponse;

/// 获取会话列表请求
#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct ListConversationsHttpRequest {
    /// 用户 ID
    pub user_id: String,
    /// 页码
    #[serde(default = "default_page")]
    pub page: u32,
    /// 每页数量
    #[serde(default = "default_page_size")]
    pub page_size: u32,
}

fn default_page() -> u32 { 1 }
fn default_page_size() -> u32 { 20 }

/// 会话信息
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct ConversationHttpResponse {
    /// 会话 ID
    pub conversation_id: String,
    /// 会话类型
    pub conversation_type: i32,
    /// 未读数
    pub unread_count: u32,
}

/// 会话列表响应
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct ListConversationsHttpResponse {
    /// 会话列表
    pub conversations: Vec<ConversationHttpResponse>,
    /// 总数
    pub total: u32,
}

/// 获取会话列表
#[utoipa::path(
    get,
    path = "/api/v1/conversations",
    tag = "Conversation",
    params(
        ("user_id" = String, Query, description = "用户 ID"),
        ("page" = Option<u32>, Query, description = "页码"),
        ("page_size" = Option<u32>, Query, description = "每页数量"),
    ),
    responses(
        (status = 200, description = "成功", body = ApiResponse<ListConversationsHttpResponse>),
        (status = 400, description = "参数错误"),
    ),
)]
#[instrument(skip(headers, _clients))]
pub async fn list_conversations(
    headers: HeaderMap,
    Extension(_clients): Extension<Arc<GrpcClients>>,
    axum::extract::Query(req): axum::extract::Query<ListConversationsHttpRequest>,
) -> Result<Json<ApiResponse<ListConversationsHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    info!(
        trace_id = %ctx.trace_id,
        user_id = %req.user_id,
        page = req.page,
        page_size = req.page_size,
        "Listing conversations"
    );

    // TODO: 实现 gRPC 调用
    let response = ListConversationsHttpResponse {
        conversations: vec![],
        total: 0,
    };

    Ok(Json(ApiResponse::success(response)))
}
