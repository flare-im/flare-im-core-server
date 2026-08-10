use axum::{
    extract::{Extension, Json},
    http::HeaderMap,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, instrument};

use flare_grpc_proto::conversation::{
    ListConversationParticipantsRequest, ListConversationsRequest, ManageParticipantsRequest,
    ParticipantRoleUpdate,
};
use flare_im_service_kit::clients::GrpcClients;
use flare_server_core::{
    context::Ctx,
    http::{ApiResponse, ContextFromHeaders, HttpApiError as GatewayError, Result},
};

/// 获取会话列表请求
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ListConversationsHttpRequest {
    /// 页码
    #[serde(default = "default_page")]
    pub page: u32,
    /// 每页数量
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    /// Opaque cursor；传入后优先于 page
    #[serde(default)]
    pub cursor: String,
    /// 每页数量；传入后优先于 page_size
    #[serde(default)]
    pub limit: i32,
}

fn default_page() -> u32 {
    1
}
fn default_page_size() -> u32 {
    20
}

/// 会话信息
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ConversationHttpResponse {
    /// 会话 ID
    pub conversation_id: String,
    /// 会话类型
    pub conversation_type: String,
    /// 业务类型
    pub business_type: String,
    /// 展示名
    pub display_name: String,
    /// 消息通道 ID
    pub channel_id: String,
    /// 未读数
    pub unread_count: u32,
    /// 最大消息序号
    pub max_seq: u64,
    /// 成员数
    pub member_count: i32,
    /// 成员版本，客户端可据此判断是否需要拉参与者增量
    pub participant_version: u64,
}

/// 会话列表响应
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ListConversationsHttpResponse {
    /// 会话列表
    pub conversations: Vec<ConversationHttpResponse>,
    /// 下一页游标
    pub next_cursor: String,
    /// 是否还有更多
    pub has_more: bool,
}

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ConversationParticipantHttp {
    pub user_id: String,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default)]
    pub muted: bool,
    #[serde(default)]
    pub pinned: bool,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
    /// 该成员可见的历史下限：只能读到 seq 高于此值的消息；0=不限。
    /// 由业务方（如群历史可见性策略）在加人时决定是否设值，核心只负责执行。
    #[serde(default)]
    pub visible_from_seq: i64,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ListConversationParticipantsHttpRequest {
    pub conversation_id: String,
    #[serde(default)]
    pub cursor: String,
    #[serde(default = "default_participant_limit")]
    pub limit: i32,
    #[serde(default)]
    pub include_removed: bool,
}

fn default_participant_limit() -> i32 {
    200
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ListConversationParticipantsHttpResponse {
    pub conversation_id: String,
    pub participants: Vec<ConversationParticipantHttp>,
    pub next_cursor: String,
    pub has_more: bool,
    pub participant_version: u64,
    pub member_count: i32,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ParticipantRoleUpdateHttp {
    pub user_id: String,
    #[serde(default)]
    pub roles: Vec<String>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ManageParticipantsHttpRequest {
    pub conversation_id: String,
    #[serde(default)]
    pub to_add: Vec<ConversationParticipantHttp>,
    #[serde(default)]
    pub to_remove: Vec<String>,
    #[serde(default)]
    pub role_updates: Vec<ParticipantRoleUpdateHttp>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ManageParticipantsHttpResponse {
    pub participants: Vec<ConversationParticipantHttp>,
}

/// 获取会话列表
#[utoipa::path(
    get,
    path = "/api/v1/conversations",
    tag = "Conversation",
    params(
        ("page" = Option<u32>, Query, description = "页码"),
        ("page_size" = Option<u32>, Query, description = "每页数量"),
    ),
    responses(
        (status = 200, description = "成功", body = ApiResponse<ListConversationsHttpResponse>),
        (status = 400, description = "参数错误"),
    ),
)]
#[instrument(skip(headers, clients))]
pub async fn list_conversations(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    axum::extract::Query(req): axum::extract::Query<ListConversationsHttpRequest>,
) -> Result<Json<ApiResponse<ListConversationsHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(
        trace_id = %ctx.trace_id(),
        user_id = %ctx.user_id().unwrap_or(""),
        page = req.page,
        page_size = req.page_size,
        "Listing conversations"
    );

    let limit = if req.limit > 0 {
        req.limit
    } else {
        req.page_size as i32
    };
    let cursor = if !req.cursor.trim().is_empty() {
        req.cursor
    } else if req.page > 1 {
        ((req.page - 1) * req.page_size).to_string()
    } else {
        String::new()
    };
    let grpc_req = ListConversationsRequest {
        cursor,
        limit,
        order: 0,
    };
    let mut read_client = clients.conversation_read.lock().await;
    let grpc_res = read_client
        .list_conversations_with_ctx(&ctx, grpc_req)
        .await
        .map_err(|err| GatewayError::internal("CONVERSATION_LIST_FAILED", err.to_string()))?;
    let response = ListConversationsHttpResponse {
        conversations: grpc_res
            .conversations
            .into_iter()
            .map(|c| ConversationHttpResponse {
                conversation_id: c.conversation_id,
                conversation_type: c.conversation_type,
                business_type: String::new(),
                display_name: c.display_name,
                channel_id: c.channel_id,
                unread_count: c.unread_count,
                max_seq: c.max_conversation_seq,
                member_count: c.member_count,
                participant_version: c.participant_version,
            })
            .collect(),
        next_cursor: grpc_res.next_cursor,
        has_more: grpc_res.has_more,
    };

    Ok(Json(ApiResponse::success(response)))
}

#[utoipa::path(
    get,
    path = "/api/v1/conversations/participants",
    tag = "Conversation",
    params(
        ("conversation_id" = String, Query, description = "会话 ID"),
        ("cursor" = Option<String>, Query, description = "游标"),
        ("limit" = Option<i32>, Query, description = "数量"),
        ("include_removed" = Option<bool>, Query, description = "是否包含已移除成员"),
    ),
    responses((status = 200, description = "成功", body = ApiResponse<ListConversationParticipantsHttpResponse>)),
)]
#[instrument(skip(headers, clients))]
pub async fn list_conversation_participants(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    axum::extract::Query(req): axum::extract::Query<ListConversationParticipantsHttpRequest>,
) -> Result<Json<ApiResponse<ListConversationParticipantsHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let grpc_req = ListConversationParticipantsRequest {
        conversation_id: req.conversation_id,
        cursor: req.cursor,
        limit: req.limit,
        include_removed: req.include_removed,
        ..Default::default()
    };
    let mut read_client = clients.conversation_read.lock().await;
    let grpc_res = read_client
        .list_conversation_participants_with_ctx(&ctx, grpc_req)
        .await
        .map_err(|err| {
            GatewayError::internal("CONVERSATION_PARTICIPANTS_FAILED", err.to_string())
        })?;
    Ok(Json(ApiResponse::success(
        ListConversationParticipantsHttpResponse {
            conversation_id: grpc_res.conversation_id,
            participants: grpc_res
                .participants
                .into_iter()
                .map(participant_proto_to_http)
                .collect(),
            next_cursor: grpc_res.next_cursor,
            has_more: grpc_res.has_more,
            participant_version: grpc_res.participant_version,
            member_count: grpc_res.member_count,
        },
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/conversations/participants/manage",
    tag = "Conversation",
    request_body = ManageParticipantsHttpRequest,
    responses((status = 200, description = "成功", body = ApiResponse<ManageParticipantsHttpResponse>)),
)]
#[instrument(skip(headers, clients))]
pub async fn manage_participants(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<ManageParticipantsHttpRequest>,
) -> Result<Json<ApiResponse<ManageParticipantsHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let grpc_req = ManageParticipantsRequest {
        conversation_id: req.conversation_id,
        to_add: req
            .to_add
            .into_iter()
            .map(participant_http_to_proto)
            .collect(),
        to_remove: req.to_remove,
        role_updates: req
            .role_updates
            .into_iter()
            .map(|u| ParticipantRoleUpdate {
                user_id: u.user_id,
                roles: u.roles,
            })
            .collect(),
    };
    let mut manage_client = clients.conversation_manage.lock().await;
    let grpc_res = manage_client
        .manage_participants_with_ctx(&ctx, grpc_req)
        .await
        .map_err(|err| {
            GatewayError::internal("CONVERSATION_MANAGE_PARTICIPANTS_FAILED", err.to_string())
        })?;
    Ok(Json(ApiResponse::success(ManageParticipantsHttpResponse {
        participants: grpc_res
            .participants
            .into_iter()
            .map(participant_proto_to_http)
            .collect(),
    })))
}

fn participant_proto_to_http(
    p: flare_proto::common::ConversationParticipant,
) -> ConversationParticipantHttp {
    ConversationParticipantHttp {
        user_id: p.user_id,
        roles: p.roles,
        muted: p.muted,
        pinned: p.pinned,
        attributes: p.attributes,
        visible_from_seq: p.visible_from_seq,
    }
}

fn participant_http_to_proto(
    p: ConversationParticipantHttp,
) -> flare_proto::common::ConversationParticipant {
    flare_proto::common::ConversationParticipant {
        user_id: p.user_id,
        roles: p.roles,
        muted: p.muted,
        pinned: p.pinned,
        attributes: p.attributes,
        joined_at: 0,
        visible_from_seq: p.visible_from_seq,
    }
}
