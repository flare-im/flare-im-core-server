use axum::{
    Json,
    extract::{Extension, Path},
    http::HeaderMap,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, instrument};

use flare_grpc_proto::signaling::online::{
    BatchGetUserPresenceRequest, GetUserPresenceRequest, LogoutRequest, UserPresence,
};
use flare_im_service_kit::clients::GrpcClients;
use flare_server_core::{
    context::Ctx,
    http::{ApiResponse, ContextFromHeaders, HttpApiError as GatewayError, Result},
};

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DevicePresenceHttp {
    pub device_id: String,
    pub platform: String,
    pub last_active_time_ms: i64,
    pub conversation_id: String,
    pub gateway_id: String,
    pub server_id: String,
}

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UserPresenceHttp {
    pub user_id: String,
    pub is_online: bool,
    pub status: String,
    pub last_seen_ms: i64,
    pub devices: Vec<DevicePresenceHttp>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct BatchGetUserPresenceHttpRequest {
    pub user_ids: Vec<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BatchGetUserPresenceHttpResponse {
    pub presences: HashMap<String, UserPresenceHttp>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct LogoutPresenceHttpRequest {
    #[serde(default)]
    pub conversation_id: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct LogoutPresenceHttpResponse {
    pub success: bool,
}

fn timestamp_ms(ts: Option<prost_types::Timestamp>) -> i64 {
    ts.map(|t| (t.seconds * 1000) + i64::from(t.nanos / 1_000_000))
        .unwrap_or(0)
}

fn user_presence_to_http(p: UserPresence) -> UserPresenceHttp {
    let is_online = p.is_online;
    UserPresenceHttp {
        user_id: p.user_id,
        is_online,
        status: if is_online {
            "online".to_string()
        } else {
            "offline".to_string()
        },
        last_seen_ms: timestamp_ms(p.last_seen),
        devices: p
            .devices
            .into_iter()
            .map(|d| DevicePresenceHttp {
                device_id: d.device_id,
                platform: d.platform,
                last_active_time_ms: timestamp_ms(d.last_active_time),
                conversation_id: d.conversation_id,
                gateway_id: d.gateway_id,
                server_id: d.server_id,
            })
            .collect(),
    }
}

/// 查询单个用户在线状态
#[utoipa::path(
    get,
    path = "/api/v1/presence/users/{user_id}",
    tag = "Presence",
    responses(
        (status = 200, description = "成功", body = ApiResponse<UserPresenceHttp>),
        (status = 400, description = "参数错误"),
    )
)]
#[instrument(skip(headers, clients))]
pub async fn get_user_presence(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Path(user_id): Path<String>,
) -> Result<Json<ApiResponse<UserPresenceHttp>>> {
    let ctx = Ctx::from_headers(&headers);
    let uid = user_id.trim();
    if uid.is_empty() {
        return Err(GatewayError::bad_request(
            "USER_ID_REQUIRED",
            "user_id required",
        ));
    }
    debug!(trace_id = %ctx.trace_id(), user_id = %uid, "GetUserPresence");

    let grpc_req = GetUserPresenceRequest {
        user_id: uid.to_string(),
    };
    let mut client = clients.online.lock().await;
    let resp = client
        .get_user_presence_with_ctx(&ctx, grpc_req)
        .await
        .map_err(|e| GatewayError::internal("GET_USER_PRESENCE_FAILED", e.to_string()))?;
    let presence = resp
        .presence
        .map(user_presence_to_http)
        .ok_or_else(|| GatewayError::bad_request("PRESENCE_NOT_FOUND", "presence not found"))?;
    Ok(Json(ApiResponse::success(presence)))
}

/// 批量查询用户在线状态
#[utoipa::path(
    post,
    path = "/api/v1/presence/users/batch",
    tag = "Presence",
    request_body = BatchGetUserPresenceHttpRequest,
    responses(
        (status = 200, description = "成功", body = ApiResponse<BatchGetUserPresenceHttpResponse>),
        (status = 400, description = "参数错误"),
    )
)]
#[instrument(skip(headers, clients, req))]
pub async fn batch_get_user_presence(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<BatchGetUserPresenceHttpRequest>,
) -> Result<Json<ApiResponse<BatchGetUserPresenceHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let ids: Vec<String> = req
        .user_ids
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if ids.is_empty() {
        return Ok(Json(ApiResponse::success(
            BatchGetUserPresenceHttpResponse {
                presences: HashMap::new(),
            },
        )));
    }
    debug!(trace_id = %ctx.trace_id(), count = ids.len(), "BatchGetUserPresence");

    let grpc_req = BatchGetUserPresenceRequest { user_ids: ids };
    let mut client = clients.online.lock().await;
    let resp = client
        .batch_get_user_presence_with_ctx(&ctx, grpc_req)
        .await
        .map_err(|e| GatewayError::internal("BATCH_GET_USER_PRESENCE_FAILED", e.to_string()))?;
    let presences = resp
        .presences
        .into_iter()
        .map(|(user_id, p)| (user_id, user_presence_to_http(p)))
        .collect();
    Ok(Json(ApiResponse::success(
        BatchGetUserPresenceHttpResponse { presences },
    )))
}

/// 登出当前 Online 会话（需认证上下文 user_id + conversation_id）
#[utoipa::path(
    post,
    path = "/api/v1/presence/logout",
    tag = "Presence",
    request_body = LogoutPresenceHttpRequest,
    responses(
        (status = 200, description = "成功", body = ApiResponse<LogoutPresenceHttpResponse>),
        (status = 400, description = "参数错误"),
    )
)]
#[instrument(skip(headers, clients, req))]
pub async fn logout_presence(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<LogoutPresenceHttpRequest>,
) -> Result<Json<ApiResponse<LogoutPresenceHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let user_id = ctx
        .require_user_id()
        .map_err(|e| GatewayError::bad_request("USER_REQUIRED", e))?
        .to_string();
    let conversation_id = req.conversation_id.trim();
    if conversation_id.is_empty() {
        return Err(GatewayError::bad_request(
            "CONVERSATION_ID_REQUIRED",
            "conversation_id required",
        ));
    }
    debug!(
        trace_id = %ctx.trace_id(),
        user_id = %user_id,
        conversation_id = %conversation_id,
        "LogoutPresence"
    );

    let grpc_req = LogoutRequest {
        user_id,
        conversation_id: conversation_id.to_string(),
    };
    let mut client = clients.online.lock().await;
    let resp = client
        .logout_with_ctx(&ctx, grpc_req)
        .await
        .map_err(|e| GatewayError::internal("LOGOUT_PRESENCE_FAILED", e.to_string()))?;
    Ok(Json(ApiResponse::success(LogoutPresenceHttpResponse {
        success: resp.success,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_grpc_proto::signaling::online::{DeviceInfo, UserPresence};

    #[test]
    fn user_presence_to_http_maps_online_status() {
        let http = user_presence_to_http(UserPresence {
            user_id: "alice".into(),
            is_online: true,
            last_seen: None,
            devices: vec![DeviceInfo {
                device_id: "d1".into(),
                platform: "web".into(),
                conversation_id: "c1".into(),
                gateway_id: "gw1".into(),
                server_id: "srv1".into(),
                ..Default::default()
            }],
        });
        assert!(http.is_online);
        assert_eq!(http.status, "online");
        assert_eq!(http.user_id, "alice");
        assert_eq!(http.devices.len(), 1);
        assert_eq!(http.devices[0].platform, "web");
    }

    #[test]
    fn user_presence_to_http_maps_offline_status() {
        let http = user_presence_to_http(UserPresence {
            user_id: "bob".into(),
            is_online: false,
            last_seen: None,
            devices: vec![],
        });
        assert!(!http.is_online);
        assert_eq!(http.status, "offline");
        assert_eq!(http.devices.len(), 0);
    }
}
