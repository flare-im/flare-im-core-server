//! 推送到达回执：设备收到推送后（iOS 通知扩展或应用进程）把一批回执报上来，网关转给 Push Proxy。
//!
//! 与推送令牌接口同一套身份规则：用户与租户只取自已验证的 IM 令牌，网关按该身份现签一枚 User
//! 内部上下文；令牌绑定了设备时只能替这台设备上报。请求体拒收未知字段，身份字段写不进来。
//! 回执只进指标与推送决策链：形状不对的条目跳过，不让整批失败；取值（通道、来路、版本）由
//! Push Proxy 校验，网关与 Proxy 不维护两份规则。

use axum::{
    Json,
    extract::{Extension, rejection::JsonRejection},
    http::HeaderMap,
};
use flare_grpc_proto::push::{PushReceipt, ReportPushReceiptsRequest};
use flare_server_core::{
    AuthenticatedPrincipal,
    error::{ErrorBuilder, ErrorCode},
    http::{ApiResponse, HttpApiError as GatewayError, Result},
};
use serde::{Deserialize, Serialize};
use tonic::Code;
use tracing::{debug, instrument, warn};

use super::MessageContextSigner;
use super::push_token_handler::{PushTokenClient, device_id, receipt_caller};

/// 一次至多报多少条回执。
pub(super) const MAX_RECEIPTS_PER_REPORT: usize = 100;

/// 请求体上限：100 条回执每条几百字节，留足余量。
pub(super) const RECEIPTS_BODY_LIMIT: usize = 64 * 1024;

/// 一台设备的一批到达回执。
#[derive(Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReportPushReceiptsHttpRequest {
    /// 上报的设备。令牌绑定了设备时必须是它。
    pub device_id: String,
    /// 应用版本：至多 32 个 `[0-9A-Za-z._+-]` 字符，其余记为 `other`。
    #[serde(default)]
    pub app_version: Option<String>,
    /// 至多 100 条；形状不对的条目跳过。
    #[schema(value_type = Vec<PushReceiptHttp>)]
    pub receipts: Vec<serde_json::Value>,
}

/// 一条到达回执：收到的那条推送载荷里的标识，加上通道、来路与设备时钟的收到时刻。
#[derive(Debug, Default, PartialEq, Eq, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PushReceiptHttp {
    #[serde(default)]
    pub conversation_id: String,
    #[serde(default)]
    pub message_id: String,
    #[serde(default)]
    pub seq: u64,
    #[serde(default)]
    pub collapse_id: String,
    /// 载荷里的处置：`loud`、`quiet`、`clear`、`badge_only`、`recall_replace`、`digest`。
    #[serde(default)]
    pub disposition: String,
    /// `apns`、`fcm` 或 `getui`。
    #[serde(default)]
    pub provider: String,
    /// `extension`（iOS 通知扩展）或 `app`（应用进程处理了这条推送）。
    #[serde(default)]
    pub path: String,
    /// 设备时钟的收到时刻（Unix 毫秒）。
    #[serde(default)]
    pub received_at_ms: i64,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReportPushReceiptsHttpResponse {
    /// 记下的回执条数。
    pub accepted: u32,
}

/// 能按形状读出来的条目；读不出来的跳过。
fn receipts_of(raw: Vec<serde_json::Value>) -> Vec<PushReceipt> {
    raw.into_iter()
        .filter_map(|value| serde_json::from_value::<PushReceiptHttp>(value).ok())
        .map(|receipt| PushReceipt {
            conversation_id: receipt.conversation_id.trim().to_string(),
            message_id: receipt.message_id.trim().to_string(),
            seq: receipt.seq,
            collapse_id: receipt.collapse_id.trim().to_string(),
            disposition: receipt.disposition.trim().to_string(),
            provider: receipt.provider.trim().to_ascii_lowercase(),
            path: receipt.path.trim().to_ascii_lowercase(),
            received_at_ms: receipt.received_at_ms,
        })
        .collect()
}

/// Push Proxy 的 gRPC 状态到 HTTP 信封。与推送令牌接口相同：只有参数与权限类错误把原因告诉调用方；
/// 下游 `Unauthenticated` 是网关自己的内部签名被拒，报 502，客户端不该据此刷新令牌。
fn receipt_proxy_error(status: tonic::Status) -> GatewayError {
    let (code, reason) = match status.code() {
        Code::InvalidArgument => (ErrorCode::HttpBadRequest, "INVALID_ARGUMENT"),
        Code::PermissionDenied => (ErrorCode::HttpForbidden, "PERMISSION_DENIED"),
        Code::ResourceExhausted => (ErrorCode::HttpTooManyRequests, "RESOURCE_EXHAUSTED"),
        Code::Unavailable => (ErrorCode::HttpServiceUnavailable, "UPSTREAM_UNAVAILABLE"),
        Code::DeadlineExceeded => (ErrorCode::HttpGatewayTimeout, "UPSTREAM_TIMEOUT"),
        _ => (ErrorCode::HttpBadGateway, "GRPC_ERROR"),
    };
    warn!(
        grpc_code = ?status.code(),
        "push proxy rejected a push receipt report"
    );
    GatewayError(ErrorBuilder::new(code, reason).build_error())
}

/// 上报一批推送到达回执
#[utoipa::path(
    post,
    path = "/api/v1/push/receipts",
    tag = "Device",
    request_body = ReportPushReceiptsHttpRequest,
    responses(
        (status = 200, description = "成功；accepted 是记下的条数，形状不对或取值认不出的条目跳过", body = ApiResponse<ReportPushReceiptsHttpResponse>),
        (status = 400, description = "请求体不合法、包含未知字段、设备 ID 无效或超过 100 条"),
        (status = 401, description = "缺少或无效的 IM 令牌"),
        (status = 403, description = "令牌未绑定租户、是管理员令牌或绑定了其他设备"),
        (status = 503, description = "Push Proxy 不可用"),
    )
)]
#[instrument(skip_all)]
pub async fn report_push_receipts(
    headers: HeaderMap,
    Extension(client): Extension<PushTokenClient>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(signer): Extension<MessageContextSigner>,
    body: std::result::Result<Json<ReportPushReceiptsHttpRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<ReportPushReceiptsHttpResponse>>> {
    let Json(body) = body.map_err(|_| {
        GatewayError::bad_request("INVALID_REQUEST_BODY", "push receipt body rejected")
    })?;
    let device_id = device_id(&body.device_id)?;
    let caller = receipt_caller(&headers, &principal, &signer, &device_id)?;
    if body.receipts.len() > MAX_RECEIPTS_PER_REPORT {
        return Err(GatewayError::bad_request(
            "TOO_MANY_RECEIPTS",
            "at most 100 receipts per report",
        ));
    }
    let receipts = receipts_of(body.receipts);
    if receipts.is_empty() {
        return Ok(Json(ApiResponse::success(ReportPushReceiptsHttpResponse {
            accepted: 0,
        })));
    }
    debug!(
        trace_id = %caller.ctx.trace_id(),
        device_id = %device_id,
        receipts = receipts.len(),
        "ReportPushReceipts"
    );
    let request = caller.request(ReportPushReceiptsRequest {
        tenant_id: caller.tenant_id.clone(),
        user_id: caller.user_id.clone(),
        device_id,
        app_version: body
            .app_version
            .map(|version| version.trim().to_string())
            .unwrap_or_default(),
        receipts,
    })?;
    let mut push_proxy = client.client().await?;
    let response = client
        .call(|| push_proxy.report_push_receipts(request))
        .await
        .map_err(receipt_proxy_error)?
        .into_inner();
    Ok(Json(ApiResponse::success(ReportPushReceiptsHttpResponse {
        accepted: response.accepted,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn entries_with_the_wrong_shape_are_skipped_and_values_trimmed() {
        let receipts = receipts_of(vec![
            json!({"conversationId": " c-1 ", "messageId": "m-9", "seq": 3, "provider": " APNS ", "path": "Extension", "receivedAtMs": 5}),
            json!({"conversationId": "c-1", "seq": "three"}),
            json!("not an object"),
            json!({"messageId": "m-10", "provider": "fcm", "path": "app", "extra": true}),
        ]);
        assert_eq!(receipts.len(), 2);
        assert_eq!(receipts[0].conversation_id, "c-1");
        assert_eq!(receipts[0].provider, "apns");
        assert_eq!(receipts[0].path, "extension");
        assert_eq!(receipts[0].received_at_ms, 5);
        assert_eq!(receipts[1].message_id, "m-10");
        assert_eq!(receipts[1].seq, 0);
    }

    #[test]
    fn downstream_identity_rejection_is_a_bad_gateway() {
        let status =
            |error: GatewayError| axum::response::IntoResponse::into_response(error).status();
        assert_eq!(
            status(receipt_proxy_error(tonic::Status::unauthenticated(
                "bad sig"
            ))),
            axum::http::StatusCode::BAD_GATEWAY
        );
        assert_eq!(
            status(receipt_proxy_error(tonic::Status::invalid_argument("x"))),
            axum::http::StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status(receipt_proxy_error(tonic::Status::unavailable("x"))),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
