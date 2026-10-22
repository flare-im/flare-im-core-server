use axum::{
    extract::{Extension, Json},
    http::{HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
};
use std::sync::Arc;
use tracing::{debug, instrument};

use super::downstream::downstream_failure;
use crate::application::dto::{
    ExecuteCustomEventHttpRequest, ExecuteCustomEventHttpResponse, RecallMessageHttpRequest,
    RecallMessageHttpResponse, SendMessageHttpRequest, SendMessageHttpResponse,
};
use flare_core_base::error::{ErrorBuilder, ErrorCode, FlareError};
use flare_grpc_proto::message::{ExecuteEventRequest, RecallMessageRequest, SendMessageRequest};
use flare_im_service_kit::clients::GrpcClients;
use flare_proto::common::{
    CustomContent, CustomEvent, Event, EventType, Message, MessageContent, MessageSource,
    MessageStatus, event, message_content,
};
use flare_server_core::{
    AuthenticatedPrincipal,
    auth::{SignedActorType, SignedContextIdentity, SignedContextSigner},
    context::Ctx,
    http::{ApiResponse, ContextFromHeaders, HttpApiError as GatewayError, Result},
};

const TENANT_UNAVAILABLE: &str = "TENANT_UNAVAILABLE";
const MESSAGE_REJECTED: &str = "MESSAGE_REJECTED";
const INVALID_MESSAGE: &str = "INVALID_MESSAGE";
const NOT_FOUND: &str = "NOT_FOUND";

/// HTTP 发送把这次实际使用的 `client_msg_id` 放进这个响应头，成功与失败都带。
pub(crate) const CLIENT_MSG_ID_HEADER: &str = "x-client-msg-id";
/// 超过这个长度（或含非可见 ASCII 字符）的 `client_msg_id` 不进响应头、只在响应体里返回：
/// 过长的响应头会被 Edge 当成上游错误。
const CLIENT_MSG_ID_HEADER_MAX_LEN: usize = 256;

/// Present only to the API gateway ingress process; no downstream Core worker
/// receives this private signing capability.
#[derive(Clone)]
pub(crate) struct MessageContextSigner(pub Option<SignedContextSigner>);

/// 发送消息
#[utoipa::path(
    post,
    path = "/api/v1/messages/send",
    tag = "Message",
    request_body = SendMessageHttpRequest,
    responses(
        (status = 200, description = "成功；data.client_msg_id 是这次实际使用的 client_msg_id", body = ApiResponse<SendMessageHttpResponse>,
            headers(("x-client-msg-id" = String, description = "这次实际使用的 client_msg_id"))),
        (status = 400, description = "参数错误，或摄入侧判消息不合法（INVALID_MESSAGE），改了请求再发；顶层 client_msg_id 同响应头",
            headers(("x-client-msg-id" = String, description = "这次实际使用的 client_msg_id"))),
        (status = 403, description = "租户不可用（TENANT_UNAVAILABLE）或无权发送；顶层 client_msg_id 同响应头",
            headers(("x-client-msg-id" = String, description = "这次实际使用的 client_msg_id"))),
        (status = 404, description = "摄入侧找不到发送依赖的对象（NOT_FOUND）；顶层 client_msg_id 同响应头",
            headers(("x-client-msg-id" = String, description = "这次实际使用的 client_msg_id"))),
        (status = 422, description = "PreSend 扩展按策略拒绝了这条消息（MESSAGE_REJECTED），不要原样重试；顶层 client_msg_id 同响应头",
            headers(("x-client-msg-id" = String, description = "这次实际使用的 client_msg_id"))),
        (status = 500, description = "发送失败（MESSAGE_SEND_FAILED），不带下游错误原文；顶层 client_msg_id 同响应头",
            headers(("x-client-msg-id" = String, description = "这次实际使用的 client_msg_id"))),
        (status = 503, description = "暂时无法受理（SERVICE_UNAVAILABLE；含必需 PreSend Hook 不可达或超时、摄入超时），带同一 client_msg_id（请求里没带时取应答里的）退避重试",
            headers(("x-client-msg-id" = String, description = "这次实际使用的 client_msg_id"))),
    ),
)]
#[instrument(skip(headers, clients, principal, signer))]
pub async fn send_message(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(signer): Extension<MessageContextSigner>,
    Json(req): Json<SendMessageHttpRequest>,
) -> Response {
    // 调用方没带 client_msg_id 时网关生成一个。不论成败都把它回给调用方：以前只有成功路径看得到，
    // 503 之后的盲重试只能再生成一个新 id，摄入侧无从去重。调用方应自带 client_msg_id；没带的，
    // 重试时带回应答里的这个。
    let client_msg_id = effective_client_msg_id(&req.client_msg_id);
    let outcome = send_message_with_id(
        &headers,
        &clients,
        &principal,
        &signer,
        req,
        client_msg_id.clone(),
    )
    .await;
    let mut response = match outcome {
        Ok(response) => Json(ApiResponse::success(response)).into_response(),
        Err(error) => error_with_client_msg_id(error, &client_msg_id),
    };
    if let Some(value) = client_msg_id_header(&client_msg_id) {
        response.headers_mut().insert(CLIENT_MSG_ID_HEADER, value);
    }
    response
}

/// 请求里的 `client_msg_id`（原样）；空白时生成 `http_<uuid>`。
fn effective_client_msg_id(requested: &str) -> String {
    if requested.trim().is_empty() {
        format!("http_{}", uuid::Uuid::new_v4())
    } else {
        requested.to_string()
    }
}

/// 只有不太长、全是可见 ASCII 的 id 才进响应头；其余只在响应体里返回。
fn client_msg_id_header(client_msg_id: &str) -> Option<HeaderValue> {
    if client_msg_id.len() > CLIENT_MSG_ID_HEADER_MAX_LEN
        || !client_msg_id.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return None;
    }
    HeaderValue::from_str(client_msg_id).ok()
}

/// 错误应答沿用统一错误体（`code`、`reason`、`message`、`track`），顶层加可选字段
/// `client_msg_id`；状态码与不带它时相同。
fn error_with_client_msg_id(error: GatewayError, client_msg_id: &str) -> Response {
    let body = serde_json::to_value(ApiResponse::<()>::from(error.0.clone()));
    let status = error.clone().into_response().status();
    match body {
        Ok(serde_json::Value::Object(mut fields)) => {
            fields.insert(
                "client_msg_id".to_string(),
                serde_json::Value::String(client_msg_id.to_string()),
            );
            (status, Json(serde_json::Value::Object(fields))).into_response()
        }
        _ => error.into_response(),
    }
}

async fn send_message_with_id(
    headers: &HeaderMap,
    clients: &GrpcClients,
    principal: &AuthenticatedPrincipal,
    signer: &MessageContextSigner,
    req: SendMessageHttpRequest,
    client_msg_id: String,
) -> Result<SendMessageHttpResponse> {
    let ctx = Ctx::from_headers(headers);
    debug!(
        trace_id = %ctx.trace_id(),
        conversation_id = %req.conversation_id,
        message_type = req.message_type,
        "Sending message"
    );

    let sender_id = principal.user_id.trim().to_string();
    let tenant_id = principal
        .tenant_id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| {
            GatewayError::bad_request("TENANT_REQUIRED", "authenticated tenant required")
        })?;
    if sender_id.is_empty() || principal.has_gateway_admin_scope() {
        return Err(GatewayError::bad_request(
            "USER_REQUIRED",
            "authenticated user required",
        ));
    }
    let signer = signer.0.as_ref().ok_or_else(|| {
        GatewayError::internal(
            "MESSAGE_SIGNER_UNAVAILABLE",
            "internal identity signer not configured",
        )
    })?;
    let signed_identity = SignedContextIdentity::new(
        SignedActorType::User,
        &sender_id,
        if tenant_id == "default" {
            "0"
        } else {
            tenant_id
        },
        uuid::Uuid::new_v4().to_string(),
    );
    let signed_token = signer.sign(&signed_identity).map_err(|_| {
        GatewayError::internal("MESSAGE_SIGNING_FAILED", "internal identity signing failed")
    })?;
    let content = serde_json::to_vec(&req.content)?;
    let message = Message {
        server_id: String::new(),
        conversation_id: req.conversation_id.clone(),
        client_msg_id: client_msg_id.clone(),
        sender_id,
        source: MessageSource::User as i32,
        conversation_seq: 0,
        created_at: 0,
        conversation_type: req.conversation_type,
        message_type: req.message_type,
        message_seq: None,
        channel_id: req.channel_id,
        thread_id: None,
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

    let grpc_res = clients
        .bounded(async {
            let mut message_client = clients.message_ingest_send.lock().await;
            message_client
                .send_message_with_signed_context(grpc_req, &signed_token)
                .await
        })
        .await
        .map_err(send_status_failure)?;
    Ok(SendMessageHttpResponse {
        server_msg_id: grpc_res.server_msg_id,
        seq: grpc_res.conversation_seq,
        success: grpc_res.success,
        client_msg_id,
    })
}

/// MessageIngest 回的 gRPC 状态映射为 HTTP。
///
/// 发送方自己可以纠正或预期的结果按类型回 4xx，说明只取摄入侧结构化 `ErrorDetail.reason`
/// （摄入侧自己写的固定原因；`Status.message` 与 `details` 里可能带内部文本，不回显）：
/// - `FAILED_PRECONDITION`：PreSend 扩展的策略拒绝（摄入侧只对它回这个状态码）→ 422
///   `MESSAGE_REJECTED`，不要原样重试。
/// - `INVALID_ARGUMENT`：摄入侧判消息本身不合法（缺消息体、校验不过、阅后即焚参数不对等）→ 400
///   `INVALID_MESSAGE`，改了请求再发。
/// - `NOT_FOUND`：摄入侧找不到发送依赖的对象 → 404 `NOT_FOUND`。
///
/// 以前这三种都被并进未知错误，回带原文的 500 `MESSAGE_SEND_FAILED`。其余状态见 [`send_failure`]。
fn send_status_failure(status: tonic::Status) -> GatewayError {
    let (code, reason) = match status.code() {
        tonic::Code::FailedPrecondition => (ErrorCode::HttpUnprocessableEntity, MESSAGE_REJECTED),
        tonic::Code::InvalidArgument => (ErrorCode::HttpBadRequest, INVALID_MESSAGE),
        tonic::Code::NotFound => (ErrorCode::HttpNotFound, NOT_FOUND),
        _ => return send_failure(FlareError::from(status)),
    };
    let mut error = ErrorBuilder::new(code, reason);
    if let Some(detail) = ingest_detail_reason(&status) {
        error = error.details(detail);
    }
    GatewayError(error.build_error())
}

/// 摄入侧结构化 `ErrorDetail` 里的原因；没有结构化详情（例如纯文本的 `Status`）时为 `None`。
fn ingest_detail_reason(status: &tonic::Status) -> Option<String> {
    flare_server_core::error::grpc::decode_error_detail(status)
        .map(|detail| detail.reason)
        .filter(|reason| !reason.trim().is_empty())
}

/// MessageIngest 拒绝映射为 HTTP。摄入侧有自己的租户闸门：租户刚停用时它的缓存可能比本网关先看到，
/// 答 `PERMISSION_DENIED TENANT_UNAVAILABLE`。那和网关自己的拒绝是同一回事，要回确定的 403，不能当 500。
/// 摄入侧暂时放不了行（严格策略下租户投影读不到、同一条消息正在处理、摄入不可达、必需的 PreSend
/// Hook 不可达或超时）答 `SERVICE_UNAVAILABLE`：回 503，调用方退避重试；不能回 403，否则会被当成租户
/// 被停用。摄入侧按时限放弃（`DEADLINE_EXCEEDED`，如阶段超时、旧版摄入的 Hook 超时）同样回 503：
/// 重试带同一个 `client_msg_id`，摄入侧按它去重。
/// 其余是 500 `MESSAGE_SEND_FAILED`。内部文本一律不回显：500 只带固定原因，原文只进本网关日志。
fn send_failure(err: FlareError) -> GatewayError {
    if matches!(
        err.code(),
        Some(ErrorCode::ServiceUnavailable | ErrorCode::OperationTimeout)
    ) {
        return GatewayError(
            ErrorBuilder::new(ErrorCode::HttpServiceUnavailable, "SERVICE_UNAVAILABLE")
                .build_error(),
        );
    }
    if err.code() != Some(ErrorCode::PermissionDenied) {
        tracing::warn!(error = %err, "message ingest failed the send");
        return GatewayError(
            ErrorBuilder::new(ErrorCode::HttpInternalServerError, "MESSAGE_SEND_FAILED")
                .build_error(),
        );
    }
    let error = if err.reason().contains(TENANT_UNAVAILABLE) {
        ErrorBuilder::new(ErrorCode::HttpForbidden, TENANT_UNAVAILABLE).build_error()
    } else {
        ErrorBuilder::new(ErrorCode::HttpForbidden, "FORBIDDEN")
            .details(err.reason())
            .build_error()
    };
    GatewayError(error)
}

/// 执行业务自定义事件
#[utoipa::path(
    post,
    path = "/api/v1/messages/events/custom",
    tag = "Message",
    request_body = ExecuteCustomEventHttpRequest,
    responses(
        (status = 200, description = "成功", body = ApiResponse<ExecuteCustomEventHttpResponse>),
        (status = 400, description = "参数错误"),
        (status = 503, description = "下游不可用或超过请求超时（reason=SERVICE_UNAVAILABLE），可退避重试"),
    ),
)]
#[instrument(skip(headers, clients))]
pub async fn execute_custom_event(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<ExecuteCustomEventHttpRequest>,
) -> Result<Json<ApiResponse<ExecuteCustomEventHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    ctx.require_user_id()
        .map_err(|e| GatewayError::bad_request("USER_REQUIRED", e))?;

    let (event_id, grpc_req) = build_execute_custom_event_request(&ctx, req)?;
    debug!(
        trace_id = %ctx.trace_id(),
        event_id = %event_id,
        "Executing custom message event"
    );

    clients
        .bounded(async {
            let mut event_client = clients.message_event.lock().await;
            event_client.execute_event_with_ctx(&ctx, grpc_req).await
        })
        .await
        .map_err(|err| downstream_failure("MESSAGE_EVENT_EXECUTE_FAILED", err))?;

    Ok(Json(ApiResponse::success(ExecuteCustomEventHttpResponse {
        event_id,
        success: true,
    })))
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
        (status = 503, description = "下游不可用或超过请求超时（reason=SERVICE_UNAVAILABLE），可退避重试"),
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
    let grpc_res = clients
        .bounded(async {
            let mut action_client = clients.message_action.lock().await;
            action_client.recall_message_with_ctx(&ctx, grpc_req).await
        })
        .await
        .map_err(|err| downstream_failure("MESSAGE_RECALL_FAILED", err))?;
    let response = RecallMessageHttpResponse {
        success: grpc_res.success,
    };

    Ok(Json(ApiResponse::success(response)))
}

fn build_execute_custom_event_request(
    ctx: &Ctx,
    req: ExecuteCustomEventHttpRequest,
) -> Result<(String, ExecuteEventRequest)> {
    let conversation_id = require_non_empty(req.conversation_id, "conversation_id")?;
    let namespace = require_non_empty(req.namespace, "namespace")?;
    let name = require_non_empty(req.name, "name")?;
    let version = if req.version.trim().is_empty() {
        "v1".to_string()
    } else {
        req.version.trim().to_string()
    };
    let event_id = if req.event_id.trim().is_empty() {
        format!("http_event_{}", uuid::Uuid::new_v4())
    } else {
        req.event_id.trim().to_string()
    };
    let request_id = if ctx.request_id().trim().is_empty() {
        None
    } else {
        Some(ctx.request_id().to_string())
    };

    let payload = serde_json::to_vec(&req.payload)?;
    let event = Event {
        conversation_id,
        conversation_seq: 0,
        r#type: EventType::EventCustom as i32,
        created_at: now_millis(),
        event_id: event_id.clone(),
        request_id,
        payload: Some(event::Payload::Custom(CustomEvent {
            namespace,
            name,
            version,
            payload,
            attributes: req.attributes,
        })),
    };

    Ok((
        event_id,
        ExecuteEventRequest {
            svid: req.svid,
            event: Some(event),
        },
    ))
}

fn require_non_empty(value: String, field: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(GatewayError::bad_request(
            "INVALID_MESSAGE_EVENT",
            format!("{field} is required"),
        ));
    }
    Ok(value.to_string())
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::event;
    use flare_server_core::context::Context;
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn custom_event_request_builds_event_service_command() {
        let ctx = Arc::new(
            Context::with_request_id("req-1")
                .with_tenant_id("tenant-1")
                .with_user_id("alice"),
        );
        let (event_id, request) = build_execute_custom_event_request(
            &ctx,
            ExecuteCustomEventHttpRequest {
                conversation_id: " group:g1 ".to_string(),
                namespace: "mall".to_string(),
                name: "order_paid".to_string(),
                version: String::new(),
                payload: json!({"order_id": "o1"}),
                attributes: std::collections::HashMap::from([(
                    "source".to_string(),
                    "gateway".to_string(),
                )]),
                event_id: "evt-1".to_string(),
                svid: "mall-service".to_string(),
            },
        )
        .expect("custom event request");

        assert_eq!(event_id, "evt-1");
        assert_eq!(request.svid, "mall-service");
        let event = request.event.expect("event");
        assert_eq!(event.conversation_id, "group:g1");
        assert_eq!(event.r#type, EventType::EventCustom as i32);
        assert_eq!(event.request_id.as_deref(), Some("req-1"));
        match event.payload.expect("payload") {
            event::Payload::Custom(custom) => {
                assert_eq!(custom.namespace, "mall");
                assert_eq!(custom.name, "order_paid");
                assert_eq!(custom.version, "v1");
                assert_eq!(
                    custom.attributes.get("source").map(String::as_str),
                    Some("gateway")
                );
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&custom.payload)
                        .expect("json payload"),
                    json!({"order_id": "o1"})
                );
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    /// 摄入侧租户闸门的拒绝经 gRPC 回到网关时的形态（Status → FlareError）。
    fn ingest_rejection(code: tonic::Code, message: &str) -> FlareError {
        FlareError::from(tonic::Status::new(code, message))
    }

    #[test]
    fn ingest_tenant_rejection_is_a_typed_403_not_a_500() {
        use axum::response::IntoResponse;
        let ingest = FlareError::from(tonic::Status::from(
            ErrorBuilder::new(ErrorCode::PermissionDenied, TENANT_UNAVAILABLE).build_error(),
        ));
        let err = send_failure(ingest);
        let localized = err.0.as_localized().expect("localized error");
        assert_eq!(localized.reason, TENANT_UNAVAILABLE);
        assert_eq!(localized.details, None);
        assert_eq!(
            err.into_response().status(),
            axum::http::StatusCode::FORBIDDEN
        );
    }

    /// 摄入侧读不到租户投影（严格策略）是暂时性拒绝：503 `SERVICE_UNAVAILABLE`，不是 403
    /// `TENANT_UNAVAILABLE`，也不回显内部文本。
    #[test]
    fn ingest_transient_rejection_is_a_503_not_a_tenant_403() {
        use axum::response::IntoResponse;
        let ingest = FlareError::from(tonic::Status::from(
            ErrorBuilder::new(
                ErrorCode::ServiceUnavailable,
                "tenant runtime projection unavailable",
            )
            .build_error(),
        ));
        let err = send_failure(ingest);
        let localized = err.0.as_localized().expect("localized error");
        assert_eq!(localized.reason, "SERVICE_UNAVAILABLE");
        assert_eq!(localized.details, None);
        assert_eq!(
            err.into_response().status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );

        let unreachable = send_failure(ingest_rejection(
            tonic::Code::Unavailable,
            "connection refused",
        ));
        assert_eq!(
            unreachable.into_response().status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn other_ingest_denials_are_403_and_failures_stay_500() {
        use axum::response::IntoResponse;
        let denied = send_failure(ingest_rejection(
            tonic::Code::PermissionDenied,
            "not a member",
        ));
        assert_eq!(denied.0.as_localized().unwrap().reason, "FORBIDDEN");
        assert_eq!(
            denied.into_response().status(),
            axum::http::StatusCode::FORBIDDEN
        );
        let failed = send_failure(ingest_rejection(tonic::Code::Internal, "boom"));
        assert_eq!(
            failed.0.as_localized().unwrap().reason,
            "MESSAGE_SEND_FAILED"
        );
        // 下游原文不回显（以前 500 的说明就是 `err.to_string()`）。
        assert_eq!(failed.0.as_localized().unwrap().details, None);
        assert_eq!(
            failed.into_response().status(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    /// 空白的 client_msg_id 由网关生成；只有不太长的可见 ASCII 才进响应头（响应体总是带）。
    #[test]
    fn client_msg_id_is_generated_when_blank_and_header_safe_only_when_short_ascii() {
        assert_eq!(effective_client_msg_id("biz-1"), "biz-1");
        for blank in ["", "  "] {
            let generated = effective_client_msg_id(blank);
            assert!(generated.starts_with("http_"), "{generated}");
            assert_ne!(generated, effective_client_msg_id(blank));
        }

        assert_eq!(
            client_msg_id_header("biz-1")
                .as_ref()
                .map(HeaderValue::as_bytes),
            Some(&b"biz-1"[..])
        );
        let longest = "a".repeat(CLIENT_MSG_ID_HEADER_MAX_LEN);
        assert!(client_msg_id_header(&longest).is_some());
        for unsafe_id in [
            "a".repeat(CLIENT_MSG_ID_HEADER_MAX_LEN + 1),
            "订单-1".to_string(),
            "biz 1".to_string(),
            "biz-1\r\nx-injected: 1".to_string(),
        ] {
            assert!(client_msg_id_header(&unsafe_id).is_none(), "{unsafe_id:?}");
        }
    }

    #[test]
    fn custom_event_request_rejects_empty_namespace() {
        let ctx = Arc::new(Context::with_request_id("req-1").with_user_id("alice"));
        let err = build_execute_custom_event_request(
            &ctx,
            ExecuteCustomEventHttpRequest {
                conversation_id: "group:g1".to_string(),
                namespace: " ".to_string(),
                name: "order_paid".to_string(),
                version: String::new(),
                payload: json!({}),
                attributes: std::collections::HashMap::new(),
                event_id: String::new(),
                svid: String::new(),
            },
        )
        .expect_err("empty namespace must fail");

        let localized = err.0.as_localized().expect("localized error");
        assert_eq!(localized.reason, "INVALID_MESSAGE_EVENT");
        assert_eq!(localized.details.as_deref(), Some("namespace is required"));
    }
}
