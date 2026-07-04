use axum::{
    extract::{Extension, Json},
    http::HeaderMap,
};
use std::sync::Arc;
use tracing::{debug, instrument};

use crate::application::dto::{
    ExecuteCustomEventHttpRequest, ExecuteCustomEventHttpResponse, RecallMessageHttpRequest,
    RecallMessageHttpResponse, SendMessageHttpRequest, SendMessageHttpResponse,
};
use flare_grpc_proto::message::{ExecuteEventRequest, RecallMessageRequest, SendMessageRequest};
use flare_im_service_kit::clients::GrpcClients;
use flare_proto::common::{
    CustomContent, CustomEvent, Event, EventType, Message, MessageContent, MessageSource,
    MessageStatus, event, message_content,
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

    let mut message_client = clients.message_ingest_send.lock().await;
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

/// 执行业务自定义事件
#[utoipa::path(
    post,
    path = "/api/v1/messages/events/custom",
    tag = "Message",
    request_body = ExecuteCustomEventHttpRequest,
    responses(
        (status = 200, description = "成功", body = ApiResponse<ExecuteCustomEventHttpResponse>),
        (status = 400, description = "参数错误"),
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

    let mut event_client = clients.message_event.lock().await;
    event_client
        .execute_event_with_ctx(&ctx, grpc_req)
        .await
        .map_err(|err| GatewayError::internal("MESSAGE_EVENT_EXECUTE_FAILED", err.to_string()))?;

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
