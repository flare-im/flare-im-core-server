//! HTTP 发送接口对 MessageIngest 失败的映射契约。
//!
//! 替身 MessageIngest 挂真实的 `SignedContextLayer`，按真实摄入侧的形态回错：PreSend 策略拒绝是
//! `FAILED_PRECONDITION` 加原样的 `ErrorDetail`（与 flare-message-ingest `send_failure_status` 相同），
//! 其余失败是 `FlareError` 直接转出的 `Status`。用例经路由、鉴权、签名与真实的 gRPC 连接断言 HTTP
//! 状态码与响应体。
use super::*;
use crate::interface::http::MessageContextSigner;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_core_base::error::{ErrorBuilder, ErrorCode};
use flare_grpc_proto::message::message_send_service_server::{
    MessageSendService, MessageSendServiceServer,
};
use flare_grpc_proto::message::*;
use flare_im_service_kit::clients::*;
use flare_im_service_kit::{
    gateway_auth::TenantRuntimeGuard,
    tenant_runtime::{TenantAccessPolicy, TenantRuntimeCache},
};
use flare_server_core::{
    AuthError, AuthenticatedPrincipal, SignedContextLayer, SignedContextSigner,
    SignedContextVerifier, TokenValidationRequest, TokenValidator,
};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tower::ServiceExt;

struct Validator;

#[async_trait::async_trait]
impl TokenValidator for Validator {
    async fn validate(
        &self,
        request: TokenValidationRequest,
    ) -> Result<AuthenticatedPrincipal, AuthError> {
        if request.token != "alice-a" {
            return Err(AuthError::InvalidToken("unknown test token".into()));
        }
        Ok(AuthenticatedPrincipal {
            user_id: "alice".into(),
            tenant_id: Some("tenant-a".into()),
            device_id: None,
            app_id: None,
            expires_at: None,
            scopes: vec![],
            metadata: Default::default(),
        })
    }
}

/// 按会话 id 决定回什么的替身 MessageIngest；记下收到的每条消息的 `client_msg_id`。
#[derive(Clone, Default)]
struct FakeIngest {
    seen_client_msg_ids: Arc<std::sync::Mutex<Vec<String>>>,
}

/// 摄入侧对 PreSend 策略拒绝回的状态：错误码、原因与说明同以前（`OPERATION_FAILED` /
/// `pre-send hook rejected` / `allow=false`），gRPC 状态码是 `FAILED_PRECONDITION`。
fn ingest_policy_rejection() -> tonic::Status {
    let rejection = ErrorBuilder::new(ErrorCode::OperationFailed, "pre-send hook rejected")
        .details("allow=false")
        .param("pre_send_rejection", "true")
        .build_error();
    flare_server_core::error::grpc::with_code(
        tonic::Status::from(rejection),
        tonic::Code::FailedPrecondition,
    )
}

#[tonic::async_trait]
impl MessageSendService for FakeIngest {
    async fn send_message(
        &self,
        request: tonic::Request<SendMessageRequest>,
    ) -> Result<tonic::Response<SendMessageResponse>, tonic::Status> {
        // 只受理带签名身份的请求（SignedContextLayer 已验签）。
        flare_server_core::require_ctx_from_request(&request)?;
        let request = request.into_inner();
        if let Some(message) = request.message.as_ref() {
            self.seen_client_msg_ids
                .lock()
                .unwrap()
                .push(message.client_msg_id.clone());
        }
        let conversation_id = request.conversation_id;
        match conversation_id.as_str() {
            "group:policy" => Err(ingest_policy_rejection()),
            "group:tenant-off" => Err(tonic::Status::from(
                ErrorBuilder::new(ErrorCode::PermissionDenied, "TENANT_UNAVAILABLE").build_error(),
            )),
            "group:busy" => Err(tonic::Status::from(
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "pre-send hook failed")
                    .details("transport error")
                    .build_error(),
            )),
            "group:boom" => Err(tonic::Status::internal("database exploded at 10.0.0.7")),
            // 摄入侧按时限放弃（`OperationTimeout` 经 `Status::from` 是 `DEADLINE_EXCEEDED`）：
            // 旧版摄入的必需 Hook 超时、阶段超时都是这个形态。
            "group:deadline" => Err(tonic::Status::from(
                ErrorBuilder::new(ErrorCode::OperationTimeout, "pre-send hook timed out")
                    .details("hook=social-policy-pre-send at 10.0.0.7")
                    .build_error(),
            )),
            // 摄入侧判消息不合法：领域校验的 InvalidParameter 经 `Status::from` 带结构化详情，
            // 接口层的纯文本 `Status::invalid_argument` 不带。
            "group:invalid" => Err(tonic::Status::from(
                ErrorBuilder::new(
                    ErrorCode::InvalidParameter,
                    "burn_after_read_seconds must be positive when burn is enabled",
                )
                .details("validator internals at 10.0.0.7")
                .build_error(),
            )),
            "group:invalid-plain" => Err(tonic::Status::invalid_argument(
                "message required (decoder at 10.0.0.7)",
            )),
            "group:missing" => Err(tonic::Status::from(
                ErrorBuilder::new(ErrorCode::UserNotFound, "conversation not found")
                    .details("lookup in shard 10.0.0.7")
                    .build_error(),
            )),
            _ => Ok(tonic::Response::new(SendMessageResponse {
                server_msg_id: "srv-1".into(),
                conversation_seq: 7,
                success: true,
                ..Default::default()
            })),
        }
    }

    async fn batch_send_message(
        &self,
        _: tonic::Request<BatchSendMessageRequest>,
    ) -> Result<tonic::Response<BatchSendMessageResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
    }

    async fn send_system_message(
        &self,
        _: tonic::Request<SendSystemMessageRequest>,
    ) -> Result<tonic::Response<SendSystemMessageResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
    }

    async fn send_ack(
        &self,
        _: tonic::Request<SendAckRequest>,
    ) -> Result<tonic::Response<SendAckResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
    }

    async fn send_custom_data(
        &self,
        _: tonic::Request<SendCustomDataRequest>,
    ) -> Result<tonic::Response<SendCustomDataResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
    }
}

fn cell_keys() -> (SignedContextSigner, SignedContextVerifier) {
    let key = SigningKey::from_bytes(&[43; 32]);
    let private = key.to_pkcs8_pem(Default::default()).unwrap();
    let public = key
        .verifying_key()
        .to_public_key_pem(Default::default())
        .unwrap();
    (
        SignedContextSigner::from_private_key(&private).unwrap(),
        SignedContextVerifier::from_public_key(&public).unwrap(),
    )
}

async fn start_ingest(verifier: SignedContextVerifier, ingest: FakeIngest) -> Channel {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = SignedContextLayer::new(verifier).layer(MessageSendServiceServer::new(ingest));
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
    );
    Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_lazy()
}

async fn router() -> Router {
    router_with(FakeIngest::default(), true).await
}

/// `signed=false` 模拟网关没有配置 Cell 私钥（HTTP 发送 fail closed）。
async fn router_with(fake: FakeIngest, signed: bool) -> Router {
    let (signer, verifier) = cell_keys();
    let ingest = start_ingest(verifier, fake).await;
    // 其它下游的惰性通道在这些用例里从不连接。
    let c = Channel::from_static("http://127.0.0.1:9").connect_lazy();
    let clients = GrpcClients {
        media: Arc::new(Mutex::new(MediaServiceClientWrapper::from_channel(
            c.clone(),
            None,
        ))),
        message_ingest_send: Arc::new(Mutex::new(MessageSendServiceClientWrapper::from_channel(
            ingest,
        ))),
        message_event: Arc::new(Mutex::new(MessageEventServiceClientWrapper::from_channel(
            c.clone(),
        ))),
        message_action: Arc::new(Mutex::new(MessageActionServiceClientWrapper::from_channel(
            c.clone(),
        ))),
        conversation_read: Arc::new(Mutex::new(
            ConversationReadServiceClientWrapper::from_channel(c.clone()),
        )),
        conversation_manage: Arc::new(Mutex::new(
            ConversationManageServiceClientWrapper::from_channel(c.clone()),
        )),
        online: Arc::new(Mutex::new(OnlineServiceClientWrapper::from_channel(
            c.clone(),
        ))),
        storage_reader: Arc::new(Mutex::new(StorageReaderServiceClientWrapper::from_channel(
            c,
        ))),
        request_timeout: None,
    };
    create_public_router(Arc::new(clients))
        .layer(axum::Extension(
            Arc::new(Validator) as Arc<dyn TokenValidator>
        ))
        .layer(axum::Extension(TenantRuntimeGuard::new(
            TenantRuntimeCache::disabled(),
            TenantAccessPolicy::Lenient,
        )))
        .layer(axum::Extension(MessageContextSigner(
            signed.then_some(signer),
        )))
}

async fn send_to(router: &Router, conversation_id: &str) -> (StatusCode, Value) {
    let (status, _, json) = send_with_id(router, conversation_id, None).await;
    (status, json)
}

/// 发一条消息，`client_msg_id` 为 `None` 时请求体不带该字段；返回状态、响应头里的
/// `x-client-msg-id` 与响应体。
async fn send_with_id(
    router: &Router,
    conversation_id: &str,
    client_msg_id: Option<&str>,
) -> (StatusCode, Option<String>, Value) {
    let mut body = json!({
        "conversation_id": conversation_id,
        "content": {"text": "hi"},
        "message_type": 1,
    });
    if let Some(client_msg_id) = client_msg_id {
        body["client_msg_id"] = json!(client_msg_id);
    }
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/messages/send")
        .header("content-type", "application/json")
        .header("authorization", "Bearer alice-a")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let header = response
        .headers()
        .get("x-client-msg-id")
        .map(|value| value.to_str().expect("ascii header").to_string());
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json = serde_json::from_slice(&body).unwrap_or_else(|_| {
        panic!(
            "{status} body is not JSON: {:?}",
            String::from_utf8_lossy(&body)
        )
    });
    (status, header, json)
}

/// PreSend 策略拒绝（摄入侧回 `FAILED_PRECONDITION`）回 422 `MESSAGE_REJECTED`，说明是扩展给出的
/// 拒绝原因；以前回 500 `MESSAGE_SEND_FAILED`。
#[tokio::test]
async fn pre_send_policy_rejection_is_a_422_not_a_500() {
    let router = router().await;
    let (status, body) = send_to(&router, "group:policy").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], json!(422), "{body}");
    assert_eq!(body["reason"], json!("MESSAGE_REJECTED"), "{body}");
    assert_eq!(body["message"], json!("pre-send hook rejected"), "{body}");
}

/// 其余失败的映射不变：租户停用 403、暂时不可用 503（不回显内部文本）、内部错误 500；成功 200。
#[tokio::test]
async fn other_ingest_outcomes_keep_their_http_mapping() {
    let router = router().await;

    let (status, body) = send_to(&router, "group:tenant-off").await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["reason"], json!("TENANT_UNAVAILABLE"), "{body}");

    let (status, body) = send_to(&router, "group:busy").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["reason"], json!("SERVICE_UNAVAILABLE"), "{body}");
    assert!(!body.to_string().contains("transport error"), "{body}");

    let (status, body) = send_to(&router, "group:boom").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["reason"], json!("MESSAGE_SEND_FAILED"), "{body}");
    assert!(!body.to_string().contains("10.0.0.7"), "{body}");

    let (status, body) = send_to(&router, "group:ok").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["server_msg_id"], json!("srv-1"), "{body}");
}

/// 摄入侧判消息不合法（INVALID_ARGUMENT）回 400 `INVALID_MESSAGE`，找不到发送依赖的对象（NOT_FOUND）
/// 回 404 `NOT_FOUND`；说明只取结构化详情里的原因，`Status.message` 与详情的 `message` 不回显。
/// 以前两者都回带原文的 500 `MESSAGE_SEND_FAILED`。
#[tokio::test]
async fn invalid_and_missing_ingest_outcomes_are_typed_4xx_not_500() {
    let router = router().await;

    let (status, body) = send_to(&router, "group:invalid").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], json!(400), "{body}");
    assert_eq!(body["reason"], json!("INVALID_MESSAGE"), "{body}");
    assert_eq!(
        body["message"],
        json!("burn_after_read_seconds must be positive when burn is enabled"),
        "{body}"
    );
    assert!(!body.to_string().contains("10.0.0.7"), "{body}");

    let (status, body) = send_to(&router, "group:invalid-plain").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["reason"], json!("INVALID_MESSAGE"), "{body}");
    assert!(!body.to_string().contains("10.0.0.7"), "{body}");

    let (status, body) = send_to(&router, "group:missing").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["code"], json!(404), "{body}");
    assert_eq!(body["reason"], json!("NOT_FOUND"), "{body}");
    assert_eq!(body["message"], json!("conversation not found"), "{body}");
    assert!(!body.to_string().contains("10.0.0.7"), "{body}");
}

/// 摄入侧按时限放弃（`DEADLINE_EXCEEDED`）回可重试的 503 `SERVICE_UNAVAILABLE`，与 Hook 不可达
/// 相同，不回显内部文本；以前回 500 `MESSAGE_SEND_FAILED`。重试要带同一个 `client_msg_id`，
/// 摄入侧按它去重。
#[tokio::test]
async fn ingest_deadline_exceeded_is_a_retryable_503_not_a_500() {
    let router = router().await;
    let (status, body) = send_to(&router, "group:deadline").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["code"], json!(503), "{body}");
    assert_eq!(body["reason"], json!("SERVICE_UNAVAILABLE"), "{body}");
    assert!(!body.to_string().contains("10.0.0.7"), "{body}");
    assert!(!body.to_string().contains("social-policy"), "{body}");
}

/// 调用方没带 `client_msg_id` 时网关自己生成一个，这个 id 要出现在每个应答里（响应头
/// `x-client-msg-id`；成功在 `data.client_msg_id`，失败在顶层 `client_msg_id`）。以前 503 不回它，
/// 盲重试只能再生成一个新 id，摄入侧无从去重；现在调用方带回这个 id 重试，摄入侧收到的是同一个。
#[tokio::test]
async fn generated_client_msg_id_is_returned_on_errors_so_retries_can_reuse_it() {
    let fake = FakeIngest::default();
    let router = router_with(fake.clone(), true).await;

    let (status, header, body) = send_with_id(&router, "group:busy", None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["reason"], json!("SERVICE_UNAVAILABLE"), "{body}");
    let generated = header.expect("503 carries x-client-msg-id");
    assert!(generated.starts_with("http_"), "{generated}");
    assert_eq!(body["client_msg_id"], json!(generated), "{body}");
    assert_eq!(
        fake.seen_client_msg_ids.lock().unwrap().last().cloned(),
        Some(generated.clone()),
        "the id returned is the one ingest saw"
    );

    // 带回同一个 id 重试：摄入侧收到同一个 id，成功应答也回它。
    let (status, header, body) = send_with_id(&router, "group:ok", Some(&generated)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(header.as_deref(), Some(generated.as_str()));
    assert_eq!(body["data"]["client_msg_id"], json!(generated), "{body}");
    assert_eq!(body["data"]["server_msg_id"], json!("srv-1"), "{body}");
    assert_eq!(
        fake.seen_client_msg_ids.lock().unwrap().last().cloned(),
        Some(generated)
    );
}

/// 调用方带了 `client_msg_id` 时原样回显，各类失败（4xx、5xx）都带；成功时也带。
#[tokio::test]
async fn caller_client_msg_id_is_echoed_on_every_outcome() {
    let router = router().await;
    for (conversation_id, expected) in [
        ("group:ok", StatusCode::OK),
        ("group:busy", StatusCode::SERVICE_UNAVAILABLE),
        ("group:deadline", StatusCode::SERVICE_UNAVAILABLE),
        ("group:boom", StatusCode::INTERNAL_SERVER_ERROR),
        ("group:policy", StatusCode::UNPROCESSABLE_ENTITY),
        ("group:invalid", StatusCode::BAD_REQUEST),
        ("group:missing", StatusCode::NOT_FOUND),
        ("group:tenant-off", StatusCode::FORBIDDEN),
    ] {
        let (status, header, body) =
            send_with_id(&router, conversation_id, Some("biz-10001")).await;
        assert_eq!(status, expected, "{conversation_id}: {body}");
        assert_eq!(header.as_deref(), Some("biz-10001"), "{conversation_id}");
        let echoed = if status == StatusCode::OK {
            &body["data"]["client_msg_id"]
        } else {
            &body["client_msg_id"]
        };
        assert_eq!(echoed, &json!("biz-10001"), "{conversation_id}: {body}");
        // 错误体其余字段保持原结构。
        if status != StatusCode::OK {
            assert_eq!(body["code"], json!(status.as_u16()), "{body}");
            assert!(body.get("data").is_none(), "{body}");
        }
    }
}

/// 网关自己拒绝（还没到摄入侧，例如没有配置签名私钥）时同样带上 id。
#[tokio::test]
async fn gateway_side_send_failures_carry_the_client_msg_id_too() {
    let router = router_with(FakeIngest::default(), false).await;
    let (status, header, body) = send_with_id(&router, "group:ok", None).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(
        body["reason"],
        json!("MESSAGE_SIGNER_UNAVAILABLE"),
        "{body}"
    );
    let generated = header.expect("x-client-msg-id");
    assert_eq!(body["client_msg_id"], json!(generated), "{body}");
}
