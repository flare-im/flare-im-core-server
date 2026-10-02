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

/// 按会话 id 决定回什么的替身 MessageIngest。
#[derive(Clone, Default)]
struct FakeIngest;

/// 摄入侧对 PreSend 策略拒绝回的状态：错误码、原因与说明同以前（`OPERATION_FAILED` /
/// `pre-send hook rejected` / `allow=false`），gRPC 状态码是 `FAILED_PRECONDITION`。
fn ingest_policy_rejection() -> tonic::Status {
    let rejection = ErrorBuilder::new(ErrorCode::OperationFailed, "pre-send hook rejected")
        .details("allow=false")
        .param("pre_send_rejection", "true")
        .build_error();
    let status = tonic::Status::from(rejection);
    tonic::Status::with_details(
        tonic::Code::FailedPrecondition,
        status.message().to_string(),
        status.details().to_vec().into(),
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
        let conversation_id = request.into_inner().conversation_id;
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

async fn start_ingest(verifier: SignedContextVerifier) -> Channel {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service =
        SignedContextLayer::new(verifier).layer(MessageSendServiceServer::new(FakeIngest));
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
    let (signer, verifier) = cell_keys();
    let ingest = start_ingest(verifier).await;
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
    };
    create_public_router(Arc::new(clients))
        .layer(axum::Extension(
            Arc::new(Validator) as Arc<dyn TokenValidator>
        ))
        .layer(axum::Extension(TenantRuntimeGuard::new(
            TenantRuntimeCache::disabled(),
            TenantAccessPolicy::Lenient,
        )))
        .layer(axum::Extension(MessageContextSigner(Some(signer))))
}

async fn send_to(router: &Router, conversation_id: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/messages/send")
        .header("content-type", "application/json")
        .header("authorization", "Bearer alice-a")
        .body(Body::from(
            json!({
                "conversation_id": conversation_id,
                "content": {"text": "hi"},
                "message_type": 1,
            })
            .to_string(),
        ))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json = serde_json::from_slice(&body).unwrap_or_else(|_| {
        panic!(
            "{status} body is not JSON: {:?}",
            String::from_utf8_lossy(&body)
        )
    });
    (status, json)
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

    let (status, body) = send_to(&router, "group:ok").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["server_msg_id"], json!("srv-1"), "{body}");
}
