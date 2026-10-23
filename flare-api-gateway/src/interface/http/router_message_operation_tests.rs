//! HTTP 撤回与自定义事件代用户签名（与发送相同）。
//!
//! 替身 Orchestrator 挂与 Core 相同的身份边界（`SignedFirstContextLayer`，要求签名），只看边界验过
//! 的身份。用例经路由、鉴权、签名与真实的 gRPC 连接断言：网关为令牌验出的用户与规范化租户签名，
//! 客户端自带的 `x-user-id`、`x-tenant-id`、`x-actor-*` 头带不下去；没配签名私钥的网关不转发；
//! Orchestrator 的授权拒绝回 4xx 与它的原因码，不并进 500。
use super::*;
use crate::interface::http::MessageContextSigner;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_grpc_proto::message::message_action_service_server::{
    MessageActionService, MessageActionServiceServer,
};
use flare_grpc_proto::message::message_event_service_server::{
    MessageEventService, MessageEventServiceServer,
};
use flare_grpc_proto::message::*;
use flare_im_service_kit::clients::*;
use flare_im_service_kit::{
    gateway_auth::TenantRuntimeGuard,
    tenant_runtime::{TenantAccessPolicy, TenantRuntimeCache},
};
use flare_server_core::error::{ErrorBuilder, ErrorCode};
use flare_server_core::grpc::middleware::signed_context::VerifiedContext;
use flare_server_core::{
    AuthError, AuthenticatedPrincipal, SignedActorType, SignedContextSigner, SignedContextVerifier,
    SignedFirstContextLayer, TokenValidationRequest, TokenValidator,
};
use serde_json::{Value, json};
use std::sync::Mutex as StdMutex;
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
        let (user, tenant) = match request.token.as_str() {
            "alice-a" => ("alice", "tenant-a"),
            // 旧令牌的租户写法：签名用规范化后的 "0"。
            "carol-default" => ("carol", "default"),
            _ => return Err(AuthError::InvalidToken("unknown test token".into())),
        };
        Ok(AuthenticatedPrincipal {
            user_id: user.into(),
            tenant_id: Some(tenant.into()),
            device_id: None,
            app_id: None,
            expires_at: None,
            scopes: vec![],
            metadata: Default::default(),
        })
    }
}

/// 边界验过的身份：(RPC, actor 类型, actor, 租户, 凭据)。
type Seen = Arc<
    StdMutex<
        Vec<(
            &'static str,
            SignedActorType,
            String,
            String,
            Option<String>,
        )>,
    >,
>;

#[derive(Clone)]
struct FakeOrchestrator(Seen);

impl FakeOrchestrator {
    fn record<T>(&self, rpc: &'static str, request: &tonic::Request<T>) {
        let claims = request
            .extensions()
            .get::<flare_server_core::context::Ctx>()
            .and_then(|ctx| ctx.get_data::<VerifiedContext>())
            .and_then(VerifiedContext::claims)
            .expect("the boundary only admits verified calls");
        self.0.lock().unwrap().push((
            rpc,
            claims.actor_type,
            claims.actor_id.clone(),
            claims.tenant_id.clone(),
            claims.credential_id.clone(),
        ));
    }
}

fn unused<T>() -> Result<tonic::Response<T>, tonic::Status> {
    Err(tonic::Status::unimplemented("not used"))
}

/// The answer the fake gives for a target named `refuse:<kind>` (the recall's message id, the
/// custom event's conversation): built as the Orchestrator builds its refusals (`FlareError` →
/// `Status` with the structured `ErrorDetail`), or a bare status where noted.
fn refusal(target: &str) -> Option<tonic::Status> {
    let built = |code, reason: &str, details: &str| {
        Some(tonic::Status::from(
            ErrorBuilder::new(code, reason)
                .details(details)
                .build_error(),
        ))
    };
    match target.strip_prefix("refuse:")? {
        "not-sender" => built(
            ErrorCode::PermissionDenied,
            "MESSAGE_OPERATION_NOT_SENDER",
            "only the sender may do this (orchestrator 10.0.0.7)",
        ),
        "window" => built(
            ErrorCode::PermissionDenied,
            "MESSAGE_OPERATION_WINDOW_EXPIRED",
            "the recall window has passed",
        ),
        "not-member" => built(
            ErrorCode::PermissionDenied,
            "MESSAGE_OPERATION_NOT_MEMBER",
            "not a member of this conversation",
        ),
        "invalid" => built(
            ErrorCode::InvalidParameter,
            "MESSAGE_OPERATION_INVALID",
            "the event names no message",
        ),
        // A refusal whose reason is a sentence, not a code: the fixed reason instead.
        "sentence" => built(
            ErrorCode::InvalidParameter,
            "conversation_id is required for event",
            "",
        ),
        "not-found" => built(ErrorCode::MessageNotFound, "MESSAGE_NOT_FOUND", ""),
        // No structured detail at all.
        "bare" => Some(tonic::Status::permission_denied(
            "policy refused at 10.0.0.7",
        )),
        "not-stored" => built(ErrorCode::ServiceUnavailable, "message not stored yet", ""),
        "boom" => built(ErrorCode::InternalError, "INTERNAL", "boom"),
        _ => None,
    }
}

#[tonic::async_trait]
impl MessageEventService for FakeOrchestrator {
    async fn execute_event(
        &self,
        request: tonic::Request<ExecuteEventRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        self.record("ExecuteEvent", &request);
        let target = request
            .get_ref()
            .event
            .as_ref()
            .map(|event| event.conversation_id.as_str())
            .unwrap_or_default();
        if let Some(refused) = refusal(target) {
            return Err(refused);
        }
        Ok(tonic::Response::new(()))
    }
}

#[tonic::async_trait]
impl MessageActionService for FakeOrchestrator {
    async fn recall_message(
        &self,
        request: tonic::Request<RecallMessageRequest>,
    ) -> Result<tonic::Response<RecallMessageResponse>, tonic::Status> {
        self.record("RecallMessage", &request);
        if let Some(refused) = refusal(&request.get_ref().message_id) {
            return Err(refused);
        }
        Ok(tonic::Response::new(RecallMessageResponse {
            success: true,
            ..Default::default()
        }))
    }

    async fn edit_message(
        &self,
        _: tonic::Request<EditMessageRequest>,
    ) -> Result<tonic::Response<EditMessageResponse>, tonic::Status> {
        unused()
    }

    async fn delete_message(
        &self,
        _: tonic::Request<DeleteMessageRequest>,
    ) -> Result<tonic::Response<DeleteMessageResponse>, tonic::Status> {
        unused()
    }

    async fn add_reaction(
        &self,
        _: tonic::Request<AddReactionRequest>,
    ) -> Result<tonic::Response<AddReactionResponse>, tonic::Status> {
        unused()
    }

    async fn remove_reaction(
        &self,
        _: tonic::Request<RemoveReactionRequest>,
    ) -> Result<tonic::Response<RemoveReactionResponse>, tonic::Status> {
        unused()
    }

    async fn pin_message(
        &self,
        _: tonic::Request<PinMessageRequest>,
    ) -> Result<tonic::Response<PinMessageResponse>, tonic::Status> {
        unused()
    }

    async fn unpin_message(
        &self,
        _: tonic::Request<UnpinMessageRequest>,
    ) -> Result<tonic::Response<UnpinMessageResponse>, tonic::Status> {
        unused()
    }

    async fn mark_message(
        &self,
        _: tonic::Request<MarkMessageRequest>,
    ) -> Result<tonic::Response<MarkMessageResponse>, tonic::Status> {
        unused()
    }

    async fn unmark_message(
        &self,
        _: tonic::Request<UnmarkMessageRequest>,
    ) -> Result<tonic::Response<UnmarkMessageResponse>, tonic::Status> {
        unused()
    }
}

fn cell_keys(seed: u8) -> (SignedContextSigner, SignedContextVerifier) {
    let key = SigningKey::from_bytes(&[seed; 32]);
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

async fn start_orchestrator(verifier: SignedContextVerifier) -> (Channel, Seen) {
    let seen: Seen = Arc::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let boundary = SignedFirstContextLayer::new(Some(verifier)).require_signed(true);
    let fake = FakeOrchestrator(seen.clone());
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(boundary.layer(MessageEventServiceServer::new(fake.clone())))
            .add_service(boundary.layer(MessageActionServiceServer::new(fake)))
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
    );
    let channel = Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_lazy();
    (channel, seen)
}

/// `signer` 为网关的签名私钥；`None` 是没配私钥的网关。
async fn router(
    signer: Option<SignedContextSigner>,
    verifier: SignedContextVerifier,
) -> (Router, Seen) {
    let (orchestrator, seen) = start_orchestrator(verifier).await;
    // 其它下游的惰性通道在这些用例里从不连接。
    let c = Channel::from_static("http://127.0.0.1:9").connect_lazy();
    let clients = GrpcClients {
        media: Arc::new(Mutex::new(MediaServiceClientWrapper::from_channel(
            c.clone(),
            None,
        ))),
        message_ingest_send: Arc::new(Mutex::new(MessageSendServiceClientWrapper::from_channel(
            c.clone(),
        ))),
        message_event: Arc::new(Mutex::new(MessageEventServiceClientWrapper::from_channel(
            orchestrator.clone(),
        ))),
        message_action: Arc::new(Mutex::new(MessageActionServiceClientWrapper::from_channel(
            orchestrator,
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
    let router = create_public_router(Arc::new(clients))
        .layer(axum::Extension(
            Arc::new(Validator) as Arc<dyn TokenValidator>
        ))
        .layer(axum::Extension(TenantRuntimeGuard::new(
            TenantRuntimeCache::disabled(),
            TenantAccessPolicy::Lenient,
        )))
        .layer(axum::Extension(MessageContextSigner(signer)));
    (router, seen)
}

async fn post(
    router: &Router,
    uri: &str,
    token: &str,
    extra: &[(&str, &str)],
    body: Value,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json");
    for (name, value) in extra {
        request = request.header(*name, *value);
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

fn recall() -> Value {
    json!({"conversation_id": "1-alice-bob", "message_id": "m-1"})
}

fn custom_event() -> Value {
    json!({
        "conversation_id": "2-group-a",
        "namespace": "mall",
        "name": "order_paid",
        "payload": {"order_id": "o1"},
    })
}

/// 撤回与自定义事件都带网关为令牌用户签的身份，要求签名的 Orchestrator 照常应答；客户端冒充
/// 别人的 `x-user-id`、`x-tenant-id`、`x-actor-*` 头不影响身份。
#[tokio::test]
async fn message_operations_carry_the_gateway_signed_user() {
    let (signer, verifier) = cell_keys(52);
    let (router, seen) = router(Some(signer), verifier).await;
    let forged = [
        ("x-user-id", "bob"),
        ("x-tenant-id", "tenant-b"),
        ("x-actor-type", "2"),
        ("x-actor-id", "flare-orchestrator"),
    ];

    let (status, body) = post(
        &router,
        "/api/v1/messages/recall",
        "alice-a",
        &forged,
        recall(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["success"], json!(true));

    let (status, body) = post(
        &router,
        "/api/v1/messages/events/custom",
        "alice-a",
        &forged,
        custom_event(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = post(
        &router,
        "/api/v1/messages/recall",
        "carol-default",
        &[],
        recall(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let user = |rpc, actor: &str, tenant: &str| {
        (
            rpc,
            SignedActorType::User,
            actor.to_owned(),
            tenant.to_owned(),
            Some("api-gateway".to_owned()),
        )
    };
    assert_eq!(
        *seen.lock().unwrap(),
        [
            user("RecallMessage", "alice", "tenant-a"),
            user("ExecuteEvent", "alice", "tenant-a"),
            user("RecallMessage", "carol", "0"),
        ]
    );
}

/// 没配签名私钥的网关不转发未签名的身份：两个接口都回 500，Orchestrator 收不到请求。
#[tokio::test]
async fn without_a_signer_message_operations_are_not_forwarded() {
    let (_, verifier) = cell_keys(52);
    let (router, seen) = router(None, verifier).await;
    for (uri, body) in [
        ("/api/v1/messages/recall", recall()),
        ("/api/v1/messages/events/custom", custom_event()),
    ] {
        let (status, body) = post(&router, uri, "alice-a", &[], body).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
        assert_eq!(
            body["reason"],
            json!("MESSAGE_SIGNER_UNAVAILABLE"),
            "{body}"
        );
    }
    assert!(seen.lock().unwrap().is_empty());
}

/// Orchestrator 的授权拒绝回 403 与它的原因码，事件不合法回 400、找不到回 404；原因只取形如常量
/// 的码，说明文字（可能带内部地址）不回显，没有码时用固定原因。不可用仍回 503，其余仍回接口的 500。
#[tokio::test]
async fn refused_message_operations_answer_4xx_with_the_orchestrator_reason() {
    let (signer, verifier) = cell_keys(52);
    let (router, _) = router(Some(signer), verifier).await;
    let cases = [
        ("not-sender", 403, "MESSAGE_OPERATION_NOT_SENDER"),
        ("window", 403, "MESSAGE_OPERATION_WINDOW_EXPIRED"),
        ("not-member", 403, "MESSAGE_OPERATION_NOT_MEMBER"),
        ("bare", 403, "MESSAGE_OPERATION_FORBIDDEN"),
        ("invalid", 400, "MESSAGE_OPERATION_INVALID"),
        ("sentence", 400, "MESSAGE_OPERATION_INVALID"),
        ("not-found", 404, "MESSAGE_NOT_FOUND"),
        ("not-stored", 503, "SERVICE_UNAVAILABLE"),
    ];
    for (kind, status, reason) in cases {
        let target = format!("refuse:{kind}");
        let recall = json!({"conversation_id": "1-alice-bob", "message_id": target});
        let mut event = custom_event();
        event["conversation_id"] = json!(target);
        for (uri, body) in [
            ("/api/v1/messages/recall", recall),
            ("/api/v1/messages/events/custom", event),
        ] {
            let (code, body) = post(&router, uri, "alice-a", &[], body).await;
            assert_eq!(code.as_u16(), status, "{uri} {kind}: {body}");
            assert_eq!(body["reason"], json!(reason), "{uri} {kind}: {body}");
            let text = body.to_string();
            assert!(!text.contains("10.0.0.7"), "{uri} {kind}: {body}");
            assert!(!text.contains("only the sender"), "{uri} {kind}: {body}");
        }
    }
    // Anything else keeps each endpoint's own 500.
    let target = "refuse:boom";
    let (code, body) = post(
        &router,
        "/api/v1/messages/recall",
        "alice-a",
        &[],
        json!({"conversation_id": "1-alice-bob", "message_id": target}),
    )
    .await;
    assert_eq!(code, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["reason"], json!("MESSAGE_RECALL_FAILED"), "{body}");
    let mut event = custom_event();
    event["conversation_id"] = json!(target);
    let (code, body) = post(
        &router,
        "/api/v1/messages/events/custom",
        "alice-a",
        &[],
        event,
    )
    .await;
    assert_eq!(code, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(
        body["reason"],
        json!("MESSAGE_EVENT_EXECUTE_FAILED"),
        "{body}"
    );
}
