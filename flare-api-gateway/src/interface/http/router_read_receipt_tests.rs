//! HTTP 已读摘要与已读名单：经路由、鉴权、代签与真实 gRPC 连接，到挂着读接口身份边界（要求
//! 签名）的替身 `MessageReadReceiptService`。断言网关为令牌用户签名、请求与应答字段一一对应，
//! 以及会话服务的拒绝如何落到 HTTP（400 / 403 原因原样 / 404 / 503）。
use super::*;
use crate::interface::http::MessageContextSigner;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_grpc_proto::conversation::message_read_receipt_service_server::{
    MessageReadReceiptService, MessageReadReceiptServiceServer,
};
use flare_grpc_proto::conversation::*;
use flare_im_service_kit::clients::*;
use flare_im_service_kit::{
    gateway_auth::TenantRuntimeGuard,
    tenant_runtime::{TenantAccessPolicy, TenantRuntimeCache},
};
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

/// What the boundary verified and what the request asked: (actor, tenant, request).
type Seen = Arc<StdMutex<Vec<(String, String, String)>>>;

#[derive(Clone)]
struct FakeReceipts(Seen);

impl FakeReceipts {
    fn record<T>(&self, request: &tonic::Request<T>, asked: String) {
        let claims = request
            .extensions()
            .get::<flare_server_core::context::Ctx>()
            .and_then(|ctx| ctx.get_data::<VerifiedContext>())
            .and_then(VerifiedContext::claims)
            .expect("the boundary only admits verified calls");
        assert_eq!(claims.actor_type, SignedActorType::User);
        self.0
            .lock()
            .unwrap()
            .push((claims.actor_id.clone(), claims.tenant_id.clone(), asked));
    }
}

#[tonic::async_trait]
impl MessageReadReceiptService for FakeReceipts {
    async fn get_message_read_summaries(
        &self,
        request: tonic::Request<GetMessageReadSummariesRequest>,
    ) -> Result<tonic::Response<GetMessageReadSummariesResponse>, tonic::Status> {
        let asked = format!("ids={:?}", request.get_ref().server_msg_ids);
        self.record(&request, asked);
        let req = request.into_inner();
        match req.conversation_id.as_str() {
            "2A-down" => Err(tonic::Status::unavailable(
                "conversation at 10.0.0.7 is down",
            )),
            "2A-gone" => Err(tonic::Status::not_found("conversation not found")),
            "2A-large" => Ok(tonic::Response::new(GetMessageReadSummariesResponse {
                conversation_id: req.conversation_id,
                available: false,
                summaries: vec![],
            })),
            _ if req.server_msg_ids.is_empty() => Err(tonic::Status::invalid_argument(
                "conversation_id and 1..=50 distinct server_msg_ids are required",
            )),
            _ => Ok(tonic::Response::new(GetMessageReadSummariesResponse {
                conversation_id: req.conversation_id,
                available: true,
                summaries: vec![MessageReadSummary {
                    server_msg_id: req.server_msg_ids[0].clone(),
                    read_count: 2,
                    unread_count: 1,
                    total: 3,
                }],
            })),
        }
    }

    async fn list_message_readers(
        &self,
        request: tonic::Request<ListMessageReadersRequest>,
    ) -> Result<tonic::Response<ListMessageReadersResponse>, tonic::Status> {
        let r = request.get_ref();
        let asked = format!("state={} cursor={} limit={}", r.state, r.cursor, r.limit);
        self.record(&request, asked);
        let req = request.into_inner();
        // The service answers another sender's message as one that does not exist.
        if matches!(req.server_msg_id.as_str(), "m-bob" | "m-ghost") {
            return Err(tonic::Status::not_found("message not found"));
        }
        Ok(tonic::Response::new(ListMessageReadersResponse {
            conversation_id: req.conversation_id,
            server_msg_id: req.server_msg_id.clone(),
            available: true,
            summary: Some(MessageReadSummary {
                server_msg_id: req.server_msg_id,
                read_count: 2,
                unread_count: 1,
                total: 3,
            }),
            user_ids: if req.state == MessageReaderState::Read as i32 {
                vec!["bob".into()]
            } else {
                vec!["carol".into()]
            },
            next_cursor: "bob".into(),
            has_more: req.cursor.is_empty(),
        }))
    }
}

async fn router() -> (Router, Seen) {
    let key = SigningKey::from_bytes(&[61; 32]);
    let signer =
        SignedContextSigner::from_private_key(&key.to_pkcs8_pem(Default::default()).unwrap())
            .unwrap();
    let verifier = SignedContextVerifier::from_public_key(
        &key.verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap(),
    )
    .unwrap();
    let seen: Seen = Arc::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = SignedFirstContextLayer::new(Some(verifier))
        .require_signed(true)
        .layer(MessageReadReceiptServiceServer::new(FakeReceipts(
            seen.clone(),
        )));
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
    );
    let conversations = Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_lazy();
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
            c.clone(),
        ))),
        message_action: Arc::new(Mutex::new(MessageActionServiceClientWrapper::from_channel(
            c.clone(),
        ))),
        conversation_read: Arc::new(Mutex::new(
            ConversationReadServiceClientWrapper::from_channel(conversations),
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
        .layer(axum::Extension(MessageContextSigner(Some(signer))));
    (router, seen)
}

async fn call(router: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

fn post_summaries(body: Value) -> Request<Body> {
    Request::post("/api/v1/messages/read-summaries")
        .header("authorization", "Bearer alice-a")
        .header("content-type", "application/json")
        // A client-sent identity never reaches the conversation service.
        .header("x-user-id", "bob")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn get_readers(query: &str) -> Request<Body> {
    Request::get(format!("/api/v1/messages/readers?{query}"))
        .header("authorization", "Bearer alice-a")
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn read_summaries_carry_the_signed_user_and_map_the_answer() {
    let (router, seen) = router().await;
    let (status, body) = call(
        &router,
        post_summaries(json!({"conversation_id": "2A-g1", "server_msg_ids": ["m1", "m2"]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["data"],
        json!({
            "conversation_id": "2A-g1",
            "available": true,
            "summaries": [{"server_msg_id": "m1", "read_count": 2, "unread_count": 1, "total": 3}],
        })
    );
    let (status, body) = call(
        &router,
        post_summaries(json!({"conversation_id": "2A-large", "server_msg_ids": ["m1"]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["available"], json!(false));
    assert_eq!(body["data"]["summaries"], json!([]));

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    for (actor, tenant, request) in &seen {
        assert_eq!((actor.as_str(), tenant.as_str()), ("alice", "tenant-a"));
        assert!(request.starts_with("ids=[\"m1\""), "{request}");
    }
}

#[tokio::test]
async fn read_receipt_refusals_become_http_statuses_without_downstream_text() {
    let (router, _) = router().await;
    for (body, status, reason) in [
        (
            json!({"conversation_id": "2A-g1", "server_msg_ids": []}),
            StatusCode::BAD_REQUEST,
            "INVALID_PARAMETER",
        ),
        (
            json!({"conversation_id": "2A-gone", "server_msg_ids": ["m1"]}),
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
        ),
        (
            json!({"conversation_id": "2A-down", "server_msg_ids": ["m1"]}),
            StatusCode::SERVICE_UNAVAILABLE,
            "SERVICE_UNAVAILABLE",
        ),
    ] {
        let (code, answer) = call(&router, post_summaries(body)).await;
        assert_eq!(code, status, "{answer}");
        assert_eq!(answer["reason"], json!(reason), "{answer}");
        assert!(!answer.to_string().contains("10.0.0.7"), "{answer}");
    }

    // Another sender's message and a message that does not exist get the same answer.
    for id in ["m-bob", "m-ghost"] {
        let (code, answer) = call(
            &router,
            get_readers(&format!(
                "conversation_id=2A-g1&server_msg_id={id}&state=read"
            )),
        )
        .await;
        assert_eq!(code, StatusCode::NOT_FOUND, "{id}: {answer}");
        assert_eq!(answer["reason"], json!("NOT_FOUND"), "{id}: {answer}");
    }
}

#[tokio::test]
async fn readers_page_by_state_and_cursor() {
    let (router, seen) = router().await;
    let (status, body) = call(
        &router,
        get_readers("conversation_id=2A-g1&server_msg_id=m1&state=read&limit=1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["data"],
        json!({
            "conversation_id": "2A-g1",
            "server_msg_id": "m1",
            "available": true,
            "summary": {"server_msg_id": "m1", "read_count": 2, "unread_count": 1, "total": 3},
            "user_ids": ["bob"],
            "next_cursor": "bob",
            "has_more": true,
        })
    );
    let (status, body) = call(
        &router,
        get_readers("conversation_id=2A-g1&server_msg_id=m1&state=UNREAD&cursor=bob"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["user_ids"], json!(["carol"]));
    assert_eq!(body["data"]["has_more"], json!(false));

    // An unknown state never reaches the conversation service.
    let before = seen.lock().unwrap().len();
    let (status, body) = call(
        &router,
        get_readers("conversation_id=2A-g1&server_msg_id=m1&state=seen"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["reason"], json!("INVALID_PARAMETER"));
    assert_eq!(seen.lock().unwrap().len(), before);
    let requests: Vec<_> = seen.lock().unwrap().iter().map(|s| s.2.clone()).collect();
    assert_eq!(
        requests,
        ["state=1 cursor= limit=1", "state=2 cursor=bob limit=0"]
    );
}
