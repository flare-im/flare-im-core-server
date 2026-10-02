//! HTTP 读会话接口代用户签名（与发送、在线的写路径一致）。
//!
//! 替身会话服务挂与 Core 相同的读接口身份边界（`SignedFirstContextLayer`，要求签名），只看边界
//! 验过的身份。用例经路由、鉴权、签名与真实的 gRPC 连接断言：网关为令牌验出的用户与规范化
//! 租户签名，元数据与签名一致，客户端自带的身份头带不下去。以前网关只转发未签名的
//! `x-tenant-id` / `x-user-id`，会话服务要求签名后这两个接口直接失败。
use super::*;
use crate::interface::http::MessageContextSigner;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_grpc_proto::conversation::conversation_read_service_server::{
    ConversationReadService, ConversationReadServiceServer,
};
use flare_grpc_proto::conversation::*;
use flare_im_service_kit::clients::*;
use flare_im_service_kit::{
    gateway_auth::TenantRuntimeGuard,
    tenant_runtime::{TenantAccessPolicy, TenantRuntimeCache},
};
use flare_proto::common::{ConversationParticipant, ConversationSummary};
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
            // 旧令牌的租户写法：签名与元数据都用规范化后的 "0"。
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
struct FakeConversations(Seen);

impl FakeConversations {
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

#[tonic::async_trait]
impl ConversationReadService for FakeConversations {
    async fn list_conversations(
        &self,
        request: tonic::Request<ListConversationsRequest>,
    ) -> Result<tonic::Response<ListConversationsResponse>, tonic::Status> {
        self.record("ListConversations", &request);
        Ok(tonic::Response::new(ListConversationsResponse {
            conversations: vec![ConversationSummary {
                conversation_id: "conv-1".into(),
                max_conversation_seq: 3,
                ..Default::default()
            }],
            next_cursor: String::new(),
            has_more: false,
        }))
    }

    async fn list_conversation_participants(
        &self,
        request: tonic::Request<ListConversationParticipantsRequest>,
    ) -> Result<tonic::Response<ListConversationParticipantsResponse>, tonic::Status> {
        self.record("ListConversationParticipants", &request);
        Ok(tonic::Response::new(ListConversationParticipantsResponse {
            conversation_id: request.into_inner().conversation_id,
            participants: vec![ConversationParticipant {
                user_id: "alice".into(),
                ..Default::default()
            }],
            ..Default::default()
        }))
    }

    async fn conversation_bootstrap(
        &self,
        _: tonic::Request<ConversationBootstrapRequest>,
    ) -> Result<tonic::Response<ConversationBootstrapResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
    }

    async fn get_conversation_detail(
        &self,
        _: tonic::Request<GetConversationDetailRequest>,
    ) -> Result<tonic::Response<GetConversationDetailResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
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

async fn start_conversations(verifier: SignedContextVerifier) -> (Channel, Seen) {
    let seen: Seen = Arc::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = SignedFirstContextLayer::new(Some(verifier))
        .require_signed(true)
        .layer(ConversationReadServiceServer::new(FakeConversations(
            seen.clone(),
        )));
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service)
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
    let (conversations, seen) = start_conversations(verifier).await;
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

async fn get(
    router: &Router,
    uri: &str,
    token: &str,
    extra: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .uri(uri)
        .header("authorization", format!("Bearer {token}"));
    for (name, value) in extra {
        request = request.header(*name, *value);
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

/// 两个读接口都带网关为令牌用户签的身份，会话服务（要求签名）照常应答；客户端自带的
/// `x-user-id`、`x-tenant-id`、`x-actor-*` 头不影响身份，也不会被判为与签名冲突。
#[tokio::test]
async fn conversation_reads_carry_the_gateway_signed_user() {
    let (signer, verifier) = cell_keys(51);
    let (router, seen) = router(Some(signer), verifier).await;
    let forged = [
        ("x-user-id", "bob"),
        ("x-tenant-id", "tenant-b"),
        ("x-actor-type", "2"),
        ("x-actor-id", "flare-orchestrator"),
    ];

    let (status, body) = get(&router, "/api/v1/conversations?limit=5", "alice-a", &forged).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["data"]["conversations"][0]["conversation_id"],
        json!("conv-1")
    );
    assert_eq!(body["data"]["conversations"][0]["max_seq"], json!(3));

    let (status, body) = get(
        &router,
        "/api/v1/conversations/participants?conversation_id=conv-1",
        "alice-a",
        &forged,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["participants"][0]["user_id"], json!("alice"));

    let (status, body) = get(&router, "/api/v1/conversations", "carol-default", &[]).await;
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
            user("ListConversations", "alice", "tenant-a"),
            user("ListConversationParticipants", "alice", "tenant-a"),
            user("ListConversations", "carol", "0"),
        ]
    );
}

/// 没配签名私钥的网关只带元数据：要求签名的会话服务拒绝，网关回 500 而不是冒充成功。
#[tokio::test]
async fn without_a_signer_a_signing_conversation_service_refuses() {
    let (_, verifier) = cell_keys(51);
    let (router, seen) = router(None, verifier).await;
    let (status, body) = get(&router, "/api/v1/conversations", "alice-a", &[]).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["reason"], json!("CONVERSATION_LIST_FAILED"), "{body}");
    assert!(seen.lock().unwrap().is_empty());
}
