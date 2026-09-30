use super::*;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_im_service_kit::clients::*;
use flare_im_service_kit::{
    gateway_auth::TenantRuntimeGuard,
    tenant_runtime::{TenantAccessPolicy, TenantRuntimeCache},
};
use flare_server_core::{
    AuthError, AuthenticatedPrincipal, TokenValidationRequest, TokenValidator,
};
use flare_server_core::{SignedActorType, SignedContextSigner, SignedContextVerifier};
use std::sync::Mutex as StdMutex;
use std::task::Poll;
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
        Ok(AuthenticatedPrincipal {
            user_id: "user".to_string(),
            tenant_id: if request.token == "unbound" {
                None
            } else {
                Some("tenant-a".to_string())
            },
            device_id: None,
            app_id: None,
            expires_at: None,
            scopes: if request.token == "admin-scope" {
                vec!["gateway:admin".to_string()]
            } else {
                vec![]
            },
            metadata: Default::default(),
        })
    }
}

fn router() -> Router {
    // Lazy channels never connect for the rejected requests tested below.
    router_with(
        Channel::from_static("http://127.0.0.1:9").connect_lazy(),
        None,
    )
}

fn router_with(media: Channel, signer: Option<SignedContextSigner>) -> Router {
    let c = Channel::from_static("http://127.0.0.1:9").connect_lazy();
    let clients = GrpcClients {
        media: Arc::new(Mutex::new(MediaServiceClientWrapper::from_channel(
            media, None,
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
        .layer(axum::Extension(
            crate::interface::http::MessageContextSigner(signer),
        ))
}

fn cell_keys() -> (SignedContextSigner, SignedContextVerifier) {
    let key = SigningKey::from_bytes(&[23; 32]);
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

/// Stand-in Media endpoint: records the signed-context header of every RPC it receives.
#[derive(Clone, Default)]
struct CapturingMedia(Arc<StdMutex<Vec<Option<String>>>>);

impl tonic::server::NamedService for CapturingMedia {
    const NAME: &'static str = flare_grpc_proto::media::media_service_server::SERVICE_NAME;
}

impl tower::Service<axum::http::Request<tonic::body::Body>> for CapturingMedia {
    type Response = axum::http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: axum::http::Request<tonic::body::Body>) -> Self::Future {
        let token = request
            .headers()
            .get("x-flare-ctx")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        self.0.lock().unwrap().push(token);
        std::future::ready(Ok(tonic::Status::unavailable("captured").into_http()))
    }
}

async fn capturing_media() -> (Channel, CapturingMedia) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let media = CapturingMedia::default();
    let server = tonic::transport::Server::builder()
        .add_service(media.clone())
        .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener));
    tokio::spawn(server);
    let channel = Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_lazy();
    (channel, media)
}

fn file_info_request(token: &str) -> Request<Body> {
    Request::builder()
        .uri("/api/v1/medias/file-info?file_id=file")
        .header("authorization", format!("Bearer {token}"))
        // Spoofed identity headers must not reach the signed Media identity.
        .header("x-tenant-id", "victim")
        .header("x-user-id", "victim-user")
        .header("x-actor-type", "tenant_admin")
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn media_rpc_carries_signed_identity_from_token_principal_only() {
    let (signer, verifier) = cell_keys();
    let (channel, media) = capturing_media().await;
    let response = router_with(channel, Some(signer))
        .oneshot(file_info_request("ordinary-user"))
        .await
        .unwrap();
    // The stand-in rejects every RPC; the point is what reached it.
    assert_ne!(response.status(), StatusCode::OK);
    let captured = media.0.lock().unwrap().clone();
    assert_eq!(captured.len(), 1);
    let claims = verifier
        .verify(captured[0].as_deref().expect("signed context header"))
        .unwrap();
    assert_eq!(claims.actor_type, SignedActorType::User);
    assert_eq!(claims.tenant_id, "tenant-a");
    assert_eq!(claims.actor_id, "user");
}

#[tokio::test]
async fn media_route_never_sends_unsigned_or_admin_scoped_rpcs() {
    let (channel, media) = capturing_media().await;
    let response = router_with(channel.clone(), None)
        .oneshot(file_info_request("ordinary-user"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let (signer, _) = cell_keys();
    let response = router_with(channel, Some(signer))
        .oneshot(file_info_request("admin-scope"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(media.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn ordinary_user_cannot_reach_any_media_management_route() {
    let router = router();
    for (method, resource) in [
        ("POST", "cleanup-orphaned-assets"),
        ("POST", "object-acl"),
        ("GET", "objects"),
        ("GET", "bucket"),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(format!("/api/v1/medias/{resource}"))
                    .header("authorization", "Bearer ordinary-user")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{resource}");
    }
}

#[tokio::test]
async fn anonymous_download_and_unbound_token_with_spoofed_tenant_are_denied_before_rpc() {
    let router = router();
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/medias/files/file")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v1/medias/files/file")
                .header("authorization", "Bearer unbound")
                .header("x-tenant-id", "victim")
                .header("x-user-id", "victim-user")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[test]
fn public_openapi_has_no_media_management_operations() {
    let document = ApiDoc::openapi();
    for path in ["cleanup-orphaned-assets", "object-acl", "objects", "bucket"] {
        assert!(
            !document
                .paths
                .paths
                .contains_key(&format!("/api/v1/medias/{path}"))
        );
    }
}
