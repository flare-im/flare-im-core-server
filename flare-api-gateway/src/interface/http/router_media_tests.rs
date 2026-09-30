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

/// What the scripted Media endpoint answers to every file RPC.
#[derive(Clone, Copy)]
enum MediaVerdict {
    /// Owner of an object-store file whose persisted cdn_url is a bare object key.
    OwnerWithLegacyCdnKey,
    /// Media looked the file up under the caller's tenant and found nothing.
    CrossTenant,
    /// Same tenant, different owner.
    CrossOwner,
}

const FILE_ID: &str = "4a8f0c2e-5d1b-4f7e-9c3a-7b6d5e4f3a21";
const LEGACY_CDN_KEY: &str = "tenant-a/media/documents/2026/09/30/note.txt";
const SIGNED_URL: &str = "https://edge.test:18443/flare-media/tenant-a/media/documents/2026/09/30/note.txt?X-Amz-Signature=unit-test";

/// Stand-in Media endpoint that answers like the real service: domain errors are encoded
/// exactly as `into_grpc()` does, successes as one length-prefixed protobuf frame.
#[derive(Clone)]
struct ScriptedMedia(MediaVerdict);

impl tonic::server::NamedService for ScriptedMedia {
    const NAME: &'static str = flare_grpc_proto::media::media_service_server::SERVICE_NAME;
}

fn grpc_ok(message: impl prost::Message) -> axum::http::Response<tonic::body::Body> {
    let payload = message.encode_to_vec();
    let mut frame = Vec::with_capacity(payload.len() + 5);
    frame.push(0);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    axum::http::Response::builder()
        .header("content-type", "application/grpc")
        .header("grpc-status", "0")
        .body(tonic::body::Body::new(Body::from(frame)))
        .unwrap()
}

impl tower::Service<axum::http::Request<tonic::body::Body>> for ScriptedMedia {
    type Response = axum::http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: axum::http::Request<tonic::body::Body>) -> Self::Future {
        use flare_grpc_proto::media::*;
        use flare_server_core::error::ErrorCode;
        let method = request.uri().path().rsplit('/').next().unwrap_or_default();
        let response = match self.0 {
            MediaVerdict::CrossTenant => {
                tonic::Status::from(flare_server_core::flare_err_details!(
                    ErrorCode::MessageNotFound,
                    "metadata not found",
                    FILE_ID
                ))
                .into_http()
            }
            MediaVerdict::CrossOwner => tonic::Status::from(flare_server_core::flare_err!(
                ErrorCode::PermissionDenied,
                "media access is not authorized"
            ))
            .into_http(),
            MediaVerdict::OwnerWithLegacyCdnKey => match method {
                "GetFileInfo" => grpc_ok(GetFileInfoResponse {
                    info: Some(FileInfo {
                        file_id: FILE_ID.to_string(),
                        bucket: "flare-media".to_string(),
                        object_key: LEGACY_CDN_KEY.to_string(),
                        cdn_url: LEGACY_CDN_KEY.to_string(),
                        ..Default::default()
                    }),
                    success: true,
                    ..Default::default()
                }),
                "GetFileUrl" => grpc_ok(GetFileUrlResponse {
                    url: SIGNED_URL.to_string(),
                    cdn_url: LEGACY_CDN_KEY.to_string(),
                    success: true,
                    ..Default::default()
                }),
                _ => tonic::Status::unimplemented("not scripted").into_http(),
            },
        };
        std::future::ready(Ok(response))
    }
}

async fn scripted_router(verdict: MediaVerdict) -> Router {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tonic::transport::Server::builder()
        .add_service(ScriptedMedia(verdict))
        .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener));
    tokio::spawn(server);
    let channel = Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_lazy();
    let (signer, _) = cell_keys();
    router_with(channel, Some(signer))
}

/// The file routes a W2-07 isolation probe hits, as `(method, uri, json body)`.
fn file_routes() -> Vec<(&'static str, String, Option<String>)> {
    vec![
        ("GET", format!("/api/v1/medias/files/{FILE_ID}"), None),
        (
            "GET",
            format!("/api/v1/medias/file-info?file_id={FILE_ID}"),
            None,
        ),
        (
            "POST",
            "/api/v1/medias/file-url".to_string(),
            Some(format!(r#"{{"file_id":"{FILE_ID}"}}"#)),
        ),
        (
            "DELETE",
            "/api/v1/medias/file".to_string(),
            Some(format!(r#"{{"file_id":"{FILE_ID}","hard_delete":true}}"#)),
        ),
        (
            "GET",
            format!("/api/v1/medias/references?file_id={FILE_ID}"),
            None,
        ),
    ]
}

async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    body: Option<String>,
) -> axum::response::Response {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", "Bearer ordinary-user");
    if body.is_some() {
        request = request.header("content-type", "application/json");
    }
    router
        .clone()
        .oneshot(
            request
                .body(body.map(Body::from).unwrap_or_default())
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn file_download_redirect_skips_a_bare_key_cdn_url_for_the_signed_url() {
    let router = scripted_router(MediaVerdict::OwnerWithLegacyCdnKey).await;
    let response = call(
        &router,
        "GET",
        &format!("/api/v1/medias/files/{FILE_ID}"),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        response
            .headers()
            .get("location")
            .unwrap()
            .to_str()
            .unwrap(),
        SIGNED_URL
    );
}

#[tokio::test]
async fn cross_tenant_and_cross_owner_denials_are_typed_and_do_not_echo_media_text() {
    for (verdict, status, reason, leaked) in [
        (
            MediaVerdict::CrossTenant,
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            FILE_ID,
        ),
        (
            MediaVerdict::CrossOwner,
            StatusCode::FORBIDDEN,
            "FORBIDDEN",
            "not authorized",
        ),
    ] {
        let router = scripted_router(verdict).await;
        for (method, uri, body) in file_routes() {
            let response = call(&router, method, &uri, body).await;
            assert_eq!(response.status(), status, "{method} {uri}");
            let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .unwrap();
            let envelope: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(envelope["code"], status.as_u16(), "{method} {uri}");
            assert_eq!(envelope["reason"], reason, "{method} {uri}");
            assert!(envelope.get("data").is_none(), "{method} {uri}");
            assert!(
                !String::from_utf8_lossy(&bytes).contains(leaked),
                "{method} {uri}: {envelope}"
            );
        }
    }
}
