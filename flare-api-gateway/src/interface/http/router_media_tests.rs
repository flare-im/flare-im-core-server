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
            // Same tenant, different end user: owns upload sessions the default caller must not see.
            user_id: if request.token == "other-user" {
                "other".to_string()
            } else {
                "user".to_string()
            },
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

/// The OpenAPI document must describe what the upload-class routes really answer: the typed
/// Media rejections, the gateway's own 401/403, and the retryable 503/504.
#[test]
fn upload_class_operations_document_their_error_responses() {
    let document = serde_json::to_value(ApiDoc::openapi()).unwrap();
    let mut documented = 0;
    for (method, uri, _) in upload_routes() {
        let path = uri.split('?').next().unwrap();
        let operation = &document["paths"][path][method.to_ascii_lowercase()];
        let responses = operation["responses"]
            .as_object()
            .unwrap_or_else(|| panic!("{method} {path} is not documented"));
        let mut expected = vec!["200", "400", "401", "403", "500", "503", "504"];
        let session_or_file = path.contains("/multipart/") && !path.ends_with("/initiate")
            || path.contains("/uploads/") && !path.ends_with("/initiate")
            || path.contains("/process-");
        if session_or_file {
            expected.push("404");
        }
        for status in expected {
            assert!(
                responses.contains_key(status),
                "{method} {path} does not document {status}: {:?}",
                responses.keys().collect::<Vec<_>>()
            );
        }
        documented += 1;
    }
    assert_eq!(documented, 14);
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
    /// Upload session of another caller (or none at all): Media says not found, naming the id.
    ForeignUploadSession,
    /// Media rejected the arguments (part out of range, oversized chunk, caller-chosen key).
    InvalidArgument,
    /// Storage failure whose text carries an object key; Media encodes it as UNAVAILABLE.
    StorageFailure,
    /// Media did not answer in time.
    Timeout,
    /// Media-internal failure whose text carries an object key.
    InternalFailure,
}

const FILE_ID: &str = "4a8f0c2e-5d1b-4f7e-9c3a-7b6d5e4f3a21";
const UPLOAD_ID: &str = "9d2c7e41-3b8a-4c6f-a1e5-0f4b2d8c6a93";
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
            MediaVerdict::ForeignUploadSession => {
                tonic::Status::from(flare_server_core::flare_err_details!(
                    ErrorCode::MessageNotFound,
                    "upload session not found",
                    format!("upload_id={UPLOAD_ID}")
                ))
                .into_http()
            }
            MediaVerdict::InvalidArgument => {
                tonic::Status::from(flare_server_core::flare_err_details!(
                    ErrorCode::InvalidParameter,
                    "part number out of range",
                    format!("part_number=9 total_parts=2 upload_id={UPLOAD_ID} file_id={FILE_ID}")
                ))
                .into_http()
            }
            MediaVerdict::StorageFailure => {
                tonic::Status::from(flare_server_core::flare_err_details!(
                    ErrorCode::NetworkError,
                    "failed to presign s3 put url",
                    format!("key={LEGACY_CDN_KEY}")
                ))
                .into_http()
            }
            MediaVerdict::Timeout => {
                tonic::Status::deadline_exceeded(format!("media slow key={LEGACY_CDN_KEY}"))
                    .into_http()
            }
            MediaVerdict::InternalFailure => {
                tonic::Status::from(flare_server_core::flare_err_details!(
                    ErrorCode::InternalError,
                    "failed to presign s3 put url",
                    format!("key={LEGACY_CDN_KEY}")
                ))
                .into_http()
            }
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
    call_as(router, "ordinary-user", method, uri, body).await
}

async fn call_as(
    router: &Router,
    token: &str,
    method: &str,
    uri: &str,
    body: Option<String>,
) -> axum::response::Response {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"));
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

/// Every upload-class route that reaches Media, as `(method, uri, json body)`. The bodies are
/// valid for their extractors, so a typed envelope proves the answer came from Media's verdict.
fn upload_routes() -> Vec<(&'static str, String, Option<String>)> {
    let metadata = r#"{"file_name":"a.txt","mime_type":"text/plain","file_size":1,"file_type":0,"upload_id":""}"#;
    let session = format!(r#"{{"upload_id":"{UPLOAD_ID}"}}"#);
    vec![
        (
            "POST",
            "/api/v1/medias/upload-url".to_string(),
            Some(
                r#"{"bucket":"","object_key":"","mime_type":"text/plain","expected_size":1,"expires_in":60}"#
                    .to_string(),
            ),
        ),
        (
            "POST",
            "/api/v1/medias/upload-file".to_string(),
            Some(format!(r#"{{"metadata":{metadata},"payload":"YQ=="}}"#)),
        ),
        (
            "POST",
            "/api/v1/medias/multipart/initiate".to_string(),
            Some(format!(
                r#"{{"metadata":{metadata},"desired_chunk_size":1048576}}"#
            )),
        ),
        (
            "POST",
            "/api/v1/medias/multipart/chunk".to_string(),
            Some(format!(
                r#"{{"upload_id":"{UPLOAD_ID}","chunk_index":0,"payload":"YQ=="}}"#
            )),
        ),
        (
            "POST",
            "/api/v1/medias/multipart/complete".to_string(),
            Some(session.clone()),
        ),
        (
            "POST",
            "/api/v1/medias/multipart/abort".to_string(),
            Some(session.clone()),
        ),
        (
            "POST",
            "/api/v1/medias/uploads/initiate".to_string(),
            Some(format!(r#"{{"metadata":{metadata}}}"#)),
        ),
        (
            "GET",
            format!("/api/v1/medias/uploads/status?upload_id={UPLOAD_ID}"),
            None,
        ),
        (
            "POST",
            "/api/v1/medias/uploads/presign-parts".to_string(),
            Some(format!(
                r#"{{"upload_id":"{UPLOAD_ID}","part_numbers":[1]}}"#
            )),
        ),
        (
            "POST",
            "/api/v1/medias/uploads/commit-parts".to_string(),
            Some(format!(
                r#"{{"upload_id":"{UPLOAD_ID}","parts":[{{"part_number":1,"etag":"e","size":1}}]}}"#
            )),
        ),
        (
            "POST",
            "/api/v1/medias/uploads/complete".to_string(),
            Some(session.clone()),
        ),
        ("POST", "/api/v1/medias/uploads/abort".to_string(), Some(session)),
        (
            "POST",
            "/api/v1/medias/process-image".to_string(),
            Some(format!(r#"{{"file_id":"{FILE_ID}"}}"#)),
        ),
        (
            "POST",
            "/api/v1/medias/process-video".to_string(),
            Some(format!(r#"{{"file_id":"{FILE_ID}"}}"#)),
        ),
    ]
}

/// Media text that must never reach an HTTP body: identifiers and internal rule wording.
const NEVER_ECHOED: [&str; 7] = [
    FILE_ID,
    UPLOAD_ID,
    LEGACY_CDN_KEY,
    "not authorized",
    "upload session",
    "out of range",
    "presign",
];

async fn assert_typed_rejection(
    response: axum::response::Response,
    status: StatusCode,
    reason: &str,
    context: &str,
) {
    assert_eq!(response.status(), status, "{context}");
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    let envelope: serde_json::Value =
        serde_json::from_slice(&bytes).unwrap_or_else(|_| panic!("{context}: not a JSON envelope"));
    assert_eq!(envelope["code"], status.as_u16(), "{context}");
    assert_eq!(envelope["reason"], reason, "{context}");
    assert!(envelope.get("data").is_none(), "{context}");
    let text = String::from_utf8_lossy(&bytes);
    for leaked in NEVER_ECHOED {
        assert!(!text.contains(leaked), "{context}: {envelope}");
    }
}

/// Upload, multipart, direct-upload and processing routes used to turn Media rejections into
/// 500 and echo Media's text (upload ids, object keys). They share the /files mapping now.
#[tokio::test]
async fn upload_class_routes_map_media_rejections_to_typed_statuses_without_echo() {
    let routes = upload_routes();
    assert_eq!(routes.len(), 14);
    for (verdict, status, reason) in [
        (
            MediaVerdict::ForeignUploadSession,
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
        ),
        (
            MediaVerdict::CrossTenant,
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
        ),
        (MediaVerdict::CrossOwner, StatusCode::FORBIDDEN, "FORBIDDEN"),
        (
            MediaVerdict::InvalidArgument,
            StatusCode::BAD_REQUEST,
            "BAD_REQUEST",
        ),
        (
            MediaVerdict::StorageFailure,
            StatusCode::SERVICE_UNAVAILABLE,
            "MEDIA_UNAVAILABLE",
        ),
        (
            MediaVerdict::Timeout,
            StatusCode::GATEWAY_TIMEOUT,
            "MEDIA_TIMEOUT",
        ),
        (
            MediaVerdict::InternalFailure,
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEDIA_REQUEST_FAILED",
        ),
    ] {
        let router = scripted_router(verdict).await;
        for (method, uri, body) in &routes {
            let response = call(&router, method, uri, body.clone()).await;
            assert_typed_rejection(response, status, reason, &format!("{method} {uri}")).await;
        }
    }
}

/// Media itself unreachable (nothing listens on its address): every Media route answers a
/// retryable 503, not the generic 500, and the file routes behave the same way.
#[tokio::test]
async fn unreachable_media_is_a_retryable_503_on_every_media_route() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let channel = Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_lazy();
    let (signer, _) = cell_keys();
    let router = router_with(channel, Some(signer));
    let mut routes = upload_routes();
    routes.extend(file_routes());
    for (method, uri, body) in &routes {
        let response = call(&router, method, uri, body.clone()).await;
        assert_typed_rejection(
            response,
            StatusCode::SERVICE_UNAVAILABLE,
            "MEDIA_UNAVAILABLE",
            &format!("{method} {uri}"),
        )
        .await;
    }
}

/// Contract against the real Media service (in-memory ports, production gRPC composition with
/// the signed-context layer) read through the public HTTP routes.
mod real_media {
    use super::*;
    use flare_media::application::handlers::{MediaCommandHandler, MediaQueryHandler};
    use flare_media::domain::{
        FileAccessType, MediaAssetStatus, MediaDomainConfig, MediaFileMetadata, MediaMetadataStore,
        MediaObjectRepository, MediaService, OrphanAssetClaim, STORAGE_BUCKET_METADATA_KEY,
        STORAGE_PATH_METADATA_KEY, UploadContext, UploadSession, UploadSessionStore,
    };
    use flare_media::interface::grpc::{MediaGrpcHandler, authenticated_media_service};
    use flare_server_core::context::Context;
    use flare_server_core::error::{ErrorCode, Result as FlareResult};
    use std::collections::HashMap;

    const EDGE_MEDIA: &str = "https://edge.test:18443/flare-media";
    const BUCKET: &str = "flare-media";
    const LEGACY_FILE: &str = "legacy-bare-key-file";
    const LEGACY_KEY: &str = "tenant-a/media/documents/2026/09/30/legacy-bare-key-file.txt";
    const CDN_FILE: &str = "absolute-cdn-file";
    const CDN_KEY: &str = "tenant-a/media/documents/2026/09/30/absolute-cdn-file.txt";
    const CDN_URL: &str =
        "https://cdn.example.com/tenant-a/media/documents/2026/09/30/absolute-cdn-file.txt";

    fn signed(key: &str) -> String {
        format!("{EDGE_MEDIA}/{key}?X-Amz-Signature=unit-test")
    }

    /// Private bucket behind the Edge: reads and single PUTs are signed, nothing is stored.
    struct SigningBucket;

    #[async_trait::async_trait]
    impl MediaObjectRepository for SigningBucket {
        async fn put_object(&self, _context: &UploadContext<'_>) -> FlareResult<String> {
            Err(flare_server_core::flare_err!(
                ErrorCode::InternalError,
                "this contract never streams bytes"
            ))
        }
        async fn delete_object(&self, _object_path: &str) -> FlareResult<()> {
            Ok(())
        }
        async fn presign_object(&self, object_path: &str, _expires_in: i64) -> FlareResult<String> {
            Ok(signed(object_path))
        }
        async fn presign_put_object(
            &self,
            object_path: &str,
            _content_type: &str,
            _content_length: i64,
            _expires_in: i64,
        ) -> FlareResult<String> {
            Ok(signed(object_path))
        }
        fn base_url(&self) -> Option<String> {
            Some(EDGE_MEDIA.to_string())
        }
        fn cdn_base_url(&self) -> Option<String> {
            None
        }
        fn use_presigned_urls(&self) -> bool {
            true
        }
        fn bucket_name(&self) -> Option<String> {
            Some(BUCKET.to_string())
        }
    }

    /// `media_assets` stand-in keyed like the real table: (tenant, file_id).
    #[derive(Default)]
    struct Rows(StdMutex<HashMap<(String, String), MediaFileMetadata>>);

    fn tenant_of(ctx: &Context) -> String {
        ctx.tenant_id().unwrap_or_default().to_string()
    }

    #[async_trait::async_trait]
    impl MediaMetadataStore for Rows {
        async fn save_metadata(
            &self,
            ctx: &Context,
            metadata: &MediaFileMetadata,
        ) -> FlareResult<()> {
            self.0
                .lock()
                .unwrap()
                .insert((tenant_of(ctx), metadata.file_id.clone()), metadata.clone());
            Ok(())
        }
        async fn load_metadata(
            &self,
            ctx: &Context,
            file_id: &str,
        ) -> FlareResult<Option<MediaFileMetadata>> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .get(&(tenant_of(ctx), file_id.to_string()))
                .cloned())
        }
        async fn load_by_hash(
            &self,
            _ctx: &Context,
            _sha256: &str,
        ) -> FlareResult<Option<MediaFileMetadata>> {
            Ok(None)
        }
        async fn delete_metadata(&self, ctx: &Context, file_id: &str) -> FlareResult<()> {
            self.0
                .lock()
                .unwrap()
                .remove(&(tenant_of(ctx), file_id.to_string()));
            Ok(())
        }
        async fn list_orphaned_assets(
            &self,
            _ctx: &Context,
            _before: chrono::DateTime<chrono::Utc>,
            _limit: u32,
        ) -> FlareResult<Vec<MediaFileMetadata>> {
            Ok(Vec::new())
        }
        async fn claim_orphaned_asset(
            &self,
            _ctx: &Context,
            _file_id: &str,
            _before: chrono::DateTime<chrono::Utc>,
        ) -> FlareResult<Option<Box<dyn OrphanAssetClaim>>> {
            Ok(None)
        }
        async fn update_status(
            &self,
            _ctx: &Context,
            _file_id: &str,
            _status: MediaAssetStatus,
            _grace_expires_at: Option<chrono::DateTime<chrono::Utc>>,
        ) -> FlareResult<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct Sessions(StdMutex<HashMap<String, UploadSession>>);

    #[async_trait::async_trait]
    impl UploadSessionStore for Sessions {
        async fn create_session(&self, session: &UploadSession) -> FlareResult<()> {
            self.upsert_session(session).await
        }
        async fn get_session(&self, upload_id: &str) -> FlareResult<Option<UploadSession>> {
            Ok(self.0.lock().unwrap().get(upload_id).cloned())
        }
        async fn upsert_session(&self, session: &UploadSession) -> FlareResult<()> {
            self.0
                .lock()
                .unwrap()
                .insert(session.upload_id.clone(), session.clone());
            Ok(())
        }
        async fn delete_session(&self, upload_id: &str) -> FlareResult<()> {
            self.0.lock().unwrap().remove(upload_id);
            Ok(())
        }
        async fn claim_expired_sessions(
            &self,
            _expired_before: chrono::DateTime<chrono::Utc>,
            _lease_until: chrono::DateTime<chrono::Utc>,
            _limit: usize,
        ) -> FlareResult<Vec<UploadSession>> {
            Ok(Vec::new())
        }
    }

    struct Harness {
        router: Router,
        rows: Arc<Rows>,
        chunks: std::path::PathBuf,
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.chunks);
        }
    }

    async fn serve() -> Harness {
        let (signer, verifier) = cell_keys();
        let rows = Arc::new(Rows::default());
        let chunks = std::env::temp_dir().join(format!(
            "flare-gateway-media-contract-{}",
            uuid::Uuid::new_v4()
        ));
        let service = Arc::new(MediaService::new(
            Some(Arc::new(SigningBucket)),
            Some(rows.clone()),
            None,
            None,
            Some(Arc::new(Sessions::default())),
            None,
            MediaDomainConfig::new(3600, None, 60, chunks.clone(), 3600, 8 * 1024 * 1024),
        ));
        let handler = MediaGrpcHandler::new(
            Arc::new(MediaCommandHandler::new(service.clone())),
            Arc::new(MediaQueryHandler::new(service)),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(authenticated_media_service(handler, verifier))
                .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
        );
        let channel = Channel::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_lazy();
        Harness {
            router: router_with(channel, Some(signer)),
            rows,
            chunks,
        }
    }

    /// A `media_assets` row as the PostgreSQL mapping yields it: bucket and key live in the
    /// metadata JSON, `cdn_url`/`url` are whatever the writer stored at the time.
    fn stored_row(file_id: &str, key: &str, stored_cdn_url: &str) -> MediaFileMetadata {
        let metadata = HashMap::from([
            ("tenant_id".to_string(), "tenant-a".to_string()),
            (STORAGE_BUCKET_METADATA_KEY.to_string(), BUCKET.to_string()),
            (STORAGE_PATH_METADATA_KEY.to_string(), key.to_string()),
        ]);
        MediaFileMetadata {
            owner_id: Some("user".to_string()),
            file_id: file_id.to_string(),
            file_name: "note.txt".to_string(),
            mime_type: "text/plain".to_string(),
            file_size: 61,
            url: stored_cdn_url.to_string(),
            cdn_url: stored_cdn_url.to_string(),
            metadata,
            status: MediaAssetStatus::Active,
            reference_count: 1,
            access_type: FileAccessType::Private,
            storage_bucket: Some(BUCKET.to_string()),
            storage_path: Some(key.to_string()),
            ..Default::default()
        }
    }

    fn seed(h: &Harness, row: MediaFileMetadata) {
        h.rows
            .0
            .lock()
            .unwrap()
            .insert(("tenant-a".to_string(), row.file_id.clone()), row);
    }

    async fn json_data(response: axum::response::Response) -> serde_json::Value {
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let envelope: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        envelope["data"].clone()
    }

    fn location(response: &axum::response::Response) -> &str {
        response
            .headers()
            .get("location")
            .expect("redirect location")
            .to_str()
            .unwrap()
    }

    /// Rows written before the cdn_url fix carry the bare object key as `cdn_url` (19 such
    /// rows on p0e2e). Read through HTTP they must fall back to the signed URL, never to a
    /// relative Location or a bare key presented as a URL.
    #[tokio::test]
    async fn legacy_bare_key_row_reads_back_as_the_signed_url_over_http() {
        let h = serve().await;
        seed(&h, stored_row(LEGACY_FILE, LEGACY_KEY, LEGACY_KEY));
        seed(&h, stored_row(CDN_FILE, CDN_KEY, CDN_URL));

        let response = call(
            &h.router,
            "GET",
            &format!("/api/v1/medias/files/{LEGACY_FILE}"),
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(location(&response), signed(LEGACY_KEY));

        let info = json_data(
            call(
                &h.router,
                "GET",
                &format!("/api/v1/medias/file-info?file_id={LEGACY_FILE}"),
                None,
            )
            .await,
        )
        .await;
        assert_eq!(info["file_id"], LEGACY_FILE);
        assert!(info.get("cdn_url").is_none(), "{info}");

        let url = json_data(
            call(
                &h.router,
                "POST",
                "/api/v1/medias/file-url",
                Some(format!(r#"{{"file_id":"{LEGACY_FILE}"}}"#)),
            )
            .await,
        )
        .await;
        assert_eq!(url["url"], signed(LEGACY_KEY));
        assert!(url.get("cdn_url").is_none(), "{url}");

        // Control: a row with a real absolute CDN URL still redirects there, so the fallback
        // above is the legacy filter at work rather than the CDN path being ignored.
        let response = call(
            &h.router,
            "GET",
            &format!("/api/v1/medias/files/{CDN_FILE}"),
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(location(&response), CDN_URL);
    }

    /// `upload-url` hands out a server-generated key whose first segment is the caller's
    /// tenant; caller-chosen keys and foreign buckets are typed rejections without echo.
    #[tokio::test]
    async fn upload_url_key_is_tenant_scoped_through_the_real_media_service() {
        let h = serve().await;
        let body = |bucket: &str, key: &str| {
            Some(format!(
                r#"{{"bucket":"{bucket}","object_key":"{key}","mime_type":"text/plain","expected_size":1,"expires_in":60}}"#
            ))
        };
        let data =
            json_data(call(&h.router, "POST", "/api/v1/medias/upload-url", body("", "")).await)
                .await;
        let key = data["object_key"].as_str().unwrap();
        assert_eq!(key.split('/').next(), Some("tenant-a"), "{data}");
        assert_eq!(data["upload_url"], format!("{EDGE_MEDIA}/{key}"));

        for (bucket, key, status, reason) in [
            (
                "",
                "tenant-b/media/documents/x.txt",
                StatusCode::BAD_REQUEST,
                "BAD_REQUEST",
            ),
            (
                "",
                "uploads/test.jpg",
                StatusCode::BAD_REQUEST,
                "BAD_REQUEST",
            ),
            ("media-bucket", "", StatusCode::FORBIDDEN, "FORBIDDEN"),
        ] {
            let response = call(
                &h.router,
                "POST",
                "/api/v1/medias/upload-url",
                body(bucket, key),
            )
            .await;
            let context = format!("bucket={bucket:?} key={key:?}");
            assert_eq!(response.status(), status, "{context}");
            let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .unwrap();
            let envelope: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(envelope["reason"], reason, "{context}");
            let text = String::from_utf8_lossy(&bytes);
            for leaked in ["tenant-b", "media-bucket", "assigned", "outside"] {
                assert!(!text.contains(leaked), "{context}: {envelope}");
            }
        }
    }

    /// A direct-upload session belongs to its creator: another end user of the same tenant
    /// gets 404 on every session route, and the upload id never comes back in the body.
    #[tokio::test]
    async fn foreign_upload_session_is_not_found_on_every_session_route() {
        let h = serve().await;
        let initiated = json_data(
            call_as(
                &h.router,
                "other-user",
                "POST",
                "/api/v1/medias/uploads/initiate",
                Some(
                    r#"{"metadata":{"file_name":"a.txt","mime_type":"text/plain","file_size":1,"file_type":0,"upload_id":""}}"#
                        .to_string(),
                ),
            )
            .await,
        )
        .await;
        let upload_id = initiated["upload_id"].as_str().unwrap().to_string();
        let object_key = initiated["object_key"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        assert!(!upload_id.is_empty(), "{initiated}");

        // The creator can read its own session: the routes below are live, not mis-wired.
        let own = json_data(
            call_as(
                &h.router,
                "other-user",
                "GET",
                &format!("/api/v1/medias/uploads/status?upload_id={upload_id}"),
                None,
            )
            .await,
        )
        .await;
        assert_eq!(own["upload_id"], upload_id.as_str());

        let session = format!(r#"{{"upload_id":"{upload_id}"}}"#);
        for (method, uri, body) in [
            (
                "GET",
                format!("/api/v1/medias/uploads/status?upload_id={upload_id}"),
                None,
            ),
            (
                "POST",
                "/api/v1/medias/uploads/presign-parts".to_string(),
                Some(format!(
                    r#"{{"upload_id":"{upload_id}","part_numbers":[1]}}"#
                )),
            ),
            (
                "POST",
                "/api/v1/medias/uploads/commit-parts".to_string(),
                Some(format!(
                    r#"{{"upload_id":"{upload_id}","parts":[{{"part_number":1,"etag":"e","size":1}}]}}"#
                )),
            ),
            (
                "POST",
                "/api/v1/medias/uploads/complete".to_string(),
                Some(session.clone()),
            ),
            (
                "POST",
                "/api/v1/medias/uploads/abort".to_string(),
                Some(session.clone()),
            ),
        ] {
            let response = call(&h.router, method, &uri, body).await;
            let context = format!("{method} {uri}");
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{context}");
            let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .unwrap();
            let envelope: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(envelope["reason"], "NOT_FOUND", "{context}");
            let text = String::from_utf8_lossy(&bytes);
            assert!(!text.contains(&upload_id), "{context}: {envelope}");
            assert!(
                object_key.is_empty() || !text.contains(&object_key),
                "{context}"
            );
            assert!(!text.contains("upload session"), "{context}: {envelope}");
        }
    }
}
