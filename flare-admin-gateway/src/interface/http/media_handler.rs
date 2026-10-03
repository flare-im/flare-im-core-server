//! Tenant-scoped media operations; reachable only through admin_auth_middleware.
//!
//! Media rejections go through the same typed mapping as the other admin routes
//! ([`admin_rpc_error`]): Media's own error text is never echoed.
use axum::{
    Json,
    extract::{Extension, FromRequestParts},
    http::{HeaderMap, request::Parts},
};
use flare_grpc_proto::media::*;
use flare_im_service_kit::clients::{GrpcClients, MediaCaller};
use flare_server_core::{
    AuthenticatedPrincipal, SignedContextSigner,
    context::Ctx,
    error::{ErrorBuilder, ErrorCode},
    http::{ApiResponse, ContextFromHeaders, HttpApiError, Result},
};
use std::sync::Arc;

use super::admin_extract::{AdminJson, AdminQuery};
use super::audit_target::AuditTarget;
use super::ops_handler::{admin_rpc_error, audit_write, verified_tenant};

/// Cell internal-context signer, used only to turn the Control-verified admin into a
/// context Media can verify. Without it every media admin route fails; nothing unsigned
/// is sent.
#[derive(Clone)]
pub struct MediaContextSigner(pub Option<SignedContextSigner>);

/// Caller of media admin RPCs: the tenant admin comes only from the verified principal
/// that admin_auth_middleware installed, never from request headers.
pub struct AdminMediaIdentity(MediaCaller);

impl<S: Send + Sync> FromRequestParts<S> for AdminMediaIdentity {
    type Rejection = HttpApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> std::result::Result<Self, Self::Rejection> {
        let signer = parts
            .extensions
            .get::<MediaContextSigner>()
            .and_then(|signer| signer.0.clone())
            .ok_or_else(|| {
                HttpApiError::internal(
                    "MEDIA_SIGNER_UNAVAILABLE",
                    "internal identity signer not configured",
                )
            })?;
        let principal = parts
            .extensions
            .get::<AuthenticatedPrincipal>()
            .filter(|principal| !principal.user_id.is_empty())
            .ok_or_else(|| forbidden("verified admin actor required"))?;
        let tenant_id = principal
            .tenant_id
            .as_deref()
            .filter(|tenant| !tenant.is_empty() && *tenant != "*")
            .ok_or_else(|| forbidden("one concrete tenant is required for admin api"))?;
        let ctx = Ctx::from_headers(&parts.headers);
        Ok(Self(MediaCaller::tenant_admin(
            ctx,
            signer,
            tenant_id,
            &principal.user_id,
        )))
    }
}

fn forbidden(message: &str) -> HttpApiError {
    HttpApiError(
        ErrorBuilder::new(ErrorCode::HttpForbidden, "ADMIN_IDENTITY_REQUIRED")
            .details(message)
            .build_error(),
    )
}

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct ListObjectsQuery {
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct AdminMediaObject {
    pub file_id: String,
    pub file_name: String,
    pub mime_type: String,
    pub size: i64,
    pub object_key: String,
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct AdminMediaObjects {
    pub files: Vec<AdminMediaObject>,
}

#[utoipa::path(post, path = "/api/v1/admin/medias/cleanup-orphaned-assets", tag = "Admin",
    request_body(content = CleanupOrphanedAssetsRequest, description = "Both fields are required; `limit` 0 means 100, capped at 1000 by Media; `dry_run` must be false"),
    params(
        ("x-audit-reason" = String, Header, description = "Operator reason, recorded in the audit log"),
        ("idempotency-key" = Option<String>, Header, description = "Required unless x-request-id is present; correlation only, not deduplicated"),
        ("x-request-id" = Option<String>, Header, description = "Required unless idempotency-key is present"),
    ),
    responses(
        (status = 200, description = "Orphaned message assets of the token tenant that this pass deleted", body = ApiResponse<CleanupOrphanedAssetsResponse>),
        (status = 400, description = "dry_run=true: Media does not honor dry runs, so the gateway refuses instead of deleting (ADMIN_MEDIA_DRY_RUN_UNSUPPORTED), or Media refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Admin token is missing or invalid"),
        (status = 403, description = "Tenant header mismatch, audit reason / idempotency header missing, or Media refused the tenant admin (FORBIDDEN)"),
        (status = 500, description = "Media failed (ADMIN_MEDIA_CLEANUP_FAILED), or the internal identity signer is not configured (MEDIA_SIGNER_UNAVAILABLE)"),
        (status = 502, description = "Media refused the signed internal context as unauthenticated, a deployment error: FLARE_INTERNAL_CTX_PUBLIC_KEY does not pair with the gateway key (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Media service is unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Media service did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ))]
pub async fn cleanup_orphaned_assets(
    AdminMediaIdentity(caller): AdminMediaIdentity,
    headers: HeaderMap,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(clients): Extension<Arc<GrpcClients>>,
    AdminJson(request): AdminJson<CleanupOrphanedAssetsRequest>,
) -> Result<Json<ApiResponse<CleanupOrphanedAssetsResponse>>> {
    // Media ignores dry_run and deletes as in a real run; a "preview" must not delete.
    if request.dry_run {
        return Err(HttpApiError::bad_request(
            "ADMIN_MEDIA_DRY_RUN_UNSUPPORTED",
            "media cleanup has no dry run; send dry_run=false to run it",
        ));
    }
    let tenant_id = verified_tenant(&principal)?;
    audit_write(
        "medias.cleanup_orphaned_assets",
        &principal,
        &headers,
        &AuditTarget::id(&tenant_id),
    );
    let response = clients
        .media
        .lock()
        .await
        .cleanup_orphaned_assets(&caller, request)
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_MEDIA_CLEANUP_FAILED"))?;
    Ok(Json(ApiResponse::success(response)))
}

#[utoipa::path(post, path = "/api/v1/admin/medias/object-acl", tag = "Admin",
    request_body(content = SetObjectAclRequest, description = "File of the token tenant and its ACL entries"),
    params(
        ("x-audit-reason" = String, Header, description = "Operator reason, recorded in the audit log"),
        ("idempotency-key" = Option<String>, Header, description = "Required unless x-request-id is present; correlation only, not deduplicated"),
        ("x-request-id" = Option<String>, Header, description = "Required unless idempotency-key is present"),
    ),
    responses(
        (status = 200, description = "ACL applied (`data: true`)", body = ApiResponse<bool>),
        (status = 400, description = "Media refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Admin token is missing or invalid"),
        (status = 403, description = "Tenant header mismatch, audit reason / idempotency header missing, or Media refused the tenant admin (FORBIDDEN)"),
        (status = 404, description = "No such file in the token tenant (NOT_FOUND)"),
        (status = 500, description = "ACL mutation is not implemented by Media (contract status not_implemented; ADMIN_MEDIA_OBJECT_ACL_FAILED), never a successful no-op; or the internal identity signer is not configured (MEDIA_SIGNER_UNAVAILABLE)"),
        (status = 502, description = "Media refused the signed internal context as unauthenticated, a deployment error: FLARE_INTERNAL_CTX_PUBLIC_KEY does not pair with the gateway key (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Media service is unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Media service did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ))]
pub async fn set_object_acl(
    AdminMediaIdentity(caller): AdminMediaIdentity,
    headers: HeaderMap,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(clients): Extension<Arc<GrpcClients>>,
    AdminJson(request): AdminJson<SetObjectAclRequest>,
) -> Result<Json<ApiResponse<bool>>> {
    audit_write(
        "medias.object_acl",
        &principal,
        &headers,
        &AuditTarget::id(&request.file_id),
    );
    clients
        .media
        .lock()
        .await
        .set_object_acl(&caller, request)
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_MEDIA_OBJECT_ACL_FAILED"))?;
    Ok(Json(ApiResponse::success(true)))
}

#[utoipa::path(get, path = "/api/v1/admin/medias/objects", tag = "Admin",
    params(("bucket" = String, Query, description = "Configured bucket"), ("prefix" = Option<String>, Query, description = "Tenant-relative prefix")),
    responses(
        (status = 200, description = "Objects of the token tenant; only file_id, file_name, mime_type, size and object_key", body = ApiResponse<AdminMediaObjects>),
        (status = 400, description = "Media refused the bucket or prefix (BAD_REQUEST)"),
        (status = 401, description = "Admin token is missing or invalid"),
        (status = 403, description = "Tenant header mismatch, or Media refused the tenant admin or the bucket (FORBIDDEN)"),
        (status = 500, description = "Object inventory is not implemented by Media (contract status not_implemented; ADMIN_MEDIA_LIST_OBJECTS_FAILED), never a fabricated empty inventory; or the internal identity signer is not configured (MEDIA_SIGNER_UNAVAILABLE)"),
        (status = 502, description = "Media refused the signed internal context as unauthenticated, a deployment error: FLARE_INTERNAL_CTX_PUBLIC_KEY does not pair with the gateway key (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Media service is unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Media service did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ))]
pub async fn list_objects(
    AdminMediaIdentity(caller): AdminMediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    AdminQuery(request): AdminQuery<ListObjectsQuery>,
) -> Result<Json<ApiResponse<AdminMediaObjects>>> {
    let response = clients
        .media
        .lock()
        .await
        .list_objects(
            &caller,
            ListObjectsRequest {
                bucket: request.bucket,
                prefix: request.prefix,
                pagination: None,
                filters: Vec::new(),
                sort: Vec::new(),
            },
        )
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_MEDIA_LIST_OBJECTS_FAILED"))?;
    Ok(Json(ApiResponse::success(AdminMediaObjects {
        files: response
            .files
            .into_iter()
            .map(|file| AdminMediaObject {
                file_id: file.file_id,
                file_name: file.file_name,
                mime_type: file.mime_type,
                size: file.size,
                object_key: file.object_key,
            })
            .collect(),
    })))
}

#[utoipa::path(get, path = "/api/v1/admin/medias/bucket", tag = "Admin",
    params(("bucket" = String, Query, description = "Configured bucket")),
    responses(
        (status = 200, description = "Configured media bucket with this tenant's object-key prefix; no global object statistics", body = ApiResponse<DescribeBucketResponse>),
        (status = 400, description = "Media refused the bucket (BAD_REQUEST)"),
        (status = 401, description = "Admin token is missing or invalid"),
        (status = 403, description = "Tenant header mismatch, or the bucket is not the one Media serves (FORBIDDEN)"),
        (status = 500, description = "Media failed (ADMIN_MEDIA_BUCKET_FAILED), or the internal identity signer is not configured (MEDIA_SIGNER_UNAVAILABLE)"),
        (status = 502, description = "Media refused the signed internal context as unauthenticated, a deployment error: FLARE_INTERNAL_CTX_PUBLIC_KEY does not pair with the gateway key (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Media service is unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Media service did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ))]
pub async fn describe_bucket(
    AdminMediaIdentity(caller): AdminMediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    AdminQuery(request): AdminQuery<DescribeBucketRequest>,
) -> Result<Json<ApiResponse<DescribeBucketResponse>>> {
    let response = clients
        .media
        .lock()
        .await
        .describe_bucket(&caller, request)
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_MEDIA_BUCKET_FAILED"))?;
    Ok(Json(ApiResponse::success(response)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_im_service_kit::clients::*;
    use flare_im_service_kit::control_auth::{
        CONSOLE_ISSUER, CORE_ADMIN_AUDIENCE, ConsoleControlVerifier,
    };
    use flare_server_core::{SignedActorType, SignedContextVerifier};
    use std::sync::Mutex as StdMutex;
    use std::task::Poll;
    use tonic::transport::Channel;
    use tower::ServiceExt;

    /// Stand-in Media endpoint recording the signed-context header of each RPC.
    #[derive(Clone, Default)]
    struct CapturingMedia(Arc<StdMutex<Vec<Option<String>>>>);

    impl tonic::server::NamedService for CapturingMedia {
        const NAME: &'static str = flare_grpc_proto::media::media_service_server::SERVICE_NAME;
    }

    impl tower::Service<axum::http::Request<tonic::body::Body>> for CapturingMedia {
        type Response = axum::http::Response<tonic::body::Body>;
        type Error = std::convert::Infallible;
        type Future = std::future::Ready<std::result::Result<Self::Response, Self::Error>>;

        fn poll_ready(
            &mut self,
            _: &mut std::task::Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
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

    fn pem_pair(seed: u8) -> (String, String) {
        let key = SigningKey::from_bytes(&[seed; 32]);
        (
            key.to_pkcs8_pem(Default::default()).unwrap().to_string(),
            key.verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap(),
        )
    }

    async fn admin_router(
        signer: Option<SignedContextSigner>,
    ) -> (axum::Router, CapturingMedia, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let media = CapturingMedia::default();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(media.clone())
                .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
        );
        let media_channel = Channel::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_lazy();
        let c = Channel::from_static("http://127.0.0.1:9").connect_lazy();
        let clients = GrpcClients {
            media: Arc::new(tokio::sync::Mutex::new(
                MediaServiceClientWrapper::from_channel(media_channel, None),
            )),
            message_ingest_send: Arc::new(tokio::sync::Mutex::new(
                MessageSendServiceClientWrapper::from_channel(c.clone()),
            )),
            message_event: Arc::new(tokio::sync::Mutex::new(
                MessageEventServiceClientWrapper::from_channel(c.clone()),
            )),
            message_action: Arc::new(tokio::sync::Mutex::new(
                MessageActionServiceClientWrapper::from_channel(c.clone()),
            )),
            conversation_read: Arc::new(tokio::sync::Mutex::new(
                ConversationReadServiceClientWrapper::from_channel(c.clone()),
            )),
            conversation_manage: Arc::new(tokio::sync::Mutex::new(
                ConversationManageServiceClientWrapper::from_channel(c.clone()),
            )),
            online: Arc::new(tokio::sync::Mutex::new(
                OnlineServiceClientWrapper::from_channel(c.clone()),
            )),
            storage_reader: Arc::new(tokio::sync::Mutex::new(
                StorageReaderServiceClientWrapper::from_channel(c),
            )),
            request_timeout: None,
        };
        let (control_private, control_public) = pem_pair(61);
        let token = flare_server_core::TokenService::from_ed25519_private_key(
            &control_private,
            CONSOLE_ISSUER,
            60,
        )
        .unwrap()
        .generate_with_ed25519("employee-1", Some("tenant-a"), "admin", CORE_ADMIN_AUDIENCE)
        .unwrap();
        let router = super::super::create_admin_router(Arc::new(clients), None)
            .layer(Extension(Arc::new(
                ConsoleControlVerifier::from_public_key(&control_public).unwrap(),
            )))
            .layer(Extension(MediaContextSigner(signer)));
        (router, media, token)
    }

    fn bucket_request(token: &str) -> Request<Body> {
        Request::get("/api/v1/admin/medias/bucket?bucket=b")
            .header("authorization", format!("Bearer {token}"))
            .header("x-tenant-id", "tenant-a")
            // Spoofed user/actor-type headers must not shape the signed Media identity.
            .header("x-user-id", "victim-user")
            .header("x-actor-type", "system")
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn admin_media_rpc_carries_signed_tenant_admin_of_verified_actor() {
        let (cell_private, cell_public) = pem_pair(62);
        let signer = SignedContextSigner::from_private_key(&cell_private).unwrap();
        let (router, media, token) = admin_router(Some(signer)).await;
        let response = router.oneshot(bucket_request(&token)).await.unwrap();
        assert_ne!(response.status(), StatusCode::OK);
        let captured = media.0.lock().unwrap().clone();
        assert_eq!(captured.len(), 1);
        let claims = SignedContextVerifier::from_public_key(&cell_public)
            .unwrap()
            .verify(captured[0].as_deref().expect("signed context header"))
            .unwrap();
        assert_eq!(claims.actor_type, SignedActorType::TenantAdmin);
        assert_eq!(claims.tenant_id, "tenant-a");
        assert_eq!(claims.actor_id, "employee-1");
    }

    #[tokio::test]
    async fn admin_media_route_without_signer_never_calls_media() {
        let (router, media, token) = admin_router(None).await;
        let response = router.oneshot(bucket_request(&token)).await.unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(media.0.lock().unwrap().is_empty());
    }
}
