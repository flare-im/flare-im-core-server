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
use flare_im_service_kit::clients::{AdminGrpcClients, MediaCaller};
use flare_server_core::{
    AuthenticatedPrincipal,
    error::{ErrorBuilder, ErrorCode},
    http::{ApiResponse, HttpApiError, Result},
};
use std::sync::Arc;

use super::admin_auth_middleware::VerifiedStaffContext;
use super::admin_extract::{AdminJson, AdminQuery};
use super::audit_target::AuditTarget;
use super::ops_handler::{admin_rpc_error, audit_write, verified_tenant};

/// Media admin RPCs forward the verified Staff assertion unchanged; no private key here.
pub struct AdminMediaIdentity(MediaCaller);

impl<S: Send + Sync> FromRequestParts<S> for AdminMediaIdentity {
    type Rejection = HttpApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> std::result::Result<Self, Self::Rejection> {
        let staff = parts
            .extensions
            .get::<VerifiedStaffContext>()
            .ok_or_else(|| forbidden("verified staff context required"))?;
        Ok(Self(MediaCaller::staff(staff.ctx().clone()).map_err(
            |_| forbidden("verified staff context required"),
        )?))
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
    #[serde(default)]
    pub cursor: String,
    #[serde(default = "default_media_limit")]
    pub limit: i32,
}
fn default_media_limit() -> i32 {
    50
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
    pub has_more: bool,
    pub next_cursor: String,
}

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AdminMediaCleanupRequest {
    pub limit: u32,
    pub dry_run: bool,
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct AdminMediaCleanupResponse {
    pub file_ids: Vec<String>,
    pub success: bool,
    pub error_message: String,
    pub scanned: u32,
}

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AdminMediaAclEntry {
    pub principal: String,
    pub permissions: Vec<String>,
}

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AdminMediaAclRequest {
    pub file_id: String,
    pub entries: Vec<AdminMediaAclEntry>,
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct AdminMediaBucketResponse {
    pub bucket: String,
    pub region: String,
    pub storage_class: String,
    pub versioning_enabled: bool,
    pub metadata: std::collections::HashMap<String, String>,
}

#[utoipa::path(post, path = "/api/v1/admin/medias/cleanup-orphaned-assets", tag = "Admin",
    request_body(content = AdminMediaCleanupRequest, description = "Both fields are required; `limit` 0 means 100, capped at 1000 by Media; `dry_run` must be false"),
    params(
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8 (encodeURIComponent; plain ASCII may be sent as is), at most 512 bytes decoded and 1536 bytes as sent; recorded decoded in the audit log"),
        ("idempotency-key" = Option<String>, Header, description = "Required unless x-request-id is present; correlation only, not deduplicated"),
        ("x-request-id" = Option<String>, Header, description = "Required unless idempotency-key is present"),
    ),
    responses(
        (status = 200, description = "Orphaned message assets of the staff application that this pass deleted", body = ApiResponse<AdminMediaCleanupResponse>),
        (status = 400, description = "dry_run=true is refused before deletion (ADMIN_MEDIA_DRY_RUN_UNSUPPORTED), or Media refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:medias:write (ADMIN_PERMISSION_DENIED), audit reason / idempotency header missing (FORBIDDEN), or Media refused the Staff application or permission (FORBIDDEN)"),
        (status = 500, description = "Media failed (ADMIN_MEDIA_CLEANUP_FAILED)"),
        (status = 502, description = "Media refused the signed internal context as unauthenticated, a deployment error: Media's key set does not hold the gateway key (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Media service is unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Media service did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ))]
pub async fn cleanup_orphaned_assets(
    AdminMediaIdentity(caller): AdminMediaIdentity,
    headers: HeaderMap,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    AdminJson(request): AdminJson<AdminMediaCleanupRequest>,
) -> Result<Json<ApiResponse<AdminMediaCleanupResponse>>> {
    // Refuse previews at both boundaries; neither the gateway nor direct Media RPC may delete.
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
        .bounded(async {
            clients
                .media
                .lock()
                .await
                .cleanup_orphaned_assets(
                    &caller,
                    CleanupOrphanedAssetsRequest {
                        limit: request.limit,
                        dry_run: request.dry_run,
                    },
                )
                .await
        })
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_MEDIA_CLEANUP_FAILED"))?;
    Ok(Json(ApiResponse::success(AdminMediaCleanupResponse {
        file_ids: response.file_ids,
        success: response.success,
        error_message: response.error_message,
        scanned: response.scanned,
    })))
}

#[utoipa::path(post, path = "/api/v1/admin/medias/object-acl", tag = "Admin",
    request_body(content = AdminMediaAclRequest, description = "Replace at most 128 application read grants: principal=user:<IM user id>, permissions=[read]. Empty entries revoke explicit grants; owner/public policy and delete rights are unchanged. Not an S3 bucket ACL."),
    params(
        ("x-audit-reason" = String, Header, description = "Operator reason as percent-encoded UTF-8 (encodeURIComponent; plain ASCII may be sent as is), at most 512 bytes decoded and 1536 bytes as sent; recorded decoded in the audit log"),
        ("idempotency-key" = Option<String>, Header, description = "Required unless x-request-id is present; correlation only, not deduplicated"),
        ("x-request-id" = Option<String>, Header, description = "Required unless idempotency-key is present"),
    ),
    responses(
        (status = 200, description = "ACL applied (`data: true`)", body = ApiResponse<bool>),
        (status = 400, description = "Media refused an argument (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:medias:write (ADMIN_PERMISSION_DENIED), audit reason / idempotency header missing (FORBIDDEN), or Media refused the Staff application or permission (FORBIDDEN)"),
        (status = 404, description = "No such file in the staff application (NOT_FOUND)"),
        (status = 500, description = "Media ACL transaction failed (ADMIN_MEDIA_OBJECT_ACL_FAILED)"),
        (status = 502, description = "Media refused the signed internal context as unauthenticated, a deployment error: Media's key set does not hold the gateway key (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Media service is unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Media service did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ))]
pub async fn set_object_acl(
    AdminMediaIdentity(caller): AdminMediaIdentity,
    headers: HeaderMap,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    AdminJson(request): AdminJson<AdminMediaAclRequest>,
) -> Result<Json<ApiResponse<bool>>> {
    audit_write(
        "medias.object_acl",
        &principal,
        &headers,
        &AuditTarget::id(&request.file_id),
    );
    clients
        .bounded(async {
            clients
                .media
                .lock()
                .await
                .set_object_acl(
                    &caller,
                    SetObjectAclRequest {
                        file_id: request.file_id,
                        entries: request
                            .entries
                            .into_iter()
                            .map(|entry| AccessControlEntry {
                                principal: entry.principal,
                                permissions: entry.permissions,
                            })
                            .collect(),
                    },
                )
                .await
        })
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_MEDIA_OBJECT_ACL_FAILED"))?;
    Ok(Json(ApiResponse::success(true)))
}

#[utoipa::path(get, path = "/api/v1/admin/medias/objects", tag = "Admin",
    params(("bucket" = String, Query, description = "Configured bucket"), ("prefix" = Option<String>, Query, description = "Application-relative registered object prefix"), ("cursor" = Option<String>, Query, description = "Last file_id of preceding page"), ("limit" = Option<i32>, Query, description = "Page size 1..200, default 50")),
    responses(
        (status = 200, description = "Paged registered asset index of the staff application, not a bucket scan or proof of physical object existence; no signed URLs or metadata", body = ApiResponse<AdminMediaObjects>),
        (status = 400, description = "Media refused the bucket or prefix (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:medias:read (ADMIN_PERMISSION_DENIED), or Media refused the Staff application, permission or bucket (FORBIDDEN)"),
        (status = 500, description = "Media asset index failed (ADMIN_MEDIA_LIST_OBJECTS_FAILED)"),
        (status = 502, description = "Media refused the signed internal context as unauthenticated, a deployment error: Media's key set does not hold the gateway key (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Media service is unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Media service did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ))]
pub async fn list_objects(
    AdminMediaIdentity(caller): AdminMediaIdentity,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    AdminQuery(request): AdminQuery<ListObjectsQuery>,
) -> Result<Json<ApiResponse<AdminMediaObjects>>> {
    let response = clients
        .bounded(async {
            clients
                .media
                .lock()
                .await
                .list_objects(
                    &caller,
                    ListObjectsRequest {
                        bucket: request.bucket,
                        prefix: request.prefix,
                        pagination: Some(flare_grpc_proto::Pagination {
                            cursor: request.cursor,
                            limit: request.limit,
                            ..Default::default()
                        }),
                        filters: Vec::new(),
                        sort: Vec::new(),
                    },
                )
                .await
        })
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_MEDIA_LIST_OBJECTS_FAILED"))?;
    let page = response.pagination.unwrap_or_default();
    Ok(Json(ApiResponse::success(AdminMediaObjects {
        has_more: page.has_more,
        next_cursor: page.cursor,
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
        (status = 200, description = "Configured media bucket with this application's object-key prefix; no global object statistics", body = ApiResponse<AdminMediaBucketResponse>),
        (status = 400, description = "Media refused the bucket (BAD_REQUEST)"),
        (status = 401, description = "Staff context is missing or invalid"),
        (status = 403, description = "Signed permission is not core:medias:read (ADMIN_PERMISSION_DENIED), or the bucket is not the one Media serves (FORBIDDEN)"),
        (status = 500, description = "Media failed (ADMIN_MEDIA_BUCKET_FAILED)"),
        (status = 502, description = "Media refused the signed internal context as unauthenticated, a deployment error: Media's key set does not hold the gateway key (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
        (status = 503, description = "Media service is unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
        (status = 504, description = "Media service did not answer in time (ADMIN_UPSTREAM_TIMEOUT)"),
    ))]
pub async fn describe_bucket(
    AdminMediaIdentity(caller): AdminMediaIdentity,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    AdminQuery(request): AdminQuery<DescribeBucketRequest>,
) -> Result<Json<ApiResponse<AdminMediaBucketResponse>>> {
    let response = clients
        .bounded(async {
            clients
                .media
                .lock()
                .await
                .describe_bucket(&caller, request)
                .await
        })
        .await
        .map_err(|err| admin_rpc_error(err, "ADMIN_MEDIA_BUCKET_FAILED"))?;
    Ok(Json(ApiResponse::success(AdminMediaBucketResponse {
        bucket: response.bucket,
        region: response.region,
        storage_class: response.storage_class,
        versioning_enabled: response.versioning_enabled,
        metadata: response.metadata,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_im_service_kit::clients::*;
    use flare_server_core::auth::{KeySet, kid};
    use flare_server_core::{
        SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
    };
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

    async fn admin_router() -> (axum::Router, CapturingMedia, String) {
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
        let clients = AdminGrpcClients {
            media: Arc::new(tokio::sync::Mutex::new(
                MediaServiceClientWrapper::from_channel(media_channel, None),
            )),
            conversation_read: Arc::new(tokio::sync::Mutex::new(
                ConversationReadServiceClientWrapper::from_channel(c.clone()),
            )),
            online: Arc::new(tokio::sync::Mutex::new(
                OnlineServiceClientWrapper::from_channel(c.clone()),
            )),
            storage_reader: Arc::new(tokio::sync::Mutex::new(
                StorageReaderServiceClientWrapper::from_channel(c),
            )),
            request_timeout: None,
        };
        // The Social gateway admin partition signs the staff context with the gateway key.
        let (gateway_private, gateway_public) = pem_pair(61);
        let mut keys = KeySet::empty();
        keys.insert(kid::GATEWAY, &gateway_public).unwrap();
        let verifier = SignedContextVerifier::from_key_set(keys).require_staff_kid([kid::GATEWAY]);
        let token = SignedContextSigner::from_private_key(&gateway_private)
            .unwrap()
            .with_kid(kid::GATEWAY)
            .unwrap()
            .sign(
                &SignedContextIdentity::new(
                    SignedActorType::Staff,
                    "employee-1",
                    "tenant-a",
                    "req-media-unit",
                )
                .with_staff_grant(None, Some("core:medias:read".to_owned()), 1),
            )
            .unwrap();
        let router = super::super::create_admin_router(Arc::new(clients), None)
            .layer(Extension(Arc::new(verifier)));
        (router, media, token)
    }

    fn bucket_request(token: &str) -> Request<Body> {
        Request::get("/api/v1/admin/medias/bucket?bucket=b")
            .header("x-flare-ctx", token)
            // Spoofed identity headers must not shape the signed Media identity.
            .header("x-tenant-id", "tenant-z")
            .header("x-user-id", "victim-user")
            .header("x-actor-type", "system")
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn admin_media_rpc_forwards_original_staff_without_a_private_key() {
        let (_, gateway_public) = pem_pair(61);
        let (router, media, token) = admin_router().await;
        let response = router.oneshot(bucket_request(&token)).await.unwrap();
        assert_ne!(response.status(), StatusCode::OK);
        let captured = media.0.lock().unwrap().clone();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].as_deref(), Some(token.as_str()));
        let claims = SignedContextVerifier::from_public_key(&gateway_public)
            .unwrap()
            .verify(captured[0].as_deref().expect("signed context header"))
            .unwrap();
        assert_eq!(claims.actor_type, SignedActorType::Staff);
        assert_eq!(claims.tenant_id, "tenant-a");
        assert_eq!(claims.actor_id, "employee-1");
        assert_eq!(claims.request_id, "req-media-unit");
        assert_eq!(claims.kid, kid::GATEWAY);
        assert_eq!(claims.permission.as_deref(), Some("core:medias:read"));
    }

    #[tokio::test]
    async fn admin_media_route_without_valid_staff_never_calls_media() {
        let (router, media, _) = admin_router().await;
        let response = router
            .oneshot(bucket_request("not-a-staff-token"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(media.0.lock().unwrap().is_empty());
    }
}
