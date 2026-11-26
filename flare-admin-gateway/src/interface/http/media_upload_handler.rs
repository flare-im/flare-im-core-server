//! Staff image uploads. Tenant and actor are taken exclusively from the verified assertion.
use super::{
    admin_extract::{AdminJson, AdminQuery},
    audit_target::AuditTarget,
    media_handler::AdminMediaIdentity,
    ops_handler::{admin_rpc_error, audit_write, verified_tenant},
};
use axum::{
    Json,
    extract::{Extension, Path},
    http::HeaderMap,
};
use flare_grpc_proto::media::*;
use flare_im_core_service_kit::clients::AdminGrpcClients;
use flare_server_core::{
    AuthenticatedPrincipal,
    http::{ApiResponse, HttpApiError, Result},
};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AdminImagePurpose {
    Avatar,
    Image,
}
impl AdminImagePurpose {
    fn wire(self) -> i32 {
        match self {
            Self::Avatar => 1,
            Self::Image => 2,
        }
    }
}

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AdminUploadPolicyQuery {
    pub purpose: AdminImagePurpose,
}
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct AdminUploadPolicy {
    pub tenant_id: String,
    pub purpose: AdminImagePurpose,
    pub max_bytes: u64,
    pub max_pixels: u64,
    pub max_dimension: u32,
    pub allowed_mime_types: Vec<String>,
}
#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AdminImageUploadRequest {
    pub purpose: AdminImagePurpose,
    pub file_name: String,
    pub mime_type: String,
    pub file_size: i64,
}
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AdminUploadTransport {
    SinglePut,
    MultipartPut,
}
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct AdminImageUploadSession {
    pub tenant_id: String,
    pub upload_id: String,
    pub file_id: String,
    pub transport_kind: AdminUploadTransport,
    pub part_size: i64,
    pub total_parts: u32,
    /// A server-signed storage URL. Send only upload_headers, never Staff credentials.
    pub upload_url: String,
    pub upload_headers: HashMap<String, String>,
    pub expires_at_unix_seconds: i64,
}
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct AdminUploadStatus {
    pub tenant_id: String,
    pub upload_id: String,
    pub file_id: String,
    pub transport_kind: AdminUploadTransport,
    pub part_size: i64,
    pub total_size: i64,
    pub total_parts: u32,
    pub uploaded_parts: Vec<AdminUploadedPart>,
    pub expires_at_unix_seconds: i64,
}
#[derive(Debug, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AdminUploadedPart {
    pub part_number: u32,
    pub etag: String,
    pub size: i64,
    #[serde(default)]
    pub sha256: String,
}
#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AdminUploadId {
    pub upload_id: String,
}
#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AdminPresignPartsRequest {
    pub upload_id: String,
    pub part_numbers: Vec<u32>,
}
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct AdminPresignedPart {
    pub part_number: u32,
    pub upload_url: String,
    pub headers: HashMap<String, String>,
}
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct AdminPresignedParts {
    pub parts: Vec<AdminPresignedPart>,
}
#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AdminCommitPartsRequest {
    pub upload_id: String,
    pub parts: Vec<AdminUploadedPart>,
}
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct AdminCommittedParts {
    pub committed_parts: Vec<u32>,
    pub uploaded_size: i64,
}
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct AdminUploadedImage {
    pub tenant_id: String,
    pub file_id: String,
    pub purpose: AdminImagePurpose,
    pub mime_type: String,
    pub size: i64,
}
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct AdminMediaAccess {
    pub tenant_id: String,
    pub file_id: String,
    pub url: String,
    pub expires_at_unix_seconds: i64,
}

fn transport(value: i32) -> Result<AdminUploadTransport> {
    match value {
        1 => Ok(AdminUploadTransport::SinglePut),
        2 => Ok(AdminUploadTransport::MultipartPut),
        _ => Err(HttpApiError::bad_request(
            "ADMIN_MEDIA_TRANSPORT_UNSUPPORTED",
            "unsupported upload transport",
        )),
    }
}
fn purpose(value: i32) -> Result<AdminImagePurpose> {
    match value {
        1 => Ok(AdminImagePurpose::Avatar),
        2 => Ok(AdminImagePurpose::Image),
        _ => Err(HttpApiError::bad_request(
            "ADMIN_MEDIA_PURPOSE_UNSUPPORTED",
            "unsupported media purpose",
        )),
    }
}

#[utoipa::path(get, path = "/api/v1/admin/medias/upload-policy", tag = "Admin", params(("purpose" = AdminImagePurpose, Query, description = "Image purpose")), responses(
    (status = 200, description = "Success", body = ApiResponse<AdminUploadPolicy>),
    (status = 400, description = "Media refused an argument (BAD_REQUEST)"),
    (status = 401, description = "Staff context is missing or invalid"),
    (status = 403, description = "Signed route permission denied (ADMIN_PERMISSION_DENIED), required write audit headers missing, or Media refused the actor or application (FORBIDDEN)"),
    (status = 404, description = "Media or upload session not found (NOT_FOUND)"),
    (status = 500, description = "Media failed (ADMIN_MEDIA_POLICY_FAILED)"),
    (status = 502, description = "Media rejected the signed assertion (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
    (status = 503, description = "Media unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
    (status = 504, description = "Media timed out (ADMIN_UPSTREAM_TIMEOUT)"),
))]
pub async fn upload_policy(
    AdminMediaIdentity(caller): AdminMediaIdentity,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    AdminQuery(query): AdminQuery<AdminUploadPolicyQuery>,
) -> Result<Json<ApiResponse<AdminUploadPolicy>>> {
    let response = clients
        .bounded(async {
            clients
                .media
                .lock()
                .await
                .get_admin_upload_policy(
                    &caller,
                    AdminUploadPolicyRequest {
                        purpose: query.purpose.wire(),
                    },
                )
                .await
        })
        .await
        .map_err(|e| admin_rpc_error(e, "ADMIN_MEDIA_POLICY_FAILED"))?;
    Ok(Json(ApiResponse::success(AdminUploadPolicy {
        tenant_id: response.tenant_id,
        purpose: query.purpose,
        max_bytes: response.max_bytes,
        max_pixels: response.max_pixels,
        max_dimension: response.max_dimension,
        allowed_mime_types: response.allowed_mime_types,
    })))
}

#[utoipa::path(post, path = "/api/v1/admin/medias/uploads/initiate", tag = "Admin", request_body = AdminImageUploadRequest, responses(
    (status = 200, description = "Success", body = ApiResponse<AdminImageUploadSession>),
    (status = 400, description = "Media refused an argument (BAD_REQUEST)"),
    (status = 401, description = "Staff context is missing or invalid"),
    (status = 403, description = "Signed route permission denied (ADMIN_PERMISSION_DENIED), required write audit headers missing, or Media refused the actor or application (FORBIDDEN)"),
    (status = 404, description = "Media or upload session not found (NOT_FOUND)"),
    (status = 500, description = "Media failed (ADMIN_MEDIA_INITIATE_FAILED)"),
    (status = 502, description = "Media rejected the signed assertion (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
    (status = 503, description = "Media unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
    (status = 504, description = "Media timed out (ADMIN_UPSTREAM_TIMEOUT)"),
))]
pub async fn initiate_upload(
    AdminMediaIdentity(caller): AdminMediaIdentity,
    headers: HeaderMap,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    AdminJson(request): AdminJson<AdminImageUploadRequest>,
) -> Result<Json<ApiResponse<AdminImageUploadSession>>> {
    let tenant_id = verified_tenant(&principal)?;
    audit_write(
        "medias.upload.initiate",
        &principal,
        &headers,
        &AuditTarget::id(&tenant_id),
    );
    let mime_type = request.mime_type.clone();
    let response = clients
        .bounded(async {
            clients
                .media
                .lock()
                .await
                .admin_initiate_direct_upload(
                    &caller,
                    AdminInitiateDirectUploadRequest {
                        purpose: request.purpose.wire(),
                        file_name: request.file_name,
                        mime_type: request.mime_type,
                        file_size: request.file_size,
                    },
                )
                .await
        })
        .await
        .map_err(|e| admin_rpc_error(e, "ADMIN_MEDIA_INITIATE_FAILED"))?;
    Ok(Json(ApiResponse::success(AdminImageUploadSession {
        tenant_id,
        upload_id: response.upload_id,
        file_id: response.file_id,
        transport_kind: transport(response.transport_kind)?,
        part_size: response.part_size,
        total_parts: response.total_parts,
        upload_url: response.upload_url,
        upload_headers: HashMap::from([("Content-Type".into(), mime_type)]),
        expires_at_unix_seconds: response.expires_at.map(|t| t.seconds).unwrap_or_default(),
    })))
}

#[utoipa::path(get, path = "/api/v1/admin/medias/uploads/{upload_id}", tag = "Admin", params(("upload_id" = String, Path, description = "Upload session ID")), responses(
    (status = 200, description = "Success", body = ApiResponse<AdminUploadStatus>),
    (status = 400, description = "Media refused an argument (BAD_REQUEST)"),
    (status = 401, description = "Staff context is missing or invalid"),
    (status = 403, description = "Signed route permission denied (ADMIN_PERMISSION_DENIED), required write audit headers missing, or Media refused the actor or application (FORBIDDEN)"),
    (status = 404, description = "Media or upload session not found (NOT_FOUND)"),
    (status = 500, description = "Media failed (ADMIN_MEDIA_STATUS_FAILED)"),
    (status = 502, description = "Media rejected the signed assertion (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
    (status = 503, description = "Media unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
    (status = 504, description = "Media timed out (ADMIN_UPSTREAM_TIMEOUT)"),
))]
pub async fn upload_status(
    AdminMediaIdentity(caller): AdminMediaIdentity,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Path(upload_id): Path<String>,
) -> Result<Json<ApiResponse<AdminUploadStatus>>> {
    let tenant_id = verified_tenant(&principal)?;
    let response = clients
        .bounded(async {
            clients
                .media
                .lock()
                .await
                .get_direct_upload_status(&caller, GetDirectUploadStatusRequest { upload_id })
                .await
        })
        .await
        .map_err(|e| admin_rpc_error(e, "ADMIN_MEDIA_STATUS_FAILED"))?;
    Ok(Json(ApiResponse::success(AdminUploadStatus {
        tenant_id,
        upload_id: response.upload_id,
        file_id: response.file_id,
        transport_kind: transport(response.transport_kind)?,
        part_size: response.part_size,
        total_size: response.total_size,
        total_parts: response.total_parts,
        uploaded_parts: response
            .uploaded_parts
            .into_iter()
            .map(|p| AdminUploadedPart {
                part_number: p.part_number,
                etag: p.etag,
                size: p.size,
                sha256: p.sha256,
            })
            .collect(),
        expires_at_unix_seconds: response.expires_at.map(|t| t.seconds).unwrap_or_default(),
    })))
}

#[utoipa::path(post, path = "/api/v1/admin/medias/uploads/presign-parts", tag = "Admin", request_body = AdminPresignPartsRequest, responses(
    (status = 200, description = "Success", body = ApiResponse<AdminPresignedParts>),
    (status = 400, description = "Media refused an argument (BAD_REQUEST)"),
    (status = 401, description = "Staff context is missing or invalid"),
    (status = 403, description = "Signed route permission denied (ADMIN_PERMISSION_DENIED), required write audit headers missing, or Media refused the actor or application (FORBIDDEN)"),
    (status = 404, description = "Media or upload session not found (NOT_FOUND)"),
    (status = 500, description = "Media failed (ADMIN_MEDIA_PRESIGN_FAILED)"),
    (status = 502, description = "Media rejected the signed assertion (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
    (status = 503, description = "Media unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
    (status = 504, description = "Media timed out (ADMIN_UPSTREAM_TIMEOUT)"),
))]
pub async fn presign_parts(
    AdminMediaIdentity(caller): AdminMediaIdentity,
    headers: HeaderMap,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    AdminJson(request): AdminJson<AdminPresignPartsRequest>,
) -> Result<Json<ApiResponse<AdminPresignedParts>>> {
    audit_write(
        "medias.upload.presign_parts",
        &principal,
        &headers,
        &AuditTarget::id(&request.upload_id),
    );
    let response = clients
        .bounded(async {
            clients
                .media
                .lock()
                .await
                .presign_direct_upload_parts(
                    &caller,
                    PresignDirectUploadPartsRequest {
                        upload_id: request.upload_id,
                        part_numbers: request.part_numbers,
                        expires_in: 300,
                    },
                )
                .await
        })
        .await
        .map_err(|e| admin_rpc_error(e, "ADMIN_MEDIA_PRESIGN_FAILED"))?;
    Ok(Json(ApiResponse::success(AdminPresignedParts {
        parts: response
            .parts
            .into_iter()
            .map(|p| AdminPresignedPart {
                part_number: p.part_number,
                upload_url: p.upload_url,
                headers: p.headers,
            })
            .collect(),
    })))
}

#[utoipa::path(post, path = "/api/v1/admin/medias/uploads/commit-parts", tag = "Admin", request_body = AdminCommitPartsRequest, responses(
    (status = 200, description = "Success", body = ApiResponse<AdminCommittedParts>),
    (status = 400, description = "Media refused an argument (BAD_REQUEST)"),
    (status = 401, description = "Staff context is missing or invalid"),
    (status = 403, description = "Signed route permission denied (ADMIN_PERMISSION_DENIED), required write audit headers missing, or Media refused the actor or application (FORBIDDEN)"),
    (status = 404, description = "Media or upload session not found (NOT_FOUND)"),
    (status = 500, description = "Media failed (ADMIN_MEDIA_COMMIT_FAILED)"),
    (status = 502, description = "Media rejected the signed assertion (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
    (status = 503, description = "Media unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
    (status = 504, description = "Media timed out (ADMIN_UPSTREAM_TIMEOUT)"),
))]
pub async fn commit_parts(
    AdminMediaIdentity(caller): AdminMediaIdentity,
    headers: HeaderMap,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    AdminJson(request): AdminJson<AdminCommitPartsRequest>,
) -> Result<Json<ApiResponse<AdminCommittedParts>>> {
    audit_write(
        "medias.upload.commit_parts",
        &principal,
        &headers,
        &AuditTarget::id(&request.upload_id),
    );
    let response = clients
        .bounded(async {
            clients
                .media
                .lock()
                .await
                .commit_direct_upload_parts(
                    &caller,
                    CommitDirectUploadPartsRequest {
                        upload_id: request.upload_id,
                        parts: request
                            .parts
                            .into_iter()
                            .map(|p| UploadedPartInfo {
                                part_number: p.part_number,
                                etag: p.etag,
                                size: p.size,
                                sha256: p.sha256,
                            })
                            .collect(),
                    },
                )
                .await
        })
        .await
        .map_err(|e| admin_rpc_error(e, "ADMIN_MEDIA_COMMIT_FAILED"))?;
    Ok(Json(ApiResponse::success(AdminCommittedParts {
        committed_parts: response.committed_parts,
        uploaded_size: response.uploaded_size,
    })))
}

#[utoipa::path(post, path = "/api/v1/admin/medias/uploads/complete", tag = "Admin", request_body = AdminUploadId, responses(
    (status = 200, description = "Success", body = ApiResponse<AdminUploadedImage>),
    (status = 400, description = "Media refused an argument (BAD_REQUEST)"),
    (status = 401, description = "Staff context is missing or invalid"),
    (status = 403, description = "Signed route permission denied (ADMIN_PERMISSION_DENIED), required write audit headers missing, or Media refused the actor or application (FORBIDDEN)"),
    (status = 404, description = "Media or upload session not found (NOT_FOUND)"),
    (status = 500, description = "Media failed (ADMIN_MEDIA_COMPLETE_FAILED)"),
    (status = 502, description = "Media rejected the signed assertion (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
    (status = 503, description = "Media unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
    (status = 504, description = "Media timed out (ADMIN_UPSTREAM_TIMEOUT)"),
))]
pub async fn complete_upload(
    AdminMediaIdentity(caller): AdminMediaIdentity,
    headers: HeaderMap,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    AdminJson(request): AdminJson<AdminUploadId>,
) -> Result<Json<ApiResponse<AdminUploadedImage>>> {
    let tenant_id = verified_tenant(&principal)?;
    audit_write(
        "medias.upload.complete",
        &principal,
        &headers,
        &AuditTarget::id(&request.upload_id),
    );
    let response = clients
        .bounded(async {
            clients
                .media
                .lock()
                .await
                .complete_direct_upload(
                    &caller,
                    CompleteDirectUploadRequest {
                        upload_id: request.upload_id,
                    },
                )
                .await
        })
        .await
        .map_err(|e| admin_rpc_error(e, "ADMIN_MEDIA_COMPLETE_FAILED"))?;
    let info = response.info.ok_or_else(|| {
        HttpApiError::bad_request("ADMIN_MEDIA_INFO_MISSING", "uploaded image info missing")
    })?;
    Ok(Json(ApiResponse::success(AdminUploadedImage {
        tenant_id,
        file_id: response.file_id,
        purpose: purpose(info.purpose)?,
        mime_type: info.mime_type,
        size: info.size,
    })))
}

#[utoipa::path(post, path = "/api/v1/admin/medias/uploads/abort", tag = "Admin", request_body = AdminUploadId, responses(
    (status = 200, description = "Success", body = ApiResponse<bool>),
    (status = 400, description = "Media refused an argument (BAD_REQUEST)"),
    (status = 401, description = "Staff context is missing or invalid"),
    (status = 403, description = "Signed route permission denied (ADMIN_PERMISSION_DENIED), required write audit headers missing, or Media refused the actor or application (FORBIDDEN)"),
    (status = 404, description = "Media or upload session not found (NOT_FOUND)"),
    (status = 500, description = "Media failed (ADMIN_MEDIA_ABORT_FAILED)"),
    (status = 502, description = "Media rejected the signed assertion (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
    (status = 503, description = "Media unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
    (status = 504, description = "Media timed out (ADMIN_UPSTREAM_TIMEOUT)"),
))]
pub async fn abort_upload(
    AdminMediaIdentity(caller): AdminMediaIdentity,
    headers: HeaderMap,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    AdminJson(request): AdminJson<AdminUploadId>,
) -> Result<Json<ApiResponse<bool>>> {
    audit_write(
        "medias.upload.abort",
        &principal,
        &headers,
        &AuditTarget::id(&request.upload_id),
    );
    clients
        .bounded(async {
            clients
                .media
                .lock()
                .await
                .abort_direct_upload(
                    &caller,
                    AbortDirectUploadRequest {
                        upload_id: request.upload_id,
                    },
                )
                .await
        })
        .await
        .map_err(|e| admin_rpc_error(e, "ADMIN_MEDIA_ABORT_FAILED"))?;
    Ok(Json(ApiResponse::success(true)))
}

#[utoipa::path(get, path = "/api/v1/admin/medias/files/{file_id}/url", tag = "Admin", params(("file_id" = String, Path, description = "Persistent media file ID")), responses(
    (status = 200, description = "Success", body = ApiResponse<AdminMediaAccess>),
    (status = 400, description = "Media refused an argument (BAD_REQUEST)"),
    (status = 401, description = "Staff context is missing or invalid"),
    (status = 403, description = "Signed route permission denied (ADMIN_PERMISSION_DENIED), required write audit headers missing, or Media refused the actor or application (FORBIDDEN)"),
    (status = 404, description = "Media or upload session not found (NOT_FOUND)"),
    (status = 500, description = "Media failed (ADMIN_MEDIA_RESOLVE_FAILED)"),
    (status = 502, description = "Media rejected the signed assertion (ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
    (status = 503, description = "Media unavailable (ADMIN_UPSTREAM_UNAVAILABLE)"),
    (status = 504, description = "Media timed out (ADMIN_UPSTREAM_TIMEOUT)"),
))]
pub async fn resolve_media(
    AdminMediaIdentity(caller): AdminMediaIdentity,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(clients): Extension<Arc<AdminGrpcClients>>,
    Path(file_id): Path<String>,
) -> Result<Json<ApiResponse<AdminMediaAccess>>> {
    let tenant_id = verified_tenant(&principal)?;
    let response = clients
        .bounded(async {
            clients
                .media
                .lock()
                .await
                .get_file_url(
                    &caller,
                    GetFileUrlRequest {
                        file_id: file_id.clone(),
                        expires_in: 300,
                        ..Default::default()
                    },
                )
                .await
        })
        .await
        .map_err(|e| admin_rpc_error(e, "ADMIN_MEDIA_RESOLVE_FAILED"))?;
    Ok(Json(ApiResponse::success(AdminMediaAccess {
        tenant_id,
        file_id,
        url: response.url,
        expires_at_unix_seconds: response.expires_at.map(|t| t.seconds).unwrap_or_default(),
    })))
}
