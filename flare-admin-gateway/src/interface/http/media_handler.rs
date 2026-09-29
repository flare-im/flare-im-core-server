//! Tenant-scoped media operations; reachable only through admin_auth_middleware.
use axum::{
    Json,
    extract::{Extension, Query},
    http::HeaderMap,
};
use flare_grpc_proto::media::*;
use flare_im_service_kit::clients::GrpcClients;
use flare_server_core::{
    context::Ctx,
    http::{ApiResponse, ContextFromHeaders, Result},
};
use std::sync::Arc;

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
    responses((status = 200, description = "Tenant-scoped cleanup; limit defaults to 100, capped at 1000")))]
pub async fn cleanup_orphaned_assets(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(request): Json<CleanupOrphanedAssetsRequest>,
) -> Result<Json<ApiResponse<CleanupOrphanedAssetsResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let response = clients
        .media
        .lock()
        .await
        .cleanup_orphaned_assets(&ctx, request)
        .await?;
    Ok(Json(ApiResponse::success(response)))
}

#[utoipa::path(post, path = "/api/v1/admin/medias/object-acl", tag = "Admin",
    responses((status = 500, description = "ACL mutation is not implemented; never reports a successful no-op")))]
pub async fn set_object_acl(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(request): Json<SetObjectAclRequest>,
) -> Result<Json<ApiResponse<bool>>> {
    let ctx = Ctx::from_headers(&headers);
    clients
        .media
        .lock()
        .await
        .set_object_acl(&ctx, request)
        .await?;
    Ok(Json(ApiResponse::success(true)))
}

#[utoipa::path(get, path = "/api/v1/admin/medias/objects", tag = "Admin",
    params(("bucket" = String, Query, description = "Configured bucket"), ("prefix" = Option<String>, Query, description = "Tenant-relative prefix")),
    responses((status = 500, description = "Object inventory is not implemented; never returns a fabricated empty inventory")))]
pub async fn list_objects(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Query(request): Query<ListObjectsQuery>,
) -> Result<Json<ApiResponse<AdminMediaObjects>>> {
    let ctx = Ctx::from_headers(&headers);
    let response = clients
        .media
        .lock()
        .await
        .list_objects(
            &ctx,
            ListObjectsRequest {
                bucket: request.bucket,
                prefix: request.prefix,
                pagination: None,
                filters: Vec::new(),
                sort: Vec::new(),
            },
        )
        .await?;
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
    responses((status = 200, description = "Configured media bucket with this tenant's object-key prefix; no global object statistics")))]
pub async fn describe_bucket(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Query(request): Query<DescribeBucketRequest>,
) -> Result<Json<ApiResponse<DescribeBucketResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let response = clients
        .media
        .lock()
        .await
        .describe_bucket(&ctx, request)
        .await?;
    Ok(Json(ApiResponse::success(response)))
}
