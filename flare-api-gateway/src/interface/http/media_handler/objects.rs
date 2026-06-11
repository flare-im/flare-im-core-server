use axum::{
    extract::{Extension, Json},
    http::HeaderMap,
};
use std::sync::Arc;
use tracing::{debug, instrument};

use crate::application::dto::ListObjectsHttpResponse;
use flare_grpc_proto::media::*;
use flare_im_service_kit::clients::GrpcClients;
use flare_server_core::{
    context::Ctx,
    http::{ApiResponse, ContextFromHeaders, Result},
};

#[utoipa::path(
    post,
    path = "/api/v1/medias/object-acl",
    tag = "Media",
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(headers, clients))]
pub async fn set_object_acl(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<SetObjectAclRequest>,
) -> Result<Json<ApiResponse<bool>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(trace_id = %ctx.trace_id(), file_id = %req.file_id, "Setting media object ACL");

    let mut media_client = clients.media.lock().await;
    media_client.set_object_acl(req).await?;
    Ok(Json(ApiResponse::success(true)))
}

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct ListObjectsHttpRequest {
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
}

#[utoipa::path(
    get,
    path = "/api/v1/medias/objects",
    tag = "Media",
    params(
        ("bucket" = String, Query, description = "桶名"),
        ("prefix" = Option<String>, Query, description = "前缀"),
    ),
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(headers, clients))]
pub async fn list_objects(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    axum::extract::Query(req): axum::extract::Query<ListObjectsHttpRequest>,
) -> Result<Json<ApiResponse<ListObjectsHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(trace_id = %ctx.trace_id(), bucket = %req.bucket, prefix = %req.prefix, "Listing media objects");

    let grpc_req = ListObjectsRequest {
        bucket: req.bucket,
        prefix: req.prefix,
        pagination: None,
        filters: Vec::new(),
        sort: Vec::new(),
    };
    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.list_objects(grpc_req).await?;
    Ok(Json(ApiResponse::success(ListObjectsHttpResponse::from(
        grpc_res,
    ))))
}

#[utoipa::path(
    get,
    path = "/api/v1/medias/bucket",
    tag = "Media",
    params(
        ("bucket" = String, Query, description = "桶名"),
    ),
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(headers, clients))]
pub async fn describe_bucket(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    axum::extract::Query(req): axum::extract::Query<DescribeBucketRequest>,
) -> Result<Json<ApiResponse<DescribeBucketResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(trace_id = %ctx.trace_id(), bucket = %req.bucket, "Describing bucket");

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.describe_bucket(req).await?;
    Ok(Json(ApiResponse::success(grpc_res)))
}
