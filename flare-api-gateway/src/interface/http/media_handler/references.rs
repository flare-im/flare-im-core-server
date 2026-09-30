use axum::extract::{Extension, Json};
use std::sync::Arc;
use tracing::{debug, instrument};

use crate::application::dto::{
    CreateReferenceHttpResponse, DeleteReferenceHttpResponse, ListReferencesHttpResponse,
};
use flare_grpc_proto::media::*;
use flare_im_service_kit::clients::GrpcClients;
use flare_server_core::http::{ApiResponse, Result};

use super::errors::media_rpc_error;
use super::identity::MediaIdentity;

#[utoipa::path(
    post,
    path = "/api/v1/medias/references",
    tag = "Media",
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(caller, clients))]
pub async fn create_reference(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<CreateReferenceRequest>,
) -> Result<Json<ApiResponse<CreateReferenceHttpResponse>>> {
    let ctx = caller.ctx();
    debug!(trace_id = %ctx.trace_id(), file_id = %req.file_id, "Creating media reference");

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .create_reference(&caller, req)
        .await
        .map_err(media_rpc_error)?;
    Ok(Json(ApiResponse::success(
        CreateReferenceHttpResponse::from(grpc_res),
    )))
}

#[utoipa::path(
    delete,
    path = "/api/v1/medias/references",
    tag = "Media",
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(caller, clients))]
pub async fn delete_reference(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<DeleteReferenceRequest>,
) -> Result<Json<ApiResponse<DeleteReferenceHttpResponse>>> {
    let ctx = caller.ctx();
    debug!(trace_id = %ctx.trace_id(), file_id = %req.file_id, "Deleting media reference");

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .delete_reference(&caller, req)
        .await
        .map_err(media_rpc_error)?;
    Ok(Json(ApiResponse::success(
        DeleteReferenceHttpResponse::from(grpc_res),
    )))
}

#[utoipa::path(
    get,
    path = "/api/v1/medias/references",
    tag = "Media",
    params(
        ("file_id" = String, Query, description = "文件 ID"),
    ),
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(caller, clients))]
pub async fn list_references(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    axum::extract::Query(req): axum::extract::Query<GetFileInfoRequest>,
) -> Result<Json<ApiResponse<ListReferencesHttpResponse>>> {
    let ctx = caller.ctx();
    debug!(trace_id = %ctx.trace_id(), file_id = %req.file_id, "Listing media references");

    let grpc_req = ListReferencesRequest {
        file_id: req.file_id,
        pagination: None,
        filters: Vec::new(),
        sort: Vec::new(),
    };
    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .list_references(&caller, grpc_req)
        .await
        .map_err(media_rpc_error)?;
    Ok(Json(ApiResponse::success(
        ListReferencesHttpResponse::from(grpc_res),
    )))
}
