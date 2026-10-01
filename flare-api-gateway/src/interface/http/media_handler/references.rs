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
    responses(
        (status = 200, description = "成功"),
        (status = 400, description = "参数错误：请求不合法，或 Media 拒绝参数（reason=BAD_REQUEST）"),
        (status = 401, description = "缺少或无效的访问令牌"),
        (status = 403, description = "不是绑定租户的终端用户令牌（reason=MEDIA_IDENTITY_REQUIRED）、租户不可用，或 Media 拒绝越权（reason=FORBIDDEN）"),
        (status = 404, description = "文件不存在，或属于其他租户（reason=NOT_FOUND，不回显标识）"),
        (status = 500, description = "内部错误（reason=MEDIA_REQUEST_FAILED，不回显 Media 文案）"),
        (status = 503, description = "Media 不可用，可稍后重试（reason=MEDIA_UNAVAILABLE）"),
        (status = 504, description = "Media 超时，可稍后重试（reason=MEDIA_TIMEOUT）"),
    ),
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
    responses(
        (status = 200, description = "成功"),
        (status = 400, description = "参数错误：请求不合法，或 Media 拒绝参数（reason=BAD_REQUEST）"),
        (status = 401, description = "缺少或无效的访问令牌"),
        (status = 403, description = "不是绑定租户的终端用户令牌（reason=MEDIA_IDENTITY_REQUIRED）、租户不可用，或 Media 拒绝越权（reason=FORBIDDEN）"),
        (status = 404, description = "文件不存在，或属于其他租户（reason=NOT_FOUND，不回显标识）"),
        (status = 500, description = "内部错误（reason=MEDIA_REQUEST_FAILED，不回显 Media 文案）"),
        (status = 503, description = "Media 不可用，可稍后重试（reason=MEDIA_UNAVAILABLE）"),
        (status = 504, description = "Media 超时，可稍后重试（reason=MEDIA_TIMEOUT）"),
    ),
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
    responses(
        (status = 200, description = "成功"),
        (status = 400, description = "参数错误：请求不合法，或 Media 拒绝参数（reason=BAD_REQUEST）"),
        (status = 401, description = "缺少或无效的访问令牌"),
        (status = 403, description = "不是绑定租户的终端用户令牌（reason=MEDIA_IDENTITY_REQUIRED）、租户不可用，或 Media 拒绝越权（reason=FORBIDDEN）"),
        (status = 404, description = "文件不存在，或属于其他租户（reason=NOT_FOUND，不回显标识）"),
        (status = 500, description = "内部错误（reason=MEDIA_REQUEST_FAILED，不回显 Media 文案）"),
        (status = 503, description = "Media 不可用，可稍后重试（reason=MEDIA_UNAVAILABLE）"),
        (status = 504, description = "Media 超时，可稍后重试（reason=MEDIA_TIMEOUT）"),
    ),
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
