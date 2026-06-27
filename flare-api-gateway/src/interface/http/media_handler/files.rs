use axum::{
    body::{Body, Bytes},
    extract::{Extension, Json, Path},
    http::{HeaderMap, HeaderValue, header},
    response::{IntoResponse, Redirect, Response},
};
use futures::StreamExt;
use std::{collections::HashMap, sync::Arc};
use tracing::{debug, instrument};

use crate::application::dto::{
    FileInfoHttpResponse, GetFileUrlHttpRequest as GetFileUrlHttpRequestDto, GetFileUrlHttpResponse,
};
use flare_grpc_proto::media::*;
use flare_im_service_kit::clients::GrpcClients;
use flare_server_core::{
    context::Ctx,
    http::{ApiResponse, ContextFromHeaders, HttpApiError as GatewayError, Result},
};

#[utoipa::path(
    post,
    path = "/api/v1/medias/file-url",
    tag = "Media",
    responses(
        (status = 200, description = "成功"),
        (status = 400, description = "参数错误"),
        (status = 404, description = "文件不存在"),
    ),
)]
#[instrument(skip(headers, clients))]
pub async fn get_file_url(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<GetFileUrlHttpRequestDto>,
) -> Result<Json<ApiResponse<GetFileUrlHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(trace_id = %ctx.trace_id(), file_id = %req.file_id, "Getting file URL");

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.get_file_url(req.into()).await?;
    Ok(Json(ApiResponse::success(GetFileUrlHttpResponse::from(
        grpc_res,
    ))))
}

#[utoipa::path(
    get,
    path = "/api/v1/medias/file-info",
    tag = "Media",
    params(
        ("file_id" = String, Query, description = "文件 ID"),
    ),
    responses(
        (status = 200, description = "成功"),
        (status = 404, description = "文件不存在"),
    ),
)]
#[instrument(skip(headers, clients))]
pub async fn get_file_info(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    axum::extract::Query(req): axum::extract::Query<GetFileInfoRequest>,
) -> Result<Json<ApiResponse<FileInfoHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(trace_id = %ctx.trace_id(), file_id = %req.file_id, "Getting file info");

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.get_file_info(req).await?;
    let info = grpc_res
        .info
        .ok_or_else(|| GatewayError::not_found("File info not found"))?;
    Ok(Json(ApiResponse::success(FileInfoHttpResponse::from(info))))
}

pub async fn serve_file(
    headers: HeaderMap,
    Path(file_id): Path<String>,
    Extension(clients): Extension<Arc<GrpcClients>>,
) -> Result<Response> {
    let ctx = Ctx::from_headers(&headers);
    debug!(trace_id = %ctx.trace_id(), file_id = %file_id, "Serving media file");

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .get_file_info_with_ctx(
            &ctx,
            GetFileInfoRequest {
                file_id: file_id.clone(),
            },
        )
        .await?;
    let info = grpc_res
        .info
        .ok_or_else(|| GatewayError::not_found("File info not found"))?;

    if !info.bucket.trim().is_empty() {
        let url_res = media_client
            .get_file_url_with_ctx(
                &ctx,
                GetFileUrlRequest {
                    file_id,
                    expires_in: 3600,
                    download: false,
                    response_headers: HashMap::new(),
                    retention_protected: false,
                    content_visibility: flare_proto::common::ContentVisibility::Available as i32,
                },
            )
            .await?;
        drop(media_client);

        let redirect_to = if !url_res.cdn_url.trim().is_empty() {
            url_res.cdn_url
        } else {
            url_res.url
        };
        if redirect_to.trim().is_empty() {
            return Err(GatewayError::not_found("File url is empty"));
        }
        return Ok(Redirect::temporary(&redirect_to).into_response());
    }

    let stream = media_client
        .download_file_with_ctx(
            &ctx,
            DownloadFileRequest {
                file_id: file_id.clone(),
            },
        )
        .await?;
    drop(media_client);

    let body_stream = stream.map(|item| {
        item.map(|chunk| Bytes::from(chunk.chunk_data))
            .map_err(|status| std::io::Error::other(status.to_string()))
    });
    let body = Body::from_stream(body_stream);
    let mut response = Response::new(body);
    let response_headers = response.headers_mut();
    response_headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, max-age=60"),
    );
    if let Ok(value) = HeaderValue::from_str(if info.mime_type.trim().is_empty() {
        "application/octet-stream"
    } else {
        info.mime_type.as_str()
    }) {
        response_headers.insert(header::CONTENT_TYPE, value);
    }
    if info.size > 0
        && let Ok(value) = HeaderValue::from_str(&info.size.to_string())
    {
        response_headers.insert(header::CONTENT_LENGTH, value);
    }

    Ok(response)
}

#[utoipa::path(
    delete,
    path = "/api/v1/medias/file",
    tag = "Media",
    responses(
        (status = 200, description = "成功"),
        (status = 404, description = "文件不存在"),
    ),
)]
#[instrument(skip(headers, clients))]
pub async fn delete_file(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<DeleteFileRequest>,
) -> Result<Json<ApiResponse<DeleteFileResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(trace_id = %ctx.trace_id(), file_id = %req.file_id, "Deleting file");

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.delete_file(req).await?;
    Ok(Json(ApiResponse::success(grpc_res)))
}
