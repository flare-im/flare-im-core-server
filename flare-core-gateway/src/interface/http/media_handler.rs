use axum::{
    body::{Body, Bytes},
    extract::{Extension, Json, Path},
    http::{HeaderMap, HeaderValue, header},
    response::{IntoResponse, Redirect, Response},
};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, instrument};

use crate::application::dto::{
    AbortDirectUploadHttpRequest, AbortMultipartUploadHttpRequest,
    AbortMultipartUploadHttpResponse, CommitDirectUploadPartsHttpRequest,
    CommitDirectUploadPartsHttpResponse, CompleteDirectUploadHttpRequest,
    CompleteMultipartUploadHttpRequest, CreateReferenceHttpResponse, DeleteReferenceHttpResponse,
    FileInfoHttpResponse, GetDirectUploadStatusHttpRequest, GetDirectUploadStatusHttpResponse,
    GetFileUrlHttpResponse, ImageOperationHttp, InitiateDirectUploadHttpRequest,
    InitiateDirectUploadHttpResponse, InitiateMultipartUploadHttpRequest,
    InitiateMultipartUploadHttpResponse, ListObjectsHttpResponse, ListReferencesHttpResponse,
    PresignDirectUploadPartsHttpRequest, PresignDirectUploadPartsHttpResponse,
    ProcessImageHttpRequest, ProcessImageHttpResponse, ProcessVideoHttpRequest,
    ProcessVideoHttpResponse, UploadFileHttpRequest, UploadFileHttpResponse,
    UploadMultipartChunkHttpRequest, UploadMultipartChunkHttpResponse, VideoOperationHttp,
};
use flare_grpc_proto::media::*;
use flare_im_core::clients::GrpcClients;
use flare_server_core::context::keys;
use flare_server_core::{
    context::Ctx,
    http::{ApiResponse, ContextFromHeaders, HttpApiError as GatewayError, Result},
};

#[utoipa::path(
    post,
    path = "/api/v1/medias/upload-url",
    tag = "Media",
    responses(
        (status = 200, description = "成功"),
        (status = 400, description = "参数错误"),
        (status = 500, description = "内部错误"),
    ),
)]
#[instrument(skip(headers, clients))]
pub async fn generate_upload_url(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<GenerateUploadUrlRequest>,
) -> Result<Json<ApiResponse<GenerateUploadUrlResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let user_id = headers
        .get(keys::USER_ID)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous");
    debug!(trace_id = %ctx.trace_id(), user_id = %user_id, "Generating upload URL");

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.generate_upload_url(req).await?;
    Ok(Json(ApiResponse::success(grpc_res)))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/upload-file",
    tag = "Media",
    request_body = UploadFileHttpRequest,
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(headers, clients))]
pub async fn upload_file(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<UploadFileHttpRequest>,
) -> Result<Json<ApiResponse<UploadFileHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(
        trace_id = %ctx.trace_id(),
        file_name = %req.metadata.file_name,
        payload_size = req.payload.len(),
        "Uploading file by stream"
    );

    let metadata = UploadFileMetadata::from(req.metadata);
    let chunk_size = if req.chunk_size == 0 {
        256 * 1024
    } else {
        req.chunk_size
    };

    let mut chunks = Vec::with_capacity((req.payload.len() / chunk_size) + 2);
    chunks.push(UploadFileRequest {
        request: Some(upload_file_request::Request::Metadata(metadata)),
    });
    for part in req.payload.chunks(chunk_size) {
        chunks.push(UploadFileRequest {
            request: Some(upload_file_request::Request::ChunkData(part.to_vec())),
        });
    }

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.upload_file(chunks).await?;
    Ok(Json(ApiResponse::success(UploadFileHttpResponse::from(
        grpc_res,
    ))))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/multipart/initiate",
    tag = "Media",
    request_body = InitiateMultipartUploadHttpRequest,
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(headers, clients))]
pub async fn initiate_multipart_upload(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<InitiateMultipartUploadHttpRequest>,
) -> Result<Json<ApiResponse<InitiateMultipartUploadHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(trace_id = %ctx.trace_id(), file_name = %req.metadata.file_name, "Initiating multipart upload");

    let grpc_req = InitiateMultipartUploadRequest {
        metadata: Some(UploadFileMetadata::from(req.metadata)),
        desired_chunk_size: req.desired_chunk_size,
    };
    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .initiate_multipart_upload(grpc_req)
        .await
        .map_err(|err| {
            tracing::error!(
                trace_id = %ctx.trace_id(),
                error = %err,
                "initiate multipart upload grpc call failed"
            );
            GatewayError::internal("MEDIA_MULTIPART_INITIATE_FAILED", err.to_string())
        })?;
    Ok(Json(ApiResponse::success(
        InitiateMultipartUploadHttpResponse::from(grpc_res),
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/multipart/chunk",
    tag = "Media",
    request_body = UploadMultipartChunkHttpRequest,
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(headers, clients))]
pub async fn upload_multipart_chunk(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<UploadMultipartChunkHttpRequest>,
) -> Result<Json<ApiResponse<UploadMultipartChunkHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(
        trace_id = %ctx.trace_id(),
        upload_id = %req.upload_id,
        chunk_index = req.chunk_index,
        payload_size = req.payload.len(),
        "Uploading multipart chunk"
    );

    let grpc_req = UploadMultipartChunkRequest {
        upload_id: req.upload_id,
        chunk_index: req.chunk_index,
        payload: req.payload,
    };
    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.upload_multipart_chunk(grpc_req).await?;
    Ok(Json(ApiResponse::success(
        UploadMultipartChunkHttpResponse::from(grpc_res),
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/multipart/complete",
    tag = "Media",
    request_body = CompleteMultipartUploadHttpRequest,
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(headers, clients))]
pub async fn complete_multipart_upload(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<CompleteMultipartUploadHttpRequest>,
) -> Result<Json<ApiResponse<UploadFileHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(trace_id = %ctx.trace_id(), upload_id = %req.upload_id, "Completing multipart upload");

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .complete_multipart_upload(CompleteMultipartUploadRequest {
            upload_id: req.upload_id,
        })
        .await?;
    Ok(Json(ApiResponse::success(UploadFileHttpResponse::from(
        grpc_res,
    ))))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/multipart/abort",
    tag = "Media",
    request_body = AbortMultipartUploadHttpRequest,
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(headers, clients))]
pub async fn abort_multipart_upload(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<AbortMultipartUploadHttpRequest>,
) -> Result<Json<ApiResponse<AbortMultipartUploadHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(trace_id = %ctx.trace_id(), upload_id = %req.upload_id, "Aborting multipart upload");

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .abort_multipart_upload(AbortMultipartUploadRequest {
            upload_id: req.upload_id,
        })
        .await?;
    Ok(Json(ApiResponse::success(
        AbortMultipartUploadHttpResponse::from(grpc_res),
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/uploads/initiate",
    tag = "Media",
    request_body = InitiateDirectUploadHttpRequest,
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(headers, clients))]
pub async fn initiate_direct_upload(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<InitiateDirectUploadHttpRequest>,
) -> Result<Json<ApiResponse<InitiateDirectUploadHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let grpc_req = InitiateDirectUploadRequest {
        metadata: Some(UploadFileMetadata::from(req.metadata)),
        desired_part_size: req.desired_part_size,
        file_fingerprint: req.file_fingerprint,
        head_tail_sha256: req.head_tail_sha256,
        full_sha256: req.full_sha256,
    };
    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.initiate_direct_upload(grpc_req).await.map_err(|err| {
        tracing::error!(trace_id = %ctx.trace_id(), error = %err, "initiate direct upload grpc call failed");
        GatewayError::internal("MEDIA_DIRECT_UPLOAD_INITIATE_FAILED", err.to_string())
    })?;
    Ok(Json(ApiResponse::success(
        InitiateDirectUploadHttpResponse::from(grpc_res),
    )))
}

#[utoipa::path(
    get,
    path = "/api/v1/medias/uploads/status",
    tag = "Media",
    params(("upload_id" = String, Query, description = "上传 ID")),
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(headers, clients))]
pub async fn get_direct_upload_status(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    axum::extract::Query(req): axum::extract::Query<GetDirectUploadStatusHttpRequest>,
) -> Result<Json<ApiResponse<GetDirectUploadStatusHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .get_direct_upload_status(GetDirectUploadStatusRequest {
            upload_id: req.upload_id,
        })
        .await
        .map_err(|err| {
            tracing::error!(trace_id = %ctx.trace_id(), error = %err, "get direct upload status grpc call failed");
            GatewayError::internal("MEDIA_DIRECT_UPLOAD_STATUS_FAILED", err.to_string())
        })?;
    Ok(Json(ApiResponse::success(
        GetDirectUploadStatusHttpResponse::from(grpc_res),
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/uploads/presign-parts",
    tag = "Media",
    request_body = PresignDirectUploadPartsHttpRequest,
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(headers, clients))]
pub async fn presign_direct_upload_parts(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<PresignDirectUploadPartsHttpRequest>,
) -> Result<Json<ApiResponse<PresignDirectUploadPartsHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .presign_direct_upload_parts(PresignDirectUploadPartsRequest {
            upload_id: req.upload_id,
            part_numbers: req.part_numbers,
            expires_in: req.expires_in,
        })
        .await
        .map_err(|err| {
            tracing::error!(trace_id = %ctx.trace_id(), error = %err, "presign direct upload parts grpc call failed");
            GatewayError::internal("MEDIA_DIRECT_UPLOAD_PRESIGN_FAILED", err.to_string())
        })?;
    Ok(Json(ApiResponse::success(
        PresignDirectUploadPartsHttpResponse::from(grpc_res),
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/uploads/commit-parts",
    tag = "Media",
    request_body = CommitDirectUploadPartsHttpRequest,
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(headers, clients))]
pub async fn commit_direct_upload_parts(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<CommitDirectUploadPartsHttpRequest>,
) -> Result<Json<ApiResponse<CommitDirectUploadPartsHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let grpc_parts = req
        .parts
        .into_iter()
        .map(|part| UploadedPartInfo {
            part_number: part.part_number,
            etag: part.etag,
            size: part.size,
            sha256: part.sha256.unwrap_or_default(),
        })
        .collect();
    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .commit_direct_upload_parts(CommitDirectUploadPartsRequest {
            upload_id: req.upload_id,
            parts: grpc_parts,
        })
        .await
        .map_err(|err| {
            tracing::error!(trace_id = %ctx.trace_id(), error = %err, "commit direct upload parts grpc call failed");
            GatewayError::internal("MEDIA_DIRECT_UPLOAD_COMMIT_FAILED", err.to_string())
        })?;
    Ok(Json(ApiResponse::success(
        CommitDirectUploadPartsHttpResponse::from(grpc_res),
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/uploads/complete",
    tag = "Media",
    request_body = CompleteDirectUploadHttpRequest,
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(headers, clients))]
pub async fn complete_direct_upload(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<CompleteDirectUploadHttpRequest>,
) -> Result<Json<ApiResponse<UploadFileHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .complete_direct_upload(CompleteDirectUploadRequest {
            upload_id: req.upload_id,
        })
        .await
        .map_err(|err| {
            tracing::error!(trace_id = %ctx.trace_id(), error = %err, "complete direct upload grpc call failed");
            GatewayError::internal("MEDIA_DIRECT_UPLOAD_COMPLETE_FAILED", err.to_string())
        })?;
    Ok(Json(ApiResponse::success(UploadFileHttpResponse::from(
        grpc_res,
    ))))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/uploads/abort",
    tag = "Media",
    request_body = AbortDirectUploadHttpRequest,
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(headers, clients))]
pub async fn abort_direct_upload(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<AbortDirectUploadHttpRequest>,
) -> Result<Json<ApiResponse<AbortMultipartUploadHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .abort_direct_upload(AbortDirectUploadRequest {
            upload_id: req.upload_id,
        })
        .await
        .map_err(|err| {
            tracing::error!(trace_id = %ctx.trace_id(), error = %err, "abort direct upload grpc call failed");
            GatewayError::internal("MEDIA_DIRECT_UPLOAD_ABORT_FAILED", err.to_string())
        })?;
    Ok(Json(ApiResponse::success(
        AbortMultipartUploadHttpResponse::from(grpc_res),
    )))
}

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
    Json(req): Json<GetFileUrlRequest>,
) -> Result<Json<ApiResponse<GetFileUrlHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(trace_id = %ctx.trace_id(), file_id = %req.file_id, "Getting file URL");

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.get_file_url(req).await?;
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

#[utoipa::path(
    post,
    path = "/api/v1/medias/references",
    tag = "Media",
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(headers, clients))]
pub async fn create_reference(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<CreateReferenceRequest>,
) -> Result<Json<ApiResponse<CreateReferenceHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(trace_id = %ctx.trace_id(), file_id = %req.file_id, "Creating media reference");

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.create_reference(req).await?;
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
#[instrument(skip(headers, clients))]
pub async fn delete_reference(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<DeleteReferenceRequest>,
) -> Result<Json<ApiResponse<DeleteReferenceHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(trace_id = %ctx.trace_id(), file_id = %req.file_id, "Deleting media reference");

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.delete_reference(req).await?;
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
#[instrument(skip(headers, clients))]
pub async fn list_references(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    axum::extract::Query(req): axum::extract::Query<GetFileInfoRequest>,
) -> Result<Json<ApiResponse<ListReferencesHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(trace_id = %ctx.trace_id(), file_id = %req.file_id, "Listing media references");

    let grpc_req = ListReferencesRequest {
        file_id: req.file_id,
        pagination: None,
        filters: Vec::new(),
        sort: Vec::new(),
    };
    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.list_references(grpc_req).await?;
    Ok(Json(ApiResponse::success(
        ListReferencesHttpResponse::from(grpc_res),
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/cleanup-orphaned-assets",
    tag = "Media",
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(headers, clients))]
pub async fn cleanup_orphaned_assets(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<CleanupOrphanedAssetsRequest>,
) -> Result<Json<ApiResponse<CleanupOrphanedAssetsResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(trace_id = %ctx.trace_id(), limit = req.limit, "Cleaning up orphaned media assets");

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.cleanup_orphaned_assets(req).await?;
    Ok(Json(ApiResponse::success(grpc_res)))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/process-image",
    tag = "Media",
    request_body = ProcessImageHttpRequest,
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(headers, clients))]
pub async fn process_image(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<ProcessImageHttpRequest>,
) -> Result<Json<ApiResponse<ProcessImageHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(trace_id = %ctx.trace_id(), file_id = %req.file_id, "Processing image");

    let grpc_req = ProcessImageRequest {
        file_id: req.file_id,
        operations: req
            .operations
            .into_iter()
            .map(|op| ImageOperation {
                operation: Some(match op {
                    ImageOperationHttp::Resize {
                        width,
                        height,
                        keep_aspect_ratio,
                    } => image_operation::Operation::Resize(ResizeOperation {
                        width,
                        height,
                        keep_aspect_ratio,
                    }),
                    ImageOperationHttp::Compress { quality } => {
                        image_operation::Operation::Compress(CompressOperation { quality })
                    }
                    ImageOperationHttp::Thumbnail { size } => {
                        image_operation::Operation::Thumbnail(ThumbnailOperation { size })
                    }
                    ImageOperationHttp::Watermark { text, position } => {
                        image_operation::Operation::Watermark(WatermarkOperation { text, position })
                    }
                }),
            })
            .collect(),
        target_bucket: req.target_bucket,
        output_prefix: req.output_prefix,
    };

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.process_image(grpc_req).await?;
    Ok(Json(ApiResponse::success(ProcessImageHttpResponse::from(
        grpc_res,
    ))))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/process-video",
    tag = "Media",
    request_body = ProcessVideoHttpRequest,
    responses((status = 200, description = "成功")),
)]
#[instrument(skip(headers, clients))]
pub async fn process_video(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<ProcessVideoHttpRequest>,
) -> Result<Json<ApiResponse<ProcessVideoHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    debug!(trace_id = %ctx.trace_id(), file_id = %req.file_id, "Processing video");

    let grpc_req = ProcessVideoRequest {
        file_id: req.file_id,
        operations: req
            .operations
            .into_iter()
            .map(|op| VideoOperation {
                operation: Some(match op {
                    VideoOperationHttp::Transcode {
                        format,
                        quality,
                        target_bitrate_kbps,
                        max_width,
                    } => video_operation::Operation::Transcode(TranscodeOperation {
                        format,
                        quality,
                        target_bitrate_kbps,
                        max_width,
                    }),
                    VideoOperationHttp::ExtractThumbnail { time } => {
                        video_operation::Operation::ExtractThumbnail(ExtractThumbnailOperation {
                            time,
                        })
                    }
                    VideoOperationHttp::Compress { bitrate, preset } => {
                        video_operation::Operation::Compress(CompressVideoOperation {
                            bitrate,
                            preset,
                        })
                    }
                    VideoOperationHttp::SubtitleBurn {
                        subtitle_file_id,
                        language,
                    } => video_operation::Operation::SubtitleBurn(SubtitleBurnOperation {
                        subtitle_file_id,
                        language,
                    }),
                }),
            })
            .collect(),
        target_bucket: req.target_bucket,
        output_prefix: req.output_prefix,
    };

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.process_video(grpc_req).await?;
    Ok(Json(ApiResponse::success(ProcessVideoHttpResponse::from(
        grpc_res,
    ))))
}

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
