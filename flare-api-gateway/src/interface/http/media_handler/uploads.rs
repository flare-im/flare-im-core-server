use axum::{
    extract::{Extension, Json},
    http::HeaderMap,
};
use std::sync::Arc;
use tracing::{debug, instrument};

use crate::application::dto::{
    AbortDirectUploadHttpRequest, AbortMultipartUploadHttpRequest,
    AbortMultipartUploadHttpResponse, CommitDirectUploadPartsHttpRequest,
    CommitDirectUploadPartsHttpResponse, CompleteDirectUploadHttpRequest,
    CompleteMultipartUploadHttpRequest, GetDirectUploadStatusHttpRequest,
    GetDirectUploadStatusHttpResponse, InitiateDirectUploadHttpRequest,
    InitiateDirectUploadHttpResponse, InitiateMultipartUploadHttpRequest,
    InitiateMultipartUploadHttpResponse, PresignDirectUploadPartsHttpRequest,
    PresignDirectUploadPartsHttpResponse, UploadFileHttpRequest, UploadFileHttpResponse,
    UploadMultipartChunkHttpRequest, UploadMultipartChunkHttpResponse,
};
use flare_grpc_proto::media::*;
use flare_im_service_kit::clients::GrpcClients;
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
