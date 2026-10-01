use axum::extract::{Extension, Json};
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
use flare_server_core::http::{ApiResponse, Result};

use super::errors::media_rpc_error;
use super::identity::MediaIdentity;

#[utoipa::path(
    post,
    path = "/api/v1/medias/upload-url",
    tag = "Media",
    responses(
        (status = 200, description = "成功：object_key 首段为调用方租户"),
        (status = 400, description = "参数错误（含调用方指定 object_key）"),
        (status = 401, description = "缺少或无效的访问令牌"),
        (status = 403, description = "bucket 不属于本服务，或不是绑定租户的终端用户令牌"),
        (status = 500, description = "内部错误（reason=MEDIA_REQUEST_FAILED，不回显 Media 文案）"),
        (status = 503, description = "Media 不可用，可稍后重试（reason=MEDIA_UNAVAILABLE）"),
        (status = 504, description = "Media 超时，可稍后重试（reason=MEDIA_TIMEOUT）"),
    ),
)]
#[instrument(skip(caller, clients))]
pub async fn generate_upload_url(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<GenerateUploadUrlRequest>,
) -> Result<Json<ApiResponse<GenerateUploadUrlResponse>>> {
    let ctx = caller.ctx();
    debug!(trace_id = %ctx.trace_id(), user_id = %caller.actor_id(), "Generating upload URL");

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .generate_upload_url(&caller, req)
        .await
        .map_err(media_rpc_error)?;
    Ok(Json(ApiResponse::success(grpc_res)))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/upload-file",
    tag = "Media",
    request_body = UploadFileHttpRequest,
    responses(
        (status = 200, description = "成功"),
        (status = 400, description = "参数错误：请求体不合法，或 Media 拒绝参数（reason=BAD_REQUEST）"),
        (status = 401, description = "缺少或无效的访问令牌"),
        (status = 403, description = "不是绑定租户的终端用户令牌（reason=MEDIA_IDENTITY_REQUIRED）、租户不可用，或 Media 拒绝越权（reason=FORBIDDEN）"),
        (status = 500, description = "内部错误（reason=MEDIA_REQUEST_FAILED，不回显 Media 文案）"),
        (status = 503, description = "Media 不可用，可稍后重试（reason=MEDIA_UNAVAILABLE）"),
        (status = 504, description = "Media 超时，可稍后重试（reason=MEDIA_TIMEOUT）"),
    ),
)]
#[instrument(skip(caller, clients))]
pub async fn upload_file(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<UploadFileHttpRequest>,
) -> Result<Json<ApiResponse<UploadFileHttpResponse>>> {
    let ctx = caller.ctx();
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
    let grpc_res = media_client
        .upload_file(&caller, chunks)
        .await
        .map_err(media_rpc_error)?;
    Ok(Json(ApiResponse::success(UploadFileHttpResponse::from(
        grpc_res,
    ))))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/multipart/initiate",
    tag = "Media",
    request_body = InitiateMultipartUploadHttpRequest,
    responses(
        (status = 200, description = "成功"),
        (status = 400, description = "参数错误：请求体不合法，或 Media 拒绝参数（reason=BAD_REQUEST）"),
        (status = 401, description = "缺少或无效的访问令牌"),
        (status = 403, description = "不是绑定租户的终端用户令牌（reason=MEDIA_IDENTITY_REQUIRED）、租户不可用，或 Media 拒绝越权（reason=FORBIDDEN）"),
        (status = 500, description = "内部错误（reason=MEDIA_REQUEST_FAILED，不回显 Media 文案）"),
        (status = 503, description = "Media 不可用，可稍后重试（reason=MEDIA_UNAVAILABLE）"),
        (status = 504, description = "Media 超时，可稍后重试（reason=MEDIA_TIMEOUT）"),
    ),
)]
#[instrument(skip(caller, clients))]
pub async fn initiate_multipart_upload(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<InitiateMultipartUploadHttpRequest>,
) -> Result<Json<ApiResponse<InitiateMultipartUploadHttpResponse>>> {
    let ctx = caller.ctx();
    debug!(trace_id = %ctx.trace_id(), file_name = %req.metadata.file_name, "Initiating multipart upload");

    let grpc_req = InitiateMultipartUploadRequest {
        metadata: Some(UploadFileMetadata::from(req.metadata)),
        desired_chunk_size: req.desired_chunk_size,
    };
    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .initiate_multipart_upload(&caller, grpc_req)
        .await
        .map_err(media_rpc_error)?;
    Ok(Json(ApiResponse::success(
        InitiateMultipartUploadHttpResponse::from(grpc_res),
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/multipart/chunk",
    tag = "Media",
    request_body = UploadMultipartChunkHttpRequest,
    responses(
        (status = 200, description = "成功"),
        (status = 400, description = "参数错误：请求体不合法，或 Media 拒绝参数（reason=BAD_REQUEST）"),
        (status = 401, description = "缺少或无效的访问令牌"),
        (status = 403, description = "不是绑定租户的终端用户令牌（reason=MEDIA_IDENTITY_REQUIRED）、租户不可用，或 Media 拒绝越权（reason=FORBIDDEN）"),
        (status = 404, description = "上传会话不存在，或属于其他用户、其他租户（reason=NOT_FOUND，不回显标识）"),
        (status = 500, description = "内部错误（reason=MEDIA_REQUEST_FAILED，不回显 Media 文案）"),
        (status = 503, description = "Media 不可用，可稍后重试（reason=MEDIA_UNAVAILABLE）"),
        (status = 504, description = "Media 超时，可稍后重试（reason=MEDIA_TIMEOUT）"),
    ),
)]
#[instrument(skip(caller, clients))]
pub async fn upload_multipart_chunk(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<UploadMultipartChunkHttpRequest>,
) -> Result<Json<ApiResponse<UploadMultipartChunkHttpResponse>>> {
    let ctx = caller.ctx();
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
    let grpc_res = media_client
        .upload_multipart_chunk(&caller, grpc_req)
        .await
        .map_err(media_rpc_error)?;
    Ok(Json(ApiResponse::success(
        UploadMultipartChunkHttpResponse::from(grpc_res),
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/multipart/complete",
    tag = "Media",
    request_body = CompleteMultipartUploadHttpRequest,
    responses(
        (status = 200, description = "成功"),
        (status = 400, description = "参数错误：请求体不合法，或 Media 拒绝参数（reason=BAD_REQUEST）"),
        (status = 401, description = "缺少或无效的访问令牌"),
        (status = 403, description = "不是绑定租户的终端用户令牌（reason=MEDIA_IDENTITY_REQUIRED）、租户不可用，或 Media 拒绝越权（reason=FORBIDDEN）"),
        (status = 404, description = "上传会话不存在，或属于其他用户、其他租户（reason=NOT_FOUND，不回显标识）"),
        (status = 500, description = "内部错误（reason=MEDIA_REQUEST_FAILED，不回显 Media 文案）"),
        (status = 503, description = "Media 不可用，可稍后重试（reason=MEDIA_UNAVAILABLE）"),
        (status = 504, description = "Media 超时，可稍后重试（reason=MEDIA_TIMEOUT）"),
    ),
)]
#[instrument(skip(caller, clients))]
pub async fn complete_multipart_upload(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<CompleteMultipartUploadHttpRequest>,
) -> Result<Json<ApiResponse<UploadFileHttpResponse>>> {
    let ctx = caller.ctx();
    debug!(trace_id = %ctx.trace_id(), upload_id = %req.upload_id, "Completing multipart upload");

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .complete_multipart_upload(
            &caller,
            CompleteMultipartUploadRequest {
                upload_id: req.upload_id,
            },
        )
        .await
        .map_err(media_rpc_error)?;
    Ok(Json(ApiResponse::success(UploadFileHttpResponse::from(
        grpc_res,
    ))))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/multipart/abort",
    tag = "Media",
    request_body = AbortMultipartUploadHttpRequest,
    responses(
        (status = 200, description = "成功；会话不存在时视为已中止"),
        (status = 400, description = "参数错误：请求体不合法，或 Media 拒绝参数（reason=BAD_REQUEST）"),
        (status = 401, description = "缺少或无效的访问令牌"),
        (status = 403, description = "不是绑定租户的终端用户令牌（reason=MEDIA_IDENTITY_REQUIRED）、租户不可用，或 Media 拒绝越权（reason=FORBIDDEN）"),
        (status = 404, description = "上传会话不存在，或属于其他用户、其他租户（reason=NOT_FOUND，不回显标识）"),
        (status = 500, description = "内部错误（reason=MEDIA_REQUEST_FAILED，不回显 Media 文案）"),
        (status = 503, description = "Media 不可用，可稍后重试（reason=MEDIA_UNAVAILABLE）"),
        (status = 504, description = "Media 超时，可稍后重试（reason=MEDIA_TIMEOUT）"),
    ),
)]
#[instrument(skip(caller, clients))]
pub async fn abort_multipart_upload(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<AbortMultipartUploadHttpRequest>,
) -> Result<Json<ApiResponse<AbortMultipartUploadHttpResponse>>> {
    let ctx = caller.ctx();
    debug!(trace_id = %ctx.trace_id(), upload_id = %req.upload_id, "Aborting multipart upload");

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .abort_multipart_upload(
            &caller,
            AbortMultipartUploadRequest {
                upload_id: req.upload_id,
            },
        )
        .await
        .map_err(media_rpc_error)?;
    Ok(Json(ApiResponse::success(
        AbortMultipartUploadHttpResponse::from(grpc_res),
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/uploads/initiate",
    tag = "Media",
    request_body = InitiateDirectUploadHttpRequest,
    responses(
        (status = 200, description = "成功：单次 PUT 地址已签入声明的大小，有效期不超过会话；PUT 须带签入的 Content-Type 与 Content-Length，空文件也要显式带 Content-Length: 0"),
        (status = 400, description = "参数错误：请求体不合法，或 Media 拒绝参数（reason=BAD_REQUEST）"),
        (status = 401, description = "缺少或无效的访问令牌"),
        (status = 403, description = "不是绑定租户的终端用户令牌（reason=MEDIA_IDENTITY_REQUIRED）、租户不可用，或 Media 拒绝越权（reason=FORBIDDEN）"),
        (status = 500, description = "内部错误（reason=MEDIA_REQUEST_FAILED，不回显 Media 文案）"),
        (status = 503, description = "Media 不可用，可稍后重试（reason=MEDIA_UNAVAILABLE）"),
        (status = 504, description = "Media 超时，可稍后重试（reason=MEDIA_TIMEOUT）"),
    ),
)]
#[instrument(skip(caller, clients))]
pub async fn initiate_direct_upload(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<InitiateDirectUploadHttpRequest>,
) -> Result<Json<ApiResponse<InitiateDirectUploadHttpResponse>>> {
    let ctx = caller.ctx();
    debug!(trace_id = %ctx.trace_id(), file_name = %req.metadata.file_name, "Initiating direct upload");
    let grpc_req = InitiateDirectUploadRequest {
        metadata: Some(UploadFileMetadata::from(req.metadata)),
        desired_part_size: req.desired_part_size,
        file_fingerprint: req.file_fingerprint,
        head_tail_sha256: req.head_tail_sha256,
        full_sha256: req.full_sha256,
    };
    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .initiate_direct_upload(&caller, grpc_req)
        .await
        .map_err(media_rpc_error)?;
    Ok(Json(ApiResponse::success(
        InitiateDirectUploadHttpResponse::from(grpc_res),
    )))
}

#[utoipa::path(
    get,
    path = "/api/v1/medias/uploads/status",
    tag = "Media",
    params(("upload_id" = String, Query, description = "上传 ID")),
    responses(
        (status = 200, description = "成功"),
        (status = 400, description = "参数错误：请求体不合法，或 Media 拒绝参数（reason=BAD_REQUEST）"),
        (status = 401, description = "缺少或无效的访问令牌"),
        (status = 403, description = "不是绑定租户的终端用户令牌（reason=MEDIA_IDENTITY_REQUIRED）、租户不可用，或 Media 拒绝越权（reason=FORBIDDEN）"),
        (status = 404, description = "上传会话不存在，或属于其他用户、其他租户（reason=NOT_FOUND，不回显标识）"),
        (status = 500, description = "内部错误（reason=MEDIA_REQUEST_FAILED，不回显 Media 文案）"),
        (status = 503, description = "Media 不可用，可稍后重试（reason=MEDIA_UNAVAILABLE）"),
        (status = 504, description = "Media 超时，可稍后重试（reason=MEDIA_TIMEOUT）"),
    ),
)]
#[instrument(skip(caller, clients))]
pub async fn get_direct_upload_status(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    axum::extract::Query(req): axum::extract::Query<GetDirectUploadStatusHttpRequest>,
) -> Result<Json<ApiResponse<GetDirectUploadStatusHttpResponse>>> {
    let ctx = caller.ctx();
    debug!(trace_id = %ctx.trace_id(), upload_id = %req.upload_id, "Getting direct upload status");
    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .get_direct_upload_status(
            &caller,
            GetDirectUploadStatusRequest {
                upload_id: req.upload_id,
            },
        )
        .await
        .map_err(media_rpc_error)?;
    Ok(Json(ApiResponse::success(
        GetDirectUploadStatusHttpResponse::from(grpc_res),
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/uploads/presign-parts",
    tag = "Media",
    request_body = PresignDirectUploadPartsHttpRequest,
    responses(
        (status = 200, description = "成功：每片地址签入该片的长度（最后一片为余下的字节），长度不符的 PUT 由存储拒绝"),
        (status = 400, description = "参数错误：请求体不合法，或 Media 拒绝参数（reason=BAD_REQUEST）"),
        (status = 401, description = "缺少或无效的访问令牌"),
        (status = 403, description = "不是绑定租户的终端用户令牌（reason=MEDIA_IDENTITY_REQUIRED）、租户不可用，或 Media 拒绝越权（reason=FORBIDDEN）"),
        (status = 404, description = "上传会话不存在，或属于其他用户、其他租户（reason=NOT_FOUND，不回显标识）"),
        (status = 500, description = "内部错误（reason=MEDIA_REQUEST_FAILED，不回显 Media 文案）"),
        (status = 503, description = "Media 不可用，可稍后重试（reason=MEDIA_UNAVAILABLE）"),
        (status = 504, description = "Media 超时，可稍后重试（reason=MEDIA_TIMEOUT）"),
    ),
)]
#[instrument(skip(caller, clients))]
pub async fn presign_direct_upload_parts(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<PresignDirectUploadPartsHttpRequest>,
) -> Result<Json<ApiResponse<PresignDirectUploadPartsHttpResponse>>> {
    let ctx = caller.ctx();
    debug!(trace_id = %ctx.trace_id(), upload_id = %req.upload_id, "Presigning direct upload parts");
    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .presign_direct_upload_parts(
            &caller,
            PresignDirectUploadPartsRequest {
                upload_id: req.upload_id,
                part_numbers: req.part_numbers,
                expires_in: req.expires_in,
            },
        )
        .await
        .map_err(media_rpc_error)?;
    Ok(Json(ApiResponse::success(
        PresignDirectUploadPartsHttpResponse::from(grpc_res),
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/uploads/commit-parts",
    tag = "Media",
    request_body = CommitDirectUploadPartsHttpRequest,
    responses(
        (status = 200, description = "成功"),
        (status = 400, description = "参数错误：请求体不合法，或 Media 拒绝参数（reason=BAD_REQUEST）"),
        (status = 401, description = "缺少或无效的访问令牌"),
        (status = 403, description = "不是绑定租户的终端用户令牌（reason=MEDIA_IDENTITY_REQUIRED）、租户不可用，或 Media 拒绝越权（reason=FORBIDDEN）"),
        (status = 404, description = "上传会话不存在，或属于其他用户、其他租户（reason=NOT_FOUND，不回显标识）"),
        (status = 500, description = "内部错误（reason=MEDIA_REQUEST_FAILED，不回显 Media 文案）"),
        (status = 503, description = "Media 不可用，可稍后重试（reason=MEDIA_UNAVAILABLE）"),
        (status = 504, description = "Media 超时，可稍后重试（reason=MEDIA_TIMEOUT）"),
    ),
)]
#[instrument(skip(caller, clients))]
pub async fn commit_direct_upload_parts(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<CommitDirectUploadPartsHttpRequest>,
) -> Result<Json<ApiResponse<CommitDirectUploadPartsHttpResponse>>> {
    let ctx = caller.ctx();
    debug!(trace_id = %ctx.trace_id(), upload_id = %req.upload_id, "Committing direct upload parts");
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
        .commit_direct_upload_parts(
            &caller,
            CommitDirectUploadPartsRequest {
                upload_id: req.upload_id,
                parts: grpc_parts,
            },
        )
        .await
        .map_err(media_rpc_error)?;
    Ok(Json(ApiResponse::success(
        CommitDirectUploadPartsHttpResponse::from(grpc_res),
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/uploads/complete",
    tag = "Media",
    request_body = CompleteDirectUploadHttpRequest,
    responses(
        (status = 200, description = "成功；重复完成（上次已登记、会话未清掉或答复丢失）返回已登记的文件；合并后登记失败的分片上传重试即补登记"),
        (status = 400, description = "参数错误（reason=BAD_REQUEST）：请求体不合法；对象尚未上传（声明大小为 0 的上传写入 0 字节对象即可完成）；提交的分片与存储中的不符；存储中已没有这个分片上传、也没有合并出的对象，需重新发起上传；存储中的对象大小与声明不符（对象随即删除，删除失败时同样回 400，残留由回收任务清理）；会话已因大小不符中止"),
        (status = 401, description = "缺少或无效的访问令牌"),
        (status = 403, description = "不是绑定租户的终端用户令牌（reason=MEDIA_IDENTITY_REQUIRED）、租户不可用，或 Media 拒绝越权（reason=FORBIDDEN）"),
        (status = 404, description = "上传会话不存在，或属于其他用户、其他租户（reason=NOT_FOUND，不回显标识）"),
        (status = 500, description = "内部错误（reason=MEDIA_REQUEST_FAILED，不回显 Media 文案）"),
        (status = 503, description = "Media 不可用，可稍后重试（reason=MEDIA_UNAVAILABLE）"),
        (status = 504, description = "Media 超时，可稍后重试（reason=MEDIA_TIMEOUT）"),
    ),
)]
#[instrument(skip(caller, clients))]
pub async fn complete_direct_upload(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<CompleteDirectUploadHttpRequest>,
) -> Result<Json<ApiResponse<UploadFileHttpResponse>>> {
    let ctx = caller.ctx();
    debug!(trace_id = %ctx.trace_id(), upload_id = %req.upload_id, "Completing direct upload");
    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .complete_direct_upload(
            &caller,
            CompleteDirectUploadRequest {
                upload_id: req.upload_id,
            },
        )
        .await
        .map_err(media_rpc_error)?;
    Ok(Json(ApiResponse::success(UploadFileHttpResponse::from(
        grpc_res,
    ))))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/uploads/abort",
    tag = "Media",
    request_body = AbortDirectUploadHttpRequest,
    responses(
        (status = 200, description = "成功：已写入的对象与未完成的分片上传一并释放"),
        (status = 400, description = "参数错误：请求体不合法，或 Media 拒绝参数（reason=BAD_REQUEST）"),
        (status = 401, description = "缺少或无效的访问令牌"),
        (status = 403, description = "不是绑定租户的终端用户令牌（reason=MEDIA_IDENTITY_REQUIRED）、租户不可用，或 Media 拒绝越权（reason=FORBIDDEN）"),
        (status = 404, description = "上传会话不存在，或属于其他用户、其他租户（reason=NOT_FOUND，不回显标识）"),
        (status = 500, description = "内部错误（reason=MEDIA_REQUEST_FAILED，不回显 Media 文案）"),
        (status = 503, description = "Media 不可用，可稍后重试（reason=MEDIA_UNAVAILABLE）"),
        (status = 504, description = "Media 超时，可稍后重试（reason=MEDIA_TIMEOUT）"),
    ),
)]
#[instrument(skip(caller, clients))]
pub async fn abort_direct_upload(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<AbortDirectUploadHttpRequest>,
) -> Result<Json<ApiResponse<AbortMultipartUploadHttpResponse>>> {
    let ctx = caller.ctx();
    debug!(trace_id = %ctx.trace_id(), upload_id = %req.upload_id, "Aborting direct upload");
    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client
        .abort_direct_upload(
            &caller,
            AbortDirectUploadRequest {
                upload_id: req.upload_id,
            },
        )
        .await
        .map_err(media_rpc_error)?;
    Ok(Json(ApiResponse::success(
        AbortMultipartUploadHttpResponse::from(grpc_res),
    )))
}
