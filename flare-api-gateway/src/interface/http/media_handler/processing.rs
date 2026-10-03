use axum::extract::{Extension, Json};
use std::sync::Arc;
use tracing::{debug, instrument};

use crate::application::dto::{
    ImageOperationHttp, ProcessImageHttpRequest, ProcessImageHttpResponse, ProcessVideoHttpRequest,
    ProcessVideoHttpResponse, VideoOperationHttp,
};
use flare_grpc_proto::media::*;
use flare_im_service_kit::clients::GrpcClients;
use flare_server_core::http::{ApiResponse, Result};

use super::errors::media_rpc_error;
use super::identity::MediaIdentity;

#[utoipa::path(
    post,
    path = "/api/v1/medias/process-image",
    tag = "Media",
    request_body = ProcessImageHttpRequest,
    responses(
        (status = 200, description = "成功"),
        (status = 400, description = "参数错误：请求体不合法，或 Media 拒绝参数（reason=BAD_REQUEST）"),
        (status = 401, description = "缺少或无效的访问令牌"),
        (status = 403, description = "不是绑定租户的终端用户令牌（reason=MEDIA_IDENTITY_REQUIRED）、租户不可用，或 Media 拒绝越权（reason=FORBIDDEN）"),
        (status = 404, description = "文件不存在，或属于其他用户、其他租户（reason=NOT_FOUND，不回显标识）"),
        (status = 500, description = "内部错误（reason=MEDIA_REQUEST_FAILED，不回显 Media 文案）"),
        (status = 503, description = "Media 不可用，可稍后重试（reason=MEDIA_UNAVAILABLE）"),
        (status = 504, description = "Media 超时，可稍后重试（reason=MEDIA_TIMEOUT）"),
    ),
)]
#[instrument(skip(caller, clients))]
pub async fn process_image(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<ProcessImageHttpRequest>,
) -> Result<Json<ApiResponse<ProcessImageHttpResponse>>> {
    let ctx = caller.ctx();
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

    let grpc_res = clients
        .bounded(async {
            let mut media_client = clients.media.lock().await;
            media_client.process_image(&caller, grpc_req).await
        })
        .await
        .map_err(media_rpc_error)?;
    Ok(Json(ApiResponse::success(ProcessImageHttpResponse::from(
        grpc_res,
    ))))
}

#[utoipa::path(
    post,
    path = "/api/v1/medias/process-video",
    tag = "Media",
    request_body = ProcessVideoHttpRequest,
    responses(
        (status = 200, description = "成功"),
        (status = 400, description = "参数错误：请求体不合法，或 Media 拒绝参数（reason=BAD_REQUEST）"),
        (status = 401, description = "缺少或无效的访问令牌"),
        (status = 403, description = "不是绑定租户的终端用户令牌（reason=MEDIA_IDENTITY_REQUIRED）、租户不可用，或 Media 拒绝越权（reason=FORBIDDEN）"),
        (status = 404, description = "文件不存在，或属于其他用户、其他租户（reason=NOT_FOUND，不回显标识）"),
        (status = 500, description = "内部错误（reason=MEDIA_REQUEST_FAILED，不回显 Media 文案）"),
        (status = 503, description = "Media 不可用，可稍后重试（reason=MEDIA_UNAVAILABLE）"),
        (status = 504, description = "Media 超时，可稍后重试（reason=MEDIA_TIMEOUT）"),
    ),
)]
#[instrument(skip(caller, clients))]
pub async fn process_video(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<ProcessVideoHttpRequest>,
) -> Result<Json<ApiResponse<ProcessVideoHttpResponse>>> {
    let ctx = caller.ctx();
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

    let grpc_res = clients
        .bounded(async {
            let mut media_client = clients.media.lock().await;
            media_client.process_video(&caller, grpc_req).await
        })
        .await
        .map_err(media_rpc_error)?;
    Ok(Json(ApiResponse::success(ProcessVideoHttpResponse::from(
        grpc_res,
    ))))
}
