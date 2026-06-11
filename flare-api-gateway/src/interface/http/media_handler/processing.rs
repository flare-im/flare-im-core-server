use axum::{
    extract::{Extension, Json},
    http::HeaderMap,
};
use std::sync::Arc;
use tracing::{debug, instrument};

use crate::application::dto::{
    ImageOperationHttp, ProcessImageHttpRequest, ProcessImageHttpResponse, ProcessVideoHttpRequest,
    ProcessVideoHttpResponse, VideoOperationHttp,
};
use flare_grpc_proto::media::*;
use flare_im_service_kit::clients::GrpcClients;
use flare_server_core::{
    context::Ctx,
    http::{ApiResponse, ContextFromHeaders, Result},
};

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
