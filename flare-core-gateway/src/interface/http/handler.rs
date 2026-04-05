use axum::{
    extract::{Extension, Json},
    http::HeaderMap,
};
use std::sync::Arc;
use tracing::{info, instrument};

use crate::context::Ctx;
use crate::error::{GatewayError, Result};
use crate::infrastructure::grpc::GrpcClients;
use crate::interface::http::response::*;

use flare_grpc_proto::media::*;
use flare_server_core::context::keys;

/// 生成上传 URL
#[utoipa::path(
    post,
    path = "/api/v1/medias/upload-url",
    tag = "Media",
    request_body = GenerateUploadUrlHttpRequest,
    responses(
        (status = 200, description = "成功", body = ApiResponse<GenerateUploadUrlHttpResponse>),
        (status = 400, description = "参数错误", body = ErrorResponse),
        (status = 500, description = "内部错误", body = ErrorResponse),
    ),
)]
#[instrument(skip(headers, clients))]
pub async fn generate_upload_url(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<GenerateUploadUrlHttpRequest>,
) -> Result<Json<ApiResponse<GenerateUploadUrlHttpResponse>>> {
    // 1. 从 Header 构建上下文 (包含认证中间件注入的用户信息)
    let ctx = Ctx::from_headers(&headers);
    
    // 2. 提取用户信息 (由认证中间件注入)
    let user_id = headers
        .get(keys::USER_ID)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous");
    
    info!(
        trace_id = %ctx.trace_id,
        user_id = %user_id,
        "Generating upload URL"
    );

    // 3. 构建 gRPC 请求
    let grpc_req = GenerateUploadUrlRequest {
        bucket: req.bucket,
        object_key: req.object_key,
        mime_type: req.mime_type,
        expected_size: req.expected_size,
        expires_in: req.expires_in,
    };

    // 4. 调用 gRPC 服务
    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.generate_upload_url(grpc_req).await?;

    // 5. 转换响应
    let response = GenerateUploadUrlHttpResponse {
        upload_url: grpc_res.upload_url,
        object_key: grpc_res.object_key,
    };

    Ok(Json(ApiResponse::success(response)))
}

/// 获取文件 URL
#[utoipa::path(
    post,
    path = "/api/v1/medias/file-url",
    tag = "Media",
    request_body = GetFileUrlHttpRequest,
    responses(
        (status = 200, description = "成功", body = ApiResponse<GetFileUrlHttpResponse>),
        (status = 400, description = "参数错误", body = ErrorResponse),
        (status = 404, description = "文件不存在", body = ErrorResponse),
    ),
)]
#[instrument(skip(headers, clients))]
pub async fn get_file_url(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<GetFileUrlHttpRequest>,
) -> Result<Json<ApiResponse<GetFileUrlHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    info!(trace_id = %ctx.trace_id, file_id = %req.file_id, "Getting file URL");

    let grpc_req = GetFileUrlRequest {
        file_id: req.file_id,
        expires_in: req.expires_in,
        download: req.download,
        response_headers: std::collections::HashMap::new(),
    };

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.get_file_url(grpc_req).await?;

    let response = GetFileUrlHttpResponse {
        url: grpc_res.url,
        cdn_url: if grpc_res.cdn_url.is_empty() {
            None
        } else {
            Some(grpc_res.cdn_url)
        },
    };

    Ok(Json(ApiResponse::success(response)))
}

/// 获取文件信息
#[utoipa::path(
    get,
    path = "/api/v1/medias/file-info",
    tag = "Media",
    params(
        ("file_id" = String, Query, description = "文件 ID"),
    ),
    responses(
        (status = 200, description = "成功", body = ApiResponse<FileInfoHttpResponse>),
        (status = 404, description = "文件不存在", body = ErrorResponse),
    ),
)]
#[instrument(skip(headers, clients))]
pub async fn get_file_info(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    axum::extract::Query(req): axum::extract::Query<GetFileInfoHttpRequest>,
) -> Result<Json<ApiResponse<FileInfoHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    info!(trace_id = %ctx.trace_id, file_id = %req.file_id, "Getting file info");

    let grpc_req = GetFileInfoRequest {
        file_id: req.file_id,
    };

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.get_file_info(grpc_req).await?;

    let info = grpc_res.info.ok_or_else(|| {
        GatewayError::NotFound("File info not found".to_string())
    })?;

    let response = FileInfoHttpResponse {
        file_id: info.file_id,
        file_name: info.file_name,
        mime_type: info.mime_type,
        size: info.size,
        url: if info.url.is_empty() { None } else { Some(info.url) },
        cdn_url: if info.cdn_url.is_empty() { None } else { Some(info.cdn_url) },
    };

    Ok(Json(ApiResponse::success(response)))
}

/// 删除文件
#[utoipa::path(
    delete,
    path = "/api/v1/medias/file",
    tag = "Media",
    request_body = DeleteFileHttpRequest,
    responses(
        (status = 200, description = "成功", body = ApiResponse<DeleteFileHttpResponse>),
        (status = 404, description = "文件不存在", body = ErrorResponse),
    ),
)]
#[instrument(skip(headers, clients))]
pub async fn delete_file(
    headers: HeaderMap,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<DeleteFileHttpRequest>,
) -> Result<Json<ApiResponse<DeleteFileHttpResponse>>> {
    let ctx = Ctx::from_headers(&headers);
    info!(trace_id = %ctx.trace_id, file_id = %req.file_id, "Deleting file");

    let grpc_req = DeleteFileRequest {
        file_id: req.file_id,
        hard_delete: req.hard_delete,
    };

    let mut media_client = clients.media.lock().await;
    let grpc_res = media_client.delete_file(grpc_req).await?;

    let response = DeleteFileHttpResponse {
        success: grpc_res.success,
    };

    Ok(Json(ApiResponse::success(response)))
}
