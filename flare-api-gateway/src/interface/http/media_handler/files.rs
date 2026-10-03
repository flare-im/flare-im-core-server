use axum::{
    body::{Body, Bytes},
    extract::{Extension, Json, Path},
    http::{HeaderValue, header},
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
use flare_server_core::error::{ErrorBuilder, ErrorCode};
use flare_server_core::http::{ApiResponse, HttpApiError as GatewayError, Result};

use super::errors::media_rpc_error;
use super::identity::MediaIdentity;

#[utoipa::path(
    post,
    path = "/api/v1/medias/file-url",
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
pub async fn get_file_url(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<GetFileUrlHttpRequestDto>,
) -> Result<Json<ApiResponse<GetFileUrlHttpResponse>>> {
    let ctx = caller.ctx();
    debug!(trace_id = %ctx.trace_id(), file_id = %req.file_id, "Getting file URL");

    let grpc_res = clients
        .bounded(async {
            let mut media_client = clients.media.lock().await;
            media_client.get_file_url(&caller, req.into()).await
        })
        .await
        .map_err(media_rpc_error)?;
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
pub async fn get_file_info(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    axum::extract::Query(req): axum::extract::Query<GetFileInfoRequest>,
) -> Result<Json<ApiResponse<FileInfoHttpResponse>>> {
    let ctx = caller.ctx();
    debug!(trace_id = %ctx.trace_id(), file_id = %req.file_id, "Getting file info");

    let grpc_res = clients
        .bounded(async {
            let mut media_client = clients.media.lock().await;
            media_client.get_file_info(&caller, req).await
        })
        .await
        .map_err(media_rpc_error)?;
    let info = grpc_res
        .info
        .ok_or_else(|| GatewayError::not_found("File info not found"))?;
    Ok(Json(ApiResponse::success(FileInfoHttpResponse::from(info))))
}

pub async fn serve_file(
    MediaIdentity(caller): MediaIdentity,
    Path(file_id): Path<String>,
    Extension(clients): Extension<Arc<GrpcClients>>,
) -> Result<Response> {
    let ctx = caller.ctx();
    debug!(trace_id = %ctx.trace_id(), file_id = %file_id, "Serving media file");

    let grpc_res = clients
        .bounded(async {
            let mut media_client = clients.media.lock().await;
            media_client
                .get_file_info(
                    &caller,
                    GetFileInfoRequest {
                        file_id: file_id.clone(),
                    },
                )
                .await
        })
        .await
        .map_err(media_rpc_error)?;
    let info = grpc_res
        .info
        .ok_or_else(|| GatewayError::not_found("File info not found"))?;

    if !info.bucket.trim().is_empty() {
        let url_res = clients
            .bounded(async {
                let mut media_client = clients.media.lock().await;
                media_client
                    .get_file_url(
                        &caller,
                        GetFileUrlRequest {
                            file_id,
                            expires_in: 3600,
                            download: false,
                            response_headers: HashMap::new(),
                            retention_protected: false,
                            content_visibility: flare_proto::common::ContentVisibility::Available
                                as i32,
                        },
                    )
                    .await
            })
            .await
            .map_err(media_rpc_error)?;

        let Some(redirect_to) = redirect_target(&url_res.cdn_url, &url_res.url) else {
            // 只记形状：url 是签名地址，不能进日志。
            tracing::warn!(
                trace_id = %ctx.trace_id(),
                cdn_url_present = !url_res.cdn_url.is_empty(),
                url_present = !url_res.url.is_empty(),
                "media returned no absolute download url"
            );
            return Err(GatewayError(
                ErrorBuilder::new(ErrorCode::HttpBadGateway, "MEDIA_URL_UNAVAILABLE").build_error(),
            ));
        };
        return Ok(Redirect::temporary(redirect_to).into_response());
    }

    // 请求超时只限到下载流打开（Media 回应答头）为止；之后按块转发的整段下载不限时，大文件
    // 不会被 GRPC_REQUEST_TIMEOUT_SECS 截断。
    let stream = clients
        .bounded(async {
            let mut media_client = clients.media.lock().await;
            media_client
                .download_file(
                    &caller,
                    DownloadFileRequest {
                        file_id: file_id.clone(),
                    },
                )
                .await
        })
        .await
        .map_err(media_rpc_error)?;

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
pub async fn delete_file(
    MediaIdentity(caller): MediaIdentity,
    Extension(clients): Extension<Arc<GrpcClients>>,
    Json(req): Json<DeleteFileRequest>,
) -> Result<Json<ApiResponse<DeleteFileResponse>>> {
    let ctx = caller.ctx();
    debug!(trace_id = %ctx.trace_id(), file_id = %req.file_id, "Deleting file");

    let grpc_res = clients
        .bounded(async {
            let mut media_client = clients.media.lock().await;
            media_client.delete_file(&caller, req).await
        })
        .await
        .map_err(media_rpc_error)?;
    Ok(Json(ApiResponse::success(grpc_res)))
}

/// `/files/{id}` 的 307 目标：CDN 地址优先，其次 `url`（私有对象即签名 URL），只接受绝对 http(s)。
///
/// 空 `cdn_base_url` 曾让 Media 把裸对象 key 记成 cdn_url，作为 `Location` 会被按相对路径解析；
/// 这类历史值直接跳过。
fn redirect_target<'a>(cdn_url: &'a str, url: &'a str) -> Option<&'a str> {
    [cdn_url, url]
        .into_iter()
        .find(|candidate| is_absolute_http_url(candidate))
}

fn is_absolute_http_url(value: &str) -> bool {
    let prefix = value.get(..8).unwrap_or(value).to_ascii_lowercase();
    (prefix.starts_with("http://") || prefix.starts_with("https://"))
        && value.parse::<axum::http::Uri>().is_ok_and(|uri| {
            uri.scheme_str().is_some_and(|scheme| {
                scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
            }) && uri.host().is_some_and(|host| !host.is_empty())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIGNED: &str =
        "https://edge.test:18443/flare-media/tenant-a/media/x.txt?X-Amz-Signature=unit-test";

    #[test]
    fn bare_key_cdn_url_falls_back_to_the_signed_url() {
        for legacy in [
            "tenant-a/media/documents/2026/09/30/x.txt",
            "/tenant-a/media/x.txt",
            "//tenant-a/media/x.txt",
            "https:tenant-a/media/x.txt",
            " https://cdn.example.com/x.txt",
            "",
        ] {
            assert_eq!(redirect_target(legacy, SIGNED), Some(SIGNED), "{legacy:?}");
        }
    }

    #[test]
    fn absolute_cdn_url_still_wins() {
        let cdn = "https://cdn.example.com/tenant-a/media/x.txt";
        assert_eq!(redirect_target(cdn, SIGNED), Some(cdn));
    }

    #[test]
    fn no_absolute_candidate_means_no_redirect() {
        assert_eq!(redirect_target("tenant-a/x.txt", "tenant-a/x.txt"), None);
        assert_eq!(redirect_target("", ""), None);
        assert_eq!(redirect_target("", "ftp://files.example.com/x"), None);
    }
}
