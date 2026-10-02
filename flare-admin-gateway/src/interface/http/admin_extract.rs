//! 请求体与查询串的提取器：解析失败时回网关自己的类型化 400 信封（`code`、`reason`、
//! `message`，与其他错误同形），而不是 axum 默认的纯文本 400、415 或 422。
//!
//! 调用方 Control 按 HTTP 状态映射核心管理错误：axum 默认的 422 曾被它当成 Core 故障
//! （503 `error.im.core.unavailable`），字段类型写错的检索在 Console 上显示为 Core 不可用。
//! 解析器的错误文本里有请求里的取值与字段位置，不回显，也不进日志；答复只给固定说明。
use axum::{
    Json,
    extract::{
        FromRequest, FromRequestParts, Query, Request,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{StatusCode, request::Parts},
};
use flare_server_core::http::HttpApiError;
use serde::de::DeserializeOwned;

/// 请求体不是声明为 `application/json` 的 JSON，或字段缺失、类型不对。
pub const INVALID_JSON_BODY: &str = "ADMIN_INVALID_JSON_BODY";
/// 请求体超过网关的大小上限。
pub const REQUEST_BODY_TOO_LARGE: &str = "ADMIN_REQUEST_BODY_TOO_LARGE";
/// 查询串的参数缺失或类型不对。
pub const INVALID_QUERY: &str = "ADMIN_INVALID_QUERY";

/// [`Json`] 的替身：拒绝时回 400 [`INVALID_JSON_BODY`]（超出大小上限为
/// [`REQUEST_BODY_TOO_LARGE`]）。
pub struct AdminJson<T>(pub T);

impl<T, S> FromRequest<S> for AdminJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = HttpApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(request, state).await {
            Ok(Json(value)) => Ok(Self(value)),
            Err(rejection) => Err(json_rejection(&rejection)),
        }
    }
}

/// [`Query`] 的替身：拒绝时回 400 [`INVALID_QUERY`]。
pub struct AdminQuery<T>(pub T);

impl<T, S> FromRequestParts<S> for AdminQuery<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = HttpApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match Query::<T>::from_request_parts(parts, state).await {
            Ok(Query(value)) => Ok(Self(value)),
            Err(rejection) => Err(query_rejection(&rejection)),
        }
    }
}

fn json_rejection(rejection: &JsonRejection) -> HttpApiError {
    // 只记拒绝的种类与 axum 原本的状态码；body_text() 含请求取值，不记。
    let kind = match rejection {
        JsonRejection::JsonDataError(_) => "data",
        JsonRejection::JsonSyntaxError(_) => "syntax",
        JsonRejection::MissingJsonContentType(_) => "content_type",
        JsonRejection::BytesRejection(_) => "body",
        _ => "other",
    };
    tracing::debug!(
        kind,
        axum_status = rejection.status().as_u16(),
        "admin request body rejected"
    );
    if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
        return HttpApiError::bad_request(
            REQUEST_BODY_TOO_LARGE,
            "request body exceeds the gateway size limit",
        );
    }
    HttpApiError::bad_request(
        INVALID_JSON_BODY,
        "request body must be JSON sent as application/json, with every required field present and of the documented type",
    )
}

fn query_rejection(rejection: &QueryRejection) -> HttpApiError {
    tracing::debug!(
        axum_status = rejection.status().as_u16(),
        "admin query string rejected"
    );
    HttpApiError::bad_request(
        INVALID_QUERY,
        "query parameters must be present where required and of the documented type",
    )
}
