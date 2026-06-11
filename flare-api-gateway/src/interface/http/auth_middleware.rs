use std::sync::Arc;

use axum::{
    extract::{Extension, Request},
    middleware::Next,
    response::Response,
};
use flare_im_service_kit::gateway_auth::{auth_error_response, authenticate_http_request};
use flare_server_core::TokenValidator;

const GATEWAY_NAME: &str = "api-gateway";

pub async fn gateway_auth_middleware(
    Extension(validator): Extension<Arc<dyn TokenValidator>>,
    request: Request,
    next: Next,
) -> Response {
    match authenticate_http_request(&validator, request, GATEWAY_NAME).await {
        Ok(request) => next.run(request).await,
        Err(err) => auth_error_response(err, GATEWAY_NAME),
    }
}
