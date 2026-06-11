//! Shared HTTP gateway authentication helpers.
//!
//! Token validation itself lives in `flare-server-core::auth`; this module keeps
//! Axum request adaptation consistent across Flare IM gateways.

use std::sync::Arc;

use axum::{
    Json,
    extract::Request,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use flare_core_base::{
    context::keys,
    error::{ErrorBuilder, ErrorCode},
};
use flare_server_core::{
    AuthError, AuthenticatedPrincipal, TokenValidationRequest, TokenValidator, http::ApiResponse,
};
use tracing::{debug, warn};

pub const APP_ID_HEADER: &str = "x-app-id";

pub async fn authenticate_http_request(
    validator: &Arc<dyn TokenValidator>,
    request: Request,
    gateway_name: &'static str,
) -> Result<Request, AuthError> {
    let token = extract_bearer_token(request.headers())?;
    let validation_request = build_validation_request(token, &request);

    match validator.validate(validation_request).await {
        Ok(principal) => {
            debug!(
                gateway = gateway_name,
                user_id = %principal.user_id,
                tenant_id = ?principal.tenant_id,
                device_id = ?principal.device_id,
                app_id = ?principal.app_id,
                "gateway token validated"
            );

            let mut request = request;
            inject_principal(&mut request, &principal)?;
            request.extensions_mut().insert(principal);
            Ok(request)
        }
        Err(err) => Err(err),
    }
}

pub fn extract_bearer_token(headers: &HeaderMap) -> Result<String, AuthError> {
    let value = headers
        .get("authorization")
        .ok_or(AuthError::MissingToken)?
        .to_str()
        .map_err(|_| AuthError::InvalidToken("authorization header is not valid utf-8".into()))?;
    let Some((scheme, token)) = value.split_once(' ') else {
        return Err(AuthError::InvalidToken(
            "authorization header must use bearer scheme".into(),
        ));
    };
    if !scheme.eq_ignore_ascii_case("bearer") {
        return Err(AuthError::InvalidToken(
            "authorization header must use bearer scheme".into(),
        ));
    }

    let token = token.trim();
    if token.is_empty() {
        return Err(AuthError::MissingToken);
    }

    Ok(token.to_string())
}

pub fn build_validation_request(token: String, request: &Request) -> TokenValidationRequest {
    TokenValidationRequest {
        token,
        trace_id: header_value(request.headers(), keys::TRACE_ID),
        request_id: header_value(request.headers(), keys::REQUEST_ID),
        path: Some(request.uri().path().to_string()),
        method: Some(request.method().as_str().to_string()),
    }
}

pub fn inject_principal(
    request: &mut Request,
    principal: &AuthenticatedPrincipal,
) -> Result<(), AuthError> {
    insert_required_header(request.headers_mut(), keys::USER_ID, &principal.user_id)?;
    if let Some(device_id) = principal.device_id.as_deref() {
        insert_optional_header(request.headers_mut(), keys::DEVICE_ID, device_id)?;
    }
    if let Some(tenant_id) = principal.tenant_id.as_deref() {
        insert_optional_header(request.headers_mut(), keys::TENANT_ID, tenant_id)?;
    }
    if let Some(app_id) = principal.app_id.as_deref() {
        insert_optional_header(request.headers_mut(), APP_ID_HEADER, app_id)?;
    }
    Ok(())
}

pub fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub fn auth_error_response(error: AuthError, gateway_name: &'static str) -> Response {
    let (status, code, reason) = match &error {
        AuthError::MissingToken | AuthError::InvalidToken(_) => (
            StatusCode::UNAUTHORIZED,
            ErrorCode::HttpUnauthorized,
            "UNAUTHORIZED",
        ),
        AuthError::Forbidden(_) => (StatusCode::FORBIDDEN, ErrorCode::HttpForbidden, "FORBIDDEN"),
        AuthError::ProviderUnavailable(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::HttpServiceUnavailable,
            "AUTH_PROVIDER_UNAVAILABLE",
        ),
    };
    warn!(
        gateway = gateway_name,
        %status,
        %reason,
        error = %error,
        "gateway authentication failed"
    );

    let error = ErrorBuilder::new(code, reason).build_error();
    let body: ApiResponse<()> = ApiResponse::from(error);
    (status, Json(body)).into_response()
}

fn insert_required_header(
    headers: &mut HeaderMap,
    name: &'static str,
    value: &str,
) -> Result<(), AuthError> {
    let value = HeaderValue::from_str(value)
        .map_err(|_| AuthError::InvalidToken(format!("principal {name} is invalid")))?;
    headers.insert(name, value);
    Ok(())
}

fn insert_optional_header(
    headers: &mut HeaderMap,
    name: &'static str,
    value: &str,
) -> Result<(), AuthError> {
    if value.trim().is_empty() {
        return Ok(());
    }
    let value = HeaderValue::from_str(value)
        .map_err(|_| AuthError::InvalidToken(format!("principal {name} is invalid")))?;
    headers.insert(name, value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;

    #[test]
    fn extracts_bearer_token_case_insensitively() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("bearer token-a"));

        assert_eq!(extract_bearer_token(&headers).unwrap(), "token-a");
    }

    #[test]
    fn rejects_missing_or_malformed_bearer_token() {
        let headers = HeaderMap::new();
        assert!(matches!(
            extract_bearer_token(&headers),
            Err(AuthError::MissingToken)
        ));

        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("Basic token-a"));
        assert!(matches!(
            extract_bearer_token(&headers),
            Err(AuthError::InvalidToken(_))
        ));
    }

    #[test]
    fn injects_principal_context_headers() {
        let mut request = Request::builder().body(Body::empty()).unwrap();
        let principal = AuthenticatedPrincipal {
            user_id: "user-a".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            device_id: Some("device-a".to_string()),
            app_id: Some("business-console".to_string()),
            expires_at: None,
            scopes: Vec::new(),
            metadata: Default::default(),
        };

        inject_principal(&mut request, &principal).expect("headers");

        assert_eq!(request.headers().get(keys::USER_ID).unwrap(), "user-a");
        assert_eq!(request.headers().get(keys::TENANT_ID).unwrap(), "tenant-a");
        assert_eq!(request.headers().get(keys::DEVICE_ID).unwrap(), "device-a");
        assert_eq!(
            request.headers().get(APP_ID_HEADER).unwrap(),
            "business-console"
        );
    }
}
