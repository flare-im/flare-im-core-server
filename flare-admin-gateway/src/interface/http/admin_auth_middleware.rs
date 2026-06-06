use std::sync::Arc;

use axum::{
    Json,
    extract::Request,
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    middleware::Next,
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

use super::admin_contract::{APP_ID_HEADER, AUDIT_REASON_HEADER, IDEMPOTENCY_KEY_HEADER};

pub async fn admin_auth_middleware(
    axum::extract::Extension(validator): axum::extract::Extension<Arc<dyn TokenValidator>>,
    request: Request,
    next: Next,
) -> Response {
    let request = match authenticate_request(&validator, request).await {
        Ok(request) => request,
        Err(err) => return auth_error_response(err),
    };

    let Some(principal) = request.extensions().get::<AuthenticatedPrincipal>() else {
        return auth_error_response(AuthError::InvalidToken(
            "authenticated principal is missing".to_string(),
        ));
    };
    if let Err(err) = authorize_admin_request(request.method(), request.headers(), principal) {
        return auth_error_response(err);
    }

    next.run(request).await
}

async fn authenticate_request(
    validator: &Arc<dyn TokenValidator>,
    request: Request,
) -> Result<Request, AuthError> {
    let token = extract_bearer_token(request.headers())?;
    let validation_request = build_validation_request(token, &request);

    match validator.validate(validation_request).await {
        Ok(principal) => {
            debug!(
                user_id = %principal.user_id,
                tenant_id = ?principal.tenant_id,
                app_id = ?principal.app_id,
                "admin gateway token validated"
            );

            let mut request = request;
            inject_principal(&mut request, &principal)?;
            request.extensions_mut().insert(principal);
            Ok(request)
        }
        Err(err) => Err(err),
    }
}

fn extract_bearer_token(headers: &HeaderMap) -> Result<String, AuthError> {
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

fn build_validation_request(token: String, request: &Request) -> TokenValidationRequest {
    TokenValidationRequest {
        token,
        trace_id: header_value(request.headers(), keys::TRACE_ID),
        request_id: header_value(request.headers(), keys::REQUEST_ID),
        path: Some(request.uri().path().to_string()),
        method: Some(request.method().as_str().to_string()),
    }
}

fn inject_principal(
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

fn authorize_admin_request(
    method: &Method,
    headers: &HeaderMap,
    principal: &AuthenticatedPrincipal,
) -> Result<(), AuthError> {
    if !principal.has_gateway_admin_scope() {
        return Err(AuthError::Forbidden(
            "admin scope is required for admin api".to_string(),
        ));
    }
    let tenant_id = header_value(headers, keys::TENANT_ID).ok_or_else(|| {
        AuthError::Forbidden("tenant context is required for admin api".to_string())
    })?;
    if let Some(principal_tenant) = principal.tenant_id.as_deref().map(str::trim)
        && !principal_tenant.is_empty()
        && principal_tenant != tenant_id
    {
        return Err(AuthError::Forbidden(
            "admin tenant context does not match authenticated principal".to_string(),
        ));
    }
    if !is_admin_write_method(method) {
        return Ok(());
    }

    require_header(
        headers,
        keys::ACTOR_ID,
        "actor is required for admin write api",
    )?;
    require_header(
        headers,
        AUDIT_REASON_HEADER,
        "audit reason is required for admin write api",
    )?;

    if header_value(headers, IDEMPOTENCY_KEY_HEADER).is_none()
        && header_value(headers, keys::REQUEST_ID).is_none()
    {
        return Err(AuthError::Forbidden(
            "x-request-id or idempotency-key is required for admin write api".to_string(),
        ));
    }

    Ok(())
}

fn require_header(
    headers: &HeaderMap,
    name: &str,
    error_message: &'static str,
) -> Result<(), AuthError> {
    header_value(headers, name)
        .map(|_| ())
        .ok_or_else(|| AuthError::Forbidden(error_message.to_string()))
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn is_admin_write_method(method: &Method) -> bool {
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

fn auth_error_response(error: AuthError) -> Response {
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
    warn!(%status, %reason, error = %error, "admin gateway authentication failed");

    let error = ErrorBuilder::new(code, reason).build_error();
    let body: ApiResponse<()> = ApiResponse::from(error);
    (status, Json(body)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_authorization_accepts_admin_gateway_scope() {
        let mut headers = HeaderMap::new();
        headers.insert(keys::TENANT_ID, HeaderValue::from_static("tenant-a"));
        let principal = AuthenticatedPrincipal {
            user_id: "admin-a".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            device_id: None,
            app_id: None,
            expires_at: None,
            scopes: vec!["admin_gateway:admin:*".to_string()],
            metadata: Default::default(),
        };

        authorize_admin_request(&Method::GET, &headers, &principal).expect("admin read");
    }

    #[test]
    fn admin_write_requires_audit_context() {
        let mut headers = HeaderMap::new();
        headers.insert(keys::TENANT_ID, HeaderValue::from_static("tenant-a"));
        let principal = AuthenticatedPrincipal {
            user_id: "admin-a".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            device_id: None,
            app_id: None,
            expires_at: None,
            scopes: vec!["admin_gateway:admin:*".to_string()],
            metadata: Default::default(),
        };

        assert!(matches!(
            authorize_admin_request(&Method::POST, &headers, &principal),
            Err(AuthError::Forbidden(_))
        ));

        headers.insert(keys::ACTOR_ID, HeaderValue::from_static("admin-a"));
        headers.insert(AUDIT_REASON_HEADER, HeaderValue::from_static("ops-audit"));
        headers.insert(keys::REQUEST_ID, HeaderValue::from_static("request-a"));

        authorize_admin_request(&Method::POST, &headers, &principal).expect("admin write");
    }

    #[test]
    fn admin_authorization_rejects_tenant_mismatch() {
        let mut headers = HeaderMap::new();
        headers.insert(keys::TENANT_ID, HeaderValue::from_static("tenant-b"));
        let principal = AuthenticatedPrincipal {
            user_id: "admin-a".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            device_id: None,
            app_id: None,
            expires_at: None,
            scopes: vec!["admin_gateway:admin:*".to_string()],
            metadata: Default::default(),
        };

        assert!(matches!(
            authorize_admin_request(&Method::GET, &headers, &principal),
            Err(AuthError::Forbidden(_))
        ));
    }
}
