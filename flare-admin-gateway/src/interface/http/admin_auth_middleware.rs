use std::sync::Arc;

use axum::{
    extract::Request,
    http::{HeaderMap, Method},
    middleware::Next,
    response::Response,
};
use flare_core_base::context::keys;
use flare_im_service_kit::gateway_auth::{
    auth_error_response, authenticate_http_request, header_value,
};
use flare_server_core::{AuthError, AuthenticatedPrincipal, TokenValidator};

use super::admin_contract::{AUDIT_REASON_HEADER, IDEMPOTENCY_KEY_HEADER};

const GATEWAY_NAME: &str = "admin-gateway";

pub async fn admin_auth_middleware(
    axum::extract::Extension(validator): axum::extract::Extension<Arc<dyn TokenValidator>>,
    request: Request,
    next: Next,
) -> Response {
    let request = match authenticate_http_request(&validator, request, GATEWAY_NAME).await {
        Ok(request) => request,
        Err(err) => return auth_error_response(err, GATEWAY_NAME),
    };

    let Some(principal) = request.extensions().get::<AuthenticatedPrincipal>() else {
        return auth_error_response(
            AuthError::InvalidToken("authenticated principal is missing".to_string()),
            GATEWAY_NAME,
        );
    };
    if let Err(err) = authorize_admin_request(request.method(), request.headers(), principal) {
        return auth_error_response(err, GATEWAY_NAME);
    }

    next.run(request).await
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

fn is_admin_write_method(method: &Method) -> bool {
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

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
