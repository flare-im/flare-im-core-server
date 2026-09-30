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

use crate::tenant_runtime::{TenantAccessPolicy, TenantLookup, TenantRuntimeCache};

pub const APP_ID_HEADER: &str = "x-app-id";

/// Tenant gate shared by API authentication and token issuance. A configured
/// projection always rejects suspended/deleting tenants; strict mode also
/// rejects missing or unavailable projections.
#[derive(Clone)]
pub struct TenantRuntimeGuard {
    cache: TenantRuntimeCache,
    policy: TenantAccessPolicy,
}

impl TenantRuntimeGuard {
    pub fn new(cache: TenantRuntimeCache, policy: TenantAccessPolicy) -> Self {
        Self { cache, policy }
    }

    pub async fn admit(&self, tenant_id: Option<&str>) -> Result<(), AuthError> {
        let tenant_id = tenant_id.map(str::trim).filter(|value| !value.is_empty());
        if tenant_id == Some("*")
            || (tenant_id.is_none() && self.policy == TenantAccessPolicy::Strict)
        {
            return Err(AuthError::Forbidden("tenant binding required".into()));
        }
        let tenant_id = tenant_id.unwrap_or("0");
        let lookup = self.cache.lookup(tenant_id).await;
        if lookup.admits_connection(self.policy) {
            return Ok(());
        }
        match lookup {
            TenantLookup::Unavailable => Err(AuthError::ProviderUnavailable(
                "tenant runtime projection unavailable".into(),
            )),
            TenantLookup::Known(_) | TenantLookup::Unknown => {
                Err(AuthError::Forbidden("TENANT_UNAVAILABLE".into()))
            }
        }
    }
}

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
    // Incoming identity headers are untrusted, including when a legacy token
    // has no tenant/device binding. Remove them before inserting verified data.
    for name in [
        keys::USER_ID,
        keys::DEVICE_ID,
        keys::TENANT_ID,
        APP_ID_HEADER,
    ] {
        request.headers_mut().remove(name);
    }
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

/// Gateway auth failure envelope. A `Forbidden` whose detail is a typed error
/// code (upper-case identifier such as `TENANT_UNAVAILABLE`) keeps it as the
/// reason, so the tenant gate answers with the same code as `/ws`, kicks and
/// message ingest; free-text details stay out of the response.
pub fn auth_error_response(error: AuthError, gateway_name: &'static str) -> Response {
    let (status, code, reason) = match &error {
        AuthError::MissingToken | AuthError::InvalidToken(_) => (
            StatusCode::UNAUTHORIZED,
            ErrorCode::HttpUnauthorized,
            "UNAUTHORIZED",
        ),
        AuthError::Forbidden(detail) => (
            StatusCode::FORBIDDEN,
            ErrorCode::HttpForbidden,
            if is_typed_reason(detail) {
                detail.as_str()
            } else {
                "FORBIDDEN"
            },
        ),
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

/// Upper-case letter first, then only upper-case letters, digits and `_`, at
/// most 64 bytes: the shape of a client-facing error code.
fn is_typed_reason(reason: &str) -> bool {
    let mut chars = reason.chars();
    chars.next().is_some_and(|c| c.is_ascii_uppercase())
        && reason.len() <= 64
        && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
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
    use crate::tenant_runtime::{TenantRuntimeCacheOptions, TenantRuntimeSource};
    use axum::body::Body;
    use flare_im_contracts::domain::tenant::{TenantQuota, TenantRuntimeSnapshot, TenantStatus};
    use std::collections::HashMap;

    struct TenantRows(HashMap<String, TenantStatus>);

    #[async_trait::async_trait]
    impl TenantRuntimeSource for TenantRows {
        async fn load(&self, tenant_id: &str) -> Result<Option<TenantRuntimeSnapshot>, String> {
            Ok(self.0.get(tenant_id).map(|status| TenantRuntimeSnapshot {
                tenant_id: tenant_id.to_string(),
                status: *status,
                quota: TenantQuota::default(),
                version: 1,
            }))
        }
    }

    fn tenant_guard(policy: TenantAccessPolicy) -> TenantRuntimeGuard {
        let cache = TenantRuntimeCache::with_source(
            Arc::new(TenantRows(HashMap::from([
                ("active".into(), TenantStatus::Active),
                ("suspended".into(), TenantStatus::Suspended),
                ("deleting".into(), TenantStatus::Deleting),
            ]))),
            TenantRuntimeCacheOptions::default(),
        );
        TenantRuntimeGuard::new(cache, policy)
    }

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

    #[test]
    fn unbound_principal_cannot_keep_spoofed_identity_headers() {
        let mut request = Request::builder()
            .header(keys::TENANT_ID, "victim")
            .header(keys::DEVICE_ID, "forged-device")
            .header(APP_ID_HEADER, "forged-app")
            .body(Body::empty())
            .unwrap();
        let principal = AuthenticatedPrincipal {
            user_id: "user-a".into(),
            tenant_id: None,
            device_id: None,
            app_id: None,
            expires_at: None,
            scopes: Vec::new(),
            metadata: Default::default(),
        };
        inject_principal(&mut request, &principal).unwrap();
        assert!(request.headers().get(keys::TENANT_ID).is_none());
        assert!(request.headers().get(keys::DEVICE_ID).is_none());
        assert!(request.headers().get(APP_ID_HEADER).is_none());
        assert_eq!(request.headers().get(keys::USER_ID).unwrap(), "user-a");
    }

    #[tokio::test]
    async fn tenant_guard_rejects_disabled_tenants_under_both_policies() {
        for policy in [TenantAccessPolicy::Lenient, TenantAccessPolicy::Strict] {
            let guard = tenant_guard(policy);
            assert!(guard.admit(Some("active")).await.is_ok());
            assert!(matches!(
                guard.admit(Some("suspended")).await,
                Err(AuthError::Forbidden(_))
            ));
            assert!(matches!(
                guard.admit(Some("deleting")).await,
                Err(AuthError::Forbidden(_))
            ));
            assert!(guard.admit(Some("*")).await.is_err());
        }
    }

    async fn response_reason(response: Response) -> (StatusCode, String) {
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        (
            status,
            body["reason"].as_str().unwrap_or_default().to_string(),
        )
    }

    /// The tenant gate answers 403 `TENANT_UNAVAILABLE` (the code `/ws`, kicks
    /// and message ingest use), not a bare `FORBIDDEN`; free-text details never
    /// become the reason.
    #[tokio::test]
    async fn tenant_rejection_keeps_its_typed_reason() {
        for policy in [TenantAccessPolicy::Lenient, TenantAccessPolicy::Strict] {
            for tenant in ["suspended", "deleting"] {
                let error = tenant_guard(policy).admit(Some(tenant)).await.unwrap_err();
                assert_eq!(
                    response_reason(auth_error_response(error, "test")).await,
                    (StatusCode::FORBIDDEN, "TENANT_UNAVAILABLE".to_string()),
                    "{tenant}"
                );
            }
        }
        let unbound = tenant_guard(TenantAccessPolicy::Strict)
            .admit(None)
            .await
            .unwrap_err();
        for error in [
            unbound,
            AuthError::Forbidden("tenantId does not match app credential".into()),
            AuthError::Forbidden("Tenant_x".into()),
            AuthError::Forbidden(String::new()),
        ] {
            assert_eq!(
                response_reason(auth_error_response(error, "test")).await,
                (StatusCode::FORBIDDEN, "FORBIDDEN".to_string())
            );
        }
        assert_eq!(
            response_reason(auth_error_response(AuthError::MissingToken, "test")).await,
            (StatusCode::UNAUTHORIZED, "UNAUTHORIZED".to_string())
        );
    }

    #[tokio::test]
    async fn tenant_guard_fail_closed_policy_applies_to_missing_projection() {
        assert!(
            tenant_guard(TenantAccessPolicy::Lenient)
                .admit(Some("unknown"))
                .await
                .is_ok()
        );
        assert!(
            tenant_guard(TenantAccessPolicy::Lenient)
                .admit(None)
                .await
                .is_ok()
        );
        assert!(
            tenant_guard(TenantAccessPolicy::Strict)
                .admit(Some("unknown"))
                .await
                .is_err()
        );
        assert!(
            tenant_guard(TenantAccessPolicy::Strict)
                .admit(None)
                .await
                .is_err()
        );
    }
}
