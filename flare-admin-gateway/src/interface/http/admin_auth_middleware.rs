use axum::{
    extract::Request,
    http::{HeaderMap, HeaderValue, Method},
    middleware::Next,
    response::Response,
};
use flare_core_base::context::keys;
use flare_im_service_kit::control_auth::{
    CORE_ADMIN_AUDIENCE, ConsoleControlVerifier, ControlActorKind, VerifiedControlActor,
    bearer_token,
};
use flare_im_service_kit::gateway_auth::{auth_error_response, header_value};
use flare_server_core::{AuthError, AuthenticatedPrincipal};
use std::sync::Arc;
use tonic::Status;

use super::admin_contract::{AUDIT_REASON_HEADER, IDEMPOTENCY_KEY_HEADER};

const GATEWAY_NAME: &str = "admin-gateway";

/// The admin assertion this middleware has just verified, kept verbatim for the
/// downstream admin RPCs (Conversation, Online, Capability re-verify it themselves).
///
/// The gateway holds no signing key, so forwarding the unchanged assertion is the
/// only way it can speak for the employee; `Debug` never prints the token.
#[derive(Clone)]
pub struct VerifiedAdminAssertion(Arc<str>);

impl VerifiedAdminAssertion {
    pub fn token(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for VerifiedAdminAssertion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("VerifiedAdminAssertion(<redacted>)")
    }
}

pub async fn admin_auth_middleware(
    axum::extract::Extension(verifier): axum::extract::Extension<Arc<ConsoleControlVerifier>>,
    mut request: Request,
    next: Next,
) -> Response {
    let (actor, assertion) = match verified_admin_actor(&verifier, request.headers()) {
        Ok(verified) => verified,
        Err(status) => return control_status_response(status),
    };
    if let Err(status) = authorize_admin_request(
        request.method(),
        request.uri().path(),
        request.headers(),
        &actor,
    ) {
        return control_status_response(status);
    }

    // Downstream handlers still consume a request Context built from headers;
    // replace every identity-bearing legacy value with verified token claims.
    for name in [
        keys::USER_ID,
        keys::TENANT_ID,
        keys::ACTOR_ID,
        keys::DEVICE_ID,
        keys::SESSION_ID,
    ] {
        request.headers_mut().remove(name);
    }
    let untrusted_actor_headers: Vec<_> = request
        .headers()
        .keys()
        .filter(|name| name.as_str().starts_with("x-actor-"))
        .cloned()
        .collect();
    for name in untrusted_actor_headers {
        request.headers_mut().remove(name);
    }
    let Ok(subject) = HeaderValue::from_str(&actor.subject) else {
        return control_status_response(Status::unauthenticated("invalid control actor"));
    };
    let Ok(tenant) = HeaderValue::from_str(&actor.tenant_id) else {
        return control_status_response(Status::unauthenticated("invalid control tenant"));
    };
    request.headers_mut().insert(keys::USER_ID, subject.clone());
    request.headers_mut().insert(keys::ACTOR_ID, subject);
    request.headers_mut().insert(keys::TENANT_ID, tenant);
    request.extensions_mut().insert(AuthenticatedPrincipal {
        user_id: actor.subject.clone(),
        tenant_id: Some(actor.tenant_id.clone()),
        device_id: None,
        app_id: None,
        expires_at: None,
        scopes: vec!["admin_gateway:admin".into()],
        metadata: Default::default(),
    });
    request.extensions_mut().insert(assertion);

    next.run(request).await
}

fn verified_admin_actor(
    verifier: &ConsoleControlVerifier,
    headers: &HeaderMap,
) -> Result<(VerifiedControlActor, VerifiedAdminAssertion), Status> {
    let mut auth_values = headers.get_all("authorization").iter();
    let value = auth_values
        .next()
        .ok_or_else(|| Status::unauthenticated("admin control token required"))?;
    if auth_values.next().is_some() {
        return Err(Status::unauthenticated("duplicate authorization"));
    }
    let token = bearer_token(
        value
            .to_str()
            .map_err(|_| Status::unauthenticated("invalid authorization"))?,
    )
    .ok_or_else(|| Status::unauthenticated("bearer admin token required"))?;
    let actor = verifier.verify_token(token, "admin", CORE_ADMIN_AUDIENCE)?;
    actor.require_kind(ControlActorKind::Employee)?;
    actor.require_tenant(&actor.tenant_id)?;
    Ok((actor, VerifiedAdminAssertion(Arc::from(token))))
}

fn control_status_response(status: Status) -> Response {
    let error = if status.code() == tonic::Code::Unauthenticated {
        AuthError::InvalidToken(status.message().to_owned())
    } else {
        AuthError::Forbidden(status.message().to_owned())
    };
    auth_error_response(error, GATEWAY_NAME)
}

fn authorize_admin_request(
    method: &Method,
    path: &str,
    headers: &HeaderMap,
    actor: &VerifiedControlActor,
) -> Result<(), Status> {
    let tenant_id = header_value(headers, keys::TENANT_ID)
        .ok_or_else(|| Status::permission_denied("tenant context is required for admin api"))?;
    if tenant_id == "*" || headers.get_all(keys::TENANT_ID).iter().count() != 1 {
        return Err(Status::permission_denied(
            "one concrete tenant is required for admin api",
        ));
    }
    actor.require_tenant(&tenant_id)?;
    if headers.get_all(keys::ACTOR_ID).iter().count() > 1
        || header_value(headers, keys::ACTOR_ID).is_some_and(|id| id != actor.subject)
    {
        return Err(Status::permission_denied(
            "admin actor header does not match token",
        ));
    }
    if !is_admin_write_request(method, path) {
        return Ok(());
    }

    if header_value(headers, keys::ACTOR_ID).as_deref() != Some(actor.subject.as_str()) {
        return Err(Status::permission_denied(
            "verified actor header is required for admin write api",
        ));
    }
    require_header(
        headers,
        AUDIT_REASON_HEADER,
        "audit reason is required for admin write api",
    )?;

    if header_value(headers, IDEMPOTENCY_KEY_HEADER).is_none()
        && header_value(headers, keys::REQUEST_ID).is_none()
    {
        return Err(Status::permission_denied(
            "x-request-id or idempotency-key is required for admin write api",
        ));
    }

    Ok(())
}

fn require_header(
    headers: &HeaderMap,
    name: &str,
    error_message: &'static str,
) -> Result<(), Status> {
    header_value(headers, name)
        .map(|_| ())
        .ok_or_else(|| Status::permission_denied(error_message))
}

fn is_admin_write_request(method: &Method, path: &str) -> bool {
    // Axum's nested admin router passes the prefix-stripped path here.
    // These query POSTs are declared read-only by the admin contract;
    // message searches are audited by the Control Plane before this call.
    if *method == Method::POST && matches!(path, "/messages/query" | "/messages/write-ledger/query")
    {
        return false;
    }
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};

    fn signed_headers(
        subject: &str,
        tenant: &str,
        use_name: &str,
        audience: &str,
    ) -> (ConsoleControlVerifier, HeaderMap) {
        let key = SigningKey::from_bytes(&[89; 32]);
        let private = key.to_pkcs8_pem(Default::default()).unwrap();
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        let signer = flare_server_core::TokenService::from_ed25519_private_key(
            &private,
            flare_im_service_kit::control_auth::CONSOLE_ISSUER,
            60,
        )
        .unwrap();
        let token = signer
            .generate_with_ed25519(subject, Some(tenant), use_name, audience)
            .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("authorization", format!("Bearer {token}").parse().unwrap());
        (
            ConsoleControlVerifier::from_public_key(&public).unwrap(),
            headers,
        )
    }
    fn employee() -> VerifiedControlActor {
        VerifiedControlActor {
            subject: "employee-1".into(),
            tenant_id: "tenant-a".into(),
            kind: ControlActorKind::Employee,
        }
    }

    #[test]
    fn admin_read_requires_one_exact_tenant() {
        let actor = employee();
        let mut headers = HeaderMap::new();
        headers.insert(keys::TENANT_ID, HeaderValue::from_static("tenant-a"));
        authorize_admin_request(&Method::GET, "/api/v1/admin/messages/m1", &headers, &actor)
            .unwrap();
        headers.insert(keys::TENANT_ID, HeaderValue::from_static("tenant-b"));
        assert!(
            authorize_admin_request(&Method::GET, "/api/v1/admin/messages/m1", &headers, &actor)
                .is_err()
        );
        headers.insert(keys::TENANT_ID, HeaderValue::from_static("*"));
        assert!(
            authorize_admin_request(&Method::GET, "/api/v1/admin/messages/m1", &headers, &actor)
                .is_err()
        );
        headers.remove(keys::TENANT_ID);
        assert!(
            authorize_admin_request(&Method::GET, "/api/v1/admin/messages/m1", &headers, &actor)
                .is_err()
        );
        headers.insert(keys::TENANT_ID, HeaderValue::from_static("tenant-a"));
        headers.append(keys::TENANT_ID, HeaderValue::from_static("tenant-b"));
        assert!(
            authorize_admin_request(&Method::GET, "/api/v1/admin/messages/m1", &headers, &actor)
                .is_err()
        );
    }

    #[test]
    fn admin_write_requires_verified_actor_and_audit_context() {
        let actor = employee();
        let mut headers = HeaderMap::new();
        headers.insert(keys::TENANT_ID, HeaderValue::from_static("tenant-a"));
        assert!(
            authorize_admin_request(&Method::POST, "/api/v1/admin/messages/m1", &headers, &actor)
                .is_err()
        );
        headers.insert(keys::ACTOR_ID, HeaderValue::from_static("attacker"));
        assert!(
            authorize_admin_request(&Method::POST, "/api/v1/admin/messages/m1", &headers, &actor)
                .is_err()
        );
        headers.insert(keys::ACTOR_ID, HeaderValue::from_static("employee-1"));
        headers.insert(AUDIT_REASON_HEADER, HeaderValue::from_static("ops-audit"));
        headers.insert(keys::REQUEST_ID, HeaderValue::from_static("request-a"));
        authorize_admin_request(&Method::POST, "/api/v1/admin/messages/m1", &headers, &actor)
            .unwrap();
    }

    #[test]
    fn message_query_post_is_read_only_but_other_posts_still_require_audit_context() {
        let actor = employee();
        let mut headers = HeaderMap::new();
        headers.insert(keys::TENANT_ID, HeaderValue::from_static("tenant-a"));
        authorize_admin_request(&Method::POST, "/messages/query", &headers, &actor).unwrap();
        authorize_admin_request(
            &Method::POST,
            "/messages/write-ledger/query",
            &headers,
            &actor,
        )
        .unwrap();
        assert!(
            authorize_admin_request(&Method::POST, "/messages/export", &headers, &actor).is_err()
        );
        headers.insert(keys::TENANT_ID, HeaderValue::from_static("tenant-b"));
        assert!(
            authorize_admin_request(&Method::POST, "/messages/query", &headers, &actor).is_err()
        );
    }

    #[test]
    fn admin_boundary_accepts_only_signed_employee_admin_audience() {
        let (verifier, headers) =
            signed_headers("employee-1", "tenant-a", "admin", CORE_ADMIN_AUDIENCE);
        let (actor, assertion) = verified_admin_actor(&verifier, &headers).unwrap();
        assert_eq!(actor.subject, "employee-1");
        // The forwarded assertion is exactly the bearer token that was verified.
        let bearer = headers.get("authorization").unwrap().to_str().unwrap();
        assert_eq!(Some(assertion.token()), bearer.strip_prefix("Bearer "));
        assert!(!format!("{assertion:?}").contains(assertion.token()));
        let (_, headers) = signed_headers("employee-1", "tenant-a", "control", CORE_ADMIN_AUDIENCE);
        assert_eq!(
            verified_admin_actor(&verifier, &headers)
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        let (_, headers) = signed_headers(
            "employee-1",
            "tenant-a",
            "admin",
            flare_im_service_kit::control_auth::CORE_CONTROL_AUDIENCE,
        );
        assert_eq!(
            verified_admin_actor(&verifier, &headers)
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        let (_, headers) = signed_headers(
            flare_im_service_kit::control_auth::PROJECTION_WORKER_SUBJECT,
            "tenant-a",
            "admin",
            CORE_ADMIN_AUDIENCE,
        );
        assert_eq!(
            verified_admin_actor(&verifier, &headers)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }
}
