//! The admin boundary (v4 R3-2): one Staff `x-flare-ctx`, signed by the Social gateway admin
//! partition with the Cell key registered as `kid=gateway`, is the only identity on
//! `/api/v1/admin/**`.
//!
//! Order of checks: exactly one `x-flare-ctx` (401), verified Staff with an accepted kid (401
//! `ADMIN_STAFF_CONTEXT_INVALID`), the route exists in the shared Core admin route table (404),
//! the signed permission is the route's code (403 `ADMIN_PERMISSION_DENIED`), a platform-only
//! route is called by platform staff (403 `ADMIN_SCOPE_DENIED`), the signed application is one
//! concrete tenant (403 `ADMIN_IDENTITY_REQUIRED`), and a write route carries the audit headers
//! (403). Before the route checks, on reads too, the audit headers must be within their limits
//! (400 `ADMIN_AUDIT_HEADER_TOO_LONG`) and `x-audit-reason`, when present, must be one header of
//! percent-encoded UTF-8 (400 `ADMIN_AUDIT_REASON_INVALID`); see
//! [`audit_reason`](super::admin_contract::audit_reason). The gateway never reads `x-tenant-id`
//! or `x-actor-id`: tenant and actor come from the signed claims only, and every identity header
//! is stripped before the handlers run.
use axum::{
    extract::Request,
    http::{HeaderMap, Method},
    middleware::Next,
    response::{IntoResponse, Response},
};
use flare_core_base::context::keys;
use flare_im_service_kit::gateway_auth::{auth_error_response, header_value};
use flare_im_service_kit::staff_context::VerifiedStaff;
use flare_server_core::auth::core_admin::{self, CoreAdminRoute};
use flare_server_core::grpc::middleware::signed_context::{
    SIGNED_CONTEXT_HEADER, verify_signed_context_metadata,
};
use flare_server_core::{
    AuthError, AuthenticatedPrincipal, SignedActorType, SignedContextVerifier, VerifiedContext,
    context::Ctx,
    error::{ErrorBuilder, ErrorCode},
    http::HttpApiError,
};
use std::sync::Arc;
use tonic::Status;

use super::admin_contract::{
    AUDIT_REASON_HEADER, AuditReasonError, IDEMPOTENCY_KEY_HEADER, MAX_AUDIT_REASON_BYTES,
    MAX_AUDIT_REASON_HEADER_BYTES, MAX_CORRELATION_ID_BYTES, audit_reason,
};

const GATEWAY_NAME: &str = "admin-gateway";

/// The Staff context this middleware has just verified, kept verbatim for the downstream
/// admin RPCs (Conversation, Online, Capability and the Storage Reader re-verify it) and as
/// the context of the audit effect events.
///
/// The gateway signs nothing on that path, so forwarding the unchanged token is the only way
/// it can speak for the employee; `Debug` never prints the token.
#[derive(Clone)]
pub struct VerifiedStaffContext(Arc<VerifiedStaff>);

impl VerifiedStaffContext {
    pub fn new(staff: VerifiedStaff) -> Self {
        Self(Arc::new(staff))
    }

    /// The verified token, byte for byte.
    pub fn token(&self) -> &str {
        self.0.token()
    }

    /// The context the verifier built: request id and trace id from the claims, the
    /// `VerifiedContext` marker with the claims and the token.
    pub fn ctx(&self) -> &Ctx {
        &self.0.ctx
    }

    pub fn staff(&self) -> &VerifiedStaff {
        &self.0
    }

    pub fn tenant_id(&self) -> &str {
        &self.0.tenant_id
    }

    pub fn actor_id(&self) -> &str {
        &self.0.actor_id
    }

    pub fn request_id(&self) -> &str {
        &self.0.request_id
    }
}

impl std::fmt::Debug for VerifiedStaffContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("VerifiedStaffContext(<redacted>)")
    }
}

/// The resolved entry of the shared Core admin route table, for handlers that log the
/// permission code or branch on the write flag.
#[derive(Debug, Clone, Copy)]
pub struct AdminRoute(pub &'static CoreAdminRoute);

pub async fn admin_auth_middleware(
    axum::extract::Extension(verifier): axum::extract::Extension<Arc<SignedContextVerifier>>,
    mut request: Request,
    next: Next,
) -> Response {
    let staff = match verified_staff(&verifier, request.headers()) {
        Ok(staff) => staff,
        Err(rejection) => return *rejection,
    };
    if let Some((name, max)) = audit_header_too_long(request.headers()) {
        return bad_request(
            "ADMIN_AUDIT_HEADER_TOO_LONG",
            format!("{name} must be at most {max} bytes"),
        );
    }
    match audit_reason(request.headers()) {
        Ok(_) => {}
        Err(AuditReasonError::TooLong) => {
            return bad_request(
                "ADMIN_AUDIT_HEADER_TOO_LONG",
                format!(
                    "{AUDIT_REASON_HEADER} must be at most {MAX_AUDIT_REASON_BYTES} bytes once percent-decoded"
                ),
            );
        }
        Err(AuditReasonError::Invalid) => {
            return bad_request(
                "ADMIN_AUDIT_REASON_INVALID",
                format!("{AUDIT_REASON_HEADER} must be one header of percent-encoded UTF-8"),
            );
        }
    }
    // Axum's nested admin router passes the prefix-stripped path here.
    let route = match authorize_admin_request(
        request.method(),
        request.uri().path(),
        request.headers(),
        &staff,
    ) {
        Ok(route) => route,
        Err(rejection) => return *rejection,
    };

    // Downstream handlers never read identity from headers; drop every identity-bearing
    // value so a forged one cannot shape a legacy context either.
    for name in [
        keys::USER_ID,
        keys::TENANT_ID,
        keys::ACTOR_ID,
        keys::DEVICE_ID,
        keys::SESSION_ID,
        "authorization",
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
    request.extensions_mut().insert(AuthenticatedPrincipal {
        user_id: staff.actor_id.clone(),
        tenant_id: Some(staff.tenant_id.clone()),
        device_id: None,
        app_id: None,
        expires_at: None,
        scopes: vec!["admin_gateway:admin".into()],
        metadata: Default::default(),
    });
    request.extensions_mut().insert(AdminRoute(route));
    request
        .extensions_mut()
        .insert(VerifiedStaffContext::new(staff));

    next.run(request).await
}

/// A refusal, ready to send. Boxed: the response is large and the success path is the common one.
type Rejection = Box<Response>;

/// Exactly one `x-flare-ctx`, verified as a Staff actor by a key id the verifier accepts.
/// The permission and tenant are checked against the route afterwards.
fn verified_staff(
    verifier: &SignedContextVerifier,
    headers: &HeaderMap,
) -> Result<VerifiedStaff, Rejection> {
    let supplied = headers.get_all(SIGNED_CONTEXT_HEADER).iter().count();
    if supplied == 0 {
        return Err(Box::new(auth_error_response(
            AuthError::MissingToken,
            GATEWAY_NAME,
        )));
    }
    if supplied > 1 {
        return Err(unauthenticated(
            "ADMIN_STAFF_CONTEXT_INVALID",
            "duplicate staff context",
        ));
    }
    let metadata = tonic::metadata::MetadataMap::from_headers(headers.clone());
    let ctx = verify_signed_context_metadata(&metadata, verifier).map_err(|_| {
        unauthenticated(
            "ADMIN_STAFF_CONTEXT_INVALID",
            "staff context is missing, malformed, expired, or signed by a key this gateway does not accept for staff",
        )
    })?;
    let claims = ctx
        .get_data::<VerifiedContext>()
        .and_then(VerifiedContext::claims)
        .filter(|claims| claims.actor_type == SignedActorType::Staff)
        .cloned()
        .ok_or_else(|| {
            unauthenticated(
                "ADMIN_STAFF_CONTEXT_INVALID",
                "the verified context is not a staff context",
            )
        })?;
    VerifiedStaff::from_verified(&claims, ctx).map_err(|_| {
        forbidden(
            "ADMIN_IDENTITY_REQUIRED",
            "staff context must name one concrete application",
        )
    })
}

/// 进审计日志的请求头（原因、请求号、幂等键）与各自原始头值的长度上限（字节）。原因另有
/// 解码后的上限，见 [`audit_reason`]。
const AUDIT_HEADER_LIMITS: [(&str, usize); 3] = [
    (AUDIT_REASON_HEADER, MAX_AUDIT_REASON_HEADER_BYTES),
    (keys::REQUEST_ID, MAX_CORRELATION_ID_BYTES),
    (IDEMPOTENCY_KEY_HEADER, MAX_CORRELATION_ID_BYTES),
];

/// 第一个超长的审计头及其上限。读写请求都查：敏感读也把这些头记进审计日志。超长不截断，
/// 回 400：截断后的原因或关联号在审计里对不上调用方的记录。
fn audit_header_too_long(headers: &HeaderMap) -> Option<(&'static str, usize)> {
    AUDIT_HEADER_LIMITS.into_iter().find(|(name, max)| {
        headers
            .get_all(*name)
            .iter()
            .any(|value| value.len() > *max)
    })
}

fn bad_request(reason: &'static str, message: String) -> Response {
    HttpApiError(
        ErrorBuilder::new(ErrorCode::HttpBadRequest, reason)
            .details(message)
            .build_error(),
    )
    .into_response()
}

fn unauthenticated(reason: &'static str, message: &str) -> Rejection {
    Box::new(
        HttpApiError(
            ErrorBuilder::new(ErrorCode::HttpUnauthorized, reason)
                .details(message)
                .build_error(),
        )
        .into_response(),
    )
}

fn forbidden(reason: &'static str, message: &str) -> Rejection {
    tracing::debug!(reason, message, "admin request refused");
    Box::new(auth_error_response(
        AuthError::Forbidden(reason.to_owned()),
        GATEWAY_NAME,
    ))
}

fn not_found() -> Rejection {
    Box::new(HttpApiError::not_found("no such admin route").into_response())
}

fn control_status_response(status: Status) -> Rejection {
    let error = if status.code() == tonic::Code::Unauthenticated {
        AuthError::InvalidToken(status.message().to_owned())
    } else {
        AuthError::Forbidden(status.message().to_owned())
    };
    Box::new(auth_error_response(error, GATEWAY_NAME))
}

/// The route table decides what the request is; the signed grant decides whether this
/// employee may do it. Returns the resolved route for the handlers.
fn authorize_admin_request(
    method: &Method,
    path: &str,
    headers: &HeaderMap,
    staff: &VerifiedStaff,
) -> Result<&'static CoreAdminRoute, Rejection> {
    // A plugin admin route requires `plugin:<plugin_id>:<permission>` of its path, not the
    // template.
    let (route, permission) =
        core_admin::resolve_with_permission(method.as_str(), path).ok_or_else(not_found)?;
    if staff.permission.as_deref() != Some(permission.as_str()) {
        return Err(forbidden(
            "ADMIN_PERMISSION_DENIED",
            "the signed staff permission is not this route's code",
        ));
    }
    if route.platform_only && !staff.is_platform_staff() {
        return Err(forbidden(
            "ADMIN_SCOPE_DENIED",
            "this route is for platform staff only",
        ));
    }
    if !route.write {
        return Ok(route);
    }
    // A reason that is blank once decoded counts as missing; the middleware has already
    // refused a malformed one with 400.
    if !matches!(audit_reason(headers), Ok(Some(_))) {
        return Err(control_status_response(Status::permission_denied(
            "audit reason is required for admin write api",
        )));
    }
    if header_value(headers, IDEMPOTENCY_KEY_HEADER).is_none()
        && header_value(headers, keys::REQUEST_ID).is_none()
    {
        return Err(control_status_response(Status::permission_denied(
            "x-request-id or idempotency-key is required for admin write api",
        )));
    }
    Ok(route)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderValue, StatusCode};
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_server_core::auth::{KeySet, kid};
    use flare_server_core::{SignedContextIdentity, SignedContextSigner};

    /// A gateway key, a hook key, and a verifier holding both with Staff pinned to the gateway.
    fn keys() -> (
        SignedContextSigner,
        SignedContextSigner,
        SignedContextVerifier,
    ) {
        let pem = |seed: u8| {
            let key = SigningKey::from_bytes(&[seed; 32]);
            (
                key.to_pkcs8_pem(Default::default()).unwrap().to_string(),
                key.verifying_key()
                    .to_public_key_pem(Default::default())
                    .unwrap(),
            )
        };
        let (gateway_private, gateway_public) = pem(89);
        let (hook_private, hook_public) = pem(90);
        let mut set = KeySet::empty();
        set.insert(kid::GATEWAY, &gateway_public).unwrap();
        set.insert(kid::HOOK, &hook_public).unwrap();
        (
            SignedContextSigner::from_private_key(&gateway_private)
                .unwrap()
                .with_kid(kid::GATEWAY)
                .unwrap(),
            SignedContextSigner::from_private_key(&hook_private)
                .unwrap()
                .with_kid(kid::HOOK)
                .unwrap(),
            SignedContextVerifier::from_key_set(set).require_staff_kid([kid::GATEWAY]),
        )
    }

    fn identity(tenant: &str, permission: &str, org_id: Option<&str>) -> SignedContextIdentity {
        SignedContextIdentity::new(SignedActorType::Staff, "employee-1", tenant, "req-1")
            .with_staff_grant(org_id.map(str::to_owned), Some(permission.to_owned()), 1)
    }

    fn headers_with(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(SIGNED_CONTEXT_HEADER, token.parse().unwrap());
        headers
    }

    fn status_of(response: &Response) -> StatusCode {
        response.status()
    }

    #[test]
    fn audit_headers_are_bounded_in_bytes() {
        let mut headers = HeaderMap::new();
        assert_eq!(audit_header_too_long(&headers), None);
        let reason = "r".repeat(MAX_AUDIT_REASON_BYTES);
        let id = "i".repeat(MAX_CORRELATION_ID_BYTES);
        headers.insert(AUDIT_REASON_HEADER, reason.parse().unwrap());
        headers.insert(keys::REQUEST_ID, id.parse().unwrap());
        headers.insert(IDEMPOTENCY_KEY_HEADER, id.parse().unwrap());
        assert_eq!(audit_header_too_long(&headers), None);
        for (name, max) in AUDIT_HEADER_LIMITS {
            let mut longer = headers.clone();
            longer.insert(name, "x".repeat(max + 1).parse().unwrap());
            assert_eq!(audit_header_too_long(&longer), Some((name, max)), "{name}");
            // A second, over-long copy of the header is refused too.
            let mut appended = headers.clone();
            appended.append(name, "x".repeat(max + 1).parse().unwrap());
            assert_eq!(
                audit_header_too_long(&appended),
                Some((name, max)),
                "{name}"
            );
        }
    }

    #[test]
    fn the_boundary_admits_only_a_gateway_signed_staff_context() {
        let (gateway, hook, verifier) = keys();
        let token = gateway
            .sign(&identity("tenant-a", "core:users:read", None))
            .unwrap();
        let staff = verified_staff(&verifier, &headers_with(&token)).unwrap();
        assert_eq!(staff.actor_id, "employee-1");
        assert_eq!(staff.tenant_id, "tenant-a");
        assert_eq!(staff.token(), token);
        assert!(!format!("{:?}", VerifiedStaffContext::new(staff)).contains(&token));

        // No context, two contexts, a hook-signed staff context, a service context, and a
        // staff context without an application.
        assert_eq!(
            status_of(&verified_staff(&verifier, &HeaderMap::new()).unwrap_err()),
            StatusCode::UNAUTHORIZED
        );
        let mut duplicated = headers_with(&token);
        duplicated.append(SIGNED_CONTEXT_HEADER, token.parse().unwrap());
        assert_eq!(
            status_of(&verified_staff(&verifier, &duplicated).unwrap_err()),
            StatusCode::UNAUTHORIZED
        );
        let forged = hook
            .sign(&identity("tenant-a", "core:users:read", None))
            .unwrap();
        assert_eq!(
            status_of(&verified_staff(&verifier, &headers_with(&forged)).unwrap_err()),
            StatusCode::UNAUTHORIZED
        );
        let service = gateway
            .sign(&SignedContextIdentity::new(
                SignedActorType::Service,
                "flare-social-bridge",
                "tenant-a",
                "req-2",
            ))
            .unwrap();
        assert_eq!(
            status_of(&verified_staff(&verifier, &headers_with(&service)).unwrap_err()),
            StatusCode::UNAUTHORIZED
        );
        let platform_scoped = gateway
            .sign(&identity("", "core:users:read", None))
            .unwrap();
        assert_eq!(
            status_of(&verified_staff(&verifier, &headers_with(&platform_scoped)).unwrap_err()),
            StatusCode::FORBIDDEN
        );
    }

    fn staff_with(permission: &str, org_id: Option<&str>) -> VerifiedStaff {
        let (gateway, _, verifier) = keys();
        let token = gateway
            .sign(&identity("tenant-a", permission, org_id))
            .unwrap();
        verified_staff(&verifier, &headers_with(&token)).unwrap()
    }

    #[test]
    fn the_route_table_and_the_signed_permission_decide() {
        let headers = HeaderMap::new();
        let staff = staff_with("core:messages:read", Some("org-1"));
        let route =
            authorize_admin_request(&Method::GET, "/messages/m1", &headers, &staff).unwrap();
        assert_eq!(route.permission, "core:messages:read");
        assert!(!route.write);
        // The query POSTs are reads by contract: no audit headers needed.
        assert!(
            authorize_admin_request(&Method::POST, "/messages/query", &headers, &staff).is_ok()
        );
        // Another route's code.
        assert_eq!(
            status_of(
                &authorize_admin_request(&Method::GET, "/conversations", &headers, &staff)
                    .unwrap_err()
            ),
            StatusCode::FORBIDDEN
        );
        // Not an admin route, an unsafe path, an invalid parameter.
        for (method, path) in [
            (Method::GET, "/messages"),
            (Method::GET, "/messages/%FF"),
            (Method::GET, "/messages/m 1"),
            (Method::DELETE, "/messages/m1"),
        ] {
            assert_eq!(
                status_of(&authorize_admin_request(&method, path, &headers, &staff).unwrap_err()),
                StatusCode::NOT_FOUND,
                "{method} {path}"
            );
        }
    }

    #[test]
    fn write_routes_require_audit_context_and_platform_routes_platform_staff() {
        let staff = staff_with("core:users:kick", Some("org-1"));
        let mut headers = HeaderMap::new();
        assert_eq!(
            status_of(
                &authorize_admin_request(&Method::POST, "/users/u1/kick", &headers, &staff)
                    .unwrap_err()
            ),
            StatusCode::FORBIDDEN
        );
        headers.insert(keys::REQUEST_ID, HeaderValue::from_static("request-a"));
        // A reason that is blank once percent-decoded is no reason.
        headers.insert(AUDIT_REASON_HEADER, HeaderValue::from_static("%20%09"));
        assert_eq!(
            status_of(
                &authorize_admin_request(&Method::POST, "/users/u1/kick", &headers, &staff)
                    .unwrap_err()
            ),
            StatusCode::FORBIDDEN
        );
        headers.remove(keys::REQUEST_ID);
        headers.insert(AUDIT_REASON_HEADER, HeaderValue::from_static("ops-audit"));
        assert_eq!(
            status_of(
                &authorize_admin_request(&Method::POST, "/users/u1/kick", &headers, &staff)
                    .unwrap_err()
            ),
            StatusCode::FORBIDDEN
        );
        headers.insert(keys::REQUEST_ID, HeaderValue::from_static("request-a"));
        let route =
            authorize_admin_request(&Method::POST, "/users/u1/kick", &headers, &staff).unwrap();
        assert!(route.write);
        // A tenant or actor header is neither required nor consulted.
        headers.insert(keys::TENANT_ID, HeaderValue::from_static("tenant-z"));
        headers.insert(keys::ACTOR_ID, HeaderValue::from_static("attacker"));
        assert!(authorize_admin_request(&Method::POST, "/users/u1/kick", &headers, &staff).is_ok());

        // Plugin registration is platform-only: organization staff are refused even with the code.
        let mut write_headers = HeaderMap::new();
        write_headers.insert(AUDIT_REASON_HEADER, HeaderValue::from_static("ops-audit"));
        write_headers.insert(IDEMPOTENCY_KEY_HEADER, HeaderValue::from_static("idem-1"));
        let org_staff = staff_with("core:plugins:write", Some("org-1"));
        assert_eq!(
            status_of(
                &authorize_admin_request(
                    &Method::POST,
                    "/plugins/register",
                    &write_headers,
                    &org_staff
                )
                .unwrap_err()
            ),
            StatusCode::FORBIDDEN
        );
        let platform_staff = staff_with("core:plugins:write", None);
        assert!(
            authorize_admin_request(
                &Method::POST,
                "/plugins/register",
                &write_headers,
                &platform_staff
            )
            .is_ok()
        );
    }
}
