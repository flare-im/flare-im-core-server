//! Staff identity on the Core admin surface (v4 R3-2).
//!
//! Employees reach Core only through the Social gateway admin partition, which signs a Staff
//! `x-flare-ctx` with the Cell key registered as `kid=gateway`: `tenant_id` is the application
//! being administered and `permission` is the route code from
//! [`flare_server_core::auth::core_admin::ROUTES`]. Core admin-gateway and the downstream admin
//! RPCs (Conversation, Online, Capability, Storage Reader) verify that one token with the Cell
//! [`KeySet`](flare_server_core::auth::KeySet) and compare the signed permission with the
//! operation they serve. No platform-issued employee assertion exists any more; the platform key
//! verifies only the projection worker and the scheduler (see [`crate::control_auth`]).

use flare_im_contracts::domain::tenant::is_valid_tenant_id;
use flare_im_contracts::utils::normalize_tenant_id;
use flare_server_core::SignedContextVerifier;
use flare_server_core::auth::{
    INTERNAL_CTX_PUBLIC_KEY_ENV, INTERNAL_CTX_PUBLIC_KEYS_ENV, KeySet, SignedContextClaims, kid,
};
use flare_server_core::context::{Ctx, keys};
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata;
use flare_server_core::{require_staff_permission, require_staff_permission_any, staff_claims};
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};

/// The verifier every Core admin surface uses: the Cell key set from
/// [`INTERNAL_CTX_PUBLIC_KEYS_ENV`] layered over [`INTERNAL_CTX_PUBLIC_KEY_ENV`], with `Staff`
/// pinned to the gateway key id. See [`admin_staff_verifier_from_sources`].
pub fn admin_staff_verifier_from_env(service: &str) -> Result<SignedContextVerifier> {
    let keys_spec = env_source(service, INTERNAL_CTX_PUBLIC_KEYS_ENV)?;
    let single_key = env_source(service, INTERNAL_CTX_PUBLIC_KEY_ENV)?;
    admin_staff_verifier_from_sources(service, keys_spec.as_deref(), single_key.as_deref())
}

/// [`admin_staff_verifier_from_env`] on explicit values: `keys_spec` is the key-set spec (a
/// JSON object of key id to public key, or a directory of `<kid>.pem` files) and `single_key`
/// the legacy single Cell key. `Staff` verifies only under `kid=gateway`; on a single-key
/// deployment that kid resolves to the only key, and a kid-less Staff context is refused.
/// Neither source set is a configuration error naming both variables, so the operator sees
/// what to provide.
pub fn admin_staff_verifier_from_sources(
    service: &str,
    keys_spec: Option<&str>,
    single_key: Option<&str>,
) -> Result<SignedContextVerifier> {
    let keys = KeySet::from_sources(keys_spec, single_key)
        .map_err(|error| verifier_configuration_error(service, error))?
        .ok_or_else(|| verifier_configuration_error(service, "neither is set"))?;
    Ok(SignedContextVerifier::from_key_set(keys).require_staff_kid([kid::GATEWAY]))
}

fn verifier_configuration_error(
    service: &str,
    error: impl std::fmt::Display,
) -> flare_server_core::error::FlareError {
    ErrorBuilder::new(
        ErrorCode::ConfigurationError,
        format!(
            "{service} needs {INTERNAL_CTX_PUBLIC_KEYS_ENV} or {INTERNAL_CTX_PUBLIC_KEY_ENV} to verify the staff context: {error}"
        ),
    )
    .build_error()
}

/// One key source from the environment; unset is `None`, a value that is not UTF-8 is an error.
fn env_source(service: &str, name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(verifier_configuration_error(
            service,
            format!("{name} has invalid encoding"),
        )),
    }
}

/// A verified Staff caller of one admin operation. The fields mirror the signed claims; `ctx`
/// is the context the verifier built, carrying the `VerifiedContext` marker (and so the token)
/// for forwarding and for audit effects.
#[derive(Clone)]
pub struct VerifiedStaff {
    pub actor_id: String,
    /// The application (IM tenant) being administered: never empty, never `*`.
    pub tenant_id: String,
    /// `None` for platform staff.
    pub org_id: Option<String>,
    /// The route code the admin partition matched; equal to the code the operation required.
    pub permission: Option<String>,
    pub request_id: String,
    pub trace_id: Option<String>,
    /// The reason the staff member stated, as the admin partition signed it (`x-audit-reason`).
    pub reason: Option<String>,
    pub ctx: Ctx,
}

impl std::fmt::Debug for VerifiedStaff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedStaff")
            .field("actor_id", &self.actor_id)
            .field("tenant_id", &self.tenant_id)
            .field("org_id", &self.org_id)
            .field("permission", &self.permission)
            .field("request_id", &self.request_id)
            .field("trace_id", &self.trace_id)
            .field("reason", &self.reason)
            .finish()
    }
}

impl VerifiedStaff {
    /// The verified token, for forwarding to the next admin service unchanged.
    pub fn token(&self) -> &str {
        self.ctx
            .get_data::<flare_server_core::VerifiedContext>()
            .and_then(flare_server_core::VerifiedContext::token)
            .unwrap_or_default()
    }

    /// Whether the caller is platform staff (no organization).
    pub fn is_platform_staff(&self) -> bool {
        self.org_id.is_none()
    }

    /// A tenant-scoped request must name exactly the signed application.
    pub fn require_tenant(&self, tenant_id: &str) -> std::result::Result<(), Status> {
        if tenant_id != self.tenant_id {
            return Err(Status::permission_denied("staff tenant mismatch"));
        }
        Ok(())
    }

    /// Builds the staff view of claims a verifier (or the signed context layer) has already
    /// verified as `Staff`; `ctx` is the context that verification produced. Refuses a
    /// platform-scoped (empty) or wildcard application.
    pub fn from_verified(
        claims: &SignedContextClaims,
        ctx: Ctx,
    ) -> std::result::Result<Self, Status> {
        if claims.tenant_id.is_empty()
            || claims.tenant_id == "*"
            || !is_valid_tenant_id(&claims.tenant_id)
            || normalize_tenant_id(&claims.tenant_id) != claims.tenant_id
        {
            return Err(Status::permission_denied(
                "staff context must name one concrete application",
            ));
        }
        Ok(Self {
            actor_id: claims.actor_id.clone(),
            tenant_id: claims.tenant_id.clone(),
            org_id: claims.org_id.clone(),
            permission: claims.permission.clone(),
            request_id: claims.request_id.clone(),
            trace_id: claims.trace_id.clone(),
            reason: claims.reason.clone(),
            ctx,
        })
    }
}

/// Verifies the `x-flare-ctx` entry of `metadata` and requires a `Staff` actor whose signed
/// permission is `permission`. Missing, duplicate or invalid tokens (including a Staff token
/// signed by a key id the verifier does not accept) are `UNAUTHENTICATED`; a verified non-staff
/// actor, another permission, or a token without a concrete application is `PERMISSION_DENIED`.
pub fn verify_staff_metadata(
    metadata: &MetadataMap,
    verifier: &SignedContextVerifier,
    permission: &str,
) -> std::result::Result<VerifiedStaff, Status> {
    let ctx = verify_staff_context(metadata, verifier)?;
    let claims = require_staff_permission(&ctx, permission)?.clone();
    VerifiedStaff::from_verified(&claims, ctx)
}

/// [`verify_staff_metadata`] admitting any of `codes`.
pub fn verify_staff_metadata_any(
    metadata: &MetadataMap,
    verifier: &SignedContextVerifier,
    codes: &[&str],
) -> std::result::Result<VerifiedStaff, Status> {
    let ctx = verify_staff_context(metadata, verifier)?;
    let claims = require_staff_permission_any(&ctx, codes)?.clone();
    VerifiedStaff::from_verified(&claims, ctx)
}

/// A missing context gets its own message, so a receiver behind a signed-context layer can tell
/// its admin gate's refusal from the layer's.
fn verify_staff_context(
    metadata: &MetadataMap,
    verifier: &SignedContextVerifier,
) -> std::result::Result<Ctx, Status> {
    if metadata
        .get_all(flare_server_core::grpc::middleware::signed_context::SIGNED_CONTEXT_HEADER)
        .iter()
        .next()
        .is_none()
    {
        return Err(Status::unauthenticated("staff context required"));
    }
    verify_signed_context_metadata(metadata, verifier)
}

/// The RPC gate of a Staff-only admin operation: see [`verify_staff_metadata`].
pub fn require_staff<T>(
    request: &Request<T>,
    verifier: &SignedContextVerifier,
    permission: &str,
) -> std::result::Result<VerifiedStaff, Status> {
    verify_staff_metadata(request.metadata(), verifier, permission)
}

/// [`require_staff`] admitting any of `codes`.
pub fn require_staff_any<T>(
    request: &Request<T>,
    verifier: &SignedContextVerifier,
    codes: &[&str],
) -> std::result::Result<VerifiedStaff, Status> {
    verify_staff_metadata_any(request.metadata(), verifier, codes)
}

/// The verified Staff claims of a context built by [`verify_staff_metadata`] or by the signed
/// context layer, if the actor is Staff.
pub fn staff_claims_of(ctx: &Ctx) -> Option<&SignedContextClaims> {
    staff_claims(ctx)
}

/// A downstream admin call: the verified Staff token forwarded unchanged as `x-flare-ctx`, and
/// nothing else. The gateway signs nothing on this path, so it can only replay what the admin
/// partition signed, within the token's lifetime.
pub fn staff_context_request<T>(payload: T, token: &str) -> Result<Request<T>> {
    let rejected = || {
        ErrorBuilder::new(
            ErrorCode::AuthenticationRequired,
            "staff context required for downstream admin call",
        )
        .build_error()
    };
    // A JWT is printable ASCII; the obs-text bytes HTTP allows are refused too.
    if token.is_empty() || !token.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(rejected());
    }
    let mut request = Request::new(payload);
    flare_server_core::set_signed_context_token(&mut request, token).map_err(|_| rejected())?;
    Ok(request)
}

/// [`staff_context_request`] plus the request and trace ids of `ctx` as `x-request-id` and
/// `x-trace-id`. They are correlation only: the receiver takes tenant and actor from the token.
pub fn staff_traced_request<T>(payload: T, token: &str, ctx: &Ctx) -> Result<Request<T>> {
    let mut request = staff_context_request(payload, token)?;
    for (key, value) in [
        (keys::REQUEST_ID, ctx.request_id()),
        (keys::TRACE_ID, ctx.trace_id()),
    ] {
        if value.is_empty() {
            continue;
        }
        if let Ok(value) = tonic::metadata::MetadataValue::try_from(value) {
            request.metadata_mut().insert(key, value);
        }
    }
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_server_core::auth::{KeySet, LEGACY_KID};
    use flare_server_core::{SignedActorType, SignedContextIdentity, SignedContextSigner};
    use tonic::Code;

    fn pem_pair(seed: u8) -> (String, String) {
        let key = SigningKey::from_bytes(&[seed; 32]);
        (
            key.to_pkcs8_pem(Default::default()).unwrap().to_string(),
            key.verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap(),
        )
    }

    /// A gateway key and a hook key registered under their kids, plus a verifier pinning Staff
    /// to the gateway.
    fn keyed() -> (
        SignedContextSigner,
        SignedContextSigner,
        SignedContextVerifier,
    ) {
        let (gateway_private, gateway_public) = pem_pair(21);
        let (hook_private, hook_public) = pem_pair(22);
        let mut keys = KeySet::empty();
        keys.insert(kid::GATEWAY, &gateway_public).unwrap();
        keys.insert(kid::HOOK, &hook_public).unwrap();
        (
            SignedContextSigner::from_private_key(&gateway_private)
                .unwrap()
                .with_kid(kid::GATEWAY)
                .unwrap(),
            SignedContextSigner::from_private_key(&hook_private)
                .unwrap()
                .with_kid(kid::HOOK)
                .unwrap(),
            SignedContextVerifier::from_key_set(keys).require_staff_kid([kid::GATEWAY]),
        )
    }

    fn staff(tenant: &str, permission: &str) -> SignedContextIdentity {
        SignedContextIdentity::new(SignedActorType::Staff, "employee-1", tenant, "req-1")
            .with_staff_grant(None, Some(permission.to_owned()), 3)
            .with_trace_id("trace-1")
    }

    fn with_token<T>(payload: T, token: &str) -> Request<T> {
        let mut request = Request::new(payload);
        request
            .metadata_mut()
            .insert("x-flare-ctx", token.parse().unwrap());
        request
    }

    fn code<T>(result: std::result::Result<T, Status>) -> Code {
        result.map(|_| ()).unwrap_err().code()
    }

    #[test]
    fn a_gateway_signed_staff_context_with_the_route_code_passes() {
        let (gateway, _, verifier) = keyed();
        let token = gateway
            .sign(&staff("tenant-a", "core:conversations:read"))
            .unwrap();
        let staff = require_staff(
            &with_token((), &token),
            &verifier,
            "core:conversations:read",
        )
        .unwrap();
        assert_eq!(staff.actor_id, "employee-1");
        assert_eq!(staff.tenant_id, "tenant-a");
        assert_eq!(staff.org_id, None);
        assert_eq!(staff.permission.as_deref(), Some("core:conversations:read"));
        assert_eq!(staff.request_id, "req-1");
        assert_eq!(staff.trace_id.as_deref(), Some("trace-1"));
        assert_eq!(staff.token(), token);
        assert!(staff.is_platform_staff());
        assert_eq!(staff.ctx.tenant_id(), Some("tenant-a"));
        assert_eq!(staff.ctx.user_id(), Some("employee-1"));
        assert_eq!(staff.ctx.trace_id(), "trace-1");
        assert!(staff.require_tenant("tenant-a").is_ok());
        assert_eq!(
            code(staff.require_tenant("tenant-b")),
            Code::PermissionDenied
        );
        assert!(!format!("{staff:?}").contains(&token));

        let any = require_staff_any(
            &with_token((), &token),
            &verifier,
            &["core:messages:read", "core:conversations:read"],
        )
        .unwrap();
        assert_eq!(any.permission.as_deref(), Some("core:conversations:read"));
    }

    #[test]
    fn missing_duplicate_foreign_kid_and_non_staff_tokens_are_refused() {
        let (gateway, hook, verifier) = keyed();
        assert_eq!(
            code(require_staff(
                &Request::new(()),
                &verifier,
                "core:users:read"
            )),
            Code::Unauthenticated
        );
        let token = gateway.sign(&staff("tenant-a", "core:users:read")).unwrap();
        let mut duplicated = with_token((), &token);
        duplicated
            .metadata_mut()
            .append("x-flare-ctx", token.parse().unwrap());
        assert_eq!(
            code(require_staff(&duplicated, &verifier, "core:users:read")),
            Code::Unauthenticated
        );
        // A Staff context signed by the hook key: the verifier pins Staff to the gateway.
        let forged = hook.sign(&staff("tenant-a", "core:users:read")).unwrap();
        assert_eq!(
            code(require_staff(
                &with_token((), &forged),
                &verifier,
                "core:users:read"
            )),
            Code::Unauthenticated
        );
        // A verified non-staff actor is not an employee.
        let service = gateway
            .sign(&SignedContextIdentity::new(
                SignedActorType::Service,
                "flare-social-bridge",
                "tenant-a",
                "req-2",
            ))
            .unwrap();
        assert_eq!(
            code(require_staff(
                &with_token((), &service),
                &verifier,
                "core:users:read"
            )),
            Code::PermissionDenied
        );
        // Another route's code, or no code at all.
        assert_eq!(
            code(require_staff(
                &with_token((), &token),
                &verifier,
                "core:users:kick"
            )),
            Code::PermissionDenied
        );
        let codeless = gateway
            .sign(
                &SignedContextIdentity::new(SignedActorType::Staff, "employee-1", "tenant-a", "r")
                    .with_staff_grant(Some("org-1".into()), None, 3),
            )
            .unwrap();
        assert_eq!(
            code(require_staff(
                &with_token((), &codeless),
                &verifier,
                "core:users:read"
            )),
            Code::PermissionDenied
        );
        assert_eq!(
            code(require_staff_any(
                &with_token((), &codeless),
                &verifier,
                &["core:users:read", "core:users:kick"]
            )),
            Code::PermissionDenied
        );
        assert_eq!(
            code(require_staff_any(
                &with_token((), &token),
                &verifier,
                &["core:users:kick", "core:tenant:kick"]
            )),
            Code::PermissionDenied
        );
        // Platform-scoped (no application) and wildcard tenants do not reach Core.
        for tenant in ["", "*"] {
            let scoped = gateway.sign(&staff(tenant, "core:users:read")).unwrap();
            assert_eq!(
                code(require_staff(
                    &with_token((), &scoped),
                    &verifier,
                    "core:users:read"
                )),
                Code::PermissionDenied,
                "{tenant:?}"
            );
        }
    }

    #[test]
    fn single_key_deployment_verifies_the_gateway_kid_with_its_only_key() {
        // Until the per-component keys exist a deployment holds one Cell key as `legacy`:
        // a token naming any kid resolves to that key. The pin still reads the header kid, so
        // a Staff token without one (`legacy`) is refused even here: the admin partition must
        // sign with `kid=gateway` on every deployment.
        let (private, public) = pem_pair(23);
        let signer = SignedContextSigner::from_private_key(&private).unwrap();
        let verifier = SignedContextVerifier::from_public_key(&public)
            .unwrap()
            .require_staff_kid([kid::GATEWAY]);
        assert!(verifier.key_set().is_single_key());
        let kid_less = signer
            .sign(&SignedContextIdentity::new(
                SignedActorType::User,
                "u1",
                "tenant-a",
                "req-1",
            ))
            .unwrap();
        assert_eq!(verifier.verify(&kid_less).unwrap().kid, LEGACY_KID);
        let token = signer.sign(&staff("tenant-a", "core:users:read")).unwrap();
        assert!(verifier.verify(&token).is_err());
        assert_eq!(
            code(require_staff(
                &with_token((), &token),
                &verifier,
                "core:users:read"
            )),
            Code::Unauthenticated
        );
        let gateway = SignedContextSigner::from_private_key(&private)
            .unwrap()
            .with_kid(kid::GATEWAY)
            .unwrap();
        let token = gateway.sign(&staff("tenant-a", "core:users:read")).unwrap();
        let staff = require_staff(&with_token((), &token), &verifier, "core:users:read").unwrap();
        assert_eq!(verifier.verify(&token).unwrap().kid, kid::GATEWAY);
        assert_eq!(staff.tenant_id, "tenant-a");
    }

    #[test]
    fn forwarded_requests_carry_only_the_token_and_correlation_ids() {
        let request = staff_context_request((), "a.b.c").unwrap();
        assert_eq!(request.metadata().get("x-flare-ctx").unwrap(), "a.b.c");
        for key in ["authorization", "x-tenant-id", "x-user-id", "x-actor-id"] {
            assert!(request.metadata().get(key).is_none(), "{key}");
        }
        for token in ["", "a.b c", "a.b.c\n", "a.b.\u{7f}c", "token"] {
            if token == "token" {
                assert!(staff_context_request((), token).is_ok());
            } else {
                assert!(staff_context_request((), token).is_err(), "{token:?}");
            }
        }
        let ctx = std::sync::Arc::new(
            flare_server_core::context::Context::with_request_id("req-1")
                .with_trace_id("trace-1")
                .with_tenant_id("tenant-a")
                .with_user_id("employee-1"),
        );
        let traced = staff_traced_request((), "a.b.c", &ctx).unwrap();
        assert_eq!(traced.metadata().get("x-request-id").unwrap(), "req-1");
        assert_eq!(traced.metadata().get("x-trace-id").unwrap(), "trace-1");
        for key in ["authorization", "x-tenant-id", "x-user-id"] {
            assert!(traced.metadata().get(key).is_none(), "{key}");
        }
    }

    fn signer(private: &str, key_id: Option<&str>) -> SignedContextSigner {
        let signer = SignedContextSigner::from_private_key(private).unwrap();
        match key_id {
            Some(key_id) => signer.with_kid(key_id).unwrap(),
            None => signer,
        }
    }

    /// The production constructor on a key-set spec holding the gateway, hook and admin-gateway
    /// keys plus a legacy key: Staff verifies only under `kid=gateway`.
    #[test]
    fn the_production_verifier_pins_staff_to_the_gateway_kid() {
        let (gateway_private, gateway_public) = pem_pair(31);
        let (hook_private, hook_public) = pem_pair(32);
        let (admin_private, admin_public) = pem_pair(33);
        let (legacy_private, legacy_public) = pem_pair(34);
        let spec = serde_json::to_string(&std::collections::BTreeMap::from([
            (kid::GATEWAY, gateway_public.as_str()),
            (kid::HOOK, hook_public.as_str()),
            (kid::ADMIN_GATEWAY, admin_public.as_str()),
        ]))
        .unwrap();
        let verifier = admin_staff_verifier_from_sources(
            "test",
            Some(spec.as_str()),
            Some(legacy_public.as_str()),
        )
        .unwrap();
        assert!(!verifier.key_set().is_single_key());
        let read = |token: &str| {
            code(require_staff(
                &with_token((), token),
                &verifier,
                "core:users:read",
            ))
        };

        let token = signer(&gateway_private, Some(kid::GATEWAY))
            .sign(&staff("tenant-a", "core:users:read"))
            .unwrap();
        let verified =
            require_staff(&with_token((), &token), &verifier, "core:users:read").unwrap();
        assert_eq!(verified.tenant_id, "tenant-a");
        // Valid signatures under the hook and admin-gateway kids (the latter is the context
        // admin-gateway signs for Media), and a kid-less one under the legacy key.
        for (private, key_id) in [
            (&hook_private, Some(kid::HOOK)),
            (&admin_private, Some(kid::ADMIN_GATEWAY)),
            (&legacy_private, None),
        ] {
            let forged = signer(private, key_id)
                .sign(&staff("tenant-a", "core:users:read"))
                .unwrap();
            assert!(verifier.verify(&forged).is_err(), "{key_id:?}");
            assert_eq!(read(&forged), Code::Unauthenticated, "{key_id:?}");
        }
        // The pin is on Staff only: a non-staff context under the hook kid still verifies.
        let service = signer(&hook_private, Some(kid::HOOK))
            .sign(&SignedContextIdentity::new(
                SignedActorType::Service,
                "flare-hook-engine",
                "tenant-a",
                "req-3",
            ))
            .unwrap();
        assert_eq!(verifier.verify(&service).unwrap().kid, kid::HOOK);
    }

    #[test]
    fn the_production_verifier_on_a_single_key_deployment() {
        let (private, public) = pem_pair(35);
        // Blank key-set spec, as an unset variable in a compose file reads.
        let verifier =
            admin_staff_verifier_from_sources("test", Some("  "), Some(public.as_str())).unwrap();
        assert!(verifier.key_set().is_single_key());
        let token = signer(&private, Some(kid::GATEWAY))
            .sign(&staff("tenant-a", "core:users:read"))
            .unwrap();
        assert!(require_staff(&with_token((), &token), &verifier, "core:users:read").is_ok());
        let kid_less = signer(&private, None)
            .sign(&staff("tenant-a", "core:users:read"))
            .unwrap();
        assert_eq!(
            code(require_staff(
                &with_token((), &kid_less),
                &verifier,
                "core:users:read"
            )),
            Code::Unauthenticated
        );
    }

    #[test]
    fn the_production_verifier_needs_a_cell_key() {
        let message = |result: Result<SignedContextVerifier>| match result {
            Ok(_) => panic!("a verifier was built without a usable Cell key"),
            Err(error) => error.to_string(),
        };
        for (keys_spec, single_key) in [(None, None), (Some(""), Some(" "))] {
            let error = message(admin_staff_verifier_from_sources(
                "flare-test",
                keys_spec,
                single_key,
            ));
            assert!(error.contains("flare-test"), "{error}");
            assert!(error.contains(INTERNAL_CTX_PUBLIC_KEYS_ENV), "{error}");
            assert!(error.contains(INTERNAL_CTX_PUBLIC_KEY_ENV), "{error}");
        }
        // A malformed spec is refused, not read as no key.
        let error = message(admin_staff_verifier_from_sources(
            "flare-test",
            Some("{not json"),
            None,
        ));
        assert!(error.contains(INTERNAL_CTX_PUBLIC_KEYS_ENV), "{error}");
    }
}
