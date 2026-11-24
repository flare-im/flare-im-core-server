//! Platform → Core service-assertion boundary.
//!
//! This verifier only holds the platform public key (`FLARE_PLATFORM_PUBLIC_KEY`). It cannot
//! mint credentials, and neither a user access JWT nor a cell-internal `x-flare-ctx` is
//! accepted here.
//!
//! Exactly one kind of assertion reaches Core gRPC services on this boundary: the control
//! assertion the platform sends directly (`token_use=control`), and only for the two platform
//! service subjects: the projection worker ([`PROJECTION_WORKER_SUBJECT`], Capability
//! `TenantProjection`) and the scheduler ([`SCHEDULER_SUBJECT`], Online `KickTenant`). Its
//! audience names what it may do: [`CORE_PROJECTION_AUDIENCE`] writes projections and kicks
//! tenants; [`CORE_BUILD_STATS_AUDIENCE`] only reads the per-build connection counts
//! (`ClientBuildStats`). Neither audience is accepted where the other is expected, so a read
//! credential cannot write a projection and a projection credential cannot read the stats. Employees never reach Core with a platform
//! assertion: they come as a Staff `x-flare-ctx` signed by the gateway admin partition (see
//! [`crate::staff_context`]), so an employee subject on this boundary is `UNAUTHENTICATED`.

use std::sync::Arc;

use flare_im_contracts::domain::tenant::is_valid_tenant_id;
use flare_im_contracts::utils::normalize_tenant_id;
use flare_server_core::TokenService;
use tonic::{Request, Status};

// Wire values shared with the platform signer live in flare-core-base (single source of
// truth for both sides of the control boundary); re-exported under these historical
// names so callers do not change.
pub use flare_core_base::platform_auth::{
    CORE_BUILD_STATS_AUDIENCE, CORE_PROJECTION_AUDIENCE, PROJECTION_WORKER_SUBJECT,
    SCHEDULER_SUBJECT,
};
pub use flare_core_base::platform_auth::FLARE_PLATFORM_ISSUER as PLATFORM_ISSUER;
/// The platform's Ed25519 public key (PEM or base64); only Capability and Online read it.
pub const PLATFORM_PUBLIC_KEY_ENV: &str = "FLARE_PLATFORM_PUBLIC_KEY";
const TOKEN_USE_CONTROL: &str = "control";
const MAX_TOKEN_BYTES: usize = 8192;
const MAX_TOKEN_LIFETIME_SECONDS: usize =
    flare_core_base::platform_auth::PLATFORM_CONTROL_TOKEN_TTL_SECONDS as usize;

#[derive(Clone)]
pub struct PlatformAssertionVerifier(Arc<TokenService>);

/// The platform service that signed the assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlatformActorKind {
    ProjectionWorker,
    Scheduler,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedPlatformActor {
    pub subject: String,
    pub tenant_id: String,
    pub kind: PlatformActorKind,
}

impl VerifiedPlatformActor {
    /// A tenant-scoped operation must name exactly the tenant in the token.
    pub fn require_tenant(&self, tenant_id: &str) -> Result<(), Status> {
        if !is_valid_tenant_id(tenant_id)
            || tenant_id != normalize_tenant_id(tenant_id)
            || self.tenant_id != tenant_id
        {
            return Err(Status::permission_denied("control tenant mismatch"));
        }
        Ok(())
    }

    pub fn require_kind(&self, kind: PlatformActorKind) -> Result<(), Status> {
        if self.kind != kind {
            return Err(Status::permission_denied("control actor not permitted"));
        }
        Ok(())
    }
}

impl PlatformAssertionVerifier {
    pub fn from_public_key(material: &str) -> flare_server_core::error::Result<Self> {
        Ok(Self(Arc::new(TokenService::from_ed25519_public_key(
            material,
            PLATFORM_ISSUER,
        )?)))
    }

    /// Parsing an absent key does not grant a legacy fallback: protected RPCs
    /// will return UNAUTHENTICATED until the platform public key is configured.
    pub fn from_env() -> flare_server_core::error::Result<Option<Self>> {
        match std::env::var("FLARE_PLATFORM_PUBLIC_KEY") {
            Ok(material) => Ok(Some(Self::from_public_key(&material)?)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(flare_server_core::error::FlareError::system(
                    "FLARE_PLATFORM_PUBLIC_KEY has invalid encoding",
                ))
            }
        }
    }

    /// Verifies a control assertion: EdDSA with the platform key, issuer `flare-platform`,
    /// audience [`CORE_PROJECTION_AUDIENCE`], `token_use=control`, lifetime at most 60 s, and
    /// one of the two platform service subjects. Any other subject is `UNAUTHENTICATED`.
    pub fn verify_token(&self, token: &str) -> Result<VerifiedPlatformActor, Status> {
        self.verify_token_for(token, CORE_PROJECTION_AUDIENCE)
    }

    /// [`Self::verify_token`] for an explicit audience (exact match, nothing else accepted).
    pub fn verify_token_for(
        &self,
        token: &str,
        audience: &str,
    ) -> Result<VerifiedPlatformActor, Status> {
        if token.is_empty()
            || token.len() > MAX_TOKEN_BYTES
            || token.chars().any(char::is_whitespace)
        {
            return Err(Status::unauthenticated("invalid control token"));
        }
        let claims = self
            .0
            .validate_ed25519_with_use(token, TOKEN_USE_CONTROL, audience)
            .map_err(|_| Status::unauthenticated("invalid control token"))?;
        if claims.exp - claims.iat > MAX_TOKEN_LIFETIME_SECONDS {
            return Err(Status::unauthenticated("invalid control token"));
        }
        let kind = match claims.sub.as_str() {
            PROJECTION_WORKER_SUBJECT => PlatformActorKind::ProjectionWorker,
            SCHEDULER_SUBJECT => PlatformActorKind::Scheduler,
            _ => return Err(Status::unauthenticated("control subject not permitted")),
        };
        let tenant_id = claims
            .tenant_id
            .ok_or_else(|| Status::unauthenticated("control tenant required"))?;
        if tenant_id != "*"
            && (!is_valid_tenant_id(&tenant_id) || normalize_tenant_id(&tenant_id) != tenant_id)
        {
            return Err(Status::unauthenticated("invalid control tenant"));
        }
        Ok(VerifiedPlatformActor {
            subject: claims.sub,
            tenant_id,
            kind,
        })
    }

    pub fn verify_grpc<T>(&self, request: &Request<T>) -> Result<VerifiedPlatformActor, Status> {
        self.verify_token(grpc_bearer(request)?)
    }

    pub fn verify_grpc_for<T>(
        &self,
        request: &Request<T>,
        audience: &str,
    ) -> Result<VerifiedPlatformActor, Status> {
        self.verify_token_for(grpc_bearer(request)?, audience)
    }
}

/// Exactly one `authorization: Bearer <token>` entry; anything else is ambiguous.
fn grpc_bearer<T>(request: &Request<T>) -> Result<&str, Status> {
    let mut values = request.metadata().get_all("authorization").iter();
    let value = values
        .next()
        .ok_or_else(|| Status::unauthenticated("control token required"))?;
    if values.next().is_some() {
        return Err(Status::unauthenticated("duplicate control authorization"));
    }
    let value = value
        .to_str()
        .map_err(|_| Status::unauthenticated("invalid control authorization"))?;
    bearer_token(value).ok_or_else(|| Status::unauthenticated("bearer control token required"))
}

pub fn bearer_token(value: &str) -> Option<&str> {
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() || token.trim() != token {
        return None;
    }
    Some(token)
}

/// The RPC gate of a platform-service operation: the caller still checks the subject kind and
/// the tenant per method.
pub fn require_platform_assertion<T>(
    request: &Request<T>,
    verifier: Option<&PlatformAssertionVerifier>,
) -> Result<VerifiedPlatformActor, Status> {
    verifier
        .ok_or_else(|| Status::unauthenticated("platform public key not configured"))?
        .verify_grpc(request)
}

/// [`require_platform_assertion`] for an RPC with its own audience (e.g.
/// [`CORE_BUILD_STATS_AUDIENCE`]).
pub fn require_platform_assertion_for<T>(
    request: &Request<T>,
    verifier: Option<&PlatformAssertionVerifier>,
    audience: &str,
) -> Result<VerifiedPlatformActor, Status> {
    verifier
        .ok_or_else(|| Status::unauthenticated("platform public key not configured"))?
        .verify_grpc_for(request, audience)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};

    fn token_pair(ttl: u64) -> (TokenService, PlatformAssertionVerifier) {
        let key = SigningKey::from_bytes(&[83; 32]);
        let private = key.to_pkcs8_pem(Default::default()).unwrap();
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        (
            TokenService::from_ed25519_private_key(&private, PLATFORM_ISSUER, ttl).unwrap(),
            PlatformAssertionVerifier::from_public_key(&public).unwrap(),
        )
    }

    fn bearer_request(token: &str) -> Request<()> {
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        request
    }

    #[test]
    fn bearer_parser_rejects_ambiguous_values() {
        assert_eq!(bearer_token("Bearer a.b.c"), Some("a.b.c"));
        for value in [
            "Basic a.b.c",
            "Bearer ",
            "Bearer  a.b.c",
            "Bearer a.b.c ",
            "a.b.c",
        ] {
            assert!(bearer_token(value).is_none());
        }
    }

    #[test]
    fn tenant_and_actor_policy_is_exact() {
        let worker = VerifiedPlatformActor {
            subject: PROJECTION_WORKER_SUBJECT.into(),
            tenant_id: "tenant-a".into(),
            kind: PlatformActorKind::ProjectionWorker,
        };
        assert!(
            worker
                .require_kind(PlatformActorKind::ProjectionWorker)
                .is_ok()
        );
        assert!(worker.require_tenant("tenant-a").is_ok());
        for tenant in ["tenant-b", "default", "*", " tenant-a"] {
            assert!(worker.require_tenant(tenant).is_err());
        }
        assert!(worker.require_kind(PlatformActorKind::Scheduler).is_err());
    }

    #[test]
    fn platform_signature_audience_purpose_subject_and_lifetime_are_strict() {
        let (signer, verifier) = token_pair(60);
        let valid = signer
            .generate_with_ed25519(
                PROJECTION_WORKER_SUBJECT,
                Some("tenant-a"),
                "control",
                CORE_PROJECTION_AUDIENCE,
            )
            .unwrap();
        let actor = verifier.verify_token(&valid).unwrap();
        assert_eq!(actor.subject, PROJECTION_WORKER_SUBJECT);
        assert_eq!(actor.tenant_id, "tenant-a");
        assert_eq!(actor.kind, PlatformActorKind::ProjectionWorker);
        assert!(verifier.verify_token(&format!("{valid}x")).is_err());

        let scheduler = signer
            .generate_with_ed25519(
                SCHEDULER_SUBJECT,
                Some("*"),
                "control",
                CORE_PROJECTION_AUDIENCE,
            )
            .unwrap();
        let actor = verifier.verify_token(&scheduler).unwrap();
        assert_eq!(actor.kind, PlatformActorKind::Scheduler);
        assert!(actor.require_tenant("tenant-a").is_err());

        // An employee subject is not a platform service: no admin assertion exists any more.
        for subject in ["employee-1", "flare-platform-control-worker", "root"] {
            let employee = signer
                .generate_with_ed25519(
                    subject,
                    Some("tenant-a"),
                    "control",
                    CORE_PROJECTION_AUDIENCE,
                )
                .unwrap();
            assert_eq!(
                verifier.verify_token(&employee).unwrap_err().code(),
                tonic::Code::Unauthenticated,
                "{subject}"
            );
        }
        // The retired admin purpose and audience are refused.
        for (token_use, audience) in [
            ("admin", "flare-core-admin"),
            ("admin", CORE_PROJECTION_AUDIENCE),
            ("control", "flare-core-admin"),
            ("access", CORE_PROJECTION_AUDIENCE),
            ("control", "flare-social-projection"),
        ] {
            let token = signer
                .generate_with_ed25519(
                    PROJECTION_WORKER_SUBJECT,
                    Some("tenant-a"),
                    token_use,
                    audience,
                )
                .unwrap();
            assert_eq!(
                verifier.verify_token(&token).unwrap_err().code(),
                tonic::Code::Unauthenticated,
                "{token_use}/{audience}"
            );
        }

        let hmac = TokenService::new("test-only-secret", PLATFORM_ISSUER, 60)
            .generate_token(PROJECTION_WORKER_SUBJECT, None, Some("tenant-a"))
            .unwrap();
        assert!(verifier.verify_token(&hmac).is_err());
        assert!(
            verifier
                .0
                .generate_with_ed25519(
                    PROJECTION_WORKER_SUBJECT,
                    Some("tenant-a"),
                    "control",
                    CORE_PROJECTION_AUDIENCE
                )
                .is_err()
        );

        let (long_signer, _) = token_pair(61);
        let too_long = long_signer
            .generate_with_ed25519(
                PROJECTION_WORKER_SUBJECT,
                Some("tenant-a"),
                "control",
                CORE_PROJECTION_AUDIENCE,
            )
            .unwrap();
        assert!(verifier.verify_token(&too_long).is_err());
    }

    #[test]
    fn projection_and_build_stats_audiences_do_not_stand_in_for_each_other() {
        let (signer, verifier) = token_pair(60);
        let mint = |audience: &str| {
            signer
                .generate_with_ed25519(
                    PROJECTION_WORKER_SUBJECT,
                    Some("tenant-a"),
                    "control",
                    audience,
                )
                .unwrap()
        };
        let projection = mint(CORE_PROJECTION_AUDIENCE);
        let stats = mint(CORE_BUILD_STATS_AUDIENCE);
        assert!(verifier.verify_token(&projection).is_ok());
        assert!(
            verifier
                .verify_token_for(&stats, CORE_BUILD_STATS_AUDIENCE)
                .is_ok()
        );
        assert_eq!(
            verifier.verify_token(&stats).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            verifier
                .verify_token_for(&projection, CORE_BUILD_STATS_AUDIENCE)
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            require_platform_assertion_for(
                &bearer_request(&projection),
                Some(&verifier),
                CORE_BUILD_STATS_AUDIENCE
            )
            .unwrap_err()
            .code(),
            tonic::Code::Unauthenticated
        );
        assert!(
            require_platform_assertion_for(
                &bearer_request(&stats),
                Some(&verifier),
                CORE_BUILD_STATS_AUDIENCE
            )
            .is_ok()
        );
    }

    #[test]
    fn grpc_gate_rejects_missing_duplicate_or_unconfigured() {
        let (signer, verifier) = token_pair(60);
        assert_eq!(
            require_platform_assertion(&Request::new(()), Some(&verifier))
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        let mut request = Request::new(());
        request
            .metadata_mut()
            .append("authorization", "Bearer a.b.c".parse().unwrap());
        request
            .metadata_mut()
            .append("authorization", "Bearer d.e.f".parse().unwrap());
        assert_eq!(
            require_platform_assertion(&request, Some(&verifier))
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        let valid = signer
            .generate_with_ed25519(
                SCHEDULER_SUBJECT,
                Some("tenant-a"),
                "control",
                CORE_PROJECTION_AUDIENCE,
            )
            .unwrap();
        assert_eq!(
            require_platform_assertion(&bearer_request(&valid), None)
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        let actor = require_platform_assertion(&bearer_request(&valid), Some(&verifier)).unwrap();
        assert_eq!(actor.kind, PlatformActorKind::Scheduler);
    }

    #[tokio::test]
    async fn expired_platform_token_is_rejected_without_leeway() {
        let (signer, verifier) = token_pair(1);
        let token = signer
            .generate_with_ed25519(
                PROJECTION_WORKER_SUBJECT,
                Some("tenant-a"),
                "control",
                CORE_PROJECTION_AUDIENCE,
            )
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert_eq!(
            verifier.verify_token(&token).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }

    #[test]
    fn legacy_console_issuer_and_audience_are_rejected() {
        let (signer, verifier) = token_pair(60);
        let key = SigningKey::from_bytes(&[83; 32]);
        let private = key.to_pkcs8_pem(Default::default()).unwrap();
        let legacy =
            TokenService::from_ed25519_private_key(&private, "flare-im-console", 60).unwrap();
        let worker = legacy
            .generate_with_ed25519(
                PROJECTION_WORKER_SUBJECT,
                Some("tenant-a"),
                "control",
                CORE_PROJECTION_AUDIENCE,
            )
            .unwrap();
        assert!(verifier.verify_token(&worker).is_err());
        let old_audience = signer
            .generate_with_ed25519(
                PROJECTION_WORKER_SUBJECT,
                Some("tenant-a"),
                "control",
                "flare-core-control",
            )
            .unwrap();
        assert!(verifier.verify_token(&old_audience).is_err());
    }
}
