//! Platform → Core control-plane JWT boundary.
//!
//! This verifier only holds the platform public key. It cannot mint credentials,
//! and neither a user access JWT nor a cell-internal `x-flare-ctx` is accepted.
//!
//! Two assertions reach Core gRPC services: the control assertion the platform sends
//! directly (`token_use=control`, audience [`CORE_PROJECTION_AUDIENCE`]) and the
//! admin assertion that admin-gateway has already verified and forwards unchanged
//! (`token_use=admin`, audience [`CORE_ADMIN_AUDIENCE`]). The admin branch is
//! always employee-only and bound to one concrete tenant; admin-gateway never
//! holds a signing key, so it cannot widen what the forwarded assertion says.

use std::sync::Arc;

use flare_im_contracts::domain::tenant::is_valid_tenant_id;
use flare_im_contracts::utils::normalize_tenant_id;
use flare_server_core::TokenService;
use tonic::{Request, Status};

pub const PLATFORM_ISSUER: &str = "flare-platform";
pub const CORE_PROJECTION_AUDIENCE: &str = "flare-core-projection";
pub const CORE_ADMIN_AUDIENCE: &str = "flare-core-admin";
pub const PROJECTION_WORKER_SUBJECT: &str = "flare-platform-worker";
pub const SCHEDULER_SUBJECT: &str = "flare-platform-control-scheduler";
const MAX_TOKEN_BYTES: usize = 8192;
const MAX_TOKEN_LIFETIME_SECONDS: usize = 60;

#[derive(Clone)]
pub struct ConsoleControlVerifier(Arc<TokenService>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlActorKind {
    Employee,
    ProjectionWorker,
    Scheduler,
}

/// Which platform assertion authorized a gRPC call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlChannel {
    /// Console → Core directly: `token_use=control`, audience [`CORE_PROJECTION_AUDIENCE`].
    Control,
    /// Console → admin-gateway → Core, forwarded unchanged: `token_use=admin`,
    /// audience [`CORE_ADMIN_AUDIENCE`], employee subject, concrete tenant.
    Admin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedControlActor {
    pub subject: String,
    pub tenant_id: String,
    pub kind: ControlActorKind,
}

impl VerifiedControlActor {
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

    pub fn require_kind(&self, kind: ControlActorKind) -> Result<(), Status> {
        if self.kind != kind {
            return Err(Status::permission_denied("control actor not permitted"));
        }
        Ok(())
    }
}

impl ConsoleControlVerifier {
    pub fn from_public_key(material: &str) -> flare_server_core::error::Result<Self> {
        Ok(Self(Arc::new(TokenService::from_ed25519_public_key(
            material,
            PLATFORM_ISSUER,
        )?)))
    }

    /// Parsing an absent key does not grant a legacy fallback: protected RPCs
    /// will return UNAUTHENTICATED until a Console public key is configured.
    pub fn from_env() -> flare_server_core::error::Result<Option<Self>> {
        match std::env::var("FLARE_CONSOLE_PUBLIC_KEY") {
            Ok(material) => Ok(Some(Self::from_public_key(&material)?)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(flare_server_core::error::FlareError::system(
                    "FLARE_CONSOLE_PUBLIC_KEY has invalid encoding",
                ))
            }
        }
    }

    pub fn verify_token(
        &self,
        token: &str,
        expected_use: &str,
        audience: &str,
    ) -> Result<VerifiedControlActor, Status> {
        if token.is_empty()
            || token.len() > MAX_TOKEN_BYTES
            || token.chars().any(char::is_whitespace)
        {
            return Err(Status::unauthenticated("invalid control token"));
        }
        let claims = self
            .0
            .validate_ed25519_with_use(token, expected_use, audience)
            .map_err(|_| Status::unauthenticated("invalid control token"))?;
        if claims.exp - claims.iat > MAX_TOKEN_LIFETIME_SECONDS
            || claims.sub.len() > 256
            || claims.sub.trim() != claims.sub
            || !claims.sub.is_ascii()
            || claims.sub.chars().any(char::is_control)
        {
            return Err(Status::unauthenticated("invalid control token"));
        }
        let tenant_id = claims
            .tenant_id
            .ok_or_else(|| Status::unauthenticated("control tenant required"))?;
        if tenant_id != "*"
            && (!is_valid_tenant_id(&tenant_id) || normalize_tenant_id(&tenant_id) != tenant_id)
        {
            return Err(Status::unauthenticated("invalid control tenant"));
        }
        let kind = match claims.sub.as_str() {
            PROJECTION_WORKER_SUBJECT => ControlActorKind::ProjectionWorker,
            SCHEDULER_SUBJECT => ControlActorKind::Scheduler,
            _ => ControlActorKind::Employee,
        };
        Ok(VerifiedControlActor {
            subject: claims.sub,
            tenant_id,
            kind,
        })
    }

    pub fn verify_grpc<T>(&self, request: &Request<T>) -> Result<VerifiedControlActor, Status> {
        self.verify_token(grpc_bearer(request)?, "control", CORE_PROJECTION_AUDIENCE)
    }

    /// The admin assertion admin-gateway forwards unchanged. Only an employee
    /// bound to one concrete tenant passes; `*` and the reserved service
    /// subjects are rejected, so the caller may scope every read to
    /// `actor.tenant_id` without looking at the request body.
    pub fn verify_admin_token(&self, token: &str) -> Result<VerifiedControlActor, Status> {
        let actor = self.verify_token(token, "admin", CORE_ADMIN_AUDIENCE)?;
        actor.require_kind(ControlActorKind::Employee)?;
        actor.require_tenant(&actor.tenant_id)?;
        Ok(actor)
    }

    pub fn verify_grpc_admin<T>(
        &self,
        request: &Request<T>,
    ) -> Result<VerifiedControlActor, Status> {
        self.verify_admin_token(grpc_bearer(request)?)
    }

    /// Operations Console may invoke either directly (control assertion) or
    /// through admin-gateway (forwarded admin assertion). The control branch
    /// keeps its existing per-method policy; the admin branch is already
    /// narrowed to an employee with a concrete tenant.
    pub fn verify_grpc_control_or_admin<T>(
        &self,
        request: &Request<T>,
    ) -> Result<(VerifiedControlActor, ControlChannel), Status> {
        let token = grpc_bearer(request)?;
        if let Ok(actor) = self.verify_token(token, "control", CORE_PROJECTION_AUDIENCE) {
            return Ok((actor, ControlChannel::Control));
        }
        Ok((self.verify_admin_token(token)?, ControlChannel::Admin))
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

pub fn require_grpc_control<T>(
    request: &Request<T>,
    verifier: Option<&ConsoleControlVerifier>,
) -> Result<VerifiedControlActor, Status> {
    verifier
        .ok_or_else(|| Status::unauthenticated("Console control public key not configured"))?
        .verify_grpc(request)
}

/// Admin-only read RPCs: the forwarded admin assertion, nothing else.
pub fn require_grpc_admin<T>(
    request: &Request<T>,
    verifier: Option<&ConsoleControlVerifier>,
) -> Result<VerifiedControlActor, Status> {
    verifier
        .ok_or_else(|| Status::unauthenticated("Console control public key not configured"))?
        .verify_grpc_admin(request)
}

pub fn require_grpc_control_or_admin<T>(
    request: &Request<T>,
    verifier: Option<&ConsoleControlVerifier>,
) -> Result<(VerifiedControlActor, ControlChannel), Status> {
    verifier
        .ok_or_else(|| Status::unauthenticated("Console control public key not configured"))?
        .verify_grpc_control_or_admin(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};

    fn token_pair(ttl: u64) -> (TokenService, ConsoleControlVerifier) {
        let key = SigningKey::from_bytes(&[83; 32]);
        let private = key.to_pkcs8_pem(Default::default()).unwrap();
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        (
            TokenService::from_ed25519_private_key(&private, PLATFORM_ISSUER, ttl).unwrap(),
            ConsoleControlVerifier::from_public_key(&public).unwrap(),
        )
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
        let employee = VerifiedControlActor {
            subject: "employee-1".into(),
            tenant_id: "tenant-a".into(),
            kind: ControlActorKind::Employee,
        };
        assert!(employee.require_kind(ControlActorKind::Employee).is_ok());
        assert!(employee.require_tenant("tenant-a").is_ok());
        for tenant in ["tenant-b", "default", "*", " tenant-a"] {
            assert!(employee.require_tenant(tenant).is_err());
        }
        assert!(employee.require_kind(ControlActorKind::Scheduler).is_err());
    }

    #[test]
    fn console_control_signature_audience_purpose_and_lifetime_are_strict() {
        let (signer, verifier) = token_pair(60);
        let valid = signer
            .generate_with_ed25519(
                "employee-1",
                Some("tenant-a"),
                "control",
                CORE_PROJECTION_AUDIENCE,
            )
            .unwrap();
        let actor = verifier
            .verify_token(&valid, "control", CORE_PROJECTION_AUDIENCE)
            .unwrap();
        assert_eq!(actor.subject, "employee-1");
        assert_eq!(actor.tenant_id, "tenant-a");
        assert!(
            verifier
                .verify_token(&valid, "admin", CORE_ADMIN_AUDIENCE)
                .is_err()
        );
        assert!(
            verifier
                .verify_token(&valid, "control", CORE_ADMIN_AUDIENCE)
                .is_err()
        );
        assert!(
            verifier
                .verify_token(&format!("{valid}x"), "control", CORE_PROJECTION_AUDIENCE)
                .is_err()
        );

        let wrong_use = signer
            .generate_with_ed25519(
                "employee-1",
                Some("tenant-a"),
                "access",
                CORE_PROJECTION_AUDIENCE,
            )
            .unwrap();
        assert!(
            verifier
                .verify_token(&wrong_use, "control", CORE_PROJECTION_AUDIENCE)
                .is_err()
        );
        let wrong_aud = signer
            .generate_with_ed25519(
                "employee-1",
                Some("tenant-a"),
                "control",
                "flare-social-projection",
            )
            .unwrap();
        assert!(
            verifier
                .verify_token(&wrong_aud, "control", CORE_PROJECTION_AUDIENCE)
                .is_err()
        );
        let wildcard = signer
            .generate_with_ed25519("employee-1", Some("*"), "control", CORE_PROJECTION_AUDIENCE)
            .unwrap();
        assert!(
            verifier
                .verify_token(&wildcard, "control", CORE_PROJECTION_AUDIENCE)
                .unwrap()
                .require_tenant("tenant-a")
                .is_err()
        );

        let hmac = TokenService::new("test-only-secret", PLATFORM_ISSUER, 60)
            .generate_token("employee-1", None, Some("tenant-a"))
            .unwrap();
        assert!(
            verifier
                .verify_token(&hmac, "control", CORE_PROJECTION_AUDIENCE)
                .is_err()
        );
        assert!(
            verifier
                .0
                .generate_with_ed25519(
                    "employee-1",
                    Some("tenant-a"),
                    "control",
                    CORE_PROJECTION_AUDIENCE
                )
                .is_err()
        );

        let (long_signer, _) = token_pair(61);
        let too_long = long_signer
            .generate_with_ed25519(
                "employee-1",
                Some("tenant-a"),
                "control",
                CORE_PROJECTION_AUDIENCE,
            )
            .unwrap();
        assert!(
            verifier
                .verify_token(&too_long, "control", CORE_PROJECTION_AUDIENCE)
                .is_err()
        );
    }

    #[test]
    fn grpc_control_rejects_missing_or_duplicate_authorization() {
        let (_, verifier) = token_pair(60);
        let request = Request::new(());
        assert_eq!(
            verifier.verify_grpc(&request).unwrap_err().code(),
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
            verifier.verify_grpc(&request).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }

    fn bearer_request(token: &str) -> Request<()> {
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        request
    }

    #[test]
    fn forwarded_admin_assertion_is_employee_with_a_concrete_tenant_only() {
        let (signer, verifier) = token_pair(60);
        let admin = |subject: &str, tenant: &str, token_use: &str, audience: &str| {
            signer
                .generate_with_ed25519(subject, Some(tenant), token_use, audience)
                .unwrap()
        };

        let actor = require_grpc_admin(
            &bearer_request(&admin(
                "employee-1",
                "tenant-a",
                "admin",
                CORE_ADMIN_AUDIENCE,
            )),
            Some(&verifier),
        )
        .unwrap();
        assert_eq!(actor.subject, "employee-1");
        assert_eq!(actor.tenant_id, "tenant-a");
        assert_eq!(actor.kind, ControlActorKind::Employee);

        // Missing token, or no public key configured.
        assert_eq!(
            require_grpc_admin(&Request::new(()), Some(&verifier))
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            require_grpc_admin(
                &bearer_request(&admin(
                    "employee-1",
                    "tenant-a",
                    "admin",
                    CORE_ADMIN_AUDIENCE
                )),
                None
            )
            .unwrap_err()
            .code(),
            tonic::Code::Unauthenticated
        );
        // Wrong audience / purpose: a direct control assertion is not an admin one.
        for token in [
            admin(
                "employee-1",
                "tenant-a",
                "control",
                CORE_PROJECTION_AUDIENCE,
            ),
            admin("employee-1", "tenant-a", "admin", CORE_PROJECTION_AUDIENCE),
            admin("employee-1", "tenant-a", "control", CORE_ADMIN_AUDIENCE),
        ] {
            assert_eq!(
                require_grpc_admin(&bearer_request(&token), Some(&verifier))
                    .unwrap_err()
                    .code(),
                tonic::Code::Unauthenticated
            );
        }
        // Platform-wide or service subjects never pass the admin branch.
        for token in [
            admin("employee-1", "*", "admin", CORE_ADMIN_AUDIENCE),
            admin(
                PROJECTION_WORKER_SUBJECT,
                "tenant-a",
                "admin",
                CORE_ADMIN_AUDIENCE,
            ),
            admin(SCHEDULER_SUBJECT, "tenant-a", "admin", CORE_ADMIN_AUDIENCE),
        ] {
            assert_eq!(
                require_grpc_admin(&bearer_request(&token), Some(&verifier))
                    .unwrap_err()
                    .code(),
                tonic::Code::PermissionDenied
            );
        }
    }

    #[test]
    fn control_or_admin_reports_the_channel_and_keeps_admin_narrowing() {
        let (signer, verifier) = token_pair(60);
        let token = |subject: &str, tenant: &str, token_use: &str, audience: &str| {
            bearer_request(
                &signer
                    .generate_with_ed25519(subject, Some(tenant), token_use, audience)
                    .unwrap(),
            )
        };

        // Control branch is unchanged: its per-method policy still decides.
        let (actor, channel) = require_grpc_control_or_admin(
            &token(
                SCHEDULER_SUBJECT,
                "tenant-a",
                "control",
                CORE_PROJECTION_AUDIENCE,
            ),
            Some(&verifier),
        )
        .unwrap();
        assert_eq!(channel, ControlChannel::Control);
        assert_eq!(actor.kind, ControlActorKind::Scheduler);

        let (actor, channel) = require_grpc_control_or_admin(
            &token("employee-1", "tenant-a", "admin", CORE_ADMIN_AUDIENCE),
            Some(&verifier),
        )
        .unwrap();
        assert_eq!(channel, ControlChannel::Admin);
        assert_eq!(actor.tenant_id, "tenant-a");

        assert_eq!(
            require_grpc_control_or_admin(
                &token(SCHEDULER_SUBJECT, "tenant-a", "admin", CORE_ADMIN_AUDIENCE),
                Some(&verifier)
            )
            .unwrap_err()
            .code(),
            tonic::Code::PermissionDenied
        );
        for request in [
            Request::new(()),
            token("employee-1", "tenant-a", "access", CORE_ADMIN_AUDIENCE),
            token("employee-1", "tenant-a", "admin", "flare-social-projection"),
        ] {
            assert_eq!(
                require_grpc_control_or_admin(&request, Some(&verifier))
                    .unwrap_err()
                    .code(),
                tonic::Code::Unauthenticated
            );
        }
    }

    #[tokio::test]
    async fn expired_console_control_token_is_rejected_without_leeway() {
        let (signer, verifier) = token_pair(1);
        let token = signer
            .generate_with_ed25519(
                "employee-1",
                Some("tenant-a"),
                "control",
                CORE_PROJECTION_AUDIENCE,
            )
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert_eq!(
            verifier
                .verify_token(&token, "control", CORE_PROJECTION_AUDIENCE)
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
    }

    #[test]
    fn legacy_console_issuer_audience_and_worker_subject_are_rejected() {
        let (signer, verifier) = token_pair(60);
        let key = SigningKey::from_bytes(&[83; 32]);
        let private = key.to_pkcs8_pem(Default::default()).unwrap();
        let legacy =
            TokenService::from_ed25519_private_key(&private, "flare-im-console", 60).unwrap();
        let employee = legacy
            .generate_with_ed25519(
                "employee-1",
                Some("tenant-a"),
                "control",
                CORE_PROJECTION_AUDIENCE,
            )
            .unwrap();
        assert!(
            verifier
                .verify_token(&employee, "control", CORE_PROJECTION_AUDIENCE)
                .is_err()
        );
        let old_audience = signer
            .generate_with_ed25519(
                "employee-1",
                Some("tenant-a"),
                "control",
                "flare-core-control",
            )
            .unwrap();
        assert!(
            verifier
                .verify_token(&old_audience, "control", CORE_PROJECTION_AUDIENCE)
                .is_err()
        );
        let old_subject = signer
            .generate_with_ed25519(
                "flare-platform-control-worker",
                Some("*"),
                "control",
                CORE_PROJECTION_AUDIENCE,
            )
            .unwrap();
        if let Ok(actor) = verifier.verify_token(&old_subject, "control", CORE_PROJECTION_AUDIENCE)
        {
            assert!(
                actor
                    .require_kind(ControlActorKind::ProjectionWorker)
                    .is_err()
            );
        }
    }
}
