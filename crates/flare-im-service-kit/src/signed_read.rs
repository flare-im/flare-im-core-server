//! Signed-first identity for Core read RPCs: the conversation read service,
//! Storage Reader and the Sync orchestrator.
//!
//! These services took the caller's tenant and user from bare `x-tenant-id` /
//! `x-user-id` metadata, so any process able to reach their ports inside the
//! network could read as any user of any tenant. They now admit a Cell-signed
//! `x-flare-ctx` first ([`SignedFirstContextLayer`]): the signed tenant and
//! actor decide, bare identity metadata that disagrees with them is refused,
//! and with [`REQUIRE_SIGNED_READ_CTX_ENV`] on, unsigned calls are refused.
//! The switch exists for deployments that still have unsigned callers; the
//! single-host template turns it on.

use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
    SignedFirstContextLayer,
};

use crate::push_delivery::{INTERNAL_CTX_PRIVATE_KEY_ENV, INTERNAL_CTX_PUBLIC_KEY_ENV};

/// `true` refuses unsigned read calls. Unset means `false`.
pub const REQUIRE_SIGNED_READ_CTX_ENV: &str = "FLARE_REQUIRE_SIGNED_READ_CTX";

fn config_error(message: String) -> FlareError {
    ErrorBuilder::new(ErrorCode::ConfigurationError, &message).build_error()
}

/// Parses the switch. Unset or empty is off; anything other than the usual
/// spellings of on and off is a startup error rather than a silent default.
pub fn parse_require_signed_read_ctx(value: Option<&str>) -> Result<bool> {
    match value.map(str::trim).unwrap_or_default() {
        "" => Ok(false),
        value if ["1", "true", "yes", "on"].contains(&value.to_ascii_lowercase().as_str()) => {
            Ok(true)
        }
        value if ["0", "false", "no", "off"].contains(&value.to_ascii_lowercase().as_str()) => {
            Ok(false)
        }
        _ => Err(config_error(format!(
            "{REQUIRE_SIGNED_READ_CTX_ENV} must be true or false"
        ))),
    }
}

/// The read boundary for `service` from the Cell public key and the switch.
///
/// - Key and switch on: only signed calls pass.
/// - Key, switch off: signed calls are verified, unsigned ones keep the legacy
///   metadata identity.
/// - No key: refused at startup when the switch is on; otherwise the service
///   keeps the legacy metadata identity for every call and says so once.
pub fn signed_read_layer(
    service: &str,
    public_key: Option<&str>,
    require: Option<&str>,
) -> Result<SignedFirstContextLayer> {
    let require = parse_require_signed_read_ctx(require)?;
    let verifier = match public_key.map(str::trim).filter(|key| !key.is_empty()) {
        Some(key) => Some(SignedContextVerifier::from_public_key(key).map_err(|_| {
            config_error(format!(
                "invalid {INTERNAL_CTX_PUBLIC_KEY_ENV} for {service}"
            ))
        })?),
        None if require => {
            return Err(config_error(format!(
                "{service}: {REQUIRE_SIGNED_READ_CTX_ENV} needs {INTERNAL_CTX_PUBLIC_KEY_ENV}"
            )));
        }
        None => {
            tracing::warn!(
                service,
                "{INTERNAL_CTX_PUBLIC_KEY_ENV} is missing: read RPCs keep the unsigned \
                 x-tenant-id / x-user-id identity"
            );
            None
        }
    };
    if verifier.is_some() && !require {
        tracing::warn!(
            service,
            "{REQUIRE_SIGNED_READ_CTX_ENV} is off: unsigned read RPCs are still admitted"
        );
    }
    Ok(SignedFirstContextLayer::new(verifier).require_signed(require))
}

/// [`signed_read_layer`] from `FLARE_INTERNAL_CTX_PUBLIC_KEY` and
/// [`REQUIRE_SIGNED_READ_CTX_ENV`].
pub fn signed_read_layer_from_env(service: &str) -> Result<SignedFirstContextLayer> {
    let public_key = env_text(INTERNAL_CTX_PUBLIC_KEY_ENV)?;
    let require = env_text(REQUIRE_SIGNED_READ_CTX_ENV)?;
    signed_read_layer(service, public_key.as_deref(), require.as_deref())
}

/// The Cell signer for a service that reads as itself (`Service` actor), or
/// `None` without `FLARE_INTERNAL_CTX_PRIVATE_KEY`. A key that is present but
/// unusable is a startup error.
pub fn service_signer_from_env(service: &str) -> Result<Option<SignedContextSigner>> {
    match env_text(INTERNAL_CTX_PRIVATE_KEY_ENV)? {
        Some(key) if !key.trim().is_empty() => SignedContextSigner::from_private_key(key.trim())
            .map(Some)
            .map_err(|_| {
                config_error(format!(
                    "invalid {INTERNAL_CTX_PRIVATE_KEY_ENV} for {service}"
                ))
            }),
        _ => Ok(None),
    }
}

/// A `Service` context token for `tenant_id`, for reads a service makes on its
/// own behalf (member lists for fan-out, notification preferences). It has no
/// user: services read by tenant and conversation, never as somebody.
pub fn sign_service_read(
    signer: &SignedContextSigner,
    actor_id: &str,
    tenant_id: &str,
    request_id: &str,
) -> Result<String> {
    let request_id = if request_id.trim().is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        request_id.to_owned()
    };
    signer
        .sign(&SignedContextIdentity::new(
            SignedActorType::Service,
            actor_id,
            tenant_id,
            request_id,
        ))
        .map_err(|_| {
            ErrorBuilder::new(
                ErrorCode::ServiceUnavailable,
                "internal read context could not be signed",
            )
            .build_error()
        })
}

fn env_text(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(config_error(format!("{name} is not valid UTF-8")))
        }
    }
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::EncodePublicKey;

    use super::*;

    fn public_key() -> String {
        SigningKey::from_bytes(&[9; 32])
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap()
    }

    #[test]
    fn the_switch_accepts_only_clear_values() {
        for (value, expected) in [
            (None, false),
            (Some(""), false),
            (Some("true"), true),
            (Some(" TRUE "), true),
            (Some("1"), true),
            (Some("on"), true),
            (Some("false"), false),
            (Some("0"), false),
            (Some("off"), false),
        ] {
            assert_eq!(
                parse_require_signed_read_ctx(value).unwrap(),
                expected,
                "{value:?}"
            );
        }
        for value in ["ture", "required", "2"] {
            assert!(
                parse_require_signed_read_ctx(Some(value)).is_err(),
                "{value}"
            );
        }
    }

    #[test]
    fn a_required_boundary_needs_a_usable_public_key() {
        let key = public_key();
        assert!(
            signed_read_layer("t", Some(&key), Some("true"))
                .unwrap()
                .requires_signed()
        );
        assert!(
            !signed_read_layer("t", Some(&key), None)
                .unwrap()
                .requires_signed()
        );
        assert!(
            !signed_read_layer("t", None, Some("false"))
                .unwrap()
                .requires_signed()
        );
        assert!(signed_read_layer("t", None, Some("true")).is_err());
        assert!(signed_read_layer("t", Some("   "), Some("true")).is_err());
        assert!(signed_read_layer("t", Some("not a key"), None).is_err());
    }
}
