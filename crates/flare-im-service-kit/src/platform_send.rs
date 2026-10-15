//! Verified machine authority for the narrow Platform system-message path.
use flare_server_core::{SignedActorType, auth::SignedContextClaims};
pub const SYSTEM_MESSAGE_SCOPE: &str = "platform.messages.send";
/// Distinct from the Social Staff gateway and the Cell's legacy/user signer.
pub const PLATFORM_API_KID: &str = flare_server_core::auth::kid::API_GATEWAY;
/// Signed request correlation on the private unified Gateway -> Core HTTP hop.
pub const ORIGIN_CONTEXT_HEADER: &str = "x-flare-origin-ctx";

pub fn is_platform_system_sender(claims: &SignedContextClaims) -> bool {
    claims.actor_type == SignedActorType::Service
        && claims.kid == PLATFORM_API_KID
        && claims.roles == [SYSTEM_MESSAGE_SCOPE]
        && claims.credential_id.as_deref().is_some_and(|id| {
            id.len() == 12
                && id
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || (b'2'..=b'7').contains(&b))
        })
}
