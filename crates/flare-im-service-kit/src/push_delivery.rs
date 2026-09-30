//! Push delivery provenance gate shared by Push Proxy, Push Server and Push Worker.
//!
//! Push events travel over the broker without per-record authentication today, so
//! a Push process may only run when the deployment states how forged broker
//! records are excluded (`<ROLE>_EVENT_PROVENANCE_MODE`):
//!
//! - unset / `paused` — refuse to start or enqueue (default).
//! - `broker-isolated` — single-Cell deployments: the broker admits only this
//!   Cell's Core credential and no other workload is attached to the broker
//!   network, so publishing a Push record already requires Core's own identity.
//!   The gate enforces the observable half of that claim: the configured broker
//!   is NATS and its URL carries credentials. Network placement is the
//!   installer's responsibility (see the single-host Compose template).
//! - `signed-v1` — per-record signatures for multi-host topologies; reserved and
//!   refused until producers, consumers and replay state are wired.
//!
//! Any other value is a configuration error.

use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};

pub const BROKER_ISOLATED: &str = "broker-isolated";

/// How Push records on the broker are protected for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushDeliveryMode {
    BrokerIsolated,
}

/// The broker the process will publish to or consume from, as configured.
#[derive(Debug, Clone, Copy)]
pub struct BrokerEndpoint<'a> {
    pub backend: &'a str,
    pub nats_url: &'a str,
}

fn unavailable(message: String) -> FlareError {
    ErrorBuilder::new(ErrorCode::ServiceUnavailable, message).build_error()
}

fn invalid(message: String) -> FlareError {
    ErrorBuilder::new(ErrorCode::InvalidParameter, message).build_error()
}

/// Whether a NATS URL carries user or token credentials (`nats://user:pass@host`).
/// Credentials are never included in any error text.
pub fn nats_url_has_credentials(url: &str) -> bool {
    let Some((_, rest)) = url.trim().split_once("://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    match authority.rsplit_once('@') {
        Some((userinfo, host)) => {
            let user = userinfo.split(':').next().unwrap_or_default();
            !user.is_empty() && !host.is_empty()
        }
        None => false,
    }
}

/// Enforce the provenance mode before any broker connection is opened.
///
/// `role` names the process in operator-facing errors (e.g. "Push Server");
/// `mode_env` is the environment variable that selected `mode`.
pub fn require_push_delivery(
    role: &str,
    mode_env: &str,
    mode: Option<&str>,
    broker: BrokerEndpoint<'_>,
) -> Result<PushDeliveryMode> {
    match mode {
        None | Some("paused") => Err(unavailable(format!(
            "{role} is paused: set {mode_env} once broker provenance is enforced; broker backlog remains subject to configured retention"
        ))),
        Some("signed-v1") => Err(unavailable(format!(
            "{mode_env}=signed-v1 is reserved until producer signatures, broker ACLs and consumer verification are implemented"
        ))),
        Some(BROKER_ISOLATED) => {
            if broker.backend != "nats" {
                return Err(invalid(format!(
                    "{mode_env}={BROKER_ISOLATED} is defined only for an authenticated NATS broker (configured backend: {})",
                    broker.backend
                )));
            }
            if !nats_url_has_credentials(broker.nats_url) {
                return Err(invalid(format!(
                    "{mode_env}={BROKER_ISOLATED} requires a NATS URL with this Cell's credentials; an unauthenticated broker accepts forged Push records"
                )));
            }
            Ok(PushDeliveryMode::BrokerIsolated)
        }
        Some(_) => Err(invalid(format!(
            "{mode_env} must be paused or {BROKER_ISOLATED} (signed-v1 is reserved)"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ENV: &str = "PUSH_SERVER_EVENT_PROVENANCE_MODE";

    fn nats(url: &str) -> BrokerEndpoint<'_> {
        BrokerEndpoint {
            backend: "nats",
            nats_url: url,
        }
    }

    #[test]
    fn default_and_pause_refuse_with_backlog_notice() {
        for mode in [None, Some("paused")] {
            let error = require_push_delivery("Push Server", ENV, mode, nats("nats://c:p@n:4222"))
                .unwrap_err();
            assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
            assert!(error.to_string().contains("Push Server is paused"));
            assert!(error.to_string().contains("backlog"));
        }
    }

    #[test]
    fn signed_v1_stays_reserved_and_unknown_values_are_invalid() {
        let reserved = require_push_delivery(
            "Push Server",
            ENV,
            Some("signed-v1"),
            nats("nats://c:p@n:4222"),
        )
        .unwrap_err();
        assert_eq!(reserved.code(), Some(ErrorCode::ServiceUnavailable));
        for mode in ["", "active", "true", "BROKER-ISOLATED", "broker_isolated"] {
            let error =
                require_push_delivery("Push Server", ENV, Some(mode), nats("nats://c:p@n:4222"))
                    .unwrap_err();
            assert_eq!(error.code(), Some(ErrorCode::InvalidParameter), "{mode}");
        }
    }

    #[test]
    fn broker_isolated_requires_authenticated_nats() {
        assert_eq!(
            require_push_delivery(
                "Push Server",
                ENV,
                Some(BROKER_ISOLATED),
                nats("nats://core:secret@nats:4222")
            )
            .unwrap(),
            PushDeliveryMode::BrokerIsolated
        );
        for url in [
            "nats://nats:4222",
            "nats://@nats:4222",
            "nats://:pw@nats:4222",
            "nats:4222",
            "",
        ] {
            let error = require_push_delivery("Push Server", ENV, Some(BROKER_ISOLATED), nats(url))
                .unwrap_err();
            assert_eq!(error.code(), Some(ErrorCode::InvalidParameter), "{url}");
            assert!(!error.to_string().contains("secret"));
        }
        let kafka = BrokerEndpoint {
            backend: "kafka",
            nats_url: "nats://core:secret@nats:4222",
        };
        assert!(require_push_delivery("Push Server", ENV, Some(BROKER_ISOLATED), kafka).is_err());
    }

    #[test]
    fn credential_detection() {
        assert!(nats_url_has_credentials("nats://core:secret@nats:4222"));
        assert!(nats_url_has_credentials("nats://token@nats:4222"));
        assert!(nats_url_has_credentials("tls://core:p@nats.example:4222/"));
        assert!(!nats_url_has_credentials("nats://nats:4222/path@x"));
        assert!(!nats_url_has_credentials("nats://nats:4222"));
    }
}
