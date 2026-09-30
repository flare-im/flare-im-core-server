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

use std::sync::Arc;

use flare_server_core::context::Ctx;
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata;
use flare_server_core::mq::consumer::{ConsumerError, Message, MessageHandler, MessageResult};
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
};

pub const BROKER_ISOLATED: &str = "broker-isolated";
pub const INTERNAL_CTX_PRIVATE_KEY_ENV: &str = "FLARE_INTERNAL_CTX_PRIVATE_KEY";
pub const INTERNAL_CTX_PUBLIC_KEY_ENV: &str = "FLARE_INTERNAL_CTX_PUBLIC_KEY";

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

/// Per-record identity of a Push consumer in `broker-isolated` mode.
///
/// The broker admits only this Cell's Core, so the tenant a record carries is
/// Core's own statement. Each record is attested as the consuming Push service
/// for that tenant with the Cell internal-context key, then verified through the
/// same path as an incoming RPC: downstream checks (Online queries, Gateway
/// delivery) see an ordinary signed Service context and forward its token.
#[derive(Clone)]
pub struct BrokerRecordIdentity {
    actor_id: &'static str,
    signer: SignedContextSigner,
    verifier: SignedContextVerifier,
}

impl BrokerRecordIdentity {
    /// `mode` is the result of [`require_push_delivery`]: a paused deployment has
    /// no record identity. Fails when the key pair is missing or does not match.
    pub fn new(
        mode: PushDeliveryMode,
        actor_id: &'static str,
        private_key: Option<&str>,
        public_key: Option<&str>,
    ) -> Result<Self> {
        let PushDeliveryMode::BrokerIsolated = mode;
        let key = |value: Option<&str>, env: &str| {
            value
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| {
                    unavailable(format!(
                        "{actor_id} requires {env} to attest broker records"
                    ))
                })
        };
        let signer =
            SignedContextSigner::from_private_key(&key(private_key, INTERNAL_CTX_PRIVATE_KEY_ENV)?)
                .map_err(|_| {
                    invalid(format!("{INTERNAL_CTX_PRIVATE_KEY_ENV} is not a valid key"))
                })?;
        let verifier =
            SignedContextVerifier::from_public_key(&key(public_key, INTERNAL_CTX_PUBLIC_KEY_ENV)?)
                .map_err(|_| {
                    invalid(format!("{INTERNAL_CTX_PUBLIC_KEY_ENV} is not a valid key"))
                })?;
        let identity = Self {
            actor_id,
            signer,
            verifier,
        };
        identity.attest("0", "startup-key-check", "").map_err(|_| {
            invalid(format!(
                "{INTERNAL_CTX_PRIVATE_KEY_ENV} and {INTERNAL_CTX_PUBLIC_KEY_ENV} are not one key pair"
            ))
        })?;
        Ok(identity)
    }

    pub fn from_env(mode: PushDeliveryMode, actor_id: &'static str) -> Result<Self> {
        let private_key = std::env::var(INTERNAL_CTX_PRIVATE_KEY_ENV).ok();
        let public_key = std::env::var(INTERNAL_CTX_PUBLIC_KEY_ENV).ok();
        Self::new(
            mode,
            actor_id,
            private_key.as_deref(),
            public_key.as_deref(),
        )
    }

    /// Verified context for a record consumed with context `record`, whose tenant
    /// comes from the record headers Core wrote.
    pub fn attest_record(&self, record: &Ctx) -> Result<Ctx> {
        self.attest(
            record.tenant_id().unwrap_or_default(),
            record.request_id(),
            record.trace_id(),
        )
    }

    /// Verified context for one broker record of `tenant_id`.
    pub fn attest(&self, tenant_id: &str, request_id: &str, trace_id: &str) -> Result<Ctx> {
        if !flare_im_contracts::domain::tenant::is_valid_tenant_id(tenant_id)
            || flare_im_contracts::utils::normalize_tenant_id(tenant_id) != tenant_id
        {
            return Err(ErrorBuilder::new(
                ErrorCode::PermissionDenied,
                "broker record tenant missing or not canonical",
            )
            .build_error());
        }
        let request_id = if request_id.trim().is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            request_id.to_owned()
        };
        let token = self
            .signer
            .sign(&SignedContextIdentity::new(
                SignedActorType::Service,
                self.actor_id,
                tenant_id,
                request_id,
            ))
            .map_err(|_| unavailable("broker record attestation could not be signed".into()))?;
        let mut carrier = tonic::Request::new(());
        flare_server_core::set_signed_context_token(&mut carrier, &token)
            .map_err(|_| unavailable("broker record attestation is not a valid header".into()))?;
        let verified = verify_signed_context_metadata(carrier.metadata(), &self.verifier)
            .map_err(|_| unavailable("broker record attestation did not verify".into()))?;
        Ok(if trace_id.trim().is_empty() {
            verified
        } else {
            Arc::new(verified.with_trace_id(trace_id))
        })
    }
}

/// Runs a Push consumer's handler on attested records: each record's context is
/// replaced by [`BrokerRecordIdentity::attest_record`] before the handler sees it.
/// A record without a canonical tenant goes to the dead-letter stream.
pub struct AttestedRecordHandler {
    identity: BrokerRecordIdentity,
    inner: Arc<dyn MessageHandler>,
}

impl AttestedRecordHandler {
    pub fn new(identity: BrokerRecordIdentity, inner: Arc<dyn MessageHandler>) -> Self {
        Self { identity, inner }
    }

    fn attest(&self, mut message: Message) -> Option<Message> {
        match self.identity.attest_record(&message.context.ctx) {
            Ok(ctx) => {
                message.context.ctx = ctx;
                Some(message)
            }
            Err(error) => {
                tracing::error!(
                    %error,
                    topic = %message.context.topic,
                    offset = message.context.offset,
                    "broker record not attested, sending to DLQ"
                );
                None
            }
        }
    }
}

#[async_trait::async_trait]
impl MessageHandler for AttestedRecordHandler {
    async fn handle(&self, message: Message) -> std::result::Result<MessageResult, ConsumerError> {
        match self.attest(message) {
            Some(message) => self.inner.handle(message).await,
            None => Ok(MessageResult::DeadLetter),
        }
    }

    async fn handle_batch(
        &self,
        messages: Vec<Message>,
    ) -> std::result::Result<Vec<MessageResult>, ConsumerError> {
        let mut results = vec![MessageResult::DeadLetter; messages.len()];
        let mut positions = Vec::with_capacity(messages.len());
        let mut attested = Vec::with_capacity(messages.len());
        for (position, message) in messages.into_iter().enumerate() {
            if let Some(message) = self.attest(message) {
                positions.push(position);
                attested.push(message);
            }
        }
        if !attested.is_empty() {
            let inner = self.inner.handle_batch(attested).await?;
            if inner.len() != positions.len() {
                return Err(ConsumerError::Handler(
                    "batch handler returned a result count that does not match its input".into(),
                ));
            }
            for (position, result) in positions.into_iter().zip(inner) {
                results[position] = result;
            }
        }
        Ok(results)
    }

    fn supports_batch(&self) -> bool {
        self.inner.supports_batch()
    }

    fn name(&self) -> &str {
        self.inner.name()
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

    fn key_pair() -> (String, String) {
        use base64::Engine as _;
        use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
        let key = ed25519_dalek::SigningKey::from_bytes(&[7u8; 32]);
        let private = key
            .to_pkcs8_pem(Default::default())
            .expect("private pem")
            .to_string();
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .expect("public pem");
        let b64 = base64::engine::general_purpose::STANDARD;
        (b64.encode(private), b64.encode(public))
    }

    #[test]
    fn broker_record_identity_is_a_verified_service_context() {
        use flare_server_core::grpc::middleware::signed_context::VerifiedContext;
        let (private, public) = key_pair();
        let identity = BrokerRecordIdentity::new(
            PushDeliveryMode::BrokerIsolated,
            "flare-push-worker",
            Some(&private),
            Some(&public),
        )
        .expect("identity");
        let ctx = identity
            .attest("tenant-a", "req-1", "trace-1")
            .expect("attested");
        assert_eq!(ctx.tenant_id(), Some("tenant-a"));
        assert_eq!(ctx.trace_id(), "trace-1");
        let marker = ctx.get_data::<VerifiedContext>().expect("verified marker");
        let claims = marker.claims().expect("claims");
        assert_eq!(claims.actor_type, SignedActorType::Service);
        assert_eq!(claims.actor_id, "flare-push-worker");
        assert_eq!(claims.tenant_id, "tenant-a");
        assert!(marker.token().is_some());

        for tenant in ["", " tenant-a", "Tenant-A "] {
            assert!(identity.attest(tenant, "req", "").is_err(), "{tenant:?}");
        }
    }

    #[test]
    fn broker_record_identity_requires_one_key_pair() {
        let (private, public) = key_pair();
        let (other_private, _) = {
            use base64::Engine as _;
            use ed25519_dalek::pkcs8::EncodePrivateKey;
            let key = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
            let pem = key.to_pkcs8_pem(Default::default()).unwrap().to_string();
            (base64::engine::general_purpose::STANDARD.encode(pem), ())
        };
        let mode = PushDeliveryMode::BrokerIsolated;
        assert!(BrokerRecordIdentity::new(mode, "flare-push-server", None, Some(&public)).is_err());
        assert!(
            BrokerRecordIdentity::new(mode, "flare-push-server", Some(&private), None).is_err()
        );
        let mismatch = BrokerRecordIdentity::new(
            mode,
            "flare-push-server",
            Some(&other_private),
            Some(&public),
        );
        assert!(mismatch.is_err());
    }

    /// Records what the wrapped handler saw: tenant and verified service actor.
    #[derive(Default)]
    struct Recording(std::sync::Mutex<Vec<(String, String)>>);

    #[async_trait::async_trait]
    impl MessageHandler for Recording {
        async fn handle(
            &self,
            message: Message,
        ) -> std::result::Result<MessageResult, ConsumerError> {
            use flare_server_core::grpc::middleware::signed_context::VerifiedContext;
            let ctx = &message.context.ctx;
            let actor = ctx
                .get_data::<VerifiedContext>()
                .and_then(|marker| marker.claims())
                .map(|claims| claims.actor_id.clone())
                .unwrap_or_default();
            self.0
                .lock()
                .unwrap()
                .push((ctx.tenant_id().unwrap_or_default().to_string(), actor));
            Ok(MessageResult::Ack)
        }

        fn name(&self) -> &str {
            "recording"
        }
    }

    fn record(tenant: Option<&str>) -> Message {
        use flare_server_core::context::Context;
        use flare_server_core::mq::consumer::{ContentType, MessageContext};
        let mut ctx = Context::with_request_id("req-1");
        if let Some(tenant) = tenant {
            ctx = ctx.with_tenant_id(tenant);
        }
        Message::new(
            Vec::new(),
            ContentType::Json,
            MessageContext::new(Arc::new(ctx), "flare.im.push.messages".into()),
        )
    }

    #[tokio::test]
    async fn attested_handler_signs_each_record_and_dead_letters_the_rest() {
        let (private, public) = key_pair();
        let identity = BrokerRecordIdentity::new(
            PushDeliveryMode::BrokerIsolated,
            "flare-push-server",
            Some(&private),
            Some(&public),
        )
        .unwrap();
        let recording = Arc::new(Recording::default());
        let handler = AttestedRecordHandler::new(identity, recording.clone());

        assert_eq!(
            handler.handle(record(Some("tenant-a"))).await.unwrap(),
            MessageResult::Ack
        );
        assert_eq!(
            handler.handle(record(None)).await.unwrap(),
            MessageResult::DeadLetter
        );
        let batch = handler
            .handle_batch(vec![
                record(Some("tenant-b")),
                record(Some(" bad")),
                record(Some("tenant-c")),
            ])
            .await
            .unwrap();
        assert_eq!(
            batch,
            vec![
                MessageResult::Ack,
                MessageResult::DeadLetter,
                MessageResult::Ack
            ]
        );
        let seen = recording.0.lock().unwrap().clone();
        assert_eq!(
            seen,
            ["tenant-a", "tenant-b", "tenant-c"]
                .map(|tenant| (tenant.to_string(), "flare-push-server".to_string()))
                .to_vec()
        );
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
