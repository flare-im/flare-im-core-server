//! Ed25519 proof for versioned Push broker records.
//!
//! This module does not authenticate an ingress request or authorize an IM
//! operation. Only call `PushRecordSigner::sign` after the producer has already
//! established the caller's identity, tenant, and right to publish the exact
//! payload. In particular, an MQ header or an unverified envelope is never
//! sufficient evidence to sign a derived record.
//!
//! Verification authenticates the record and binds it to the broker's *actual*
//! topic and key. It deliberately does not decode the inner payload. Consumers
//! must then compare its tenant and targets to the verified record before any
//! side effect, and durably deduplicate `PushReplayKey`. A valid signature can
//! otherwise be replayed repeatedly until expiry.
//! V1 only accepts dedicated topics ending in `.v1`; this prevents accidental
//! mixed-wire publication but does not replace broker ACLs, a hard consumer
//! cutover, or explicit handling of unsigned legacy backlog.

use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use ed25519_dalek::pkcs8::{DecodePrivateKey, DecodePublicKey};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use flare_proto::SignedPushRecordV1;
use prost::Message as _;
use sha2::{Digest, Sha256};

const VERSION: u32 = 1;
const SIGNING_DOMAIN: &[u8] = b"flare.im.push.signed-record.v1\0";
const MAX_WIRE_OVERHEAD_BYTES: usize = 8 * 1024;
pub const MAX_PUSH_RECORD_PAYLOAD_BYTES: usize = 10 * 1024 * 1024;
pub const MAX_PUSH_RECORD_TTL_MS: i64 = 24 * 60 * 60 * 1000;
pub const MAX_PUSH_RECORD_CLOCK_SKEW_MS: i64 = 30 * 1000;

#[derive(Debug, PartialEq, Eq)]
pub enum PushProvenanceError {
    InvalidField(&'static str),
    InvalidKeyWindow,
    SigningKeyInactive,
    UnknownKey,
    DuplicateKey,
    KeyReuseAcrossProducers,
    TopicNotAllowed,
    WrongTopic,
    WrongKey,
    WrongSchema,
    WrongTenant,
    NotYetValid,
    Expired,
    LifetimeTooLong,
    PayloadTooLarge,
    DigestMismatch,
    InvalidSignature,
    UnsupportedVersion,
    InvalidWire,
    Clock,
}

impl std::fmt::Display for PushProvenanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidField(field) => write!(f, "invalid signed push record field: {field}"),
            Self::InvalidKeyWindow => f.write_str("invalid push signing key window"),
            Self::SigningKeyInactive => f.write_str("push record signing key is not active"),
            Self::UnknownKey => f.write_str("push record key is unknown or revoked"),
            Self::DuplicateKey => f.write_str("push record key already registered"),
            Self::KeyReuseAcrossProducers => {
                f.write_str("push record public key cannot be shared by different producers")
            }
            Self::TopicNotAllowed => {
                f.write_str("push record key is not authorized for this topic")
            }
            Self::WrongTopic => {
                f.write_str("push record broker topic does not match the signed topic")
            }
            Self::WrongKey => f.write_str("push record broker key does not match the signed key"),
            Self::WrongSchema => f.write_str("push record payload schema does not match"),
            Self::WrongTenant => {
                f.write_str("push record body tenant does not match the signed tenant")
            }
            Self::NotYetValid => f.write_str("push record is not yet valid"),
            Self::Expired => f.write_str("push record has expired"),
            Self::LifetimeTooLong => f.write_str("push record lifetime exceeds the allowed window"),
            Self::PayloadTooLarge => f.write_str("push record payload exceeds the allowed size"),
            Self::DigestMismatch => f.write_str("push record payload digest does not match"),
            Self::InvalidSignature => f.write_str("invalid push record signature"),
            Self::UnsupportedVersion => f.write_str("unsupported signed push record version"),
            Self::InvalidWire => f.write_str("invalid or non-canonical signed push record wire"),
            Self::Clock => f.write_str("system clock is unavailable"),
        }
    }
}

impl std::error::Error for PushProvenanceError {}

pub type PushProvenanceResult<T> = Result<T, PushProvenanceError>;

/// Signing and verification overlap for key rotation. Keep old verification
/// keys until every outstanding signed record has expired and its replay
/// obligation has been discharged. Emergency revocation removes a key at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushKeyWindow {
    pub signing_from: i64,
    pub signing_until: i64,
    pub verify_until: i64,
}

impl PushKeyWindow {
    pub fn new(
        signing_from: i64,
        signing_until: i64,
        verify_until: i64,
    ) -> PushProvenanceResult<Self> {
        if signing_from <= 0 || signing_until < signing_from || verify_until < signing_until {
            return Err(PushProvenanceError::InvalidKeyWindow);
        }
        Ok(Self {
            signing_from,
            signing_until,
            verify_until,
        })
    }
}

/// The producer supplies these values only after its own authentication and
/// body authorization. This type itself is NOT an authorization credential.
pub struct PushRecordInput<'a> {
    pub logical_topic: &'a str,
    pub logical_key: &'a str,
    pub tenant_id: &'a str,
    pub event_id: &'a str,
    pub payload_schema: &'a str,
    pub source_event_id: Option<&'a str>,
    pub payload: &'a [u8],
}

/// Private-key capability. Never initialize this with the cell-internal gRPC
/// context key; broker provenance keys have their own rotation and ACL policy.
pub struct PushRecordSigner {
    producer_id: String,
    key_id: String,
    key: SigningKey,
    allowed_topics: HashSet<String>,
    window: PushKeyWindow,
}

impl PushRecordSigner {
    pub fn new(
        producer_id: impl Into<String>,
        key_id: impl Into<String>,
        key: SigningKey,
        allowed_topics: impl IntoIterator<Item = String>,
        window: PushKeyWindow,
    ) -> PushProvenanceResult<Self> {
        let producer_id = producer_id.into();
        let key_id = key_id.into();
        valid_identifier(&producer_id, "producer_id", 256)?;
        valid_identifier(&key_id, "key_id", 128)?;
        let allowed_topics = checked_topics(allowed_topics)?;
        Ok(Self {
            producer_id,
            key_id,
            key,
            allowed_topics,
            window,
        })
    }

    pub fn from_private_key_pem(
        producer_id: impl Into<String>,
        key_id: impl Into<String>,
        pem: &str,
        allowed_topics: impl IntoIterator<Item = String>,
        window: PushKeyWindow,
    ) -> PushProvenanceResult<Self> {
        let key = SigningKey::from_pkcs8_pem(pem)
            .map_err(|_| PushProvenanceError::InvalidField("private_key"))?;
        Self::new(producer_id, key_id, key, allowed_topics, window)
    }

    /// Returns canonical, encoded `SignedPushRecordV1` bytes. A caller holding
    /// this key can attest only for its configured exact topics.
    pub fn sign(&self, input: PushRecordInput<'_>, ttl_ms: i64) -> PushProvenanceResult<Vec<u8>> {
        self.sign_at(input, ttl_ms, system_time_ms()?)
    }

    fn sign_at(
        &self,
        input: PushRecordInput<'_>,
        ttl_ms: i64,
        issued_at: i64,
    ) -> PushProvenanceResult<Vec<u8>> {
        if !self.allowed_topics.contains(input.logical_topic) {
            return Err(PushProvenanceError::TopicNotAllowed);
        }
        if issued_at < self.window.signing_from || issued_at > self.window.signing_until {
            return Err(PushProvenanceError::SigningKeyInactive);
        }
        if ttl_ms <= 0 || ttl_ms > MAX_PUSH_RECORD_TTL_MS {
            return Err(PushProvenanceError::LifetimeTooLong);
        }
        let expires_at = issued_at
            .checked_add(ttl_ms)
            .ok_or(PushProvenanceError::LifetimeTooLong)?;
        if expires_at > self.window.verify_until {
            return Err(PushProvenanceError::InvalidKeyWindow);
        }
        let mut record = SignedPushRecordV1 {
            version: VERSION,
            logical_topic: input.logical_topic.to_string(),
            logical_key: input.logical_key.to_string(),
            tenant_id: input.tenant_id.to_string(),
            producer_id: self.producer_id.clone(),
            key_id: self.key_id.clone(),
            event_id: input.event_id.to_string(),
            payload_schema: input.payload_schema.to_string(),
            payload_sha256: Sha256::digest(input.payload).to_vec(),
            issued_at,
            expires_at,
            source_event_id: input.source_event_id.unwrap_or_default().to_string(),
            payload: input.payload.to_vec(),
            signature: Vec::new(),
        };
        validate_record_fields(&record)?;
        record.signature = self.key.sign(&signing_frame(&record)).to_bytes().to_vec();
        Ok(record.encode_to_vec())
    }
}

struct PublicKeyPolicy {
    key: VerifyingKey,
    allowed_topics: HashSet<String>,
    window: PushKeyWindow,
}

/// Public-key-only verifier. Keys are addressed by exact (producer_id, key_id)
/// and can be added for rotation or removed for emergency revocation. There is
/// no fallback key, algorithm negotiation, or wildcard topic authorization.
#[derive(Default)]
pub struct PushRecordVerifier {
    keys: HashMap<(String, String), PublicKeyPolicy>,
}

impl PushRecordVerifier {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_key(
        &mut self,
        producer_id: impl Into<String>,
        key_id: impl Into<String>,
        key: VerifyingKey,
        allowed_topics: impl IntoIterator<Item = String>,
        window: PushKeyWindow,
    ) -> PushProvenanceResult<()> {
        let producer_id = producer_id.into();
        let key_id = key_id.into();
        valid_identifier(&producer_id, "producer_id", 256)?;
        valid_identifier(&key_id, "key_id", 128)?;
        let allowed_topics = checked_topics(allowed_topics)?;
        let identity = (producer_id, key_id);
        if self.keys.contains_key(&identity) {
            return Err(PushProvenanceError::DuplicateKey);
        }
        if self.keys.iter().any(|((other_producer, _), policy)| {
            other_producer != &identity.0 && policy.key.as_bytes() == key.as_bytes()
        }) {
            return Err(PushProvenanceError::KeyReuseAcrossProducers);
        }
        self.keys.insert(
            identity,
            PublicKeyPolicy {
                key,
                allowed_topics,
                window,
            },
        );
        Ok(())
    }

    pub fn add_public_key_pem(
        &mut self,
        producer_id: impl Into<String>,
        key_id: impl Into<String>,
        pem: &str,
        allowed_topics: impl IntoIterator<Item = String>,
        window: PushKeyWindow,
    ) -> PushProvenanceResult<()> {
        let key = VerifyingKey::from_public_key_pem(pem)
            .map_err(|_| PushProvenanceError::InvalidField("public_key"))?;
        self.add_key(producer_id, key_id, key, allowed_topics, window)
    }

    pub fn revoke_key(&mut self, producer_id: &str, key_id: &str) -> bool {
        self.keys
            .remove(&(producer_id.to_string(), key_id.to_string()))
            .is_some()
    }

    /// Verify the raw wrapper before decoding its inner payload. `broker_topic`
    /// and `broker_key` MUST come from the broker's delivery metadata, never
    /// from this record's untrusted fields or application-level MQ headers.
    pub fn verify(
        &self,
        encoded_record: &[u8],
        broker_topic: &str,
        broker_key: Option<&str>,
        expected_schema: &str,
    ) -> PushProvenanceResult<VerifiedPushRecord> {
        self.verify_at(
            encoded_record,
            broker_topic,
            broker_key,
            expected_schema,
            system_time_ms()?,
        )
    }

    fn verify_at(
        &self,
        encoded_record: &[u8],
        broker_topic: &str,
        broker_key: Option<&str>,
        expected_schema: &str,
        now: i64,
    ) -> PushProvenanceResult<VerifiedPushRecord> {
        if encoded_record.len() > MAX_PUSH_RECORD_PAYLOAD_BYTES + MAX_WIRE_OVERHEAD_BYTES {
            return Err(PushProvenanceError::PayloadTooLarge);
        }
        let record = SignedPushRecordV1::decode(encoded_record)
            .map_err(|_| PushProvenanceError::InvalidWire)?;
        // Reject unknown fields, duplicate fields and alternative encodings:
        // all v1 producers must emit exactly prost's canonical wrapper bytes.
        if record.encode_to_vec().as_slice() != encoded_record {
            return Err(PushProvenanceError::InvalidWire);
        }
        validate_record_fields(&record)?;
        if record.logical_topic != broker_topic {
            return Err(PushProvenanceError::WrongTopic);
        }
        if broker_key != Some(record.logical_key.as_str()) {
            return Err(PushProvenanceError::WrongKey);
        }
        if record.payload_schema != expected_schema {
            return Err(PushProvenanceError::WrongSchema);
        }
        let lifetime = record
            .expires_at
            .checked_sub(record.issued_at)
            .ok_or(PushProvenanceError::LifetimeTooLong)?;
        if lifetime <= 0 || lifetime > MAX_PUSH_RECORD_TTL_MS {
            return Err(PushProvenanceError::LifetimeTooLong);
        }
        if record.issued_at > now.saturating_add(MAX_PUSH_RECORD_CLOCK_SKEW_MS) {
            return Err(PushProvenanceError::NotYetValid);
        }
        if now >= record.expires_at {
            return Err(PushProvenanceError::Expired);
        }
        let policy = self
            .keys
            .get(&(record.producer_id.clone(), record.key_id.clone()))
            .ok_or(PushProvenanceError::UnknownKey)?;
        if !policy.allowed_topics.contains(&record.logical_topic) {
            return Err(PushProvenanceError::TopicNotAllowed);
        }
        if record.issued_at < policy.window.signing_from
            || record.issued_at > policy.window.signing_until
            || now > policy.window.verify_until
        {
            return Err(PushProvenanceError::SigningKeyInactive);
        }
        let payload_digest: [u8; 32] = Sha256::digest(&record.payload).into();
        if payload_digest.as_slice() != record.payload_sha256.as_slice() {
            return Err(PushProvenanceError::DigestMismatch);
        }
        let signature = Signature::from_slice(&record.signature)
            .map_err(|_| PushProvenanceError::InvalidSignature)?;
        policy
            .key
            .verify_strict(&signing_frame(&record), &signature)
            .map_err(|_| PushProvenanceError::InvalidSignature)?;
        Ok(VerifiedPushRecord { record })
    }
}

/// Only obtainable from a successful public-key verification.
pub struct VerifiedPushRecord {
    record: SignedPushRecordV1,
}

impl VerifiedPushRecord {
    pub fn tenant_id(&self) -> &str {
        &self.record.tenant_id
    }

    pub fn producer_id(&self) -> &str {
        &self.record.producer_id
    }

    pub fn event_id(&self) -> &str {
        &self.record.event_id
    }

    pub fn source_event_id(&self) -> Option<&str> {
        (!self.record.source_event_id.is_empty()).then_some(&self.record.source_event_id)
    }

    pub fn payload(&self) -> &[u8] {
        &self.record.payload
    }

    /// Persist alongside `replay_key` so a reused event ID with different
    /// payload is treated as a conflict rather than a successful duplicate.
    pub fn payload_digest(&self) -> &[u8] {
        &self.record.payload_sha256
    }

    pub fn require_body_tenant(&self, body_tenant_id: &str) -> PushProvenanceResult<()> {
        if self.record.tenant_id == body_tenant_id {
            Ok(())
        } else {
            Err(PushProvenanceError::WrongTenant)
        }
    }

    /// Caller must use a durable, tenant-scoped store before external effects.
    /// Signature verification alone does not provide at-most-once delivery.
    pub fn replay_key(&self) -> PushReplayKey {
        PushReplayKey {
            tenant_id: self.record.tenant_id.clone(),
            logical_topic: self.record.logical_topic.clone(),
            logical_key: self.record.logical_key.clone(),
            producer_id: self.record.producer_id.clone(),
            event_id: self.record.event_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PushReplayKey {
    pub tenant_id: String,
    pub logical_topic: String,
    pub logical_key: String,
    pub producer_id: String,
    pub event_id: String,
}

fn checked_topics(
    topics: impl IntoIterator<Item = String>,
) -> PushProvenanceResult<HashSet<String>> {
    let mut checked = HashSet::new();
    for topic in topics {
        valid_identifier(&topic, "logical_topic", 256)?;
        if topic.contains('*') || topic.contains('>') || !topic.ends_with(".v1") {
            return Err(PushProvenanceError::InvalidField("allowed_topics"));
        }
        checked.insert(topic);
    }
    if checked.is_empty() {
        return Err(PushProvenanceError::InvalidField("allowed_topics"));
    }
    Ok(checked)
}

fn valid_identifier(value: &str, field: &'static str, max: usize) -> PushProvenanceResult<()> {
    if value.is_empty()
        || value.len() > max
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(PushProvenanceError::InvalidField(field));
    }
    Ok(())
}

fn validate_record_fields(record: &SignedPushRecordV1) -> PushProvenanceResult<()> {
    if record.version != VERSION {
        return Err(PushProvenanceError::UnsupportedVersion);
    }
    valid_identifier(&record.logical_topic, "logical_topic", 256)?;
    if record.logical_topic.contains('*')
        || record.logical_topic.contains('>')
        || !record.logical_topic.ends_with(".v1")
    {
        return Err(PushProvenanceError::InvalidField("logical_topic"));
    }
    valid_identifier(&record.logical_key, "logical_key", 512)?;
    valid_identifier(&record.tenant_id, "tenant_id", 256)?;
    if record.tenant_id == "*" {
        return Err(PushProvenanceError::InvalidField("tenant_id"));
    }
    valid_identifier(&record.producer_id, "producer_id", 256)?;
    valid_identifier(&record.key_id, "key_id", 128)?;
    valid_identifier(&record.event_id, "event_id", 256)?;
    valid_identifier(&record.payload_schema, "payload_schema", 256)?;
    if !record.source_event_id.is_empty() {
        valid_identifier(&record.source_event_id, "source_event_id", 256)?;
    }
    if record.issued_at <= 0 || record.expires_at <= 0 {
        return Err(PushProvenanceError::InvalidField("issued_at/expires_at"));
    }
    if record.payload.len() > MAX_PUSH_RECORD_PAYLOAD_BYTES {
        return Err(PushProvenanceError::PayloadTooLarge);
    }
    if record.payload.is_empty() {
        return Err(PushProvenanceError::InvalidField("payload"));
    }
    if record.payload_sha256.len() != 32 {
        return Err(PushProvenanceError::InvalidField("payload_sha256"));
    }
    if !record.signature.is_empty() && record.signature.len() != 64 {
        return Err(PushProvenanceError::InvalidSignature);
    }
    Ok(())
}

fn signing_frame(record: &SignedPushRecordV1) -> Vec<u8> {
    let mut frame = Vec::with_capacity(1024);
    frame.extend_from_slice(SIGNING_DOMAIN);
    frame.extend_from_slice(&record.version.to_be_bytes());
    for value in [
        record.logical_topic.as_bytes(),
        record.logical_key.as_bytes(),
        record.tenant_id.as_bytes(),
        record.producer_id.as_bytes(),
        record.key_id.as_bytes(),
        record.event_id.as_bytes(),
        record.payload_schema.as_bytes(),
        record.payload_sha256.as_slice(),
        record.source_event_id.as_bytes(),
    ] {
        frame.extend_from_slice(&(value.len() as u32).to_be_bytes());
        frame.extend_from_slice(value);
    }
    frame.extend_from_slice(&record.issued_at.to_be_bytes());
    frame.extend_from_slice(&record.expires_at.to_be_bytes());
    frame.extend_from_slice(&(record.payload.len() as u64).to_be_bytes());
    frame
}

fn system_time_ms() -> PushProvenanceResult<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PushProvenanceError::Clock)?;
    i64::try_from(duration.as_millis()).map_err(|_| PushProvenanceError::Clock)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_800_000_000_000;
    const TOPIC: &str = "flare.im.push.messages.v1";
    const SCHEMA: &str = "flare.common.v1.MqEnvelope";

    fn window() -> PushKeyWindow {
        PushKeyWindow::new(
            NOW - 1_000,
            NOW + MAX_PUSH_RECORD_TTL_MS,
            NOW + 2 * MAX_PUSH_RECORD_TTL_MS,
        )
        .unwrap()
    }

    fn signer() -> PushRecordSigner {
        PushRecordSigner::new(
            "message-ingest",
            "key-2026-09",
            SigningKey::from_bytes(&[7; 32]),
            [TOPIC.to_string()],
            window(),
        )
        .unwrap()
    }

    fn verifier() -> PushRecordVerifier {
        let mut verifier = PushRecordVerifier::new();
        verifier
            .add_key(
                "message-ingest",
                "key-2026-09",
                SigningKey::from_bytes(&[7; 32]).verifying_key(),
                [TOPIC.to_string()],
                window(),
            )
            .unwrap();
        verifier
    }

    fn input<'a>(payload: &'a [u8]) -> PushRecordInput<'a> {
        PushRecordInput {
            logical_topic: TOPIC,
            logical_key: "conversation-1",
            tenant_id: "tenant-a",
            event_id: "event-1",
            payload_schema: SCHEMA,
            source_event_id: Some("source-event-1"),
            payload,
        }
    }

    fn signed() -> Vec<u8> {
        signer()
            .sign_at(input(b"original bytes"), 60_000, NOW)
            .unwrap()
    }

    fn verify_at(
        verifier: &PushRecordVerifier,
        bytes: &[u8],
        now: i64,
    ) -> PushProvenanceResult<VerifiedPushRecord> {
        verifier.verify_at(bytes, TOPIC, Some("conversation-1"), SCHEMA, now)
    }

    #[test]
    fn authentic_record_binds_payload_and_tenant_but_needs_durable_replay_store() {
        let verifier = verifier();
        let bytes = signed();
        let first = verify_at(&verifier, &bytes, NOW).unwrap();
        let retry = verify_at(&verifier, &bytes, NOW + 1).unwrap();
        assert_eq!(first.payload(), b"original bytes");
        assert_eq!(first.tenant_id(), "tenant-a");
        assert_eq!(first.source_event_id(), Some("source-event-1"));
        assert_eq!(first.replay_key(), retry.replay_key());
        assert_eq!(
            first.require_body_tenant("tenant-b"),
            Err(PushProvenanceError::WrongTenant)
        );
    }

    #[test]
    fn broker_topic_key_and_schema_must_be_independent_exact_inputs() {
        let verifier = verifier();
        let bytes = signed();
        assert!(matches!(
            verifier.verify_at(
                &bytes,
                "flare.im.push.events.v1",
                Some("conversation-1"),
                SCHEMA,
                NOW
            ),
            Err(PushProvenanceError::WrongTopic)
        ));
        assert!(matches!(
            verifier.verify_at(&bytes, TOPIC, Some("other-key"), SCHEMA, NOW),
            Err(PushProvenanceError::WrongKey)
        ));
        assert!(matches!(
            verifier.verify_at(&bytes, TOPIC, None, SCHEMA, NOW),
            Err(PushProvenanceError::WrongKey)
        ));
        assert!(matches!(
            verifier.verify_at(
                &bytes,
                TOPIC,
                Some("conversation-1"),
                "flare.common.v1.PushEnvelope",
                NOW
            ),
            Err(PushProvenanceError::WrongSchema)
        ));
    }

    #[test]
    fn tampering_with_any_signed_claim_or_payload_fails() {
        let verifier = verifier();
        let original = SignedPushRecordV1::decode(signed().as_slice()).unwrap();
        let mut variants = Vec::new();
        let mut tenant = original.clone();
        tenant.tenant_id = "tenant-b".into();
        variants.push(tenant);
        let mut producer = original.clone();
        producer.producer_id = "push-proxy".into();
        variants.push(producer);
        let mut source = original.clone();
        source.source_event_id = "other-source".into();
        variants.push(source);
        let mut event = original.clone();
        event.event_id = "other-event".into();
        variants.push(event);
        let mut body = original.clone();
        body.payload = b"replacement".to_vec();
        variants.push(body);
        let mut digest = original.clone();
        digest.payload_sha256[0] ^= 1;
        variants.push(digest);
        for record in variants {
            assert!(verify_at(&verifier, &record.encode_to_vec(), NOW).is_err());
        }
    }

    #[test]
    fn empty_identity_fields_and_wrong_version_fail_closed() {
        let verifier = verifier();
        let original = SignedPushRecordV1::decode(signed().as_slice()).unwrap();
        for field in [
            "tenant_id",
            "producer_id",
            "key_id",
            "event_id",
            "payload_schema",
            "logical_key",
        ] {
            let mut record = original.clone();
            match field {
                "tenant_id" => record.tenant_id.clear(),
                "producer_id" => record.producer_id.clear(),
                "key_id" => record.key_id.clear(),
                "event_id" => record.event_id.clear(),
                "payload_schema" => record.payload_schema.clear(),
                "logical_key" => record.logical_key.clear(),
                _ => unreachable!(),
            }
            assert!(
                verify_at(&verifier, &record.encode_to_vec(), NOW).is_err(),
                "{field} must be required"
            );
        }
        let mut wrong_version = original.clone();
        wrong_version.version = 2;
        assert!(matches!(
            verify_at(&verifier, &wrong_version.encode_to_vec(), NOW),
            Err(PushProvenanceError::UnsupportedVersion)
        ));
        let mut bad_signature = original;
        bad_signature.signature[0] ^= 1;
        assert!(matches!(
            verify_at(&verifier, &bad_signature.encode_to_vec(), NOW),
            Err(PushProvenanceError::InvalidSignature)
        ));
    }

    #[test]
    fn expiry_future_issue_and_excessive_lifetime_fail() {
        let verifier = verifier();
        let bytes = signed();
        assert!(matches!(
            verify_at(&verifier, &bytes, NOW + 60_000),
            Err(PushProvenanceError::Expired)
        ));
        assert!(matches!(
            verify_at(&verifier, &bytes, NOW - MAX_PUSH_RECORD_CLOCK_SKEW_MS - 1),
            Err(PushProvenanceError::NotYetValid)
        ));
        let mut excessive = SignedPushRecordV1::decode(bytes.as_slice()).unwrap();
        excessive.expires_at = excessive.issued_at + MAX_PUSH_RECORD_TTL_MS + 1;
        assert!(matches!(
            verify_at(&verifier, &excessive.encode_to_vec(), NOW),
            Err(PushProvenanceError::LifetimeTooLong)
        ));
        assert_eq!(
            signer().sign_at(input(b"body"), MAX_PUSH_RECORD_TTL_MS + 1, NOW),
            Err(PushProvenanceError::LifetimeTooLong)
        );
    }

    #[test]
    fn key_rotation_is_exact_and_emergency_revocation_is_immediate() {
        let bytes = signed();
        let mut verifier = PushRecordVerifier::new();
        assert!(matches!(
            verify_at(&verifier, &bytes, NOW),
            Err(PushProvenanceError::UnknownKey)
        ));
        verifier
            .add_key(
                "message-ingest",
                "key-2026-10",
                SigningKey::from_bytes(&[9; 32]).verifying_key(),
                [TOPIC.to_string()],
                window(),
            )
            .unwrap();
        assert!(matches!(
            verify_at(&verifier, &bytes, NOW),
            Err(PushProvenanceError::UnknownKey)
        ));
        verifier
            .add_key(
                "message-ingest",
                "key-2026-09",
                SigningKey::from_bytes(&[7; 32]).verifying_key(),
                [TOPIC.to_string()],
                window(),
            )
            .unwrap();
        assert!(verify_at(&verifier, &bytes, NOW).is_ok());
        assert!(verifier.revoke_key("message-ingest", "key-2026-09"));
        assert!(matches!(
            verify_at(&verifier, &bytes, NOW),
            Err(PushProvenanceError::UnknownKey)
        ));
    }

    #[test]
    fn keyring_enforces_exact_topic_and_rotation_window() {
        let bytes = signed();
        let mut verifier = PushRecordVerifier::new();
        verifier
            .add_key(
                "message-ingest",
                "key-2026-09",
                SigningKey::from_bytes(&[7; 32]).verifying_key(),
                ["flare.im.push.events.v1".to_string()],
                window(),
            )
            .unwrap();
        assert!(matches!(
            verify_at(&verifier, &bytes, NOW),
            Err(PushProvenanceError::TopicNotAllowed)
        ));

        verifier.revoke_key("message-ingest", "key-2026-09");
        let expired_window = PushKeyWindow::new(NOW - 1_000, NOW - 1, NOW + 60_000).unwrap();
        verifier
            .add_key(
                "message-ingest",
                "key-2026-09",
                SigningKey::from_bytes(&[7; 32]).verifying_key(),
                [TOPIC.to_string()],
                expired_window,
            )
            .unwrap();
        assert!(matches!(
            verify_at(&verifier, &bytes, NOW),
            Err(PushProvenanceError::SigningKeyInactive)
        ));
    }

    #[test]
    fn one_public_key_cannot_impersonate_two_producer_identities() {
        let mut verifier = verifier();
        assert_eq!(
            verifier.add_key(
                "other-producer",
                "other-kid",
                SigningKey::from_bytes(&[7; 32]).verifying_key(),
                [TOPIC.to_string()],
                window(),
            ),
            Err(PushProvenanceError::KeyReuseAcrossProducers)
        );
    }

    #[test]
    fn noncanonical_or_unknown_wire_fields_are_rejected() {
        let verifier = verifier();
        let mut bytes = signed();
        // Unknown varint field 100, value 1.
        bytes.extend_from_slice(&[0xa0, 0x06, 0x01]);
        assert!(matches!(
            verify_at(&verifier, &bytes, NOW),
            Err(PushProvenanceError::InvalidWire)
        ));
        let mut bytes = signed();
        // Duplicate version field 1, value 1.
        bytes.extend_from_slice(&[0x08, 0x01]);
        assert!(matches!(
            verify_at(&verifier, &bytes, NOW),
            Err(PushProvenanceError::InvalidWire)
        ));
    }

    #[test]
    fn signer_and_keyring_reject_wildcards_empty_identity_and_unauthorized_topics() {
        assert!(matches!(
            PushRecordSigner::new(
                "",
                "k",
                SigningKey::from_bytes(&[7; 32]),
                [TOPIC.to_string()],
                window()
            ),
            Err(PushProvenanceError::InvalidField("producer_id"))
        ));
        assert!(matches!(
            PushRecordSigner::new(
                "p",
                "k",
                SigningKey::from_bytes(&[7; 32]),
                ["flare.im.push.*".to_string()],
                window()
            ),
            Err(PushProvenanceError::InvalidField("allowed_topics"))
        ));
        assert!(matches!(
            PushRecordSigner::new(
                "p",
                "k",
                SigningKey::from_bytes(&[7; 32]),
                ["flare.im.push.messages".to_string()],
                window()
            ),
            Err(PushProvenanceError::InvalidField("allowed_topics"))
        ));
        let mut unauthorized = input(b"body");
        unauthorized.logical_topic = "flare.im.push.events.v1";
        assert_eq!(
            signer().sign_at(unauthorized, 60_000, NOW),
            Err(PushProvenanceError::TopicNotAllowed)
        );
    }
}
