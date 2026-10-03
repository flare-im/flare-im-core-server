use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::Ctx;
use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use super::super::config::HookDefinition;
use super::super::types::{
    DeliveryEvent, DeliveryHook, HookOutcome, MessageDraft, MessageRecord, PostSendHook,
    PreSendDecision, PreSendHook, RecallEvent, RecallHook, pre_send_rejection,
};

#[derive(Clone)]
pub struct WebhookHookFactory {
    client: Client,
}

impl WebhookHookFactory {
    pub fn new() -> Result<Self> {
        let builder = Client::builder().use_rustls_tls();
        // Unit tests deliver to receivers on the loopback interface: in test builds the client
        // ignores HTTP(S)_PROXY / ALL_PROXY and the system proxy settings, which would otherwise
        // route loopback deliveries through a developer's proxy. Release builds are unchanged.
        #[cfg(test)]
        let builder = builder.no_proxy();
        let client = builder.build().map_err(|err| {
            ErrorBuilder::new(ErrorCode::ConfigurationError, "failed to build http client")
                .details(err.to_string())
                .build_error()
        })?;
        Ok(Self { client })
    }

    pub fn build_pre_send(
        &self,
        def: &HookDefinition,
        endpoint: &str,
        secret: Option<String>,
        headers: HashMap<String, String>,
    ) -> Arc<dyn PreSendHook> {
        Arc::new(WebhookPreSendHook {
            client: self.client.clone(),
            endpoint: endpoint.to_string(),
            secret,
            headers,
            static_metadata: def.metadata.clone(),
        })
    }

    pub fn build_post_send(
        &self,
        def: &HookDefinition,
        endpoint: &str,
        secret: Option<String>,
        headers: HashMap<String, String>,
    ) -> Arc<dyn PostSendHook> {
        Arc::new(WebhookPostSendHook {
            client: self.client.clone(),
            endpoint: endpoint.to_string(),
            secret,
            headers,
            static_metadata: def.metadata.clone(),
        })
    }

    pub fn build_delivery(
        &self,
        def: &HookDefinition,
        endpoint: &str,
        secret: Option<String>,
        headers: HashMap<String, String>,
    ) -> Arc<dyn DeliveryHook> {
        Arc::new(WebhookDeliveryHook {
            client: self.client.clone(),
            endpoint: endpoint.to_string(),
            secret,
            headers,
            static_metadata: def.metadata.clone(),
        })
    }

    pub fn build_recall(
        &self,
        def: &HookDefinition,
        endpoint: &str,
        secret: Option<String>,
        headers: HashMap<String, String>,
    ) -> Arc<dyn RecallHook> {
        Arc::new(WebhookRecallHook {
            client: self.client.clone(),
            endpoint: endpoint.to_string(),
            secret,
            headers,
            static_metadata: def.metadata.clone(),
        })
    }
}

#[derive(Serialize)]
struct WebhookContextPayload {
    tenant_id: String,
    conversation_id: Option<String>,
    conversation_type: Option<String>,
    message_type: Option<String>,
    sender_id: Option<String>,
    trace_id: Option<String>,
    tags: HashMap<String, String>,
    attributes: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct WebhookDraftPayload {
    message_id: Option<String>,
    client_message_id: Option<String>,
    conversation_id: Option<String>,
    payload: String,
    headers: HashMap<String, String>,
    metadata: HashMap<String, String>,
}

impl From<&MessageDraft> for WebhookDraftPayload {
    fn from(value: &MessageDraft) -> Self {
        Self {
            message_id: value.message_id.clone(),
            client_message_id: value.client_message_id.clone(),
            conversation_id: value.conversation_id.clone(),
            payload: STANDARD.encode(&value.payload),
            headers: value.headers.clone(),
            metadata: value.metadata.clone(),
        }
    }
}

impl WebhookDraftPayload {
    fn apply_to(self, draft: &mut MessageDraft) -> Result<()> {
        if let Some(id) = self.message_id {
            draft.message_id = Some(id);
        }
        if let Some(id) = self.client_message_id {
            draft.client_message_id = Some(id);
        }
        if let Some(conv) = self.conversation_id {
            draft.conversation_id = Some(conv);
        }
        draft.payload = STANDARD.decode(self.payload).map_err(|err| {
            ErrorBuilder::new(
                ErrorCode::DeserializationError,
                "invalid payload encoding from webhook",
            )
            .details(err.to_string())
            .build_error()
        })?;
        draft.headers = self.headers;
        draft.metadata = self.metadata;
        Ok(())
    }
}

#[derive(Serialize)]
struct PreSendWebhookRequest {
    context: WebhookContextPayload,
    draft: WebhookDraftPayload,
    metadata: HashMap<String, String>,
}

#[derive(Deserialize)]
struct PreSendWebhookResponse {
    allow: bool,
    draft: Option<WebhookDraftPayload>,
    #[serde(default)]
    status: Option<WebhookStatus>,
}

#[derive(Deserialize)]
struct WebhookStatus {
    code: Option<String>,
    message: Option<String>,
}

/// HTTP hook signature header (SEC-06, `docs/HOOK-PLUGIN-CONTRACT.md` §7):
/// `Flare-Signature: t=<unix seconds>,v1=<hex(HMAC-SHA256(secret, t + "." + raw body))>`.
pub const WEBHOOK_SIGNATURE_HEADER: &str = "Flare-Signature";

/// Clock skew (seconds) a receiver accepts on the signed timestamp: the replay window. The
/// receiver example in the contract uses the same value.
pub const WEBHOOK_SIGNATURE_TOLERANCE_SECS: u64 = 300;

/// Header names custom transport headers cannot use: the signature header, the removed
/// plaintext header, the old body-only signature header without a timestamp, and Content-Type
/// (the signature covers the raw bytes sent as JSON).
const RESERVED_WEBHOOK_HEADERS: [&str; 4] = [
    "flare-signature",
    "x-flare-signature",
    "x-hook-signature",
    "content-type",
];

/// Whether a custom webhook header `name` is reserved (one of the signature headers or
/// Content-Type, compared case-insensitively). Configuration loaders reject such headers when
/// the configuration is loaded or written; [`sign_webhook_request`] refuses them as well.
pub fn is_reserved_webhook_header(name: &str) -> bool {
    RESERVED_WEBHOOK_HEADERS
        .iter()
        .any(|reserved| name.eq_ignore_ascii_case(reserved))
}

fn signed_request<T: Serialize>(
    request_builder: reqwest::RequestBuilder,
    secret: &Option<String>,
    headers: &HashMap<String, String>,
    payload: &T,
) -> Result<reqwest::RequestBuilder> {
    // Serialize once: the signature covers exactly the bytes sent, including map order.
    let body = serde_json::to_vec(payload).map_err(|_| {
        ErrorBuilder::new(
            ErrorCode::SerializationError,
            "failed to serialize hook request",
        )
        .build_error()
    })?;
    sign_webhook_request(request_builder, secret.as_deref(), headers, body)
}

/// Adds the custom headers, `Content-Type: application/json` and, when a secret is
/// configured, [`WEBHOOK_SIGNATURE_HEADER`] to one HTTP hook delivery whose body is `body`:
/// the signature covers exactly the bytes sent.
///
/// The secret itself never appears in the request. A custom header named like one of
/// [`RESERVED_WEBHOOK_HEADERS`], or an empty secret, is a `ConfigurationError`. Core's webhook
/// adapter (Message Ingest and others) and the Capability service's own webhook adapter both
/// sign here, so both paths follow the same contract.
pub fn sign_webhook_request(
    request_builder: reqwest::RequestBuilder,
    secret: Option<&str>,
    headers: &HashMap<String, String>,
    body: Vec<u8>,
) -> Result<reqwest::RequestBuilder> {
    let mut builder = request_builder;
    for (key, value) in headers {
        if is_reserved_webhook_header(key) {
            return Err(ErrorBuilder::new(
                ErrorCode::ConfigurationError,
                "hook transport headers cannot override signature or content type",
            )
            .build_error());
        }
        builder = builder.header(key, value);
    }
    builder = builder.header("content-type", "application/json");
    if let Some(secret) = secret {
        if secret.is_empty() {
            return Err(ErrorBuilder::new(
                ErrorCode::ConfigurationError,
                "hook signing secret is empty",
            )
            .build_error());
        }
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| {
                ErrorBuilder::new(
                    ErrorCode::ConfigurationError,
                    "system clock precedes Unix epoch",
                )
                .build_error()
            })?
            .as_secs();
        builder = builder.header(
            WEBHOOK_SIGNATURE_HEADER,
            webhook_signature(secret.as_bytes(), timestamp, &body),
        );
    }
    Ok(builder.body(body))
}

/// The [`WEBHOOK_SIGNATURE_HEADER`] value: `t=<timestamp>,v1=<hex(HMAC-SHA256(secret, t "." body))>`.
pub fn webhook_signature(secret: &[u8], timestamp: u64, body: &[u8]) -> String {
    format!(
        "t={timestamp},v1={}",
        hex::encode(
            signature_mac(secret, timestamp, body)
                .finalize()
                .into_bytes()
        )
    )
}

fn signature_mac(secret: &[u8], timestamp: u64, body: &[u8]) -> Hmac<Sha256> {
    // HMAC accepts keys of any length; configuration rejects empty secrets above.
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret).expect("HMAC supports arbitrary key lengths");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    mac
}

/// Why a receiver rejects a [`WEBHOOK_SIGNATURE_HEADER`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookSignatureError {
    /// Not `t=<decimal seconds>,v1=<64 hex digits>`.
    Malformed,
    /// The timestamp is further from the receiver clock than the tolerance (outside the replay
    /// window); the MAC is not checked.
    OutsideTolerance { timestamp: u64 },
    /// The MAC does not match this secret, timestamp and raw body (another secret, or an
    /// altered body or timestamp).
    Mismatch,
}

impl std::fmt::Display for WebhookSignatureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed => f.write_str("malformed webhook signature"),
            Self::OutsideTolerance { timestamp } => {
                write!(
                    f,
                    "webhook signature timestamp {timestamp} outside tolerance"
                )
            }
            Self::Mismatch => f.write_str("webhook signature mismatch"),
        }
    }
}

impl std::error::Error for WebhookSignatureError {}

/// Receiver example: verifies one delivery with the receiver's own secret and returns the signed
/// timestamp.
///
/// Checks the shape and the timestamp first (more than `tolerance_secs` away from `now` is
/// rejected, so a captured delivery cannot be replayed after the window), then compares the MAC
/// over the **raw body bytes** (before JSON parsing) in constant time. A duplicate inside the
/// window still verifies; receivers should also deduplicate side effects by message or event ID.
pub fn verify_webhook_signature(
    secret: &[u8],
    body: &[u8],
    header: &str,
    now: u64,
    tolerance_secs: u64,
) -> std::result::Result<u64, WebhookSignatureError> {
    let (timestamp, mac) = header
        .split_once(',')
        .and_then(|(t, v1)| Some((t.strip_prefix("t=")?, v1.strip_prefix("v1=")?)))
        .ok_or(WebhookSignatureError::Malformed)?;
    if timestamp.is_empty() || !timestamp.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(WebhookSignatureError::Malformed);
    }
    let timestamp: u64 = timestamp
        .parse()
        .map_err(|_| WebhookSignatureError::Malformed)?;
    if mac.len() != 64 {
        return Err(WebhookSignatureError::Malformed);
    }
    let mac = hex::decode(mac).map_err(|_| WebhookSignatureError::Malformed)?;
    if now.abs_diff(timestamp) > tolerance_secs {
        return Err(WebhookSignatureError::OutsideTolerance { timestamp });
    }
    signature_mac(secret, timestamp, body)
        .verify_slice(&mac)
        .map(|()| timestamp)
        .map_err(|_| WebhookSignatureError::Mismatch)
}

#[cfg(test)]
mod signature_tests {
    use super::*;

    #[test]
    fn signature_matches_independent_hmac_sha256_vector() {
        assert_eq!(
            webhook_signature(b"test-secret", 1_700_000_000, br#"{"message":"hello"}"#),
            "t=1700000000,v1=543eae34d92c82da22df2744e174c470175e5d72cec71947502699d5cb85672a"
        );
    }

    #[test]
    fn signature_binds_body_timestamp_and_secret() {
        let original = webhook_signature(b"a-secret", 100, b"body");
        assert_ne!(original, webhook_signature(b"a-secret", 101, b"body"));
        assert_ne!(original, webhook_signature(b"a-secret", 100, b"tampered"));
        assert_ne!(original, webhook_signature(b"other-secret", 100, b"body"));
    }

    #[test]
    fn outbound_signature_covers_exact_body_and_never_contains_secret() {
        let secret = "not-in-the-headers";
        let request = signed_request(
            Client::new().post("http://127.0.0.1/hook"),
            &Some(secret.into()),
            &HashMap::new(),
            &serde_json::json!({"message":"hello"}),
        )
        .unwrap()
        .build()
        .unwrap();
        let header = request.headers()["flare-signature"].to_str().unwrap();
        let timestamp = header
            .split(',')
            .next()
            .unwrap()
            .strip_prefix("t=")
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(
            header,
            webhook_signature(
                secret.as_bytes(),
                timestamp,
                request.body().unwrap().as_bytes().unwrap()
            )
        );
        assert!(!header.contains(secret));
        assert!(!request.headers().contains_key("x-flare-signature"));
        assert!(!request.headers().contains_key("x-hook-signature"));
    }

    #[test]
    fn reserved_headers_cannot_reintroduce_plaintext_or_override_signature() {
        for header in [
            "X-Flare-Signature",
            "FLARE-SIGNATURE",
            "X-Hook-Signature",
            "Content-Type",
        ] {
            let headers = HashMap::from([(header.to_owned(), "secret".to_owned())]);
            assert!(
                signed_request(
                    Client::new().post("http://127.0.0.1/hook"),
                    &Some("secret".into()),
                    &headers,
                    &()
                )
                .is_err(),
                "{header}"
            );
        }
        assert!(
            signed_request(
                Client::new().post("http://127.0.0.1/hook"),
                &Some(String::new()),
                &HashMap::new(),
                &()
            )
            .is_err()
        );
    }

    #[test]
    fn receiver_accepts_a_fresh_signature_and_rejects_one_outside_the_replay_window() {
        let body = br#"{"message":"hello"}"#;
        let header = webhook_signature(b"own", 1_000, body);
        let tolerance = WEBHOOK_SIGNATURE_TOLERANCE_SECS;
        assert_eq!(
            verify_webhook_signature(b"own", body, &header, 1_000, tolerance),
            Ok(1_000)
        );
        assert_eq!(
            verify_webhook_signature(b"own", body, &header, 1_000 + tolerance, tolerance),
            Ok(1_000)
        );
        assert_eq!(
            verify_webhook_signature(b"own", body, &header, 1_000 - tolerance, tolerance),
            Ok(1_000)
        );
        // A captured delivery replayed after the window, or a clock far behind, is rejected.
        for now in [1_000 + tolerance + 1, 1_000 - tolerance - 1] {
            assert_eq!(
                verify_webhook_signature(b"own", body, &header, now, tolerance),
                Err(WebhookSignatureError::OutsideTolerance { timestamp: 1_000 })
            );
        }
        // Moving the timestamp into the window breaks the MAC: the timestamp is signed.
        let (_, mac) = header.split_once(',').unwrap();
        let refreshed = format!("t=1600,{mac}");
        assert_eq!(
            verify_webhook_signature(b"own", body, &refreshed, 1_600, tolerance),
            Err(WebhookSignatureError::Mismatch)
        );
        assert_eq!(
            verify_webhook_signature(b"other", body, &header, 1_000, tolerance),
            Err(WebhookSignatureError::Mismatch)
        );
        assert_eq!(
            verify_webhook_signature(b"own", b"{}", &header, 1_000, tolerance),
            Err(WebhookSignatureError::Mismatch)
        );
        for malformed in [
            "",
            "sha256=00",
            "t=1000",
            "v1=00,t=1000",
            "t=-1,v1=00",
            "t=+1000,v1=543eae34d92c82da22df2744e174c470175e5d72cec71947502699d5cb85672a",
            "t=1000,v1=zz",
            "t=1000,v1=543e",
        ] {
            assert_eq!(
                verify_webhook_signature(b"own", body, malformed, 1_000, tolerance),
                Err(WebhookSignatureError::Malformed),
                "{malformed}"
            );
        }
    }
}

/// The delivery's context. A context without a tenant is refused instead of being sent as
/// tenant "0": the receiver would act on another tenant's data (W11-T).
fn webhook_context(ctx: &Ctx) -> Result<WebhookContextPayload> {
    use crate::hooks::hook_context_data::get_hook_context_data;

    let tenant_id = ctx
        .tenant_id()
        .filter(|tenant| !tenant.trim().is_empty())
        .ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "webhook hook context has no tenant",
            )
            .build_error()
        })?
        .to_string();
    let hook_data = get_hook_context_data(ctx).cloned().unwrap_or_default();
    let trace_id = {
        let trace_id = ctx.trace_id();
        if trace_id.is_empty() {
            None
        } else {
            Some(trace_id.to_string())
        }
    };

    Ok(WebhookContextPayload {
        tenant_id,
        conversation_id: hook_data.conversation_id,
        conversation_type: hook_data.conversation_type,
        message_type: hook_data.message_type,
        sender_id: hook_data.sender_id,
        trace_id,
        tags: hook_data.tags,
        attributes: hook_data.attributes,
    })
}

#[derive(Clone)]
struct WebhookPreSendHook {
    client: Client,
    endpoint: String,
    secret: Option<String>,
    headers: HashMap<String, String>,
    static_metadata: HashMap<String, String>,
}

#[async_trait]
impl PreSendHook for WebhookPreSendHook {
    async fn handle(&self, ctx: &Ctx, draft: &mut MessageDraft) -> PreSendDecision {
        let context = match webhook_context(ctx) {
            Ok(context) => context,
            Err(error) => return PreSendDecision::Reject { error },
        };
        let request_body = PreSendWebhookRequest {
            context,
            draft: WebhookDraftPayload::from(&*draft),
            metadata: self.static_metadata.clone(),
        };

        let builder = self.client.post(&self.endpoint);
        let builder = match signed_request(builder, &self.secret, &self.headers, &request_body) {
            Ok(builder) => builder,
            Err(error) => return PreSendDecision::Reject { error },
        };
        let response = builder.send().await;

        match response {
            Ok(resp) => match resp.json::<PreSendWebhookResponse>().await {
                Ok(payload) => {
                    if payload.allow {
                        if let Some(draft_payload) = payload.draft
                            && let Err(err) = draft_payload.apply_to(draft)
                        {
                            return PreSendDecision::Reject { error: err };
                        }
                        PreSendDecision::Continue
                    } else {
                        let err = payload
                            .status
                            .map(|status| {
                                let code = status.code.unwrap_or_else(|| "BusinessRejected".into());
                                let message = status
                                    .message
                                    .unwrap_or_else(|| "rejected by webhook".into());
                                ErrorBuilder::new(ErrorCode::OperationFailed, &message)
                                    .details(code)
                            })
                            .unwrap_or_else(|| {
                                ErrorBuilder::new(
                                    ErrorCode::OperationFailed,
                                    "webhook rejected message",
                                )
                            });
                        PreSendDecision::Reject {
                            error: pre_send_rejection(err),
                        }
                    }
                }
                Err(err) => PreSendDecision::Reject {
                    error: ErrorBuilder::new(
                        ErrorCode::DeserializationError,
                        "failed to decode webhook response",
                    )
                    .details(err.to_string())
                    .build_error(),
                },
            },
            Err(err) => PreSendDecision::Reject {
                error: ErrorBuilder::new(ErrorCode::ServiceUnavailable, "webhook request failed")
                    .details(err.to_string())
                    .build_error(),
            },
        }
    }
}

#[derive(Serialize)]
struct PostSendWebhookRequest {
    context: WebhookContextPayload,
    record: MessageRecord,
    draft: WebhookDraftPayload,
    metadata: HashMap<String, String>,
}

#[derive(Clone)]
struct WebhookPostSendHook {
    client: Client,
    endpoint: String,
    secret: Option<String>,
    headers: HashMap<String, String>,
    static_metadata: HashMap<String, String>,
}

#[async_trait]
impl PostSendHook for WebhookPostSendHook {
    async fn handle(&self, ctx: &Ctx, record: &MessageRecord, draft: &MessageDraft) -> HookOutcome {
        let context = match webhook_context(ctx) {
            Ok(context) => context,
            Err(error) => return HookOutcome::Failed(error),
        };
        let request_body = PostSendWebhookRequest {
            context,
            record: record.clone(),
            draft: WebhookDraftPayload::from(draft),
            metadata: self.static_metadata.clone(),
        };

        let builder = self.client.post(&self.endpoint);
        let builder = match signed_request(builder, &self.secret, &self.headers, &request_body) {
            Ok(builder) => builder,
            Err(error) => return HookOutcome::Failed(error),
        };
        match builder.send().await {
            Ok(resp) if resp.status().is_success() => HookOutcome::Completed,
            Ok(resp) => {
                let err =
                    ErrorBuilder::new(ErrorCode::ServiceUnavailable, "webhook post-send failed")
                        .details(resp.status().to_string())
                        .build_error();
                HookOutcome::Failed(err)
            }
            Err(err) => {
                let err =
                    ErrorBuilder::new(ErrorCode::ServiceUnavailable, "webhook post-send failed")
                        .details(err.to_string())
                        .build_error();
                HookOutcome::Failed(err)
            }
        }
    }
}

#[derive(Serialize)]
struct DeliveryWebhookRequest {
    context: WebhookContextPayload,
    event: DeliveryEvent,
    metadata: HashMap<String, String>,
}

#[derive(Clone)]
struct WebhookDeliveryHook {
    client: Client,
    endpoint: String,
    secret: Option<String>,
    headers: HashMap<String, String>,
    static_metadata: HashMap<String, String>,
}

#[async_trait]
impl DeliveryHook for WebhookDeliveryHook {
    async fn handle(&self, ctx: &Ctx, event: &DeliveryEvent) -> HookOutcome {
        let context = match webhook_context(ctx) {
            Ok(context) => context,
            Err(error) => return HookOutcome::Failed(error),
        };
        let request_body = DeliveryWebhookRequest {
            context,
            event: event.clone(),
            metadata: self.static_metadata.clone(),
        };
        let builder = self.client.post(&self.endpoint);
        let builder = match signed_request(builder, &self.secret, &self.headers, &request_body) {
            Ok(builder) => builder,
            Err(error) => return HookOutcome::Failed(error),
        };
        match builder.send().await {
            Ok(resp) if resp.status().is_success() => HookOutcome::Completed,
            Ok(resp) => {
                let err =
                    ErrorBuilder::new(ErrorCode::ServiceUnavailable, "webhook delivery failed")
                        .details(resp.status().to_string())
                        .build_error();
                HookOutcome::Failed(err)
            }
            Err(err) => {
                let err =
                    ErrorBuilder::new(ErrorCode::ServiceUnavailable, "webhook delivery failed")
                        .details(err.to_string())
                        .build_error();
                HookOutcome::Failed(err)
            }
        }
    }
}

#[derive(Serialize)]
struct RecallWebhookRequest {
    context: WebhookContextPayload,
    event: RecallEvent,
    metadata: HashMap<String, String>,
}

#[derive(Clone)]
struct WebhookRecallHook {
    client: Client,
    endpoint: String,
    secret: Option<String>,
    headers: HashMap<String, String>,
    static_metadata: HashMap<String, String>,
}

#[async_trait]
impl RecallHook for WebhookRecallHook {
    async fn handle(&self, ctx: &Ctx, event: &RecallEvent) -> HookOutcome {
        let context = match webhook_context(ctx) {
            Ok(context) => context,
            Err(error) => return HookOutcome::Failed(error),
        };
        let request_body = RecallWebhookRequest {
            context,
            event: event.clone(),
            metadata: self.static_metadata.clone(),
        };
        let builder = self.client.post(&self.endpoint);
        let builder = match signed_request(builder, &self.secret, &self.headers, &request_body) {
            Ok(builder) => builder,
            Err(error) => return HookOutcome::Failed(error),
        };

        match builder.send().await {
            Ok(resp) if resp.status().is_success() => HookOutcome::Completed,
            Ok(resp) => {
                let err = ErrorBuilder::new(ErrorCode::ServiceUnavailable, "webhook recall failed")
                    .details(resp.status().to_string())
                    .build_error();
                HookOutcome::Failed(err)
            }
            Err(err) => {
                let err = ErrorBuilder::new(ErrorCode::ServiceUnavailable, "webhook recall failed")
                    .details(err.to_string())
                    .build_error();
                HookOutcome::Failed(err)
            }
        }
    }
}

#[cfg(test)]
mod tenant_tests {
    use super::*;
    use flare_server_core::context::Context;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::net::TcpListener;

    /// A delivery is never sent as tenant "0" when the context has no tenant: the hook refuses
    /// before connecting. With a tenant the same hook does connect.
    #[tokio::test]
    async fn a_context_without_a_tenant_is_refused_before_anything_is_sent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/hook", listener.local_addr().unwrap());
        // Count connections and drop them at once, so a request that is sent fails fast.
        let connections = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&connections);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                counted.fetch_add(1, Ordering::SeqCst);
                drop(stream);
            }
        });
        let factory = WebhookHookFactory::new().unwrap();
        let definition = HookDefinition::default();
        let pre_send = factory.build_pre_send(
            &definition,
            &endpoint,
            Some("secret".into()),
            HashMap::new(),
        );
        let post_send = factory.build_post_send(&definition, &endpoint, None, HashMap::new());
        let bounded = Duration::from_secs(5);

        for ctx in [Context::root(), Context::root().with_tenant_id(" ")] {
            let ctx: Ctx = Arc::new(ctx);
            let mut draft = MessageDraft::new(b"hello".to_vec());
            match tokio::time::timeout(bounded, pre_send.handle(&ctx, &mut draft))
                .await
                .expect("pre-send hung")
            {
                PreSendDecision::Reject { error } => {
                    assert_eq!(error.code(), Some(ErrorCode::InvalidParameter), "{error}")
                }
                PreSendDecision::Continue => panic!("pre-send without a tenant was allowed"),
            }
            let record = MessageRecord {
                message_id: "m1".into(),
                client_message_id: None,
                conversation_id: "c1".into(),
                sender_id: "u1".into(),
                conversation_type: None,
                message_type: None,
                persisted_at: SystemTime::now(),
                metadata: HashMap::new(),
            };
            let outcome = tokio::time::timeout(bounded, post_send.handle(&ctx, &record, &draft))
                .await
                .expect("post-send hung");
            assert!(matches!(
                outcome,
                HookOutcome::Failed(error) if error.code() == Some(ErrorCode::InvalidParameter)
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            connections.load(Ordering::SeqCst),
            0,
            "a delivery without a tenant reached the endpoint"
        );

        // With a tenant the hook connects.
        let ctx: Ctx = Arc::new(Context::root().with_tenant_id("tenant-a"));
        let mut draft = MessageDraft::new(b"hello".to_vec());
        let _ = tokio::time::timeout(bounded, pre_send.handle(&ctx, &mut draft))
            .await
            .expect("pre-send hung");
        assert_eq!(connections.load(Ordering::SeqCst), 1);
    }
}
