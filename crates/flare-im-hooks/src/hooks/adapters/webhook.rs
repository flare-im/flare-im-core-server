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
        let client = Client::builder().use_rustls_tls().build().map_err(|err| {
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
    let mut builder = request_builder;
    for (key, value) in headers {
        if key.eq_ignore_ascii_case("flare-signature")
            || key.eq_ignore_ascii_case("x-flare-signature")
            || key.eq_ignore_ascii_case("content-type")
        {
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
            "Flare-Signature",
            signature(secret.as_bytes(), timestamp, &body),
        );
    }
    Ok(builder.body(body))
}

fn signature(secret: &[u8], timestamp: u64, body: &[u8]) -> String {
    // HMAC accepts keys of any length; configuration rejects empty secrets above.
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret).expect("HMAC supports arbitrary key lengths");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    format!(
        "t={timestamp},v1={}",
        hex::encode(mac.finalize().into_bytes())
    )
}

#[cfg(test)]
mod signature_tests {
    use super::*;

    #[test]
    fn signature_matches_independent_hmac_sha256_vector() {
        assert_eq!(
            signature(b"test-secret", 1_700_000_000, br#"{"message":"hello"}"#),
            "t=1700000000,v1=543eae34d92c82da22df2744e174c470175e5d72cec71947502699d5cb85672a"
        );
    }

    #[test]
    fn signature_binds_body_timestamp_and_secret() {
        let original = signature(b"a-secret", 100, b"body");
        assert_ne!(original, signature(b"a-secret", 101, b"body"));
        assert_ne!(original, signature(b"a-secret", 100, b"tampered"));
        assert_ne!(original, signature(b"other-secret", 100, b"body"));
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
            signature(
                secret.as_bytes(),
                timestamp,
                request.body().unwrap().as_bytes().unwrap()
            )
        );
        assert!(!header.contains(secret));
        assert!(!request.headers().contains_key("x-flare-signature"));
    }

    #[test]
    fn reserved_headers_cannot_reintroduce_plaintext_or_override_signature() {
        for header in ["X-Flare-Signature", "FLARE-SIGNATURE", "Content-Type"] {
            let headers = HashMap::from([(header.to_owned(), "secret".to_owned())]);
            assert!(
                signed_request(
                    Client::new().post("http://127.0.0.1/hook"),
                    &Some("secret".into()),
                    &headers,
                    &()
                )
                .is_err()
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
}

fn webhook_context(ctx: &Ctx) -> WebhookContextPayload {
    use crate::hooks::hook_context_data::get_hook_context_data;

    let hook_data = get_hook_context_data(ctx).cloned().unwrap_or_default();
    let tenant_id = ctx.tenant_id().unwrap_or("0").to_string();
    let trace_id = {
        let trace_id = ctx.trace_id();
        if trace_id.is_empty() {
            None
        } else {
            Some(trace_id.to_string())
        }
    };

    WebhookContextPayload {
        tenant_id,
        conversation_id: hook_data.conversation_id,
        conversation_type: hook_data.conversation_type,
        message_type: hook_data.message_type,
        sender_id: hook_data.sender_id,
        trace_id,
        tags: hook_data.tags,
        attributes: hook_data.attributes,
    }
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
        let request_body = PreSendWebhookRequest {
            context: webhook_context(ctx),
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
        let request_body = PostSendWebhookRequest {
            context: webhook_context(ctx),
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
        let request_body = DeliveryWebhookRequest {
            context: webhook_context(ctx),
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
        let request_body = RecallWebhookRequest {
            context: webhook_context(ctx),
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
