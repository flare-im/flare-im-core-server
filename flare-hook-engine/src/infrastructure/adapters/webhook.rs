//! # WebHook适配器
//!
//! 提供基于HTTP WebHook的Hook传输适配器实现。

use std::collections::HashMap;

use anyhow::{Context as AnyhowContext, Result};
use base64::Engine;
use reqwest::Client;

use flare_im_core::hooks::hook_context_data::get_hook_context_data;
use flare_im_core::{DeliveryEvent, MessageDraft, MessageRecord, PreSendDecision, RecallEvent};
use flare_server_core::context::Context;

/// WebHook适配器
pub struct WebhookHookAdapter {
    client: Client,
    endpoint: String,
    secret: Option<String>,
    headers: HashMap<String, String>,
}

impl WebhookHookAdapter {
    /// 创建WebHook适配器
    pub async fn new(
        endpoint: String,
        secret: Option<String>,
        headers: HashMap<String, String>,
    ) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        Ok(Self {
            client,
            endpoint,
            secret,
            headers,
        })
    }

    /// 执行PreSend Hook
    pub async fn pre_send(
        &self,
        ctx: &Context,
        draft: &mut MessageDraft,
    ) -> Result<PreSendDecision> {
        use serde_json::json;
        use std::time::{SystemTime, UNIX_EPOCH};

        let hook_data = get_hook_context_data(ctx).cloned().unwrap_or_default();
        let tenant_id = ctx.tenant_id().unwrap_or("0").to_string();

        let payload = json!({
            "hook_type": "pre_send",
            "context": {
                "tenant_id": tenant_id,
                "conversation_id": hook_data.conversation_id,
                "conversation_type": hook_data.conversation_type,
            },
            "draft": {
                "message_id": draft.message_id,
                "client_message_id": draft.client_message_id,
                "conversation_id": draft.conversation_id,
                "payload": base64::engine::general_purpose::STANDARD.encode(&draft.payload),
                "headers": draft.headers,
                "metadata": draft.metadata,
            },
            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        });

        let mut request = self
            .client
            .post(&self.endpoint)
            .json(&payload)
            .timeout(std::time::Duration::from_secs(30));

        // 设置自定义请求头
        for (key, value) in &self.headers {
            request = request.header(key, value);
        }

        // 如果配置了密钥，生成签名
        if let Some(ref secret) = self.secret {
            let signature = self.generate_signature(&payload.to_string(), secret)?;
            request = request.header("X-Hook-Signature", signature);
        }

        let response = request
            .send()
            .await
            .with_context(|| "WebHook PreSend request failed")?;

        if response.status().is_success() {
            let result: serde_json::Value = response
                .json()
                .await
                .with_context(|| "Failed to parse WebHook response")?;

            // 检查是否允许发送
            let allow = result
                .get("allow")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);

            if allow {
                // 如果允许发送，检查是否有修改后的 draft
                if let Some(updated_draft) = result.get("draft") {
                    if let Some(payload_base64) =
                        updated_draft.get("payload").and_then(|v| v.as_str())
                    {
                        if let Ok(payload) =
                            base64::engine::general_purpose::STANDARD.decode(payload_base64)
                        {
                            draft.payload = payload;
                        }
                    }
                    if let Some(headers) = updated_draft.get("headers").and_then(|v| v.as_object())
                    {
                        for (key, value) in headers {
                            if let Some(value_str) = value.as_str() {
                                draft.header(key.clone(), value_str.to_string());
                            }
                        }
                    }
                    if let Some(metadata) =
                        updated_draft.get("metadata").and_then(|v| v.as_object())
                    {
                        for (key, value) in metadata {
                            if let Some(value_str) = value.as_str() {
                                draft.metadata(key.clone(), value_str.to_string());
                            }
                        }
                    }
                }
                Ok(PreSendDecision::Continue)
            } else {
                use flare_im_core::error::{ErrorBuilder, ErrorCode};
                let error =
                    ErrorBuilder::new(ErrorCode::PermissionDenied, "WebHook rejected the request")
                        .build_error();
                Ok(PreSendDecision::Reject { error })
            }
        } else {
            use flare_im_core::error::{ErrorBuilder, ErrorCode};
            let error = ErrorBuilder::new(
                ErrorCode::InternalError,
                &format!("WebHook returned error status: {}", response.status()),
            )
            .build_error();
            Err(error.into())
        }
    }

    /// 执行PostSend Hook
    pub async fn post_send(
        &self,
        ctx: &Context,
        record: &MessageRecord,
        _draft: &MessageDraft,
    ) -> Result<()> {
        use serde_json::json;
        use std::time::{SystemTime, UNIX_EPOCH};

        let hook_data = get_hook_context_data(ctx).cloned().unwrap_or_default();
        let tenant_id = ctx.tenant_id().unwrap_or("0").to_string();

        let payload = json!({
            "hook_type": "post_send",
            "context": {
                "tenant_id": tenant_id,
                "conversation_id": hook_data.conversation_id,
            },
            "record": {
                "message_id": record.message_id,
                "conversation_id": record.conversation_id,
                "sender_id": record.sender_id,
            },
            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        });

        let mut request = self
            .client
            .post(&self.endpoint)
            .json(&payload)
            .timeout(std::time::Duration::from_secs(30));

        for (key, value) in &self.headers {
            request = request.header(key, value);
        }

        if let Some(ref secret) = self.secret {
            let signature = self.generate_signature(&payload.to_string(), secret)?;
            request = request.header("X-Hook-Signature", signature);
        }

        let response = request
            .send()
            .await
            .with_context(|| "WebHook PostSend request failed")?;

        if response.status().is_success() {
            Ok(())
        } else {
            use flare_im_core::error::{ErrorBuilder, ErrorCode};
            let error = ErrorBuilder::new(
                ErrorCode::InternalError,
                &format!("WebHook returned error status: {}", response.status()),
            )
            .build_error();
            Err(error.into())
        }
    }

    /// 执行Delivery Hook
    pub async fn delivery(&self, ctx: &Context, event: &DeliveryEvent) -> Result<()> {
        use serde_json::json;

        let tenant_id = ctx.tenant_id().unwrap_or("0").to_string();

        let payload = json!({
            "hook_type": "delivery",
            "context": {
                "tenant_id": tenant_id,
            },
            "event": {
                "message_id": event.message_id,
                "user_id": event.user_id,
                "channel": event.channel,
            },
        });

        let mut request = self
            .client
            .post(&self.endpoint)
            .json(&payload)
            .timeout(std::time::Duration::from_secs(30));

        for (key, value) in &self.headers {
            request = request.header(key, value);
        }

        if let Some(ref secret) = self.secret {
            let signature = self.generate_signature(&payload.to_string(), secret)?;
            request = request.header("X-Hook-Signature", signature);
        }

        let _response = request
            .send()
            .await
            .with_context(|| "WebHook Delivery request failed")?;

        Ok(())
    }

    /// 执行Recall Hook
    pub async fn recall(&self, ctx: &Context, event: &RecallEvent) -> Result<PreSendDecision> {
        use serde_json::json;

        let tenant_id = ctx.tenant_id().unwrap_or("0").to_string();

        let payload = json!({
            "hook_type": "recall",
            "context": {
                "tenant_id": tenant_id,
            },
            "event": {
                "message_id": event.message_id,
                "operator_id": event.operator_id,
            },
        });

        let mut request = self
            .client
            .post(&self.endpoint)
            .json(&payload)
            .timeout(std::time::Duration::from_secs(30));

        for (key, value) in &self.headers {
            request = request.header(key, value);
        }

        if let Some(ref secret) = self.secret {
            let signature = self.generate_signature(&payload.to_string(), secret)?;
            request = request.header("X-Hook-Signature", signature);
        }

        let response = request
            .send()
            .await
            .with_context(|| "WebHook Recall request failed")?;

        if response.status().is_success() {
            let result: serde_json::Value = response
                .json()
                .await
                .with_context(|| "Failed to parse WebHook response")?;

            let allow = result
                .get("allow")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);

            if allow {
                Ok(PreSendDecision::Continue)
            } else {
                use flare_im_core::error::{ErrorBuilder, ErrorCode};
                let error = ErrorBuilder::new(
                    ErrorCode::PermissionDenied,
                    "WebHook rejected the recall request",
                )
                .build_error();
                Ok(PreSendDecision::Reject { error })
            }
        } else {
            use flare_im_core::error::{ErrorBuilder, ErrorCode};
            let error = ErrorBuilder::new(
                ErrorCode::InternalError,
                &format!("WebHook returned error status: {}", response.status()),
            )
            .build_error();
            Err(error.into())
        }
    }

    /// 生成签名（使用 HMAC-SHA256）
    fn generate_signature(&self, payload: &str, secret: &str) -> Result<String> {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        type HmacSha256 = Hmac<Sha256>;

        let mut mac =
            HmacSha256::new_from_slice(secret.as_bytes()).with_context(|| "Invalid secret key")?;
        mac.update(payload.as_bytes());
        let result = mac.finalize();
        let signature = hex::encode(result.into_bytes());

        Ok(format!("sha256={}", signature))
    }
}
