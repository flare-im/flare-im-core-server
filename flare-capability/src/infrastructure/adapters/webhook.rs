//! # WebHook适配器
//!
//! 提供基于HTTP WebHook的Hook传输适配器实现。
//!
//! 签名与 Core 的 Webhook 适配器同一契约（SEC-06，`docs/HOOK-PLUGIN-CONTRACT.md` §7）：配了密钥
//! 时带 `Flare-Signature: t=<unix 秒>,v1=<hex(HMAC-SHA256(secret, t "." 原始请求体))>`，签名直接
//! 复用 [`flare_im_hooks::hooks::adapters::sign_webhook_request`]；密钥从不出现在请求里，旧的
//! `X-Hook-Signature`（只签请求体、不带时间戳，截获后可无限重放）已删除。投递的租户只取 Hook
//! 上下文的租户（HookPlugin 验过的签名租户），缺租户时拒绝，不再按租户 "0" 发出。

use std::collections::HashMap;

use base64::Engine;
use reqwest::Client;

use flare_im_hooks::hooks::adapters::sign_webhook_request;
use flare_im_hooks::hooks::hook_context_data::get_hook_context_data;
use flare_im_hooks::{DeliveryEvent, MessageDraft, MessageRecord, PreSendDecision, RecallEvent};
use flare_server_core::context::Context;

use flare_server_core::error::{ErrorBuilder, ErrorCode, Result, map_infra_error};

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
        let builder = Client::builder().timeout(std::time::Duration::from_secs(30));
        // 单测的接收端都在本机回环上：单测构建里不读 HTTP(S)_PROXY、ALL_PROXY 与系统代理设置，
        // 否则开发机导出的代理会截走回环投递（以前须 `env -u HTTPS_PROXY …` 才能跑）。正式构建不变。
        #[cfg(test)]
        let builder = builder.no_proxy();
        let client = builder.build().map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::InternalError,
                "failed to build HTTP client for webhook hook",
            )
        })?;

        Ok(Self {
            client,
            endpoint,
            secret,
            headers,
        })
    }

    /// 序列化一次、签名、发出：签名覆盖的正是发出去的字节（见模块说明）。
    async fn post_signed(
        &self,
        payload: &serde_json::Value,
        failure: &'static str,
    ) -> Result<reqwest::Response> {
        let body = serde_json::to_vec(payload).map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::SerializationError,
                "failed to serialize WebHook request",
            )
        })?;
        let request = sign_webhook_request(
            self.client
                .post(&self.endpoint)
                .timeout(std::time::Duration::from_secs(30)),
            self.secret.as_deref(),
            &self.headers,
            body,
        )?;
        request
            .send()
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, failure))
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
        let tenant_id = delivery_tenant(ctx)?;

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
            "timestamp": SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        });

        let response = self
            .post_signed(&payload, "WebHook PreSend request failed")
            .await?;

        if response.status().is_success() {
            let result: serde_json::Value = response.json().await.map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::InternalError,
                    "failed to parse WebHook PreSend JSON response",
                )
            })?;

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
                        && let Ok(payload) =
                            base64::engine::general_purpose::STANDARD.decode(payload_base64)
                    {
                        draft.payload = payload;
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
                let error =
                    ErrorBuilder::new(ErrorCode::PermissionDenied, "WebHook rejected the request")
                        .build_error();
                Ok(PreSendDecision::Reject { error })
            }
        } else {
            let error = ErrorBuilder::new(
                ErrorCode::InternalError,
                format!("WebHook returned error status: {}", response.status()),
            )
            .build_error();
            Err(error)
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
        let tenant_id = delivery_tenant(ctx)?;

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
            "timestamp": SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        });

        let response = self
            .post_signed(&payload, "WebHook PostSend request failed")
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            let error = ErrorBuilder::new(
                ErrorCode::InternalError,
                format!("WebHook returned error status: {}", response.status()),
            )
            .build_error();
            Err(error)
        }
    }

    /// 执行Delivery Hook
    pub async fn delivery(&self, ctx: &Context, event: &DeliveryEvent) -> Result<()> {
        use serde_json::json;

        let tenant_id = delivery_tenant(ctx)?;

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

        self.post_signed(&payload, "WebHook Delivery request failed")
            .await?;

        Ok(())
    }

    /// 执行Recall Hook
    pub async fn recall(&self, ctx: &Context, event: &RecallEvent) -> Result<PreSendDecision> {
        use serde_json::json;

        let tenant_id = delivery_tenant(ctx)?;

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

        let response = self
            .post_signed(&payload, "WebHook Recall request failed")
            .await?;

        if response.status().is_success() {
            let result: serde_json::Value = response.json().await.map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::InternalError,
                    "failed to parse WebHook Recall JSON response",
                )
            })?;

            let allow = result
                .get("allow")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);

            if allow {
                Ok(PreSendDecision::Continue)
            } else {
                let error = ErrorBuilder::new(
                    ErrorCode::PermissionDenied,
                    "WebHook rejected the recall request",
                )
                .build_error();
                Ok(PreSendDecision::Reject { error })
            }
        } else {
            let error = ErrorBuilder::new(
                ErrorCode::InternalError,
                format!("WebHook returned error status: {}", response.status()),
            )
            .build_error();
            Err(error)
        }
    }
}

/// 投递的租户：只取 Hook 上下文的租户。缺租户时拒绝，不按租户 "0" 发出——接收端会把它当成
/// 租户 0 的事件处理（W11-T）。
fn delivery_tenant(ctx: &Context) -> Result<String> {
    ctx.tenant_id()
        .filter(|tenant| !tenant.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "webhook hook context has no tenant",
            )
            .build_error()
        })
}

#[cfg(test)]
mod tests {
    //! Capability 自带 Webhook 适配器的签名契约（W11-S1 F2）：经 HookPlugin 的签名调用投到租户
    //! 端点的请求只带 `Flare-Signature`（时间戳绑进 MAC），不带旧的 `X-Hook-Signature`，也不带
    //! 密钥；接收端按契约示例校验，窗口外的重放被拒。缺租户时不投递。
    use super::*;
    use std::sync::Arc;

    use flare_im_hooks::hooks::adapters::{
        WEBHOOK_SIGNATURE_TOLERANCE_SECS, WebhookSignatureError, verify_webhook_signature,
    };
    use flare_server_core::SignedActorType;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    use crate::application::handler::HookCommandHandler;
    use crate::composition::hook_registry::CoreHookRegistry;
    use crate::domain::model::HookConfig;
    use crate::domain::service::HookOrchestrationService;
    use crate::infrastructure::adapters::HookAdapterFactory;
    use crate::infrastructure::capability::CapabilityExtensionRegistry;
    use crate::infrastructure::config::{ConfigWatcher, HookConfigSnapshot};
    use crate::interface::grpc::ImHookPluginServer;
    use crate::interface::grpc::hooks::test_support::{
        WebhookReceiver, cell_keys, ingest_pre_send, serve_hook_plugin, signed_ctx,
        signed_webhook_hook,
    };

    const OWN_SECRET: &str = "w12w-tenant-a-signing-secret";
    const OTHER_SECRET: &str = "w12w-tenant-b-signing-secret";

    /// 与适配器无关的 HMAC-SHA256(secret, t "." body)。
    fn independent_mac(secret: &str, timestamp: u64, body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(format!("{timestamp}.").as_bytes());
        mac.update(body);
        hex::encode(mac.finalize().into_bytes())
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }

    #[tokio::test]
    async fn signed_hook_plugin_calls_deliver_the_timestamped_signature_contract() {
        let receiver = WebhookReceiver::start().await;
        let keys = cell_keys(97);
        let pre_send = |name: &str, path: &str, secret: &str| HookConfig {
            pre_send: vec![signed_webhook_hook(name, &receiver.url(path), Some(secret))],
            ..Default::default()
        };
        let snapshot = HookConfigSnapshot::new(
            HookConfig::default(),
            std::collections::HashMap::from([
                ("tenant-a".to_string(), pre_send("wh-a", "/a", OWN_SECRET)),
                ("tenant-b".to_string(), pre_send("wh-b", "/b", OTHER_SECRET)),
            ]),
        );
        let endpoint = serve_hook_plugin(ImHookPluginServer::new(
            Arc::new(HookCommandHandler::new(Arc::new(HookOrchestrationService))),
            Arc::new(CoreHookRegistry::new(Arc::new(
                ConfigWatcher::with_snapshot(snapshot),
            ))),
            Arc::new(HookAdapterFactory::new()),
            CapabilityExtensionRegistry::new(),
            Some(keys.verifier.clone()),
        ))
        .await;

        let ctx = signed_ctx(&keys, SignedActorType::User, "u-w12w", "tenant-a");
        match ingest_pre_send(&endpoint, &ctx, "w12w-sig").await {
            PreSendDecision::Continue => {}
            PreSendDecision::Reject { error } => panic!("pre-send rejected: {error}"),
        }
        let deliveries = receiver.wait_for("w12w-sig", 1).await;
        let [delivery] = deliveries.as_slice() else {
            panic!("expected one delivery, got {deliveries:?}");
        };
        assert_eq!(delivery.path, "/a");
        assert_eq!(delivery.tenant(), Some("tenant-a"));
        assert!(delivery.header_values("x-hook-signature").is_empty());
        assert!(delivery.header_values("x-flare-signature").is_empty());
        let signatures = delivery.header_values("flare-signature");
        let [header] = signatures.as_slice() else {
            panic!("expected one Flare-Signature, got {:?}", delivery.headers);
        };

        // t=<unix>,v1=<hex HMAC-SHA256(secret, t "." raw body)>，按独立实现复算。
        let (t, v1) = header.split_once(',').unwrap();
        let timestamp: u64 = t.strip_prefix("t=").unwrap().parse().unwrap();
        assert_eq!(
            v1.strip_prefix("v1=").unwrap(),
            independent_mac(OWN_SECRET, timestamp, &delivery.raw)
        );
        assert!(
            delivery.received.abs_diff(timestamp) <= 5,
            "stale t={timestamp}"
        );

        // 接收端示例：本方密钥在窗口内通过，他方密钥不通过；同一份截获在窗口之后重放被拒，
        // 把时间戳改到窗口内则 MAC 不再成立。
        let tolerance = WEBHOOK_SIGNATURE_TOLERANCE_SECS;
        assert_eq!(
            verify_webhook_signature(
                OWN_SECRET.as_bytes(),
                &delivery.raw,
                header,
                delivery.received,
                tolerance
            ),
            Ok(timestamp)
        );
        assert_eq!(
            verify_webhook_signature(
                OTHER_SECRET.as_bytes(),
                &delivery.raw,
                header,
                delivery.received,
                tolerance
            ),
            Err(WebhookSignatureError::Mismatch)
        );
        let replayed_at = timestamp + tolerance + 1;
        assert_eq!(
            verify_webhook_signature(
                OWN_SECRET.as_bytes(),
                &delivery.raw,
                header,
                replayed_at,
                tolerance
            ),
            Err(WebhookSignatureError::OutsideTolerance { timestamp })
        );
        let refreshed = format!("t={replayed_at},{v1}");
        assert_eq!(
            verify_webhook_signature(
                OWN_SECRET.as_bytes(),
                &delivery.raw,
                &refreshed,
                replayed_at,
                tolerance
            ),
            Err(WebhookSignatureError::Mismatch)
        );

        // 密钥不上线：请求头与请求体里都没有任一租户的密钥。
        for secret in [OWN_SECRET, OTHER_SECRET] {
            assert!(!contains(&delivery.raw, secret.as_bytes()));
            assert!(
                delivery
                    .headers
                    .iter()
                    .all(|(_, value)| !value.contains(secret))
            );
        }
    }

    #[tokio::test]
    async fn a_context_without_a_tenant_or_with_a_reserved_header_delivers_nothing() {
        let receiver = WebhookReceiver::start().await;
        let unsigned_tenantless = Context::with_request_id("req-w12w");
        let adapter = WebhookHookAdapter::new(
            receiver.url("/no-tenant"),
            Some(OWN_SECRET.into()),
            HashMap::new(),
        )
        .await
        .unwrap();
        let mut draft = MessageDraft::new(b"hello".to_vec());
        draft.set_client_message_id("w12w-no-tenant".to_string());
        let error = adapter
            .pre_send(&unsigned_tenantless, &mut draft)
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::InvalidParameter), "{error}");
        let blank = Context::with_request_id("req-w12w").with_tenant_id("  ");
        assert!(adapter.pre_send(&blank, &mut draft).await.is_err());

        // 自定义头不能借旧头名再带一份签名。
        let reserved = WebhookHookAdapter::new(
            receiver.url("/reserved"),
            Some(OWN_SECRET.into()),
            HashMap::from([("X-Hook-Signature".to_string(), "sha256=00".to_string())]),
        )
        .await
        .unwrap();
        let tenant = Context::with_request_id("req-w12w").with_tenant_id("tenant-a");
        assert!(reserved.pre_send(&tenant, &mut draft).await.is_err());

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(receiver.count(), 0);

        // 没配密钥时投递照常，只是不带签名头。
        let unsigned = WebhookHookAdapter::new(receiver.url("/plain"), None, HashMap::new())
            .await
            .unwrap();
        draft.set_client_message_id("w12w-plain".to_string());
        assert!(matches!(
            unsigned.pre_send(&tenant, &mut draft).await.unwrap(),
            PreSendDecision::Continue
        ));
        let deliveries = receiver.wait_for("w12w-plain", 1).await;
        assert_eq!(deliveries.len(), 1);
        assert!(deliveries[0].header_values("flare-signature").is_empty());
        assert_eq!(deliveries[0].tenant(), Some("tenant-a"));
    }
}
