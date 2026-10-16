//! APNs（Apple Push Notification service）离线推送通道。
//!
//! 本适配器只发送 regular alert；VoIP、Live Activity、Critical Alert 需要独立的
//! typed 负载、entitlement 与设备生命周期，不因接入 APNs 就视为已经实现。
//!
//! ## 认证：provider token（JWT）
//!
//! 用 p8 私钥以 ES256 签发，header 带 `kid`（Key ID），claims 带 `iss`（Team ID）。
//! **Apple 要求 token 有效期在 20–60 分钟之间，且刷新间隔不得短于 20 分钟** ——
//! 刷得太勤会被判定为滥用并拒绝（403 TooManyProviderTokenUpdates）。
//! 这里按 50 分钟刷新，两侧都留足余量。
//!
//! ## HTTP/2
//!
//! APNs 只接受 HTTP/2。客户端必须开启 `http2` 特性，否则连接直接失败。

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flare_im_contracts::Ctx;
use flare_proto::PushTaskEnvelope;
use flare_server_core::error::{ErrorCode, FlareError};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::RwLock;

use crate::domain::offline_delivery::DeviceTokenRepository;
use crate::infrastructure::push_display::notification_display;
use crate::infrastructure::vendor_http;

/// 注册表中标识 APNs 通道的 provider 名。
pub const APNS_PROVIDER: &str = "apns";

const PROD_HOST: &str = "https://api.push.apple.com";
const SANDBOX_HOST: &str = "https://api.sandbox.push.apple.com";
/// Apple 允许 20–60 分钟；取 50 分钟，两侧都留余量。
const TOKEN_TTL: Duration = Duration::from_secs(50 * 60);

/// APNs 提供者凭据。
#[derive(Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApnsCredentials {
    /// Apple Developer Team ID（10 位）。
    pub team_id: String,
    /// p8 私钥的 Key ID（10 位）。
    pub key_id: String,
    /// p8 私钥内容（PEM）。**走环境变量或密钥管理注入，勿入版本控制。**
    pub private_key_pem: String,
    /// 目标 App 的 bundle id，作为 `apns-topic`。
    pub topic: String,
    /// true 走沙箱环境（开发构建的设备 token 只能用沙箱）。
    #[serde(default)]
    pub sandbox: bool,
}

impl ApnsCredentials {
    fn host(&self) -> &'static str {
        if self.sandbox {
            SANDBOX_HOST
        } else {
            PROD_HOST
        }
    }
}

#[derive(Debug, Serialize)]
struct JwtClaims<'a> {
    iss: &'a str,
    iat: u64,
}

#[derive(Debug, Clone)]
struct CachedToken {
    value: String,
    /// 该时刻之后才允许重新签发（unix 秒）。
    refresh_after: u64,
}

pub struct ApnsOfflinePushExecutor {
    creds: ApnsCredentials,
    http: reqwest::Client,
    tokens: Arc<dyn DeviceTokenRepository>,
    cached: RwLock<Option<CachedToken>>,
}

impl ApnsOfflinePushExecutor {
    pub fn new(
        creds: ApnsCredentials,
        http: reqwest::Client,
        tokens: Arc<dyn DeviceTokenRepository>,
    ) -> Result<Self, FlareError> {
        if [&creds.team_id, &creds.key_id].iter().any(|s| {
            s.len() != 10
                || !s
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        }) || creds.topic.is_empty()
            || creds.topic.len() > 255
            || !creds
                .topic
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-'))
        {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "APNs configuration rejected",
            ));
        }
        jsonwebtoken::EncodingKey::from_ec_pem(creds.private_key_pem.as_bytes()).map_err(|_| {
            FlareError::localized(ErrorCode::InvalidParameter, "APNs EC private key rejected")
        })?;
        Ok(Self {
            creds,
            http,
            tokens,
            cached: RwLock::new(None),
        })
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// 取 provider token；未到刷新时刻则复用缓存。
    ///
    /// 复用不是单纯的性能优化 —— Apple 对过于频繁的 token 更新会直接拒绝。
    async fn provider_token(&self) -> Result<String, FlareError> {
        let now = Self::now_secs();
        let mut cached_slot = self.cached.write().await;
        if let Some(cached) = cached_slot.as_ref()
            && cached.refresh_after > now
        {
            return Ok(cached.value.clone());
        }

        let claims = JwtClaims {
            iss: &self.creds.team_id,
            iat: now,
        };
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::ES256);
        header.kid = Some(self.creds.key_id.clone());

        let key = jsonwebtoken::EncodingKey::from_ec_pem(self.creds.private_key_pem.as_bytes())
            .map_err(|_| {
                FlareError::localized(ErrorCode::InvalidParameter, "APNs EC private key rejected")
            })?;
        let jwt = jsonwebtoken::encode(&header, &claims, &key).map_err(|_| {
            FlareError::localized(ErrorCode::InternalError, "APNs JWT signing failed")
        })?;

        *cached_slot = Some(CachedToken {
            value: jwt.clone(),
            refresh_after: now + TOKEN_TTL.as_secs(),
        });
        Ok(jwt)
    }
}

/// 组装 APNs 通知负载。
///
/// 独立成纯函数便于测试：aps 结构写错的表现是「推送到达但不弹横幅」，
/// 排查起来比彻底失败更费劲。
pub fn build_apns_payload(envelope: &PushTaskEnvelope) -> Value {
    let display = notification_display(envelope);
    let mut payload = json!({
        "aps": {
            "alert": { "title": display.title, "body": display.body },
            // mutable-content 让 Notification Service Extension 有机会
            // 在展示前替换内容（如拉取头像、解密 E2EE 正文）。
            "mutable-content": 1,
        },
        // 自定义字段与 aps 平级；客户端点击后据此直达会话。
        "message_id": envelope.message_id,
        "conversation_id": envelope.conversation_id,
        "tenant_id": envelope.tenant_id,
        "show_badge":display.show_badge,
    });
    if display.play_sound {
        payload["aps"]["sound"] = json!("default");
    }
    payload
}

/// APNs 的这次失败是否意味着「该 device token 永久失效」。
///
/// 判定必须保守：把限流或 Apple 侧故障当成失效会误删正常设备的 token，
/// 用户从此收不到任何推送，且无法自愈（除非重装 App 重新注册）。
pub fn is_dead_device_token(status: reqwest::StatusCode, reason: &str) -> bool {
    status == reqwest::StatusCode::GONE && reason == "Unregistered"
}

/// 从 APNs 错误响应体里取出 `reason` 字段。
pub fn parse_apns_reason(body: &str) -> String {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v.get("reason").and_then(Value::as_str).map(str::to_string))
        .filter(|s| s.len() <= 64 && s.bytes().all(|b| b.is_ascii_alphabetic()))
        .unwrap_or_else(|| "Unknown".to_string())
}

fn invalidation_applies(status: reqwest::StatusCode, body: &str, registered_at_ms: i64) -> bool {
    if !is_dead_device_token(status, &parse_apns_reason(body)) {
        return false;
    }
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v.get("timestamp").and_then(Value::as_i64))
        .is_some_and(|timestamp| timestamp > 0 && registered_at_ms <= timestamp)
}

fn expiration_seconds(expire_at_ms: i64) -> i64 {
    expire_at_ms.max(0) / 1000
}

#[async_trait::async_trait]
impl crate::domain::offline_delivery::OfflinePushExecutor for ApnsOfflinePushExecutor {
    async fn deliver(&self, ctx: &Ctx, envelope: &PushTaskEnvelope) -> Result<(), FlareError> {
        if !crate::infrastructure::push_display::mobile_alert_eligible(envelope) {
            return Ok(());
        }
        let tokens = self
            .tokens
            .list_user_tokens(ctx, &envelope.tenant_id, &envelope.user_id)
            .await?;
        let mut failure = None;
        for token in tokens
            .into_iter()
            .filter(|t| t.usable_for_provider(APNS_PROVIDER))
        {
            if self.tokens.was_accepted(&token, envelope).await? {
                continue;
            }
            if token.token.len() > 512 || !token.token.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(FlareError::localized(
                    ErrorCode::MessageDeliveryFailed,
                    "APNs device token shape rejected",
                ));
            }
            let jwt = self.provider_token().await?;
            if !self.tokens.is_current(&token).await? {
                continue;
            }
            let mut request = self
                .http
                .post(format!("{}/3/device/{}", self.creds.host(), token.token))
                .bearer_auth(&jwt)
                .header("apns-topic", &self.creds.topic)
                .header("apns-push-type", "alert")
                .header(
                    "apns-priority",
                    if envelope.priority > 0 { "10" } else { "5" },
                );
            if let Some(expiration) = envelope.expire_at.filter(|value| *value > 0) {
                request = request.header(
                    "apns-expiration",
                    expiration_seconds(expiration).to_string(),
                );
            }
            let result = match request.json(&build_apns_payload(envelope)).send().await {
                Ok(response) if response.status() == reqwest::StatusCode::OK => {
                    self.tokens.record_accepted(&token, envelope).await
                }
                Ok(response) => {
                    let status = response.status();
                    match vendor_http::body(response).await {
                        Ok(body) if invalidation_applies(status, &body, token.registered_at_ms) => {
                            self.tokens.remove_device_token(ctx, &token).await
                        }
                        Ok(_) => {
                            let code = if status.is_server_error()
                                || status == reqwest::StatusCode::TOO_MANY_REQUESTS
                                || status == reqwest::StatusCode::GONE
                            {
                                ErrorCode::ServiceUnavailable
                            } else {
                                ErrorCode::MessageDeliveryFailed
                            };
                            Err(FlareError::localized(code, "APNs delivery rejected"))
                        }
                        Err(error) => Err(error),
                    }
                }
                Err(_) => Err(vendor_http::failure(
                    "APNs transport failed; acceptance unknown",
                )),
            };
            if let Err(error) = result
                && failure
                    .as_ref()
                    .is_none_or(|previous: &FlareError| !previous.is_retryable())
            {
                failure = Some(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(priority: i32) -> PushTaskEnvelope {
        PushTaskEnvelope {
            user_id: "u1".into(),
            message_id: "m1".into(),
            conversation_id: "c1".into(),
            tenant_id: "t1".into(),
            priority,
            expire_at: None,
            push_payload: Vec::new(),
            headers: Default::default(),
            payload_kind: 0,
        }
    }

    #[test]
    fn payload_has_aps_alert_and_routing_keys() {
        let p = build_apns_payload(&envelope(1));
        assert!(p["aps"]["alert"]["title"].is_string());
        assert!(p["aps"]["alert"]["body"].is_string());
        assert_eq!(p["aps"]["mutable-content"], 1);
        // 自定义字段必须与 aps 平级，嵌进 aps 里客户端读不到
        assert_eq!(p["conversation_id"], "c1");
        assert_eq!(p["message_id"], "m1");
        assert_eq!(p["tenant_id"], "t1");
    }

    #[test]
    fn only_apple_confirmed_dead_tokens_are_removed() {
        use reqwest::StatusCode;
        // Apple 明确的失效信号
        assert!(is_dead_device_token(StatusCode::GONE, "Unregistered"));
        assert!(!is_dead_device_token(
            StatusCode::BAD_REQUEST,
            "BadDeviceToken"
        ));
        assert!(!is_dead_device_token(
            StatusCode::BAD_REQUEST,
            "DeviceTokenNotForTopic"
        ));

        // 以下都不能删 token —— 误删后用户不重装 App 就再也收不到推送
        assert!(!is_dead_device_token(
            StatusCode::TOO_MANY_REQUESTS,
            "TooManyRequests"
        ));
        assert!(!is_dead_device_token(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalServerError"
        ));
        assert!(!is_dead_device_token(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable"
        ));
        assert!(!is_dead_device_token(
            StatusCode::FORBIDDEN,
            "ExpiredProviderToken"
        ));
        // 400 但原因是负载过大 —— 是我们的 bug，不是 token 的问题
        assert!(!is_dead_device_token(
            StatusCode::BAD_REQUEST,
            "PayloadTooLarge"
        ));
    }

    #[test]
    fn reason_is_extracted_from_apple_error_body() {
        assert_eq!(
            parse_apns_reason(r#"{"reason":"BadDeviceToken"}"#),
            "BadDeviceToken"
        );
        assert_eq!(parse_apns_reason("PRIVATE vendor body"), "Unknown");
        assert!(!is_dead_device_token(reqwest::StatusCode::GONE, "anything"));
        assert!(!invalidation_applies(
            reqwest::StatusCode::GONE,
            r#"{"reason":"Unregistered","timestamp":1000}"#,
            1001
        ));
        assert!(invalidation_applies(
            reqwest::StatusCode::GONE,
            r#"{"reason":"Unregistered","timestamp":1000}"#,
            1000
        ));
        assert!(!invalidation_applies(
            reqwest::StatusCode::GONE,
            r#"{"reason":"Unregistered"}"#,
            1
        ));
        assert_eq!(expiration_seconds(1_800_000_000_123), 1_800_000_000);
    }

    #[test]
    fn sandbox_flag_selects_apple_host() {
        let mk = |sandbox| ApnsCredentials {
            team_id: "T".into(),
            key_id: "K".into(),
            private_key_pem: String::new(),
            topic: "com.example.app".into(),
            sandbox,
        };
        // 开发构建的 device token 只在沙箱有效，选错环境会得到 BadDeviceToken
        assert!(mk(true).host().contains("sandbox"));
        assert!(!mk(false).host().contains("sandbox"));
    }
}
