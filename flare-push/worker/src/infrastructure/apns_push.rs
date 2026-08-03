//! APNs（Apple Push Notification service）离线推送通道。
//!
//! 为什么不能只靠 FCM 转发：VoIP 来电、Live Activity、Critical Alert 这些
//! 推送类型只有原生 APNs 支持，FCM 转发覆盖不到。做 IM 少了 VoIP 推送，
//! 就意味着 iOS 端收不到来电。
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

/// 注册表中标识 APNs 通道的 provider 名。
pub const APNS_PROVIDER: &str = "apns";

const PROD_HOST: &str = "https://api.push.apple.com";
const SANDBOX_HOST: &str = "https://api.sandbox.push.apple.com";
/// Apple 允许 20–60 分钟；取 50 分钟，两侧都留余量。
const TOKEN_TTL: Duration = Duration::from_secs(50 * 60);

/// APNs 提供者凭据。
#[derive(Debug, Clone)]
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
    ) -> Self {
        Self {
            creds,
            http,
            tokens,
            cached: RwLock::new(None),
        }
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
        if let Some(cached) = self.cached.read().await.as_ref()
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
            .map_err(|e| {
                FlareError::localized(
                    ErrorCode::InvalidParameter,
                    format!("APNs p8 私钥无法解析（需 PEM 格式 EC 私钥）: {e}"),
                )
            })?;
        let jwt = jsonwebtoken::encode(&header, &claims, &key).map_err(|e| {
            FlareError::localized(ErrorCode::InternalError, format!("APNs JWT 签名失败: {e}"))
        })?;

        *self.cached.write().await = Some(CachedToken {
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
    json!({
        "aps": {
            "alert": { "title": display.title, "body": display.body },
            "sound": "default",
            // mutable-content 让 Notification Service Extension 有机会
            // 在展示前替换内容（如拉取头像、解密 E2EE 正文）。
            "mutable-content": 1,
        },
        // 自定义字段与 aps 平级；客户端点击后据此直达会话。
        "message_id": envelope.message_id,
        "conversation_id": envelope.conversation_id,
        "tenant_id": envelope.tenant_id,
    })
}

/// APNs 的这次失败是否意味着「该 device token 永久失效」。
///
/// 判定必须保守：把限流或 Apple 侧故障当成失效会误删正常设备的 token，
/// 用户从此收不到任何推送，且无法自愈（除非重装 App 重新注册）。
pub fn is_dead_device_token(status: reqwest::StatusCode, reason: &str) -> bool {
    // 410 Gone 是 Apple 明确的「此 token 不再有效」信号。
    if status == reqwest::StatusCode::GONE {
        return true;
    }
    // 400 里只有这两个 reason 指向 token 本身，其余多是我们请求组装错误。
    if status == reqwest::StatusCode::BAD_REQUEST {
        return reason.contains("BadDeviceToken") || reason.contains("DeviceTokenNotForTopic");
    }
    false
}

/// 从 APNs 错误响应体里取出 `reason` 字段。
pub fn parse_apns_reason(body: &str) -> String {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v.get("reason").and_then(Value::as_str).map(str::to_string))
        .unwrap_or_else(|| body.chars().take(120).collect())
}

#[async_trait::async_trait]
impl crate::interface::messaging::offline_consumer::OfflinePushExecutor
    for ApnsOfflinePushExecutor
{
    async fn deliver(&self, ctx: &Ctx, envelope: &PushTaskEnvelope) -> Result<(), FlareError> {
        let tokens = self
            .tokens
            .list_user_tokens(ctx, &envelope.tenant_id, &envelope.user_id)
            .await?;
        let usable: Vec<_> = tokens
            .into_iter()
            .filter(|t| t.usable_for_provider(APNS_PROVIDER))
            .collect();
        if usable.is_empty() {
            tracing::debug!(
                tenant_id = %envelope.tenant_id,
                user_id = %envelope.user_id,
                message_id = %envelope.message_id,
                "离线推送：该用户无 APNs 设备 token"
            );
            return Ok(());
        }

        let jwt = self.provider_token().await?;
        let payload = build_apns_payload(envelope);
        let host = self.creds.host();
        let priority = if envelope.priority > 0 { "10" } else { "5" };

        let mut delivered = 0usize;
        let mut terminal_error: Option<FlareError> = None;

        for token in usable {
            let url = format!("{host}/3/device/{}", token.token);
            let mut req = self
                .http
                .post(&url)
                .bearer_auth(&jwt)
                .header("apns-topic", &self.creds.topic)
                .header("apns-push-type", "alert")
                .header("apns-priority", priority);
            if let Some(expire_at) = envelope.expire_at {
                // APNs 的 apns-expiration 是绝对 unix 秒；0 表示只投递一次不重试。
                req = req.header("apns-expiration", expire_at.to_string());
            }

            match req.json(&payload).send().await {
                Ok(r) if r.status().is_success() => delivered += 1,
                Ok(r) => {
                    let status = r.status();
                    let body = r.text().await.unwrap_or_default();
                    let reason = parse_apns_reason(&body);
                    if is_dead_device_token(status, &reason) {
                        tracing::info!(
                            tenant_id = %envelope.tenant_id,
                            user_id = %envelope.user_id,
                            device_id = %token.device_id,
                            reason = %reason,
                            "APNs 目标已失效，从注册表移除该设备 token"
                        );
                        let _ = self.tokens.remove_device_token(ctx, &token).await;
                    } else {
                        terminal_error = Some(FlareError::localized(
                            ErrorCode::InternalError,
                            format!("APNs 投递失败（{status}）: {reason}"),
                        ));
                    }
                }
                Err(e) => {
                    terminal_error = Some(FlareError::localized(
                        ErrorCode::InternalError,
                        format!("APNs 请求失败: {e}"),
                    ));
                }
            }
        }

        // 与 FCM 通道一致：一台成功即整体成功，避免重试给其余设备造成重复通知。
        if delivered > 0 {
            return Ok(());
        }
        match terminal_error {
            Some(err) => Err(err),
            None => Ok(()), // 全部目标已失效并清理，无需重试
        }
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
        assert!(is_dead_device_token(
            StatusCode::BAD_REQUEST,
            "BadDeviceToken"
        ));
        assert!(is_dead_device_token(
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
        // 非 JSON 时退回原文截断，保证日志里仍有可诊断信息
        assert_eq!(parse_apns_reason("gateway timeout"), "gateway timeout");
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
