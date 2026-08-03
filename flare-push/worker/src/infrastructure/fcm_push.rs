//! FCM（Firebase Cloud Messaging）HTTP v1 离线推送通道。
//!
//! 补齐离线推送的 last-mile：此前 worker 只有个推一家通道，
//! 海外 Android 与走 FCM 的 iOS 无法投递 —— 用户离线时消息就是收不到，
//! 这是移动端产品的底线能力。
//!
//! ## 认证
//!
//! HTTP v1 用服务账号 JWT 换取 OAuth2 access token（有效期 1 小时）。
//! token 在内存缓存并提前 5 分钟刷新，避免每条推送都换一次。
//!
//! ## 失效 token 清理
//!
//! FCM 对已卸载/失效的 registration token 返回 `UNREGISTERED` 或
//! `INVALID_ARGUMENT`。这类目标必须从注册表删除，否则每条消息都会为
//! 一个永不可达的设备重试，积累成持续的无效流量。

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flare_im_contracts::Ctx;
use flare_proto::PushTaskEnvelope;
use flare_server_core::error::{ErrorCode, FlareError};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::RwLock;

use crate::domain::offline_delivery::DeviceTokenRepository;
use crate::infrastructure::push_display::notification_display;

/// 注册表中标识 FCM 通道的 provider 名。
pub const FCM_PROVIDER: &str = "fcm";

const OAUTH_SCOPE: &str = "https://www.googleapis.com/auth/firebase.messaging";
const OAUTH_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
/// 提前于真实过期时间刷新，留出时钟漂移与请求耗时的余量。
const TOKEN_REFRESH_SKEW: Duration = Duration::from_secs(300);

/// FCM 服务账号凭据（取自 Google Cloud 下发的 service-account JSON）。
#[derive(Debug, Clone, Deserialize)]
pub struct FcmServiceAccount {
    pub project_id: String,
    pub client_email: String,
    /// PEM 格式的 RSA 私钥。**不要写进配置文件提交到版本控制**，
    /// 用环境变量或密钥管理服务注入。
    pub private_key: String,
    #[serde(default = "default_token_uri")]
    pub token_uri: String,
}

fn default_token_uri() -> String {
    OAUTH_TOKEN_URI.to_string()
}

#[derive(Debug, Serialize)]
struct JwtClaims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    exp: u64,
    iat: u64,
}

#[derive(Debug, Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
    expires_in: u64,
}

#[derive(Debug, Clone)]
struct CachedToken {
    value: String,
    /// 到期时刻（unix 秒），已扣除 `TOKEN_REFRESH_SKEW`。
    refresh_after: u64,
}

pub struct FcmOfflinePushExecutor {
    account: FcmServiceAccount,
    http: reqwest::Client,
    tokens: Arc<dyn DeviceTokenRepository>,
    cached: RwLock<Option<CachedToken>>,
}

impl FcmOfflinePushExecutor {
    pub fn new(
        account: FcmServiceAccount,
        http: reqwest::Client,
        tokens: Arc<dyn DeviceTokenRepository>,
    ) -> Self {
        Self {
            account,
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

    /// 取可用的 OAuth2 access token；缓存未过期则直接复用。
    async fn access_token(&self) -> Result<String, FlareError> {
        let now = Self::now_secs();
        if let Some(cached) = self.cached.read().await.as_ref()
            && cached.refresh_after > now
        {
            return Ok(cached.value.clone());
        }

        let jwt = self.sign_jwt(now)?;
        let resp = self
            .http
            .post(&self.account.token_uri)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", jwt.as_str()),
            ])
            .send()
            .await
            .map_err(|e| {
                FlareError::localized(
                    ErrorCode::InternalError,
                    format!("FCM 换取 token 失败: {e}"),
                )
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(FlareError::localized(
                ErrorCode::InternalError,
                format!(
                    "FCM OAuth 拒绝（{status}）: {}",
                    body.chars().take(200).collect::<String>()
                ),
            ));
        }

        let token: OAuthTokenResponse = resp.json().await.map_err(|e| {
            FlareError::localized(
                ErrorCode::InternalError,
                format!("FCM token 响应解析失败: {e}"),
            )
        })?;

        let refresh_after = now
            .saturating_add(token.expires_in)
            .saturating_sub(TOKEN_REFRESH_SKEW.as_secs());
        *self.cached.write().await = Some(CachedToken {
            value: token.access_token.clone(),
            refresh_after,
        });
        Ok(token.access_token)
    }

    fn sign_jwt(&self, now: u64) -> Result<String, FlareError> {
        let claims = JwtClaims {
            iss: &self.account.client_email,
            scope: OAUTH_SCOPE,
            aud: &self.account.token_uri,
            exp: now + 3600,
            iat: now,
        };
        let key = jsonwebtoken::EncodingKey::from_rsa_pem(self.account.private_key.as_bytes())
            .map_err(|e| {
                FlareError::localized(
                    ErrorCode::InvalidParameter,
                    format!("FCM 服务账号私钥无法解析（需 PEM 格式 RSA 私钥）: {e}"),
                )
            })?;
        jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
            &claims,
            &key,
        )
        .map_err(|e| {
            FlareError::localized(ErrorCode::InternalError, format!("FCM JWT 签名失败: {e}"))
        })
    }

    fn endpoint(&self) -> String {
        format!(
            "https://fcm.googleapis.com/v1/projects/{}/messages:send",
            self.account.project_id
        )
    }
}

/// 组装 FCM HTTP v1 的请求体。
///
/// 独立成纯函数便于测试 —— 推送体一旦组错，线上表现是「通知发出去了但点开
/// 跳不到会话」，比彻底发不出去更难排查。
pub fn build_fcm_message_body(envelope: &PushTaskEnvelope, registration_token: &str) -> Value {
    let display = notification_display(envelope);
    json!({
        "message": {
            "token": registration_token,
            "notification": {
                "title": display.title,
                "body": display.body,
            },
            // data 里带路由信息，客户端点击通知后据此直达会话。
            // 值必须是字符串 —— FCM 的 data 只接受 string map。
            "data": {
                "message_id": envelope.message_id,
                "conversation_id": envelope.conversation_id,
                "tenant_id": envelope.tenant_id,
            },
            "android": {
                // 有正文的消息按 high 投递，保证熄屏也能唤醒；
                // 静默类走 normal 由系统合并，省电。
                "priority": if envelope.priority > 0 { "high" } else { "normal" },
            },
            "apns": {
                "headers": {
                    "apns-priority": if envelope.priority > 0 { "10" } else { "5" },
                },
            },
        }
    })
}

/// 判断 FCM 的错误响应是否意味着「这个 token 已永久失效」。
///
/// 只有确认失效才删注册表项 —— 把限流、服务端故障之类的临时错误当成失效，
/// 会把正常设备的 token 误删，用户从此再也收不到推送。
pub fn is_unrecoverable_target(status: reqwest::StatusCode, body: &str) -> bool {
    if status == reqwest::StatusCode::NOT_FOUND {
        return true; // token 不存在
    }
    if status == reqwest::StatusCode::BAD_REQUEST {
        // 400 既可能是 token 格式错（永久），也可能是请求体错（我们的 bug）。
        // 仅在错误码明确指向 token 时才判定失效。
        return body.contains("UNREGISTERED") || body.contains("INVALID_ARGUMENT");
    }
    body.contains("UNREGISTERED")
}

#[async_trait::async_trait]
impl crate::interface::messaging::offline_consumer::OfflinePushExecutor for FcmOfflinePushExecutor {
    async fn deliver(&self, ctx: &Ctx, envelope: &PushTaskEnvelope) -> Result<(), FlareError> {
        let tokens = self
            .tokens
            .list_user_tokens(ctx, &envelope.tenant_id, &envelope.user_id)
            .await?;
        let usable: Vec<_> = tokens
            .into_iter()
            .filter(|t| t.usable_for_provider(FCM_PROVIDER))
            .collect();
        if usable.is_empty() {
            tracing::debug!(
                tenant_id = %envelope.tenant_id,
                user_id = %envelope.user_id,
                message_id = %envelope.message_id,
                "离线推送：该用户无 FCM 设备 token"
            );
            return Ok(());
        }

        let access_token = self.access_token().await?;
        let endpoint = self.endpoint();
        let mut terminal_error: Option<FlareError> = None;
        let mut delivered = 0usize;

        for token in usable {
            let body = build_fcm_message_body(envelope, &token.token);
            let resp = self
                .http
                .post(&endpoint)
                .bearer_auth(&access_token)
                .json(&body)
                .send()
                .await;

            match resp {
                Ok(r) if r.status().is_success() => delivered += 1,
                Ok(r) => {
                    let status = r.status();
                    let text = r.text().await.unwrap_or_default();
                    if is_unrecoverable_target(status, &text) {
                        // 确认失效才清理，避免误删正常设备。
                        tracing::info!(
                            tenant_id = %envelope.tenant_id,
                            user_id = %envelope.user_id,
                            device_id = %token.device_id,
                            "FCM 目标已失效，从注册表移除该设备 token"
                        );
                        let _ = self.tokens.remove_device_token(ctx, &token).await;
                    } else {
                        terminal_error = Some(FlareError::localized(
                            ErrorCode::InternalError,
                            format!(
                                "FCM 投递失败（{status}）: {}",
                                text.chars().take(200).collect::<String>()
                            ),
                        ));
                    }
                }
                Err(e) => {
                    terminal_error = Some(FlareError::localized(
                        ErrorCode::InternalError,
                        format!("FCM 请求失败: {e}"),
                    ));
                }
            }
        }

        // 只要有一台设备投递成功就算成功：多设备场景下，
        // 因为某台旧设备失败而整体重试，会给其余设备造成重复通知。
        if delivered > 0 {
            return Ok(());
        }
        match terminal_error {
            Some(err) => Err(err),
            // 全部目标都被判定为失效并已清理 —— 无需重试。
            None => Ok(()),
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
    fn body_carries_routing_data_and_token() {
        let v = build_fcm_message_body(&envelope(1), "reg-token-1");
        let m = &v["message"];
        assert_eq!(m["token"], "reg-token-1");
        // 路由信息必须在 data 里，客户端靠它点击直达会话
        assert_eq!(m["data"]["conversation_id"], "c1");
        assert_eq!(m["data"]["message_id"], "m1");
        assert_eq!(m["data"]["tenant_id"], "t1");
        assert!(m["notification"]["title"].is_string());
        assert!(m["notification"]["body"].is_string());
    }

    #[test]
    fn priority_maps_to_platform_specific_fields() {
        let high = build_fcm_message_body(&envelope(1), "t");
        assert_eq!(high["message"]["android"]["priority"], "high");
        assert_eq!(high["message"]["apns"]["headers"]["apns-priority"], "10");

        let normal = build_fcm_message_body(&envelope(0), "t");
        assert_eq!(normal["message"]["android"]["priority"], "normal");
        assert_eq!(normal["message"]["apns"]["headers"]["apns-priority"], "5");
    }

    #[test]
    fn only_confirmed_dead_tokens_are_treated_as_unrecoverable() {
        use reqwest::StatusCode;
        // 确认失效
        assert!(is_unrecoverable_target(StatusCode::NOT_FOUND, ""));
        assert!(is_unrecoverable_target(
            StatusCode::BAD_REQUEST,
            r#"{"error":{"status":"UNREGISTERED"}}"#
        ));

        // 临时故障绝不能当作失效 —— 误删 token 会让正常设备从此收不到推送
        assert!(!is_unrecoverable_target(
            StatusCode::TOO_MANY_REQUESTS,
            "quota exceeded"
        ));
        assert!(!is_unrecoverable_target(
            StatusCode::INTERNAL_SERVER_ERROR,
            "backend error"
        ));
        assert!(!is_unrecoverable_target(
            StatusCode::SERVICE_UNAVAILABLE,
            "try again"
        ));
        // 400 但错误码不指向 token（可能是我们请求体写错）→ 不清理
        assert!(!is_unrecoverable_target(
            StatusCode::BAD_REQUEST,
            r#"{"error":{"status":"PERMISSION_DENIED"}}"#
        ));
    }
}
