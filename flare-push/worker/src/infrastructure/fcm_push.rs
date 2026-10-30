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
//! 仅 HTTP404 的结构化 FcmError/UNREGISTERED 允许条件删除观测到的注册。
//! INVALID_ARGUMENT 也可能是负载错误，不能因此删除正常设备。

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
use crate::infrastructure::{vendor_http, vendor_jwt};

/// 注册表中标识 FCM 通道的 provider 名。
pub const FCM_PROVIDER: &str = "fcm";

const OAUTH_SCOPE: &str = "https://www.googleapis.com/auth/firebase.messaging";
const OAUTH_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
/// 提前于真实过期时间刷新，留出时钟漂移与请求耗时的余量。
const TOKEN_REFRESH_SKEW: Duration = Duration::from_secs(300);

/// FCM 服务账号凭据（取自 Google Cloud 下发的 service-account JSON）。
#[derive(Clone, Deserialize, Serialize)]
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
    endpoint: String,
}

impl FcmOfflinePushExecutor {
    pub fn new(
        account: FcmServiceAccount,
        http: reqwest::Client,
        tokens: Arc<dyn DeviceTokenRepository>,
    ) -> Result<Self, FlareError> {
        if account.token_uri != OAUTH_TOKEN_URI
            || account.project_id.is_empty()
            || account.project_id.len() > 128
            || !account
                .project_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || account.client_email.len() > 320
            || !account.client_email.contains('@')
        {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "FCM service account configuration rejected",
            ));
        }
        jsonwebtoken::EncodingKey::from_rsa_pem(account.private_key.as_bytes()).map_err(|_| {
            FlareError::localized(ErrorCode::InvalidParameter, "FCM RSA private key rejected")
        })?;
        let endpoint = format!(
            "https://fcm.googleapis.com/v1/projects/{}/messages:send",
            account.project_id
        );
        Ok(Self {
            account,
            http,
            tokens,
            cached: RwLock::new(None),
            endpoint,
        })
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
        // Single flight: recheck while holding the refresh lock, including OAuth roundtrip.
        let mut cached_slot = self.cached.write().await;
        if let Some(cached) = cached_slot.as_ref()
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
            .map_err(|_| vendor_http::failure("FCM OAuth transport failed"))?;

        if !resp.status().is_success() {
            return Err(vendor_http::failure("FCM OAuth rejected"));
        }

        let token: OAuthTokenResponse = serde_json::from_str(&vendor_http::body(resp).await?)
            .map_err(|_| vendor_http::failure("FCM OAuth response invalid"))?;
        if token.access_token.is_empty()
            || token.access_token.len() > 8192
            || token
                .access_token
                .bytes()
                .any(|b| b.is_ascii_control() || b == b' ')
            || token.expires_in <= TOKEN_REFRESH_SKEW.as_secs()
            || token.expires_in > 86400
        {
            return Err(vendor_http::failure(
                "FCM OAuth token lifetime or shape invalid",
            ));
        }

        let refresh_after = now
            .saturating_add(token.expires_in)
            .saturating_sub(TOKEN_REFRESH_SKEW.as_secs());
        *cached_slot = Some(CachedToken {
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
            .map_err(|_| {
                FlareError::localized(ErrorCode::InvalidParameter, "FCM RSA private key rejected")
            })?;
        // 只签 RS256，走 aws-lc（vendor_jwt），不经进程级默认 JWT provider。
        vendor_jwt::sign_vendor_jwt(
            jsonwebtoken::Algorithm::RS256,
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
            &claims,
            &key,
        )
        .map_err(|_| FlareError::localized(ErrorCode::InternalError, "FCM JWT signing failed"))
    }
}

/// 组装 FCM HTTP v1 的请求体。
///
/// 独立成纯函数便于测试 —— 推送体一旦组错，线上表现是「通知发出去了但点开
/// 跳不到会话」，比彻底发不出去更难排查。
pub fn build_fcm_message_body(envelope: &PushTaskEnvelope, registration_token: &str) -> Value {
    let display = notification_display(envelope);
    let mut body = json!({
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
                "show_badge":display.show_badge.to_string(),
                "play_sound":display.play_sound.to_string(),
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
    });
    if display.play_sound {
        body["message"]["android"]["notification"]["sound"] = json!("default");
        body["message"]["apns"]["payload"]["aps"]["sound"] = json!("default");
    }
    if let Some(expires) = envelope.expire_at.filter(|value| *value > 0) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        body["message"]["android"]["ttl"] = json!(format!(
            "{}s",
            expires
                .saturating_sub(now_ms)
                .clamp(0, 28 * 24 * 3600 * 1000)
                / 1000
        ));
        body["message"]["apns"]["headers"]["apns-expiration"] = json!((expires / 1000).to_string());
    }
    body
}

/// 判断 FCM 的错误响应是否意味着「这个 token 已永久失效」。
///
/// 只有确认失效才删注册表项 —— 把限流、服务端故障之类的临时错误当成失效，
/// 会把正常设备的 token 误删，用户从此再也收不到推送。
pub fn is_unrecoverable_target(status: reqwest::StatusCode, body: &str) -> bool {
    if status != reqwest::StatusCode::NOT_FOUND {
        return false;
    }
    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return false;
    };
    value
        .pointer("/error/details")
        .and_then(Value::as_array)
        .is_some_and(|details| {
            details.iter().any(|detail| {
                detail.get("@type").and_then(Value::as_str)
                    == Some("type.googleapis.com/google.firebase.fcm.v1.FcmError")
                    && detail.get("errorCode").and_then(Value::as_str) == Some("UNREGISTERED")
            })
        })
}

#[async_trait::async_trait]
impl crate::domain::offline_delivery::OfflinePushExecutor for FcmOfflinePushExecutor {
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
            .filter(|t| t.usable_for_provider(FCM_PROVIDER))
        {
            if self.tokens.was_accepted(&token, envelope).await? {
                continue;
            }
            let access_token = self.access_token().await?;
            if !self.tokens.is_current(&token).await? {
                continue;
            }
            let response = self
                .http
                .post(&self.endpoint)
                .bearer_auth(&access_token)
                .json(&build_fcm_message_body(envelope, &token.token))
                .send()
                .await;
            let result = match response {
                Ok(response) => {
                    let status = response.status();
                    match vendor_http::body(response).await {
                        Ok(body) if status == reqwest::StatusCode::OK => {
                            let valid =
                                serde_json::from_str::<Value>(&body).ok().is_some_and(|v| {
                                    v.get("name").and_then(Value::as_str).is_some_and(|s| {
                                        s.starts_with("projects/") && s.contains("/messages/")
                                    })
                                });
                            if valid {
                                self.tokens.record_accepted(&token, envelope).await
                            } else {
                                Err(vendor_http::failure("FCM acceptance response invalid"))
                            }
                        }
                        Ok(body) if is_unrecoverable_target(status, &body) => {
                            self.tokens.remove_device_token(ctx, &token).await
                        }
                        Ok(_) => {
                            if status == reqwest::StatusCode::UNAUTHORIZED {
                                let mut cached = self.cached.write().await;
                                if cached.as_ref().is_some_and(|v| v.value == access_token) {
                                    *cached = None;
                                }
                            }
                            let code = if status.is_server_error()
                                || status == reqwest::StatusCode::TOO_MANY_REQUESTS
                                || status == reqwest::StatusCode::UNAUTHORIZED
                            {
                                ErrorCode::ServiceUnavailable
                            } else {
                                ErrorCode::MessageDeliveryFailed
                            };
                            Err(FlareError::localized(code, "FCM delivery rejected"))
                        }
                        Err(error) => Err(error),
                    }
                }
                Err(_) => Err(vendor_http::failure(
                    "FCM transport failed; acceptance unknown",
                )),
            };
            if let Err(error) = result {
                // Any unresolved device keeps the task unresolved; success on another is not an ACK.
                if failure
                    .as_ref()
                    .is_none_or(|previous: &FlareError| !previous.is_retryable())
                {
                    failure = Some(error);
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::offline_delivery::OfflinePushExecutor;
    use crate::infrastructure::push_test_support::{Tokens, http_peer, token};

    fn test_executor(endpoint: String, tokens: Arc<Tokens>) -> FcmOfflinePushExecutor {
        FcmOfflinePushExecutor {
            account: FcmServiceAccount {
                project_id: "local-test".into(),
                client_email: "test@example.invalid".into(),
                private_key: String::new(),
                token_uri: OAUTH_TOKEN_URI.into(),
            },
            http: vendor_http::client().unwrap(),
            tokens,
            cached: RwLock::new(Some(CachedToken {
                value: "controlled-token".into(),
                refresh_after: u64::MAX,
            })),
            endpoint,
        }
    }

    #[tokio::test]
    async fn retired_binding_is_rechecked_before_vendor_send() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let repo = Arc::new(Tokens::default());
        let row = token("device-a");
        repo.removed
            .lock()
            .unwrap()
            .push(row.registration_id.clone());
        *repo.rows.lock().unwrap() = vec![row];
        test_executor(endpoint, repo.clone())
            .deliver(&Ctx::default(), &envelope(1))
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
        assert!(repo.accepted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn partial_http_failure_retries_only_unaccepted_registration() {
        let accepted = r#"{"name":"projects/local-test/messages/one"}"#.to_string();
        let (endpoint, peer) = http_peer(vec![
            (200, accepted.clone()),
            (503, "PRIVATE provider error".into()),
            (200, accepted),
        ])
        .await;
        let repo = Arc::new(Tokens::default());
        *repo.rows.lock().unwrap() = vec![token("device-a"), token("device-b")];
        let executor = test_executor(endpoint, repo.clone());
        let task = envelope(1);
        let ctx = Ctx::default();
        let error = executor.deliver(&ctx, &task).await.unwrap_err();
        assert!(error.is_retryable());
        assert!(!error.to_string().contains("PRIVATE"));
        executor.deliver(&ctx, &task).await.unwrap();
        let bodies = peer.await.unwrap();
        assert_eq!(
            bodies
                .iter()
                .map(|v| v["message"]["token"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["device-a", "device-b", "device-b"]
        );
        assert_eq!(repo.accepted.lock().unwrap().len(), 2);
        assert!(repo.removed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn generic_404_cannot_remove_device_and_unknown_200_is_not_acceptance() {
        let (endpoint, peer) = http_peer(vec![
            (404, "UNREGISTERED PRIVATE".into()),
            (200, "{}".into()),
        ])
        .await;
        let repo = Arc::new(Tokens::default());
        *repo.rows.lock().unwrap() = vec![token("device-a")];
        let executor = test_executor(endpoint, repo.clone());
        assert!(
            executor
                .deliver(&Ctx::default(), &envelope(1))
                .await
                .is_err()
        );
        assert!(
            executor
                .deliver(&Ctx::default(), &envelope(1))
                .await
                .unwrap_err()
                .is_retryable()
        );
        peer.await.unwrap();
        assert!(repo.removed.lock().unwrap().is_empty());
        assert!(repo.accepted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn oversized_provider_body_is_bounded_and_cannot_be_echoed() {
        let (endpoint, peer) = http_peer(vec![(503, "SECRET".repeat(4096))]).await;
        let repo = Arc::new(Tokens::default());
        *repo.rows.lock().unwrap() = vec![token("device-a")];
        let error = test_executor(endpoint, repo)
            .deliver(&Ctx::default(), &envelope(1))
            .await
            .unwrap_err();
        assert!(error.is_retryable());
        assert!(!error.to_string().contains("SECRET"));
        peer.await.unwrap();
    }

    #[test]
    fn oauth_credential_destination_is_not_configurable() {
        let account = FcmServiceAccount {
            project_id: "project".into(),
            client_email: "test@example.invalid".into(),
            private_key: "SECRET".into(),
            token_uri: "http://127.0.0.1/steal".into(),
        };
        let error = FcmOfflinePushExecutor::new(
            account,
            vendor_http::client().unwrap(),
            Arc::new(Tokens::default()),
        )
        .err()
        .unwrap();
        assert!(!error.to_string().contains("SECRET"));
    }

    #[tokio::test]
    async fn a_sync_only_carrier_never_reaches_vendor_http() {
        use flare_proto::common::{
            Message, MessageContent, NotificationContent, message_content::Content,
        };
        use prost::Message as _;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let repo = Arc::new(Tokens::default());
        *repo.rows.lock().unwrap() = vec![token("device-a")];
        let mut task = envelope(1);
        task.push_payload = flare_grpc_proto::access_gateway::PushMessageRequest {
            messages: vec![Message {
                content: Some(MessageContent {
                    content: Some(Content::Notification(NotificationContent::default())),
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode_to_vec();
        tokio::time::timeout(
            Duration::from_millis(200),
            test_executor(endpoint, repo.clone()).deliver(&Ctx::default(), &task),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(repo.accepted.lock().unwrap().is_empty());
        assert!(repo.removed.lock().unwrap().is_empty());
    }

    fn envelope(priority: i32) -> PushTaskEnvelope {
        PushTaskEnvelope {
            user_id: "u1".into(),
            message_id: "m1".into(),
            conversation_id: "c1".into(),
            tenant_id: "t1".into(),
            priority,
            expire_at: None,
            push_payload: crate::infrastructure::push_test_support::alert_payload(),
            headers: Default::default(),
            payload_kind: flare_proto::common::PushTaskPayloadKind::Message as i32,
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
    fn quiet_notification_preserves_title_but_never_injects_default_sound() {
        use flare_proto::common::{
            Message, MessageContent, NotificationContent, message_content::Content,
        };
        use prost::Message as _;
        let mut task = envelope(1);
        task.push_payload = flare_grpc_proto::access_gateway::PushMessageRequest {
            messages: vec![Message {
                content: Some(MessageContent {
                    content: Some(Content::Notification(NotificationContent {
                        title: "Title".into(),
                        body: "Quiet".into(),
                        play_sound: false,
                        show_badge: false,
                        ..Default::default()
                    })),
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode_to_vec();
        let body = build_fcm_message_body(&task, "token");
        assert_eq!(body["message"]["notification"]["body"], "Quiet");
        assert_eq!(body["message"]["data"]["play_sound"], "false");
        assert!(body["message"]["apns"]["payload"]["aps"]["sound"].is_null());
        let apns = crate::infrastructure::apns_push::build_apns_payload(&task);
        assert_eq!(apns["aps"]["alert"]["body"], "Quiet");
        assert!(apns["aps"]["sound"].is_null());
        let token = crate::infrastructure::push_test_support::token("device");
        let request =
            crate::infrastructure::getui_push::build_getui_push_request(&task, &token, 1000);
        let getui = crate::infrastructure::getui_push::build_getui_single_cid_body(&request);
        assert!(getui["push_channel"]["ios"]["aps"]["sound"].is_null());
        assert!(getui["push_channel"]["ios"]["auto_badge"].is_null());
    }

    #[test]
    fn only_confirmed_dead_tokens_are_treated_as_unrecoverable() {
        use reqwest::StatusCode;
        // 确认失效
        assert!(!is_unrecoverable_target(StatusCode::NOT_FOUND, ""));
        assert!(is_unrecoverable_target(
            StatusCode::NOT_FOUND,
            r#"{"error":{"details":[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"UNREGISTERED"}]}}"#
        ));

        for status in [
            StatusCode::BAD_REQUEST,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::NOT_FOUND,
        ] {
            assert!(!is_unrecoverable_target(
                status,
                r#"{"error":{"message":"UNREGISTERED INVALID_ARGUMENT"}}"#
            ));
        }

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
