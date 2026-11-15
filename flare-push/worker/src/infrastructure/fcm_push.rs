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
//!
//! ## 处置
//!
//! 发给 Android 设备的一律是数据消息（没有 `notification` 块）：通知由端上的平台模块按
//! `disposition`、`alert_class` 选渠道渲染，同一会话靠 `collapse_id` 覆盖，静默更新不再响铃，
//! 别的账号的推送也能在端上丢掉。系统代为渲染的通知消息做不到这些。响铃与摘要用高优先级，
//! 保证熄屏也能唤醒；其余用普通优先级。
//!
//! 数据消息不带 `android.collapse_key`：带了就是可折叠消息，FCM 对每个应用每台设备按突发 20 条、
//! 每 3 分钟补 1 条限流（Firebase《Throttling and quotas》），聊天一热闹就会被限。同一会话的通知由
//! 端上按载荷里的 `collapse_id` 覆盖，发多少由路由的防打扰闸门管。
//!
//! 经 FCM 注册的苹果设备保留 `notification` 与 `apns` 块，沿用 APNs 适配器的同一套规则：
//! 苹果设备上收到推送时没有能自己渲染通知的端上代码。
//!
//! 服务端渲染的标题正文按目标绑定登记的语言出，露出多少由接收者的预览设置决定（`push_display`）。
//!
//! ## 限流与失败
//!
//! 429（`QUOTA_EXCEEDED` 等）按限流报给调用方（带 Retry-After），由它决定换绑定还是攒成摘要；
//! 5xx 与无应答是可重试的失败，计入厂商熔断；401 是本应用凭据的问题，不计入熔断。

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flare_im_contracts::{Ctx, DevicePushToken};
use flare_notify_rules::text::Lang;
use flare_proto::PushTaskEnvelope;
use flare_proto::common::PushDisposition;
use flare_server_core::error::{ErrorCode, FlareError};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::RwLock;

use crate::domain::offline_delivery::{DeviceTokenRepository, task_disposition};
use crate::domain::vendor_delivery::{
    BindingDelivery, BindingOutcome, FailureReason, ThrottleScope, is_apple_platform,
};
use crate::infrastructure::apns_push::{apns_priority, build_aps, wants_mutable_content};
use crate::infrastructure::push_display::{
    AlertText, ClientText, PushPlan, ValueShape, client_payload, push_plan,
};
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

/// 组装 FCM HTTP v1 的请求体，文案按绑定登记的语言渲染。`None` 表示这条任务对该设备没有
/// 可做的事，直接确认。
///
/// 独立成纯函数便于测试 —— 推送体一旦组错，线上表现是「通知发出去了但点开
/// 跳不到会话」，比彻底发不出去更难排查。
pub fn build_fcm_message_body(
    envelope: &PushTaskEnvelope,
    plan: &PushPlan,
    token: &DevicePushToken,
) -> Option<Value> {
    if *plan == PushPlan::Nothing {
        return None;
    }
    let lang = Lang::from_locale(&token.locale);
    // 经 FCM 注册的苹果设备没有能自己渲染通知的端上代码（通知扩展除外），
    // 不展示的任务只有带角标时才有效果。
    let message = if is_apple_platform(&token.platform) {
        apple_message(envelope, plan, token, lang)?
    } else {
        android_message(envelope, plan, token, lang)
    };
    Some(json!({ "message": message }))
}

/// 发给 Android 设备的数据消息。data 是 [`client_payload`] 的全部键，值都是字符串
/// （FCM 的 data 只接受 string map），带服务端渲染好的标题正文，端上代码用它渲染、
/// 也可以按本地的会话名与语言重新渲染。没有 `notification` 块：有了它，应用在后台时系统
/// 会自己弹一条通知，端上代码没机会按处置覆盖、静默或丢弃。
fn android_message(
    envelope: &PushTaskEnvelope,
    plan: &PushPlan,
    token: &DevicePushToken,
    lang: Lang,
) -> Value {
    let data = client_payload(
        envelope,
        plan,
        ValueShape::Strings,
        ClientText::Included(lang),
    );
    // 要立刻提示的通知按 high 投递，保证熄屏也能唤醒；静默覆盖、清除与仅角标走 normal，
    // 由系统合并，省电。
    let priority = match task_disposition(envelope) {
        Some(PushDisposition::Loud | PushDisposition::Digest) => "high",
        _ => "normal",
    };
    // 不带 collapse_key（见模块说明）：同一会话的覆盖靠 data 里的 collapse_id 在端上做。
    let mut android = json!({ "priority": priority });
    if let Some(expires) = expires_at(envelope) {
        android["ttl"] = json!(format!("{}s", remaining_ms(expires) / 1000));
    }
    json!({ "token": token.token, "data": data, "android": android })
}

/// 发给苹果设备的通知：`notification` 块（只有要展示的任务才有）加 `apns` 块，`aps` 与直连
/// APNs 是同一份。data 里是 [`client_payload`] 的键，FCM 把它们放在 APNs 载荷的顶层；
/// 标题正文已经在 `aps.alert` 里，不再重复。`None` 表示 APNs 上无事可做。
fn apple_message(
    envelope: &PushTaskEnvelope,
    plan: &PushPlan,
    token: &DevicePushToken,
    lang: Lang,
) -> Option<Value> {
    let aps = build_aps(envelope, plan, lang, wants_mutable_content(envelope, token))?;
    let data = client_payload(envelope, plan, ValueShape::Strings, ClientText::Omitted);
    let mut message = json!({ "token": token.token, "data": data });
    if let PushPlan::Alert(alert) = plan {
        let AlertText { title, body } = alert.text(lang);
        message["notification"] = json!({ "title": title, "body": body });
    }
    let mut headers = json!({
        "apns-push-type": "alert",
        "apns-priority": apns_priority(plan),
    });
    if !envelope.collapse_key.is_empty() {
        headers["apns-collapse-id"] = json!(envelope.collapse_key);
    }
    if let Some(expires) = expires_at(envelope) {
        headers["apns-expiration"] = json!((expires / 1000).to_string());
    }
    message["apns"] = json!({ "headers": headers, "payload": { "aps": aps } });
    Some(message)
}

fn expires_at(envelope: &PushTaskEnvelope) -> Option<i64> {
    envelope.expire_at.filter(|value| *value > 0)
}

/// 到过期还剩多少毫秒，夹在 FCM 允许的 0 到 28 天之间。
fn remaining_ms(expires_at_ms: i64) -> i64 {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    expires_at_ms
        .saturating_sub(now_ms)
        .clamp(0, 28 * 24 * 3600 * 1000)
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

/// FCM 错误应答里可以放进决策链的代码：`FcmError` 的 `errorCode`，没有时是 `error.status`，
/// 再没有就是状态码。不带应答正文的其余部分。
fn fcm_error_code(status: reqwest::StatusCode, body: &str) -> String {
    let value = serde_json::from_str::<Value>(body).ok();
    value
        .as_ref()
        .and_then(|value| value.pointer("/error/details"))
        .and_then(Value::as_array)
        .and_then(|details| {
            details
                .iter()
                .find_map(|detail| detail.get("errorCode").and_then(Value::as_str))
        })
        .or_else(|| {
            value
                .as_ref()
                .and_then(|value| value.pointer("/error/status"))
                .and_then(Value::as_str)
        })
        .filter(|code| {
            code.len() <= 40 && code.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        })
        .map_or_else(|| status.as_str().to_string(), str::to_string)
}

/// 非 200、也不是确认失效的 FCM 应答怎么算。
fn fcm_failure(
    status: reqwest::StatusCode,
    code: String,
    retry_after: Option<Duration>,
) -> BindingOutcome {
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return BindingOutcome::Throttled {
            retry_after,
            code,
            scope: ThrottleScope::Application,
        };
    }
    if status.is_server_error() {
        return BindingOutcome::retryable(
            FlareError::localized(ErrorCode::ServiceUnavailable, "FCM delivery rejected"),
            FailureReason::Server,
            code,
        );
    }
    if status == reqwest::StatusCode::UNAUTHORIZED {
        return BindingOutcome::retryable(
            FlareError::localized(ErrorCode::ServiceUnavailable, "FCM delivery rejected"),
            FailureReason::Auth,
            code,
        );
    }
    BindingOutcome::terminal(
        FlareError::localized(ErrorCode::MessageDeliveryFailed, "FCM delivery rejected"),
        FailureReason::Rejected,
        code,
    )
}

#[async_trait::async_trait]
impl BindingDelivery for FcmOfflinePushExecutor {
    async fn deliver_binding(
        &self,
        ctx: &Ctx,
        envelope: &PushTaskEnvelope,
        token: &DevicePushToken,
    ) -> BindingOutcome {
        let plan = match push_plan(envelope) {
            Ok(plan) => plan,
            Err(error) => return BindingOutcome::terminal(error, FailureReason::Config, "task"),
        };
        if !token.usable_for_provider(FCM_PROVIDER) {
            return BindingOutcome::Skipped;
        }
        let Some(body) = build_fcm_message_body(envelope, &plan, token) else {
            return BindingOutcome::Skipped;
        };
        match self.tokens.was_accepted(token, envelope).await {
            Ok(true) => return BindingOutcome::AlreadyAccepted,
            Ok(false) => {}
            Err(error) => return BindingOutcome::registry(error),
        }
        let access_token = match self.access_token().await {
            Ok(access_token) => access_token,
            Err(error) => return BindingOutcome::from_error(error, FailureReason::Auth, "oauth"),
        };
        match self.tokens.is_current(token).await {
            Ok(true) => {}
            Ok(false) => return BindingOutcome::Skipped,
            Err(error) => return BindingOutcome::registry(error),
        }
        let response = match self
            .http
            .post(&self.endpoint)
            .bearer_auth(&access_token)
            .json(&body)
            .send()
            .await
        {
            Ok(response) => response,
            Err(_) => {
                return BindingOutcome::retryable(
                    vendor_http::failure("FCM transport failed; acceptance unknown"),
                    FailureReason::Transport,
                    "transport",
                );
            }
        };
        let status = response.status();
        let retry_after = vendor_http::retry_after(&response);
        let body = match vendor_http::body(response).await {
            Ok(body) => body,
            Err(error) => {
                return BindingOutcome::retryable(error, FailureReason::Server, status.as_str());
            }
        };
        if status == reqwest::StatusCode::OK {
            let valid = serde_json::from_str::<Value>(&body).ok().is_some_and(|v| {
                v.get("name")
                    .and_then(Value::as_str)
                    .is_some_and(|s| s.starts_with("projects/") && s.contains("/messages/"))
            });
            if !valid {
                return BindingOutcome::retryable(
                    vendor_http::failure("FCM acceptance response invalid"),
                    FailureReason::Server,
                    "invalid_acceptance",
                );
            }
            return match self.tokens.record_accepted(token, envelope).await {
                Ok(()) => BindingOutcome::Accepted,
                Err(error) => BindingOutcome::registry(error),
            };
        }
        let code = fcm_error_code(status, &body);
        if is_unrecoverable_target(status, &body) {
            return match self.tokens.remove_device_token(ctx, token).await {
                Ok(()) => BindingOutcome::InvalidToken { code },
                Err(error) => BindingOutcome::registry(error),
            };
        }
        if status == reqwest::StatusCode::UNAUTHORIZED {
            let mut cached = self.cached.write().await;
            if cached.as_ref().is_some_and(|v| v.value == access_token) {
                *cached = None;
            }
        }
        fcm_failure(status, code, retry_after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::vendor_delivery::Vendor;
    use crate::infrastructure::push_test_support::{
        Tokens, deliver_governed, device_task, http_peer, push_info_payload, token,
    };
    use flare_proto::common::{AlertClass, NotificationPreview, PushDisposition};

    const COLLAPSE_KEY: &str = "c:0123456789abcdef0123456789abcdef";

    /// 发给 device-a 的一条会话消息任务：带合并键、分组键与会话 seq。
    fn envelope(disposition: PushDisposition) -> PushTaskEnvelope {
        PushTaskEnvelope {
            collapse_key: COLLAPSE_KEY.into(),
            thread_key: COLLAPSE_KEY.into(),
            alert_class: AlertClass::Normal as i32,
            conversation_seq: 42,
            ..device_task("device-a", disposition)
        }
    }

    fn ios_token(device: &str) -> DevicePushToken {
        let mut row = token(device);
        row.platform = "ios".into();
        row
    }

    /// 厂商会收到的请求体；`None` 表示不调用 FCM。
    fn body(task: &PushTaskEnvelope, row: &DevicePushToken) -> Option<Value> {
        build_fcm_message_body(task, &push_plan(task).expect("plan"), row)
    }

    /// 像 worker 那样投递：列出目标设备的绑定，经治理过的绑定选择，FCM 是唯一的厂商。
    async fn deliver(
        executor: &Arc<FcmOfflinePushExecutor>,
        repo: &Tokens,
        task: &PushTaskEnvelope,
    ) -> Result<(), FlareError> {
        deliver_governed(Vendor::Fcm, executor.clone(), repo, task).await
    }

    fn test_executor(endpoint: String, tokens: Arc<Tokens>) -> Arc<FcmOfflinePushExecutor> {
        Arc::new(FcmOfflinePushExecutor {
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
        })
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
        deliver(
            &test_executor(endpoint, repo.clone()),
            &repo,
            &envelope(PushDisposition::Loud),
        )
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
        assert!(repo.accepted.lock().unwrap().is_empty());
    }

    /// 任务只到目标设备：同一用户的另一台设备有自己的任务，这里不能顺带推一份。
    /// 厂商受理之后的重投也不再发第二次。
    #[tokio::test]
    async fn a_task_reaches_only_its_device_and_is_not_resent_once_accepted() {
        let accepted = r#"{"name":"projects/local-test/messages/one"}"#.to_string();
        let (endpoint, peer) = http_peer(vec![
            (503, "PRIVATE provider error".into()),
            (200, accepted),
        ])
        .await;
        let repo = Arc::new(Tokens::default());
        *repo.rows.lock().unwrap() = vec![token("device-a"), token("device-b")];
        let executor = test_executor(endpoint, repo.clone());
        let mut task = envelope(PushDisposition::Loud);
        task.device_id = "device-b".into();
        task.idempotency_key = "key-1".into();
        let error = deliver(&executor, &repo, &task).await.unwrap_err();
        assert!(error.is_retryable());
        assert!(!error.to_string().contains("PRIVATE"));
        deliver(&executor, &repo, &task).await.unwrap();
        // 已受理：再投一次不会再连厂商（对端只接受两次连接，多连一次这里就会失败）。
        deliver(&executor, &repo, &task).await.unwrap();
        let bodies = peer.await.unwrap();
        assert_eq!(
            bodies
                .iter()
                .map(|v| v["message"]["token"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["device-b", "device-b"]
        );
        assert_eq!(
            repo.accepted
                .lock()
                .unwrap()
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec![("generation-device-b".to_string(), "key-1".to_string())]
        );
        assert!(repo.removed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_device_without_a_binding_needs_no_vendor_call() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let repo = Arc::new(Tokens::default());
        *repo.rows.lock().unwrap() = vec![token("device-b")];
        deliver(
            &test_executor(endpoint, repo.clone()),
            &repo,
            &envelope(PushDisposition::Loud),
        )
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
            deliver(&executor, &repo, &envelope(PushDisposition::Loud))
                .await
                .is_err()
        );
        assert!(
            deliver(&executor, &repo, &envelope(PushDisposition::Loud))
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
        let error = deliver(
            &test_executor(endpoint, repo.clone()),
            &repo,
            &envelope(PushDisposition::Loud),
        )
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
        let mut task = envelope(PushDisposition::Loud);
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
            deliver(&test_executor(endpoint, repo.clone()), &repo, &task),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(repo.accepted.lock().unwrap().is_empty());
        assert!(repo.removed.lock().unwrap().is_empty());
    }

    /// 发给 Android 的响铃通知是纯数据消息：data 带全套键（字符串）与服务端渲染的标题正文，
    /// 没有 `notification` 块，也没有 `android.notification`。
    #[test]
    fn android_bindings_get_a_data_only_message_with_every_client_key() {
        let mut task = envelope(PushDisposition::Loud);
        task.expire_at = None;
        let v = body(&task, &token("reg-token-1")).expect("body");
        assert_eq!(
            v,
            json!({
                "message": {
                    "token": "reg-token-1",
                    "data": {
                        "uid": "u1",
                        "tenant_id": "t1",
                        "conversation_id": "c1",
                        "message_id": "m1",
                        // data 只接受字符串：数字写成 JSON number 会被 FCM 以 400 拒绝
                        "seq": "42",
                        // `collapse_key` 是 FCM 的保留数据键，合并键在 data 里叫 collapse_id
                        "collapse_id": COLLAPSE_KEY,
                        "thread_id": COLLAPSE_KEY,
                        "disposition": "loud",
                        "alert_class": "normal",
                        "count": "1",
                        "title": "新消息",
                        "body": "你收到一条新消息",
                        "sound": "1",
                        "sender_id": "",
                        "sender_name": "",
                        "kind": "group",
                    },
                    // 不带 collapse_key：可折叠消息每台设备突发 20 条、每 3 分钟补 1 条
                    "android": { "priority": "high" },
                }
            })
        );
    }

    #[test]
    fn android_priority_is_high_only_for_loud_and_digest() {
        for (disposition, priority) in [
            (PushDisposition::Loud, "high"),
            (PushDisposition::Digest, "high"),
            (PushDisposition::Quiet, "normal"),
            (PushDisposition::RecallReplace, "normal"),
            (PushDisposition::Clear, "normal"),
        ] {
            let v = body(&envelope(disposition), &token("t")).expect("body");
            let m = &v["message"];
            assert_eq!(m["android"]["priority"], priority, "{disposition:?}");
            assert!(m.get("notification").is_none(), "{disposition:?}");
            assert!(
                m["android"].get("notification").is_none(),
                "{disposition:?}"
            );
            assert!(m.get("apns").is_none(), "{disposition:?}");
            assert!(
                m["data"]
                    .as_object()
                    .unwrap()
                    .values()
                    .all(Value::is_string)
            );
        }
        let mut expiring = envelope(PushDisposition::Loud);
        expiring.expire_at = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
                + 600_000,
        );
        let v = body(&expiring, &token("t")).expect("body");
        let ttl = v["message"]["android"]["ttl"].as_str().expect("ttl");
        let seconds: i64 = ttl.trim_end_matches('s').parse().unwrap();
        assert!((590..=600).contains(&seconds), "{ttl}");
    }

    /// 静默覆盖与撤回替换也是数据消息：端上按 disposition 覆盖原通知、不再响铃；
    /// 条数、角标与改过的正文都随 data 下发。
    #[test]
    fn quiet_and_recall_updates_carry_their_text_for_the_device_to_replace() {
        for disposition in [PushDisposition::Quiet, PushDisposition::RecallReplace] {
            let mut task = envelope(disposition);
            task.count = 5;
            task.badge = Some(9);
            let v = body(&task, &token("t")).expect("body");
            let data = &v["message"]["data"];
            assert_eq!(data["title"], "[5条] 新消息", "{disposition:?}");
            assert_eq!(data["count"], "5");
            assert_eq!(data["badge"], "9");
            assert_eq!(data["sound"], "0");
            for legacy in ["show_badge", "play_sound", "collapse_key"] {
                assert!(data.get(legacy).is_none(), "{legacy}");
            }
        }
        let recalled = body(&envelope(PushDisposition::RecallReplace), &token("t")).expect("body");
        assert_eq!(recalled["message"]["data"]["body"], "撤回了一条消息");
        assert_eq!(recalled["message"]["data"]["disposition"], "recall_replace");
    }

    /// 仅角标与清除在 Android 上同样是数据消息，没有文案。清除指令没带角标也要发：
    /// 端上据此移除通知。
    #[test]
    fn badge_and_clear_are_data_only_on_android() {
        let mut badge = envelope(PushDisposition::BadgeOnly);
        badge.badge = Some(3);
        let clear = envelope(PushDisposition::Clear);
        for (task, disposition) in [(&badge, "badge_only"), (&clear, "clear")] {
            let v = body(task, &token("t")).expect("body");
            let m = &v["message"];
            assert!(m.get("notification").is_none(), "{disposition}");
            assert!(m.get("apns").is_none(), "{disposition}");
            assert_eq!(m["android"]["priority"], "normal");
            assert!(m["android"].get("collapse_key").is_none());
            assert_eq!(m["data"]["disposition"], disposition);
            assert_eq!(m["data"]["collapse_id"], COLLAPSE_KEY);
            assert_eq!(m["data"]["sound"], "0");
            assert!(m["data"].get("title").is_none());
            assert!(m["data"].get("collapse_key").is_none());
        }
        let v = body(&badge, &token("t")).expect("body");
        assert_eq!(v["message"]["data"]["badge"], "3");
        let v = body(&clear, &token("t")).expect("body");
        assert!(v["message"]["data"]["badge"].is_null());
    }

    /// 苹果设备保留 notification 与 apns 块，`aps` 与直连 APNs 同一份；data 里是同一套键，
    /// 标题正文只在通知块与 `aps.alert` 里。没有 android 块。
    #[test]
    fn apple_bindings_keep_the_notification_and_apns_blocks() {
        let mut task = envelope(PushDisposition::Loud);
        task.expire_at = None;
        let v = body(&task, &ios_token("t")).expect("body");
        let m = &v["message"];
        assert_eq!(
            m["notification"],
            json!({ "title": "新消息", "body": "你收到一条新消息" })
        );
        assert!(m.get("android").is_none());
        let apns = &m["apns"];
        assert_eq!(
            apns["headers"],
            json!({
                "apns-push-type": "alert",
                "apns-priority": "10",
                "apns-collapse-id": COLLAPSE_KEY,
            })
        );
        assert_eq!(apns["payload"]["aps"]["interruption-level"], "active");
        assert_eq!(apns["payload"]["aps"]["thread-id"], COLLAPSE_KEY);
        assert_eq!(apns["payload"]["aps"]["sound"], "default");
        assert!(apns["payload"]["aps"]["mutable-content"].is_null());
        assert_eq!(m["data"]["uid"], "u1");
        // 声明了通知扩展的苹果设备：要展示的通知交给扩展先处理。
        let mut extended = ios_token("t");
        extended.capabilities = vec!["serviceExtension".into()];
        let v = body(&task, &extended).expect("body");
        assert_eq!(v["message"]["apns"]["payload"]["aps"]["mutable-content"], 1);
        assert_eq!(m["data"]["thread_id"], COLLAPSE_KEY);
        assert_eq!(m["data"]["sound"], "1");
        assert!(m["data"].get("title").is_none());
        assert!(m["data"].get("body").is_none());

        let digest = body(&envelope(PushDisposition::Digest), &ios_token("t")).expect("body");
        assert_eq!(digest["message"]["apns"]["headers"]["apns-priority"], "10");

        let mut urgent = envelope(PushDisposition::Loud);
        urgent.alert_class = AlertClass::Urgent as i32;
        let urgent = body(&urgent, &ios_token("t")).expect("body");
        assert_eq!(
            urgent["message"]["apns"]["payload"]["aps"]["interruption-level"],
            "time-sensitive"
        );
        assert_eq!(urgent["message"]["data"]["alert_class"], "urgent");

        for disposition in [PushDisposition::Quiet, PushDisposition::RecallReplace] {
            let mut task = envelope(disposition);
            task.badge = Some(9);
            let v = body(&task, &ios_token("t")).expect("body");
            let apns = &v["message"]["apns"];
            assert_eq!(apns["headers"]["apns-priority"], "5");
            assert_eq!(apns["payload"]["aps"]["interruption-level"], "passive");
            assert!(apns["payload"]["aps"]["sound"].is_null());
            assert_eq!(apns["payload"]["aps"]["badge"], 9);
        }
    }

    /// 经 FCM 注册的苹果设备沿用 APNs 的规则：清除指令没带角标时无事可做，不调用厂商。
    #[test]
    fn apple_devices_through_fcm_follow_the_apns_rules() {
        let mut clear = envelope(PushDisposition::Clear);
        assert!(body(&clear, &ios_token("t")).is_none());
        clear.badge = Some(0);
        let v = body(&clear, &ios_token("t")).expect("body");
        assert_eq!(
            v["message"]["apns"]["payload"]["aps"],
            json!({ "badge": 0 })
        );
        assert_eq!(v["message"]["apns"]["headers"]["apns-priority"], "5");
        assert!(v["message"].get("notification").is_none());

        let mut badge = envelope(PushDisposition::BadgeOnly);
        assert!(body(&badge, &ios_token("t")).is_none());
        assert!(body(&badge, &token("t")).is_none());
        badge.badge = Some(7);
        assert!(body(&badge, &ios_token("t")).is_some());
    }

    #[tokio::test]
    async fn clear_without_badge_never_calls_fcm_for_an_apple_device() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let repo = Arc::new(Tokens::default());
        *repo.rows.lock().unwrap() = vec![ios_token("device-a")];
        deliver(
            &test_executor(endpoint, repo.clone()),
            &repo,
            &envelope(PushDisposition::Clear),
        )
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
        assert!(repo.accepted.lock().unwrap().is_empty());
    }

    #[test]
    fn quiet_notification_preserves_title_but_never_injects_default_sound() {
        use flare_proto::common::{
            Message, MessageContent, NotificationContent, message_content::Content,
        };
        use prost::Message as _;
        // 处置是响铃，但消息自己声明不响：三家通道都不能替它补一个默认铃声。
        let mut task = envelope(PushDisposition::Loud);
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
        let plan = push_plan(&task).expect("plan");
        let row = token("device");
        let android = build_fcm_message_body(&task, &plan, &row).expect("body");
        assert_eq!(android["message"]["data"]["body"], "Quiet");
        assert_eq!(android["message"]["data"]["sound"], "0");
        let apple = build_fcm_message_body(&task, &plan, &ios_token("device")).expect("body");
        assert_eq!(apple["message"]["notification"]["body"], "Quiet");
        assert!(apple["message"]["apns"]["payload"]["aps"]["sound"].is_null());
        let apns =
            crate::infrastructure::apns_push::build_apns_payload(&task, &plan, Lang::Zh, false)
                .expect("payload");
        assert_eq!(apns["aps"]["alert"]["body"], "Quiet");
        assert!(apns["aps"]["sound"].is_null());
        assert_eq!(apns["sound"], 0);
        let request = crate::infrastructure::getui_push::build_getui_push_request(
            &task,
            &plan,
            &row,
            &crate::infrastructure::getui_push::GetuiAndroidVendor::default(),
            1000,
        )
        .expect("request");
        let getui = crate::infrastructure::getui_push::build_getui_single_cid_body(&request);
        assert!(getui["push_channel"]["ios"]["aps"]["sound"].is_null());
        assert!(getui["push_channel"]["ios"]["auto_badge"].is_null());
    }

    /// 一条带推送文案、点名到接收者的消息任务。
    fn mention_task(preview: NotificationPreview) -> PushTaskEnvelope {
        PushTaskEnvelope {
            push_payload: push_info_payload("小明", "今晚的方案我改好了"),
            alert_class: AlertClass::Mention as i32,
            preview: preview as i32,
            ..envelope(PushDisposition::Loud)
        }
    }

    fn with_locale(mut row: DevicePushToken, locale: &str) -> DevicePushToken {
        row.locale = locale.into();
        row
    }

    /// Android 数据消息里的标题正文按绑定的语言渲染模板、按预览设置露出内容。
    #[test]
    fn android_text_follows_the_binding_language_and_the_preview_setting() {
        for (preview, locale, title, text) in [
            (
                NotificationPreview::Full,
                "",
                "小明",
                "[有人@你] 今晚的方案我改好了",
            ),
            (
                NotificationPreview::Full,
                "en-US",
                "小明",
                "[Mentioned you] 今晚的方案我改好了",
            ),
            (
                NotificationPreview::SenderOnly,
                "zh-Hans-CN",
                "小明",
                "[有人@你] 发来一条消息",
            ),
            (
                NotificationPreview::SenderOnly,
                "ja-JP",
                "小明",
                "[Mentioned you] sent you a message",
            ),
            (
                NotificationPreview::Hidden,
                "zh-TW",
                "新消息",
                "你收到一条新消息",
            ),
            (
                NotificationPreview::Hidden,
                "en",
                "New message",
                "You have a new message",
            ),
        ] {
            let v = body(&mention_task(preview), &with_locale(token("t"), locale)).expect("body");
            let data = &v["message"]["data"];
            assert_eq!(
                (data["title"].as_str(), data["body"].as_str()),
                (Some(title), Some(text)),
                "{preview:?} {locale:?}"
            );
        }

        // 静默覆盖的条数前缀、撤回替换与摘要的模板也随语言变。
        let english = with_locale(token("t"), "en-GB");
        let mut quiet = envelope(PushDisposition::Quiet);
        quiet.count = 4;
        let v = body(&quiet, &english).expect("body");
        assert_eq!(v["message"]["data"]["title"], "[4 messages] New message");
        let v = body(&envelope(PushDisposition::RecallReplace), &english).expect("body");
        assert_eq!(v["message"]["data"]["body"], "Message recalled");
        let mut digest = envelope(PushDisposition::Digest);
        digest.count = 9;
        digest.digest_conversations = 4;
        let v = body(&digest, &english).expect("body");
        assert_eq!(v["message"]["data"]["body"], "9 new messages in 4 chats");
        assert_eq!(v["message"]["android"]["priority"], "high");
        assert!(v["message"]["android"].get("collapse_key").is_none());
    }

    /// 经 FCM 的苹果设备：通知块与 `aps.alert` 是同一份文案，同样按语言与预览设置渲染。
    #[test]
    fn apple_text_follows_the_binding_language_and_the_preview_setting() {
        let row = with_locale(ios_token("t"), "en-US");
        let v = body(&mention_task(NotificationPreview::SenderOnly), &row).expect("body");
        let expected = json!({ "title": "小明", "body": "[Mentioned you] sent you a message" });
        assert_eq!(v["message"]["notification"], expected);
        assert_eq!(v["message"]["apns"]["payload"]["aps"]["alert"], expected);
        assert!(v["message"]["data"].get("title").is_none());

        let v = body(&mention_task(NotificationPreview::Hidden), &ios_token("t")).expect("body");
        let expected = json!({ "title": "新消息", "body": "你收到一条新消息" });
        assert_eq!(v["message"]["notification"], expected);
        assert_eq!(v["message"]["apns"]["payload"]["aps"]["alert"], expected);
    }

    /// 投递时取的是目标绑定登记的语言：同一条任务发到英文设备上就是英文模板。
    #[tokio::test]
    async fn delivery_renders_in_the_language_of_the_binding() {
        let accepted = r#"{"name":"projects/local-test/messages/one"}"#.to_string();
        let (endpoint, peer) = http_peer(vec![(200, accepted)]).await;
        let repo = Arc::new(Tokens::default());
        *repo.rows.lock().unwrap() = vec![with_locale(token("device-a"), "en-US")];
        let mut task = envelope(PushDisposition::Loud);
        task.push_payload = push_info_payload("", "");
        deliver(&test_executor(endpoint, repo.clone()), &repo, &task)
            .await
            .unwrap();
        let bodies = peer.await.unwrap();
        assert_eq!(bodies[0]["message"]["data"]["title"], "New message");
        assert_eq!(
            bodies[0]["message"]["data"]["body"],
            "You have a new message"
        );
    }

    /// 429 是限流（Retry-After 照办），5xx 可重试，401 是凭据问题，其余 4xx 是终态；
    /// 决策链里的代码取 FCM 的错误码，不带应答正文。
    #[test]
    fn answers_are_classified_for_switching_and_throttling() {
        use reqwest::StatusCode;
        let quota = r#"{"error":{"status":"RESOURCE_EXHAUSTED","message":"PRIVATE","details":[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"QUOTA_EXCEEDED"}]}}"#;
        let code = fcm_error_code(StatusCode::TOO_MANY_REQUESTS, quota);
        assert_eq!(code, "QUOTA_EXCEEDED");
        assert!(matches!(
            fcm_failure(StatusCode::TOO_MANY_REQUESTS, code, Some(Duration::from_secs(20))),
            BindingOutcome::Throttled {
                retry_after: Some(after),
                scope: ThrottleScope::Application,
                ..
            } if after == Duration::from_secs(20)
        ));
        assert_eq!(fcm_error_code(StatusCode::BAD_GATEWAY, "PRIVATE"), "502");
        assert!(matches!(
            fcm_failure(StatusCode::SERVICE_UNAVAILABLE, "503".into(), None),
            BindingOutcome::Retryable {
                reason: FailureReason::Server,
                ..
            }
        ));
        assert!(matches!(
            fcm_failure(StatusCode::UNAUTHORIZED, "UNAUTHENTICATED".into(), None),
            BindingOutcome::Retryable {
                reason: FailureReason::Auth,
                ..
            }
        ));
        assert!(matches!(
            fcm_failure(StatusCode::BAD_REQUEST, "INVALID_ARGUMENT".into(), None),
            BindingOutcome::Terminal {
                reason: FailureReason::Rejected,
                ..
            }
        ));
    }

    /// 端上替换显示名用的键随 data 下发，值都是字符串。
    #[test]
    fn data_carries_the_sender_and_conversation_kind() {
        use flare_proto::common::{ConversationType, Message};
        use prost::Message as _;
        let mut task = envelope(PushDisposition::Loud);
        task.push_payload = flare_grpc_proto::access_gateway::PushMessageRequest {
            messages: vec![Message {
                sender_id: "u-2".into(),
                sender_name: "小明".into(),
                conversation_type: ConversationType::Group as i32,
                offline_push_info: Some(flare_proto::common::OfflinePushInfo {
                    title: "项目群".into(),
                    body: "小明: 今天几点开会".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode_to_vec();
        let v = body(&task, &token("t")).expect("body");
        let data = &v["message"]["data"];
        assert_eq!(data["sender_id"], "u-2");
        assert_eq!(data["sender_name"], "小明");
        assert_eq!(data["kind"], "group");
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
