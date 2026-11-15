//! APNs（Apple Push Notification service）离线推送通道。
//!
//! 本适配器只发送 regular alert；VoIP、Live Activity、Critical Alert 需要独立的
//! typed 负载、entitlement 与设备生命周期，不因接入 APNs 就视为已经实现。
//!
//! ## 认证：provider token（JWT）
//!
//! 用 p8 私钥以 ES256 签发，header 带 `kid`（Key ID），claims 带 `iss`（Team ID）。
//! 签名只走 aws-lc 的 ES256（[`vendor_jwt`]），不经进程级默认 JWT provider。
//! **Apple 要求 token 有效期在 20–60 分钟之间，且刷新间隔不得短于 20 分钟** ——
//! 刷得太勤会被判定为滥用并拒绝（403 TooManyProviderTokenUpdates）。
//! 这里按 50 分钟刷新，两侧都留足余量。
//!
//! ## HTTP/2
//!
//! APNs 只接受 HTTP/2。客户端必须开启 `http2` 特性，否则连接直接失败。
//!
//! ## 处置
//!
//! 同一会话的通知靠 `apns-collapse-id` 互相覆盖，一个会话在通知中心只占一条。
//! 响铃与摘要用优先级 10、带 `sound`；静默覆盖与撤回替换用优先级 5、`passive`，
//! 只改内容不再打扰。仅角标的推送在应用被杀时也能更新角标。
//! 这里不远程移除已经展示的横幅：清除指令带了角标就只更新角标，没带则不调用 APNs。
//!
//! 带通知扩展的 iOS 应用（绑定能力含 `serviceExtension`）收到要展示的通知（响铃、摘要、撤回替换）时，
//! `aps` 带 `mutable-content: 1`，扩展据此用本地的会话名与联系人显示名重新渲染并记下到达回执。
//! 没有扩展的应用不带：带了也只是让系统白等。
//!
//! 429 按限流报给调用方（带 Retry-After），由它决定换绑定还是攒成摘要。APNs 的 429 说的是
//! 「对同一个设备令牌请求太多」（Apple《Handling notification responses from APNs》），
//! 只限这一条绑定，不限整个应用；服务端错误与无应答是可重试的失败，计入厂商熔断。
//!
//! `aps.alert` 的标题正文按目标绑定登记的语言渲染；露出多少由接收者的预览设置决定，
//! 规则在 `push_display` 里，三家共用。
//!
//! ## topic 与环境按绑定定
//!
//! 一个应用可以有多个包名（主应用、企业版、开发版），同一把 p8 密钥覆盖同一团队下的所有包名。
//! `apns-topic` 取绑定登记的应用标识，它必须列在凭据的 `topics` 里；沙盒还是生产也按绑定定，
//! 开发构建拿到的令牌只在沙盒有效。凭据不再有全局的 topic 与环境。

use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flare_im_contracts::domain::push_binding::APNS_ENVIRONMENT_SANDBOX;
use flare_im_contracts::{Ctx, DevicePushToken};
use flare_notify_rules::text::Lang;
use flare_proto::PushTaskEnvelope;
use flare_server_core::error::{ErrorCode, FlareError};
use prometheus::{IntCounterVec, Opts};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::RwLock;

use crate::domain::offline_delivery::DeviceTokenRepository;
use crate::domain::vendor_delivery::{
    BindingDelivery, BindingOutcome, CAPABILITY_SERVICE_EXTENSION, FailureReason, ThrottleScope,
    binding_has_capability, is_alert_disposition,
};
use crate::infrastructure::push_display::{
    AlertText, ClientText, PushPlan, ValueShape, client_payload, push_plan,
};
use crate::infrastructure::{vendor_http, vendor_jwt};

/// 注册表中标识 APNs 通道的 provider 名。
pub const APNS_PROVIDER: &str = "apns";

const PROD_HOST: &str = "https://api.push.apple.com";
const SANDBOX_HOST: &str = "https://api.sandbox.push.apple.com";
/// Apple 允许 20–60 分钟；取 50 分钟，两侧都留余量。
const TOKEN_TTL: Duration = Duration::from_secs(50 * 60);
/// 一份凭据最多列多少个包名。一个团队的应用数远小于它，上限只防配置写错。
const MAX_TOPICS: usize = 64;

/// 因绑定与凭据对不上而没有发出去的 APNs 推送，按原因计：`no_app_bundle`（绑定没登记应用标识）、
/// `topic_not_allowed`（应用标识不在凭据的 topics 里）。两者都是配置问题，重试不会变好，
/// 任务进死信；计数让运维不必翻死信也能看出是哪一种。
static APNS_BINDING_REJECTED: LazyLock<Option<IntCounterVec>> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new(
            "push_worker_apns_binding_rejected_total",
            "APNs pushes not sent because the binding's app bundle is missing or not an allowed topic",
        ),
        &["reason"],
    )
    .ok()?;
    let _ = flare_im_service_kit::metrics::REGISTRY.register(Box::new(counter.clone()));
    Some(counter)
});

fn record_binding_rejected(reason: &str) {
    if let Some(counter) = APNS_BINDING_REJECTED.as_ref() {
        counter.with_label_values(&[reason]).inc();
    }
}

/// APNs 提供者凭据（`flare.mobile-push-applications/v2`）。
#[derive(Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApnsCredentials {
    /// Apple Developer Team ID（10 位）。
    pub team_id: String,
    /// p8 私钥的 Key ID（10 位）。
    pub key_id: String,
    /// p8 私钥内容（PEM）。**走环境变量或密钥管理注入，勿入版本控制。**
    pub private_key_pem: String,
    /// 允许作为 `apns-topic` 的包名。绑定登记的应用标识必须在其中。
    pub topics: Vec<String>,
}

impl ApnsCredentials {
    fn allows_topic(&self, topic: &str) -> bool {
        self.topics.iter().any(|allowed| allowed == topic)
    }
}

fn valid_topic(topic: &str) -> bool {
    !topic.is_empty()
        && topic.len() <= 255
        && topic
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-'))
}

/// Team ID 与 Key ID 是 10 位大写字母数字；包名至少一个、不重复、形状合法。私钥另行校验。
fn credentials_shape_valid(creds: &ApnsCredentials) -> bool {
    let id_valid = |value: &str| {
        value.len() == 10
            && value
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
    };
    id_valid(&creds.team_id)
        && id_valid(&creds.key_id)
        && !creds.topics.is_empty()
        && creds.topics.len() <= MAX_TOPICS
        && creds.topics.iter().all(|topic| valid_topic(topic))
        && !creds
            .topics
            .iter()
            .enumerate()
            .any(|(i, topic)| creds.topics[..i].contains(topic))
}

/// 绑定登记的环境决定发到哪个 Apple 环境；没写按生产。
fn apns_host(token: &DevicePushToken) -> &'static str {
    if token.environment == APNS_ENVIRONMENT_SANDBOX {
        SANDBOX_HOST
    } else {
        PROD_HOST
    }
}

/// 这条绑定的 `apns-topic`：绑定的应用标识，且必须是凭据允许的包名。不满足时是终态错误：
/// 拿别的包名去发，Apple 回 DeviceTokenNotForTopic，换多少次都一样。
fn apns_topic<'a>(
    creds: &ApnsCredentials,
    token: &'a DevicePushToken,
) -> Result<&'a str, FlareError> {
    let topic = token.app_bundle.as_str();
    if topic.is_empty() {
        record_binding_rejected("no_app_bundle");
        return Err(FlareError::localized(
            ErrorCode::MessageDeliveryFailed,
            "APNs binding has no app bundle",
        ));
    }
    if !creds.allows_topic(topic) {
        record_binding_rejected("topic_not_allowed");
        return Err(FlareError::localized(
            ErrorCode::MessageDeliveryFailed,
            "APNs binding app bundle is not an allowed topic",
        ));
    }
    Ok(topic)
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
        if !credentials_shape_valid(&creds) {
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
        let jwt =
            vendor_jwt::sign_vendor_jwt(jsonwebtoken::Algorithm::ES256, &header, &claims, &key)
                .map_err(|_| {
                    FlareError::localized(ErrorCode::InternalError, "APNs JWT signing failed")
                })?;

        *cached_slot = Some(CachedToken {
            value: jwt.clone(),
            refresh_after: now + TOKEN_TTL.as_secs(),
        });
        Ok(jwt)
    }
}

/// APNs 上这条任务有没有可做的事：展示的通知，或带了角标的仅角标与清除。
/// 与文案的语言无关，查绑定之前就能判定。
fn apns_has_work(envelope: &PushTaskEnvelope, plan: &PushPlan) -> bool {
    match plan {
        PushPlan::Alert(_) => true,
        PushPlan::BadgeOnly | PushPlan::Clear => envelope.badge.is_some(),
        PushPlan::Nothing => false,
    }
}

/// 这条推送是否让通知扩展先处理：绑定声明了通知扩展，且是要展示的通知（响铃、摘要、撤回替换）。
/// 静默覆盖、仅角标与清除不经扩展。
pub(crate) fn wants_mutable_content(envelope: &PushTaskEnvelope, token: &DevicePushToken) -> bool {
    is_alert_disposition(envelope) && binding_has_capability(token, CAPABILITY_SERVICE_EXTENSION)
}

/// 组装 `aps` 字典，标题正文按 `lang` 渲染。`None` 表示这条任务在 APNs 上没有可做的事。
/// `mutable` 见 [`wants_mutable_content`]。
///
/// 独立成纯函数便于测试：aps 结构写错的表现是「推送到达但不弹横幅」，
/// 排查起来比彻底失败更费劲。FCM 的 `apns` 块也用这一份，
/// 直连与经 FCM 转发的 iOS 设备才不会各走各的规则。
pub(crate) fn build_aps(
    envelope: &PushTaskEnvelope,
    plan: &PushPlan,
    lang: Lang,
    mutable: bool,
) -> Option<Value> {
    if !apns_has_work(envelope, plan) {
        return None;
    }
    match plan {
        PushPlan::Alert(alert) => {
            let AlertText { title, body } = alert.text(lang);
            let mut aps = json!({
                "alert": { "title": title, "body": body },
                "interruption-level": alert.interruption.apns_level(),
            });
            if mutable {
                // 让通知扩展在展示前按本地的会话名与显示名重新渲染。
                aps["mutable-content"] = json!(1);
            }
            if alert.play_sound {
                aps["sound"] = json!("default");
            }
            if !envelope.thread_key.is_empty() {
                aps["thread-id"] = json!(envelope.thread_key);
            }
            if let Some(badge) = envelope.badge {
                aps["badge"] = json!(badge);
            }
            Some(aps)
        }
        // 只带角标：没有 alert 也没有 sound，系统只改图标上的数字。
        // 清除指令带了角标时同样处理；没带就无事可做。
        PushPlan::BadgeOnly | PushPlan::Clear => {
            envelope.badge.map(|badge| json!({ "badge": badge }))
        }
        PushPlan::Nothing => None,
    }
}

/// 组装 APNs 通知负载，标题正文按目标绑定的语言渲染。`None` 表示直接确认任务，不调用 APNs。
///
/// 端上要读的键（[`client_payload`]）与 `aps` 平级，数字保持数字：客户端点击后据此直达会话，
/// 通知扩展据 `seq` 与 `disposition` 决定是否覆盖已展示的那条。标题正文只在 `aps.alert` 里，
/// iOS 平台模块自己把它们抄进键表。`collapse_id` 与 FCM data 同名；让系统覆盖通知的是
/// `apns-collapse-id` 头部。
pub fn build_apns_payload(
    envelope: &PushTaskEnvelope,
    plan: &PushPlan,
    lang: Lang,
    mutable: bool,
) -> Option<Value> {
    let aps = build_aps(envelope, plan, lang, mutable)?;
    let mut payload = client_payload(envelope, plan, ValueShape::Native, ClientText::Omitted);
    payload.insert("aps".into(), aps);
    Some(Value::Object(payload))
}

/// `apns-priority`：只有要立刻打扰用户的通知用 10；其余用 5，由系统择机送达，省电也不占配额。
pub(crate) fn apns_priority(plan: &PushPlan) -> &'static str {
    match plan {
        PushPlan::Alert(alert) if alert.high_priority => "10",
        _ => "5",
    }
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

/// 非 200、也不是确认失效的 APNs 应答怎么算。`code` 是 Apple 给的原因，没有时是状态码。
fn apns_failure(
    status: reqwest::StatusCode,
    code: String,
    retry_after: Option<Duration>,
) -> BindingOutcome {
    let rejected =
        || FlareError::localized(ErrorCode::MessageDeliveryFailed, "APNs delivery rejected");
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        // TooManyRequests：对同一个设备令牌请求太多。只这条绑定等，同一应用的其他设备照常走 APNs。
        return BindingOutcome::Throttled {
            retry_after,
            code,
            scope: ThrottleScope::Binding,
        };
    }
    if status.is_server_error() {
        return BindingOutcome::retryable(
            FlareError::localized(ErrorCode::ServiceUnavailable, "APNs delivery rejected"),
            FailureReason::Server,
            code,
        );
    }
    if status == reqwest::StatusCode::GONE {
        // 没有可信时间戳的「已注销」：不删令牌，稍后重试；不算 Apple 的故障。
        return BindingOutcome::retryable(
            FlareError::localized(ErrorCode::ServiceUnavailable, "APNs delivery rejected"),
            FailureReason::Rejected,
            code,
        );
    }
    if status == reqwest::StatusCode::FORBIDDEN {
        return BindingOutcome::terminal(rejected(), FailureReason::Auth, code);
    }
    BindingOutcome::terminal(rejected(), FailureReason::Rejected, code)
}

#[async_trait::async_trait]
impl BindingDelivery for ApnsOfflinePushExecutor {
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
        if !apns_has_work(envelope, &plan) || !token.usable_for_provider(APNS_PROVIDER) {
            return BindingOutcome::Skipped;
        }
        match self.tokens.was_accepted(token, envelope).await {
            Ok(true) => return BindingOutcome::AlreadyAccepted,
            Ok(false) => {}
            Err(error) => return BindingOutcome::registry(error),
        }
        if token.token.len() > 512 || !token.token.bytes().all(|b| b.is_ascii_hexdigit()) {
            return BindingOutcome::terminal(
                FlareError::localized(
                    ErrorCode::MessageDeliveryFailed,
                    "APNs device token shape rejected",
                ),
                FailureReason::Config,
                "token_shape",
            );
        }
        let topic = match apns_topic(&self.creds, token) {
            Ok(topic) => topic,
            Err(error) => return BindingOutcome::terminal(error, FailureReason::Config, "topic"),
        };
        // 文案按这条绑定登记的语言渲染。
        let Some(payload) = build_apns_payload(
            envelope,
            &plan,
            Lang::from_locale(&token.locale),
            wants_mutable_content(envelope, token),
        ) else {
            return BindingOutcome::Skipped;
        };
        let jwt = match self.provider_token().await {
            Ok(jwt) => jwt,
            Err(error) => {
                return BindingOutcome::from_error(error, FailureReason::Auth, "provider_token");
            }
        };
        match self.tokens.is_current(token).await {
            Ok(true) => {}
            Ok(false) => return BindingOutcome::Skipped,
            Err(error) => return BindingOutcome::registry(error),
        }
        let mut request = self
            .http
            .post(format!("{}/3/device/{}", apns_host(token), token.token))
            .bearer_auth(&jwt)
            .header("apns-topic", topic)
            // 只带角标的推送也属于 alert 类型；background 类型会被系统限流，角标不可靠。
            .header("apns-push-type", "alert")
            .header("apns-priority", apns_priority(&plan));
        if !envelope.collapse_key.is_empty() {
            request = request.header("apns-collapse-id", &envelope.collapse_key);
        }
        if let Some(expiration) = envelope.expire_at.filter(|value| *value > 0) {
            request = request.header(
                "apns-expiration",
                expiration_seconds(expiration).to_string(),
            );
        }
        let response = match request.json(&payload).send().await {
            Ok(response) => response,
            Err(_) => {
                return BindingOutcome::retryable(
                    vendor_http::failure("APNs transport failed; acceptance unknown"),
                    FailureReason::Transport,
                    "transport",
                );
            }
        };
        let status = response.status();
        if status == reqwest::StatusCode::OK {
            return match self.tokens.record_accepted(token, envelope).await {
                Ok(()) => BindingOutcome::Accepted,
                Err(error) => BindingOutcome::registry(error),
            };
        }
        let retry_after = vendor_http::retry_after(&response);
        let body = match vendor_http::body(response).await {
            Ok(body) => body,
            Err(error) => {
                return BindingOutcome::retryable(error, FailureReason::Server, status.as_str());
            }
        };
        let reason = parse_apns_reason(&body);
        let code = if reason == "Unknown" {
            status.as_str().to_string()
        } else {
            reason
        };
        if invalidation_applies(status, &body, token.registered_at_ms) {
            return match self.tokens.remove_device_token(ctx, token).await {
                Ok(()) => BindingOutcome::InvalidToken { code },
                Err(error) => BindingOutcome::registry(error),
            };
        }
        apns_failure(status, code, retry_after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::vendor_delivery::Vendor;
    use crate::infrastructure::push_test_support::{
        Tokens, deliver_governed, device_task, push_info_payload, token,
    };
    use flare_proto::common::{AlertClass, NotificationPreview, PushDisposition};

    const COLLAPSE_KEY: &str = "c:0123456789abcdef0123456789abcdef";

    /// 一条会话消息的离线任务：带合并键、分组键与会话 seq。
    fn envelope(disposition: PushDisposition) -> PushTaskEnvelope {
        PushTaskEnvelope {
            collapse_key: COLLAPSE_KEY.into(),
            thread_key: COLLAPSE_KEY.into(),
            alert_class: AlertClass::Normal as i32,
            conversation_seq: 42,
            ..device_task("device-a", disposition)
        }
    }

    /// 厂商会收到的负载与优先级（绑定没登记语言，按中文渲染）；`None` 表示不调用 APNs。
    fn request(envelope: &PushTaskEnvelope) -> Option<(Value, &'static str)> {
        request_in(envelope, Lang::Zh)
    }

    fn request_in(envelope: &PushTaskEnvelope, lang: Lang) -> Option<(Value, &'static str)> {
        let plan = push_plan(envelope).expect("plan");
        build_apns_payload(envelope, &plan, lang, false)
            .map(|payload| (payload, apns_priority(&plan)))
    }

    #[test]
    fn payload_has_aps_alert_and_routing_keys() {
        let (p, priority) = request(&envelope(PushDisposition::Loud)).expect("payload");
        assert!(p["aps"]["alert"]["title"].is_string());
        assert!(p["aps"]["alert"]["body"].is_string());
        // 绑定没有声明通知扩展：不让系统等一个不存在的扩展。
        assert!(p["aps"]["mutable-content"].is_null());
        assert_eq!(p["aps"]["sound"], "default");
        assert_eq!(p["aps"]["interruption-level"], "active");
        assert_eq!(p["aps"]["thread-id"], COLLAPSE_KEY);
        // 没给角标就不带 badge：带了 0 会把用户的角标清掉
        assert!(p["aps"]["badge"].is_null());
        assert_eq!(priority, "10");
        // 自定义字段必须与 aps 平级，嵌进 aps 里客户端读不到；数字保持数字。
        let mut expected = json!({
            "uid": "u1",
            "tenant_id": "t1",
            "conversation_id": "c1",
            "message_id": "m1",
            "seq": 42,
            "collapse_id": COLLAPSE_KEY,
            "thread_id": COLLAPSE_KEY,
            "disposition": "loud",
            "alert_class": "normal",
            "count": 1,
            "sound": 1,
            "sender_id": "",
            "sender_name": "",
            "kind": "group",
        });
        expected["aps"] = p["aps"].clone();
        assert_eq!(p, expected);
        // 标题正文只在 aps.alert 里；旧的键全部不再下发。
        for absent in [
            "title",
            "body",
            "badge",
            "collapse_key",
            "show_badge",
            "play_sound",
        ] {
            assert!(p.get(absent).is_none(), "{absent}");
        }
    }

    /// 静默覆盖只换内容：没有 sound、passive、优先级 5。
    /// 带了 sound 或用了优先级 10，刷屏的群每条消息都会再响一次。
    #[test]
    fn quiet_update_replaces_content_without_alerting_again() {
        let mut task = envelope(PushDisposition::Quiet);
        task.count = 3;
        task.badge = Some(12);
        let (p, priority) = request(&task).expect("payload");
        assert_eq!(p["aps"]["alert"]["title"], "[3条] 新消息");
        assert!(p["aps"]["sound"].is_null());
        assert_eq!(p["aps"]["interruption-level"], "passive");
        assert!(p["aps"]["mutable-content"].is_null());
        assert_eq!(p["aps"]["badge"], 12);
        assert_eq!(priority, "5");
        assert_eq!(p["disposition"], "quiet");
        assert_eq!(p["count"], 3);
        assert_eq!(p["badge"], 12);
        assert_eq!(p["sound"], 0);
    }

    #[test]
    fn urgent_digest_and_recall_map_to_their_interruption_levels() {
        let mut urgent = envelope(PushDisposition::Loud);
        urgent.alert_class = AlertClass::Urgent as i32;
        let (p, priority) = request(&urgent).expect("payload");
        assert_eq!(p["aps"]["interruption-level"], "time-sensitive");
        assert_eq!(p["alert_class"], "urgent");
        assert_eq!(priority, "10");

        let (p, priority) = request(&envelope(PushDisposition::Digest)).expect("payload");
        assert_eq!(p["aps"]["interruption-level"], "active");
        assert_eq!(p["aps"]["sound"], "default");
        assert_eq!(priority, "10");
        assert_eq!(p["disposition"], "digest");

        let (p, priority) = request(&envelope(PushDisposition::RecallReplace)).expect("payload");
        assert_eq!(p["aps"]["alert"]["body"], "撤回了一条消息");
        assert_eq!(p["aps"]["interruption-level"], "passive");
        assert!(p["aps"]["sound"].is_null());
        assert_eq!(priority, "5");
        assert_eq!(p["disposition"], "recall_replace");
    }

    /// 仅角标的负载里 aps 只有 badge：多出 alert 会弹一条空横幅，多出 sound 会凭空响一声。
    #[test]
    fn badge_only_payload_carries_nothing_but_the_badge() {
        let mut task = envelope(PushDisposition::BadgeOnly);
        task.badge = Some(0);
        let (p, priority) = request(&task).expect("payload");
        assert_eq!(p["aps"], json!({ "badge": 0 }));
        assert_eq!(priority, "5");
        assert_eq!(p["disposition"], "badge_only");
        assert_eq!(p["conversation_id"], "c1");
        assert_eq!(p["sound"], 0);
        assert!(p.get("title").is_none());

        // 清除指令带了角标就按仅角标发；没带则无事可做。
        task.disposition = PushDisposition::Clear as i32;
        task.badge = Some(4);
        let (p, priority) = request(&task).expect("payload");
        assert_eq!(p["aps"], json!({ "badge": 4 }));
        assert_eq!(priority, "5");
        assert_eq!(p["disposition"], "clear");
        task.badge = None;
        assert!(request(&task).is_none());
        task.disposition = PushDisposition::BadgeOnly as i32;
        assert!(request(&task).is_none());
    }

    /// `aps.alert` 按绑定的语言渲染模板、按预览设置露出内容；标题正文仍只在 `aps.alert` 里。
    #[test]
    fn alert_text_follows_the_binding_language_and_the_preview_setting() {
        let mut task = PushTaskEnvelope {
            push_payload: push_info_payload("小明", "今晚的方案我改好了"),
            alert_class: AlertClass::Mention as i32,
            ..envelope(PushDisposition::Loud)
        };
        for (preview, lang, title, body) in [
            (
                NotificationPreview::Full,
                Lang::Zh,
                "小明",
                "[有人@你] 今晚的方案我改好了",
            ),
            (
                NotificationPreview::Full,
                Lang::En,
                "小明",
                "[Mentioned you] 今晚的方案我改好了",
            ),
            (
                NotificationPreview::SenderOnly,
                Lang::Zh,
                "小明",
                "[有人@你] 发来一条消息",
            ),
            (
                NotificationPreview::SenderOnly,
                Lang::En,
                "小明",
                "[Mentioned you] sent you a message",
            ),
            (
                NotificationPreview::Hidden,
                Lang::Zh,
                "新消息",
                "你收到一条新消息",
            ),
            (
                NotificationPreview::Hidden,
                Lang::En,
                "New message",
                "You have a new message",
            ),
        ] {
            task.preview = preview as i32;
            let (p, _) = request_in(&task, lang).expect("payload");
            assert_eq!(
                p["aps"]["alert"],
                json!({ "title": title, "body": body }),
                "{preview:?} {lang:?}"
            );
            assert!(p.get("title").is_none() && p.get("body").is_none());
        }

        // 撤回替换与摘要的模板同样随语言变；摘要的合并键原样取自任务。
        let (p, _) =
            request_in(&envelope(PushDisposition::RecallReplace), Lang::En).expect("payload");
        assert_eq!(
            p["aps"]["alert"],
            json!({ "title": "New message", "body": "Message recalled" })
        );
        let mut digest = envelope(PushDisposition::Digest);
        digest.count = 12;
        digest.digest_conversations = 3;
        let (p, priority) = request_in(&digest, Lang::En).expect("payload");
        assert_eq!(
            p["aps"]["alert"],
            json!({ "title": "New message", "body": "12 new messages in 3 chats" })
        );
        assert_eq!(priority, "10");
        assert_eq!(p["collapse_id"], COLLAPSE_KEY);
        let (p, _) = request_in(&digest, Lang::Zh).expect("payload");
        assert_eq!(p["aps"]["alert"]["body"], "3 个会话有 12 条新消息");
    }

    /// 声明了通知扩展的绑定：响铃、摘要、撤回替换带 `mutable-content`，静默覆盖、仅角标与清除不带。
    #[test]
    fn mutable_content_needs_the_extension_and_a_shown_notification() {
        let mut row = apns_token("device-a");
        let loud = envelope(PushDisposition::Loud);
        assert!(!wants_mutable_content(&loud, &row));
        row.capabilities = vec!["serviceExtension".into()];
        for (disposition, mutable) in [
            (PushDisposition::Loud, true),
            (PushDisposition::Digest, true),
            (PushDisposition::RecallReplace, true),
            (PushDisposition::Quiet, false),
            (PushDisposition::BadgeOnly, false),
            (PushDisposition::Clear, false),
        ] {
            let mut task = envelope(disposition);
            task.badge = Some(1);
            assert_eq!(
                wants_mutable_content(&task, &row),
                mutable,
                "{disposition:?}"
            );
            let plan = push_plan(&task).expect("plan");
            let payload =
                build_apns_payload(&task, &plan, Lang::Zh, wants_mutable_content(&task, &row))
                    .expect("payload");
            assert_eq!(
                payload["aps"]["mutable-content"].as_i64(),
                mutable.then_some(1),
                "{disposition:?}"
            );
        }
    }

    /// 429 是这条绑定的限流（带 Retry-After 时照办），5xx 可重试并计入熔断，403 是凭据问题，
    /// 其余 4xx 是终态。
    #[test]
    fn answers_are_classified_for_switching_and_throttling() {
        use reqwest::StatusCode;
        let throttled = apns_failure(
            StatusCode::TOO_MANY_REQUESTS,
            "TooManyRequests".into(),
            Some(Duration::from_secs(7)),
        );
        assert!(matches!(
            throttled,
            BindingOutcome::Throttled {
                retry_after: Some(after),
                scope: ThrottleScope::Binding,
                ..
            } if after == Duration::from_secs(7)
        ));
        assert!(matches!(
            apns_failure(StatusCode::SERVICE_UNAVAILABLE, "503".into(), None),
            BindingOutcome::Retryable {
                reason: FailureReason::Server,
                ..
            }
        ));
        assert!(matches!(
            apns_failure(StatusCode::GONE, "Unregistered".into(), None),
            BindingOutcome::Retryable {
                reason: FailureReason::Rejected,
                ..
            }
        ));
        assert!(matches!(
            apns_failure(StatusCode::FORBIDDEN, "ExpiredProviderToken".into(), None),
            BindingOutcome::Terminal {
                reason: FailureReason::Auth,
                ..
            }
        ));
        assert!(matches!(
            apns_failure(StatusCode::BAD_REQUEST, "BadDeviceToken".into(), None),
            BindingOutcome::Terminal {
                reason: FailureReason::Rejected,
                ..
            }
        ));
    }

    fn credentials() -> ApnsCredentials {
        ApnsCredentials {
            team_id: "TEAM000000".into(),
            key_id: "KEY0000000".into(),
            private_key_pem: String::new(),
            topics: vec!["com.example.app".into(), "com.example.app.beta".into()],
        }
    }

    fn executor(tokens: Arc<Tokens>) -> Arc<ApnsOfflinePushExecutor> {
        // 私钥为空：一旦走到签发 provider token 就会失败，测试借此确认没有碰厂商。
        Arc::new(ApnsOfflinePushExecutor {
            creds: credentials(),
            http: vendor_http::client().unwrap(),
            tokens,
            cached: RwLock::new(None),
        })
    }

    /// 像 worker 那样投递：列出目标设备的绑定，经治理过的绑定选择，APNs 是唯一的厂商。
    async fn deliver(
        executor: &Arc<ApnsOfflinePushExecutor>,
        repo: &Tokens,
        task: &PushTaskEnvelope,
    ) -> Result<(), FlareError> {
        deliver_governed(Vendor::Apns, executor.clone(), repo, task).await
    }

    fn apns_token(device: &str) -> DevicePushToken {
        let mut row = token(device);
        row.platform = "ios".into();
        row.provider = APNS_PROVIDER.into();
        row.token = "0123456789abcdef".into();
        row.app_bundle = "com.example.app".into();
        row
    }

    /// topic 取绑定的应用标识，且必须是凭据允许的包名；缺了或不在其中都是终态错误，
    /// 不签发 provider token、不调用 Apple（私钥为空，走到签发这一步就会是另一种错误）。
    #[tokio::test]
    async fn the_topic_is_the_binding_bundle_and_must_be_allowed() {
        let creds = credentials();
        let mut row = apns_token("device-a");
        assert_eq!(apns_topic(&creds, &row).unwrap(), "com.example.app");
        row.app_bundle = "com.example.app.beta".into();
        assert_eq!(apns_topic(&creds, &row).unwrap(), "com.example.app.beta");

        for bundle in ["", "com.other.app"] {
            row.app_bundle = bundle.into();
            assert!(!apns_topic(&creds, &row).unwrap_err().is_retryable());
            let repo = Arc::new(Tokens::default());
            *repo.rows.lock().unwrap() = vec![row.clone()];
            let error = deliver(
                &executor(repo.clone()),
                &repo,
                &envelope(PushDisposition::Loud),
            )
            .await
            .unwrap_err();
            assert!(!error.is_retryable(), "{bundle:?}");
            assert!(repo.accepted.lock().unwrap().is_empty());
            assert!(repo.removed.lock().unwrap().is_empty());
        }
        let rejected = APNS_BINDING_REJECTED.as_ref().expect("metric");
        assert!(rejected.with_label_values(&["no_app_bundle"]).get() >= 1);
        assert!(rejected.with_label_values(&["topic_not_allowed"]).get() >= 1);
    }

    /// 凭据至少列一个包名，包名不重复、形状合法；沙盒与生产不再写在凭据里。
    #[test]
    fn credentials_list_valid_unique_topics() {
        assert!(credentials_shape_valid(&credentials()));
        for topics in [
            vec![],
            vec!["com.example.app".to_string(), "com.example.app".to_string()],
            vec!["com.example/app".to_string()],
            vec![String::new()],
            vec!["a".repeat(256)],
            (0..=MAX_TOPICS)
                .map(|i| format!("com.example.app{i}"))
                .collect(),
        ] {
            let creds = ApnsCredentials {
                topics,
                ..credentials()
            };
            assert!(!credentials_shape_valid(&creds), "{:?}", creds.topics);
        }
        assert!(
            serde_json::from_value::<ApnsCredentials>(json!({
                "team_id": "TEAM000000",
                "key_id": "KEY0000000",
                "private_key_pem": "x",
                "topic": "com.example.app",
                "sandbox": true,
            }))
            .is_err(),
            "v1 的 topic 与 sandbox 不再接受"
        );
    }

    #[tokio::test]
    async fn a_task_only_reaches_the_bindings_of_its_device() {
        let repo = Arc::new(Tokens::default());
        *repo.rows.lock().unwrap() = vec![apns_token("device-b")];
        let executor = executor(repo.clone());
        // 目标设备没有 APNs 绑定：同一用户另一台设备的绑定不能被顺带推送。
        deliver(&executor, &repo, &envelope(PushDisposition::Loud))
            .await
            .unwrap();
        // 目标设备有绑定时才会走到厂商这一步（这里停在签发 provider token）。
        *repo.rows.lock().unwrap() = vec![apns_token("device-a"), apns_token("device-b")];
        assert!(
            deliver(&executor, &repo, &envelope(PushDisposition::Loud))
                .await
                .is_err()
        );
        assert!(repo.accepted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn clear_without_badge_is_acknowledged_without_calling_apns() {
        let repo = Arc::new(Tokens::default());
        *repo.rows.lock().unwrap() = vec![apns_token("device-a")];
        let executor = executor(repo.clone());
        deliver(&executor, &repo, &envelope(PushDisposition::Clear))
            .await
            .unwrap();
        assert!(repo.accepted.lock().unwrap().is_empty());

        // 未指明处置的任务不可执行，是终态错误。
        let error = deliver(&executor, &repo, &envelope(PushDisposition::Unspecified))
            .await
            .unwrap_err();
        assert!(!error.is_retryable());
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

    /// 环境按绑定选：开发构建的 device token 只在沙箱有效，选错环境会得到 BadDeviceToken。
    /// 同一份凭据下沙盒与生产的设备各走各的。
    #[test]
    fn the_binding_environment_selects_the_apple_host() {
        let mut row = apns_token("device-a");
        row.environment = "sandbox".into();
        assert_eq!(apns_host(&row), SANDBOX_HOST);
        for environment in ["production", ""] {
            row.environment = environment.into();
            assert_eq!(apns_host(&row), PROD_HOST, "{environment:?}");
        }
    }
}
