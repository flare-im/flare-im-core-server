//! Getui RestAPI V2 offline delivery backend.
//!
//! Getui only carries notifications that are shown. Every request carries the client keys as a
//! transmission message (`push_message.transmission`), which the app's own code receives and
//! renders while the Getui channel is online. When it is offline, Getui falls back to the
//! vendor channels: APNs on iOS (`push_channel.ios`), and on Android the phone maker's system
//! channel (`push_channel.android.ups`), which shows a notification the system renders even
//! after the process was killed. Only `loud` and `digest` use the Android vendor notification:
//! most makers cannot replace a notification silently, so a quiet update or recall replacement
//! there would ring again; those reach Android through the transmission alone.
//!
//! Same-conversation notifications replace each other through `notify_id` (and
//! `apns-collapse-id` on the iOS channel). Badge-only and clear tasks have no reliable
//! equivalent on this channel and are acknowledged without a vendor call.
//!
//! Title and body are rendered in the language the binding registered (`locale`); how much
//! of the content they show follows the recipient's preview setting (`push_display`). The
//! vendor notification cuts them again to each maker's own limits (see [`vendor_options`]).
//!
//! Chat pushes are filed as instant messages with every maker that has such a class: Huawei
//! and vivo by category, Honor by importance, Xiaomi and OPPO as private messages through a
//! channel and a template registered with the maker. Notices (`kind` `notice`: system and
//! business notifications) are not chat and do not use the Xiaomi and OPPO private-message
//! channels and templates.
//!
//! An HTTP 429, a call-frequency business code or the daily push quota is reported as
//! throttling of the application (the caller switches to the device's next binding or holds
//! the task for a digest); server errors and transport failures are retryable and count
//! against the vendor's breaker. The codes and every request field name were checked against
//! the Getui REST v2 documentation on 2026-11-12 (pages cited where they are used); no request
//! has been sent to the Getui sandbox with them yet (`getui_sandbox_push`).

use std::fmt::Write as _;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::infrastructure::push_display::{
    AlertText, ChatKind, ClientText, PushPlan, ValueShape, client_payload, push_plan,
};
use crate::infrastructure::push_text::fit_utf16;
use crate::infrastructure::vendor_http;
use flare_im_contracts::{Ctx, DevicePushToken};
use flare_notify_rules::text::Lang;
use flare_proto::PushTaskEnvelope;
use flare_proto::common::PushDisposition;
use flare_proto::common::message_content::Content as MessageContentVariant;
use flare_server_core::error::{ErrorCode, FlareError};
use regex::Regex;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use unicode_segmentation::UnicodeSegmentation;

use crate::domain::DeviceTokenRepository;
use crate::domain::offline_delivery::task_disposition;
use crate::domain::vendor_delivery::{
    BindingDelivery, BindingOutcome, FailureReason, ThrottleScope,
};

const GETUI_PROVIDER: &str = "getui";
const TOKEN_REFRESH_SKEW_MS: i64 = 60_000;
const GETUI_TOKEN_EXPIRED_CODE: i64 = 10001;
/// Business codes that mean "too many calls": 10003 authentication frequency per minute
/// exceeded, 10005 call frequency per minute exceeded (both with HTTP 401). Getui REST v2
/// "业务返回码说明": https://docs.getui.com/getui/server/rest_v2/code/
const GETUI_RATE_LIMIT_CODES: [i64; 2] = [10003, 10005];
/// 30009: the daily push total is exceeded (HTTP 403, same page). The page does not name the
/// endpoints it applies to; it is a quota answer, so it is throttling, never a rejection of
/// the task.
const GETUI_DAILY_QUOTA_CODE: i64 = 30009;
/// How long a daily quota answer keeps the application off Getui when it gave no Retry-After
/// (the longest throttle window the binding selection honours).
const GETUI_DAILY_QUOTA_RETRY: Duration = Duration::from_secs(300);
/// `settings.ttl` is at most three days (Getui REST v2 "公共参数":
/// https://docs.getui.com/getui/server/rest_v2/common_args/).
const GETUI_MAX_TTL_MS: u64 = 259_200_000;
const E2EE_PLACEHOLDER_REASON: &str = "e2e_ciphertext";

/// Per-maker text limits of the vendor notification, in characters (Chinese and Latin
/// characters count one each), from the `push_channel.android.ups.notification` table of
/// Getui's "公共参数" page. Huawei and Honor give no title limit, Honor no body limit. The page
/// also lists Meizu (title 32, body 100), a channel Flare does not configure.
const XIAOMI_TITLE_MAX: usize = 50;
const XIAOMI_BODY_MAX: usize = 128;
const OPPO_TITLE_MAX: usize = 50;
const OPPO_BODY_MAX: usize = 200;
const VIVO_TITLE_MAX: usize = 40;
const VIVO_BODY_MAX: usize = 100;
const HUAWEI_BODY_MAX: usize = 256;
/// `ups.notification` is one title and one body for every maker; Getui says to keep within the
/// smallest limit when a push may reach several makers. The makers' own fields (the Xiaomi and
/// OPPO template variables) are cut to their own, larger limits. Getui documents no per-maker
/// override of the notification title and body, so none is sent.
const SHARED_TITLE_MAX: usize = smallest(&[XIAOMI_TITLE_MAX, OPPO_TITLE_MAX, VIVO_TITLE_MAX]);
const SHARED_BODY_MAX: usize = smallest(&[
    HUAWEI_BODY_MAX,
    XIAOMI_BODY_MAX,
    OPPO_BODY_MAX,
    VIVO_BODY_MAX,
]);

/// OPPO `notify_level` values (Getui "多厂商参数", OP `/notify_level`): 1 notification bar
/// only; 2 notification bar and lock screen, which OPPO describes as also ringing and vibrating
/// (Getui's table leaves the sound out), OPPO's default and the level the Communication &
/// Services permission comes with, so the default for chat; 16 adds the banner and needs a
/// separate approval ("强提醒").
const OPPO_NOTIFY_LEVELS: [u32; 3] = [1, 2, 16];
const OPPO_DEFAULT_NOTIFY_LEVEL: u32 = 2;
/// OPPO's message category for instant messages under the new message classification.
const OPPO_IM_CATEGORY: &str = "IM";

/// Characters Xiaomi refuses in a template variable value (Getui "多厂商参数", XM
/// `/extra.template_param`: no emoji or other special symbols, i.e. Unicode Other Symbol
/// `\p{So}`, and no unassigned code points), plus the pieces of emoji sequences that are not
/// symbols themselves: zero width joiner, variation selectors, skin tone modifiers, the keycap
/// mark and tag characters. A grapheme containing any of them is dropped whole.
static XIAOMI_REFUSED: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"[\p{So}\p{Cn}\x{200D}\x{FE0E}\x{FE0F}\x{20E3}\x{1F3FB}-\x{1F3FF}\x{E0020}-\x{E007F}]",
    )
    .expect("constant pattern")
});

const fn smallest(limits: &[usize]) -> usize {
    let mut smallest = usize::MAX;
    let mut index = 0;
    while index < limits.len() {
        if limits[index] < smallest {
            smallest = limits[index];
        }
        index += 1;
    }
    smallest
}

#[derive(Clone)]
pub struct GetuiConfig {
    pub app_id: String,
    pub app_key: String,
    pub master_secret: String,
    pub base_url: String,
    pub default_ttl_ms: u64,
    pub request_timeout_ms: u64,
}

impl GetuiConfig {
    pub fn new(
        app_id: String,
        app_key: String,
        master_secret: String,
        base_url: Option<String>,
        default_ttl_ms: u64,
        request_timeout_ms: u64,
    ) -> Result<Self, FlareError> {
        let app_id = app_id.trim().to_string();
        let app_key = app_key.trim().to_string();
        let master_secret = master_secret.trim().to_string();
        if app_id.is_empty() || app_key.is_empty() || master_secret.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "getui app_id/app_key/master_secret are required",
            ));
        }
        let base_url = base_url
            .map(|value| value.trim().trim_end_matches('/').to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| format!("https://restapi.getui.com/v2/{app_id}"));
        if !app_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || app_id.len() > 128
            || base_url != format!("https://restapi.getui.com/v2/{app_id}")
        {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "getui credential destination rejected",
            ));
        }
        Ok(Self {
            app_id,
            app_key,
            master_secret,
            base_url,
            default_ttl_ms,
            request_timeout_ms,
        })
    }
}

#[derive(Debug, Clone)]
struct GetuiAccessToken {
    token: String,
    expire_time_ms: i64,
}

#[derive(Debug, Deserialize)]
struct GetuiResponse<T> {
    code: i64,
    #[serde(default)]
    msg: String,
    data: Option<T>,
}

#[derive(Debug, Deserialize)]
struct GetuiAuthData {
    token: String,
    #[serde(deserialize_with = "deserialize_i64")]
    expire_time: i64,
}

#[derive(Debug, Serialize)]
struct GetuiAuthRequest<'a> {
    sign: String,
    timestamp: &'a str,
    appkey: &'a str,
}

/// Optional Android vendor-channel parameters, from `getui.android_vendor` in the application
/// credentials. Each maker files instant messages under its own category or channel; without
/// the right one the message is treated as marketing, rate-limited per day and shown silently.
/// Applying for the IM class, and registering the Xiaomi and OPPO private-message channels and
/// templates, with each maker is an operator task. Unset values are not sent.
///
/// The Xiaomi and OPPO templates have two variables, `title` and `content`, which the worker
/// fills from the rendered title and body. They are written the way each maker's template form
/// writes a variable: `{$title$}` and `{$content$}` for Xiaomi (Xiaomi's template guide); for
/// OPPO, `${title}$` and `${content}$` as OPPO's private-template guide is quoted elsewhere
/// (aggregator examples also show `${city}` without the closing `$`; follow OPPO's form). Only
/// the variable names reach the request, as the keys of the parameter JSON.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GetuiAndroidVendor {
    /// Huawei message category, e.g. `IM`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub huawei_category: Option<String>,
    /// Honor notification importance, e.g. `NORMAL`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub honor_importance: Option<String>,
    /// Xiaomi private-message channel id for direct chats ("好友聊天"), the id Xiaomi generates
    /// when it approves the channel. Also used for group chats without a group channel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub xiaomi_channel_id: Option<String>,
    /// Xiaomi private-message channel id for group chats ("群组聊天"); falls back to
    /// `xiaomi_channel_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub xiaomi_group_channel_id: Option<String>,
    /// Xiaomi private-message template id for direct chats. Required with `xiaomi_channel_id`:
    /// since 2026-07-01 Xiaomi wants a template on every private message (apps that used
    /// private messages before have until 2026-12-31).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub xiaomi_template_id: Option<String>,
    /// Xiaomi private-message template id for group chats; falls back to `xiaomi_template_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub xiaomi_group_template_id: Option<String>,
    /// OPPO private-message channel id of the old classification: the app's own Android
    /// notification channel registered with OPPO as a private channel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oppo_channel_id: Option<String>,
    /// OPPO private-message template id under the new message classification. When set, chat
    /// pushes go out as category `IM` with this template.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oppo_private_template_id: Option<String>,
    /// OPPO `notify_level` of chat pushes under the new classification: 1, 2 or 16, default 2.
    /// Only with `oppo_private_template_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oppo_notify_level: Option<u32>,
    /// vivo message category, e.g. `IM`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vivo_category: Option<String>,
}

impl GetuiAndroidVendor {
    /// Identifiers are short: visible ASCII without spaces, at most 64 bytes. A Xiaomi channel
    /// and its template come together (the group ones only next to them), and OPPO's
    /// `notify_level` is one OPPO knows and only goes with the template it applies to.
    pub fn validate(&self) -> Result<(), FlareError> {
        let rejected =
            |reason: &'static str| Err(FlareError::localized(ErrorCode::InvalidParameter, reason));
        let identifiers_valid = [
            &self.huawei_category,
            &self.honor_importance,
            &self.xiaomi_channel_id,
            &self.xiaomi_group_channel_id,
            &self.xiaomi_template_id,
            &self.xiaomi_group_template_id,
            &self.oppo_channel_id,
            &self.oppo_private_template_id,
            &self.vivo_category,
        ]
        .into_iter()
        .flatten()
        .all(|value| {
            !value.is_empty() && value.len() <= 64 && value.bytes().all(|b| b.is_ascii_graphic())
        });
        if !identifiers_valid {
            return rejected("getui android vendor options rejected");
        }
        if self.xiaomi_channel_id.is_some() != self.xiaomi_template_id.is_some() {
            return rejected(
                "getui android_vendor: xiaomi_channel_id and xiaomi_template_id go together",
            );
        }
        if (self.xiaomi_group_channel_id.is_some() || self.xiaomi_group_template_id.is_some())
            && self.xiaomi_channel_id.is_none()
        {
            return rejected(
                "getui android_vendor: the xiaomi group channel and template need xiaomi_channel_id",
            );
        }
        if let Some(level) = self.oppo_notify_level {
            if !OPPO_NOTIFY_LEVELS.contains(&level) {
                return rejected("getui android_vendor: oppo_notify_level must be 1, 2 or 16");
            }
            if self.oppo_private_template_id.is_none() {
                return rejected(
                    "getui android_vendor: oppo_notify_level needs oppo_private_template_id",
                );
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct GetuiPushRequest {
    pub cid: String,
    pub request_id: String,
    pub title: String,
    pub body: String,
    /// The client keys (string values, title and body included), sent as the transmission and
    /// as the iOS channel payload.
    pub payload: Map<String, Value>,
    pub ttl_ms: u64,
    pub play_sound: bool,
    pub show_badge: bool,
    /// Notifications sharing this key replace each other; empty means no replacement.
    pub collapse_key: String,
    /// Grouping key for the notification centre; empty means ungrouped.
    pub thread_key: String,
    /// Absolute badge. Without one, a message that counts toward the badge still adds one.
    pub badge: Option<u32>,
    /// Send the Android vendor-channel notification: `loud` and `digest` only.
    pub vendor_notification: bool,
    /// The binding's applicationId: the package whose open activity a tap on the vendor
    /// notification starts. Never empty for a request the executor sends.
    pub app_bundle: String,
    /// Per-maker options of the vendor notification (`ups.options`), resolved for this push:
    /// its conversation kind picks the Xiaomi channel and template, the template variables
    /// carry its text. Empty: none are sent.
    pub vendor_options: Map<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GetuiPushOutcome {
    Delivered,
    InvalidTarget,
    /// HTTP 429, a call-frequency business code or the daily quota (`code`), with Retry-After
    /// when given.
    Throttled {
        retry_after: Option<Duration>,
        code: i64,
    },
}

/// What was asked for, or a call-frequency refusal (`code`).
enum Answer<T> {
    Ok(T),
    Throttled(i64),
}

fn is_throttling_code(code: i64) -> bool {
    GETUI_RATE_LIMIT_CODES.contains(&code) || code == GETUI_DAILY_QUOTA_CODE
}

#[async_trait::async_trait]
pub trait GetuiPusher: Send + Sync {
    async fn push_single_cid(
        &self,
        request: &GetuiPushRequest,
    ) -> Result<GetuiPushOutcome, FlareError>;
}

pub struct GetuiClient {
    http: reqwest::Client,
    config: GetuiConfig,
    access_token: Mutex<Option<GetuiAccessToken>>,
}

impl GetuiClient {
    pub fn new(config: GetuiConfig) -> Result<Self, FlareError> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_millis(
                config.request_timeout_ms.clamp(1_000, 10_000),
            ))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .build()
            .map_err(|_| vendor_http::failure("build getui HTTP client failed"))?;
        Ok(Self {
            http,
            config,
            access_token: Mutex::new(None),
        })
    }

    async fn bearer_token(&self) -> Result<Answer<String>, FlareError> {
        let now = now_ms();
        let mut guard = self.access_token.lock().await;
        if let Some(token) = guard.as_ref()
            && token.expire_time_ms > now + TOKEN_REFRESH_SKEW_MS
        {
            return Ok(Answer::Ok(token.token.clone()));
        }
        match self.authenticate(now).await? {
            Answer::Ok(token) => {
                *guard = Some(token.clone());
                Ok(Answer::Ok(token.token))
            }
            Answer::Throttled(code) => Ok(Answer::Throttled(code)),
        }
    }

    /// A call-frequency refusal of the authentication (10003) is throttling like one of the
    /// push itself: the push cannot go out now, and asking again at once only extends it.
    async fn authenticate(
        &self,
        timestamp_ms: i64,
    ) -> Result<Answer<GetuiAccessToken>, FlareError> {
        let timestamp = timestamp_ms.to_string();
        let body = GetuiAuthRequest {
            sign: getui_auth_sign(&self.config.app_key, &timestamp, &self.config.master_secret),
            timestamp: &timestamp,
            appkey: &self.config.app_key,
        };
        let response = self
            .http
            .post(format!("{}/auth", self.config.base_url))
            .json(&body)
            .send()
            .await
            .map_err(retryable_getui_error)?;

        let status = response.status();
        let payload: GetuiResponse<GetuiAuthData> =
            serde_json::from_str(&vendor_http::body(response).await?)
                .map_err(|_| vendor_http::failure("getui auth response invalid"))?;
        if payload.code != 0 {
            if is_throttling_code(payload.code) {
                return Ok(Answer::Throttled(payload.code));
            }
            return Err(getui_business_error(
                payload.code,
                &payload.msg,
                "getui auth",
            ));
        }
        if !status.is_success() {
            return Err(http_status_error(status, "getui auth"));
        }
        let Some(data) = payload.data else {
            return Err(FlareError::localized(
                ErrorCode::ServiceUnavailable,
                "getui auth response missing token data",
            ));
        };
        if data.token.trim().is_empty()
            || data.token.len() > 8192
            || data
                .token
                .bytes()
                .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
            || data.expire_time <= timestamp_ms + TOKEN_REFRESH_SKEW_MS
        {
            return Err(FlareError::localized(
                ErrorCode::ServiceUnavailable,
                "getui auth response token is empty",
            ));
        }
        Ok(Answer::Ok(GetuiAccessToken {
            token: data.token,
            expire_time_ms: data.expire_time,
        }))
    }

    async fn push_single_cid_inner(
        &self,
        request: &GetuiPushRequest,
    ) -> Result<GetuiPushOutcome, FlareError> {
        let throttled = |code: i64| GetuiPushOutcome::Throttled {
            retry_after: None,
            code,
        };
        let token = match self.bearer_token().await? {
            Answer::Ok(token) => token,
            Answer::Throttled(code) => return Ok(throttled(code)),
        };
        match self.push_single_cid_with_token(&token, request).await {
            Ok(outcome) => Ok(outcome),
            Err(error) if error.code() == Some(ErrorCode::AuthenticationRequired) => {
                {
                    let mut guard = self.access_token.lock().await;
                    if guard.as_ref().is_some_and(|cached| cached.token == token) {
                        *guard = None;
                    }
                }
                match self.bearer_token().await? {
                    Answer::Ok(refreshed) => {
                        self.push_single_cid_with_token(&refreshed, request).await
                    }
                    Answer::Throttled(code) => Ok(throttled(code)),
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn push_single_cid_with_token(
        &self,
        token: &str,
        request: &GetuiPushRequest,
    ) -> Result<GetuiPushOutcome, FlareError> {
        let body = build_getui_single_cid_body(request);
        let response = self
            .http
            .post(format!("{}/push/single/cid", self.config.base_url))
            .header("token", token)
            .json(&body)
            .send()
            .await
            .map_err(retryable_getui_error)?;

        let status = response.status();
        let retry_after = vendor_http::retry_after(&response);
        let response_text = vendor_http::body(response).await?;
        let parsed = serde_json::from_str::<GetuiResponse<Value>>(&response_text).ok();
        if status == StatusCode::TOO_MANY_REQUESTS {
            return Ok(GetuiPushOutcome::Throttled {
                retry_after,
                code: parsed
                    .as_ref()
                    .map(|payload| payload.code)
                    .filter(|code| *code != 0)
                    .unwrap_or(429),
            });
        }
        if let Some(payload) = parsed {
            if payload.code == 0 && status == StatusCode::OK {
                return Ok(GetuiPushOutcome::Delivered);
            }
            if is_throttling_code(payload.code) {
                let quota =
                    (payload.code == GETUI_DAILY_QUOTA_CODE).then_some(GETUI_DAILY_QUOTA_RETRY);
                return Ok(GetuiPushOutcome::Throttled {
                    retry_after: retry_after.or(quota),
                    code: payload.code,
                });
            }
            if status == StatusCode::BAD_REQUEST
                && is_invalid_target_business_error(payload.code, &payload.msg)
            {
                return Ok(GetuiPushOutcome::InvalidTarget);
            }
            if payload.code != 0 && !status.is_server_error() {
                return Err(getui_business_error(
                    payload.code,
                    &payload.msg,
                    "getui push single cid",
                ));
            }
        }
        if !status.is_success() {
            return Err(http_status_error(status, "getui push single cid"));
        }
        Err(FlareError::localized(
            ErrorCode::ServiceUnavailable,
            "getui push single cid response invalid",
        ))
    }
}

#[async_trait::async_trait]
impl GetuiPusher for GetuiClient {
    async fn push_single_cid(
        &self,
        request: &GetuiPushRequest,
    ) -> Result<GetuiPushOutcome, FlareError> {
        self.push_single_cid_inner(request).await
    }
}

pub struct GetuiOfflinePushExecutor {
    tokens: Arc<dyn DeviceTokenRepository>,
    client: Arc<dyn GetuiPusher>,
    default_ttl_ms: u64,
    android_vendor: GetuiAndroidVendor,
}

impl GetuiOfflinePushExecutor {
    pub fn new(
        tokens: Arc<dyn DeviceTokenRepository>,
        client: Arc<dyn GetuiPusher>,
        default_ttl_ms: u64,
        android_vendor: GetuiAndroidVendor,
    ) -> Self {
        Self {
            tokens,
            client,
            default_ttl_ms,
            android_vendor,
        }
    }
}

#[async_trait::async_trait]
impl BindingDelivery for GetuiOfflinePushExecutor {
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
        if !matches!(plan, PushPlan::Alert(_)) || !token.usable_for_provider(GETUI_PROVIDER) {
            return BindingOutcome::Skipped;
        }
        // Registration requires the app bundle: the vendor notification's tap names a component
        // in that package. Without it no request can be built, and no retry helps.
        if token.app_bundle.is_empty() {
            tracing::warn!(
                tenant_id = %envelope.tenant_id,
                user_id = %envelope.user_id,
                device_id = %token.device_id,
                "getui binding has no app bundle"
            );
            return BindingOutcome::terminal(
                FlareError::localized(
                    ErrorCode::MessageDeliveryFailed,
                    "getui binding has no app bundle",
                ),
                FailureReason::Config,
                "no_app_bundle",
            );
        }
        let Some(request) = build_getui_push_request(
            envelope,
            &plan,
            token,
            &self.android_vendor,
            self.default_ttl_ms,
        ) else {
            return BindingOutcome::Skipped;
        };
        match self.tokens.was_accepted(token, envelope).await {
            Ok(true) => return BindingOutcome::AlreadyAccepted,
            Ok(false) => {}
            Err(error) => return BindingOutcome::registry(error),
        }
        match self.tokens.is_current(token).await {
            Ok(true) => {}
            Ok(false) => return BindingOutcome::Skipped,
            Err(error) => return BindingOutcome::registry(error),
        }
        match self.client.push_single_cid(&request).await {
            Ok(GetuiPushOutcome::Delivered) => {
                match self.tokens.record_accepted(token, envelope).await {
                    Ok(()) => BindingOutcome::Accepted,
                    Err(error) => BindingOutcome::registry(error),
                }
            }
            Ok(GetuiPushOutcome::InvalidTarget) => {
                tracing::warn!(
                    tenant_id = %envelope.tenant_id,
                    user_id = %envelope.user_id,
                    device_id = %token.device_id,
                    provider = %token.provider,
                    "getui reported invalid cid; removing device push token"
                );
                match self.tokens.remove_device_token(ctx, token).await {
                    Ok(()) => BindingOutcome::InvalidToken {
                        code: "20001".to_string(),
                    },
                    Err(error) => BindingOutcome::registry(error),
                }
            }
            // Call frequency and quota belong to the application's Getui credential.
            Ok(GetuiPushOutcome::Throttled { retry_after, code }) => BindingOutcome::Throttled {
                retry_after,
                code: code.to_string(),
                scope: ThrottleScope::Application,
            },
            Err(error) if error.is_retryable() => {
                BindingOutcome::retryable(error, FailureReason::Server, "server")
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    tenant_id = %envelope.tenant_id,
                    user_id = %envelope.user_id,
                    device_id = %token.device_id,
                    "getui terminal delivery failure for device"
                );
                let reason = if error.code() == Some(ErrorCode::AuthenticationRequired) {
                    FailureReason::Auth
                } else {
                    FailureReason::Rejected
                };
                BindingOutcome::terminal(error, reason, reason.as_str())
            }
        }
    }
}

pub fn getui_auth_sign(app_key: &str, timestamp: &str, master_secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(app_key.as_bytes());
    hasher.update(timestamp.as_bytes());
    hasher.update(master_secret.as_bytes());
    hex::encode(hasher.finalize())
}

/// `None` when the task has nothing to show: Getui carries displayed notifications only.
/// Title and body are rendered in the binding's language.
pub fn build_getui_push_request(
    envelope: &PushTaskEnvelope,
    plan: &PushPlan,
    token: &DevicePushToken,
    android_vendor: &GetuiAndroidVendor,
    default_ttl_ms: u64,
) -> Option<GetuiPushRequest> {
    let PushPlan::Alert(alert) = plan else {
        return None;
    };
    let lang = Lang::from_locale(&token.locale);
    let text = alert.text(lang);
    let request_id = getui_request_id(envelope, token);
    let vendor_notification = matches!(
        task_disposition(envelope),
        Some(PushDisposition::Loud | PushDisposition::Digest)
    );
    let vendor_options = if vendor_notification {
        let concealed = xiaomi_concealed_text(lang, alert.origin.kind, envelope.count);
        let xiaomi_text = if alert.copy.is_generic() {
            concealed.clone()
        } else {
            text.clone()
        };
        vendor_options(
            android_vendor,
            alert.origin.kind,
            &text,
            &xiaomi_text,
            &concealed,
            &request_id,
        )
    } else {
        Map::new()
    };
    let AlertText { title, body } = text;
    Some(GetuiPushRequest {
        cid: token.token.clone(),
        request_id,
        title,
        body,
        play_sound: alert.play_sound,
        show_badge: alert.show_badge,
        collapse_key: envelope.collapse_key.clone(),
        thread_key: envelope.thread_key.clone(),
        badge: envelope.badge,
        payload: client_payload(
            envelope,
            plan,
            ValueShape::Strings,
            ClientText::Included(lang),
        ),
        vendor_notification,
        app_bundle: token.app_bundle.clone(),
        vendor_options,
        ttl_ms: envelope
            .expire_at
            .filter(|value| *value > 0)
            .map(|expires| expires.saturating_sub(now_ms()).max(0) as u64)
            .map(|remaining| remaining.min(default_ttl_ms))
            .unwrap_or(default_ttl_ms),
    })
}

/// Getui's notification override identifier is an integer in `0..=2147483647`: of two
/// notifications with the same `notify_id`, the later replaces the earlier. Take the first four
/// bytes of the collapse key's SHA-256 with the sign bit cleared; 0 reads as "unset", so it
/// becomes 1. A client that wants its local notification replaced by the push (or the reverse)
/// has to derive the identifier the same way.
pub fn getui_notify_id(collapse_key: &str) -> Option<i32> {
    if collapse_key.is_empty() {
        return None;
    }
    let digest = Sha256::digest(collapse_key.as_bytes());
    let id = u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]) & 0x7fff_ffff;
    Some(id.max(1) as i32)
}

pub fn build_getui_single_cid_body(request: &GetuiPushRequest) -> Value {
    let payload = Value::Object(request.payload.clone()).to_string();
    let mut body = json!({
        "request_id": request.request_id,
        "settings": {
            "ttl": request.ttl_ms.min(GETUI_MAX_TTL_MS),
        },
        "audience": {
            "cid": [request.cid],
        },
        // While the Getui channel is online the app's code receives the keys and renders,
        // replaces or drops the notification itself.
        "push_message": {
            "transmission": payload,
        },
        "push_channel": {
            "ios": {
                "type": "notify",
                "payload": payload,
                "aps": {
                    "alert": {
                        "title": request.title,
                        "body": request.body,
                    },
                    "sound": "default",
                    "content-available": 0,
                },
                "auto_badge": "+1",
            },
            "harmony": {
                "notification": {
                    "title": request.title,
                    "body": request.body,
                    "category": "SOCIAL",
                    "click_type": "startapp",
                },
            },
        },
    });
    if !request.play_sound
        && let Some(aps) = body["push_channel"]["ios"]["aps"].as_object_mut()
    {
        aps.remove("sound");
    }
    if let Some(badge) = request.badge {
        // `auto_badge` also takes an absolute value; the router's number wins over "+1".
        body["push_channel"]["ios"]["auto_badge"] = json!(badge.to_string());
    } else if !request.show_badge
        && let Some(ios) = body["push_channel"]["ios"].as_object_mut()
    {
        ios.remove("auto_badge");
    }
    if !request.collapse_key.is_empty() {
        body["push_channel"]["ios"]["apns-collapse-id"] = json!(request.collapse_key);
    }
    if !request.thread_key.is_empty() {
        body["push_channel"]["ios"]["aps"]["thread-id"] = json!(request.thread_key);
    }
    if request.vendor_notification {
        body["push_channel"]["android"] = android_vendor_channel(request);
    }
    body
}

/// The Android platform module's activity that receives taps on notifications the phone
/// maker's system rendered (also reachable as `flarepush://open`).
const OPEN_ACTIVITY: &str = "com.flare.im.push.FlarePushOpenActivity";
/// The string extra that carries the client keys, as one JSON object.
const OPEN_EXTRA: &str = "flare_push";
/// `launchFlags` of the tap intent: `FLAG_ACTIVITY_NEW_TASK | FLAG_ACTIVITY_CLEAR_TOP`.
const LAUNCH_FLAGS: &str = "0x14000000";

/// The Android vendor channel (`push_channel.android.ups`): a notification the maker's system
/// renders even when the app's process is dead, plus the per-maker options.
///
/// Every Getui REST v2 field name of the Android vendor channel lives here and in
/// [`vendor_options`]: `android`, `ups`, `notification` with `title`, `body`, `click_type`
/// (value `intent`), `intent` (at most 4096 characters, 2048 for vivo) and `notify_id` (OPPO
/// wants it above 0), and `options` keyed by `HW`, `HO`, `XM`, `OP`, `VV`. Checked against
/// Getui "公共参数" (https://docs.getui.com/getui/server/rest_v2/common_args/) and "多厂商参数"
/// (https://docs.getui.com/getui/server/rest_v2/third_party/) on 2026-11-12; a sandbox request
/// with them (`getui_sandbox_push`) is still outstanding.
fn android_vendor_channel(request: &GetuiPushRequest) -> Value {
    let mut notification = json!({
        "title": fit_utf16(&request.title, SHARED_TITLE_MAX),
        "body": fit_utf16(&request.body, SHARED_BODY_MAX),
        "click_type": "intent",
        "intent": open_intent(&request.app_bundle, &request.payload),
    });
    if let Some(notify_id) = getui_notify_id(&request.collapse_key) {
        notification["notify_id"] = json!(notify_id);
    }
    let mut ups = json!({ "notification": notification });
    if !request.vendor_options.is_empty() {
        ups["options"] = Value::Object(request.vendor_options.clone());
    }
    json!({ "ups": ups })
}

/// Per-maker options, keyed by Getui's maker code, each a map from the maker's own request
/// field path to its value (Getui "多厂商参数"). Only configured values are sent.
///
/// - `HW` `/message/android/category`, `HO` `/android/notification/importance`, `VV`
///   `/category`: every push, as configured.
/// - `XM`, chats only: `/extra.channel_id` and `/extra.template_id` of the conversation kind
///   (group falls back to direct), `/extra.template_param` a JSON string with the template
///   variables `title` (at most 50) and `content` (at most 128). Getui's table for this field:
///   exactly the template's variables, each 1 to 128 characters, not only spaces, no emoji or
///   other `\p{So}` symbols, no unassigned characters; see [`xiaomi_param`].
/// - `OP`, chats only: `/channel_id` (old private channel) as configured; with a private
///   template, the new classification: `/category` `IM`, `/notify_level`,
///   `/private_msg_template_id`, `/private_title_parameters` `{"title"}` (at most 50) and
///   `/private_content_parameters` `{"content"}` (at most 200), both JSON objects; and
///   `/app_message_id` (Getui lists it as required for OPPO single pushes; OPPO deduplicates on
///   it), the request id.
///
/// Notices are not chat: the Xiaomi and OPPO private-message channels and templates are for
/// person-to-person and group chat, and filing a system or business notification there is
/// the kind of content both makers penalise (Xiaomi suspends the app's private messages for
/// it). So a notice carries no `XM` and no `OP` options and Xiaomi and OPPO handle it as an
/// unclassified message: Xiaomi's limit table lists that class (one per device per day), OPPO
/// files it under content and marketing. Getui's table marks `/extra.channel_id` as required
/// and warns that Xiaomi refuses a request without it; whether that holds for a push without
/// any `XM` options is not stated and was not verified. The credentials have no notice
/// channel (a Xiaomi public-message channel such as social updates) to send instead. Huawei,
/// Honor and vivo options are sent for every push as configured.
fn vendor_options(
    vendor: &GetuiAndroidVendor,
    kind: ChatKind,
    text: &AlertText,
    xiaomi_text: &AlertText,
    xiaomi_fallback: &AlertText,
    request_id: &str,
) -> Map<String, Value> {
    let mut options = Map::new();
    if let Some(category) = &vendor.huawei_category {
        set_option(
            &mut options,
            "HW",
            "/message/android/category",
            json!(category),
        );
    }
    if let Some(importance) = &vendor.honor_importance {
        set_option(
            &mut options,
            "HO",
            "/android/notification/importance",
            json!(importance),
        );
    }
    if matches!(kind, ChatKind::Direct | ChatKind::Group) {
        let group = kind == ChatKind::Group;
        let pick = |group_value: &Option<String>, value: &Option<String>| {
            if group {
                group_value.clone().or_else(|| value.clone())
            } else {
                value.clone()
            }
        };
        let channel = pick(&vendor.xiaomi_group_channel_id, &vendor.xiaomi_channel_id);
        let template = pick(&vendor.xiaomi_group_template_id, &vendor.xiaomi_template_id);
        if let Some(channel) = channel {
            set_option(&mut options, "XM", "/extra.channel_id", json!(channel));
            if let Some(template) = template {
                let params = json!({
                    "title": xiaomi_param(
                        &xiaomi_text.title,
                        &xiaomi_fallback.title,
                        XIAOMI_TITLE_MAX,
                    ),
                    "content": xiaomi_param(
                        &xiaomi_text.body,
                        &xiaomi_fallback.body,
                        XIAOMI_BODY_MAX,
                    ),
                });
                set_option(&mut options, "XM", "/extra.template_id", json!(template));
                set_option(
                    &mut options,
                    "XM",
                    "/extra.template_param",
                    Value::String(params.to_string()),
                );
            }
        }
        if let Some(channel) = &vendor.oppo_channel_id {
            set_option(&mut options, "OP", "/channel_id", json!(channel));
        }
        if let Some(template) = &vendor.oppo_private_template_id {
            let level = vendor
                .oppo_notify_level
                .unwrap_or(OPPO_DEFAULT_NOTIFY_LEVEL);
            set_option(&mut options, "OP", "/category", json!(OPPO_IM_CATEGORY));
            set_option(&mut options, "OP", "/notify_level", json!(level));
            set_option(
                &mut options,
                "OP",
                "/private_msg_template_id",
                json!(template),
            );
            set_option(
                &mut options,
                "OP",
                "/private_title_parameters",
                json!({ "title": fit_utf16(&text.title, OPPO_TITLE_MAX) }),
            );
            set_option(
                &mut options,
                "OP",
                "/private_content_parameters",
                json!({ "content": fit_utf16(&text.body, OPPO_BODY_MAX) }),
            );
        }
        if options.contains_key("OP") {
            set_option(&mut options, "OP", "/app_message_id", json!(request_id));
        }
    }
    if let Some(category) = &vendor.vivo_category {
        set_option(&mut options, "VV", "/category", json!(category));
    }
    options
}

/// A Xiaomi template variable value: `text` without the graphemes Xiaomi refuses
/// ([`XIAOMI_REFUSED`]), whitespace runs collapsed, cut to `max` characters. When nothing is
/// left (a message of emoji only), `fallback`: Xiaomi wants 1 to 128 characters, not only
/// spaces. Only the Xiaomi parameters are stripped; the shared notification, the transmission
/// and the other makers keep the symbols.
fn xiaomi_param(text: &str, fallback: &str, max: usize) -> String {
    let mut kept = String::with_capacity(text.len());
    let mut space = false;
    for cluster in text.graphemes(true) {
        if XIAOMI_REFUSED.is_match(cluster) {
            continue;
        }
        if cluster.chars().all(char::is_whitespace) {
            space = true;
            continue;
        }
        if space && !kept.is_empty() {
            kept.push(' ');
        }
        space = false;
        kept.push_str(cluster);
    }
    let value = if kept.is_empty() {
        fallback
    } else {
        kept.as_str()
    };
    fit_utf16(value, max)
}

fn set_option(options: &mut Map<String, Value>, maker: &str, path: &str, value: Value) {
    if let Value::Object(fields) = options
        .entry(maker.to_string())
        .or_insert_with(|| Value::Object(Map::new()))
    {
        fields.insert(path.to_string(), value);
    }
}

/// Xiaomi's template variables when the alert shows neither sender nor content (preview set to
/// hidden, content that cannot be shown, nothing showable in the payload).
///
/// Xiaomi's 2026 classification wants a chat notification to carry its sender and real content
/// and lists "both titles vague" as a violation, with "你收到一条新消息" itself as the example.
/// A recipient who hid previews cannot get sender or content, and neither the credentials nor
/// the application record carries the app's display name. So the title names the kind of chat
/// (direct or group) and the body says how many messages are waiting and where to read them.
/// It is the most specific wording that shows nothing the recipient chose to hide; the
/// remaining risk is Xiaomi's review of this case, which the operator should describe in the
/// template application. The other makers and the transmission keep the generic text.
fn xiaomi_concealed_text(lang: Lang, kind: ChatKind, count: u32) -> AlertText {
    let count = count.max(1);
    let shown = if count > 99 {
        "99+".to_string()
    } else {
        count.to_string()
    };
    let (zh_title, en_title) = match kind {
        ChatKind::Group => ("群聊消息", "Group message"),
        ChatKind::Direct | ChatKind::Notice => ("私聊消息", "Direct message"),
    };
    match lang {
        Lang::Zh => AlertText {
            title: zh_title.to_string(),
            body: format!("{shown} 条新消息，打开应用查看"),
        },
        Lang::En => AlertText {
            title: en_title.to_string(),
            body: if count == 1 {
                "1 new message, open the app to read it".to_string()
            } else {
                format!("{shown} new messages, open the app to read them")
            },
        },
    }
}

/// The intent a tap on the vendor notification starts: the platform module's open activity in
/// the binding's package, with the client keys as one JSON object in the string extra
/// `flare_push` (URI-encoded). Title and body stay out: routing does not need them, and the
/// makers bound the intent length. The package is the binding's `app_bundle`, which
/// registration requires and the executor checks, so the component always names a package.
fn open_intent(app_bundle: &str, payload: &Map<String, Value>) -> String {
    let keys: Map<String, Value> = payload
        .iter()
        .filter(|(key, _)| !matches!(key.as_str(), "title" | "body"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    format!(
        "intent://open#Intent;scheme=flarepush;launchFlags={LAUNCH_FLAGS};\
         component={app_bundle}/{OPEN_ACTIVITY};S.{OPEN_EXTRA}={};end",
        uri_encode(&Value::Object(keys).to_string())
    )
}

/// Percent-encodes everything but ASCII letters, digits and `-_.~`; Android's `Uri.decode`,
/// which `Intent.parseUri` applies to extras, reverses it.
fn uri_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

pub(crate) fn message_has_e2ee_placeholder(message: &flare_proto::common::Message) -> bool {
    let Some(content) = message.content.as_ref() else {
        return false;
    };
    matches!(
        content.content.as_ref(),
        Some(MessageContentVariant::Placeholder(placeholder))
            if placeholder.reason == E2EE_PLACEHOLDER_REASON
    )
}

fn getui_request_id(envelope: &PushTaskEnvelope, token: &DevicePushToken) -> String {
    let mut hasher = Sha256::new();
    hasher.update(envelope.tenant_id.as_bytes());
    hasher.update([0]);
    hasher.update(envelope.user_id.as_bytes());
    hasher.update([0]);
    hasher.update(envelope.message_id.as_bytes());
    hasher.update([0]);
    hasher.update(token.device_id.as_bytes());
    hasher.update([0]);
    hasher.update(token.registration_id.as_bytes());
    // Getui drops a repeated request_id as a duplicate. A loud notification, its quiet refresh
    // and its recall replacement can share a message id, so the task itself is part of the id;
    // a redelivery of the same task still maps to the same id.
    hasher.update([0]);
    hasher.update(envelope.idempotency_key.as_bytes());
    hasher.update([0]);
    hasher.update(envelope.disposition.to_be_bytes());
    hasher.update(envelope.conversation_seq.to_be_bytes());
    hasher.update(envelope.count.to_be_bytes());
    let digest = hex::encode(hasher.finalize());
    format!("f{}", &digest[..31])
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

fn retryable_getui_error(_: reqwest::Error) -> FlareError {
    vendor_http::failure("getui transport failed; acceptance unknown")
}

fn http_status_error(status: StatusCode, operation: &'static str) -> FlareError {
    let code = if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        ErrorCode::AuthenticationRequired
    } else if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
        ErrorCode::ServiceUnavailable
    } else {
        ErrorCode::MessageDeliveryFailed
    };
    FlareError::localized(code, format!("{operation} http status {status}"))
}

fn getui_business_error(code: i64, _msg: &str, operation: &'static str) -> FlareError {
    let error_code = if code == GETUI_TOKEN_EXPIRED_CODE {
        ErrorCode::AuthenticationRequired
    } else if matches!(
        code,
        2 | 5000 | 30012 | 30013 | 30014 | 30015 | 30018 | 30019 | 30021 | 30022
    ) {
        ErrorCode::ServiceUnavailable
    } else {
        ErrorCode::MessageDeliveryFailed
    };
    FlareError::localized(error_code, format!("{operation} rejected: code={code}"))
}

fn is_invalid_target_business_error(code: i64, msg: &str) -> bool {
    if code != 20_001 {
        return false;
    }
    let lower = msg.to_ascii_lowercase();
    matches!(lower.trim(), "target user is invalid" | "cid无效")
}

fn deserialize_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct I64Visitor;

    impl serde::de::Visitor<'_> for I64Visitor {
        type Value = i64;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("integer or numeric string")
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
            Ok(value)
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            i64::try_from(value).map_err(E::custom)
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            value.parse::<i64>().map_err(E::custom)
        }
    }

    deserializer.deserialize_any(I64Visitor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_grpc_proto::access_gateway::PushMessageRequest;
    use flare_proto::common::{
        AlertClass, ContentVisibility, NotificationPreview, PushDisposition, PushTaskPayloadKind,
    };
    use flare_proto::common::{
        Message, MessageContent, MessageRetentionState, OfflinePushInfo, PlaceholderContent,
    };
    use prost::Message as _;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const COLLAPSE_KEY: &str = "c:0123456789abcdef0123456789abcdef";

    /// A loud task for the device `token()` is bound to, carrying `push_payload`.
    fn task(push_payload: Vec<u8>) -> PushTaskEnvelope {
        PushTaskEnvelope {
            tenant_id: "tenant-a".to_string(),
            user_id: "user-1".to_string(),
            message_id: "msg-1".to_string(),
            conversation_id: "conv-1".to_string(),
            priority: 5,
            push_payload,
            payload_kind: PushTaskPayloadKind::Message as i32,
            device_id: "ios-1".to_string(),
            disposition: PushDisposition::Loud as i32,
            ..Default::default()
        }
    }

    /// The request Getui would receive; `None` means no vendor call.
    fn request_for(envelope: &PushTaskEnvelope) -> Option<GetuiPushRequest> {
        let plan = push_plan(envelope).expect("plan");
        build_getui_push_request(
            envelope,
            &plan,
            &token(),
            &GetuiAndroidVendor::default(),
            7_200_000,
        )
    }

    /// A request with nothing but the fields a test sets.
    fn bare_request() -> GetuiPushRequest {
        GetuiPushRequest {
            cid: "cid".into(),
            request_id: "id".into(),
            title: "test".into(),
            body: "test".into(),
            payload: Map::new(),
            ttl_ms: 1000,
            play_sound: false,
            show_badge: false,
            collapse_key: String::new(),
            thread_key: String::new(),
            badge: None,
            vendor_notification: false,
            app_bundle: String::new(),
            vendor_options: Map::new(),
        }
    }

    /// Delivers `task` the way the worker does: the device's bindings listed once, the
    /// governed binding selection, Getui the only vendor.
    async fn deliver(
        executor: &Arc<GetuiOfflinePushExecutor>,
        task: &PushTaskEnvelope,
    ) -> Result<(), FlareError> {
        crate::infrastructure::push_test_support::deliver_governed(
            crate::domain::vendor_delivery::Vendor::Getui,
            executor.clone(),
            executor.tokens.as_ref(),
            task,
        )
        .await
    }

    #[tokio::test]
    async fn structured_provider_rate_limit_is_retryable_and_non_200_is_not_acceptance() {
        use crate::infrastructure::push_test_support::http_peer;
        let (endpoint, peer) = http_peer(vec![
            (401, r#"{"code":10005,"msg":"PRIVATE rate body"}"#.into()),
            (500, r#"{"code":0,"msg":"PRIVATE fake success"}"#.into()),
        ])
        .await;
        let config = GetuiConfig {
            app_id: "app".into(),
            app_key: "key".into(),
            master_secret: "secret".into(),
            base_url: endpoint,
            default_ttl_ms: 1000,
            request_timeout_ms: 1000,
        };
        let client = GetuiClient {
            http: vendor_http::client().unwrap(),
            config,
            access_token: Mutex::new(Some(GetuiAccessToken {
                token: "controlled".into(),
                expire_time_ms: i64::MAX,
            })),
        };
        let request = bare_request();
        // The call-frequency code is throttling, whatever the HTTP status.
        assert_eq!(
            client.push_single_cid(&request).await.unwrap(),
            GetuiPushOutcome::Throttled {
                retry_after: None,
                code: 10005
            }
        );
        let error = client.push_single_cid(&request).await.unwrap_err();
        assert!(error.is_retryable());
        assert!(!error.to_string().contains("PRIVATE"));
        peer.await.unwrap();
    }

    /// Getui "业务返回码说明": 10003 refuses the authentication for frequency, 30009 the push for
    /// the daily total. Both are throttling, not failures: no breaker, no dead letter.
    #[tokio::test]
    async fn auth_frequency_and_daily_quota_are_throttling() {
        use crate::infrastructure::push_test_support::http_peer;
        let (endpoint, peer) = http_peer(vec![
            (401, r#"{"code":10003,"msg":"PRIVATE auth body"}"#.into()),
            (403, r#"{"code":30009,"msg":"PRIVATE quota body"}"#.into()),
        ])
        .await;
        let config = GetuiConfig {
            app_id: "app".into(),
            app_key: "key".into(),
            master_secret: "secret".into(),
            base_url: endpoint,
            default_ttl_ms: 1000,
            request_timeout_ms: 1000,
        };
        let client = GetuiClient {
            http: vendor_http::client().unwrap(),
            config,
            access_token: Mutex::new(None),
        };
        let request = bare_request();
        assert_eq!(
            client.push_single_cid(&request).await.unwrap(),
            GetuiPushOutcome::Throttled {
                retry_after: None,
                code: 10003
            }
        );
        *client.access_token.lock().await = Some(GetuiAccessToken {
            token: "controlled".into(),
            expire_time_ms: i64::MAX,
        });
        assert_eq!(
            client.push_single_cid(&request).await.unwrap(),
            GetuiPushOutcome::Throttled {
                retry_after: Some(GETUI_DAILY_QUOTA_RETRY),
                code: 30009
            }
        );
        peer.await.unwrap();
    }

    /// `settings.ttl` never exceeds Getui's three days.
    #[test]
    fn the_ttl_stays_within_getui_bounds() {
        let request = GetuiPushRequest {
            ttl_ms: 30 * 24 * 3_600_000,
            ..bare_request()
        };
        assert_eq!(
            build_getui_single_cid_body(&request)["settings"]["ttl"],
            GETUI_MAX_TTL_MS
        );
    }

    fn token() -> DevicePushToken {
        DevicePushToken {
            tenant_id: "tenant-a".to_string(),
            user_id: "user-1".to_string(),
            device_id: "ios-1".to_string(),
            platform: "ios".to_string(),
            provider: "getui".to_string(),
            token: "cid-1".to_string(),
            registration_id: "r1".to_string(),
            registered_at_ms: 1,
            app_bundle: "com.example.app".to_string(),
            ..Default::default()
        }
    }

    /// An Android device bound through Getui.
    fn android_token() -> DevicePushToken {
        DevicePushToken {
            device_id: "android-1".to_string(),
            platform: "android".to_string(),
            ..token()
        }
    }

    struct FakeTokenRepository {
        removed: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl DeviceTokenRepository for FakeTokenRepository {
        async fn is_current(&self, _: &DevicePushToken) -> Result<bool, FlareError> {
            Ok(true)
        }
        async fn was_accepted(
            &self,
            _: &DevicePushToken,
            _: &PushTaskEnvelope,
        ) -> Result<bool, FlareError> {
            Ok(false)
        }
        async fn record_accepted(
            &self,
            _: &DevicePushToken,
            _: &PushTaskEnvelope,
        ) -> Result<(), FlareError> {
            Ok(())
        }
        async fn list_device_tokens(
            &self,
            _ctx: &Ctx,
            _tenant_id: &str,
            _user_id: &str,
            device_id: &str,
        ) -> Result<Vec<DevicePushToken>, FlareError> {
            Ok(vec![token()]
                .into_iter()
                .filter(|row| row.device_id == device_id)
                .collect())
        }

        async fn remove_device_token(
            &self,
            _ctx: &Ctx,
            _token: &DevicePushToken,
        ) -> Result<(), FlareError> {
            self.removed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct InvalidTargetPusher;

    #[async_trait::async_trait]
    impl GetuiPusher for InvalidTargetPusher {
        async fn push_single_cid(
            &self,
            _request: &GetuiPushRequest,
        ) -> Result<GetuiPushOutcome, FlareError> {
            Ok(GetuiPushOutcome::InvalidTarget)
        }
    }

    #[derive(Default)]
    struct RecordingPusher {
        requests: std::sync::Mutex<Vec<GetuiPushRequest>>,
    }

    #[async_trait::async_trait]
    impl GetuiPusher for RecordingPusher {
        async fn push_single_cid(
            &self,
            request: &GetuiPushRequest,
        ) -> Result<GetuiPushOutcome, FlareError> {
            self.requests.lock().unwrap().push(request.clone());
            Ok(GetuiPushOutcome::Delivered)
        }
    }

    fn recording_executor() -> (Arc<GetuiOfflinePushExecutor>, Arc<RecordingPusher>) {
        let pusher = Arc::new(RecordingPusher::default());
        let repo = Arc::new(FakeTokenRepository {
            removed: AtomicUsize::new(0),
        });
        (
            Arc::new(GetuiOfflinePushExecutor::new(
                repo,
                pusher.clone(),
                7_200_000,
                GetuiAndroidVendor::default(),
            )),
            pusher,
        )
    }

    #[tokio::test]
    async fn a_task_only_reaches_the_getui_binding_of_its_device() {
        use crate::infrastructure::push_test_support::alert_payload;
        let (executor, pusher) = recording_executor();
        // Another device of the same user: its own task covers it, this one must not.
        let mut other = task(alert_payload());
        other.device_id = "android-2".to_string();
        deliver(&executor, &other).await.expect("deliver");
        assert!(pusher.requests.lock().unwrap().is_empty());

        deliver(&executor, &task(alert_payload()))
            .await
            .expect("deliver");
        let requests = pusher.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].cid, "cid-1");
    }

    #[tokio::test]
    async fn tasks_with_nothing_to_show_are_acknowledged_without_calling_getui() {
        use crate::infrastructure::push_test_support::alert_payload;
        let (executor, pusher) = recording_executor();
        for (disposition, badge) in [
            (PushDisposition::BadgeOnly, Some(3)),
            (PushDisposition::BadgeOnly, None),
            (PushDisposition::Clear, Some(0)),
            (PushDisposition::Clear, None),
        ] {
            let mut envelope = task(alert_payload());
            envelope.disposition = disposition as i32;
            envelope.badge = badge;
            deliver(&executor, &envelope).await.expect("deliver");
            assert!(request_for(&envelope).is_none());
        }
        assert!(pusher.requests.lock().unwrap().is_empty());

        let mut unspecified = task(alert_payload());
        unspecified.disposition = PushDisposition::Unspecified as i32;
        let error = deliver(&executor, &unspecified).await.unwrap_err();
        assert!(!error.is_retryable());
    }

    /// A loud task: the transmission carries every client key with string values, the iOS
    /// channel replaces by collapse id, and the Android vendor notification replaces by notify id.
    #[test]
    fn loud_request_carries_the_client_keys_and_the_override_identifiers() {
        let mut envelope = task(crate::infrastructure::push_test_support::alert_payload());
        envelope.collapse_key = COLLAPSE_KEY.to_string();
        envelope.thread_key = COLLAPSE_KEY.to_string();
        envelope.conversation_seq = 42;
        envelope.alert_class = flare_proto::common::AlertClass::Mention as i32;
        let request = request_for(&envelope).expect("request");
        assert_eq!(
            Value::Object(request.payload.clone()),
            json!({
                "uid": "user-1",
                "tenant_id": "tenant-a",
                "conversation_id": "conv-1",
                "message_id": "msg-1",
                "seq": "42",
                "collapse_id": COLLAPSE_KEY,
                "thread_id": COLLAPSE_KEY,
                "disposition": "loud",
                "alert_class": "mention",
                "count": "1",
                "title": "新消息",
                "body": "[有人@你] 你收到一条新消息",
                "sound": "1",
                "sender_id": "",
                "sender_name": "",
                "kind": "group",
            })
        );
        assert!(request.vendor_notification);

        let body = build_getui_single_cid_body(&request);
        // The transmission is the same keys, as a JSON string; there is no notification message.
        let transmission: Value = serde_json::from_str(
            body["push_message"]["transmission"]
                .as_str()
                .expect("transmission string"),
        )
        .expect("transmission json");
        assert_eq!(transmission, Value::Object(request.payload.clone()));
        assert!(body["push_message"].get("notification").is_none());
        assert_eq!(
            body["push_channel"]["ios"]["payload"],
            body["push_message"]["transmission"]
        );
        assert_eq!(
            body["push_channel"]["ios"]["apns-collapse-id"],
            COLLAPSE_KEY
        );
        assert_eq!(
            body["push_channel"]["ios"]["aps"]["thread-id"],
            COLLAPSE_KEY
        );
        assert_eq!(body["push_channel"]["ios"]["aps"]["sound"], "default");
        let notify_id = getui_notify_id(COLLAPSE_KEY).expect("notify id");
        assert_eq!(
            body["push_channel"]["android"]["ups"]["notification"]["notify_id"],
            notify_id
        );
    }

    /// Reverses [`uri_encode`] the way Android's `Uri.decode` does.
    fn uri_decode(value: &str) -> String {
        let bytes = value.as_bytes();
        let mut decoded = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap();
                decoded.push(u8::from_str_radix(hex, 16).unwrap());
                i += 3;
            } else {
                decoded.push(bytes[i]);
                i += 1;
            }
        }
        String::from_utf8(decoded).unwrap()
    }

    /// The Android vendor notification: title and body, a tap that opens the platform module's
    /// open activity in the binding's package with the client keys as one JSON extra, and only
    /// the configured options.
    #[test]
    fn android_vendor_notification_opens_the_app_with_the_payload() {
        let mut envelope = task(crate::infrastructure::push_test_support::alert_payload());
        envelope.device_id = "android-1".to_string();
        envelope.collapse_key = COLLAPSE_KEY.to_string();
        envelope.thread_key = COLLAPSE_KEY.to_string();
        envelope.conversation_seq = 7;
        let plan = push_plan(&envelope).expect("plan");
        let vendor = GetuiAndroidVendor {
            huawei_category: Some("IM".into()),
            xiaomi_channel_id: Some("100001".into()),
            xiaomi_template_id: Some("xm-direct".into()),
            vivo_category: Some("IM".into()),
            ..Default::default()
        };
        let request =
            build_getui_push_request(&envelope, &plan, &android_token(), &vendor, 7_200_000)
                .expect("request");
        let body = build_getui_single_cid_body(&request);
        let ups = &body["push_channel"]["android"]["ups"];
        assert_eq!(ups["notification"]["title"], "新消息");
        assert_eq!(ups["notification"]["body"], "你收到一条新消息");
        assert_eq!(ups["notification"]["click_type"], "intent");
        let mut options = ups["options"].clone();
        let xiaomi = options["XM"]
            .as_object_mut()
            .expect("xiaomi options")
            .remove("/extra.template_param")
            .expect("template variables");
        assert_eq!(
            options,
            json!({
                "HW": { "/message/android/category": "IM" },
                // A group chat without a group channel uses the direct one.
                "XM": { "/extra.channel_id": "100001", "/extra.template_id": "xm-direct" },
                "VV": { "/category": "IM" },
            })
        );
        // Neither sender nor content: Xiaomi gets the chat kind and the count, not the vague
        // phrase it lists as a violation.
        let xiaomi: Value = serde_json::from_str(xiaomi.as_str().expect("a JSON string"))
            .expect("template variables json");
        assert_eq!(
            xiaomi,
            json!({ "title": "群聊消息", "content": "1 条新消息，打开应用查看" })
        );
        let intent = ups["notification"]["intent"].as_str().expect("intent");
        let prefix = "intent://open#Intent;scheme=flarepush;launchFlags=0x14000000;\
                      component=com.example.app/com.flare.im.push.FlarePushOpenActivity;\
                      S.flare_push=";
        let extra = intent
            .strip_prefix(prefix)
            .and_then(|rest| rest.strip_suffix(";end"))
            .unwrap_or_else(|| panic!("{intent}"));
        // The extra is URI-encoded: no separator of the intent syntax leaks out of it.
        assert!(!extra.contains(';') && !extra.contains('='), "{extra}");
        let keys: Value = serde_json::from_str(&uri_decode(extra)).expect("payload json");
        let mut expected = Value::Object(request.payload.clone());
        let object = expected.as_object_mut().unwrap();
        // Routing does not need the text, and it would only lengthen the intent.
        object.remove("title");
        object.remove("body");
        assert_eq!(keys, expected);
        assert_eq!(keys["conversation_id"], "conv-1");
        assert_eq!(keys["seq"], "7");
        assert_eq!(keys["collapse_id"], COLLAPSE_KEY);

        // Without options none are sent.
        let request = build_getui_push_request(
            &envelope,
            &plan,
            &android_token(),
            &GetuiAndroidVendor::default(),
            7_200_000,
        )
        .expect("request");
        let body = build_getui_single_cid_body(&request);
        assert!(
            body["push_channel"]["android"]["ups"]
                .get("options")
                .is_none()
        );
    }

    /// The open activity lives in the binding's package, so a Getui binding without an app
    /// bundle cannot be addressed: a terminal error, and Getui is not called.
    #[tokio::test]
    async fn a_getui_binding_without_an_app_bundle_is_terminal() {
        struct Bare(FakeTokenRepository);
        #[async_trait::async_trait]
        impl DeviceTokenRepository for Bare {
            async fn is_current(&self, token: &DevicePushToken) -> Result<bool, FlareError> {
                self.0.is_current(token).await
            }
            async fn was_accepted(
                &self,
                token: &DevicePushToken,
                task: &PushTaskEnvelope,
            ) -> Result<bool, FlareError> {
                self.0.was_accepted(token, task).await
            }
            async fn record_accepted(
                &self,
                token: &DevicePushToken,
                task: &PushTaskEnvelope,
            ) -> Result<(), FlareError> {
                self.0.record_accepted(token, task).await
            }
            async fn list_device_tokens(
                &self,
                ctx: &Ctx,
                tenant_id: &str,
                user_id: &str,
                device_id: &str,
            ) -> Result<Vec<DevicePushToken>, FlareError> {
                let mut rows = self
                    .0
                    .list_device_tokens(ctx, tenant_id, user_id, device_id)
                    .await?;
                for row in &mut rows {
                    row.app_bundle.clear();
                }
                Ok(rows)
            }
            async fn remove_device_token(
                &self,
                ctx: &Ctx,
                token: &DevicePushToken,
            ) -> Result<(), FlareError> {
                self.0.remove_device_token(ctx, token).await
            }
        }
        let pusher = Arc::new(RecordingPusher::default());
        let executor = Arc::new(GetuiOfflinePushExecutor::new(
            Arc::new(Bare(FakeTokenRepository {
                removed: AtomicUsize::new(0),
            })),
            pusher.clone(),
            7_200_000,
            GetuiAndroidVendor::default(),
        ));
        let error = deliver(
            &executor,
            &task(crate::infrastructure::push_test_support::alert_payload()),
        )
        .await
        .unwrap_err();
        assert!(!error.is_retryable());
        assert!(pusher.requests.lock().unwrap().is_empty());
    }

    #[test]
    fn intent_extras_are_uri_encoded() {
        assert_eq!(uri_encode("c:ab-_.~"), "c%3Aab-_.~");
        assert_eq!(uri_encode("a;b=c d"), "a%3Bb%3Dc%20d");
        assert_eq!(uri_encode("群"), "%E7%BE%A4");
    }

    /// Android vendor options are short identifiers that come in the pairs the makers need;
    /// anything else is a configuration error.
    #[test]
    fn android_vendor_options_are_validated() {
        assert!(GetuiAndroidVendor::default().validate().is_ok());
        for bad in ["", "I M", "x".repeat(65).as_str()] {
            for vendor in [
                GetuiAndroidVendor {
                    oppo_channel_id: Some(bad.to_string()),
                    ..Default::default()
                },
                GetuiAndroidVendor {
                    oppo_private_template_id: Some(bad.to_string()),
                    ..Default::default()
                },
                GetuiAndroidVendor {
                    xiaomi_channel_id: Some("100001".into()),
                    xiaomi_template_id: Some(bad.to_string()),
                    ..Default::default()
                },
            ] {
                assert!(vendor.validate().is_err(), "{bad:?} {vendor:?}");
            }
        }
        let xiaomi = GetuiAndroidVendor {
            xiaomi_channel_id: Some("100001".into()),
            xiaomi_template_id: Some("xm-direct".into()),
            ..Default::default()
        };
        assert!(xiaomi.validate().is_ok());
        let group = GetuiAndroidVendor {
            xiaomi_group_channel_id: Some("100002".into()),
            xiaomi_group_template_id: Some("xm-group".into()),
            ..xiaomi.clone()
        };
        assert!(group.validate().is_ok());
        let oppo = GetuiAndroidVendor {
            oppo_private_template_id: Some("op-im".into()),
            oppo_notify_level: Some(16),
            ..Default::default()
        };
        assert!(oppo.validate().is_ok());
        for bad in [
            // A Xiaomi channel without its template, or a template without a channel.
            GetuiAndroidVendor {
                xiaomi_template_id: None,
                ..xiaomi.clone()
            },
            GetuiAndroidVendor {
                xiaomi_channel_id: None,
                ..xiaomi.clone()
            },
            // Group values need the direct ones they fall back to.
            GetuiAndroidVendor {
                xiaomi_group_channel_id: Some("100002".into()),
                ..Default::default()
            },
            GetuiAndroidVendor {
                xiaomi_group_template_id: Some("xm-group".into()),
                ..Default::default()
            },
            // A level OPPO does not know, or one without the template it applies to.
            GetuiAndroidVendor {
                oppo_notify_level: Some(3),
                ..oppo.clone()
            },
            GetuiAndroidVendor {
                oppo_notify_level: Some(2),
                ..Default::default()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
        assert!(
            serde_json::from_value::<GetuiAndroidVendor>(json!({ "meizu_category": "IM" }))
                .is_err()
        );
        assert!(
            serde_json::from_value::<GetuiAndroidVendor>(json!({ "oppo_notify_level": "2" }))
                .is_err(),
            "the level is an integer"
        );
    }

    /// A chat with a sender and content: the conversation kind picks Xiaomi's channel and
    /// template, the rendered text fills the Xiaomi and OPPO template variables (each cut to
    /// its maker's limit), and the shared notification keeps within the smallest limits.
    #[test]
    fn chat_pushes_use_the_private_message_channels_and_templates_of_their_kind() {
        let vendor = GetuiAndroidVendor {
            xiaomi_channel_id: Some("100001".into()),
            xiaomi_template_id: Some("xm-direct".into()),
            xiaomi_group_channel_id: Some("100002".into()),
            xiaomi_group_template_id: Some("xm-group".into()),
            oppo_channel_id: Some("flare.messages".into()),
            oppo_private_template_id: Some("op-im".into()),
            ..Default::default()
        };
        let long_title = "项".repeat(70);
        let long_body = "正".repeat(250);
        let options_for = |conversation_id: &str, title: &str, text: &str| {
            let mut envelope = task(crate::infrastructure::push_test_support::push_info_payload(
                title, text,
            ));
            envelope.device_id = "android-1".to_string();
            envelope.conversation_id = conversation_id.to_string();
            let plan = push_plan(&envelope).expect("plan");
            let request =
                build_getui_push_request(&envelope, &plan, &android_token(), &vendor, 7_200_000)
                    .expect("request");
            (request.clone(), build_getui_single_cid_body(&request))
        };
        let template_param = |body: &Value| -> Value {
            serde_json::from_str(
                body["push_channel"]["android"]["ups"]["options"]["XM"]["/extra.template_param"]
                    .as_str()
                    .expect("a JSON string"),
            )
            .expect("template variables json")
        };

        // A direct chat (conversation ids starting with 1 are direct).
        let (request, body) = options_for("1-conv", "小明", "今晚的方案我改好了");
        let options = &body["push_channel"]["android"]["ups"]["options"];
        assert_eq!(options["XM"]["/extra.channel_id"], "100001");
        assert_eq!(options["XM"]["/extra.template_id"], "xm-direct");
        assert_eq!(
            template_param(&body),
            json!({ "title": "小明", "content": "今晚的方案我改好了" })
        );
        assert_eq!(
            options["OP"],
            json!({
                "/channel_id": "flare.messages",
                "/category": "IM",
                "/notify_level": 2,
                "/private_msg_template_id": "op-im",
                "/private_title_parameters": { "title": "小明" },
                "/private_content_parameters": { "content": "今晚的方案我改好了" },
                "/app_message_id": request.request_id,
            })
        );

        // A group chat uses the group channel and template.
        let (_, body) = options_for("conv-9", &long_title, &long_body);
        let options = &body["push_channel"]["android"]["ups"]["options"];
        assert_eq!(options["XM"]["/extra.channel_id"], "100002");
        assert_eq!(options["XM"]["/extra.template_id"], "xm-group");
        // Each maker's field is cut to its own limit, the ellipsis included; the shared
        // notification to the smallest one (vivo: title 40, body 100).
        let chars = |value: &Value| value.as_str().expect("text").chars().count();
        let xiaomi = template_param(&body);
        assert_eq!(
            (chars(&xiaomi["title"]), chars(&xiaomi["content"])),
            (50, 128)
        );
        assert!(xiaomi["title"].as_str().unwrap().ends_with('…'));
        assert_eq!(
            chars(&options["OP"]["/private_title_parameters"]["title"]),
            50
        );
        // OPPO takes 200: the server's own body limit (180) is lower.
        assert_eq!(
            chars(&options["OP"]["/private_content_parameters"]["content"]),
            180
        );
        let notification = &body["push_channel"]["android"]["ups"]["notification"];
        assert_eq!(
            (chars(&notification["title"]), chars(&notification["body"])),
            (40, 100)
        );
        assert_eq!((SHARED_TITLE_MAX, SHARED_BODY_MAX), (40, 100));
        // The transmission and the iOS channel keep the server's own limits (64 and 180).
        let alert = &body["push_channel"]["ios"]["aps"]["alert"];
        assert_eq!((chars(&alert["title"]), chars(&alert["body"])), (64, 180));
        assert_eq!(
            serde_json::from_str::<Value>(
                body["push_message"]["transmission"]
                    .as_str()
                    .expect("transmission")
            )
            .expect("transmission json")["title"],
            alert["title"]
        );

        // A notice (conversation ids starting with 4) is not chat: no private-message options.
        let (_, body) = options_for("4-system", "系统通知", "服务将于今晚升级");
        assert!(
            body["push_channel"]["android"]["ups"]
                .get("options")
                .is_none()
        );
    }

    /// Xiaomi refuses emoji and other `\p{So}` symbols in template variables: they are dropped
    /// whole (with the joiners, selectors and modifiers of their sequences), whitespace is
    /// collapsed, and a value left empty takes the fallback. Other text is kept.
    #[test]
    fn xiaomi_template_values_drop_emoji_and_symbols() {
        assert_eq!(xiaomi_param("好的👍 明天见", "x", 128), "好的 明天见");
        assert_eq!(
            xiaomi_param("小明 👨\u{200D}👩\u{200D}👧 ❤\u{FE0F} ok", "x", 128),
            "小明 ok"
        );
        assert_eq!(xiaomi_param("1\u{FE0F}\u{20E3} 🇨🇳 ©", "x", 128), "x");
        assert_eq!(
            xiaomi_param("👍🏽", "2 条新消息，打开应用查看", 128),
            "2 条新消息，打开应用查看"
        );
        assert_eq!(
            xiaomi_param("[有人@你] 今晚 7:30 开会…", "x", 128),
            "[有人@你] 今晚 7:30 开会…"
        );
        let long = format!("{}😀{}", "字".repeat(30), "字".repeat(30));
        let cut = xiaomi_param(&long, "x", 50);
        assert_eq!(cut, format!("{}…", "字".repeat(49)));
        assert!(!XIAOMI_REFUSED.is_match(&cut));

        // Through the request: only Xiaomi's parameters lose the emoji.
        let vendor = GetuiAndroidVendor {
            xiaomi_channel_id: Some("100001".into()),
            xiaomi_template_id: Some("xm-direct".into()),
            oppo_private_template_id: Some("op-im".into()),
            ..Default::default()
        };
        let mut envelope = task(crate::infrastructure::push_test_support::push_info_payload(
            "小明🌸",
            "好的👍",
        ));
        envelope.conversation_id = "1-conv".into();
        let plan = push_plan(&envelope).expect("plan");
        let request =
            build_getui_push_request(&envelope, &plan, &android_token(), &vendor, 7_200_000)
                .expect("request");
        let xiaomi: Value = serde_json::from_str(
            request.vendor_options["XM"]["/extra.template_param"]
                .as_str()
                .expect("a JSON string"),
        )
        .expect("template variables json");
        assert_eq!(xiaomi, json!({ "title": "小明", "content": "好的" }));
        assert_eq!(
            request.vendor_options["OP"]["/private_content_parameters"]["content"],
            "好的👍"
        );
        assert_eq!(request.body, "好的👍");
    }

    /// The Xiaomi wording for alerts that show neither sender nor content names the chat kind
    /// and the count, in the binding's language; the other makers keep the generic text.
    #[test]
    fn xiaomi_gets_a_specific_wording_when_nothing_can_be_shown() {
        assert_eq!(
            xiaomi_concealed_text(Lang::Zh, ChatKind::Direct, 3),
            AlertText {
                title: "私聊消息".into(),
                body: "3 条新消息，打开应用查看".into(),
            }
        );
        assert_eq!(
            xiaomi_concealed_text(Lang::Zh, ChatKind::Group, 120).body,
            "99+ 条新消息，打开应用查看"
        );
        assert_eq!(
            xiaomi_concealed_text(Lang::En, ChatKind::Group, 0),
            AlertText {
                title: "Group message".into(),
                body: "1 new message, open the app to read it".into(),
            }
        );
        assert_eq!(
            xiaomi_concealed_text(Lang::En, ChatKind::Direct, 4).body,
            "4 new messages, open the app to read them"
        );

        let vendor = GetuiAndroidVendor {
            xiaomi_channel_id: Some("100001".into()),
            xiaomi_template_id: Some("xm-direct".into()),
            oppo_private_template_id: Some("op-im".into()),
            ..Default::default()
        };
        let mut envelope = mention_task(NotificationPreview::Hidden);
        envelope.conversation_id = "1-conv".into();
        envelope.count = 2;
        let plan = push_plan(&envelope).expect("plan");
        let request =
            build_getui_push_request(&envelope, &plan, &token_in("en"), &vendor, 7_200_000)
                .expect("request");
        assert_eq!(request.title, "[2 messages] New message");
        let options = &request.vendor_options;
        let xiaomi: Value = serde_json::from_str(
            options["XM"]["/extra.template_param"]
                .as_str()
                .expect("a JSON string"),
        )
        .expect("template variables json");
        assert_eq!(
            xiaomi,
            json!({
                "title": "Direct message",
                "content": "2 new messages, open the app to read them",
            })
        );
        assert_eq!(
            options["OP"]["/private_content_parameters"]["content"],
            "You have a new message"
        );
        // Sender only shows the sender: not generic, Xiaomi gets the rendered text.
        let mut envelope = mention_task(NotificationPreview::SenderOnly);
        envelope.conversation_id = "1-conv".into();
        let plan = push_plan(&envelope).expect("plan");
        let request =
            build_getui_push_request(&envelope, &plan, &token_in("zh-CN"), &vendor, 7_200_000)
                .expect("request");
        let xiaomi: Value = serde_json::from_str(
            request.vendor_options["XM"]["/extra.template_param"]
                .as_str()
                .expect("a JSON string"),
        )
        .expect("template variables json");
        assert_eq!(
            xiaomi,
            json!({ "title": "小明", "content": "[有人@你] 发来一条消息" })
        );
        // Quiet updates carry no vendor notification, so no options either.
        let mut quiet = mention_task(NotificationPreview::Full);
        quiet.disposition = PushDisposition::Quiet as i32;
        let plan = push_plan(&quiet).expect("plan");
        let request =
            build_getui_push_request(&quiet, &plan, &token_in("zh-CN"), &vendor, 7_200_000)
                .expect("request");
        assert!(request.vendor_options.is_empty());
    }

    /// A quiet update must land on the notification it refreshes and must not ring again.
    /// It reaches Android through the transmission alone: most makers cannot replace a
    /// notification silently.
    #[test]
    fn quiet_and_recall_updates_override_the_same_notification_without_sound() {
        let loud = {
            let mut envelope = task(crate::infrastructure::push_test_support::alert_payload());
            envelope.collapse_key = COLLAPSE_KEY.to_string();
            envelope
        };
        let loud_body = build_getui_single_cid_body(&request_for(&loud).expect("request"));
        for (disposition, name) in [
            (PushDisposition::Quiet, "quiet"),
            (PushDisposition::RecallReplace, "recall_replace"),
        ] {
            let mut envelope = loud.clone();
            envelope.disposition = disposition as i32;
            envelope.count = 4;
            envelope.badge = Some(11);
            let request = request_for(&envelope).expect("request");
            assert_eq!(request.title, "[4条] 新消息");
            assert_eq!(request.payload["disposition"], name);
            assert_eq!(request.payload["count"], "4");
            assert_eq!(request.payload["badge"], "11");
            assert_eq!(request.payload["sound"], "0");
            assert!(!request.vendor_notification);
            let body = build_getui_single_cid_body(&request);
            assert!(body["push_channel"].get("android").is_none());
            assert_eq!(
                body["push_channel"]["ios"]["apns-collapse-id"],
                loud_body["push_channel"]["ios"]["apns-collapse-id"]
            );
            assert!(body["push_channel"]["ios"]["aps"]["sound"].is_null());
            // The router's absolute badge replaces the per-message increment.
            assert_eq!(body["push_channel"]["ios"]["auto_badge"], "11");
        }
        let mut recalled = loud.clone();
        recalled.disposition = PushDisposition::RecallReplace as i32;
        assert_eq!(
            request_for(&recalled).expect("request").body,
            "撤回了一条消息"
        );
        let mut digest = loud.clone();
        digest.disposition = PushDisposition::Digest as i32;
        assert!(request_for(&digest).expect("request").vendor_notification);
    }

    #[test]
    fn notify_id_is_a_stable_positive_i32() {
        assert_eq!(getui_notify_id(""), None);
        let id = getui_notify_id(COLLAPSE_KEY).expect("notify id");
        assert!(id > 0);
        assert_eq!(Some(id), getui_notify_id(COLLAPSE_KEY));
        assert_ne!(
            Some(id),
            getui_notify_id("c:fedcba9876543210fedcba9876543210")
        );
        // No collapse key, no override identifier: unrelated notifications must not replace
        // each other.
        let body = build_getui_single_cid_body(
            &request_for(&task(
                crate::infrastructure::push_test_support::alert_payload(),
            ))
            .expect("request"),
        );
        assert!(
            body["push_channel"]["android"]["ups"]["notification"]
                .get("notify_id")
                .is_none()
        );
        assert!(
            body["push_channel"]["ios"]
                .get("apns-collapse-id")
                .is_none()
        );
        assert!(
            body["push_channel"]["ios"]["aps"]
                .get("thread-id")
                .is_none()
        );
    }

    #[test]
    fn auth_sign_matches_getui_formula() {
        let sign = getui_auth_sign("app-key", "1710000000000", "secret");
        let mut hasher = Sha256::new();
        hasher.update(b"app-key1710000000000secret");
        assert_eq!(sign, hex::encode(hasher.finalize()));
    }

    #[test]
    fn request_id_is_stable_and_getui_sized() {
        let envelope = task(Vec::new());

        let id = getui_request_id(&envelope, &token());

        assert_eq!(id.len(), 32);
        assert_eq!(id, getui_request_id(&envelope, &token()));

        // Getui drops a repeated request_id: a later task about the same message needs its own.
        let mut refreshed = envelope.clone();
        refreshed.disposition = PushDisposition::Quiet as i32;
        refreshed.count = 2;
        let mut recalled = envelope.clone();
        recalled.disposition = PushDisposition::RecallReplace as i32;
        let mut keyed = envelope.clone();
        keyed.idempotency_key = "key-1".to_string();
        for other in [&refreshed, &recalled, &keyed] {
            let other_id = getui_request_id(other, &token());
            assert_eq!(other_id.len(), 32);
            assert_ne!(id, other_id);
        }
    }

    #[test]
    fn single_cid_body_uses_official_shape() {
        let mut payload = Map::new();
        payload.insert("conversation_id".into(), json!("conv-1"));
        let request = GetuiPushRequest {
            cid: "cid-1".to_string(),
            request_id: "flare-request-1".to_string(),
            title: "Flare IM".to_string(),
            body: "你收到一条新消息".to_string(),
            payload,
            ttl_ms: 7_200_000,
            play_sound: true,
            show_badge: true,
            ..bare_request()
        };

        let body = build_getui_single_cid_body(&request);

        assert_eq!(body["request_id"], "flare-request-1");
        assert_eq!(body["settings"]["ttl"], 7_200_000);
        assert_eq!(body["audience"]["cid"][0], "cid-1");
        assert_eq!(
            body["push_message"]["transmission"],
            r#"{"conversation_id":"conv-1"}"#
        );
        assert_eq!(body["push_channel"]["ios"]["type"], "notify");
        assert_eq!(body["push_channel"]["ios"]["auto_badge"], "+1");
        assert_eq!(
            body["push_channel"]["harmony"]["notification"]["title"],
            "Flare IM"
        );
    }

    #[test]
    fn invalid_target_business_error_is_cleanup_signal() {
        assert!(is_invalid_target_business_error(
            20_001,
            "target user is invalid"
        ));
        assert!(!is_invalid_target_business_error(
            20_001,
            "appid not match cid"
        ));
        assert!(!is_invalid_target_business_error(
            20_001,
            "title can not be empty"
        ));
        assert!(!is_invalid_target_business_error(10_001, "token错误/失效"));
    }

    #[tokio::test]
    async fn invalid_getui_target_removes_device_token_and_acks_task() {
        let repo = Arc::new(FakeTokenRepository {
            removed: AtomicUsize::new(0),
        });
        let executor = Arc::new(GetuiOfflinePushExecutor::new(
            repo.clone(),
            Arc::new(InvalidTargetPusher),
            7_200_000,
            GetuiAndroidVendor::default(),
        ));
        let envelope = task(crate::infrastructure::push_test_support::alert_payload());

        deliver(&executor, &envelope).await.expect("deliver");

        assert_eq!(repo.removed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn message_offline_push_info_drives_display() {
        let push = PushMessageRequest {
            user_ids: vec!["user-1".to_string()],
            messages: vec![Message {
                offline_push_info: Some(OfflinePushInfo {
                    title: "标题".to_string(),
                    body: "正文".to_string(),
                    sound: "default".to_string(),
                    badge: true,
                    payload: String::new(),
                }),
                ..Default::default()
            }],
            options: None,
        };
        let request = request_for(&task(push.encode_to_vec())).expect("request");

        assert_eq!(request.title, "标题");
        assert_eq!(request.body, "正文");
        assert!(request.play_sound);
        // No absolute badge from the router: a message that counts toward the badge adds one.
        let body = build_getui_single_cid_body(&request);
        assert_eq!(body["push_channel"]["ios"]["auto_badge"], "+1");
    }

    #[test]
    fn e2ee_message_ignores_plain_offline_push_info() {
        let push = PushMessageRequest {
            user_ids: vec!["user-1".to_string()],
            messages: vec![Message {
                content: Some(MessageContent {
                    content: Some(MessageContentVariant::Placeholder(PlaceholderContent {
                        reason: E2EE_PLACEHOLDER_REASON.to_string(),
                        payload: vec![1, 2, 3],
                        fallback_text: "[Encrypted message]".to_string(),
                        attributes: Default::default(),
                    })),
                }),
                offline_push_info: Some(OfflinePushInfo {
                    title: "secret-title".to_string(),
                    body: "secret-body".to_string(),
                    sound: "default".to_string(),
                    badge: true,
                    payload: String::new(),
                }),
                ..Default::default()
            }],
            options: None,
        };

        let request = request_for(&task(push.encode_to_vec())).expect("request");

        assert_eq!(request.title, "新消息");
        assert_eq!(request.body, "你收到一条新消息");
    }

    #[test]
    fn redacted_message_ignores_plain_offline_push_info() {
        let push = PushMessageRequest {
            user_ids: vec!["user-1".to_string()],
            messages: vec![Message {
                retention_state: Some(MessageRetentionState {
                    content_visibility: ContentVisibility::Redacted as i32,
                    ..Default::default()
                }),
                offline_push_info: Some(OfflinePushInfo {
                    title: "redacted-title".to_string(),
                    body: "redacted-body".to_string(),
                    sound: "default".to_string(),
                    badge: true,
                    payload: String::new(),
                }),
                ..Default::default()
            }],
            options: None,
        };

        let request = request_for(&task(push.encode_to_vec())).expect("request");

        assert_eq!(request.title, "新消息");
        assert_eq!(request.body, "你收到一条新消息");

        // A recall replacement of hidden content keeps the generic title as well.
        let mut recalled = task(push.encode_to_vec());
        recalled.disposition = PushDisposition::RecallReplace as i32;
        let request = request_for(&recalled).expect("request");
        assert_eq!(request.title, "新消息");
        assert_eq!(request.body, "撤回了一条消息");
    }

    /// A message whose sender filled in a title and a body, mentioning the recipient.
    fn mention_task(preview: NotificationPreview) -> PushTaskEnvelope {
        PushTaskEnvelope {
            alert_class: AlertClass::Mention as i32,
            preview: preview as i32,
            ..task(crate::infrastructure::push_test_support::push_info_payload(
                "小明",
                "今晚的方案我改好了",
            ))
        }
    }

    fn token_in(locale: &str) -> DevicePushToken {
        DevicePushToken {
            locale: locale.to_string(),
            ..android_token()
        }
    }

    /// Every rendering of the text (the request, the transmission, the Android vendor
    /// notification, the iOS channel) follows the binding's language and the preview setting.
    #[test]
    fn text_follows_the_binding_language_and_the_preview_setting() {
        for (preview, locale, title, text) in [
            (
                NotificationPreview::Unspecified,
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
                "zh_CN",
                "小明",
                "[有人@你] 发来一条消息",
            ),
            (
                NotificationPreview::SenderOnly,
                "fr-FR",
                "小明",
                "[Mentioned you] sent you a message",
            ),
            (
                NotificationPreview::Hidden,
                "zh-Hant",
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
            let envelope = mention_task(preview);
            let plan = push_plan(&envelope).expect("plan");
            let request = build_getui_push_request(
                &envelope,
                &plan,
                &token_in(locale),
                &GetuiAndroidVendor::default(),
                7_200_000,
            )
            .expect("request");
            let context = format!("{preview:?} {locale:?}");
            assert_eq!(
                (request.title.as_str(), request.body.as_str()),
                (title, text),
                "{context}"
            );
            assert_eq!(request.payload["title"], title, "{context}");
            assert_eq!(request.payload["body"], text, "{context}");
            let body = build_getui_single_cid_body(&request);
            let ups = &body["push_channel"]["android"]["ups"]["notification"];
            assert_eq!(
                (ups["title"].as_str(), ups["body"].as_str()),
                (Some(title), Some(text))
            );
            let alert = &body["push_channel"]["ios"]["aps"]["alert"];
            assert_eq!(alert, &json!({ "title": title, "body": text }), "{context}");
        }

        // Recall replacements and digests use the binding's language too.
        let mut recalled = mention_task(NotificationPreview::Full);
        recalled.disposition = PushDisposition::RecallReplace as i32;
        let plan = push_plan(&recalled).expect("plan");
        let request = build_getui_push_request(
            &recalled,
            &plan,
            &token_in("en"),
            &GetuiAndroidVendor::default(),
            7_200_000,
        )
        .expect("request");
        assert_eq!(
            (request.title.as_str(), request.body.as_str()),
            ("小明", "Message recalled")
        );
        let mut digest = task(Vec::new());
        digest.disposition = PushDisposition::Digest as i32;
        digest.count = 30;
        digest.digest_conversations = 6;
        let plan = push_plan(&digest).expect("plan");
        let request = build_getui_push_request(
            &digest,
            &plan,
            &token_in("zh-CN"),
            &GetuiAndroidVendor::default(),
            7_200_000,
        )
        .expect("request");
        assert_eq!(request.title, "新消息");
        assert_eq!(request.body, "6 个会话有 30 条新消息");
        assert!(request.vendor_notification);
    }

    /// The executor renders with the language of the binding it delivers to.
    #[tokio::test]
    async fn delivery_renders_in_the_language_of_the_binding() {
        struct English(FakeTokenRepository);
        #[async_trait::async_trait]
        impl DeviceTokenRepository for English {
            async fn is_current(&self, token: &DevicePushToken) -> Result<bool, FlareError> {
                self.0.is_current(token).await
            }
            async fn was_accepted(
                &self,
                token: &DevicePushToken,
                task: &PushTaskEnvelope,
            ) -> Result<bool, FlareError> {
                self.0.was_accepted(token, task).await
            }
            async fn record_accepted(
                &self,
                token: &DevicePushToken,
                task: &PushTaskEnvelope,
            ) -> Result<(), FlareError> {
                self.0.record_accepted(token, task).await
            }
            async fn list_device_tokens(
                &self,
                ctx: &Ctx,
                tenant_id: &str,
                user_id: &str,
                device_id: &str,
            ) -> Result<Vec<DevicePushToken>, FlareError> {
                let mut rows = self
                    .0
                    .list_device_tokens(ctx, tenant_id, user_id, device_id)
                    .await?;
                for row in &mut rows {
                    row.locale = "en-US".to_string();
                }
                Ok(rows)
            }
            async fn remove_device_token(
                &self,
                ctx: &Ctx,
                token: &DevicePushToken,
            ) -> Result<(), FlareError> {
                self.0.remove_device_token(ctx, token).await
            }
        }
        let pusher = Arc::new(RecordingPusher::default());
        let executor = Arc::new(GetuiOfflinePushExecutor::new(
            Arc::new(English(FakeTokenRepository {
                removed: AtomicUsize::new(0),
            })),
            pusher.clone(),
            7_200_000,
            GetuiAndroidVendor::default(),
        ));
        deliver(&executor, &mention_task(NotificationPreview::SenderOnly))
            .await
            .expect("deliver");
        let requests = pusher.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].title, "小明");
        assert_eq!(requests[0].body, "[Mentioned you] sent you a message");
    }

    /// Real push to the Getui sandbox. `cargo test -- --ignored` fails when the
    /// variables below are missing instead of counting as passed.
    #[tokio::test]
    #[ignore = "requires PUSH_WORKER_GETUI_APP_ID/APP_KEY/MASTER_SECRET/SANDBOX_CID for the Getui sandbox"]
    async fn getui_sandbox_push() {
        let app_id = required_env("PUSH_WORKER_GETUI_APP_ID");
        let app_key = required_env("PUSH_WORKER_GETUI_APP_KEY");
        let master_secret = required_env("PUSH_WORKER_GETUI_MASTER_SECRET");
        let cid = required_env("PUSH_WORKER_GETUI_SANDBOX_CID");
        let base_url = std::env::var("PUSH_WORKER_GETUI_BASE_URL").ok();
        let client = GetuiClient::new(
            GetuiConfig::new(app_id, app_key, master_secret, base_url, 7_200_000, 5_000)
                .expect("getui config"),
        )
        .expect("getui client");
        let mut payload = Map::new();
        payload.insert("kind".into(), json!("getui_sandbox_verification"));
        let optional = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        let vendor = GetuiAndroidVendor {
            huawei_category: Some("IM".into()),
            honor_importance: Some("NORMAL".into()),
            vivo_category: Some("IM".into()),
            // The private-message channels and templates are the app's own: set them to
            // exercise the Xiaomi and OPPO options as well.
            xiaomi_channel_id: optional("PUSH_WORKER_GETUI_SANDBOX_XIAOMI_CHANNEL_ID"),
            xiaomi_template_id: optional("PUSH_WORKER_GETUI_SANDBOX_XIAOMI_TEMPLATE_ID"),
            oppo_private_template_id: optional("PUSH_WORKER_GETUI_SANDBOX_OPPO_TEMPLATE_ID"),
            ..Default::default()
        };
        vendor.validate().expect("sandbox vendor options");
        let request_id = sandbox_request_id();
        let text = AlertText {
            title: "Flare IM".to_string(),
            body: "Getui sandbox push verification".to_string(),
        };
        let request = GetuiPushRequest {
            cid,
            vendor_options: vendor_options(
                &vendor,
                ChatKind::Direct,
                &text,
                &text,
                &text,
                &request_id,
            ),
            request_id,
            title: text.title.clone(),
            body: text.body.clone(),
            payload,
            ttl_ms: 7_200_000,
            play_sound: true,
            show_badge: true,
            // Exercises the override, grouping and Android vendor-channel fields against the
            // real API: a field name Getui does not accept fails here instead of in production.
            collapse_key: "c:getui-sandbox-verification".to_string(),
            thread_key: "c:getui-sandbox-verification".to_string(),
            badge: Some(1),
            vendor_notification: true,
            app_bundle: std::env::var("PUSH_WORKER_GETUI_SANDBOX_APP_BUNDLE").unwrap_or_default(),
        };

        let outcome = client.push_single_cid(&request).await.expect("push");

        assert_eq!(outcome, GetuiPushOutcome::Delivered);
    }

    fn required_env(name: &str) -> String {
        std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"))
    }

    fn sandbox_request_id() -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"flare-getui-sandbox");
        hasher.update(now_ms().to_string().as_bytes());
        let digest = hex::encode(hasher.finalize());
        format!("f{}", &digest[..31])
    }
}
