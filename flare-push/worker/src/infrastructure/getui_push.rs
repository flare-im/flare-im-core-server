//! Getui RestAPI V2 offline delivery backend.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::infrastructure::push_display::notification_display;
use flare_im_contracts::{Ctx, DevicePushToken};
use flare_proto::PushTaskEnvelope;
use flare_proto::common::message_content::Content as MessageContentVariant;
use flare_server_core::error::{ErrorCode, FlareError, map_infra_error};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::domain::DeviceTokenRepository;
use crate::interface::messaging::offline_consumer::OfflinePushExecutor;

const GETUI_PROVIDER: &str = "getui";
const TOKEN_REFRESH_SKEW_MS: i64 = 60_000;
const GETUI_TOKEN_EXPIRED_CODE: i64 = 10001;
const E2EE_PLACEHOLDER_REASON: &str = "e2e_ciphertext";

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct GetuiPushRequest {
    pub cid: String,
    pub request_id: String,
    pub title: String,
    pub body: String,
    pub payload: Value,
    pub ttl_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GetuiPushOutcome {
    Delivered,
    InvalidTarget,
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
            .timeout(Duration::from_millis(config.request_timeout_ms.max(1_000)))
            .build()
            .map_err(|error| {
                map_infra_error(
                    error,
                    ErrorCode::ServiceUnavailable,
                    "build getui http client",
                )
            })?;
        Ok(Self {
            http,
            config,
            access_token: Mutex::new(None),
        })
    }

    async fn bearer_token(&self) -> Result<String, FlareError> {
        let now = now_ms();
        {
            let guard = self.access_token.lock().await;
            if let Some(token) = guard.as_ref()
                && token.expire_time_ms > now + TOKEN_REFRESH_SKEW_MS
            {
                return Ok(token.token.clone());
            }
        }

        let token = self.authenticate(now).await?;
        let mut guard = self.access_token.lock().await;
        *guard = Some(token.clone());
        Ok(token.token)
    }

    async fn authenticate(&self, timestamp_ms: i64) -> Result<GetuiAccessToken, FlareError> {
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
        if !status.is_success() {
            return Err(http_status_error(status, "getui auth"));
        }

        let payload = response
            .json::<GetuiResponse<GetuiAuthData>>()
            .await
            .map_err(retryable_getui_error)?;
        if payload.code != 0 {
            return Err(getui_business_error(
                payload.code,
                &payload.msg,
                "getui auth",
            ));
        }
        let Some(data) = payload.data else {
            return Err(FlareError::localized(
                ErrorCode::ServiceUnavailable,
                "getui auth response missing token data",
            ));
        };
        if data.token.trim().is_empty() {
            return Err(FlareError::localized(
                ErrorCode::ServiceUnavailable,
                "getui auth response token is empty",
            ));
        }
        Ok(GetuiAccessToken {
            token: data.token,
            expire_time_ms: data.expire_time,
        })
    }

    async fn push_single_cid_inner(
        &self,
        request: &GetuiPushRequest,
    ) -> Result<GetuiPushOutcome, FlareError> {
        let token = self.bearer_token().await?;
        match self.push_single_cid_with_token(&token, request).await {
            Ok(outcome) => Ok(outcome),
            Err(error) if error.code() == Some(ErrorCode::AuthenticationRequired) => {
                {
                    let mut guard = self.access_token.lock().await;
                    *guard = None;
                }
                let refreshed = self.bearer_token().await?;
                self.push_single_cid_with_token(&refreshed, request).await
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
        let response_text = response.text().await.map_err(retryable_getui_error)?;
        if let Ok(payload) = serde_json::from_str::<GetuiResponse<Value>>(&response_text) {
            if payload.code == 0 {
                return Ok(GetuiPushOutcome::Delivered);
            }
            if is_invalid_target_business_error(payload.code, &payload.msg) {
                return Ok(GetuiPushOutcome::InvalidTarget);
            }
            return Err(getui_business_error(
                payload.code,
                &payload.msg,
                "getui push single cid",
            ));
        }
        if !status.is_success() {
            return Err(http_status_error(status, "getui push single cid"));
        }
        Err(FlareError::localized(
            ErrorCode::ServiceUnavailable,
            format!("getui push single cid response is not valid json: {response_text}"),
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
}

impl GetuiOfflinePushExecutor {
    pub fn new(
        tokens: Arc<dyn DeviceTokenRepository>,
        client: Arc<dyn GetuiPusher>,
        default_ttl_ms: u64,
    ) -> Self {
        Self {
            tokens,
            client,
            default_ttl_ms,
        }
    }
}

#[async_trait::async_trait]
impl OfflinePushExecutor for GetuiOfflinePushExecutor {
    async fn deliver(&self, ctx: &Ctx, envelope: &PushTaskEnvelope) -> Result<(), FlareError> {
        let tokens = self
            .tokens
            .list_user_tokens(ctx, &envelope.tenant_id, &envelope.user_id)
            .await?;
        let usable_tokens = tokens
            .into_iter()
            .filter(|token| token.usable_for_provider(GETUI_PROVIDER))
            .collect::<Vec<_>>();
        if usable_tokens.is_empty() {
            tracing::debug!(
                tenant_id = %envelope.tenant_id,
                user_id = %envelope.user_id,
                message_id = %envelope.message_id,
                "offline push has no getui device token"
            );
            return Ok(());
        }

        let mut attempts = 0usize;
        let mut successes = 0usize;
        let mut invalid_targets = 0usize;
        let mut terminal_error = None::<FlareError>;
        for token in usable_tokens {
            attempts += 1;
            let request = build_getui_push_request(envelope, &token, self.default_ttl_ms);
            match self.client.push_single_cid(&request).await {
                Ok(GetuiPushOutcome::Delivered) => successes += 1,
                Ok(GetuiPushOutcome::InvalidTarget) => {
                    invalid_targets += 1;
                    tracing::warn!(
                        tenant_id = %envelope.tenant_id,
                        user_id = %envelope.user_id,
                        device_id = %token.device_id,
                        provider = %token.provider,
                        "getui reported invalid cid; removing device push token"
                    );
                    self.tokens.remove_device_token(ctx, &token).await?;
                }
                Err(error) if error.is_retryable() => return Err(error),
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        tenant_id = %envelope.tenant_id,
                        user_id = %envelope.user_id,
                        device_id = %token.device_id,
                        "getui terminal delivery failure for device"
                    );
                    terminal_error.get_or_insert(error);
                }
            }
        }

        if successes > 0 || invalid_targets > 0 || attempts == 0 {
            Ok(())
        } else {
            Err(terminal_error.unwrap_or_else(|| {
                FlareError::localized(ErrorCode::MessageDeliveryFailed, "getui delivery failed")
            }))
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

pub fn build_getui_push_request(
    envelope: &PushTaskEnvelope,
    token: &DevicePushToken,
    default_ttl_ms: u64,
) -> GetuiPushRequest {
    let notification = notification_display(envelope);
    let payload = json!({
        "provider": GETUI_PROVIDER,
        "tenantId": envelope.tenant_id,
        "userId": envelope.user_id,
        "conversationId": envelope.conversation_id,
        "messageId": envelope.message_id,
        "payloadKind": envelope.payload_kind,
        "deviceId": token.device_id,
    });
    GetuiPushRequest {
        cid: token.token.clone(),
        request_id: getui_request_id(envelope, token),
        title: notification.title,
        body: notification.body,
        payload,
        ttl_ms: default_ttl_ms,
    }
}

pub fn build_getui_single_cid_body(request: &GetuiPushRequest) -> Value {
    let payload = request.payload.to_string();
    json!({
        "request_id": request.request_id,
        "settings": {
            "ttl": request.ttl_ms,
        },
        "audience": {
            "cid": [request.cid],
        },
        "push_message": {
            "notification": {
                "title": request.title,
                "body": request.body,
                "click_type": "payload",
                "payload": payload,
            },
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
    })
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
    let digest = hex::encode(hasher.finalize());
    format!("f{}", &digest[..31])
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

fn retryable_getui_error(error: reqwest::Error) -> FlareError {
    map_infra_error(error, ErrorCode::ServiceUnavailable, "getui request failed")
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

fn getui_business_error(code: i64, msg: &str, operation: &'static str) -> FlareError {
    let error_code = if code == GETUI_TOKEN_EXPIRED_CODE {
        ErrorCode::AuthenticationRequired
    } else {
        ErrorCode::MessageDeliveryFailed
    };
    FlareError::localized(
        error_code,
        format!("{operation} failed: code={code} msg={msg}"),
    )
}

fn is_invalid_target_business_error(code: i64, msg: &str) -> bool {
    if code != 20_001 {
        return false;
    }
    let lower = msg.to_ascii_lowercase();
    lower.contains("target user is invalid")
        || lower.contains("appid not match cid")
        || lower.contains("cid无效")
        || lower.contains("cid不是当前应用")
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
    use flare_proto::common::{ContentVisibility, PushTaskPayloadKind};
    use flare_proto::common::{
        Message, MessageContent, MessageRetentionState, OfflinePushInfo, PlaceholderContent,
    };
    use prost::Message as _;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn token() -> DevicePushToken {
        DevicePushToken {
            tenant_id: "tenant-a".to_string(),
            user_id: "user-1".to_string(),
            device_id: "ios-1".to_string(),
            platform: "ios".to_string(),
            provider: "getui".to_string(),
            token: "cid-1".to_string(),
        }
    }

    struct FakeTokenRepository {
        removed: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl DeviceTokenRepository for FakeTokenRepository {
        async fn list_user_tokens(
            &self,
            _ctx: &Ctx,
            _tenant_id: &str,
            _user_id: &str,
        ) -> Result<Vec<DevicePushToken>, FlareError> {
            Ok(vec![token()])
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

    #[test]
    fn auth_sign_matches_getui_formula() {
        let sign = getui_auth_sign("app-key", "1710000000000", "secret");
        let mut hasher = Sha256::new();
        hasher.update(b"app-key1710000000000secret");
        assert_eq!(sign, hex::encode(hasher.finalize()));
    }

    #[test]
    fn request_id_is_stable_and_getui_sized() {
        let envelope = PushTaskEnvelope {
            tenant_id: "tenant-a".to_string(),
            user_id: "user-1".to_string(),
            message_id: "msg-1".to_string(),
            conversation_id: "conv-1".to_string(),
            priority: 5,
            expire_at: None,
            push_payload: Vec::new(),
            headers: Default::default(),
            payload_kind: PushTaskPayloadKind::Message as i32,
        };

        let id = getui_request_id(&envelope, &token());

        assert_eq!(id.len(), 32);
        assert_eq!(id, getui_request_id(&envelope, &token()));
    }

    #[test]
    fn single_cid_body_uses_official_shape() {
        let request = GetuiPushRequest {
            cid: "cid-1".to_string(),
            request_id: "flare-request-1".to_string(),
            title: "Flare IM".to_string(),
            body: "你收到一条新消息".to_string(),
            payload: json!({"conversationId":"conv-1"}),
            ttl_ms: 7_200_000,
        };

        let body = build_getui_single_cid_body(&request);

        assert_eq!(body["audience"]["cid"][0], "cid-1");
        assert_eq!(
            body["push_message"]["notification"]["click_type"],
            "payload"
        );
        assert_eq!(body["push_channel"]["ios"]["auto_badge"], "+1");
    }

    #[test]
    fn invalid_target_business_error_is_cleanup_signal() {
        assert!(is_invalid_target_business_error(
            20_001,
            "target user is invalid"
        ));
        assert!(is_invalid_target_business_error(
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
        let executor =
            GetuiOfflinePushExecutor::new(repo.clone(), Arc::new(InvalidTargetPusher), 7_200_000);
        let envelope = PushTaskEnvelope {
            tenant_id: "tenant-a".to_string(),
            user_id: "user-1".to_string(),
            message_id: "msg-1".to_string(),
            conversation_id: "conv-1".to_string(),
            priority: 5,
            expire_at: None,
            push_payload: Vec::new(),
            headers: Default::default(),
            payload_kind: PushTaskPayloadKind::Message as i32,
        };
        let ctx = Arc::new(
            flare_server_core::context::Context::root()
                .with_tenant_id("tenant-a")
                .with_user_id("user-1"),
        );

        executor.deliver(&ctx, &envelope).await.expect("deliver");

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
        let envelope = PushTaskEnvelope {
            tenant_id: "tenant-a".to_string(),
            user_id: "user-1".to_string(),
            message_id: "msg-1".to_string(),
            conversation_id: "conv-1".to_string(),
            priority: 5,
            expire_at: None,
            push_payload: push.encode_to_vec(),
            headers: Default::default(),
            payload_kind: PushTaskPayloadKind::Message as i32,
        };

        let request = build_getui_push_request(&envelope, &token(), 7_200_000);

        assert_eq!(request.title, "标题");
        assert_eq!(request.body, "正文");
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
        let envelope = PushTaskEnvelope {
            tenant_id: "tenant-a".to_string(),
            user_id: "user-1".to_string(),
            message_id: "msg-1".to_string(),
            conversation_id: "conv-1".to_string(),
            priority: 5,
            expire_at: None,
            push_payload: push.encode_to_vec(),
            headers: Default::default(),
            payload_kind: PushTaskPayloadKind::Message as i32,
        };

        let request = build_getui_push_request(&envelope, &token(), 7_200_000);

        assert_eq!(request.title, "Flare IM");
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
        let envelope = PushTaskEnvelope {
            tenant_id: "tenant-a".to_string(),
            user_id: "user-1".to_string(),
            message_id: "msg-1".to_string(),
            conversation_id: "conv-1".to_string(),
            priority: 5,
            expire_at: None,
            push_payload: push.encode_to_vec(),
            headers: Default::default(),
            payload_kind: PushTaskPayloadKind::Message as i32,
        };

        let request = build_getui_push_request(&envelope, &token(), 7_200_000);

        assert_eq!(request.title, "Flare IM");
        assert_eq!(request.body, "你收到一条新消息");
    }

    #[tokio::test]
    async fn getui_sandbox_push_when_env_is_enabled() {
        if std::env::var("FLARE_GETUI_INTEGRATION").ok().as_deref() != Some("1") {
            eprintln!("skip getui sandbox push: set FLARE_GETUI_INTEGRATION=1 to enable");
            return;
        }
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
        let request = GetuiPushRequest {
            cid,
            request_id: sandbox_request_id(),
            title: "Flare IM".to_string(),
            body: "Getui sandbox push verification".to_string(),
            payload: json!({"kind":"getui_sandbox_verification"}),
            ttl_ms: 7_200_000,
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
