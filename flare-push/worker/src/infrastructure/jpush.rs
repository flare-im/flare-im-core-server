use std::sync::Arc;
use std::time::{Duration, SystemTime};

use flare_im_core_contracts::{Ctx, DevicePushToken};
use flare_im_notify_rules::text::Lang;
use flare_proto::PushTaskEnvelope;
use flare_server_core::{ErrorCode, FlareError};
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::domain::offline_delivery::DeviceTokenRepository;
use crate::domain::vendor_delivery::{
    BindingDelivery, BindingOutcome, CAPABILITY_SERVICE_EXTENSION, FailureReason, ThrottleScope,
    Vendor, binding_has_capability, is_alert_disposition, is_apple_platform,
};
use crate::infrastructure::push_display::{
    ClientText, PushPlan, ValueShape, client_payload, push_plan,
};
use crate::infrastructure::vendor_http;

const ENDPOINT: &str = "https://api.jpush.cn/v3/push";
const MAX_TTL_MS: u64 = 3 * 24 * 60 * 60 * 1_000;

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JpushCredentials {
    app_key: String,
    master_secret: String,
}

pub(crate) struct JpushExecutor {
    credentials: JpushCredentials,
    http: reqwest::Client,
    tokens: Arc<dyn DeviceTokenRepository>,
    default_ttl_ms: u64,
    request_timeout: Duration,
    endpoint: String,
}

impl JpushExecutor {
    pub(crate) fn new(
        credentials: JpushCredentials,
        http: reqwest::Client,
        tokens: Arc<dyn DeviceTokenRepository>,
        default_ttl_ms: u64,
        request_ms: u64,
    ) -> Result<Self, FlareError> {
        let valid = |value: &str| {
            !value.is_empty()
                && value.len() <= 128
                && value.bytes().all(|b| b.is_ascii_graphic() && b != b':')
        };
        if !valid(&credentials.app_key) || !valid(&credentials.master_secret) {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "JPush credentials invalid",
            ));
        }
        Ok(Self {
            credentials,
            http,
            tokens,
            default_ttl_ms,
            request_timeout: Duration::from_millis(request_ms.clamp(1_000, 10_000)),
            endpoint: ENDPOINT.into(),
        })
    }
}

fn request_body(
    task: &PushTaskEnvelope,
    plan: &PushPlan,
    token: &DevicePushToken,
    default_ttl_ms: u64,
    now_ms: i64,
) -> Option<Value> {
    if matches!(plan, PushPlan::Nothing) {
        return None;
    }
    let apple = is_apple_platform(&token.platform) && !token.platform.eq_ignore_ascii_case("macos");
    if !apple && !token.platform.eq_ignore_ascii_case("android") {
        return None;
    }
    let ttl_ms = match task.expire_at.filter(|expires| *expires > 0) {
        Some(expires) if expires <= now_ms => return None,
        Some(expires) => expires.saturating_sub(now_ms) as u64,
        None => default_ttl_ms,
    };
    let lang = Lang::from_locale(&token.locale);
    let text = if apple {
        ClientText::Omitted
    } else {
        ClientText::Included(lang)
    };
    let mut extras = client_payload(task, plan, ValueShape::Strings, text);
    extras.insert("flare_provider".into(), json!(Vendor::Jpush.as_str()));
    let mut body = json!({
        "platform": [if apple { "ios" } else { "android" }],
        "audience": { "registration_id": [token.token] },
        "options": { "time_to_live": ttl_ms.min(MAX_TTL_MS).div_ceil(1000) }
    });
    if apple {
        body["options"]["apns_production"] = json!(token.environment != "sandbox");
        if !task.collapse_key.is_empty() {
            body["options"]["apns_collapse_id"] = json!(task.collapse_key);
        }
        let mut ios = json!({"extras": extras});
        if let PushPlan::Alert(alert) = plan {
            let text = alert.text(lang);
            ios["alert"] = json!({"title": text.title, "body": text.body});
            ios["interruption-level"] = json!(alert.interruption.apns_level());
            if !task.thread_key.is_empty() {
                ios["thread-id"] = json!(task.thread_key);
            }
            if alert.play_sound {
                ios["sound"] = json!("default");
            }
            if binding_has_capability(token, CAPABILITY_SERVICE_EXTENSION)
                && is_alert_disposition(task)
            {
                ios["mutable-content"] = json!(true);
            }
        } else {
            ios["content-available"] = json!(true);
        }
        if let Some(badge) = task.badge.filter(|_| match plan {
            PushPlan::Alert(alert) => alert.show_badge,
            _ => true,
        }) {
            // JPush's APNs badge accepts values 0 through 99.
            ios["badge"] = json!(badge.min(99).to_string());
        }
        body["notification"] = json!({"ios": ios});
    } else {
        // Android uses the same native renderer, deduplication and account checks as FCM data.
        body["message"] = json!({"msg_content": "Flare", "extras": extras});
    }
    Some(body)
}

#[async_trait::async_trait]
impl BindingDelivery for JpushExecutor {
    async fn deliver_binding(
        &self,
        _ctx: &Ctx,
        task: &PushTaskEnvelope,
        token: &DevicePushToken,
    ) -> BindingOutcome {
        if !token.usable_for_provider(Vendor::Jpush.as_str()) {
            return BindingOutcome::Skipped;
        }
        let plan = match push_plan(task) {
            Ok(plan) => plan,
            Err(error) => return BindingOutcome::terminal(error, FailureReason::Config, "task"),
        };
        let now_ms = SystemTime::UNIX_EPOCH
            .elapsed()
            .unwrap_or_default()
            .as_millis() as i64;
        let Some(body) = request_body(task, &plan, token, self.default_ttl_ms, now_ms) else {
            return BindingOutcome::Skipped;
        };
        match self.tokens.was_accepted(token, task).await {
            Ok(true) => return BindingOutcome::AlreadyAccepted,
            Ok(false) => {}
            Err(error) => return BindingOutcome::registry(error),
        }
        match self.tokens.is_current(token).await {
            Ok(true) => {}
            Ok(false) => return BindingOutcome::Skipped,
            Err(error) => return BindingOutcome::registry(error),
        }
        let response = match self
            .http
            .post(&self.endpoint)
            .basic_auth(
                &self.credentials.app_key,
                Some(&self.credentials.master_secret),
            )
            .timeout(self.request_timeout)
            .json(&body)
            .send()
            .await
        {
            Ok(response) => response,
            Err(_) => {
                return BindingOutcome::retryable(
                    vendor_http::failure("JPush transport failed; acceptance unknown"),
                    FailureReason::Transport,
                    "transport",
                );
            }
        };
        let status = response.status();
        let retry_after = vendor_http::retry_after(&response);
        let value = vendor_http::body(response)
            .await
            .ok()
            .and_then(|body| serde_json::from_str::<Value>(&body).ok())
            .unwrap_or_default();
        if status == StatusCode::OK
            && value
                .get("msg_id")
                .is_some_and(|id| id.as_str().is_some_and(|id| !id.is_empty()) || id.is_u64())
        {
            return match self.tokens.record_accepted(token, task).await {
                Ok(()) => BindingOutcome::Accepted,
                Err(error) => BindingOutcome::registry(error),
            };
        }
        rejected_response(status, &value, retry_after)
    }
}

fn rejected_response(
    status: StatusCode,
    value: &Value,
    retry_after: Option<Duration>,
) -> BindingOutcome {
    let code = value.pointer("/error/code").and_then(Value::as_i64);
    if status == StatusCode::TOO_MANY_REQUESTS || matches!(code, Some(1012 | 2002)) {
        return BindingOutcome::Throttled {
            retry_after,
            code: code.map_or_else(|| status.as_str().to_owned(), |code| code.to_string()),
            scope: ThrottleScope::Application,
        };
    }
    if status == StatusCode::BAD_REQUEST && code == Some(1011) {
        // A Registration ID for a different AppKey also yields 1011. Preserve the binding.
        return BindingOutcome::Skipped;
    }
    let label = code.map_or_else(|| status.as_str().to_owned(), |code| code.to_string());
    if status.is_server_error() || status.is_success() {
        return BindingOutcome::retryable(
            vendor_http::failure("JPush response failed or acceptance missing"),
            FailureReason::Server,
            label,
        );
    }
    let reason = if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        FailureReason::Auth
    } else {
        FailureReason::Rejected
    };
    BindingOutcome::terminal(
        FlareError::localized(ErrorCode::MessageDeliveryFailed, "JPush request rejected"),
        reason,
        label,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::push_test_support::{Tokens, device_task, http_peer, token};
    use flare_proto::common::PushDisposition;

    #[test]
    fn addresses_one_registration_with_platform_specific_payload() {
        let task = device_task("device-a", PushDisposition::Loud);
        let plan = push_plan(&task).unwrap();
        let mut binding = token("device-a");
        binding.provider = "jpush".into();
        let request = request_body(&task, &plan, &binding, 6_000, 10_000).unwrap();
        assert_eq!(request["audience"]["registration_id"], json!(["device-a"]));
        assert_eq!(request["message"]["extras"]["conversation_id"], json!("c1"));
        assert_eq!(request["options"]["time_to_live"], json!(6));
        assert!(request.get("notification").is_none());

        binding.platform = "ios".into();
        binding.environment = "sandbox".into();
        binding.capabilities.push("serviceExtension".into());
        let apple = request_body(&task, &plan, &binding, 6_000, 10_000).unwrap();
        assert_eq!(apple["options"]["apns_production"], json!(false));
        assert_eq!(apple["notification"]["ios"]["mutable-content"], json!(true));
        assert_eq!(
            apple["notification"]["ios"]["extras"]["flare_provider"],
            json!("jpush")
        );
    }

    #[test]
    fn expired_tasks_and_ambiguous_target_errors_do_not_remove_bindings() {
        let mut task = device_task("device-a", PushDisposition::Loud);
        task.expire_at = Some(5_000);
        let plan = push_plan(&task).unwrap();
        assert!(request_body(&task, &plan, &token("device-a"), 6_000, 5_000).is_none());
        assert!(matches!(
            rejected_response(
                StatusCode::BAD_REQUEST,
                &json!({"error":{"code":1011}}),
                None
            ),
            BindingOutcome::Skipped
        ));
        assert!(matches!(
            rejected_response(
                StatusCode::TOO_MANY_REQUESTS,
                &Value::Null,
                Some(Duration::from_secs(3))
            ),
            BindingOutcome::Throttled {
                scope: ThrottleScope::Application,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn vendor_acceptance_is_recorded_and_replay_skips_the_network() {
        let (endpoint, captured) = http_peer(vec![(200, r#"{"msg_id":"receipt"}"#.into())]).await;
        let tokens = Arc::new(Tokens::default());
        let mut executor = JpushExecutor::new(
            JpushCredentials {
                app_key: "test-key".into(),
                master_secret: "test-secret".into(),
            },
            vendor_http::client().unwrap(),
            tokens,
            60_000,
            3_000,
        )
        .unwrap();
        executor.endpoint = endpoint;
        let task = device_task("device-a", PushDisposition::Loud);
        let mut binding = token("device-a");
        binding.provider = "jpush".into();
        let ctx = Ctx::default();
        assert!(matches!(
            executor.deliver_binding(&ctx, &task, &binding).await,
            BindingOutcome::Accepted
        ));
        assert!(matches!(
            executor.deliver_binding(&ctx, &task, &binding).await,
            BindingOutcome::AlreadyAccepted
        ));
        assert_eq!(captured.await.unwrap().len(), 1);
    }
}
