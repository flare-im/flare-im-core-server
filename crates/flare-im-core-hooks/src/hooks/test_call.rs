//! 测试调用：用样例载荷调用目标一次，给结论和耗时。走和发送路径一样的适配器，但不在发送路径上，不计
//! 熔断、不写执行记录。结论只有固定取值，下游原文不回给调用方。

use std::sync::Arc;
use std::time::{Duration, Instant};

use flare_core_base::context::Context;
use flare_server_core::error::{ErrorCode, FlareError};

use crate::Ctx;

use super::adapters::{DefaultHookFactory, test_grpc_pre_send};
use super::config::{HookDefinition, HookFactory, HookTransportConfig};
use super::hook_context_data::{HookContextData, set_hook_context_data};
use super::types::{MessageDraft, PreSendDecision, is_pre_send_rejection};

/// 样例消息的元数据：目标看到它就知道这不是真消息，不该有副作用。
const HOOK_TEST_METADATA_KEY: &str = "flare.hook.test";
const SAMPLE_CONVERSATION_ID: &str = "hook-test-conversation";
const SAMPLE_MESSAGE_ID: &str = "hook-test-message";
const SAMPLE_SENDER_ID: &str = "hook-test-sender";
const SAMPLE_PAYLOAD: &[u8] = b"flare hook test message";
const MAX_CODE_BYTES: usize = 64;

/// 测试调用的结论。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookTestOutcome {
    /// 放行，消息不变。
    Allowed,
    /// 放行，并改写了消息。
    Rewritten,
    /// 目标明确拒绝。
    Rejected,
    /// 目标通了，但要发送者的签名身份，测试调用没有。
    IdentityRequired,
    /// 超时、不可达或回答读不懂。
    Failed,
}

impl HookTestOutcome {
    /// 管理接口里的取值（固定）。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::Rewritten => "rewritten",
            Self::Rejected => "rejected",
            Self::IdentityRequired => "identity_required",
            Self::Failed => "failed",
        }
    }
}

/// 一次测试调用的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookTestReport {
    pub outcome: HookTestOutcome,
    pub latency: Duration,
    /// 目标给的拒绝原因码，或失败类别（`timeout`、`unavailable`、`bad_response`）。
    pub code: String,
}

fn sample_context(tenant: &str, request_id: &str) -> Ctx {
    let ctx = Context::with_request_id(request_id).with_tenant_id(tenant);
    let data = HookContextData::new()
        .with_conversation_id(SAMPLE_CONVERSATION_ID)
        .with_conversation_type("single")
        .with_message_type("text")
        .with_sender_id(SAMPLE_SENDER_ID);
    Arc::new(set_hook_context_data(ctx, data))
}

fn sample_draft() -> MessageDraft {
    let mut draft = MessageDraft::new(SAMPLE_PAYLOAD.to_vec());
    draft.set_message_id(SAMPLE_MESSAGE_ID);
    draft.set_client_message_id(SAMPLE_MESSAGE_ID);
    draft.set_conversation_id(SAMPLE_CONVERSATION_ID);
    draft
        .metadata
        .insert(HOOK_TEST_METADATA_KEY.to_string(), "true".to_string());
    draft
}

fn rewritten(before: &MessageDraft, after: &MessageDraft) -> bool {
    before.payload != after.payload
        || before.headers != after.headers
        || before.metadata != after.metadata
}

/// 原因码进管理接口：只留形如 `SENSITIVE_WORD` 的取值，其余当作没有。
fn safe_code(code: &str) -> String {
    let safe = !code.is_empty()
        && code.len() <= MAX_CODE_BYTES
        && code
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'));
    if safe {
        code.to_string()
    } else {
        String::new()
    }
}

fn failure_category(error: &FlareError) -> &'static str {
    match error.code() {
        Some(ErrorCode::DeserializationError) => "bad_response",
        _ => "unavailable",
    }
}

fn rejection_code(error: &FlareError) -> String {
    match error {
        FlareError::Localized {
            details: Some(details),
            ..
        } => safe_code(details),
        _ => String::new(),
    }
}

async fn call_webhook(
    definition: &HookDefinition,
    ctx: &Ctx,
    draft: &MessageDraft,
) -> (HookTestOutcome, String) {
    let factory = match DefaultHookFactory::new() {
        Ok(factory) => factory,
        Err(_) => return (HookTestOutcome::Failed, "unavailable".to_string()),
    };
    let handler = match factory.build_pre_send(definition, &definition.selector()) {
        Ok(handler) => handler,
        Err(_) => return (HookTestOutcome::Failed, "unavailable".to_string()),
    };
    let mut answered = draft.clone();
    match handler.handle(ctx, &mut answered).await {
        PreSendDecision::Continue if rewritten(draft, &answered) => {
            (HookTestOutcome::Rewritten, String::new())
        }
        PreSendDecision::Continue => (HookTestOutcome::Allowed, String::new()),
        PreSendDecision::Reject { error } if is_pre_send_rejection(&error) => {
            (HookTestOutcome::Rejected, rejection_code(&error))
        }
        PreSendDecision::Reject { error } => (
            HookTestOutcome::Failed,
            failure_category(&error).to_string(),
        ),
    }
}

async fn call_grpc(
    definition: &HookDefinition,
    endpoint: &str,
    metadata: &std::collections::HashMap<String, String>,
    ctx: &Ctx,
    draft: &MessageDraft,
) -> (HookTestOutcome, String) {
    let mut merged = definition.metadata.clone();
    merged.extend(metadata.clone());
    match test_grpc_pre_send(endpoint, &merged, ctx, draft).await {
        Ok(answer) if !answer.allow => (
            HookTestOutcome::Rejected,
            safe_code(&answer.deny_reason_code),
        ),
        Ok(answer) => {
            let changed = answer.draft.is_some_and(|answered| {
                answered.payload != draft.payload
                    || answered.headers != draft.headers
                    || answered.metadata != draft.metadata
            });
            let outcome = if changed {
                HookTestOutcome::Rewritten
            } else {
                HookTestOutcome::Allowed
            };
            (outcome, String::new())
        }
        Err(status)
            if matches!(
                status.code(),
                tonic::Code::Unauthenticated | tonic::Code::PermissionDenied
            ) =>
        {
            (HookTestOutcome::IdentityRequired, String::new())
        }
        Err(status) if status.code() == tonic::Code::Internal => {
            (HookTestOutcome::Failed, "bad_response".to_string())
        }
        Err(_) => (HookTestOutcome::Failed, "unavailable".to_string()),
    }
}

/// 调用目标一次。`tenant` 是样例消息所属的应用，`request_id` 进目标的日志；时限是这条 Hook 的超时。
pub async fn test_call(
    definition: &HookDefinition,
    tenant: &str,
    request_id: &str,
) -> HookTestReport {
    let ctx = sample_context(tenant, request_id);
    let draft = sample_draft();
    let started = Instant::now();
    let call = async {
        match &definition.transport {
            HookTransportConfig::Grpc { endpoint, metadata } => {
                call_grpc(definition, endpoint, metadata, &ctx, &draft).await
            }
            HookTransportConfig::Webhook { .. } => call_webhook(definition, &ctx, &draft).await,
            HookTransportConfig::Local { .. } => {
                (HookTestOutcome::Failed, "unavailable".to_string())
            }
        }
    };
    let timeout = Duration::from_millis(definition.timeout_ms);
    let (outcome, code) = tokio::time::timeout(timeout, call)
        .await
        .unwrap_or((HookTestOutcome::Failed, "timeout".to_string()));
    HookTestReport {
        outcome,
        latency: started.elapsed(),
        code,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn webhook(endpoint: String, timeout_ms: u64) -> HookDefinition {
        HookDefinition {
            name: "probe".into(),
            timeout_ms,
            transport: HookTransportConfig::Webhook {
                endpoint,
                secret: None,
                headers: HashMap::new(),
            },
            ..HookDefinition::default()
        }
    }

    /// 回一个固定 JSON 的本地 Webhook，回答前先把请求读完。
    async fn answering(body: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = vec![0u8; 8192];
            let _ = stream.read(&mut buffer).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
        format!("http://{address}/hook")
    }

    #[tokio::test]
    async fn an_allowing_target_reports_allowed() {
        let endpoint = answering(r#"{"allow":true}"#).await;
        let report = test_call(&webhook(endpoint, 2_000), "acme", "req").await;
        assert_eq!(report.outcome, HookTestOutcome::Allowed);
        assert!(report.code.is_empty());
    }

    #[tokio::test]
    async fn a_policy_rejection_is_not_a_failure() {
        let endpoint =
            answering(r#"{"allow":false,"status":{"code":"BLOCKED_WORD","message":"no"}}"#).await;
        let report = test_call(&webhook(endpoint, 2_000), "acme", "req").await;
        assert_eq!(report.outcome, HookTestOutcome::Rejected);
        assert_eq!(report.code, "BLOCKED_WORD");
    }

    #[tokio::test]
    async fn an_unreadable_answer_is_a_bad_response() {
        let endpoint = answering("not json").await;
        let report = test_call(&webhook(endpoint, 2_000), "acme", "req").await;
        assert_eq!(report.outcome, HookTestOutcome::Failed);
        assert_eq!(report.code, "bad_response");
    }

    #[tokio::test]
    async fn a_silent_target_times_out_within_the_hook_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/hook", listener.local_addr().unwrap());
        let report = test_call(&webhook(endpoint, 50), "acme", "req").await;
        drop(listener);
        assert_eq!(report.outcome, HookTestOutcome::Failed);
        assert_eq!(report.code, "timeout");
    }

    #[test]
    fn only_code_shaped_values_reach_the_operator() {
        assert_eq!(safe_code("BLOCKED_WORD"), "BLOCKED_WORD");
        assert_eq!(safe_code("服务端拒绝发送此消息"), "");
        assert_eq!(safe_code("a b"), "");
        assert_eq!(safe_code(&"x".repeat(65)), "");
    }
}
