//! Hook 运行时：按来源顺序跑一条调用链。
//!
//! 每一步的时限取单条超时和总时限剩余的较小值。目标明确拒绝一律拒绝；目标超时、不可达、熔断打开、
//! 改写解不开或总时限耗尽时，必需的回可重试的 503，可选的跳过这一步。
//!
//! 每一步在草稿的副本上跑，放行且改写有效才替换草稿：失败、超时或被跳过的 Hook 不会留下一半改动。

use std::sync::Arc;
use std::time::Duration;

use flare_proto::common::MessageContent;
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use prost::Message as _;
use tokio::time::{Instant, timeout};

use crate::Ctx;

use super::execution::{
    HOOK_CALL_FAILED_CODE, HOOK_REWRITE_INVALID_CODE, HookCall, HookExecutionGuard,
    HookExecutionOutcome, HookIdentity,
};
use super::name::LoggableName;
use super::points::PRE_SEND_HOOK_TYPE;
use super::registry::{HookRegistry, PreSendPlan};
use super::types::{HookMetadata, MessageDraft, PreSendDecision, is_pre_send_rejection};

/// 一次发送的 Hook 总时限默认值（`MESSAGE_INGEST_EXTENSION_PRE_SEND_TIMEOUT_MS`）。
pub const DEFAULT_PRE_SEND_BUDGET: Duration = Duration::from_millis(1_500);

/// Hook 调度器：调用链、熔断与执行记录、总时限。
#[derive(Clone)]
pub struct HookDispatcher {
    registry: Arc<HookRegistry>,
    guard: Option<Arc<HookExecutionGuard>>,
    budget: Duration,
}

impl HookDispatcher {
    pub fn new(registry: Arc<HookRegistry>) -> Self {
        Self {
            registry,
            guard: None,
            budget: DEFAULT_PRE_SEND_BUDGET,
        }
    }

    /// 挂上 (应用, Hook) 熔断与执行记录；没有时直接调用、不记录。
    pub fn with_guard(mut self, guard: Arc<HookExecutionGuard>) -> Self {
        self.guard = Some(guard);
        self
    }

    /// 一次发送的总时限。
    pub fn with_budget(mut self, budget: Duration) -> Self {
        self.budget = budget.max(Duration::from_millis(1));
        self
    }

    pub fn registry(&self) -> &Arc<HookRegistry> {
        &self.registry
    }

    /// 执行 PreSend 调用链：全部放行或跳过时 `Ok`，否则是发给发送方的错误。
    pub async fn pre_send(&self, ctx: &Ctx, draft: &mut MessageDraft) -> Result<()> {
        let chain = self.registry.chain_for(ctx.tenant_id());
        if chain.is_empty() {
            return Ok(());
        }
        let deadline = Instant::now() + self.budget;
        for plan in chain.iter().filter(|plan| plan.selector.matches(ctx)) {
            self.run_step(plan, ctx, draft, deadline).await?;
        }
        Ok(())
    }

    async fn run_step(
        &self,
        plan: &PreSendPlan,
        ctx: &Ctx,
        draft: &mut MessageDraft,
        deadline: Instant,
    ) -> Result<()> {
        let metadata = plan.metadata();
        let identity = HookIdentity {
            source: metadata.source,
            hook_type: PRE_SEND_HOOK_TYPE,
            hook_name: &metadata.name,
            config_id: metadata.config_id,
        };
        let message_id = self.guard.as_ref().and_then(|_| draft_message_id(draft));
        let message_id = message_id.as_deref();

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            if let Some(guard) = self.guard.as_ref() {
                guard.record_skipped(
                    ctx,
                    identity,
                    HookExecutionOutcome::BudgetExhausted,
                    message_id,
                );
            }
            return skip_or_unavailable(metadata, "pre-send hook budget exhausted");
        }

        let call = self
            .guard
            .as_ref()
            .and_then(|guard| guard.begin(ctx, identity));
        if call.as_ref().is_some_and(HookCall::short_circuited) {
            if let Some(call) = call {
                call.finish(HookExecutionOutcome::CircuitOpen, message_id);
            }
            return skip_or_unavailable(metadata, "pre-send hook circuit open");
        }

        let step = metadata.timeout.min(remaining);
        let mut working = draft.clone();
        let (result, outcome) = match timeout(step, plan.handler.handle(ctx, &mut working)).await {
            Err(_) if step < metadata.timeout => (
                skip_or_unavailable(metadata, "pre-send hook budget exhausted"),
                HookExecutionOutcome::BudgetExhausted,
            ),
            Err(_) => (
                skip_or_unavailable(metadata, "pre-send hook timed out"),
                HookExecutionOutcome::TimedOut,
            ),
            Ok(PreSendDecision::Continue) => match validate_rewrite(draft, &working) {
                Ok(()) => {
                    *draft = working;
                    (Ok(()), HookExecutionOutcome::Allowed)
                }
                Err(why) => {
                    tracing::warn!(hook = %LoggableName(&metadata.name), why, "pre-send hook rewrite ignored");
                    (
                        skip_or_unavailable(metadata, "pre-send hook rewrite invalid"),
                        HookExecutionOutcome::Failed {
                            code: HOOK_REWRITE_INVALID_CODE,
                        },
                    )
                }
            },
            Ok(PreSendDecision::Reject { error }) if is_target_failure(&error) => {
                // 下游原文可能带内部地址，只进日志；发送方只看到 Hook 名。
                tracing::warn!(hook = %LoggableName(&metadata.name), cause = %error, "pre-send hook call failed");
                (
                    skip_or_unavailable(metadata, "pre-send hook unavailable"),
                    failed_outcome(&error),
                )
            }
            Ok(PreSendDecision::Reject { error }) => {
                let outcome = HookExecutionOutcome::Rejected {
                    reason: error.reason().to_string(),
                };
                (Err(error), outcome)
            }
        };
        if let Some(call) = call {
            call.finish(outcome, message_id);
        }
        result
    }
}

/// 必需的回可重试的 `ServiceUnavailable`（摄入侧 gRPC `UNAVAILABLE`，HTTP 发送 503），可选的跳过。
fn skip_or_unavailable(metadata: &HookMetadata, reason: &'static str) -> Result<()> {
    if metadata.require_success {
        return Err(ErrorBuilder::new(ErrorCode::ServiceUnavailable, reason)
            .details(format!("hook={}", LoggableName(&metadata.name)))
            .build_error());
    }
    tracing::warn!(hook = %LoggableName(&metadata.name), reason, "optional pre-send hook skipped");
    Ok(())
}

/// 这个拒绝不是目标的决定，而是目标没能给出决定。
fn is_target_failure(error: &FlareError) -> bool {
    !is_pre_send_rejection(error)
        && matches!(
            error.code(),
            Some(
                ErrorCode::ConnectionFailed
                    | ErrorCode::ConnectionTimeout
                    | ErrorCode::ConnectionClosed
                    | ErrorCode::ConnectionRefused
                    | ErrorCode::ServiceUnavailable
                    | ErrorCode::ResourceExhausted
                    | ErrorCode::NetworkError
                    | ErrorCode::NetworkTimeout
                    | ErrorCode::NetworkUnreachable
                    | ErrorCode::NetworkConnectionLost
                    | ErrorCode::OperationTimeout
                    | ErrorCode::HttpBadGateway
                    | ErrorCode::HttpServiceUnavailable
                    | ErrorCode::HttpGatewayTimeout
                    | ErrorCode::HttpRequestTimeout
                    | ErrorCode::SerializationError
                    | ErrorCode::DeserializationError
                    | ErrorCode::ConfigurationError
            )
        )
}

/// 调用失败的执行结果：只记错误码，不记下游错误原文。
fn failed_outcome(cause: &FlareError) -> HookExecutionOutcome {
    HookExecutionOutcome::Failed {
        code: cause
            .code()
            .map(|code| code.as_str())
            .unwrap_or(HOOK_CALL_FAILED_CODE),
    }
}

/// 草稿的消息号：服务端号，没有时取客户端号。
fn draft_message_id(draft: &MessageDraft) -> Option<String> {
    draft
        .message_id
        .clone()
        .or_else(|| draft.client_message_id.clone())
}

/// Hook 改了正文时，新正文要能解成 `MessageContent`，也不能把有内容的消息改成空的。
fn validate_rewrite(
    before: &MessageDraft,
    after: &MessageDraft,
) -> std::result::Result<(), &'static str> {
    if after.payload == before.payload {
        return Ok(());
    }
    match MessageContent::decode(after.payload.as_slice()) {
        Err(_) => Err("rewritten payload does not decode"),
        Ok(content) if content.content.is_none() && !before.payload.is_empty() => {
            Err("rewritten payload is empty")
        }
        Ok(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use flare_proto::common::{TextContent, message_content::Content};
    use flare_server_core::context::Context;

    use super::*;
    use crate::hooks::circuit::HookCircuitPolicy;
    use crate::hooks::execution::guard::test_support::RecordingObserver;
    use crate::hooks::registry::HookScope;
    use crate::hooks::selector::HookSelector;
    use crate::hooks::types::{HookSource, PreSendHook, pre_send_rejection};

    fn ctx(tenant: &str) -> Ctx {
        Arc::new(Context::with_request_id("req-chain").with_tenant_id(tenant))
    }

    fn encoded(text: &str) -> Vec<u8> {
        MessageContent {
            content: Some(Content::Text(TextContent {
                text: text.to_string(),
                ..Default::default()
            })),
        }
        .encode_to_vec()
    }

    /// 可配置的 Hook：记下调用顺序，按 `act` 决定结果。
    struct Scripted {
        name: &'static str,
        log: Arc<Mutex<Vec<&'static str>>>,
        act: fn(&mut MessageDraft) -> PreSendDecision,
    }

    #[async_trait]
    impl PreSendHook for Scripted {
        async fn handle(&self, _ctx: &Ctx, draft: &mut MessageDraft) -> PreSendDecision {
            self.log.lock().unwrap().push(self.name);
            (self.act)(draft)
        }
    }

    fn allow(_: &mut MessageDraft) -> PreSendDecision {
        PreSendDecision::Continue
    }

    fn down(_: &mut MessageDraft) -> PreSendDecision {
        PreSendDecision::Reject {
            error: ErrorBuilder::new(ErrorCode::ServiceUnavailable, "upstream 10.0.0.7:50110")
                .build_error(),
        }
    }

    fn refuse(_: &mut MessageDraft) -> PreSendDecision {
        PreSendDecision::Reject {
            error: pre_send_rejection(ErrorBuilder::new(ErrorCode::OperationFailed, "SPAM")),
        }
    }

    fn garbage(draft: &mut MessageDraft) -> PreSendDecision {
        draft.payload = vec![0xff, 0xff, 0xff];
        draft.metadata("tagged", "by-a-failed-rewrite");
        PreSendDecision::Continue
    }

    fn emptied(draft: &mut MessageDraft) -> PreSendDecision {
        draft.payload = Vec::new();
        PreSendDecision::Continue
    }

    fn masked(draft: &mut MessageDraft) -> PreSendDecision {
        draft.payload = encoded("***");
        PreSendDecision::Continue
    }

    struct Hanging;

    #[async_trait]
    impl PreSendHook for Hanging {
        async fn handle(&self, _ctx: &Ctx, _draft: &mut MessageDraft) -> PreSendDecision {
            std::future::pending().await
        }
    }

    fn required(name: &'static str) -> HookMetadata {
        HookMetadata::default().with_name(name)
    }

    fn optional(name: &'static str) -> HookMetadata {
        required(name).with_require_success(false)
    }

    fn one_hook(
        metadata: HookMetadata,
        act: fn(&mut MessageDraft) -> PreSendDecision,
    ) -> HookDispatcher {
        let registry = HookRegistry::new();
        registry.register_pre_send(
            metadata,
            HookSelector::default(),
            Arc::new(Scripted {
                name: "hook",
                log: Arc::default(),
                act,
            }),
        );
        HookDispatcher::new(registry)
    }

    fn stored(
        source: HookSource,
        name: &'static str,
        order: i32,
        log: &Arc<Mutex<Vec<&'static str>>>,
    ) -> PreSendPlan {
        PreSendPlan::new(
            required(name).with_source(source).with_order(order),
            HookSelector::default(),
            Arc::new(Scripted {
                name,
                log: Arc::clone(log),
                act: allow,
            }),
        )
    }

    /// 顺序是应用 → 平台 → 部署，同来源按 `order`；别的应用只跑平台和部署。
    #[tokio::test]
    async fn the_chain_runs_application_then_platform_then_deployment() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let registry = HookRegistry::new();
        for (name, order) in [("deployment-2", 2), ("deployment-1", 1)] {
            registry.register_pre_send(
                required(name).with_order(order),
                HookSelector::default(),
                Arc::new(Scripted {
                    name,
                    log: Arc::clone(&log),
                    act: allow,
                }),
            );
        }
        registry.replace_stored(HashMap::from([
            (
                HookScope::Platform,
                vec![stored(HookSource::Platform, "platform", 0, &log)],
            ),
            (
                HookScope::Application("acme".into()),
                vec![
                    stored(HookSource::Application, "app-2", 2, &log),
                    stored(HookSource::Application, "app-1", 1, &log),
                ],
            ),
        ]));
        let dispatcher = HookDispatcher::new(Arc::clone(&registry));

        dispatcher
            .pre_send(&ctx("acme"), &mut MessageDraft::new(vec![]))
            .await
            .unwrap();
        assert_eq!(
            *log.lock().unwrap(),
            vec!["app-1", "app-2", "platform", "deployment-1", "deployment-2"]
        );

        log.lock().unwrap().clear();
        dispatcher
            .pre_send(&ctx("other"), &mut MessageDraft::new(vec![]))
            .await
            .unwrap();
        assert_eq!(
            *log.lock().unwrap(),
            vec!["platform", "deployment-1", "deployment-2"]
        );

        // 换指针：应用删光自己的行后走共享链。
        registry.replace_scopes(HashMap::from([(
            HookScope::Application("acme".into()),
            Vec::new(),
        )]));
        log.lock().unwrap().clear();
        dispatcher
            .pre_send(&ctx("acme"), &mut MessageDraft::new(vec![]))
            .await
            .unwrap();
        assert_eq!(
            *log.lock().unwrap(),
            vec!["platform", "deployment-1", "deployment-2"]
        );
    }

    /// 明确拒绝不看必需与否，错误原样回到发送方。
    #[tokio::test]
    async fn an_explicit_rejection_always_rejects() {
        for metadata in [required("gate"), optional("gate")] {
            let error = one_hook(metadata, refuse)
                .pre_send(&ctx("t1"), &mut MessageDraft::new(vec![]))
                .await
                .unwrap_err();
            assert!(is_pre_send_rejection(&error), "{error}");
            assert_eq!(error.reason(), "SPAM");
        }
    }

    /// 目标不可达：必需的回 503，错误里只有 Hook 名，不带下游原文；可选的跳过。
    #[tokio::test]
    async fn an_unavailable_target_is_a_retryable_503_or_a_skip() {
        let error = one_hook(required("gate"), down)
            .pre_send(&ctx("t1"), &mut MessageDraft::new(vec![]))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
        assert!(error.is_retryable(), "{error}");
        assert!(!is_pre_send_rejection(&error), "{error}");
        assert!(!format!("{error:?}").contains("10.0.0.7"), "{error:?}");

        one_hook(optional("gate"), down)
            .pre_send(&ctx("t1"), &mut MessageDraft::new(vec![]))
            .await
            .unwrap();
    }

    /// 连接类错误都按不可用处理，不管下游回的具体错误码。
    #[tokio::test]
    async fn connection_errors_follow_the_required_flag() {
        struct Fails(ErrorCode);
        #[async_trait]
        impl PreSendHook for Fails {
            async fn handle(&self, _ctx: &Ctx, _draft: &mut MessageDraft) -> PreSendDecision {
                PreSendDecision::Reject {
                    error: ErrorBuilder::new(self.0, "connection unavailable").build_error(),
                }
            }
        }
        for code in [
            ErrorCode::ConnectionRefused,
            ErrorCode::ConnectionClosed,
            ErrorCode::ConnectionTimeout,
            ErrorCode::DeserializationError,
        ] {
            for require_success in [true, false] {
                let registry = HookRegistry::new();
                registry.register_pre_send(
                    required("gate").with_require_success(require_success),
                    HookSelector::default(),
                    Arc::new(Fails(code)),
                );
                let result = HookDispatcher::new(registry)
                    .pre_send(&ctx("t1"), &mut MessageDraft::new(vec![]))
                    .await;
                if require_success {
                    assert_eq!(
                        result.unwrap_err().code(),
                        Some(ErrorCode::ServiceUnavailable)
                    );
                } else {
                    assert!(result.is_ok());
                }
            }
        }
    }

    /// 改写解不开：必需的回 503，草稿保持原样；可选的跳过这次改写，别的字段也不留下。
    #[tokio::test]
    async fn an_undecodable_rewrite_is_a_hook_failure_not_an_empty_body() {
        let original = encoded("hello");
        let mut draft = MessageDraft::new(original.clone());
        let error = one_hook(required("mask"), garbage)
            .pre_send(&ctx("t1"), &mut draft)
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
        assert_eq!(error.reason(), "pre-send hook rewrite invalid");
        assert_eq!(draft.payload, original);

        one_hook(optional("mask"), garbage)
            .pre_send(&ctx("t1"), &mut draft)
            .await
            .unwrap();
        assert_eq!(draft.payload, original);
        assert!(!draft.metadata.contains_key("tagged"));

        // 把有内容的消息改成空的，同样算改写失败。
        assert!(
            one_hook(required("mask"), emptied)
                .pre_send(&ctx("t1"), &mut draft)
                .await
                .is_err()
        );
        assert_eq!(draft.payload, original);

        // 有效的改写生效。
        one_hook(required("mask"), masked)
            .pre_send(&ctx("t1"), &mut draft)
            .await
            .unwrap();
        assert_eq!(draft.payload, encoded("***"));
    }

    fn hanging(metadata: HookMetadata) -> HookDispatcher {
        let registry = HookRegistry::new();
        registry.register_pre_send(metadata, HookSelector::default(), Arc::new(Hanging));
        HookDispatcher::new(registry)
    }

    /// A healthy Social decision can take 400 ms while the local host builds/tests.
    /// Exercise the shipped profile, not a hand-picked test timeout: 300 ms used
    /// to cancel all three queue attempts before the policy answered.
    #[tokio::test(start_paused = true)]
    async fn shipped_social_policy_accepts_slow_decisions_but_still_fails_closed() {
        use super::super::config::HookConfig;

        struct DelayedDecision(bool);
        #[async_trait]
        impl PreSendHook for DelayedDecision {
            async fn handle(&self, _: &Ctx, draft: &mut MessageDraft) -> PreSendDecision {
                tokio::time::sleep(Duration::from_millis(400)).await;
                if self.0 { allow(draft) } else { refuse(draft) }
            }
        }
        for profile in [
            "hooks.toml",
            "hooks.social.toml",
            "hooks.social.example.toml",
        ] {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../config")
                .join(profile);
            let config: HookConfig =
                toml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
            let policy = config
                .pre_send
                .iter()
                .find(|p| p.name == "social-policy-pre-send")
                .unwrap();
            assert!(policy.enabled && policy.require_success, "{profile}");
            let metadata = required("social-policy-pre-send")
                .with_timeout(Duration::from_millis(policy.timeout_ms));
            assert!(
                metadata.timeout < DEFAULT_PRE_SEND_BUDGET,
                "reserve time for the rest of the chain"
            );
            for allowed in [true, false] {
                let registry = HookRegistry::new();
                registry.register_pre_send(
                    metadata.clone(),
                    HookSelector::default(),
                    Arc::new(DelayedDecision(allowed)),
                );
                let result = HookDispatcher::new(registry)
                    .pre_send(&ctx("t1"), &mut MessageDraft::new(vec![]))
                    .await;
                if allowed {
                    assert!(
                        result.is_ok(),
                        "{profile}: healthy delayed policy was cancelled: {result:?}"
                    );
                } else {
                    assert_eq!(
                        result.unwrap_err().reason(),
                        "SPAM",
                        "the actual policy denial must survive"
                    );
                }
            }
            let started = Instant::now();
            let error = hanging(metadata.clone())
                .pre_send(&ctx("t1"), &mut MessageDraft::new(vec![]))
                .await
                .unwrap_err();
            assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
            assert_eq!(Instant::now() - started, metadata.timeout);
        }
    }

    /// 单条超时：必需的回 503，可选的跳过。
    #[tokio::test(start_paused = true)]
    async fn a_hook_timeout_is_a_503_or_a_skip() {
        let error = hanging(required("slow").with_timeout(Duration::from_millis(5)))
            .pre_send(&ctx("t1"), &mut MessageDraft::new(vec![]))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
        assert_eq!(error.reason(), "pre-send hook timed out");
        hanging(optional("slow").with_timeout(Duration::from_millis(5)))
            .pre_send(&ctx("t1"), &mut MessageDraft::new(vec![]))
            .await
            .unwrap();
    }

    /// 每一步的时限截到总时限剩余：总时限耗尽时必需的回 503，可选的跳过，后面的步骤不再发出。
    #[tokio::test(start_paused = true)]
    async fn the_total_budget_bounds_every_step() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let registry = HookRegistry::new();
        registry.register_pre_send(
            optional("first")
                .with_order(1)
                .with_timeout(Duration::from_secs(60)),
            HookSelector::default(),
            Arc::new(Hanging),
        );
        registry.register_pre_send(
            required("second").with_order(2),
            HookSelector::default(),
            Arc::new(Scripted {
                name: "second",
                log: Arc::clone(&log),
                act: allow,
            }),
        );
        let started = Instant::now();
        let error = HookDispatcher::new(registry)
            .with_budget(Duration::from_millis(50))
            .pre_send(&ctx("t1"), &mut MessageDraft::new(vec![]))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
        assert_eq!(error.reason(), "pre-send hook budget exhausted");
        assert_eq!(started.elapsed(), Duration::from_millis(50));
        assert!(log.lock().unwrap().is_empty(), "the second step never ran");

        hanging(optional("only").with_timeout(Duration::from_secs(60)))
            .with_budget(Duration::from_millis(5))
            .pre_send(&ctx("t1"), &mut MessageDraft::new(vec![]))
            .await
            .unwrap();
    }

    #[derive(Default)]
    struct Counting {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl PreSendHook for Counting {
        async fn handle(&self, _ctx: &Ctx, draft: &mut MessageDraft) -> PreSendDecision {
            self.calls.fetch_add(1, Ordering::Relaxed);
            down(draft)
        }
    }

    fn guarded(
        metadata: HookMetadata,
        hook: Arc<Counting>,
        observer: Arc<RecordingObserver>,
    ) -> HookDispatcher {
        let registry = HookRegistry::new();
        registry.register_pre_send(metadata, HookSelector::default(), hook);
        HookDispatcher::new(registry).with_guard(Arc::new(HookExecutionGuard::new(
            HookCircuitPolicy {
                enabled: true,
                failure_threshold: 2,
                failure_window: Duration::from_secs(30),
                open_for: Duration::from_secs(600),
            },
            observer,
        )))
    }

    /// 连续失败打开熔断后不再调用目标：必需的回 503；每次调用都交出记录，只带错误码。别的应用不受
    /// 影响。
    #[tokio::test]
    async fn an_open_circuit_is_a_503_per_application() {
        let observer = Arc::new(RecordingObserver::recording_everything());
        let hook = Arc::new(Counting::default());
        let dispatcher = guarded(required("dlp"), Arc::clone(&hook), Arc::clone(&observer));

        for _ in 0..2 {
            let error = dispatcher
                .pre_send(&ctx("t1"), &mut MessageDraft::new(vec![]))
                .await
                .unwrap_err();
            assert_eq!(error.reason(), "pre-send hook unavailable");
        }
        let error = dispatcher
            .pre_send(&ctx("t1"), &mut MessageDraft::new(vec![]))
            .await
            .unwrap_err();
        assert_eq!(error.reason(), "pre-send hook circuit open");
        assert_eq!(hook.calls.load(Ordering::Relaxed), 2);

        let outcomes: Vec<_> = observer
            .records()
            .iter()
            .map(|record| record.outcome.clone())
            .collect();
        assert_eq!(
            outcomes,
            vec![
                HookExecutionOutcome::Failed {
                    code: "SERVICE_UNAVAILABLE"
                },
                HookExecutionOutcome::Failed {
                    code: "SERVICE_UNAVAILABLE"
                },
                HookExecutionOutcome::CircuitOpen,
            ]
        );
        assert!(
            observer
                .records()
                .iter()
                .all(|record| record.hook_id() == "pre_send:dlp" && record.tenant_id == "t1")
        );

        assert!(
            dispatcher
                .pre_send(&ctx("t2"), &mut MessageDraft::new(vec![]))
                .await
                .is_err()
        );
        assert_eq!(hook.calls.load(Ordering::Relaxed), 3, "t2 still calls");
    }

    /// 删掉后同名重建的 Hook 是另一条：旧 Hook 打开的熔断不拦它，执行记录记各自的行号（V4 全栈：
    /// 重建的 Hook 继承了旧 Hook 的统计）。
    #[tokio::test]
    async fn a_recreated_hook_starts_with_a_fresh_circuit_and_its_own_records() {
        let observer = Arc::new(RecordingObserver::recording_everything());
        let hook = Arc::new(Counting::default());
        let dispatcher = guarded(required("unused"), Arc::clone(&hook), Arc::clone(&observer));
        let stored = |config_id: i64| {
            let handler: Arc<dyn PreSendHook> = Arc::clone(&hook) as Arc<Counting>;
            HashMap::from([(
                HookScope::Platform,
                vec![crate::hooks::registry::PreSendPlan::new(
                    required("dlp")
                        .with_source(HookSource::Platform)
                        .with_config_id(Some(config_id)),
                    HookSelector::default(),
                    handler,
                )],
            )])
        };
        async fn send(dispatcher: &HookDispatcher) -> Result<()> {
            dispatcher
                .pre_send(&ctx("t1"), &mut MessageDraft::new(vec![]))
                .await
        }

        dispatcher.registry().replace_stored(stored(41));
        let calls_before = hook.calls.load(Ordering::Relaxed);
        for _ in 0..2 {
            assert_eq!(
                send(&dispatcher).await.unwrap_err().reason(),
                "pre-send hook unavailable"
            );
        }
        assert_eq!(
            send(&dispatcher).await.unwrap_err().reason(),
            "pre-send hook circuit open"
        );

        dispatcher.registry().replace_stored(stored(42));
        assert_eq!(
            send(&dispatcher).await.unwrap_err().reason(),
            "pre-send hook unavailable",
            "the re-created hook is called, not short-circuited"
        );
        assert_eq!(hook.calls.load(Ordering::Relaxed) - calls_before, 3);

        let ids: Vec<_> = observer
            .records()
            .iter()
            .filter(|record| record.hook_name == "dlp")
            .map(|record| (record.hook_id(), record.outcome.clone()))
            .collect();
        let failed = HookExecutionOutcome::Failed {
            code: "SERVICE_UNAVAILABLE",
        };
        assert_eq!(
            ids,
            vec![
                ("41".to_string(), failed.clone()),
                ("41".to_string(), failed.clone()),
                ("41".to_string(), HookExecutionOutcome::CircuitOpen),
                ("42".to_string(), failed),
            ]
        );
    }

    /// 可选的 Hook 熔断打开后跳过，不调用。
    #[tokio::test]
    async fn an_open_circuit_skips_an_optional_hook() {
        let hook = Arc::new(Counting::default());
        let dispatcher = guarded(
            optional("tagger"),
            Arc::clone(&hook),
            Arc::new(RecordingObserver::default()),
        );
        for _ in 0..3 {
            dispatcher
                .pre_send(&ctx("t1"), &mut MessageDraft::new(vec![]))
                .await
                .unwrap();
        }
        assert_eq!(hook.calls.load(Ordering::Relaxed), 2);
    }

    /// 总时限耗尽不是这条 Hook 的错：不计入熔断。
    #[tokio::test(start_paused = true)]
    async fn budget_exhaustion_does_not_trip_the_breaker() {
        let observer = Arc::new(RecordingObserver::recording_everything());
        let registry = HookRegistry::new();
        registry.register_pre_send(
            required("slow").with_timeout(Duration::from_secs(60)),
            HookSelector::default(),
            Arc::new(Hanging),
        );
        let guard = Arc::new(HookExecutionGuard::new(
            HookCircuitPolicy {
                enabled: true,
                failure_threshold: 1,
                failure_window: Duration::from_secs(30),
                open_for: Duration::from_secs(600),
            },
            observer.clone(),
        ));
        let dispatcher = HookDispatcher::new(registry)
            .with_guard(Arc::clone(&guard))
            .with_budget(Duration::from_millis(5));
        for _ in 0..2 {
            let error = dispatcher
                .pre_send(&ctx("t1"), &mut MessageDraft::new(vec![]))
                .await
                .unwrap_err();
            assert_eq!(error.reason(), "pre-send hook budget exhausted");
        }
        assert_eq!(guard.circuits().tracked(), 0);
        assert_eq!(
            observer.records()[0].outcome,
            HookExecutionOutcome::BudgetExhausted
        );
    }

    /// 没有 Hook 的应用不打开总时限，也不碰草稿。
    #[tokio::test]
    async fn an_empty_chain_is_a_no_op() {
        let mut draft = MessageDraft::new(vec![1, 2, 3]);
        HookDispatcher::new(HookRegistry::new())
            .pre_send(&ctx("t1"), &mut draft)
            .await
            .unwrap();
        assert_eq!(draft.payload, vec![1, 2, 3]);
    }
}
