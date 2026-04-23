use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use flare_im_core::Ctx;
use flare_proto::common::event::Payload;
use flare_proto::common::{Event, Message};

use crate::application::extension::{ExtensionFailureMode, ExtensionPolicy, ExtensionRouting};
use crate::application::handlers::plugin::CallCapabilityBridge;
use crate::domain::model::MessageSubmission;
use crate::domain::service::{HookExecutionContext, HookExecutionService};
use crate::error::{ErrorCode, Result};
use flare_server_core::flare_err;

/// 与 `flare-sdk-plugin-call::rtc::RTC_EXT_ENRICH_*` 对齐（编排器不依赖该 crate，避免环依赖）。
const CALL_SIGNAL_EXT_RTC_ENRICH: &str = "flare_rtc_enrich";
const CALL_SIGNAL_EXT_RTC_ENRICH_ERR: &str = "flare_rtc_enrich_error";
const EXTENSION_LOG_SCHEMA: &str = "extension.v1";

/// 扩展编排器：收口 Hook/Plugin 扩展执行，统一失败策略与降级标记。
#[derive(Clone)]
pub struct ExtensionOrchestrator {
    hook_execution_service: Arc<HookExecutionService>,
    call_capability_bridge: Option<Arc<CallCapabilityBridge>>,
    policy: ExtensionPolicy,
    routing: ExtensionRouting,
}

impl ExtensionOrchestrator {
    pub fn new(
        hook_execution_service: Arc<HookExecutionService>,
        call_capability_bridge: Option<Arc<CallCapabilityBridge>>,
        policy: ExtensionPolicy,
        routing: ExtensionRouting,
    ) -> Self {
        Self {
            hook_execution_service,
            call_capability_bridge,
            policy,
            routing,
        }
    }

    pub async fn execute_pre_send(
        &self,
        ctx: &Ctx,
        message: Message,
        execute_pre_send: bool,
    ) -> Result<HookExecutionContext> {
        if !self.routing.allows_hook_for_message(ctx, &message) {
            self.trace_extension_skip(
                ctx,
                "hook",
                "pre_send",
                "route_filtered",
                Some(message.message_type),
                None,
            );
            return Ok(HookExecutionContext {
                message,
                hook_context: ctx.clone(),
            });
        }
        let started_at = Instant::now();
        let timeout = Duration::from_millis(self.policy.pre_send.timeout_ms.max(1));
        let attempts = self.policy.pre_send.attempts();
        for attempt in 1..=attempts {
            let message_for_attempt = message.clone();
            match tokio::time::timeout(
                timeout,
                self.hook_execution_service.execute_pre_send(
                    ctx,
                    message_for_attempt,
                    execute_pre_send,
                ),
            )
            .await
            {
                Ok(Ok(out)) => {
                    self.trace_extension_exec(
                        ctx,
                        "hook",
                        "pre_send",
                        "ok",
                        started_at,
                        Some(out.message.message_type),
                        None,
                    );
                    return Ok(out);
                }
                Ok(Err(e)) => {
                    if attempt < attempts {
                        self.trace_extension_retry(
                            ctx, "hook", "pre_send", attempt, attempts, "error",
                        );
                        continue;
                    }
                    self.trace_extension_exec(
                        ctx,
                        "hook",
                        "pre_send",
                        "error",
                        started_at,
                        Some(message.message_type),
                        None,
                    );
                    return Err(e);
                }
                Err(_) => {
                    if attempt < attempts {
                        self.trace_extension_retry(
                            ctx, "hook", "pre_send", attempt, attempts, "timeout",
                        );
                        continue;
                    }
                    self.trace_extension_exec(
                        ctx,
                        "hook",
                        "pre_send",
                        "timeout",
                        started_at,
                        Some(message.message_type),
                        None,
                    );
                    return Err(flare_err!(
                        ErrorCode::OperationFailed,
                        "pre_send hook timeout"
                    ));
                }
            }
        }
        unreachable!("pre_send attempts loop should always return");
    }

    pub async fn execute_post_send(
        &self,
        ctx: &Ctx,
        submission: &MessageSubmission,
        hook_context: &Ctx,
    ) -> Result<()> {
        if !self
            .routing
            .allows_hook_for_message_type(ctx, submission.message.message_type)
        {
            self.trace_extension_skip(
                ctx,
                "hook",
                "post_send",
                "route_filtered",
                Some(submission.message.message_type),
                None,
            );
            return Ok(());
        }
        let started_at = Instant::now();
        let timeout = Duration::from_millis(self.policy.post_send.timeout_ms.max(1));
        let attempts = self.policy.post_send.attempts();
        let mut post_send_result = Ok(());
        for attempt in 1..=attempts {
            let (result, is_timeout) = match tokio::time::timeout(
                timeout,
                self.hook_execution_service
                    .execute_post_send(hook_context, submission),
            )
            .await
            {
                Ok(inner) => (inner, false),
                Err(_) => (
                    Err(flare_err!(
                        ErrorCode::OperationFailed,
                        "post_send hook timeout"
                    )),
                    true,
                ),
            };
            post_send_result = result;
            if post_send_result.is_ok() {
                break;
            }
            if attempt < attempts {
                let reason = if is_timeout { "timeout" } else { "error" };
                self.trace_extension_retry(ctx, "hook", "post_send", attempt, attempts, reason);
            }
        }

        if let Err(e) = post_send_result {
            self.trace_extension_exec(
                ctx,
                "hook",
                "post_send",
                "error",
                started_at,
                Some(submission.message.message_type),
                None,
            );
            return match self.policy.post_send_hook_failure_mode {
                ExtensionFailureMode::FailOpen => {
                    tracing::warn!(
                        trace_id = %ctx.trace_id(),
                        request_id = %ctx.request_id(),
                        message_id = %submission.message_id,
                        error = %e,
                        "post_send hook failed, fail-open degrade"
                    );
                    self.trace_extension_exec(
                        ctx,
                        "hook",
                        "post_send",
                        "degraded",
                        started_at,
                        Some(submission.message.message_type),
                        None,
                    );
                    Ok(())
                }
                ExtensionFailureMode::FailClosed => Err(e),
            };
        }
        self.trace_extension_exec(
            ctx,
            "hook",
            "post_send",
            "ok",
            started_at,
            Some(submission.message.message_type),
            None,
        );

        Ok(())
    }

    pub async fn enrich_event_before_persist(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        event: &mut Event,
    ) -> Result<()> {
        let Some(ref bridge) = self.call_capability_bridge else {
            self.trace_extension_skip(
                ctx,
                "plugin",
                "event_enrich",
                "bridge_disabled",
                None,
                Some(event.r#type),
            );
            return Ok(());
        };
        if !self.routing.allows_plugin_for_event(ctx, event) {
            self.trace_extension_skip(
                ctx,
                "plugin",
                "event_enrich",
                "route_filtered",
                None,
                Some(event.r#type),
            );
            return Ok(());
        }
        let started_at = Instant::now();
        let timeout = Duration::from_millis(self.policy.event_enrich.timeout_ms.max(1));
        let attempts = self.policy.event_enrich.attempts();
        let mut enrich_result = Ok(());
        for attempt in 1..=attempts {
            let (result, is_timeout) = match tokio::time::timeout(
                timeout,
                bridge.enrich_call_signal_event(ctx, tenant_id, event),
            )
            .await
            {
                Ok(inner) => (inner, false),
                Err(_) => (
                    Err(flare_err!(
                        ErrorCode::OperationFailed,
                        "event enrich timeout"
                    )),
                    true,
                ),
            };
            enrich_result = result;
            if enrich_result.is_ok() {
                break;
            }
            if attempt < attempts {
                let reason = if is_timeout { "timeout" } else { "error" };
                self.trace_extension_retry(
                    ctx,
                    "plugin",
                    "event_enrich",
                    attempt,
                    attempts,
                    reason,
                );
            }
        }

        if let Err(e) = enrich_result {
            self.trace_extension_exec(
                ctx,
                "plugin",
                "event_enrich",
                "error",
                started_at,
                None,
                Some(event.r#type),
            );
            return match self.policy.call_signal_enrich_failure_mode {
                ExtensionFailureMode::FailOpen => {
                    let detail = format!("{e:#}");
                    let detail: String = detail.chars().take(240).collect();
                    tracing::warn!(
                        error = %e,
                        trace_id = %ctx.trace_id(),
                        request_id = %ctx.request_id(),
                        event_id = %event.event_id,
                        conversation_id = %event.conversation_id,
                        "call capability enrich failed, fail-open degrade"
                    );
                    if let Some(Payload::CallSignal(cs)) = event.payload.as_mut() {
                        cs.ext
                            .insert(CALL_SIGNAL_EXT_RTC_ENRICH.into(), "degraded".into());
                        if !detail.trim().is_empty() {
                            cs.ext.insert(CALL_SIGNAL_EXT_RTC_ENRICH_ERR.into(), detail);
                        }
                    }
                    self.trace_extension_exec(
                        ctx,
                        "plugin",
                        "event_enrich",
                        "degraded",
                        started_at,
                        None,
                        Some(event.r#type),
                    );
                    Ok(())
                }
                ExtensionFailureMode::FailClosed => Err(e),
            };
        }
        self.trace_extension_exec(
            ctx,
            "plugin",
            "event_enrich",
            "ok",
            started_at,
            None,
            Some(event.r#type),
        );

        Ok(())
    }

    fn trace_extension_exec(
        &self,
        ctx: &Ctx,
        extension_type: &str,
        phase: &str,
        result: &str,
        started_at: Instant,
        message_type: Option<i32>,
        event_type: Option<i32>,
    ) {
        tracing::debug!(
            event = "extension_exec",
            schema = EXTENSION_LOG_SCHEMA,
            component = "extension_orchestrator",
            extension_type,
            phase,
            result,
            elapsed_ms = started_at.elapsed().as_millis(),
            trace_id = %ctx.trace_id(),
            request_id = %ctx.request_id(),
            tenant_id = %ctx.tenant_id().unwrap_or("0"),
            message_type = ?message_type,
            event_type = ?event_type,
            "extension execution completed"
        );
    }

    fn trace_extension_skip(
        &self,
        ctx: &Ctx,
        extension_type: &str,
        phase: &str,
        reason: &str,
        message_type: Option<i32>,
        event_type: Option<i32>,
    ) {
        tracing::debug!(
            event = "extension_skip",
            schema = EXTENSION_LOG_SCHEMA,
            component = "extension_orchestrator",
            extension_type,
            phase,
            reason,
            trace_id = %ctx.trace_id(),
            request_id = %ctx.request_id(),
            tenant_id = %ctx.tenant_id().unwrap_or("0"),
            message_type = ?message_type,
            event_type = ?event_type,
            "extension execution skipped"
        );
    }

    fn trace_extension_retry(
        &self,
        ctx: &Ctx,
        extension_type: &str,
        phase: &str,
        attempt: u32,
        max_attempts: u32,
        reason: &str,
    ) {
        tracing::debug!(
            event = "extension_retry",
            schema = EXTENSION_LOG_SCHEMA,
            component = "extension_orchestrator",
            extension_type,
            phase,
            attempt,
            max_attempts,
            reason,
            trace_id = %ctx.trace_id(),
            request_id = %ctx.request_id(),
            tenant_id = %ctx.tenant_id().unwrap_or("0"),
            "extension execution will retry"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use flare_im_core::Ctx;
    use flare_im_core::hooks::{HookDispatcher, HookRegistry};
    use flare_proto::common::call_signal_event::Signal;
    use flare_proto::common::event::Payload;
    use flare_proto::common::{CallInvite, CallSignalEvent, Event, EventType, Message};
    use flare_server_core::Context;
    use serde_json::{Value, json};

    use crate::application::extension::{ExtensionPolicy, ExtensionRouting};
    use crate::application::handlers::plugin::CallCapabilityBridge;
    use crate::domain::repository::CapabilityDispatchGateway;
    use crate::domain::service::HookExecutionService;
    use crate::error::{ErrorCode, Result};
    use flare_server_core::flare_err;
    use tokio::time::{Duration, sleep};

    use super::ExtensionOrchestrator;

    fn test_ctx(tenant_id: &str) -> Ctx {
        Arc::new(
            Context::with_request_id("trace-extension-test")
                .with_user_id("user-extension-test")
                .with_tenant_id(tenant_id),
        )
    }

    fn test_hook_service() -> Arc<HookExecutionService> {
        let registry = HookRegistry::new();
        let dispatcher = Arc::new(HookDispatcher::new(registry));
        Arc::new(HookExecutionService::new(dispatcher, None))
    }

    struct CountingGateway {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl CapabilityDispatchGateway for CountingGateway {
        async fn dispatch_json(
            &self,
            _ctx: &Ctx,
            _capability_id: &str,
            _tenant_id: &str,
            _user_id: &str,
            _conversation_id: &str,
            _request_id: String,
            _payload: Value,
        ) -> Result<Value> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(json!({ "call_id": "cid-1" }))
        }
    }

    #[tokio::test]
    async fn execute_pre_send_skips_when_routing_filtered() {
        let orchestrator = ExtensionOrchestrator::new(
            test_hook_service(),
            None,
            ExtensionPolicy::new(true, false),
            ExtensionRouting::new(vec![], vec![999], vec![]),
        );
        let ctx = test_ctx("tenant-a");
        let message = Message {
            message_type: 1,
            ..Default::default()
        };

        let out = orchestrator
            .execute_pre_send(&ctx, message.clone(), true)
            .await
            .expect("pre_send route-filtered should succeed");

        assert_eq!(out.message.message_type, message.message_type);
    }

    #[tokio::test]
    async fn enrich_event_skips_bridge_when_routing_filtered() {
        let calls = Arc::new(AtomicUsize::new(0));
        let gateway = Arc::new(CountingGateway {
            calls: Arc::clone(&calls),
        });
        let bridge = Arc::new(CallCapabilityBridge::new(gateway));
        let orchestrator = ExtensionOrchestrator::new(
            test_hook_service(),
            Some(bridge),
            ExtensionPolicy::new(true, false),
            ExtensionRouting::new(vec![], vec![], vec![99999]),
        );
        let ctx = test_ctx("tenant-a");
        let mut event = Event {
            r#type: EventType::EventCallSignal as i32,
            conversation_id: "conv-1".to_string(),
            payload: Some(Payload::CallSignal(CallSignalEvent {
                from_user_id: "u1".to_string(),
                signal: Some(Signal::Invite(CallInvite::default())),
                ..Default::default()
            })),
            ..Default::default()
        };

        orchestrator
            .enrich_event_before_persist(&ctx, "tenant-a", &mut event)
            .await
            .expect("event enrich route-filtered should succeed");

        assert_eq!(
            calls.load(Ordering::Relaxed),
            0,
            "route-filtered event should not call capability gateway"
        );
    }

    #[tokio::test]
    async fn enrich_event_fail_open_degrades_instead_of_failing() {
        struct FailingGateway;
        #[async_trait]
        impl CapabilityDispatchGateway for FailingGateway {
            async fn dispatch_json(
                &self,
                _ctx: &Ctx,
                _capability_id: &str,
                _tenant_id: &str,
                _user_id: &str,
                _conversation_id: &str,
                _request_id: String,
                _payload: Value,
            ) -> Result<Value> {
                Err(flare_err!(
                    ErrorCode::InternalError,
                    "forced capability failure"
                ))
            }
        }

        let bridge = Arc::new(CallCapabilityBridge::new(Arc::new(FailingGateway)));
        let orchestrator = ExtensionOrchestrator::new(
            test_hook_service(),
            Some(bridge),
            ExtensionPolicy::new(true, false),
            ExtensionRouting::default(),
        );
        let ctx = test_ctx("tenant-a");
        let mut event = Event {
            r#type: EventType::EventCallSignal as i32,
            conversation_id: "conv-1".to_string(),
            payload: Some(Payload::CallSignal(CallSignalEvent {
                from_user_id: "u1".to_string(),
                signal: Some(Signal::Invite(CallInvite::default())),
                ..Default::default()
            })),
            ..Default::default()
        };

        orchestrator
            .enrich_event_before_persist(&ctx, "tenant-a", &mut event)
            .await
            .expect("fail-open should degrade instead of returning error");
    }

    #[tokio::test]
    async fn enrich_event_retries_and_recovers() {
        struct FlakyGateway {
            calls: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl CapabilityDispatchGateway for FlakyGateway {
            async fn dispatch_json(
                &self,
                _ctx: &Ctx,
                _capability_id: &str,
                _tenant_id: &str,
                _user_id: &str,
                _conversation_id: &str,
                _request_id: String,
                _payload: Value,
            ) -> Result<Value> {
                let n = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
                if n == 1 {
                    return Err(flare_err!(
                        ErrorCode::InternalError,
                        "first attempt failure"
                    ));
                }
                Ok(json!({ "call_id": "cid-after-retry" }))
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let bridge = Arc::new(CallCapabilityBridge::new(Arc::new(FlakyGateway {
            calls: Arc::clone(&calls),
        })));
        let policy = ExtensionPolicy::new(true, false).with_runtime(
            crate::application::extension::ExtensionRuntimePolicy::new(1000, 0),
            crate::application::extension::ExtensionRuntimePolicy::new(1000, 0),
            crate::application::extension::ExtensionRuntimePolicy::new(1000, 1),
        );
        let orchestrator = ExtensionOrchestrator::new(
            test_hook_service(),
            Some(bridge),
            policy,
            ExtensionRouting::default(),
        );
        let ctx = test_ctx("tenant-a");
        let mut event = Event {
            r#type: EventType::EventCallSignal as i32,
            conversation_id: "conv-1".to_string(),
            payload: Some(Payload::CallSignal(CallSignalEvent {
                from_user_id: "u1".to_string(),
                signal: Some(Signal::Invite(CallInvite::default())),
                ..Default::default()
            })),
            ..Default::default()
        };

        orchestrator
            .enrich_event_before_persist(&ctx, "tenant-a", &mut event)
            .await
            .expect("retry policy should recover on second attempt");

        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn enrich_event_fail_closed_returns_error() {
        struct AlwaysFailGateway;
        #[async_trait]
        impl CapabilityDispatchGateway for AlwaysFailGateway {
            async fn dispatch_json(
                &self,
                _ctx: &Ctx,
                _capability_id: &str,
                _tenant_id: &str,
                _user_id: &str,
                _conversation_id: &str,
                _request_id: String,
                _payload: Value,
            ) -> Result<Value> {
                Err(flare_err!(
                    ErrorCode::InternalError,
                    "always fail for fail-closed test"
                ))
            }
        }

        let bridge = Arc::new(CallCapabilityBridge::new(Arc::new(AlwaysFailGateway)));
        let orchestrator = ExtensionOrchestrator::new(
            test_hook_service(),
            Some(bridge),
            ExtensionPolicy::new(false, false),
            ExtensionRouting::default(),
        );
        let ctx = test_ctx("tenant-a");
        let mut event = Event {
            r#type: EventType::EventCallSignal as i32,
            conversation_id: "conv-1".to_string(),
            payload: Some(Payload::CallSignal(CallSignalEvent {
                from_user_id: "u1".to_string(),
                signal: Some(Signal::Invite(CallInvite::default())),
                ..Default::default()
            })),
            ..Default::default()
        };

        let err = orchestrator
            .enrich_event_before_persist(&ctx, "tenant-a", &mut event)
            .await
            .expect_err("fail-closed should return error");
        assert!(!err.to_string().is_empty());
    }

    #[tokio::test]
    async fn enrich_event_timeout_fail_closed_returns_error() {
        struct SlowGateway;
        #[async_trait]
        impl CapabilityDispatchGateway for SlowGateway {
            async fn dispatch_json(
                &self,
                _ctx: &Ctx,
                _capability_id: &str,
                _tenant_id: &str,
                _user_id: &str,
                _conversation_id: &str,
                _request_id: String,
                _payload: Value,
            ) -> Result<Value> {
                sleep(Duration::from_millis(30)).await;
                Ok(json!({ "call_id": "late-call-id" }))
            }
        }

        let bridge = Arc::new(CallCapabilityBridge::new(Arc::new(SlowGateway)));
        let policy = ExtensionPolicy::new(false, false).with_runtime(
            crate::application::extension::ExtensionRuntimePolicy::new(1000, 0),
            crate::application::extension::ExtensionRuntimePolicy::new(1000, 0),
            crate::application::extension::ExtensionRuntimePolicy::new(1, 0),
        );
        let orchestrator = ExtensionOrchestrator::new(
            test_hook_service(),
            Some(bridge),
            policy,
            ExtensionRouting::default(),
        );
        let ctx = test_ctx("tenant-a");
        let mut event = Event {
            r#type: EventType::EventCallSignal as i32,
            conversation_id: "conv-1".to_string(),
            payload: Some(Payload::CallSignal(CallSignalEvent {
                from_user_id: "u1".to_string(),
                signal: Some(Signal::Invite(CallInvite::default())),
                ..Default::default()
            })),
            ..Default::default()
        };

        let err = orchestrator
            .enrich_event_before_persist(&ctx, "tenant-a", &mut event)
            .await
            .expect_err("timeout with fail-closed should return error");
        assert!(!err.to_string().is_empty());
    }
}
