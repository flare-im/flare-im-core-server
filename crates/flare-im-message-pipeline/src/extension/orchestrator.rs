use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use flare_im_contracts::Ctx;
use flare_proto::common::Message;

use super::{ExtensionFailureMode, ExtensionPolicy, ExtensionRouting};
use crate::{HookExecutionContext, HookExecutionService, SubmittedMessage};
use flare_server_core::error::{ErrorCode, Result};
use flare_server_core::flare_err;

const EXTENSION_LOG_SCHEMA: &str = "extension.v1";

/// 扩展编排器：收口 Hook/Plugin 扩展执行，统一失败策略与降级标记。
#[derive(Clone)]
pub struct ExtensionOrchestrator {
    hook_execution_service: Arc<HookExecutionService>,
    policy: ExtensionPolicy,
    routing: ExtensionRouting,
}

impl ExtensionOrchestrator {
    pub fn new(
        hook_execution_service: Arc<HookExecutionService>,
        policy: ExtensionPolicy,
        routing: ExtensionRouting,
    ) -> Self {
        Self {
            hook_execution_service,
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

    pub async fn execute_post_send<S>(
        &self,
        ctx: &Ctx,
        submission: &S,
        hook_context: &Ctx,
    ) -> Result<()>
    where
        S: SubmittedMessage + ?Sized,
    {
        if !self
            .routing
            .allows_hook_for_message_type(ctx, submission.message().message_type)
        {
            self.trace_extension_skip(
                ctx,
                "hook",
                "post_send",
                "route_filtered",
                Some(submission.message().message_type),
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
                Some(submission.message().message_type),
                None,
            );
            return match self.policy.post_send_hook_failure_mode {
                ExtensionFailureMode::FailOpen => {
                    tracing::warn!(
                        trace_id = %ctx.trace_id(),
                        request_id = %ctx.request_id(),
                        message_id = %submission.message_id(),
                        error = %e,
                        "post_send hook failed, fail-open degrade"
                    );
                    self.trace_extension_exec(
                        ctx,
                        "hook",
                        "post_send",
                        "degraded",
                        started_at,
                        Some(submission.message().message_type),
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
            Some(submission.message().message_type),
            None,
        );

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
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
        tracing::trace!(
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
        tracing::trace!(
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
        tracing::trace!(
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
