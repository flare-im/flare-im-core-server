//! # Hook领域服务
//!
//! 定义Hook引擎的核心领域服务

use flare_server_core::error::Result;
use futures_util::future::join_all;
use std::sync::Arc;

use crate::domain::model::{HookExecutionPlan, LoggableName, is_hook_unavailable};
use flare_im_hooks::{
    DeliveryEvent, HookGroup, MessageDraft, MessageRecord, PreSendDecision, RecallEvent,
};
use flare_server_core::context::{Context, Ctx};

/// Hook分组结果
#[derive(Debug, Default)]
pub struct GroupedHooks {
    /// 校验类Hook组（串行执行，快速失败）
    pub validation: Vec<HookExecutionPlan>,
    /// 关键业务处理Hook组（串行执行，保证顺序）
    pub critical: Vec<HookExecutionPlan>,
    /// 非关键业务处理Hook组（并发执行，容错）
    pub business: Vec<HookExecutionPlan>,
}

/// Hook编排服务
///
/// 分组语义（与 `HookConfigItem.group` 的说明一致）：validation 组先串行，任何拒绝立即返回；
/// critical 组再串行、保证顺序，任何拒绝立即返回，组内 Hook 不可达或超时一律拒绝；business 组
/// 最后执行，拒绝与不可达都只记日志、不中断主流程。
pub struct HookOrchestrationService;

/// business 组的拒绝不中断主流程，只记日志：Hook 不可用（要求成功的 business Hook 连不上或超时）
/// 与 Hook 明确拒绝分开记。
fn log_business_rejection(
    stage: &'static str,
    hook: &HookExecutionPlan,
    decision: &PreSendDecision,
) {
    if let PreSendDecision::Reject { error } = decision {
        if is_hook_unavailable(error) {
            tracing::warn!(hook = %LoggableName(hook.name()), stage, reason = %error, "required business hook unavailable; business group does not block, continuing");
        } else {
            tracing::warn!(hook = %LoggableName(hook.name()), stage, reason = %error, "business hook rejected; business group does not block, continuing");
        }
    }
}

impl HookOrchestrationService {
    /// 分组Hook
    pub fn group_hooks(&self, hooks: Vec<HookExecutionPlan>) -> GroupedHooks {
        let mut validation = Vec::new();
        let mut critical = Vec::new();
        let mut business = Vec::new();

        for hook in hooks {
            let group = hook.group();

            match group {
                HookGroup::Validation => validation.push(hook),
                HookGroup::Critical => critical.push(hook),
                HookGroup::Business => business.push(hook),
            }
        }

        // 按priority排序（priority越小越先执行）
        validation.sort_by_key(|h| h.priority());
        critical.sort_by_key(|h| h.priority());
        business.sort_by_key(|h| h.priority());

        GroupedHooks {
            validation,
            critical,
            business,
        }
    }

    /// 执行PreSend Hook（领域业务逻辑）
    pub async fn execute_pre_send(
        &self,
        ctx: &Context,
        draft: &mut MessageDraft,
        hooks: Vec<HookExecutionPlan>,
    ) -> Result<PreSendDecision> {
        let grouped = self.group_hooks(hooks);
        let ctx_arc: Ctx = Arc::new(ctx.clone());

        // 先执行validation组（串行，快速失败）
        for hook in &grouped.validation {
            let decision = hook.execute(&ctx_arc, draft).await?;
            match decision {
                PreSendDecision::Reject { .. } => return Ok(decision),
                PreSendDecision::Continue => continue,
            }
        }

        // 再执行critical组（串行，保证顺序）
        for hook in &grouped.critical {
            let decision = hook.execute(&ctx_arc, draft).await?;
            match decision {
                PreSendDecision::Reject { .. } => return Ok(decision),
                PreSendDecision::Continue => continue,
            }
        }

        // 最后执行business组（串行执行，因为draft是&mut不能并发）；即使拒绝也不中断主流程
        for hook in &grouped.business {
            let decision = hook.execute(&ctx_arc, draft).await?;
            log_business_rejection("pre-send", hook, &decision);
        }

        Ok(PreSendDecision::Continue)
    }

    /// 执行PostSend Hook（领域业务逻辑）
    pub async fn execute_post_send(
        &self,
        ctx: &Context,
        record: &MessageRecord,
        draft: &MessageDraft,
        hooks: Vec<HookExecutionPlan>,
    ) -> Result<()> {
        let grouped = self.group_hooks(hooks);
        let ctx_arc: Ctx = Arc::new(ctx.clone());

        // 串行执行validation和critical组
        for hook in grouped.validation.iter().chain(grouped.critical.iter()) {
            if let Err(e) = hook.execute_post_send(&ctx_arc, record, draft).await {
                if hook.must_succeed() {
                    return Err(e);
                }
                tracing::warn!(hook = %LoggableName(hook.name()), error = %e, "PostSend hook failed but continuing");
            }
        }

        // 并发执行business组
        let business_futures: Vec<_> = grouped
            .business
            .iter()
            .map(|hook| hook.execute_post_send(&ctx_arc, record, draft))
            .collect();

        let results = join_all(business_futures).await;
        for (hook, result) in grouped.business.iter().zip(results) {
            if let Err(e) = result {
                if hook.require_success() {
                    tracing::warn!(hook = %LoggableName(hook.name()), error = %e, "PostSend hook failed");
                } else {
                    tracing::trace!(hook = %LoggableName(hook.name()), error = %e, "PostSend hook failed but ignored");
                }
            }
        }

        Ok(())
    }

    /// 执行Delivery Hook（领域业务逻辑）
    pub async fn execute_delivery(
        &self,
        ctx: &Context,
        event: &DeliveryEvent,
        hooks: Vec<HookExecutionPlan>,
    ) -> Result<()> {
        let grouped = self.group_hooks(hooks);
        let ctx_arc: Ctx = Arc::new(ctx.clone());

        // 串行执行validation和critical组
        for hook in grouped.validation.iter().chain(grouped.critical.iter()) {
            if let Err(e) = hook.execute_delivery(&ctx_arc, event).await {
                if hook.must_succeed() {
                    return Err(e);
                }
                tracing::warn!(hook = %LoggableName(hook.name()), error = %e, "Delivery hook failed but continuing");
            }
        }

        // 并发执行business组
        let business_futures: Vec<_> = grouped
            .business
            .iter()
            .map(|hook| hook.execute_delivery(&ctx_arc, event))
            .collect();

        let results = join_all(business_futures).await;
        for (hook, result) in grouped.business.iter().zip(results) {
            if let Err(e) = result {
                if hook.require_success() {
                    tracing::warn!(hook = %LoggableName(hook.name()), error = %e, "Delivery hook failed");
                } else {
                    tracing::trace!(hook = %LoggableName(hook.name()), error = %e, "Delivery hook failed but ignored");
                }
            }
        }

        Ok(())
    }

    /// 执行Recall Hook（领域业务逻辑）
    pub async fn execute_recall(
        &self,
        ctx: &Context,
        event: &RecallEvent,
        hooks: Vec<HookExecutionPlan>,
    ) -> Result<PreSendDecision> {
        let grouped = self.group_hooks(hooks);
        let ctx_arc: Ctx = Arc::new(ctx.clone());

        // 先执行validation组（串行，快速失败）
        for hook in &grouped.validation {
            let decision = hook.execute_recall(&ctx_arc, event).await?;
            match decision {
                PreSendDecision::Reject { .. } => return Ok(decision),
                PreSendDecision::Continue => continue,
            }
        }

        // 再执行critical组（串行，保证顺序）
        for hook in &grouped.critical {
            let decision = hook.execute_recall(&ctx_arc, event).await?;
            match decision {
                PreSendDecision::Reject { .. } => return Ok(decision),
                PreSendDecision::Continue => continue,
            }
        }

        // 最后执行business组（串行执行）；即使拒绝也不中断主流程
        for hook in &grouped.business {
            let decision = hook.execute_recall(&ctx_arc, event).await?;
            log_business_rejection("recall", hook, &decision);
        }

        Ok(PreSendDecision::Continue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_hooks::{HookErrorPolicy, HookKind, HookMetadata};
    use std::sync::Arc;
    use std::time::Duration;

    fn create_test_hook_plan(name: &str, priority: i32, group: HookGroup) -> HookExecutionPlan {
        let metadata = HookMetadata {
            name: Arc::from(name),
            version: None,
            description: None,
            kind: HookKind::PreSend,
            priority,
            timeout: Duration::from_secs(5),
            max_retries: 0,
            error_policy: HookErrorPolicy::FailFast,
            require_success: true,
        };

        // validation 用 >= 100 的 priority（与按 priority 分组一致）；critical 只能由配置的
        // group 指定，这里用 with_group，priority 仍 < 100。
        let metadata = match group {
            HookGroup::Validation => metadata.with_priority(100 + priority),
            HookGroup::Critical | HookGroup::Business => metadata.with_priority(priority),
        };

        HookExecutionPlan::new(metadata).with_group(group)
    }

    #[test]
    fn test_group_hooks() {
        let service = HookOrchestrationService;

        let hooks = vec![
            create_test_hook_plan("validation-hook-1", 100, HookGroup::Validation), // priority = 200
            create_test_hook_plan("validation-hook-2", 50, HookGroup::Validation), // priority = 150
            create_test_hook_plan("critical-hook-1", 30, HookGroup::Critical), // priority = 30，按配置归 critical
            create_test_hook_plan("business-hook-1", 10, HookGroup::Business), // priority = 10
            create_test_hook_plan("business-hook-2", 20, HookGroup::Business), // priority = 20
        ];

        let grouped = service.group_hooks(hooks);

        assert_eq!(grouped.validation.len(), 2, "Validation 组应该有 2 个 hook");
        // 以前 critical 组按 priority 永远分不到，配置的 group 被忽略
        assert_eq!(grouped.critical.len(), 1, "Critical 组按配置的 group 归组");
        assert_eq!(grouped.critical[0].name(), "critical-hook-1");
        assert_eq!(grouped.business.len(), 2, "Business 组应该有 2 个 hook");

        // 验证排序（priority越小越先执行）
        assert_eq!(grouped.validation[0].priority(), 150);
        assert_eq!(grouped.validation[1].priority(), 200);
        assert_eq!(grouped.business[0].priority(), 10);
        assert_eq!(grouped.business[1].priority(), 20);
    }

    /// 记录执行顺序的本地 PreSend Hook。
    struct Recording {
        name: &'static str,
        reject: bool,
        log: Arc<std::sync::Mutex<Vec<&'static str>>>,
    }

    #[async_trait::async_trait]
    impl flare_im_hooks::PreSendHook for Recording {
        async fn handle(
            &self,
            _ctx: &flare_server_core::context::Ctx,
            _draft: &mut MessageDraft,
        ) -> PreSendDecision {
            self.log.lock().unwrap().push(self.name);
            if self.reject {
                PreSendDecision::Reject {
                    error: flare_server_core::error::ErrorBuilder::new(
                        flare_server_core::error::ErrorCode::PermissionDenied,
                        "denied",
                    )
                    .build_error(),
                }
            } else {
                PreSendDecision::Continue
            }
        }
    }

    fn recording(
        name: &'static str,
        priority: i32,
        group: HookGroup,
        reject: bool,
        log: &Arc<std::sync::Mutex<Vec<&'static str>>>,
    ) -> HookExecutionPlan {
        let metadata = HookMetadata {
            name: Arc::from(name),
            version: None,
            description: None,
            kind: HookKind::PreSend,
            priority,
            timeout: Duration::from_secs(5),
            max_retries: 0,
            error_policy: HookErrorPolicy::FailFast,
            require_success: true,
        };
        HookExecutionPlan::new_pre_send(
            metadata,
            Arc::new(Recording {
                name,
                reject,
                log: Arc::clone(log),
            }),
        )
        .with_group(group)
    }

    /// critical 组在 validation 之后、business 之前串行执行；它的拒绝立即返回，business 组不再执行。
    /// business 组的拒绝只记日志。
    #[tokio::test]
    async fn critical_group_runs_between_validation_and_business_and_its_rejection_stops_the_chain()
    {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ctx = Context::with_request_id("req-critical").with_tenant_id("0");
        let mut draft = MessageDraft::new(b"x".to_vec());
        let decision = HookOrchestrationService
            .execute_pre_send(
                &ctx,
                &mut draft,
                vec![
                    recording("business", 1, HookGroup::Business, false, &log),
                    recording("critical", 5, HookGroup::Critical, true, &log),
                    recording("validation", 200, HookGroup::Validation, false, &log),
                ],
            )
            .await
            .unwrap();
        assert!(matches!(decision, PreSendDecision::Reject { .. }));
        assert_eq!(*log.lock().unwrap(), vec!["validation", "critical"]);

        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let decision = HookOrchestrationService
            .execute_pre_send(
                &ctx,
                &mut draft,
                vec![
                    recording("business", 1, HookGroup::Business, true, &log),
                    recording("critical", 5, HookGroup::Critical, false, &log),
                ],
            )
            .await
            .unwrap();
        assert!(matches!(decision, PreSendDecision::Continue));
        assert_eq!(*log.lock().unwrap(), vec!["critical", "business"]);
    }

    #[test]
    fn test_group_hooks_empty() {
        let service = HookOrchestrationService;
        let grouped = service.group_hooks(vec![]);

        assert!(grouped.validation.is_empty());
        assert!(grouped.critical.is_empty());
        assert!(grouped.business.is_empty());
    }

    #[test]
    fn test_group_hooks_single_group() {
        let service = HookOrchestrationService;

        let hooks = vec![
            create_test_hook_plan("hook-1", 10, HookGroup::Business),
            create_test_hook_plan("hook-2", 20, HookGroup::Business),
            create_test_hook_plan("hook-3", 30, HookGroup::Business),
        ];

        let grouped = service.group_hooks(hooks);

        assert_eq!(grouped.validation.len(), 0);
        assert_eq!(grouped.critical.len(), 0);
        assert_eq!(grouped.business.len(), 3);
    }
}
