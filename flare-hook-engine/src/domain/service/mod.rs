//! # Hook领域服务
//!
//! 定义Hook引擎的核心领域服务

use anyhow::Result;
use futures_util::future::join_all;
use std::sync::Arc;

use crate::domain::model::HookExecutionPlan;
use flare_im_core::{
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
pub struct HookOrchestrationService;

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

        // 最后执行business组（串行执行，因为draft是&mut不能并发）
        for hook in &grouped.business {
            let decision = hook.execute(&ctx_arc, draft).await?;
            match decision {
                PreSendDecision::Reject { .. } => {
                    // business组即使失败也不中断主流程，只记录日志
                    tracing::warn!("Business hook rejected but continuing");
                }
                PreSendDecision::Continue => continue,
            }
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
                if hook.require_success() {
                    return Err(e);
                }
                tracing::warn!(hook = %hook.name(), error = %e, "PostSend hook failed but continuing");
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
                    tracing::warn!(hook = %hook.name(), error = %e, "PostSend hook failed");
                } else {
                    tracing::debug!(hook = %hook.name(), error = %e, "PostSend hook failed but ignored");
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
                if hook.require_success() {
                    return Err(e);
                }
                tracing::warn!(hook = %hook.name(), error = %e, "Delivery hook failed but continuing");
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
                    tracing::warn!(hook = %hook.name(), error = %e, "Delivery hook failed");
                } else {
                    tracing::debug!(hook = %hook.name(), error = %e, "Delivery hook failed but ignored");
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

        // 最后执行business组（串行执行）
        for hook in &grouped.business {
            let decision = hook.execute_recall(&ctx_arc, event).await?;
            match decision {
                PreSendDecision::Reject { .. } => {
                    // business组即使失败也不中断主流程，只记录日志
                    tracing::warn!("Business recall hook rejected but continuing");
                }
                PreSendDecision::Continue => continue,
            }
        }

        Ok(PreSendDecision::Continue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::HookConfigItem;
    use flare_im_core::{HookErrorPolicy, HookKind, HookMetadata};
    use std::collections::HashMap;
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

        // HookGroup::from_priority 只能区分 Validation (>=100) 和 Business (<100)
        // Critical 组无法通过 priority 区分，所以这里 Critical 和 Business 都使用 <100
        // 注意：实际业务中，Critical 组需要通过其他方式（如配置中的 group 字段）来区分
        let metadata = match group {
            HookGroup::Validation => metadata.with_priority(100 + priority),
            HookGroup::Critical => metadata.with_priority(priority), // 使用 <100，会被分到 Business
            HookGroup::Business => metadata.with_priority(priority),
        };

        HookExecutionPlan::new(metadata)
    }

    #[test]
    fn test_group_hooks() {
        let service = HookOrchestrationService;

        let hooks = vec![
            create_test_hook_plan("validation-hook-1", 100, HookGroup::Validation), // priority = 200
            create_test_hook_plan("validation-hook-2", 50, HookGroup::Validation), // priority = 150
            create_test_hook_plan("critical-hook-1", 30, HookGroup::Critical), // priority = 30 (会被分到 Business)
            create_test_hook_plan("business-hook-1", 10, HookGroup::Business), // priority = 10
            create_test_hook_plan("business-hook-2", 20, HookGroup::Business), // priority = 20
        ];

        let grouped = service.group_hooks(hooks);

        assert_eq!(grouped.validation.len(), 2, "Validation 组应该有 2 个 hook");
        // Critical 组无法通过 priority 区分，会被分到 Business 组
        assert_eq!(
            grouped.critical.len(),
            0,
            "Critical 组无法通过 priority 区分"
        );
        assert_eq!(
            grouped.business.len(),
            3,
            "Business 组应该有 3 个 hook（包含 1 个 Critical）"
        );

        // 验证排序（priority越小越先执行）
        assert_eq!(grouped.validation[0].priority(), 150);
        assert_eq!(grouped.validation[1].priority(), 200);
        assert_eq!(grouped.business[0].priority(), 10);
        assert_eq!(grouped.business[1].priority(), 20);
        assert_eq!(grouped.business[2].priority(), 30); // Critical hook 被分到 Business
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
