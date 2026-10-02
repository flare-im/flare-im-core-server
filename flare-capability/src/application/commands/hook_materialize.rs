//! **Command**：`HookConfigItem` → [`HookExecutionPlan`]（按需挂载 `HookAdapter`）。

use crate::domain::hook_integration::validate_hook_item_for_materialization;
use crate::domain::model::{HookConfigItem, HookExecutionPlan, HookTransportConfig};
use crate::infrastructure::adapters::HookAdapterFactory;
use flare_server_core::error::Result as FlareResult;

/// 将一条启用中的 Hook 配置物化为可执行计划：Local 仅携带 target；Grpc/Webhook 则注入适配器。
pub async fn materialize_hook_execution_plan(
    factory: &HookAdapterFactory,
    config: HookConfigItem,
    hook_type: &str,
) -> FlareResult<HookExecutionPlan> {
    validate_hook_item_for_materialization(&config)?;

    let mut plan = HookExecutionPlan::from_hook_config(config.clone(), hook_type);

    if config.enabled && !matches!(config.transport, HookTransportConfig::Local { .. }) {
        let adapter = factory.create_adapter(&config.transport).await?;
        plan = plan.with_adapter(adapter);
    }

    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::{HookSelectorConfig, is_hook_unavailable};
    use crate::domain::service::HookOrchestrationService;
    use flare_im_hooks::{MessageDraft, PreSendDecision, RecallEvent};
    use flare_server_core::context::Context;
    use flare_server_core::error::ErrorCode;
    use std::collections::HashMap;

    /// 一个已关闭的本机端口：先绑定拿到端口号再释放，连接会被立即拒绝。
    async fn closed_port_endpoint() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        format!("http://{addr}")
    }

    fn grpc_hook(
        name: &str,
        endpoint: &str,
        priority: i32,
        require_success: bool,
    ) -> HookConfigItem {
        HookConfigItem {
            name: name.to_string(),
            version: None,
            description: None,
            enabled: true,
            priority,
            group: None,
            // 远大于连接被拒的耗时：走的是调用失败分支而不是超时分支。
            timeout_ms: 10_000,
            max_retries: 0,
            error_policy: "fail_fast".to_string(),
            require_success,
            selector: HookSelectorConfig::default(),
            transport: HookTransportConfig::Grpc {
                endpoint: Some(endpoint.to_string()),
                service_name: None,
                registry_type: None,
                namespace: None,
                load_balance: None,
                metadata: HashMap::new(),
            },
            metadata: HashMap::new(),
        }
    }

    async fn pre_send_with(hooks: Vec<HookConfigItem>) -> Result<PreSendDecision, String> {
        let factory = HookAdapterFactory::new();
        let mut plans = Vec::new();
        for hook in hooks {
            plans.push(
                materialize_hook_execution_plan(&factory, hook, "pre_send")
                    .await
                    .expect("materialize"),
            );
        }
        let ctx = Context::with_request_id("req-closed-port-hook").with_tenant_id("0");
        let mut draft = MessageDraft::new(b"hello".to_vec());
        let started = std::time::Instant::now();
        let result = HookOrchestrationService
            .execute_pre_send(&ctx, &mut draft, plans)
            .await
            .map_err(|e| e.to_string());
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "a refused connection must not wait for the hook timeout"
        );
        result
    }

    /// 关闭端口上的非必需 Hook（validation 组，`require_success=false`）不让 PreSend 失败。
    /// W8-C 起 Hook 地址不可达按调用失败处理，修复前 `execute` 原样上抛错误。
    #[tokio::test]
    async fn optional_hook_on_a_closed_port_does_not_fail_pre_send() {
        let endpoint = closed_port_endpoint().await;
        let decision = pre_send_with(vec![grpc_hook("optional-closed", &endpoint, 150, false)])
            .await
            .expect("an unreachable optional hook must not fail PreSend");
        assert!(matches!(decision, PreSendDecision::Continue));
    }

    /// business 组（priority < 100）即使 `require_success=true`，不可达时也不中断主流程。
    #[tokio::test]
    async fn business_group_hook_on_a_closed_port_does_not_fail_pre_send() {
        let endpoint = closed_port_endpoint().await;
        let decision = pre_send_with(vec![grpc_hook("business-closed", &endpoint, 10, true)])
            .await
            .expect("an unreachable business hook must not fail PreSend");
        assert!(matches!(decision, PreSendDecision::Continue));
    }

    /// 必需的 validation 组 Hook 不可达时与超时同样拒绝（不是内部错误），拒绝原因不带下游原文。
    #[tokio::test]
    async fn required_validation_hook_on_a_closed_port_rejects_like_a_timeout() {
        let endpoint = closed_port_endpoint().await;
        let decision = pre_send_with(vec![grpc_hook("required-closed", &endpoint, 150, true)])
            .await
            .expect("an unreachable required hook rejects instead of erroring");
        match decision {
            PreSendDecision::Reject { error } => {
                assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
                assert!(is_hook_unavailable(&error), "{error}");
                let text = error.to_string();
                assert!(
                    !text.contains(endpoint.trim_start_matches("http://")),
                    "rejection must not carry the hook address: {text}"
                );
            }
            other => panic!("expected Reject, got {other:?}"),
        }
    }

    /// 撤回 Hook 同理：可选的不可达时放行。
    #[tokio::test]
    async fn optional_recall_hook_on_a_closed_port_continues() {
        let endpoint = closed_port_endpoint().await;
        let factory = HookAdapterFactory::new();
        let plan = materialize_hook_execution_plan(
            &factory,
            grpc_hook("optional-recall-closed", &endpoint, 150, false),
            "recall",
        )
        .await
        .expect("materialize");
        let ctx = Context::with_request_id("req-closed-port-recall").with_tenant_id("0");
        let event = RecallEvent {
            message_id: "m-1".to_string(),
            conversation_id: None,
            operator_id: "u-1".to_string(),
            recalled_at: std::time::SystemTime::now(),
            metadata: HashMap::new(),
        };
        let decision = HookOrchestrationService
            .execute_recall(&ctx, &event, vec![plan])
            .await
            .expect("an unreachable optional recall hook must not fail");
        assert!(matches!(decision, PreSendDecision::Continue));
    }
}

#[cfg(test)]
mod group_tests {
    //! 配置的 `group` 参与分组：critical 组 Hook 不可达一律拒绝，business 组不中断主流程。
    //! 修复前 `group` 被忽略、只按 priority 分组，critical 组永远为空。
    use super::*;
    use crate::domain::model::{HookSelectorConfig, is_hook_unavailable};
    use crate::domain::service::HookOrchestrationService;
    use flare_im_hooks::{MessageDraft, PreSendDecision};
    use flare_server_core::context::Context;
    use flare_server_core::error::ErrorCode;
    use std::collections::HashMap;

    async fn closed_port_endpoint() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        format!("http://{addr}")
    }

    fn hook(
        name: &str,
        endpoint: &str,
        priority: i32,
        group: Option<&str>,
        require_success: bool,
    ) -> HookConfigItem {
        HookConfigItem {
            name: name.to_string(),
            version: None,
            description: None,
            enabled: true,
            priority,
            group: group.map(str::to_string),
            timeout_ms: 10_000,
            max_retries: 0,
            error_policy: "fail_fast".to_string(),
            require_success,
            selector: HookSelectorConfig::default(),
            transport: HookTransportConfig::Grpc {
                endpoint: Some(endpoint.to_string()),
                service_name: None,
                registry_type: None,
                namespace: None,
                load_balance: None,
                metadata: HashMap::new(),
            },
            metadata: HashMap::new(),
        }
    }

    async fn pre_send(hooks: Vec<HookConfigItem>) -> PreSendDecision {
        let factory = HookAdapterFactory::new();
        let mut plans = Vec::new();
        for hook in hooks {
            plans.push(
                materialize_hook_execution_plan(&factory, hook, "pre_send")
                    .await
                    .expect("materialize"),
            );
        }
        let ctx = Context::with_request_id("req-hook-group").with_tenant_id("0");
        let mut draft = MessageDraft::new(b"hello".to_vec());
        HookOrchestrationService
            .execute_pre_send(&ctx, &mut draft, plans)
            .await
            .expect("hook failures become decisions, not errors")
    }

    /// critical 组（配置指定，priority < 100）的 Hook 不可达时拒绝，即使 `require_success=false`。
    #[tokio::test]
    async fn an_unreachable_critical_hook_rejects_even_when_not_required() {
        let endpoint = closed_port_endpoint().await;
        let decision = pre_send(vec![hook(
            "critical-closed",
            &endpoint,
            10,
            Some("critical"),
            false,
        )])
        .await;
        match decision {
            PreSendDecision::Reject { error } => {
                assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable), "{error}");
                assert!(is_hook_unavailable(&error), "{error}");
                assert!(
                    !error
                        .to_string()
                        .contains(endpoint.trim_start_matches("http://")),
                    "{error}"
                );
            }
            PreSendDecision::Continue => panic!("an unreachable critical hook must reject"),
        }
    }

    /// 配置的 business 组覆盖按 priority 的归组（priority 150 本应是 validation）：必需的 business
    /// Hook 不可达只记日志，不中断主流程。
    #[tokio::test]
    async fn an_explicit_business_group_overrides_the_priority_group() {
        let endpoint = closed_port_endpoint().await;
        let decision = pre_send(vec![hook(
            "business-closed",
            &endpoint,
            150,
            Some("Business"),
            true,
        )])
        .await;
        assert!(matches!(decision, PreSendDecision::Continue), "{decision}");

        // 对照：同样的 Hook 不写 group 时按 priority 归 validation 组，不可达即拒绝。
        let decision = pre_send(vec![hook("validation-closed", &endpoint, 150, None, true)]).await;
        assert!(
            matches!(decision, PreSendDecision::Reject { .. }),
            "{decision}"
        );
    }

    /// 未知的 group 在物化时报参数错误，不静默按 priority 归组。
    #[tokio::test]
    async fn an_unknown_group_is_refused() {
        let endpoint = closed_port_endpoint().await;
        let factory = HookAdapterFactory::new();
        let error = materialize_hook_execution_plan(
            &factory,
            hook("typo", &endpoint, 10, Some("critcal"), true),
            "pre_send",
        )
        .await
        .expect_err("an unknown group must be refused");
        assert_eq!(error.code(), Some(ErrorCode::InvalidParameter), "{error}");
    }
}
