//! **Command**：`HookConfigItem` → [`HookExecutionPlan`]（按需挂载 `HookAdapter`）。

use crate::domain::hook_integration::validate_hook_item_for_materialization;
use crate::domain::model::{HookConfigItem, HookExecutionPlan, HookTransportConfig};
use crate::error::Result as FlareResult;
use crate::infrastructure::adapters::HookAdapterFactory;

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
