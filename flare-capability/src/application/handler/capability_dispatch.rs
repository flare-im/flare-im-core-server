//! 能力 **Dispatch** 应用编排：从扩展注册表解析 RTC 后端，委托领域 [`crate::domain::capability::execute_capability_dispatch`]。

use std::sync::Arc;

use flare_core_base::context::Ctx;

use crate::domain::capability::{
    CapabilityDispatchCommand, CapabilityDispatchResult, CapabilityPolicyBackend, Result,
};
use crate::infrastructure::capability::CapabilityExtensionRegistry;

/// `CapabilityService.Dispatch` 的应用入口：装配端口后调用领域服务。
pub async fn dispatch_capability_command(
    ctx: &Ctx,
    registry: &CapabilityExtensionRegistry,
    policy: &Arc<dyn CapabilityPolicyBackend>,
    req: &CapabilityDispatchCommand,
) -> Result<CapabilityDispatchResult> {
    let rtc = registry.rtc_router().await;
    crate::domain::capability::execute_capability_dispatch(ctx, &rtc, policy.as_ref(), req).await
}
