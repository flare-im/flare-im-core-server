//! Domain facade for capability dispatch contracts.

pub use flare_im_capability_core::{CapabilityDispatchCommand, CapabilityDispatchResult};

use super::error::{CapabilityError, Result};

/// 调度命令的租户：缺失或空白即 `PolicyDenied`（gRPC `PERMISSION_DENIED`，与同一命令缺 `user_id`
/// 的拒绝同类），在查路由簿、查授权、选 RTC 后端、调远端插件之前拒绝，不再按租户 "0" 处理
/// （W13-D）。显式的租户 "0" 照常；返回原值，不做归一化（gRPC 入口已要求规范写法并与签名身份比对）。
pub fn require_dispatch_tenant(req: &CapabilityDispatchCommand) -> Result<String> {
    req.tenant_id
        .as_deref()
        .filter(|tenant_id| !tenant_id.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| CapabilityError::PolicyDenied("tenant_id required".into()))
}
