//! Domain facade for capability dispatch contracts.

pub use flare_im_capability_core::{CapabilityDispatchCommand, CapabilityDispatchResult};

use super::error::{CapabilityError, Result};

/// 调度缺租户时的拒绝原因，与其余服务缺租户的报错同一说法（`tenant required …`，W12-T）。
pub const DISPATCH_TENANT_REQUIRED: &str = "tenant required to dispatch a capability";

/// 去掉首尾空白后的租户；缺失或只有空白时为 `None`。调度路径上判断租户与取用租户都按它
/// （`require_dispatch_tenant`、RTC 选路上下文与路由器），同一个带空白的取值在授权、选路与
/// 选 RTC 后端时是同一个租户。
pub fn trimmed_dispatch_tenant(tenant_id: Option<&str>) -> Option<&str> {
    tenant_id
        .map(str::trim)
        .filter(|tenant_id| !tenant_id.is_empty())
}

/// 调度命令的租户：缺失或空白即 `PolicyDenied`（gRPC `PERMISSION_DENIED`，与同一命令缺 `user_id`
/// 的拒绝同类），在查路由簿、查授权、选 RTC 后端、调远端插件之前拒绝，不再按租户 "0" 处理
/// （W13-D）。显式的租户 "0" 照常。返回去掉首尾空白后的取值（见 [`trimmed_dispatch_tenant`]），
/// 不做别的归一化；gRPC 入口已要求规范写法并与签名身份比对，带空白的取值只可能来自其他入口。
pub fn require_dispatch_tenant(req: &CapabilityDispatchCommand) -> Result<String> {
    trimmed_dispatch_tenant(req.tenant_id.as_deref())
        .map(str::to_string)
        .ok_or_else(|| CapabilityError::PolicyDenied(DISPATCH_TENANT_REQUIRED.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(tenant_id: Option<&str>) -> CapabilityDispatchCommand {
        CapabilityDispatchCommand {
            capability_id: "vendorx.do".into(),
            tenant_id: tenant_id.map(str::to_string),
            user_id: Some("u1".into()),
            conversation_id: None,
            payload: None,
            request_id: Some("r1".into()),
            message_id: None,
        }
    }

    /// 缺租户（`None`、空串、全空白）以 `tenant required …` 口径拒绝；带首尾空白的租户按去掉
    /// 空白后的取值返回（与 RTC 选路上下文一致），显式的 "0" 照常。
    #[test]
    fn the_dispatch_tenant_is_trimmed_before_it_is_judged_and_used() {
        assert_eq!(
            DISPATCH_TENANT_REQUIRED,
            "tenant required to dispatch a capability"
        );
        for tenant_id in [None, Some(""), Some("  "), Some("\t\n")] {
            let error = require_dispatch_tenant(&command(tenant_id)).unwrap_err();
            assert!(
                matches!(&error, CapabilityError::PolicyDenied(reason) if reason == DISPATCH_TENANT_REQUIRED),
                "{tenant_id:?}: {error:?}"
            );
        }
        for (tenant_id, expected) in [
            ("tenant-a", "tenant-a"),
            (" tenant-a ", "tenant-a"),
            ("\ttenant-a", "tenant-a"),
            ("0", "0"),
        ] {
            assert_eq!(
                require_dispatch_tenant(&command(Some(tenant_id))).unwrap(),
                expected,
                "{tenant_id:?}"
            );
        }
    }
}
