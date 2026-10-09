//! 管理员只读：租户能力开关与租户运行时投影。租户由接口层从已验证的 Staff 上下文取得。

use crate::domain::capability::{CapabilityPolicyBackend, Result, TenantCapabilitySwitch};
use crate::domain::model::TenantRuntimeView;
use crate::domain::repository::TenantRuntimeReader;

pub async fn tenant_capability_switches(
    policy: &dyn CapabilityPolicyBackend,
    tenant_id: &str,
) -> Result<Vec<TenantCapabilitySwitch>> {
    policy.list_tenant_switches(tenant_id).await
}

pub async fn tenant_runtime<R: TenantRuntimeReader + ?Sized>(
    reader: &R,
    tenant_id: &str,
) -> flare_server_core::error::Result<Option<TenantRuntimeView>> {
    reader.tenant_runtime(tenant_id).await
}
