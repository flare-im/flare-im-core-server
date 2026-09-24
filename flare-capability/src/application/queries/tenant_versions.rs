//! 租户投影对账：控制面 `GetVersions` 用来发现落后的租户并重推。

use std::collections::HashMap;

use flare_server_core::error::Result;

use crate::domain::repository::TenantProjectionRepository;

/// 本地持有的 `tenant_version`（核不消费功能开关投影，`features_version` 恒为 0，由接口层填）。
pub async fn tenant_projection_versions<R: TenantProjectionRepository>(
    repository: &R,
    tenant_ids: &[String],
) -> Result<HashMap<String, u64>> {
    Ok(repository.versions(tenant_ids).await?.into_iter().collect())
}
