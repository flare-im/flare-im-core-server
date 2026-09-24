//! 租户投影写路径：校验来件 → 按 version 幂等落表。

use flare_server_core::error::Result;

use crate::domain::model::{TenantProjectionOutcome, TenantProjectionRecord};
use crate::domain::repository::TenantProjectionRepository;

/// 应用一条已校验的租户投影。旧版本来件不报错，只返回 `accepted = false` 与本地版本，
/// 控制面据此判断是否需要重推。
pub async fn apply_tenant_projection<R: TenantProjectionRepository>(
    repository: &R,
    record: &TenantProjectionRecord,
) -> Result<TenantProjectionOutcome> {
    let outcome = repository.upsert_if_newer(record).await?;
    if outcome.accepted {
        tracing::info!(
            tenant_id = %record.tenant_id,
            status = record.status.as_str(),
            version = record.version,
            "tenant projection applied"
        );
    } else {
        tracing::debug!(
            tenant_id = %record.tenant_id,
            incoming_version = record.version,
            current_version = outcome.current_version,
            "tenant projection ignored: not newer than local"
        );
    }
    Ok(outcome)
}
