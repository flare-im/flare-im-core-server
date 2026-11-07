//! 客户端策略投影写路径：校验过的来件按 version 幂等落表。

use flare_server_core::error::Result;

use crate::domain::model::{ClientPolicyRecord, TenantProjectionOutcome};
use crate::domain::repository::TenantProjectionRepository;

/// 应用一条已校验的客户端策略投影。旧版本来件不报错，只返回 `accepted = false` 与本地版本。
pub async fn apply_client_policy<R: TenantProjectionRepository>(
    repository: &R,
    record: &ClientPolicyRecord,
) -> Result<TenantProjectionOutcome> {
    let outcome = repository.upsert_client_policy(record).await?;
    if outcome.accepted {
        tracing::info!(
            tenant_id = %record.tenant_id,
            version = record.version,
            access_serial = record.access_serial,
            release_generation = record.release_generation,
            "client policy projection applied"
        );
    } else {
        tracing::debug!(
            tenant_id = %record.tenant_id,
            incoming_version = record.version,
            current_version = outcome.current_version,
            "client policy projection ignored: not newer than local"
        );
    }
    Ok(outcome)
}
