//! 发布租户配置通知：校验发布者的命名空间 → 版本单调地发布一次。

use flare_server_core::error::Result;

use crate::domain::model::{NoticeDenied, NoticeOutcome, NoticePublishers};
use crate::domain::repository::tenant_notice::TenantNotices;

/// 服务 `service` 为 `tenant_id`（`*` 为全部租户）发布 `topic` 的 `version`。不在命名空间内、
/// 格式不对的返回 [`NoticeDenied`]；已有不低于它的版本时不再发布，`accepted = false`。
pub async fn publish_tenant_notice(
    notices: &dyn TenantNotices,
    publishers: &NoticePublishers,
    service: &str,
    tenant_id: &str,
    topic: &str,
    version: u64,
) -> std::result::Result<Result<NoticeOutcome>, NoticeDenied> {
    if version == 0 {
        return Err(NoticeDenied::InvalidVersion);
    }
    publishers.authorize(service, tenant_id, topic)?;
    let outcome = notices.publish(tenant_id, topic, version).await;
    if let Ok(outcome) = &outcome {
        tracing::info!(
            %service,
            %tenant_id,
            %topic,
            version,
            accepted = outcome.accepted,
            current_version = outcome.current_version,
            "tenant notice published"
        );
    }
    Ok(outcome)
}
