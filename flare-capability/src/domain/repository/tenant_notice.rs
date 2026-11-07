//! Port of tenant config notices: "topic `T` of tenant `X` moved to version `V`", announced to
//! that tenant's online clients (every tenant for `*`) at most once per version.

use flare_server_core::error::Result;

use crate::domain::model::NoticeOutcome;

#[async_trait::async_trait]
pub trait TenantNotices: Send + Sync {
    /// Publishes `version` of `topic` for `tenant_id` when it is above the last published one;
    /// an equal or older version is not published again and reports the current one.
    async fn publish(&self, tenant_id: &str, topic: &str, version: u64) -> Result<NoticeOutcome>;
}
