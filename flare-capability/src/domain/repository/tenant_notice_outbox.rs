//! Port of Core's own tenant notice outbox (`tenant_notice_outbox`). A projection that moves a
//! `flare.*` watermark writes one row in its own transaction; the relay publishes it after the
//! commit, so a client never hears about a version the data plane does not hold yet, and a notice
//! survives a crash between the commit and the publish. One row per (tenant, topic): a newer
//! version replaces a pending one.
use std::time::Duration;

/// One notice due for publishing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTenantNotice {
    pub tenant_id: String,
    pub topic: String,
    pub version: u64,
    /// Claims so far, this one included.
    pub attempts: i32,
}

#[async_trait::async_trait]
pub trait TenantNoticeOutbox: Send + Sync {
    /// Claim up to `limit` due rows; a claimed row stays invisible for `lease`, so a worker that
    /// dies before ending it hands it back.
    async fn claim_due(
        &self,
        limit: u32,
        lease: Duration,
    ) -> flare_server_core::error::Result<Vec<PendingTenantNotice>>;

    /// End a published row. A row that moved to a newer version meanwhile stays.
    async fn complete(&self, notice: &PendingTenantNotice) -> flare_server_core::error::Result<()>;

    /// Make a row due again after `delay`.
    async fn retry_after(
        &self,
        notice: &PendingTenantNotice,
        delay: Duration,
        error: &str,
    ) -> flare_server_core::error::Result<()>;
}
