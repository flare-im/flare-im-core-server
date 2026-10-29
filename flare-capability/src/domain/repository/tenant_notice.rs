//! Port of the tenant-wide notices Core sends after accepting a projection, so online clients of
//! that tenant refresh what depends on it.

#[async_trait::async_trait]
pub trait TenantNotices: Send + Sync {
    /// A newer plugin authorization projection `version` of `tenant_id` was committed. Best
    /// effort: a failure is logged, never returned, and never undoes the accepted projection.
    async fn plugin_catalog_changed(&self, tenant_id: &str, version: u64);
}
