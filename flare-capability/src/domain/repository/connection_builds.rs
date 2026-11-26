//! Port of the online build distribution: what the access gateways last reported for a tenant,
//! summed over the gateways that are still alive.

use flare_im_core_service_kit::gateway::connection_builds::ConnectionBuildRow;
use flare_server_core::error::Result;

/// A tenant's online connections by (platform, build), and how many gateways contributed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectionBuildSummary {
    pub rows: Vec<ConnectionBuildRow>,
    pub gateways: usize,
}

#[async_trait::async_trait]
pub trait ConnectionBuildStats: Send + Sync {
    async fn tenant_builds(&self, tenant_id: &str, now_ms: i64) -> Result<ConnectionBuildSummary>;
}
