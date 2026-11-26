//! Online build distribution read from the token store Redis, where every access gateway writes
//! its own counts every 30 s (see `flare_im_core_service_kit::gateway::connection_builds`).

use crate::domain::repository::connection_builds::{ConnectionBuildStats, ConnectionBuildSummary};
use flare_im_core_service_kit::gateway::connection_builds::read_tenant_builds;
use flare_im_core_service_kit::redis_connection::{RedisConnectionLimits, SharedRedisConnection};
use flare_server_core::error::{FlareError, Result};

pub struct RedisConnectionBuildStats {
    connection: SharedRedisConnection,
}

impl RedisConnectionBuildStats {
    pub fn new(client: redis::Client) -> Self {
        Self {
            connection: SharedRedisConnection::new(client, RedisConnectionLimits::default()),
        }
    }
}

#[async_trait::async_trait]
impl ConnectionBuildStats for RedisConnectionBuildStats {
    async fn tenant_builds(&self, tenant_id: &str, now_ms: i64) -> Result<ConnectionBuildSummary> {
        let mut connection = self
            .connection
            .get()
            .await
            .map_err(|_| FlareError::system("connection build Redis unavailable"))?;
        let (rows, gateways) = read_tenant_builds(&mut connection, tenant_id, now_ms)
            .await
            .map_err(|_| FlareError::system("connection builds not readable"))?;
        Ok(ConnectionBuildSummary { rows, gateways })
    }
}
