//! RTC 能力实例表（写侧）：active / draining 选路数据源。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityInstanceRecord {
    pub id: Uuid,
    pub tenant_id: Option<String>,
    pub grpc_endpoint: String,
    pub status: String,
    pub draining: bool,
    pub disabled: bool,
    pub version: Option<String>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
}

#[async_trait]
pub trait CapabilityInstanceRepository: Send + Sync {
    async fn save(&self, row: &CapabilityInstanceRecord) -> anyhow::Result<()>;

    async fn find_by_id(&self, id: &Uuid) -> anyhow::Result<Option<CapabilityInstanceRecord>>;

    async fn update_status(&self, id: &Uuid, status: &str, draining: bool, disabled: bool) -> anyhow::Result<()>;

    async fn list_active_instances(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<CapabilityInstanceRecord>>;

    async fn list_draining_instances(&self, tenant_id: Option<&str>) -> anyhow::Result<Vec<CapabilityInstanceRecord>>;
}

#[derive(Clone)]
pub struct PostgresCapabilityInstanceRepository {
    _pool: PgPool,
}

impl PostgresCapabilityInstanceRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { _pool: pool }
    }
}

#[async_trait]
impl CapabilityInstanceRepository for PostgresCapabilityInstanceRepository {
    async fn save(&self, _row: &CapabilityInstanceRecord) -> anyhow::Result<()> {
        Ok(())
    }

    async fn find_by_id(&self, _id: &Uuid) -> anyhow::Result<Option<CapabilityInstanceRecord>> {
        Ok(None)
    }

    async fn update_status(
        &self,
        _id: &Uuid,
        _status: &str,
        _draining: bool,
        _disabled: bool,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn list_active_instances(
        &self,
        _tenant_id: Option<&str>,
    ) -> anyhow::Result<Vec<CapabilityInstanceRecord>> {
        Ok(vec![])
    }

    async fn list_draining_instances(
        &self,
        _tenant_id: Option<&str>,
    ) -> anyhow::Result<Vec<CapabilityInstanceRecord>> {
        Ok(vec![])
    }
}
