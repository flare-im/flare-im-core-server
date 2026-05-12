//! `call_id` / `sfu_room_id` 与 capability 实例绑定（写侧物化）。

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallRoomBindingRecord {
    pub id: Uuid,
    pub tenant_id: String,
    pub call_id: String,
    pub sfu_room_id: String,
    pub capability_instance_id: String,
}

#[async_trait]
pub trait CallRoomBindingRepository: Send + Sync {
    async fn save(&self, row: &CallRoomBindingRecord) -> anyhow::Result<()>;

    async fn find_by_call_id(
        &self,
        tenant_id: &str,
        call_id: &str,
    ) -> anyhow::Result<Option<CallRoomBindingRecord>>;

    async fn find_by_room_id(
        &self,
        tenant_id: &str,
        sfu_room_id: &str,
    ) -> anyhow::Result<Option<CallRoomBindingRecord>>;
}

#[derive(Clone)]
pub struct PostgresCallRoomBindingRepository {
    _pool: PgPool,
}

impl PostgresCallRoomBindingRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { _pool: pool }
    }
}

#[async_trait]
impl CallRoomBindingRepository for PostgresCallRoomBindingRepository {
    async fn save(&self, _row: &CallRoomBindingRecord) -> anyhow::Result<()> {
        Ok(())
    }

    async fn find_by_call_id(
        &self,
        _tenant_id: &str,
        _call_id: &str,
    ) -> anyhow::Result<Option<CallRoomBindingRecord>> {
        Ok(None)
    }

    async fn find_by_room_id(
        &self,
        _tenant_id: &str,
        _sfu_room_id: &str,
    ) -> anyhow::Result<Option<CallRoomBindingRecord>> {
        Ok(None)
    }
}
