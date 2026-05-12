//! 通话会话持久化（写侧）：trait + PostgreSQL 骨架实现。
//!
//! **说明**：行模型定义在本 crate，避免 `flare-storage-writer` → `flare-conversation` 环依赖；
//! 字段与 `flare_conversation::domain::call::CallSession` 对齐，由应用层映射。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

/// 物化行（投影 / 表 `call_sessions` 对应，迁移脚本后续补）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallSessionRecord {
    pub id: Uuid,
    pub tenant_id: String,
    pub conversation_id: String,
    pub call_id: Option<String>,
    pub sfu_room_id: Option<String>,
    pub capability_instance_id: Option<String>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[async_trait]
pub trait CallSessionRepository: Send + Sync {
    async fn save(&self, row: &CallSessionRecord) -> anyhow::Result<()>;

    async fn find_by_id(&self, id: &Uuid) -> anyhow::Result<Option<CallSessionRecord>>;

    async fn find_by_room_id(&self, sfu_room_id: &str)
    -> anyhow::Result<Option<CallSessionRecord>>;

    async fn update_status(&self, id: &Uuid, status: &str) -> anyhow::Result<()>;
}

/// PostgreSQL 实现骨架（SQL 后续落地）。
#[derive(Clone)]
pub struct PostgresCallSessionRepository {
    _pool: PgPool,
}

impl PostgresCallSessionRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { _pool: pool }
    }
}

#[async_trait]
impl CallSessionRepository for PostgresCallSessionRepository {
    async fn save(&self, _row: &CallSessionRecord) -> anyhow::Result<()> {
        Ok(())
    }

    async fn find_by_id(&self, _id: &Uuid) -> anyhow::Result<Option<CallSessionRecord>> {
        Ok(None)
    }

    async fn find_by_room_id(
        &self,
        _sfu_room_id: &str,
    ) -> anyhow::Result<Option<CallSessionRecord>> {
        Ok(None)
    }

    async fn update_status(&self, _id: &Uuid, _status: &str) -> anyhow::Result<()> {
        Ok(())
    }
}
