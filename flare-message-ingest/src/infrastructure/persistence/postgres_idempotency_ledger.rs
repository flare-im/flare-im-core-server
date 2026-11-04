//! [`IdempotencyLedger`] 的 PostgreSQL 实现：按主键点查 `message_idempotency_ledger`。
//!
//! 连接池懒建、至多 2 个连接：只有客户端创建时间早于 Redis 去重窗口的重发才会用到它，平时一个连接也不开。

use std::time::Duration;

use async_trait::async_trait;
use flare_proto::common::SendAckDurability;
use flare_server_core::error::{FlareError, Result};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::domain::repository::{IdempotencyLedger, LedgerRecord};

const MAX_CONNECTIONS: u32 = 2;
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(2);
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

pub struct PostgresIdempotencyLedger {
    pool: PgPool,
}

impl PostgresIdempotencyLedger {
    /// 懒连接：只解析连接串，第一次回查时才建连。
    pub fn connect_lazy(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(MAX_CONNECTIONS)
            .min_connections(0)
            .acquire_timeout(ACQUIRE_TIMEOUT)
            .idle_timeout(IDLE_TIMEOUT)
            .connect_lazy(database_url)
            .map_err(|e| FlareError::system(format!("idempotency ledger postgres url: {e}")))?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl IdempotencyLedger for PostgresIdempotencyLedger {
    async fn find(
        &self,
        tenant_id: &str,
        sender_id: &str,
        client_msg_id: &str,
    ) -> Result<Option<LedgerRecord>> {
        let row: Option<(String, String, i64, i16)> = sqlx::query_as(
            r#"
            SELECT conversation_id, server_id, conversation_seq, durability
              FROM message_idempotency_ledger
             WHERE tenant_id = $1
               AND sender_id = $2
               AND client_msg_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(sender_id)
        .bind(client_msg_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| FlareError::system(format!("idempotency ledger lookup failed: {e}")))?;
        let Some((conversation_id, server_id, conversation_seq, durability)) = row else {
            return Ok(None);
        };
        let conversation_seq = u64::try_from(conversation_seq).map_err(|_| {
            FlareError::system(format!(
                "idempotency ledger row has a negative conversation_seq for message {server_id}"
            ))
        })?;
        Ok(Some(LedgerRecord {
            conversation_id,
            server_id,
            conversation_seq,
            // 账本行与消息同一事务写入，有行即已落库：读到不认识的值也按已持久化回复。
            durability: SendAckDurability::try_from(i32::from(durability))
                .unwrap_or(SendAckDurability::Persisted),
        }))
    }
}
