//! [`IdempotencyLedger`] 的 PostgreSQL 实现：按主键点查 `message_idempotency_ledger`。
//!
//! 旧消息回查与已提交回执共用有界的懒连接池。

use std::num::NonZeroU32;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use flare_proto::common::SendAckDurability;
use flare_server_core::error::{FlareError, Result};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::domain::repository::{IdempotencyLedger, LedgerRecord};

const DEFAULT_MAX_CONNECTIONS: NonZeroU32 = NonZeroU32::MIN.saturating_add(3); // 4
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(2);
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

pub struct PostgresIdempotencyLedger {
    pool: PgPool,
}

impl PostgresIdempotencyLedger {
    /// 懒连接：只解析连接串，第一次回查时才建连。
    pub fn connect_lazy(database_url: &str) -> Result<Self> {
        Self::connect_with_limit(database_url, DEFAULT_MAX_CONNECTIONS)
    }

    pub(crate) fn connect_with_limit(database_url: &str, limit: NonZeroU32) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(limit.get())
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
        conversation_id: &str,
        client_msg_id: &str,
    ) -> Result<Option<LedgerRecord>> {
        let started = Instant::now();
        let mut connection =
            self.pool.acquire().await.map_err(|e| {
                FlareError::system(format!("idempotency ledger lookup failed: {e}"))
            })?;
        let acquired = Instant::now();
        let row = sqlx::query_as::<_, (String, String, i64, i16)>(
            r#"
            SELECT conversation_id, server_id, conversation_seq, durability
              FROM message_idempotency_ledger
             WHERE tenant_id = $1
               AND sender_id = $2
               AND conversation_id = $3
                AND client_msg_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(sender_id)
        .bind(conversation_id)
        .bind(client_msg_id)
        .fetch_optional(&mut *connection)
        .await;
        let done = Instant::now();
        drop(connection);
        tracing::trace!(target: "flare_message_ingest::archive_receipt", kind = "historical", client_id = %client_msg_id,
            pool_us = (acquired - started).as_micros() as u64, query_us = (done - acquired).as_micros() as u64, "ledger query duration");
        let row =
            row.map_err(|e| FlareError::system(format!("idempotency ledger lookup failed: {e}")))?;
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

    async fn find_committed(
        &self,
        tenant_id: &str,
        sender_id: &str,
        conversation_id: &str,
        client_msg_id: &str,
    ) -> Result<Option<(String, u64)>> {
        let started = Instant::now();
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|e| FlareError::system(format!("archive receipt lookup failed: {e}")))?;
        let acquired = Instant::now();
        let row = sqlx::query_as::<_, (String, i64)>(
            r#"SELECT m.server_id, m.seq FROM message_idempotency_ledger l
               JOIN messages m ON m.tenant_id = l.tenant_id AND m.server_id = l.server_id
                 AND m.conversation_id = l.conversation_id AND m.seq = l.conversation_seq
                 AND m.sender_id = l.sender_id AND m.client_msg_id = l.client_msg_id
               WHERE l.tenant_id = $1 AND l.sender_id = $2
                 AND l.conversation_id = $3 AND l.client_msg_id = $4 AND m.seq > 0"#,
        )
        .bind(tenant_id)
        .bind(sender_id)
        .bind(conversation_id)
        .bind(client_msg_id)
        .fetch_optional(&mut *connection)
        .await;
        let done = Instant::now();
        drop(connection);
        tracing::trace!(target: "flare_message_ingest::archive_receipt", kind = "receipt", client_id = %client_msg_id,
            pool_us = (acquired - started).as_micros() as u64, query_us = (done - acquired).as_micros() as u64, "ledger query duration");
        let row =
            row.map_err(|e| FlareError::system(format!("archive receipt lookup failed: {e}")))?;
        Ok(row.map(|(id, seq)| (id, seq as u64)))
    }
}
