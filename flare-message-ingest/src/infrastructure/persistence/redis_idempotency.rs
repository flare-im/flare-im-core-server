//! [`IngestIdempotencyStore`] 的 Redis 实现：用 `SET key val NX EX` 原子占位，按 `client_msg_id` 去重重发。
//!
//! 状态机（值约定）：
//! - 占位中：`"PENDING"`（短 TTL `reserve_ttl`，覆盖一次发送处理时长）。
//! - 完成：`"<server_id>|<conversation_seq>|<durability>"`（长 TTL `result_ttl`，覆盖合理重试窗口）。
//!
//! `begin` 先 `SET NX`：成功 = 首次([`Fresh`]); 失败则 `GET` 判定 [`Replay`] 或 [`InFlight`]。

use std::sync::Arc;

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use tokio::sync::OnceCell;

use crate::domain::repository::ingest_idempotency::{
    IdempotencyBegin, IdempotentRecord, IngestIdempotencyStore,
};
use flare_proto::common::SendAckDurability;
use flare_server_core::error::{FlareError, Result};

const PENDING_MARKER: &str = "PENDING";

pub struct RedisIngestIdempotencyStore {
    client: Arc<redis::Client>,
    connection: OnceCell<ConnectionManager>,
    /// 占位 TTL（秒）：须 ≥ 单次发送处理时长，避免慢发送被并发重试误判为可重试。
    reserve_ttl_secs: u64,
    /// 结果 TTL（秒）：须 ≥ 合理重试窗口（含离线重连后补发）。
    result_ttl_secs: u64,
}

impl RedisIngestIdempotencyStore {
    pub fn new(client: Arc<redis::Client>, reserve_ttl_secs: u64, result_ttl_secs: u64) -> Self {
        Self {
            client,
            connection: OnceCell::new(),
            reserve_ttl_secs: reserve_ttl_secs.max(1),
            result_ttl_secs: result_ttl_secs.max(1),
        }
    }

    async fn connection(&self) -> Result<ConnectionManager> {
        let manager = self
            .connection
            .get_or_try_init(|| async {
                self.client
                    .get_connection_manager()
                    .await
                    .map_err(|e| FlareError::system(e.to_string()))
            })
            .await?;
        Ok(manager.clone())
    }

    fn encode(record: &IdempotentRecord) -> String {
        format!(
            "{}|{}|{}",
            record.server_id, record.conversation_seq, record.durability as i32
        )
    }

    fn decode(value: &str) -> Option<IdempotentRecord> {
        let mut parts = value.split('|');
        let server_id = parts.next()?;
        let seq = parts.next()?;
        if server_id.is_empty() {
            return None;
        }
        let conversation_seq = seq.parse::<u64>().ok()?;
        let durability = parts
            .next()
            .and_then(|raw| raw.parse::<i32>().ok())
            .and_then(|raw| SendAckDurability::try_from(raw).ok())
            .unwrap_or(SendAckDurability::Persisted);
        Some(IdempotentRecord {
            server_id: server_id.to_string(),
            conversation_seq,
            durability,
        })
    }
}

#[async_trait]
impl IngestIdempotencyStore for RedisIngestIdempotencyStore {
    async fn begin(&self, key: &str) -> Result<IdempotencyBegin> {
        let mut conn = self.connection().await?;
        // SET key PENDING NX EX reserve_ttl —— 原子占位。成功返回 "OK"，已存在返回 nil。
        let set: Option<String> = redis::cmd("SET")
            .arg(key)
            .arg(PENDING_MARKER)
            .arg("NX")
            .arg("EX")
            .arg(self.reserve_ttl_secs)
            .query_async(&mut conn)
            .await
            .map_err(|e| FlareError::system(e.to_string()))?;
        if set.is_some() {
            return Ok(IdempotencyBegin::Fresh);
        }
        // 已存在：读取判定是完成结果还是处理中。
        let existing: Option<String> = redis::cmd("GET")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(|e| FlareError::system(e.to_string()))?;
        match existing {
            // 占位与 GET 之间过期（极少见）：视为首次，重新占位以避免漏处理。
            None => self.begin(key).await,
            Some(value) if value == PENDING_MARKER => Ok(IdempotencyBegin::InFlight),
            Some(value) => match Self::decode(&value) {
                Some(record) => Ok(IdempotencyBegin::Replay(record)),
                // 值损坏：当作处理中（可重试）而非误放重复。
                None => Ok(IdempotencyBegin::InFlight),
            },
        }
    }

    async fn commit(&self, key: &str, record: &IdempotentRecord) -> Result<()> {
        let mut conn = self.connection().await?;
        let _: () = redis::cmd("SET")
            .arg(key)
            .arg(Self::encode(record))
            .arg("EX")
            .arg(self.result_ttl_secs)
            .query_async(&mut conn)
            .await
            .map_err(|e| FlareError::system(e.to_string()))?;
        Ok(())
    }

    async fn rollback(&self, key: &str) -> Result<()> {
        let mut conn = self.connection().await?;
        let _: () = redis::cmd("DEL")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(|e| FlareError::system(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let rec = IdempotentRecord {
            server_id: "srv-123".into(),
            conversation_seq: 42,
            durability: SendAckDurability::TransientAccepted,
        };
        let encoded = RedisIngestIdempotencyStore::encode(&rec);
        assert_eq!(encoded, "srv-123|42|1");
        assert_eq!(RedisIngestIdempotencyStore::decode(&encoded), Some(rec));
    }

    #[test]
    fn decode_old_record_defaults_to_persisted() {
        assert_eq!(
            RedisIngestIdempotencyStore::decode("srv-123|42"),
            Some(IdempotentRecord {
                server_id: "srv-123".into(),
                conversation_seq: 42,
                durability: SendAckDurability::Persisted,
            })
        );
    }

    #[test]
    fn decode_rejects_malformed() {
        assert_eq!(RedisIngestIdempotencyStore::decode("PENDING"), None);
        assert_eq!(RedisIngestIdempotencyStore::decode("|5"), None);
        assert_eq!(RedisIngestIdempotencyStore::decode("srv|notnum"), None);
    }
}
