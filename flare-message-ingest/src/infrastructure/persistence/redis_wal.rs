use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use flare_im_contracts::wal_pending_index_key;
use flare_server_core::error::Result;
use prost::Message;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

use crate::config::MessageIngestConfig;
use crate::domain::model::MessageSubmission;
use crate::domain::repository::{WalPendingMessage, WalRepository};

const CLAIM_PENDING_SCRIPT: &str = r#"
local pending_key = KEYS[1]
local wal_key = KEYS[2]
local now_ms = tonumber(ARGV[1])
local lease_until_ms = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

local candidates = redis.call('ZRANGEBYSCORE', pending_key, '-inf', now_ms, 'LIMIT', 0, limit)
local claimed = {}

for _, message_id in ipairs(candidates) do
    if redis.call('HEXISTS', wal_key, message_id) == 1 then
        redis.call('ZADD', pending_key, lease_until_ms, message_id)
        table.insert(claimed, message_id)
    else
        redis.call('ZREM', pending_key, message_id)
    end
end

return claimed
"#;

#[derive(Serialize, Deserialize)]
struct WalEntrySnapshot {
    message_id: String,
    #[serde(default)]
    tenant_id: String,
    encoded: String,
    persisted: bool,
}

pub struct RedisWalRepository {
    client: Arc<redis::Client>,
    config: Arc<MessageIngestConfig>,
    connection: OnceCell<ConnectionManager>,
}

impl std::fmt::Debug for RedisWalRepository {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisWalRepository")
            .field("wal_hash_key", &self.config.wal_hash_key)
            .finish()
    }
}

impl RedisWalRepository {
    pub fn new(client: Arc<redis::Client>, config: Arc<MessageIngestConfig>) -> Self {
        Self {
            client,
            config,
            connection: OnceCell::new(),
        }
    }

    fn now_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
            .unwrap_or_default()
    }

    fn replay_claim_lease_ms(&self) -> i64 {
        self.config.wal_replay_claim_lease_ms.max(1000) as i64
    }

    async fn connection(&self) -> Result<ConnectionManager> {
        let manager = self
            .connection
            .get_or_try_init(|| async {
                self.client
                    .get_connection_manager()
                    .await
                    .map_err(|e| flare_server_core::error::FlareError::system(e.to_string()))
            })
            .await?;
        Ok(manager.clone())
    }

    async fn claim_pending_ids(
        &self,
        conn: &mut ConnectionManager,
        wal_key: &str,
        pending_key: &str,
        limit: usize,
    ) -> Result<Vec<String>> {
        let now_ms = Self::now_millis();
        let lease_until_ms = now_ms.saturating_add(self.replay_claim_lease_ms());
        redis::Script::new(CLAIM_PENDING_SCRIPT)
            .key(pending_key)
            .key(wal_key)
            .arg(now_ms)
            .arg(lease_until_ms)
            .arg(limit)
            .invoke_async(conn)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "Redis WAL claim script error: {}",
                    e
                ))
            })
    }

    async fn cleanup_pending_index(
        conn: &mut ConnectionManager,
        pending_key: &str,
        message_id: &str,
    ) -> Result<()> {
        conn.zrem::<_, _, ()>(pending_key, message_id)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("Redis zrem error: {}", e))
            })
    }

    fn decode_wal_snapshot(entry_json: &str) -> Result<WalEntrySnapshot> {
        serde_json::from_str(entry_json).map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "Failed to deserialize WAL entry: {}",
                e
            ))
        })
    }

    fn decode_wal_message(
        entry: &WalEntrySnapshot,
        message_id: &str,
    ) -> Result<flare_proto::common::Message> {
        // 解码 base64 编码的 payload
        let payload_bytes = BASE64.decode(&entry.encoded).map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "Failed to decode base64 payload from WAL: {}",
                e
            ))
        })?;

        let message = flare_proto::common::Message::decode(&payload_bytes[..]).map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "Failed to decode Message from WAL: {}",
                e
            ))
        })?;
        tracing::trace!(
            message_id = %message_id,
            sender_id = %message.sender_id,
            "Decoded message from WAL entry"
        );
        Ok(message)
    }

    fn decode_wal_entry(
        entry_json: &str,
        message_id: &str,
    ) -> Result<Option<flare_proto::common::Message>> {
        let entry = Self::decode_wal_snapshot(entry_json)?;
        let message = Self::decode_wal_message(&entry, message_id)?;
        Ok(Some(message))
    }
}

impl WalRepository for RedisWalRepository {
    fn append<'a>(
        &'a self,
        submission: &'a MessageSubmission,
        tenant_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        let _self = self; // 保持对 self 的引用
        let _submission = submission; // 保持对 submission 的引用
        let _tenant_id = tenant_id; // 保持对 tenant_id 的引用
        Box::pin(async move {
            let wal_key = match &_self.config.wal_hash_key {
                Some(key) => key.as_str(),
                None => {
                    tracing::trace!(
                        message_id = %_submission.message_id,
                        "WAL not configured (wal_hash_key is None), skipping WAL write"
                    );
                    return Ok(());
                }
            };

            let mut conn = _self.connection().await?;
            let pending_key = wal_pending_index_key(wal_key);

            // 使用 message.server_id 作为 WAL key（确保与查询时一致）
            // 注意：submission.message_id 应该等于 submission.message.server_id，但为了安全起见，直接使用 message.server_id
            let wal_message_id = _submission.message.server_id.clone();

            let encoded_payload = BASE64.encode(_submission.message.encode_to_vec());
            let entry = WalEntrySnapshot {
                message_id: wal_message_id.clone(),
                tenant_id: _tenant_id.to_string(),
                encoded: encoded_payload,
                persisted: false,
            };

            let payload = serde_json::to_string(&entry)?;
            let now_ms = Self::now_millis();
            let mut pipe = redis::pipe();
            pipe.atomic()
                .cmd("HSET")
                .arg(wal_key)
                .arg(&wal_message_id)
                .arg(payload)
                .cmd("ZADD")
                .arg(&pending_key)
                .arg(now_ms)
                .arg(&wal_message_id);

            if _self.config.wal_ttl_seconds > 0 {
                pipe.cmd("EXPIRE")
                    .arg(wal_key)
                    .arg(_self.config.wal_ttl_seconds)
                    .cmd("EXPIRE")
                    .arg(&pending_key)
                    .arg(_self.config.wal_ttl_seconds);
            }

            let _: Vec<redis::Value> = pipe.query_async(&mut conn).await.map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "Redis WAL append pipeline error: {}",
                    e
                ))
            })?;

            tracing::trace!(
                message_id = %wal_message_id,
                submission_message_id = %_submission.message_id,
                wal_key = %wal_key,
                pending_key = %pending_key,
                ttl_seconds = %_self.config.wal_ttl_seconds,
                "✅ WAL entry written successfully"
            );

            Ok(())
        })
    }

    fn find_by_message_id<'a>(
        &'a self,
        message_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<flare_proto::common::Message>>> + Send + 'a>>
    {
        let _self = self;
        let _message_id = message_id.to_string();
        Box::pin(async move {
            let wal_key = match &_self.config.wal_hash_key {
                Some(key) => key.as_str(),
                None => {
                    tracing::trace!(
                        message_id = %_message_id,
                        "WAL not configured (wal_hash_key is None), cannot query WAL"
                    );
                    return Ok(None);
                }
            };

            tracing::trace!(
                message_id = %_message_id,
                wal_key = %wal_key,
                "🔍 Querying WAL for message"
            );

            let mut conn = _self.connection().await.map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "Redis connection error: {}",
                    e
                ))
            })?;

            // 首先尝试使用 message_id 直接查询（可能是服务端生成的 server_id）
            let entry_json: Option<String> = match conn.hget(wal_key, &_message_id).await {
                Ok(value) => value,
                Err(e) => {
                    return Err(flare_server_core::error::FlareError::system(format!(
                        "Redis hget error: {}",
                        e
                    )));
                }
            };

            if let Some(json_str) = entry_json {
                tracing::trace!(
                    message_id = %_message_id,
                    "Found WAL entry by server_id, decoding..."
                );
                return Self::decode_wal_entry(&json_str, &_message_id);
            }

            // 如果直接查询不到，遍历 WAL 中的所有条目，查找 extra.original_server_id 等于 message_id 的条目
            // 注意：这可能会影响性能，但可以确保找到消息（即使使用的是客户端生成的 server_id）
            tracing::trace!(
                message_id = %_message_id,
                "WAL entry not found by server_id, searching by original_server_id..."
            );

            let all_entries: std::collections::HashMap<String, String> =
                conn.hgetall(wal_key).await.map_err(|e| {
                    flare_server_core::error::FlareError::system(format!(
                        "Redis hgetall error: {}",
                        e
                    ))
                })?;

            for (wal_server_id, entry_json) in all_entries {
                if let Ok(Some(message)) = Self::decode_wal_entry(&entry_json, &wal_server_id) {
                    // 检查 extensions.original_server_id 是否等于查询的 message_id
                    if let Some(original_server_id) = message.extensions.get("original_server_id")
                        && original_server_id.as_slice() == _message_id.as_bytes()
                    {
                        tracing::trace!(
                            query_message_id = %_message_id,
                            wal_server_id = %wal_server_id,
                            "Found WAL entry by original_server_id"
                        );
                        return Ok(Some(message));
                    }
                }
            }

            tracing::trace!(
                message_id = %_message_id,
                wal_key = %wal_key,
                "WAL entry not found in Redis (searched by both server_id and original_server_id)"
            );
            Ok(None)
        })
    }

    fn list_pending<'a>(
        &'a self,
        limit: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<WalPendingMessage>>> + Send + 'a>> {
        let _self = self;
        Box::pin(async move {
            if limit == 0 {
                return Ok(Vec::new());
            }

            let wal_key = match &_self.config.wal_hash_key {
                Some(key) => key.as_str(),
                None => {
                    tracing::trace!(
                        "WAL not configured (wal_hash_key is None), no pending messages"
                    );
                    return Ok(Vec::new());
                }
            };

            let mut conn = _self.connection().await?;
            let pending_key = wal_pending_index_key(wal_key);
            let claimed_ids = _self
                .claim_pending_ids(&mut conn, wal_key, &pending_key, limit)
                .await?;

            if claimed_ids.is_empty() {
                return Ok(Vec::new());
            }

            let entry_jsons: Vec<Option<String>> = redis::cmd("HMGET")
                .arg(wal_key)
                .arg(&claimed_ids)
                .query_async(&mut conn)
                .await
                .map_err(|e| {
                    flare_server_core::error::FlareError::system(format!(
                        "Redis hmget WAL entries error: {}",
                        e
                    ))
                })?;

            let mut pending = Vec::with_capacity(claimed_ids.len());
            for (message_id, entry_json) in claimed_ids.into_iter().zip(entry_jsons.into_iter()) {
                let Some(entry_json) = entry_json else {
                    Self::cleanup_pending_index(&mut conn, &pending_key, &message_id).await?;
                    continue;
                };

                let entry = match Self::decode_wal_snapshot(&entry_json) {
                    Ok(entry) => entry,
                    Err(error) => {
                        tracing::warn!(
                            wal_key = %wal_key,
                            message_id = %message_id,
                            error = %error,
                            "Skipping invalid WAL entry while listing pending messages"
                        );
                        continue;
                    }
                };
                if entry.persisted {
                    let mut pipe = redis::pipe();
                    pipe.atomic()
                        .cmd("HDEL")
                        .arg(wal_key)
                        .arg(&message_id)
                        .cmd("ZREM")
                        .arg(&pending_key)
                        .arg(&message_id);
                    let _: Vec<redis::Value> = pipe.query_async(&mut conn).await.map_err(|e| {
                        flare_server_core::error::FlareError::system(format!(
                            "Redis WAL persisted cleanup error: {}",
                            e
                        ))
                    })?;
                    continue;
                }

                let message = match Self::decode_wal_message(&entry, &message_id) {
                    Ok(message) => message,
                    Err(error) => {
                        tracing::warn!(
                            wal_key = %wal_key,
                            message_id = %message_id,
                            error = %error,
                            "Skipping undecodable WAL message while listing pending messages"
                        );
                        continue;
                    }
                };

                pending.push(WalPendingMessage {
                    message_id,
                    tenant_id: entry.tenant_id,
                    message,
                });
            }

            Ok(pending)
        })
    }

    fn remove<'a>(
        &'a self,
        message_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        let _self = self;
        let _message_id = message_id.to_string();
        Box::pin(async move {
            let wal_key = match &_self.config.wal_hash_key {
                Some(key) => key.as_str(),
                None => {
                    tracing::trace!(
                        message_id = %_message_id,
                        "WAL not configured (wal_hash_key is None), skipping WAL remove"
                    );
                    return Ok(());
                }
            };

            let mut conn = _self.connection().await?;
            let pending_key = wal_pending_index_key(wal_key);
            let mut pipe = redis::pipe();
            pipe.atomic()
                .cmd("HDEL")
                .arg(wal_key)
                .arg(&_message_id)
                .cmd("ZREM")
                .arg(&pending_key)
                .arg(&_message_id);
            let _: Vec<redis::Value> = pipe.query_async(&mut conn).await.map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "Redis WAL remove pipeline error: {}",
                    e
                ))
            })?;

            tracing::trace!(
                message_id = %_message_id,
                wal_key = %wal_key,
                pending_key = %pending_key,
                "WAL entry removed after broker accept"
            );
            Ok(())
        })
    }
}
