use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::error::Result;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use prost::Message;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::{Serialize, Deserialize};

use crate::config::MessageOrchestratorConfig;
use crate::domain::model::MessageSubmission;
use crate::domain::repository::WalRepository;

#[derive(Serialize, Deserialize)]
struct WalEntrySnapshot {
    message_id: String,
    encoded: String,
    persisted: bool,
}

#[derive(Debug)]
pub struct RedisWalRepository {
    client: Arc<redis::Client>,
    config: Arc<MessageOrchestratorConfig>,
}

impl RedisWalRepository {
    pub fn new(client: Arc<redis::Client>, config: Arc<MessageOrchestratorConfig>) -> Self {
        Self { client, config }
    }

    async fn connection(&self) -> Result<ConnectionManager> {
        let manager = self
            .client
            .get_connection_manager()
            .await
            .map_err(|e| crate::error::FlareError::system(e.to_string()))?;
        Ok(manager)
    }

    fn decode_wal_entry(
        entry_json: &str,
        message_id: &str,
    ) -> Result<Option<flare_proto::common::Message>> {
        // 反序列化 WalEntrySnapshot
        let entry: WalEntrySnapshot = serde_json::from_str(entry_json)
            .map_err(|e| crate::error::FlareError::system(format!("Failed to deserialize WAL entry: {}", e)))?;
        
        // 解码 base64 编码的 payload
        let payload_bytes = BASE64.decode(&entry.encoded)
            .map_err(|e| crate::error::FlareError::system(format!("Failed to decode base64 payload from WAL: {}", e)))?;
        
        let message = flare_proto::common::Message::decode(&payload_bytes[..])
            .map_err(|e| crate::error::FlareError::system(format!("Failed to decode Message from WAL: {}", e)))?;
        tracing::debug!(
            message_id = %message_id,
            sender_id = %message.sender_id,
            "Decoded message from WAL entry"
        );
        Ok(Some(message))
    }
}

impl WalRepository for RedisWalRepository {
    fn append<'a>(
        &'a self,
        submission: &'a MessageSubmission,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        let _self = self; // 保持对 self 的引用
        let _submission = submission; // 保持对 submission 的引用
        Box::pin(async move {
            let wal_key = match &_self.config.wal_hash_key {
                Some(key) => key.as_str(),
                None => {
                    tracing::debug!(
                        message_id = %_submission.message_id,
                        "WAL not configured (wal_hash_key is None), skipping WAL write"
                    );
                    return Ok(());
                }
            };

            let mut conn = _self.connection().await?;

            // 使用 message.server_id 作为 WAL key（确保与查询时一致）
            // 注意：submission.message_id 应该等于 submission.message.server_id，但为了安全起见，直接使用 message.server_id
            let wal_message_id = _submission.message.server_id.clone();
            
            let encoded_payload = BASE64.encode(_submission.kafka_payload.clone().encode_to_vec());
            let entry = WalEntrySnapshot {
                message_id: wal_message_id.clone(),
                encoded: encoded_payload,
                persisted: false,
            };

            let payload = serde_json::to_string(&entry)?;
            conn.hset::<_, _, _, ()>(wal_key, &wal_message_id, payload)
                .await
                .map_err(|e| crate::error::FlareError::system(format!("Redis hset error: {}", e)))?;

            if _self.config.wal_ttl_seconds > 0 {
                let _: () = conn
                    .expire(wal_key, _self.config.wal_ttl_seconds as i64)
                    .await
                    .map_err(|e| crate::error::FlareError::system(format!("Redis expire error: {}", e)))?;
            }

            tracing::debug!(
                message_id = %wal_message_id,
                submission_message_id = %_submission.message_id,
                wal_key = %wal_key,
                ttl_seconds = %_self.config.wal_ttl_seconds,
                "✅ WAL entry written successfully"
            );

            Ok(())
        })
    }

    fn find_by_message_id<'a>(
        &'a self,
        message_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<flare_proto::common::Message>>> + Send + 'a>> {
        let _self = self;
        let _message_id = message_id.to_string();
        Box::pin(async move {
            let wal_key = match &_self.config.wal_hash_key {
                Some(key) => key.as_str(),
                None => {
                    tracing::debug!(
                        message_id = %_message_id,
                        "WAL not configured (wal_hash_key is None), cannot query WAL"
                    );
                    return Ok(None);
                }
            };

            tracing::debug!(
                message_id = %_message_id,
                wal_key = %wal_key,
                "🔍 Querying WAL for message"
            );

            let mut conn = _self.connection().await.map_err(|e| crate::error::FlareError::system(format!("Redis connection error: {}", e)))?;

            // 首先尝试使用 message_id 直接查询（可能是服务端生成的 server_id）
            let entry_json: Option<String> = match conn.hget(wal_key, &_message_id).await {
                Ok(value) => value,
                Err(e) => return Err(crate::error::FlareError::system(format!("Redis hget error: {}", e))),
            };
            
            if let Some(json_str) = entry_json {
                tracing::debug!(
                    message_id = %_message_id,
                    "Found WAL entry by server_id, decoding..."
                );
                return Self::decode_wal_entry(&json_str, &_message_id);
            }

            // 如果直接查询不到，遍历 WAL 中的所有条目，查找 extra.original_server_id 等于 message_id 的条目
            // 注意：这可能会影响性能，但可以确保找到消息（即使使用的是客户端生成的 server_id）
            tracing::debug!(
                message_id = %_message_id,
                "WAL entry not found by server_id, searching by original_server_id..."
            );
            
            let all_entries: std::collections::HashMap<String, String> = conn.hgetall(wal_key).await
                .map_err(|e| crate::error::FlareError::system(format!("Redis hgetall error: {}", e)))?;
            
            for (wal_server_id, entry_json) in all_entries {
                if let Ok(Some(message)) = Self::decode_wal_entry(&entry_json, &wal_server_id) {
                    // 检查 extra.original_server_id 是否等于查询的 message_id
                    if let Some(original_server_id) = message.extra.get("original_server_id") {
                        if original_server_id == &_message_id {
                            tracing::info!(
                                query_message_id = %_message_id,
                                wal_server_id = %wal_server_id,
                                "Found WAL entry by original_server_id"
                            );
                            return Ok(Some(message));
                        }
                    }
                }
            }

            tracing::debug!(
                message_id = %_message_id,
                wal_key = %wal_key,
                "WAL entry not found in Redis (searched by both server_id and original_server_id)"
            );
            Ok(None)
        })
    }
}
