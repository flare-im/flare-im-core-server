use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use flare_server_core::error::Result;
use redis::{AsyncCommands, aio::ConnectionManager};
use serde::{Deserialize, Serialize};

use crate::config::OnlineConfig;
use crate::domain::aggregate::Connection;
use crate::domain::model::OnlineStatusRecord;
use crate::domain::repository::{ConversationRepository, tenant_session_member};
use crate::domain::value_object::{ConnectionId, DeviceId, DevicePriority, TokenVersion, UserId};
use flare_server_core::context::Context as SrvContext;

const CONNECTION_KEY_PREFIX: &str = "session";
/// 租户在线索引：`tenant:sessions:{tenant}` = SET of `{user}:{device}`，TTL 与 `session:{user}` 同步续期。
const TENANT_SESSIONS_KEY_PREFIX: &str = "tenant:sessions";

fn default_tenant_id() -> String {
    flare_im_contracts::utils::DEFAULT_TENANT_ID.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RedisConnectionRecord {
    conversation_id: String,
    gateway_id: String,
    server_id: String,
    device_id: String,
    device_platform: String,
    last_seen: i64,
    device_priority: i32,
    token_version: i64,
    /// 老记录没有这个字段：按默认租户 "0" 读。
    #[serde(default = "default_tenant_id")]
    tenant_id: String,
}

pub struct RedisConversationRepository {
    client: Arc<redis::Client>,
    /// kick 频道所在实例（token_store profile），与 signaling-gateway 的订阅端一致。
    kick_client: Arc<redis::Client>,
    config: Arc<OnlineConfig>,
}

impl RedisConversationRepository {
    pub fn new(client: Arc<redis::Client>, config: Arc<OnlineConfig>) -> Self {
        let kick_client = if config.kick_redis_url == config.redis_url {
            client.clone()
        } else {
            match redis::Client::open(config.kick_redis_url.as_str()) {
                Ok(kick_client) => Arc::new(kick_client),
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "invalid kick redis url; KickTenant will publish on the session redis instead"
                    );
                    client.clone()
                }
            }
        };
        Self {
            client,
            kick_client,
            config,
        }
    }

    fn connection_key(&self, user_id: &str) -> String {
        format!("{}:{}", CONNECTION_KEY_PREFIX, user_id)
    }

    fn tenant_sessions_key(tenant_id: &str) -> String {
        format!("{}:{}", TENANT_SESSIONS_KEY_PREFIX, tenant_id)
    }

    fn ttl(&self) -> i64 {
        self.config.redis_ttl_seconds as i64
    }

    /// 会话删除后同步索引：同一 (user, device) 若还有其他会话则保留成员。
    fn unindex_if_last(
        pipe: &mut redis::Pipeline,
        user_id: &str,
        remaining: &[RedisConnectionRecord],
        removed: &RedisConnectionRecord,
    ) {
        let still_online = remaining.iter().any(|r| {
            r.device_id == removed.device_id && r.conversation_id != removed.conversation_id
        });
        if !still_online {
            pipe.srem(
                Self::tenant_sessions_key(&removed.tenant_id),
                tenant_session_member(user_id, &removed.device_id),
            )
            .ignore();
        }
    }

    async fn connection(&self) -> Result<ConnectionManager> {
        ConnectionManager::new(self.client.as_ref().clone())
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("redis connection: {}", e))
            })
    }

    fn to_timestamp(seconds: i64) -> Option<DateTime<Utc>> {
        Utc.timestamp_opt(seconds, 0).single()
    }

    fn record_from_connection(session: &Connection) -> RedisConnectionRecord {
        RedisConnectionRecord {
            tenant_id: session.tenant_id().to_string(),
            conversation_id: session.id().as_str().to_string(),
            gateway_id: session.gateway_id().to_string(),
            server_id: session.server_id().to_string(),
            device_id: session.device_id().as_str().to_string(),
            device_platform: session.device_platform().to_string(),
            last_seen: session.last_heartbeat_at().timestamp(),
            device_priority: session.device_priority().as_i32(),
            token_version: session.token_version().value(),
        }
    }

    fn parse_record(payload: &str) -> Result<RedisConnectionRecord> {
        serde_json::from_str(payload).map_err(|e| {
            flare_server_core::error::FlareError::system(format!("invalid session payload: {}", e))
        })
    }

    fn records_from_values(values: HashMap<String, String>) -> Result<Vec<RedisConnectionRecord>> {
        values
            .into_values()
            .map(|payload| Self::parse_record(&payload))
            .collect()
    }

    fn connection_from_record(
        user_id: &UserId,
        record: RedisConnectionRecord,
    ) -> Result<Connection> {
        let conversation_id = ConnectionId::from_string(record.conversation_id)
            .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))?;
        let device_id = DeviceId::new(record.device_id)
            .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))?;
        let last_seen = Self::to_timestamp(record.last_seen).ok_or_else(|| {
            flare_server_core::error::FlareError::system("invalid last_seen timestamp".to_string())
        })?;

        Ok(Connection::reconstitute(
            conversation_id,
            user_id.clone(),
            device_id,
            record.tenant_id,
            record.device_platform,
            record.server_id,
            record.gateway_id,
            DevicePriority::from_i32(record.device_priority),
            TokenVersion::from(record.token_version),
            None,
            last_seen,
            last_seen,
        ))
    }

    async fn load_user_records(
        &self,
        conn: &mut ConnectionManager,
        user_id: &str,
    ) -> Result<Vec<RedisConnectionRecord>> {
        let key = self.connection_key(user_id);
        let values: HashMap<String, String> = conn.hgetall(&key).await.map_err(|e| {
            flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
        })?;
        Self::records_from_values(values)
    }
}

impl ConversationRepository for RedisConversationRepository {
    async fn save_connection(&self, session: &Connection) -> Result<()> {
        let mut conn = self.connection().await?;
        let key = self.connection_key(session.user_id().as_str());
        let record = Self::record_from_connection(session);
        let value = serde_json::to_string(&record).map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "serialize session payload: {}",
                e
            ))
        })?;
        let tenant_key = Self::tenant_sessions_key(&record.tenant_id);
        let ttl = self.ttl();
        // 会话哈希与租户索引同一管道写入、同一 TTL：索引永远不比会话活得久。
        let _: () = redis::pipe()
            .atomic()
            .hset(&key, session.id().as_str(), value)
            .ignore()
            .expire(&key, ttl)
            .ignore()
            .sadd(
                &tenant_key,
                tenant_session_member(session.user_id().as_str(), &record.device_id),
            )
            .ignore()
            .expire(&tenant_key, ttl)
            .ignore()
            .query_async(&mut conn)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;
        Ok(())
    }

    async fn remove_connection(
        &self,
        conversation_id: &ConnectionId,
        user_id: &UserId,
    ) -> Result<()> {
        let mut conn = self.connection().await?;
        let key = self.connection_key(user_id.as_str());
        let records = self.load_user_records(&mut conn, user_id.as_str()).await?;
        let mut pipe = redis::pipe();
        pipe.atomic().hdel(&key, conversation_id.as_str()).ignore();
        if let Some(removed) = records
            .iter()
            .find(|r| r.conversation_id == conversation_id.as_str())
        {
            Self::unindex_if_last(&mut pipe, user_id.as_str(), &records, removed);
        }
        let _: () = pipe.query_async(&mut conn).await.map_err(|e| {
            flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
        })?;
        tracing::info!(conversation_id = %conversation_id.as_ref(), user_id = %user_id.as_ref(), "session removed from redis");
        Ok(())
    }

    async fn touch_connection(
        &self,
        conversation_id: &ConnectionId,
        user_id: &UserId,
    ) -> Result<()> {
        let mut conn = self.connection().await?;
        let key = self.connection_key(user_id.as_str());
        let payload = conn
            .hget::<_, _, Option<String>>(&key, conversation_id.as_str())
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?
            .ok_or_else(|| {
                flare_server_core::error::FlareError::system("connection not found".to_string())
            })?;
        let mut record = Self::parse_record(&payload)?;
        record.last_seen = Utc::now().timestamp();
        let value = serde_json::to_string(&record).map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "serialize session payload: {}",
                e
            ))
        })?;
        let ttl = self.ttl();
        // 心跳同时给租户索引续期，否则索引会先于活跃会话过期。
        let _: () = redis::pipe()
            .atomic()
            .hset(&key, conversation_id.as_str(), value)
            .ignore()
            .expire(&key, ttl)
            .ignore()
            .expire(Self::tenant_sessions_key(&record.tenant_id), ttl)
            .ignore()
            .query_async(&mut conn)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;
        Ok(())
    }

    async fn fetch_statuses(
        &self,
        user_ids: &[String],
    ) -> Result<HashMap<String, OnlineStatusRecord>> {
        if user_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut conn = self.connection().await?;
        let mut pipe = redis::pipe();
        for user_id in user_ids {
            pipe.cmd("HGETALL").arg(self.connection_key(user_id));
        }

        let records_by_user: Vec<HashMap<String, String>> =
            pipe.query_async(&mut conn).await.map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "fetch statuses pipeline failed: {}",
                    e
                ))
            })?;

        let mut result = HashMap::new();
        for (user_id, values) in user_ids.iter().zip(records_by_user) {
            let records = Self::records_from_values(values)?;
            if let Some(latest) = records.into_iter().max_by_key(|record| record.last_seen) {
                let last_seen = Self::to_timestamp(latest.last_seen);
                result.insert(
                    user_id.clone(),
                    OnlineStatusRecord {
                        online: true,
                        server_id: latest.server_id,
                        gateway_id: Some(latest.gateway_id),
                        cluster_id: None,
                        last_seen,
                        device_id: Some(latest.device_id),
                        device_platform: Some(latest.device_platform),
                    },
                );
            }
        }

        Ok(result)
    }

    async fn get_user_connections(&self, user_id: &UserId) -> Result<Vec<Connection>> {
        let mut conn = self.connection().await?;
        self.load_user_records(&mut conn, user_id.as_str())
            .await?
            .into_iter()
            .map(|record| Self::connection_from_record(user_id, record))
            .collect()
    }

    async fn remove_user_connections(
        &self,
        user_id: &UserId,
        device_ids: Option<&[DeviceId]>,
    ) -> Result<()> {
        let mut conn = self.connection().await?;
        let key = self.connection_key(user_id.as_str());
        let records = self.load_user_records(&mut conn, user_id.as_str()).await?;
        let mut pipe = redis::pipe();
        pipe.atomic();

        if let Some(device_ids) = device_ids {
            for record in records
                .iter()
                .filter(|r| device_ids.iter().any(|d| d.as_str() == r.device_id))
            {
                pipe.hdel(&key, &record.conversation_id).ignore();
                pipe.srem(
                    Self::tenant_sessions_key(&record.tenant_id),
                    tenant_session_member(user_id.as_str(), &record.device_id),
                )
                .ignore();
            }
        } else {
            pipe.del(&key).ignore();
            for record in &records {
                pipe.srem(
                    Self::tenant_sessions_key(&record.tenant_id),
                    tenant_session_member(user_id.as_str(), &record.device_id),
                )
                .ignore();
            }
        }

        let _: () = pipe.query_async(&mut conn).await.map_err(|e| {
            flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
        })?;

        Ok(())
    }

    async fn get_connection_by_device(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<Option<Connection>> {
        let sessions = self.get_user_connections(user_id).await?;
        Ok(sessions
            .into_iter()
            .find(|s| s.device_id().as_str() == device_id.as_str()))
    }

    async fn list_user_connections(&self, ctx: &SrvContext) -> Result<Vec<Connection>> {
        let user_id = ctx.user_id().ok_or_else(|| {
            flare_server_core::error::FlareError::system(
                "user_id is required in context".to_string(),
            )
        })?;
        let user_id_vo = UserId::new(user_id.to_string())
            .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))?;
        self.get_user_connections(&user_id_vo).await
    }

    async fn scan_tenant_sessions(
        &self,
        tenant_id: &str,
        cursor: u64,
        count: usize,
    ) -> Result<(u64, Vec<String>)> {
        let mut conn = self.connection().await?;
        let (next, members): (u64, Vec<String>) = redis::cmd("SSCAN")
            .arg(Self::tenant_sessions_key(tenant_id))
            .arg(cursor)
            .arg("COUNT")
            .arg(count.max(1))
            .query_async(&mut conn)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;
        Ok((next, members))
    }

    async fn unindex_tenant_session(&self, tenant_id: &str, member: &str) -> Result<()> {
        let mut conn = self.connection().await?;
        let _: usize = conn
            .srem(Self::tenant_sessions_key(tenant_id), member)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;
        Ok(())
    }

    async fn broadcast_user_kick(&self, user_id: &str) -> Result<()> {
        let mut conn = ConnectionManager::new(self.kick_client.as_ref().clone())
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("redis connection: {}", e))
            })?;
        let _: () = conn
            .publish(&self.config.kick_channel, user_id)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_record_without_tenant_reads_as_default_tenant() {
        let payload = r#"{"conversation_id":"c1","gateway_id":"g","server_id":"s","device_id":"d1","device_platform":"ios","last_seen":1,"device_priority":2,"token_version":0}"#;
        let record = RedisConversationRepository::parse_record(payload).unwrap();
        assert_eq!(record.tenant_id, "0");

        let with_tenant = r#"{"conversation_id":"c1","gateway_id":"g","server_id":"s","device_id":"d1","device_platform":"ios","last_seen":1,"device_priority":2,"token_version":0,"tenant_id":"acme"}"#;
        assert_eq!(
            RedisConversationRepository::parse_record(with_tenant)
                .unwrap()
                .tenant_id,
            "acme"
        );
    }

    #[test]
    fn tenant_index_key_and_member_shape() {
        assert_eq!(
            RedisConversationRepository::tenant_sessions_key("acme"),
            "tenant:sessions:acme"
        );
        assert_eq!(tenant_session_member("u1", "d1"), "u1:d1");
    }
}

/// 在线状态发布者实现（基于 Redis Pub/Sub）
pub struct RedisPresencePublisher {
    client: Arc<redis::Client>,
}

impl RedisPresencePublisher {
    pub fn new(client: Arc<redis::Client>) -> Self {
        Self { client }
    }

    async fn connection(&self) -> Result<ConnectionManager> {
        ConnectionManager::new(self.client.as_ref().clone())
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("redis connection: {}", e))
            })
    }

    fn presence_channel(user_id: &str) -> String {
        format!("presence:{}", user_id)
    }
}

impl crate::domain::repository::PresencePublisher for RedisPresencePublisher {
    async fn publish_presence_event(
        &self,
        event: flare_grpc_proto::signaling::online::PresenceEvent,
    ) -> Result<()> {
        let mut conn = self.connection().await?;
        let payload = prost::Message::encode_to_vec(&event);
        let _: () = conn
            .publish("presence_events", payload)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;
        if !event.user_id.trim().is_empty() {
            let status = event.status.as_ref();
            let last_seen = status
                .and_then(|s| s.last_seen.as_ref())
                .map(|ts| ts.seconds)
                .unwrap_or_else(|| chrono::Utc::now().timestamp());
            let occurred_at = event
                .occurred_at
                .as_ref()
                .map(|ts| ts.seconds)
                .unwrap_or_else(|| chrono::Utc::now().timestamp());
            let value = serde_json::json!({
                "online": status.map(|s| s.online).unwrap_or(false),
                "server_id": status.map(|s| s.server_id.as_str()).unwrap_or_default(),
                "cluster_id": status.map(|s| s.cluster_id.as_str()).unwrap_or_default(),
                "last_seen": last_seen,
                "device_id": status.map(|s| s.device_id.as_str()).unwrap_or_default(),
                "device_platform": status.map(|s| s.device_platform.as_str()).unwrap_or_default(),
                "gateway_id": status.map(|s| s.gateway_id.as_str()).unwrap_or_default(),
                "occurred_at": occurred_at,
                "conflict_action": event.conflict_action,
                "reason": event.reason.clone(),
            });
            let _: () = conn
                .publish(Self::presence_channel(&event.user_id), value.to_string())
                .await
                .map_err(|e| {
                    flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
                })?;
        }
        Ok(())
    }

    async fn publish_user_presence_event(
        &self,
        event: flare_grpc_proto::signaling::online::UserPresenceEvent,
    ) -> Result<()> {
        let mut conn = self.connection().await?;
        let payload = prost::Message::encode_to_vec(&event);
        let _: () = conn
            .publish("user_presence_events", payload)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;
        Ok(())
    }
}
