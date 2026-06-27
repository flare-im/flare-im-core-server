use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use flare_server_core::error::Result;
use redis::{AsyncCommands, aio::ConnectionManager};
use serde::{Deserialize, Serialize};

use crate::config::OnlineConfig;
use crate::domain::aggregate::Connection;
use crate::domain::model::OnlineStatusRecord;
use crate::domain::repository::ConversationRepository;
use crate::domain::value_object::{ConnectionId, DeviceId, DevicePriority, TokenVersion, UserId};
use flare_server_core::context::Context as SrvContext;

const CONNECTION_KEY_PREFIX: &str = "session";

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
}

pub struct RedisConversationRepository {
    client: Arc<redis::Client>,
    config: Arc<OnlineConfig>,
}

impl RedisConversationRepository {
    pub fn new(client: Arc<redis::Client>, config: Arc<OnlineConfig>) -> Self {
        Self { client, config }
    }

    fn connection_key(&self, user_id: &str) -> String {
        format!("{}:{}", CONNECTION_KEY_PREFIX, user_id)
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
        let _: usize = conn
            .hset(&key, session.id().as_str(), value)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;
        let _: bool = conn
            .expire(&key, self.config.redis_ttl_seconds as i64)
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
        let _: usize = conn
            .hdel(&key, conversation_id.as_str())
            .await
            .map_err(|e| {
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
        let _: usize = conn
            .hset(&key, conversation_id.as_str(), value)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;
        let _: bool = conn
            .expire(&key, self.config.redis_ttl_seconds as i64)
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

        if let Some(device_ids) = device_ids {
            let values: HashMap<String, String> = conn.hgetall(&key).await.map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;

            for (conversation_id, payload) in values {
                let record = Self::parse_record(&payload)?;
                if device_ids.iter().any(|d| d.as_str() == record.device_id) {
                    let _: usize = conn.hdel(&key, conversation_id).await.map_err(|e| {
                        flare_server_core::error::FlareError::system(format!(
                            "operation failed: {}",
                            e
                        ))
                    })?;
                }
            }
        } else {
            let _: usize = conn.del(&key).await.map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;
        }

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
}

/// Redis 订阅仓库实现
pub struct RedisSubscriptionRepository {
    client: Arc<redis::Client>,
    config: Arc<OnlineConfig>,
}

impl RedisSubscriptionRepository {
    pub fn new(client: Arc<redis::Client>, config: Arc<OnlineConfig>) -> Self {
        Self { client, config }
    }

    fn subscription_key(&self, user_id: &str, topic: &str) -> String {
        format!("subscription:{}:{}", user_id, topic)
    }

    fn topic_subscribers_key(&self, topic: &str) -> String {
        format!("topic_subscribers:{}", topic)
    }

    async fn connection(&self) -> Result<ConnectionManager> {
        ConnectionManager::new(self.client.as_ref().clone())
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("redis connection: {}", e))
            })
    }
}

impl crate::domain::repository::SubscriptionRepository for RedisSubscriptionRepository {
    async fn add_subscription(&self, user_id: String, topic: String) -> Result<()> {
        let mut conn = self.connection().await?;
        let key = self.subscription_key(&user_id, &topic);
        let _: () = conn.set(&key, "1").await.map_err(|e| {
            flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
        })?;
        let _: bool = conn
            .expire(&key, self.config.redis_ttl_seconds as i64)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;

        // 添加到主题订阅者集合
        let topic_key = self.topic_subscribers_key(&topic);
        let _: () = conn.sadd(&topic_key, &user_id).await.map_err(|e| {
            flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
        })?;
        let _: bool = conn
            .expire(&topic_key, self.config.redis_ttl_seconds as i64)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;

        Ok(())
    }

    async fn remove_subscription(
        &self,
        ctx: &flare_server_core::context::Context,
        topics: &[String],
    ) -> Result<()> {
        let user_id = ctx.user_id().ok_or_else(|| {
            flare_server_core::error::FlareError::system(
                "user_id is required in context".to_string(),
            )
        })?;
        let mut conn = self.connection().await?;

        for topic in topics {
            let key = self.subscription_key(user_id, topic);
            let _: usize = conn.del(&key).await.map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;

            // 从主题订阅者集合中移除
            let topic_key = self.topic_subscribers_key(topic);
            let _: () = conn.srem(&topic_key, user_id).await.map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;
        }

        Ok(())
    }

    async fn get_user_subscriptions(
        &self,
        ctx: &flare_server_core::context::Context,
    ) -> Result<Vec<(String, HashMap<String, String>)>> {
        let user_id = ctx.user_id().ok_or_else(|| {
            flare_server_core::error::FlareError::system(
                "user_id is required in context".to_string(),
            )
        })?;
        let mut conn = self.connection().await?;

        // 查找该用户的所有订阅键
        let pattern = format!("subscription:{}:*", user_id);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut conn)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
            })?;

        let mut subscriptions = Vec::new();
        for key in keys {
            // 从键中提取主题名称
            if let Some(topic) = key.strip_prefix(&format!("subscription:{}:", user_id)) {
                subscriptions.push((topic.to_string(), HashMap::new()));
            }
        }

        Ok(subscriptions)
    }

    async fn get_topic_subscribers(&self, topic: &str) -> Result<Vec<String>> {
        let mut conn = self.connection().await?;
        let key = self.topic_subscribers_key(topic);
        let subscribers: Vec<String> = conn.smembers(&key).await.map_err(|e| {
            flare_server_core::error::FlareError::system(format!("operation failed: {}", e))
        })?;
        Ok(subscribers)
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
