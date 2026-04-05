use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use redis::{AsyncCommands, aio::ConnectionManager};
use serde_json::json;

use crate::config::OnlineConfig;
use crate::domain::aggregate::Connection;
use crate::domain::model::OnlineStatusRecord;
use crate::domain::repository::ConversationRepository;
use crate::domain::value_object::{
    ConnectionId, ConnectionQuality, DeviceId, DevicePriority, TokenVersion, UserId,
};
use flare_server_core::context::Context as SrvContext;

const CONNECTION_KEY_PREFIX: &str = "session";

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
            .map_err(|e| anyhow::anyhow!("redis connection: {}", e))
    }

    fn to_timestamp(seconds: i64) -> Option<DateTime<Utc>> {
        Utc.timestamp_opt(seconds, 0).single()
    }
}

impl ConversationRepository for RedisConversationRepository {
    async fn save_connection(&self, session: &Connection) -> Result<()> {
        let mut conn = self.connection().await?;
        let key = self.connection_key(session.user_id().as_str());
        let value = json!({
            "conversation_id": session.id().as_str(),
            "gateway_id": session.gateway_id(),
            "server_id": session.server_id(),
            "device_id": session.device_id().as_str(),
            "device_platform": session.device_platform(),
            "last_seen": session.last_heartbeat_at().timestamp(),
            "device_priority": session.device_priority().as_i32(),
            "token_version": session.token_version().value(),
        });
        let _: () = conn
            .set(&key, value.to_string())
            .await
            .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;
        let _: bool = conn
            .expire(&key, self.config.redis_ttl_seconds as i64)
            .await
            .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;
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
            .del(&key)
            .await
            .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;
        tracing::info!(conversation_id = %conversation_id.as_ref(), user_id = %user_id.as_ref(), "session removed from redis");
        Ok(())
    }

    async fn touch_connection(&self, user_id: &UserId) -> Result<()> {
        let mut conn = self.connection().await?;
        let key = self.connection_key(user_id.as_str());
        let _: bool = conn
            .expire(&key, self.config.redis_ttl_seconds as i64)
            .await
            .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;
        Ok(())
    }

    async fn fetch_statuses(
        &self,
        user_ids: &[String],
    ) -> Result<HashMap<String, OnlineStatusRecord>> {
        let mut conn = self.connection().await?;
        let mut result = HashMap::new();
        for user_id in user_ids {
            let key = self.connection_key(user_id.as_str());
            let value: Option<String> = conn
                .get(&key)
                .await
                .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;
            if let Some(payload) = value {
                let json: serde_json::Value = serde_json::from_str(&payload)
                    .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;
                let last_seen = json
                    .get("last_seen")
                    .and_then(|v| v.as_i64())
                    .and_then(Self::to_timestamp);
                result.insert(
                    user_id.clone(),
                    OnlineStatusRecord {
                        online: true,
                        server_id: json
                            .get("server_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        gateway_id: json
                            .get("gateway_id")
                            .and_then(|v| v.as_str())
                            .map(|v| v.to_string()),
                        cluster_id: None,
                        last_seen,
                        device_id: json
                            .get("device_id")
                            .and_then(|v| v.as_str())
                            .map(|v| v.to_string()),
                        device_platform: json
                            .get("device_platform")
                            .and_then(|v| v.as_str())
                            .map(|v| v.to_string()),
                    },
                );
            }
        }

        Ok(result)
    }

    async fn get_user_connections(&self, user_id: &UserId) -> Result<Vec<Connection>> {
        let mut conn = self.connection().await?;
        let key = self.connection_key(user_id.as_str());
        let value: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;

        if let Some(payload) = value {
            let json: serde_json::Value = serde_json::from_str(&payload)
                .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;

            let conversation_id_str = json
                .get("conversation_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let conversation_id =
                ConnectionId::from_string(conversation_id_str).map_err(|e| anyhow::anyhow!(e))?;

            let device_id_str = json
                .get("device_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let device_id = DeviceId::new(device_id_str).map_err(|e| anyhow::anyhow!(e))?;

            let device_platform = json
                .get("device_platform")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();

            let server_id = json
                .get("server_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();

            let gateway_id = json
                .get("gateway_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();

            let last_seen = json
                .get("last_seen")
                .and_then(|v| v.as_i64())
                .and_then(Self::to_timestamp)
                .unwrap_or_else(Utc::now);

            let created_at = last_seen;

            let device_priority_i32 = json
                .get("device_priority")
                .and_then(|v| v.as_i64())
                .unwrap_or(0) as i32;
            let device_priority = DevicePriority::from_i32(device_priority_i32);

            let token_version_i64 = json
                .get("token_version")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let token_version = TokenVersion::from(token_version_i64);

            let connection_quality: Option<ConnectionQuality> = None;

            let session = Connection::reconstitute(
                conversation_id,
                user_id.clone(),
                device_id,
                device_platform,
                server_id,
                gateway_id,
                device_priority,
                token_version,
                connection_quality,
                created_at,
                last_seen,
            );

            Ok(vec![session])
        } else {
            Ok(vec![])
        }
    }

    async fn remove_user_connections(
        &self,
        user_id: &UserId,
        device_ids: Option<&[DeviceId]>,
    ) -> Result<()> {
        let mut conn = self.connection().await?;
        let key = self.connection_key(user_id.as_str());

        // 如果指定了设备ID列表，需要检查设备是否匹配
        // 当前实现中，一个用户只有一个会话，所以直接删除
        // 未来如果需要支持多设备，可以扩展为Hash结构存储多个设备会话
        if let Some(device_ids) = device_ids {
            // 获取当前会话
            let value: Option<String> = conn
                .get(&key)
                .await
                .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;

            if let Some(payload) = value {
                let json: serde_json::Value = serde_json::from_str(&payload)
                    .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;

                let current_device_id = json
                    .get("device_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();

                // 只删除匹配的设备
                if device_ids.iter().any(|d| d.as_str() == current_device_id) {
                    let _: usize = conn
                        .del(&key)
                        .await
                        .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;
                }
            }
        } else {
            // 删除所有会话
            let _: usize = conn
                .del(&key)
                .await
                .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;
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
        let user_id = ctx
            .user_id()
            .ok_or_else(|| anyhow::anyhow!("user_id is required in context"))?;
        let user_id_vo = UserId::new(user_id.to_string()).map_err(|e| anyhow::anyhow!(e))?;
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
            .map_err(|e| anyhow::anyhow!("redis connection: {}", e))
    }
}

impl crate::domain::repository::SubscriptionRepository for RedisSubscriptionRepository {
    async fn add_subscription(&self, user_id: String, topic: String) -> Result<()> {
        let mut conn = self.connection().await?;
        let key = self.subscription_key(&user_id, &topic);
        let _: () = conn
            .set(&key, "1")
            .await
            .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;
        let _: bool = conn
            .expire(&key, self.config.redis_ttl_seconds as i64)
            .await
            .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;

        // 添加到主题订阅者集合
        let topic_key = self.topic_subscribers_key(&topic);
        let _: () = conn
            .sadd(&topic_key, &user_id)
            .await
            .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;
        let _: bool = conn
            .expire(&topic_key, self.config.redis_ttl_seconds as i64)
            .await
            .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;

        Ok(())
    }

    async fn remove_subscription(
        &self,
        ctx: &flare_server_core::context::Context,
        topics: &[String],
    ) -> Result<()> {
        let user_id = ctx
            .user_id()
            .ok_or_else(|| anyhow::anyhow!("user_id is required in context"))?;
        let mut conn = self.connection().await?;

        for topic in topics {
            let key = self.subscription_key(&user_id, topic);
            let _: usize = conn
                .del(&key)
                .await
                .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;

            // 从主题订阅者集合中移除
            let topic_key = self.topic_subscribers_key(topic);
            let _: () = conn
                .srem(&topic_key, &user_id)
                .await
                .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;
        }

        Ok(())
    }

    async fn get_user_subscriptions(
        &self,
        ctx: &flare_server_core::context::Context,
    ) -> Result<Vec<(String, HashMap<String, String>)>> {
        let user_id = ctx
            .user_id()
            .ok_or_else(|| anyhow::anyhow!("user_id is required in context"))?;
        let mut conn = self.connection().await?;

        // 查找该用户的所有订阅键
        let pattern = format!("subscription:{}:*", user_id);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut conn)
            .await
            .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;

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
        let subscribers: Vec<String> = conn
            .smembers(&key)
            .await
            .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;
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
            .map_err(|e| anyhow::anyhow!("redis connection: {}", e))
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
            .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;
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
            .map_err(|e| anyhow::anyhow!("operation failed: {}", e))?;
        Ok(())
    }
}
