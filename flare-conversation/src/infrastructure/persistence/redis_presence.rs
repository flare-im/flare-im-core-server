use std::sync::Arc;

use anyhow::Result;
use chrono::{TimeZone, Utc};
use redis::{AsyncCommands, aio::ConnectionManager};

use crate::config::ConversationConfig;
use crate::domain::model::{DevicePresence, DeviceState};
use crate::domain::repository::{PresenceRepository, PresenceUpdate};

pub struct RedisPresenceRepository {
    client: Arc<redis::Client>,
    config: Arc<ConversationConfig>,
}

impl RedisPresenceRepository {
    pub fn new(client: Arc<redis::Client>, config: Arc<ConversationConfig>) -> Self {
        Self { client, config }
    }

    async fn connection(&self) -> Result<ConnectionManager> {
        Ok(ConnectionManager::new(self.client.as_ref().clone()).await?)
    }

    fn device_key(&self, user_id: &str, device_id: &str) -> String {
        format!("{}:{}:{}", self.config.presence_prefix, user_id, device_id)
    }

    fn device_pattern(&self, user_id: &str) -> String {
        format!("{}:{}:*", self.config.presence_prefix, user_id)
    }
}

impl PresenceRepository for RedisPresenceRepository {
    async fn list_devices(
        &self,
        ctx: &flare_server_core::context::Context,
        user_id: &str,
    ) -> Result<Vec<DevicePresence>> {
        let mut conn = self.connection().await?;
        let mut devices = Vec::new();
        let keys: Vec<String> = conn.keys(self.device_pattern(user_id)).await?;
        for key in keys {
            let map: std::collections::HashMap<String, String> = conn.hgetall(&key).await?;
            if let Some((_, device_id)) = key.rsplit_once(':') {
                let state = map
                    .get("state")
                    .map(|v| DeviceState::from_str(v.as_str()))
                    .unwrap_or(DeviceState::Unspecified);
                let last_seen_at = map
                    .get("last_seen_ts")
                    .and_then(|v| v.parse::<i64>().ok())
                    .and_then(|ts| Utc.timestamp_millis_opt(ts).single());
                let presence = DevicePresence {
                    device_id: device_id.to_string(),
                    device_platform: map.get("platform").cloned().filter(|v| !v.is_empty()),
                    state,
                    last_seen_at,
                };
                devices.push(presence);
            }
        }
        Ok(devices)
    }

    async fn update_presence(
        &self,
        ctx: &flare_server_core::context::Context,
        update: PresenceUpdate,
    ) -> Result<()> {
        let mut conn = self.connection().await?;
        let key = self.device_key(&update.user_id, &update.device_id);
        let now = Utc::now().timestamp_millis();
        let state = match update.state {
            DeviceState::Online => "online",
            DeviceState::Offline => "offline",
            DeviceState::Conflict => "conflict",
            DeviceState::Unspecified => "unknown",
        };

        let conflict_resolution = update
            .conflict_resolution
            .map(|c| c.as_str().to_string())
            .unwrap_or_default();
        let conflict_reason = update.conflict_reason.unwrap_or_default();

        let notify_conflict = if update.notify_conflict { "1" } else { "0" };

        let fields = vec![
            (
                "platform".to_string(),
                update.device_platform.clone().unwrap_or_default(),
            ),
            ("state".to_string(), state.to_string()),
            ("last_seen_ts".to_string(), now.to_string()),
            ("conflict_resolution".to_string(), conflict_resolution),
            ("notify_conflict".to_string(), notify_conflict.to_string()),
            ("conflict_reason".to_string(), conflict_reason),
        ];

        let field_refs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let _: () = conn.hset_multiple(&key, &field_refs).await?;

        Ok(())
    }
}
