use std::sync::Arc;

use flare_server_core::error::ErrorCode;
use flare_server_core::error::Result;
use futures_util::StreamExt;
use tokio::sync::mpsc;

use crate::config::OnlineConfig;
use crate::domain::model::OnlineStatusRecord;
use crate::domain::repository::{PresenceChangeEvent, PresenceWatcher};

const PRESENCE_CHANNEL_PREFIX: &str = "presence";

/// Redis 实现的在线状态监听器。
pub struct RedisPresenceWatcher {
    client: Arc<redis::Client>,
    _config: Arc<OnlineConfig>,
}

impl RedisPresenceWatcher {
    pub fn new(client: Arc<redis::Client>, config: Arc<OnlineConfig>) -> Self {
        Self {
            client,
            _config: config,
        }
    }

    fn presence_channel(&self, user_id: &str) -> String {
        format!("{}:{}", PRESENCE_CHANNEL_PREFIX, user_id)
    }

    fn parse_presence_event(user_id: &str, payload: &str) -> Result<PresenceChangeEvent> {
        use serde_json::Value;

        let json: Value = serde_json::from_str(payload).map_err(|e| {
            flare_server_core::flare_err!(
                ErrorCode::InternalError,
                format!("Failed to parse presence event: {}", e)
            )
        })?;

        let status = OnlineStatusRecord {
            online: json
                .get("online")
                .and_then(|v| v.as_bool())
                .ok_or_else(|| {
                    flare_server_core::flare_err!(
                        ErrorCode::InternalError,
                        "presence event missing online"
                    )
                })?,
            server_id: json
                .get("server_id")
                .and_then(|v: &Value| v.as_str())
                .map(|s: &str| s.to_string())
                .unwrap_or_default(),
            gateway_id: json
                .get("gateway_id")
                .and_then(|v: &Value| v.as_str())
                .map(|s: &str| s.to_string()),
            cluster_id: json
                .get("cluster_id")
                .and_then(|v: &Value| v.as_str())
                .map(|s: &str| s.to_string()),
            last_seen: json
                .get("last_seen")
                .and_then(|v: &Value| v.as_i64())
                .and_then(|ts: i64| chrono::DateTime::from_timestamp(ts, 0)),
            device_id: json
                .get("device_id")
                .and_then(|v: &Value| v.as_str())
                .map(|s: &str| s.to_string()),
            device_platform: json
                .get("device_platform")
                .and_then(|v: &Value| v.as_str())
                .map(|s: &str| s.to_string()),
        };

        Ok(PresenceChangeEvent {
            user_id: user_id.to_string(),
            status,
            occurred_at: json
                .get("occurred_at")
                .and_then(|v: &Value| v.as_i64())
                .and_then(|ts: i64| chrono::DateTime::from_timestamp(ts, 0))
                .ok_or_else(|| {
                    flare_server_core::flare_err!(
                        ErrorCode::InternalError,
                        "presence event missing occurred_at"
                    )
                })?,
            conflict_action: json.get("conflict_action").and_then(|v: &Value| {
                v.as_i64().and_then(|i: i64| {
                    if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                        Some(i as i32)
                    } else {
                        None
                    }
                })
            }),
            reason: json
                .get("reason")
                .and_then(|v: &Value| v.as_str())
                .map(|s: &str| s.to_string()),
        })
    }
}

impl PresenceWatcher for RedisPresenceWatcher {
    async fn watch_presence(
        &self,
        user_ids: &[String],
    ) -> Result<mpsc::Receiver<Result<PresenceChangeEvent>>> {
        let (tx, rx) = mpsc::channel(100);
        let client = self.client.clone();
        let channels: Vec<String> = user_ids
            .iter()
            .map(|id| id.trim())
            .filter(|id| !id.is_empty())
            .map(|id| self.presence_channel(id))
            .collect();

        tokio::spawn(async move {
            if channels.is_empty() {
                return;
            }
            let mut pubsub = match client.get_async_pubsub().await {
                Ok(pubsub) => pubsub,
                Err(err) => {
                    let _ = tx
                        .send(Err(flare_server_core::error::FlareError::system(format!(
                            "redis pubsub: {err}"
                        ))))
                        .await;
                    return;
                }
            };
            for channel in &channels {
                if let Err(err) = pubsub.subscribe(channel).await {
                    let _ = tx
                        .send(Err(flare_server_core::error::FlareError::system(format!(
                            "redis subscribe channel={channel}: {err}"
                        ))))
                        .await;
                    return;
                }
            }

            let mut messages = pubsub.on_message();
            while let Some(msg) = messages.next().await {
                let channel = msg.get_channel_name().to_string();
                let Some(user_id) = channel.strip_prefix(&format!("{PRESENCE_CHANNEL_PREFIX}:"))
                else {
                    continue;
                };
                let payload = match msg.get_payload::<String>() {
                    Ok(payload) => payload,
                    Err(err) => {
                        let _ = tx
                            .send(Err(flare_server_core::error::FlareError::system(format!(
                                "redis pubsub payload: {err}"
                            ))))
                            .await;
                        continue;
                    }
                };
                let event = Self::parse_presence_event(user_id, &payload);
                if tx.send(event).await.is_err() {
                    break;
                }
            }
        });

        Ok(rx)
    }
}
