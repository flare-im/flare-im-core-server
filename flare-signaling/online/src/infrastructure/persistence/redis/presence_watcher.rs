use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::mpsc;

use crate::config::OnlineConfig;
use crate::domain::model::OnlineStatusRecord;
use crate::domain::repository::{PresenceChangeEvent, PresenceWatcher};

const PRESENCE_CHANNEL_PREFIX: &str = "presence";

/// Redis 实现的在线状态监听器
/// 注意：这是一个简化的实现，实际生产环境可以使用 Redis Streams 或专门的 Pub/Sub 服务
pub struct RedisPresenceWatcher {
    _client: Arc<redis::Client>,
    _config: Arc<OnlineConfig>,
}

impl RedisPresenceWatcher {
    pub fn new(client: Arc<redis::Client>, config: Arc<OnlineConfig>) -> Self {
        Self {
            _client: client,
            _config: config,
        }
    }

    #[allow(dead_code)]
    fn presence_channel(&self, user_id: &str) -> String {
        format!("{}:{}", PRESENCE_CHANNEL_PREFIX, user_id)
    }

    #[allow(dead_code)]
    fn parse_presence_event(user_id: &str, payload: &str) -> Result<PresenceChangeEvent> {
        use serde_json::Value;

        let json: Value =
            serde_json::from_str(payload).context("failed to parse presence event")?;

        let status = OnlineStatusRecord {
            online: json
                .get("online")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            server_id: json
                .get("server_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_default(),
            gateway_id: json
                .get("gateway_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            cluster_id: json
                .get("cluster_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            last_seen: json
                .get("last_seen")
                .and_then(|v| v.as_i64())
                .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0)),
            device_id: json
                .get("device_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            device_platform: json
                .get("device_platform")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        };

        Ok(PresenceChangeEvent {
            user_id: user_id.to_string(),
            status,
            occurred_at: json
                .get("occurred_at")
                .and_then(|v| v.as_i64())
                .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0))
                .unwrap_or_else(chrono::Utc::now),
            conflict_action: json.get("conflict_action").and_then(|v| {
                v.as_i64().and_then(|i| {
                    if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                        Some(i as i32)
                    } else {
                        None
                    }
                })
            }),
            reason: json
                .get("reason")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        })
    }
}

impl PresenceWatcher for RedisPresenceWatcher {
    async fn watch_presence(
        &self,
        _user_ids: &[String],
    ) -> Result<mpsc::Receiver<Result<PresenceChangeEvent>>> {
        let (_tx, rx) = mpsc::channel(100);

        // 简化实现：定期轮询检查在线状态变化
        // 实际生产环境应该使用 Redis Pub/Sub 或 Streams 实现真正的实时推送
        //
        // 当前实现返回一个空的接收器作为占位符
        // 实际的在线状态变化应该通过 Redis Pub/Sub 发布，这里接收并转发
        //
        // 未来改进：
        // 1. 使用 redis::aio::PubSub 订阅 Redis Pub/Sub 频道
        // 2. 监听 presence:{user_id} 频道的消息
        // 3. 解析消息并转换为 PresenceChangeEvent
        // 4. 通过 tx 发送到接收器
        Ok(rx)
    }
}
