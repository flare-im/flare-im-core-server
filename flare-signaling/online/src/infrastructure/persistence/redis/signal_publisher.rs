use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use redis::{AsyncCommands, aio::ConnectionManager};
use serde_json::json;

use crate::config::OnlineConfig;
use crate::domain::repository::SignalPublisher;

const SIGNAL_CHANNEL_PREFIX: &str = "signal";

pub struct RedisSignalPublisher {
    client: Arc<redis::Client>,
    _config: Arc<OnlineConfig>,
}

impl RedisSignalPublisher {
    pub fn new(client: Arc<redis::Client>, config: Arc<OnlineConfig>) -> Self {
        Self {
            client,
            _config: config,
        }
    }

    fn signal_channel(&self, topic: &str) -> String {
        format!("{}:{}", SIGNAL_CHANNEL_PREFIX, topic)
    }

    async fn connection(&self) -> Result<ConnectionManager> {
        ConnectionManager::new(self.client.as_ref().clone())
            .await
            .context("failed to open redis connection")
    }
}

impl SignalPublisher for RedisSignalPublisher {
    async fn publish_signal(
        &self,
        topic: &str,
        payload: &[u8],
        metadata: &HashMap<String, String>,
    ) -> Result<()> {
        let mut conn = self.connection().await?;
        let channel = self.signal_channel(topic);

        // 构建消息（使用 hex 编码二进制数据）
        use std::fmt::Write;
        let mut payload_hex = String::with_capacity(payload.len() * 2);
        for byte in payload {
            write!(&mut payload_hex, "{:02x}", byte)
                .map_err(|e| anyhow::anyhow!("Failed to encode payload hex: {}", e))?;
        }

        let message = json!({
            "payload": payload_hex,
            "metadata": metadata,
            "timestamp": chrono::Utc::now().timestamp(),
        });

        // 发布到 Redis Pub/Sub
        let _: i64 = conn
            .publish(&channel, message.to_string())
            .await
            .context("failed to publish signal")?;

        Ok(())
    }
}
