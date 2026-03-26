//! ACK状态Redis管理器
//! 实现基于Redis的ACK状态暂存机制，用于支持ACK重传判断和状态查询

use redis::{AsyncCommands, Client, RedisError, RedisResult};
use serde::{Deserialize, Serialize};

/// ACK状态信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckStatusInfo {
    /// 消息ID
    pub message_id: String,
    /// 用户ID
    pub user_id: String,
    /// ACK类型
    pub ack_type: Option<AckType>,
    /// ACK状态
    pub status: AckStatus,
    /// 时间戳
    pub timestamp: u64,
    /// 重要性等级
    pub importance: ImportanceLevel,
}

/// ACK类型枚举
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AckType {
    /// 传输层 ACK
    TransportAck,
    /// 服务器 ACK
    ServerAck,
    /// 送达 ACK
    DeliveryAck,
    /// 存储 ACK
    StorageAck,
}

/// ACK状态枚举
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AckStatus {
    /// 待处理
    Pending,
    /// 已接收
    Received,
    /// 已处理
    Processed,
    /// 失败
    Failed,
}

/// 重要性等级
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum ImportanceLevel {
    /// 低重要性 - 仅内存缓存
    Low = 1,
    /// 中等重要性 - 内存缓存+定期持久化
    Medium = 2,
    /// 高重要性 - 立即持久化
    High = 3,
}

/// Redis ACK管理器
pub struct RedisAckManager {
    /// Redis客户端
    pub client: Client,
    /// 默认过期时间（秒）
    default_ttl: u64,
}

impl RedisAckManager {
    /// 创建新的Redis ACK管理器
    pub fn new(redis_url: &str, default_ttl: u64) -> RedisResult<Self> {
        let client = Client::open(redis_url)?;
        Ok(Self {
            client,
            default_ttl,
        })
    }

    /// 存储ACK状态
    pub async fn store_ack_status(&self, ack_info: &AckStatusInfo) -> RedisResult<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.format_key(&ack_info.message_id, &ack_info.user_id);
        let value = serde_json::to_string(ack_info).map_err(|e| {
            RedisError::from((
                redis::ErrorKind::TypeError,
                "JSON serialization error",
                e.to_string(),
            ))
        })?;

        // 根据重要性等级设置不同的过期时间
        let ttl = match ack_info.importance {
            ImportanceLevel::Low => self.default_ttl / 4, // 低重要性，较短过期时间
            ImportanceLevel::Medium => self.default_ttl / 2, // 中等重要性，中等过期时间
            ImportanceLevel::High => self.default_ttl,    // 高重要性，较长过期时间
        };

        let _: () = conn.set_ex(&key, value, ttl).await?;
        Ok(())
    }

    /// 获取ACK状态
    pub async fn get_ack_status(
        &self,
        message_id: &str,
        user_id: &str,
    ) -> RedisResult<Option<AckStatusInfo>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.format_key(message_id, user_id);
        let value: Option<String> = conn.get(&key).await?;

        match value {
            Some(data) => {
                let ack_info: AckStatusInfo = serde_json::from_str(&data).map_err(|e| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "JSON deserialization error",
                        e.to_string(),
                    ))
                })?;
                Ok(Some(ack_info))
            }
            None => Ok(None),
        }
    }

    /// 批量存储ACK状态
    pub async fn batch_store_ack_status(&self, ack_infos: &[AckStatusInfo]) -> RedisResult<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let mut pipe = redis::pipe();

        for ack_info in ack_infos {
            let key = self.format_key(&ack_info.message_id, &ack_info.user_id);
            let value = serde_json::to_string(ack_info).map_err(|e| {
                RedisError::from((
                    redis::ErrorKind::TypeError,
                    "JSON serialization error",
                    e.to_string(),
                ))
            })?;

            // 根据重要性等级设置不同的过期时间
            let ttl = match ack_info.importance {
                ImportanceLevel::Low => self.default_ttl / 4,
                ImportanceLevel::Medium => self.default_ttl / 2,
                ImportanceLevel::High => self.default_ttl,
            };

            pipe.cmd("SETEX").arg(&key).arg(ttl).arg(value);
        }

        let _: () = pipe.query_async(&mut conn).await?;
        Ok(())
    }

    /// 删除ACK状态
    pub async fn delete_ack_status(&self, message_id: &str, user_id: &str) -> RedisResult<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.format_key(message_id, user_id);
        let _: () = conn.del(&key).await?;
        Ok(())
    }

    /// 检查ACK是否存在
    pub async fn exists_ack(&self, message_id: &str, user_id: &str) -> RedisResult<bool> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.format_key(message_id, user_id);
        let exists: bool = conn.exists(&key).await?;
        Ok(exists)
    }

    /// 格式化Redis键
    fn format_key(&self, message_id: &str, user_id: &str) -> String {
        format!("ack:{}:{}", message_id, user_id)
    }

    /// 扫描所有 ACK keys（使用 SCAN 命令，避免阻塞）
    ///
    /// 返回所有匹配 `ack:*:*` 模式的 keys
    ///
    /// # 参数
    /// - `max_keys`: 最大扫描 keys 数量（0 表示不限制）
    /// - `scan_batch_size`: 每次 SCAN 的 keys 数量
    pub async fn scan_all_ack_keys(
        &self,
        max_keys: Option<usize>,
        scan_batch_size: usize,
    ) -> RedisResult<Vec<String>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let mut keys = Vec::new();
        let mut cursor: u64 = 0;
        let pattern = "ack:*:*";
        let count = scan_batch_size;
        let max_keys = max_keys.unwrap_or(0);

        loop {
            let result: (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(count)
                .query_async(&mut conn)
                .await?;

            cursor = result.0;
            let batch_keys = result.1;

            // 如果设置了最大 keys 数量，限制扫描
            if max_keys > 0 {
                let remaining = max_keys.saturating_sub(keys.len());
                if remaining == 0 {
                    break;
                }
                keys.extend(batch_keys.into_iter().take(remaining));
            } else {
                keys.extend(batch_keys);
            }

            // 如果 cursor 为 0，表示扫描完成
            // 或者如果已达到最大 keys 数量，停止扫描
            if cursor == 0 || (max_keys > 0 && keys.len() >= max_keys) {
                break;
            }
        }

        Ok(keys)
    }

    /// 批量获取 ACK 状态（用于超时检查）
    ///
    /// 从 Redis keys 中批量获取 ACK 状态信息
    ///
    /// # 参数
    /// - `keys`: 要查询的 keys 列表
    /// - `batch_size`: Pipeline 批次大小（避免单个 Pipeline 太大）
    pub async fn batch_get_ack_status_from_keys(
        &self,
        keys: &[String],
        batch_size: usize,
    ) -> RedisResult<Vec<AckStatusInfo>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let mut ack_infos = Vec::new();

        // 分批处理，避免单个 Pipeline 太大
        for chunk in keys.chunks(batch_size) {
            // 使用 pipeline 批量获取
            let mut pipe = redis::pipe();
            pipe.atomic(); // 原子性执行

            for key in chunk {
                pipe.cmd("GET").arg(key);
            }

            let values: Vec<Option<String>> = pipe.query_async(&mut conn).await?;

            for data in values.into_iter().flatten() {
                match serde_json::from_str::<AckStatusInfo>(&data) {
                    Ok(ack_info) => ack_infos.push(ack_info),
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to deserialize ACK status from Redis");
                    }
                }
            }
        }

        Ok(ack_infos)
    }

    /// 获取统计信息
    pub async fn get_stats(&self) -> RedisResult<RedisStats> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let info: String = redis::cmd("INFO")
            .arg("memory")
            .query_async(&mut conn)
            .await?;

        // 解析Redis内存信息
        let mut used_memory = 0u64;
        let mut used_memory_peak = 0u64;

        for line in info.lines() {
            if line.starts_with("used_memory:") && let Some(value) = line.split(':').nth(1) {
                used_memory = value.parse().unwrap_or(0);
            } else if line.starts_with("used_memory_peak:") && let Some(value) = line.split(':').nth(1) {
                used_memory_peak = value.parse().unwrap_or(0);
            }
        }

        Ok(RedisStats {
            used_memory,
            used_memory_peak,
        })
    }
}

/// Redis统计信息
#[derive(Debug, Clone)]
pub struct RedisStats {
    /// 已使用内存（字节）
    pub used_memory: u64,
    /// 峰值内存使用（字节）
    pub used_memory_peak: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_ack_status_management() -> RedisResult<()> {
        // 注意：这需要一个运行中的Redis实例
        let manager = RedisAckManager::new("redis://127.0.0.1/", 3600)?;

        let ack_info = AckStatusInfo {
            message_id: "test_msg_1".to_string(),
            user_id: "user_1".to_string(),
            ack_type: Some(AckType::TransportAck),
            status: AckStatus::Received,
            timestamp: 1234567890,
            importance: ImportanceLevel::High,
        };

        // 存储ACK状态
        manager.store_ack_status(&ack_info).await?;

        // 获取ACK状态
        let retrieved = manager.get_ack_status("test_msg_1", "user_1").await?;
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.message_id, "test_msg_1");
        assert_eq!(retrieved.user_id, "user_1");
        assert_eq!(retrieved.status, AckStatus::Received);

        // 检查ACK是否存在
        let exists = manager.exists_ack("test_msg_1", "user_1").await?;
        assert!(exists);

        // 删除ACK状态
        manager.delete_ack_status("test_msg_1", "user_1").await?;

        // 确认已删除
        let exists = manager.exists_ack("test_msg_1", "user_1").await?;
        assert!(!exists);

        Ok(())
    }
}
