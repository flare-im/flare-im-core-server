//! ACK异步归档机制
//! 用于审计和分析的ACK日志异步归档

use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, Row};
use std::sync::Arc;
// use std::time::{/*Duration,*/ SystemTime, UNIX_EPOCH};
use tokio::time::Duration as TokioDuration;
use tokio::sync::mpsc;
use tokio::time::interval;

/// ACK归档记录
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct AckArchiveRecord {
    /// 消息ID
    pub message_id: String,
    /// 用户ID
    pub user_id: String,
    /// ACK类型
    pub ack_type: String,
    /// ACK状态
    pub ack_status: String,
    /// 时间戳
    pub timestamp: i64,
    /// 重要性等级
    pub importance_level: i16,
    /// 元数据
    pub metadata: Option<serde_json::Value>,
    /// 归档时间
    pub archived_at: i64,
}

/// ACK归档器
pub struct AckArchiver {
    /// 数据库连接池
    db_pool: Arc<PgPool>,
    /// 归档通道发送端
    tx: mpsc::Sender<AckArchiveRecord>,
}

impl AckArchiver {
    /// 创建新的ACK归档器
    pub fn new(db_pool: PgPool, buffer_size: usize) -> Self {
        let (tx, mut rx) = mpsc::channel::<AckArchiveRecord>(buffer_size);
        let db_pool = Arc::new(db_pool);
        let db_pool_clone = db_pool.clone();

        // 启动后台归档任务
        tokio::spawn(async move {
            let mut batch = Vec::new();
            let mut interval = interval(TokioDuration::from_secs(60)); // 每分钟归档一次

            loop {
                tokio::select! {
                    // 接收归档记录
                    Some(record) = rx.recv() => {
                        batch.push(record);
                        
                        // 达到批次大小立即处理
                        if batch.len() >= 100 {
                            if let Err(e) = Self::batch_archive(&db_pool_clone, &mut batch).await {
                                eprintln!("Failed to batch archive ACKs: {}", e);
                            }
                        }
                    }
                    // 定时处理
                    _ = interval.tick() => {
                        if !batch.is_empty() {
                            if let Err(e) = Self::batch_archive(&db_pool_clone, &mut batch).await {
                                eprintln!("Failed to batch archive ACKs: {}", e);
                            }
                        }
                    }
                }
            }
        });

        Self { db_pool, tx }
    }

    /// 归档ACK记录
    pub async fn archive_ack(&self, record: AckArchiveRecord) -> Result<(), Box<dyn std::error::Error>> {
        self.tx.send(record).await?;
        Ok(())
    }

    /// 批量归档ACK记录
    async fn batch_archive(db_pool: &PgPool, batch: &mut Vec<AckArchiveRecord>) -> Result<(), Box<dyn std::error::Error>> {
        if batch.is_empty() {
            return Ok(());
        }

        // 构建批量插入语句
        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO ack_archive_records 
             (message_id, user_id, ack_type, ack_status, timestamp, importance_level, metadata, archived_at)"
        );

        query_builder.push_values(batch, |mut b, record| {
            b.push_bind(&record.message_id)
                .push_bind(&record.user_id)
                .push_bind(&record.ack_type)
                .push_bind(&record.ack_status)
                .push_bind(record.timestamp)
                .push_bind(record.importance_level)
                .push_bind(&record.metadata)
                .push_bind(record.archived_at);
        });

        query_builder.push(" ON CONFLICT DO NOTHING");

        let query = query_builder.build();
        query.execute(db_pool).await?;

        Ok(())
    }

    /// 查询归档的ACK记录
    pub async fn query_archived_acks(
        &self,
        message_id: Option<&str>,
        user_id: Option<&str>,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<u32>,
    ) -> Result<Vec<AckArchiveRecord>, Box<dyn std::error::Error>> {
        let mut query_builder = sqlx::QueryBuilder::new(
            "SELECT message_id, user_id, ack_type, ack_status, timestamp, importance_level, metadata, archived_at 
             FROM ack_archive_records WHERE 1=1"
        );

        if let Some(msg_id) = message_id {
            query_builder.push(" AND message_id = ").push_bind(msg_id);
        }

        if let Some(uid) = user_id {
            query_builder.push(" AND user_id = ").push_bind(uid);
        }

        if let Some(start) = start_time {
            query_builder.push(" AND timestamp >= ").push_bind(start);
        }

        if let Some(end) = end_time {
            query_builder.push(" AND timestamp <= ").push_bind(end);
        }

        query_builder.push(" ORDER BY timestamp DESC");

        if let Some(lim) = limit {
            query_builder.push(" LIMIT ").push_bind(lim as i64);
        }

        let query = query_builder.build_query_as::<AckArchiveRecord>();
        let records = query.fetch_all(&*self.db_pool).await?;

        Ok(records)
    }

    /// 获取归档统计信息
    pub async fn get_archive_stats(&self) -> Result<ArchiveStats, Box<dyn std::error::Error>> {
        let row = sqlx::query(
            "SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN importance_level = 3 THEN 1 END) as high_importance,
                COUNT(CASE WHEN importance_level = 2 THEN 1 END) as medium_importance,
                COUNT(CASE WHEN importance_level = 1 THEN 1 END) as low_importance,
                MAX(timestamp) as latest_timestamp
             FROM ack_archive_records"
        )
        .fetch_one(&*self.db_pool)
        .await?;

        Ok(ArchiveStats {
            total_records: row.try_get("total_records")?,
            high_importance: row.try_get("high_importance")?,
            medium_importance: row.try_get("medium_importance")?,
            low_importance: row.try_get("low_importance")?,
            latest_timestamp: row.try_get("latest_timestamp").unwrap_or(0),
        })
    }
}

/// 归档统计信息
#[derive(Debug, Clone)]
pub struct ArchiveStats {
    /// 总记录数
    pub total_records: i64,
    /// 高重要性记录数
    pub high_importance: i64,
    /// 中等重要性记录数
    pub medium_importance: i64,
    /// 低重要性记录数
    pub low_importance: i64,
    /// 最新时间戳
    pub latest_timestamp: i64,
}


#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{PgPool, postgres::PgPoolOptions};
    use tokio;

    #[tokio::test]
    async fn test_ack_archiver() -> Result<(), Box<dyn std::error::Error>> {
        // 注意：这需要一个运行中的PostgreSQL实例
        let db_pool = PgPoolOptions::new()
            .max_connections(5)
            .connect("postgresql://localhost/test_db")
            .await?;


        let archiver = AckArchiver::new(db_pool, 100);

        let record = AckArchiveRecord {
            message_id: "test_msg_1".to_string(),
            user_id: "user_1".to_string(),
            ack_type: "client".to_string(),
            ack_status: "received".to_string(),
            timestamp: 1234567890,
            importance_level: 3,
            metadata: Some(serde_json::json!({"test": "data"})),
            archived_at: 1234567890,
        };

        // 归档记录
        archiver.archive_ack(record).await?;

        // 等待归档完成
        tokio::time::sleep(TokioDuration::from_millis(100)).await;

        // 查询归档记录
        let records = archiver.query_archived_acks(
            Some("test_msg_1"),
            Some("user_1"),
            None,
            None,
            Some(10),
        ).await?;

        assert!(!records.is_empty());
        assert_eq!(records[0].message_id, "test_msg_1");
        assert_eq!(records[0].user_id, "user_1");

        // 获取统计信息
        let stats = archiver.get_archive_stats().await?;
        assert!(stats.total_records >= 1);

        Ok(())
    }
}