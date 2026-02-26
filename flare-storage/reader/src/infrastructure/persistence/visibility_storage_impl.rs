//! 可见性存储仓储实现
//!
//! 基于 PostgreSQL 实现消息的可见性状态管理

use anyhow::Result;
use async_trait::async_trait;
use flare_proto::common::VisibilityStatus;

use crate::domain::repository::visibility_storage::VisibilityStorage;
use sqlx::Row;

use crate::infrastructure::persistence::postgres_base::PostgresBaseStorage;

/// PostgreSQL 可见性存储实现
pub struct PostgresVisibilityStorageImpl {
    base: PostgresBaseStorage,
}

impl PostgresVisibilityStorageImpl {
    pub fn new(base: PostgresBaseStorage) -> Self {
        Self { base }
    }
}

#[async_trait]
impl VisibilityStorage for PostgresVisibilityStorageImpl {
    async fn set_visibility(
        &self,
        message_id: &str,
        user_id: &str,
        _conversation_id: &str,
        visibility: VisibilityStatus,
    ) -> Result<()> {
        // 获取可见性状态字符串
        let vis_status = match visibility {
            VisibilityStatus::VisibilityVisible => "VISIBLE",
            VisibilityStatus::VisibilityHidden => "HIDDEN",
            VisibilityStatus::VisibilityDeleted => "DELETED",
            #[allow(unreachable_patterns)]
            _ => "VISIBLE", // 默认为可见
        };

        // 获取消息的租户ID
        let tenant_row = sqlx::query("SELECT tenant_id FROM messages WHERE server_id = $1")
            .bind(message_id)
            .fetch_optional(&self.base.pool)
            .await?;

        let tenant_id = if let Some(row) = tenant_row {
            row.get::<String, _>("tenant_id")
        } else {
            // 如果消息不存在，我们可以使用会话级别的租户ID作为备选
            // 但这可能不是最理想的解决方案，取决于具体业务需求
            return Err(anyhow::anyhow!("Message not found: {}", message_id));
        };

        // 使用 INSERT ... ON CONFLICT DO UPDATE 语法更新或插入可见性记录
        sqlx::query(
            r#"
            INSERT INTO message_visibility (tenant_id, message_id, user_id, visibility_status, changed_at, created_at, updated_at)
            VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (tenant_id, message_id, user_id) 
            DO UPDATE SET 
                visibility_status = $4,
                changed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .bind(user_id)
        .bind(vis_status)
        .execute(&self.base.pool)
        .await?;

        Ok(())
    }

    async fn get_visibility(
        &self,
        message_id: &str,
        user_id: &str,
    ) -> Result<Option<VisibilityStatus>> {
        let row = sqlx::query(
            r#"
            SELECT visibility_status
            FROM message_visibility
            WHERE message_id = $1 AND user_id = $2
            "#,
        )
        .bind(message_id)
        .bind(user_id)
        .fetch_optional(&self.base.pool)
        .await?;

        if let Some(row) = row {
            let status_str: String = row.get("visibility_status");
            let status = match status_str.as_str() {
                "VISIBLE" => VisibilityStatus::VisibilityVisible,
                "HIDDEN" => VisibilityStatus::VisibilityHidden,
                "DELETED" => VisibilityStatus::VisibilityDeleted,
                _ => VisibilityStatus::VisibilityVisible, // 默认可见
            };
            Ok(Some(status))
        } else {
            // 如果没有找到记录，默认为可见
            Ok(Some(VisibilityStatus::VisibilityVisible))
        }
    }

    async fn batch_set_visibility(
        &self,
        message_ids: &[String],
        user_id: &str,
        _conversation_id: &str,
        visibility: VisibilityStatus,
    ) -> Result<usize> {
        if message_ids.is_empty() {
            return Ok(0);
        }

        // 获取可见性状态字符串
        let vis_status = match visibility {
            VisibilityStatus::VisibilityVisible => "VISIBLE",
            VisibilityStatus::VisibilityHidden => "HIDDEN",
            VisibilityStatus::VisibilityDeleted => "DELETED",
            #[allow(unreachable_patterns)]
            _ => "VISIBLE", // 默认为可见
        };

        // 批量更新可见性状态
        // 首先获取消息的租户ID
        let result = sqlx::query(
            r#"
            INSERT INTO message_visibility (tenant_id, message_id, user_id, visibility_status, changed_at, created_at, updated_at)
            SELECT m.tenant_id, m.server_id, $1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            FROM messages m
            WHERE m.server_id = ANY($3)
            ON CONFLICT (tenant_id, message_id, user_id) 
            DO UPDATE SET 
                visibility_status = $2,
                changed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            "#,
        )
        .bind(user_id)
        .bind(vis_status)
        .bind(message_ids)
        .execute(&self.base.pool)
        .await?;

        Ok(result.rows_affected() as usize)
    }

    async fn query_visible_message_ids(
        &self,
        user_id: &str,
        _conversation_id: &str,
        visibility_status: VisibilityStatus,
    ) -> Result<Vec<String>> {
        // 获取可见性状态字符串
        let vis_status = match visibility_status {
            VisibilityStatus::VisibilityVisible => "VISIBLE",
            VisibilityStatus::VisibilityHidden => "HIDDEN",
            VisibilityStatus::VisibilityDeleted => "DELETED",
            #[allow(unreachable_patterns)]
            _ => "VISIBLE", // 默认为可见
        };

        // 查询指定用户在指定会话中的特定可见性状态的消息ID
        let rows = sqlx::query(
            r#"
            SELECT mv.message_id
            FROM message_visibility mv
            JOIN messages m ON mv.message_id = m.server_id AND mv.tenant_id = m.tenant_id
            WHERE mv.user_id = $1 
            AND m.conversation_id = $2
            AND mv.visibility_status = $3
            "#,
        )
        .bind(user_id)
        .bind(_conversation_id)
        .bind(vis_status)
        .fetch_all(&self.base.pool)
        .await?;

        let mut message_ids = Vec::new();
        for row in rows {
            message_ids.push(row.get::<String, _>("message_id"));
        }

        Ok(message_ids)
    }
}