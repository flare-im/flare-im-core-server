//! # PostgreSQL Thread Repository
//!
//! PostgreSQL持久化层实现，用于话题（Thread）管理

use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use sqlx::{PgPool, Row};
use tracing::instrument;

use crate::domain::model::{Thread, ThreadSortOrder};
use crate::domain::repository::ThreadRepository;

/// PostgreSQL Thread Repository实现
pub struct PostgresThreadRepository {
    pool: Arc<PgPool>,
}

impl PostgresThreadRepository {
    /// 创建PostgreSQL Thread Repository
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ThreadRepository for PostgresThreadRepository {
    #[instrument(skip(self), fields(conversation_id = %conversation_id, root_message_id = %root_message_id))]
    async fn create_thread(
        &self,
        conversation_id: &str,
        root_message_id: &str,
        title: Option<&str>,
        creator_id: &str,
    ) -> Result<String> {
        let thread_id = root_message_id.to_string(); // 话题ID通常等于根消息ID
        let now = Utc::now();

        sqlx::query(
            r#"
            INSERT INTO threads (
                id, conversation_id, root_message_id, title, creator_id,
                reply_count, participant_count, is_pinned, is_locked, is_archived,
                created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, 0, 1, FALSE, FALSE, FALSE, $6, $6)
            ON CONFLICT (id) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(&thread_id)
        .bind(conversation_id)
        .bind(root_message_id)
        .bind(title)
        .bind(creator_id)
        .bind(now)
        .fetch_optional(&*self.pool)
        .await
        .context("Failed to create thread")?;

        // 添加创建者为参与者
        self.add_participant(&thread_id, creator_id).await?;

        Ok(thread_id)
    }

    #[instrument(skip(self), fields(conversation_id = %conversation_id))]
    async fn list_threads(
        &self,
        conversation_id: &str,
        limit: i32,
        offset: i32,
        include_archived: bool,
        sort_order: ThreadSortOrder,
    ) -> Result<(Vec<Thread>, i32)> {
        // 构建排序子句
        let order_by = match sort_order {
            ThreadSortOrder::UpdatedDesc => "last_reply_at DESC NULLS LAST, updated_at DESC",
            ThreadSortOrder::UpdatedAsc => "last_reply_at ASC NULLS FIRST, updated_at ASC",
            ThreadSortOrder::ReplyCountDesc => "reply_count DESC, last_reply_at DESC NULLS LAST",
        };

        // 构建WHERE子句
        let where_clause = if include_archived {
            "WHERE conversation_id = $1"
        } else {
            "WHERE conversation_id = $1 AND is_archived = FALSE"
        };

        // 查询总数
        let total_count: i64 = sqlx::query_scalar(&format!(
            r#"
                SELECT COUNT(*) FROM threads {}
                "#,
            where_clause
        ))
        .bind(conversation_id)
        .fetch_one(&*self.pool)
        .await
        .context("Failed to count threads")?;

        // 查询话题列表
        let rows = sqlx::query(&format!(
            r#"
            SELECT 
                id, conversation_id, root_message_id, title, creator_id,
                reply_count, last_reply_at, last_reply_id, last_reply_user_id,
                participant_count, is_pinned, is_locked, is_archived,
                created_at, updated_at, extra
            FROM threads
            {}
            ORDER BY {}
            LIMIT $2 OFFSET $3
            "#,
            where_clause, order_by
        ))
        .bind(conversation_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&*self.pool)
        .await
        .context("Failed to list threads")?;

        let threads: Vec<Thread> = rows
            .into_iter()
            .map(|row| {
                let extra: serde_json::Value = row.get("extra");
                let extra: std::collections::HashMap<String, String> =
                    serde_json::from_value(extra).unwrap_or_default();

                Thread {
                    id: row.get("id"),
                    conversation_id: row.get("conversation_id"),
                    root_message_id: row.get("root_message_id"),
                    title: row.get("title"),
                    creator_id: row.get("creator_id"),
                    reply_count: row.get("reply_count"),
                    last_reply_at: row.get("last_reply_at"),
                    last_reply_id: row.get("last_reply_id"),
                    last_reply_user_id: row.get("last_reply_user_id"),
                    participant_count: row.get("participant_count"),
                    is_pinned: row.get("is_pinned"),
                    is_locked: row.get("is_locked"),
                    is_archived: row.get("is_archived"),
                    created_at: row.get("created_at"),
                    updated_at: row.get("updated_at"),
                    extra,
                }
            })
            .collect();

        Ok((threads, total_count as i32))
    }

    #[instrument(skip(self), fields(thread_id = %thread_id))]
    async fn get_thread(&self, thread_id: &str) -> Result<Option<Thread>> {
        let row = sqlx::query(
            r#"
            SELECT 
                id, conversation_id, root_message_id, title, creator_id,
                reply_count, last_reply_at, last_reply_id, last_reply_user_id,
                participant_count, is_pinned, is_locked, is_archived,
                created_at, updated_at, extra
            FROM threads
            WHERE id = $1
            "#,
        )
        .bind(thread_id)
        .fetch_optional(&*self.pool)
        .await
        .context("Failed to get thread")?;

        let Some(row) = row else {
            return Ok(None);
        };

        let extra: serde_json::Value = row.get("extra");
        let extra: std::collections::HashMap<String, String> =
            serde_json::from_value(extra).unwrap_or_default();

        Ok(Some(Thread {
            id: row.get("id"),
            conversation_id: row.get("conversation_id"),
            root_message_id: row.get("root_message_id"),
            title: row.get("title"),
            creator_id: row.get("creator_id"),
            reply_count: row.get("reply_count"),
            last_reply_at: row.get("last_reply_at"),
            last_reply_id: row.get("last_reply_id"),
            last_reply_user_id: row.get("last_reply_user_id"),
            participant_count: row.get("participant_count"),
            is_pinned: row.get("is_pinned"),
            is_locked: row.get("is_locked"),
            is_archived: row.get("is_archived"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
            extra,
        }))
    }

    #[instrument(skip(self), fields(thread_id = %thread_id))]
    async fn update_thread(
        &self,
        thread_id: &str,
        title: Option<&str>,
        is_pinned: Option<bool>,
        is_locked: Option<bool>,
        is_archived: Option<bool>,
    ) -> Result<()> {
        use sqlx::QueryBuilder;

        let mut query = QueryBuilder::new("UPDATE threads SET ");
        let mut has_updates = false;
        let mut separated = query.separated(", ");

        if let Some(title) = title {
            separated.push("title = ");
            separated.push_bind(title);
            has_updates = true;
        }
        if let Some(is_pinned) = is_pinned {
            separated.push("is_pinned = ");
            separated.push_bind(is_pinned);
            has_updates = true;
        }
        if let Some(is_locked) = is_locked {
            separated.push("is_locked = ");
            separated.push_bind(is_locked);
            has_updates = true;
        }
        if let Some(is_archived) = is_archived {
            separated.push("is_archived = ");
            separated.push_bind(is_archived);
            has_updates = true;
        }

        if !has_updates {
            return Ok(()); // 没有需要更新的字段
        }

        separated.push("updated_at = ");
        separated.push_bind(Utc::now());

        query.push(" WHERE id = ");
        query.push_bind(thread_id);

        query
            .build()
            .execute(&*self.pool)
            .await
            .context("Failed to update thread")?;

        Ok(())
    }

    #[instrument(skip(self), fields(thread_id = %thread_id))]
    async fn delete_thread(&self, thread_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM threads WHERE id = $1")
            .bind(thread_id)
            .execute(&*self.pool)
            .await
            .context("Failed to delete thread")?;

        Ok(())
    }

    #[instrument(skip(self), fields(thread_id = %thread_id))]
    async fn increment_reply_count(
        &self,
        thread_id: &str,
        reply_message_id: &str,
        reply_user_id: &str,
    ) -> Result<()> {
        let now = Utc::now();

        sqlx::query(
            r#"
            UPDATE threads
            SET 
                reply_count = reply_count + 1,
                last_reply_at = $1,
                last_reply_id = $2,
                last_reply_user_id = $3,
                updated_at = $1
            WHERE id = $4
            "#,
        )
        .bind(now)
        .bind(reply_message_id)
        .bind(reply_user_id)
        .bind(thread_id)
        .execute(&*self.pool)
        .await
        .context("Failed to increment thread reply count")?;

        // 更新参与者信息
        self.add_participant(thread_id, reply_user_id).await?;

        Ok(())
    }

    #[instrument(skip(self), fields(thread_id = %thread_id, user_id = %user_id))]
    async fn add_participant(&self, thread_id: &str, user_id: &str) -> Result<()> {
        let now = Utc::now();

        // 插入或更新参与者
        sqlx::query(
            r#"
            INSERT INTO thread_participants (thread_id, user_id, first_participated_at, last_participated_at, reply_count)
            VALUES ($1, $2, $3, $3, 0)
            ON CONFLICT (thread_id, user_id) 
            DO UPDATE SET 
                last_participated_at = $3,
                reply_count = thread_participants.reply_count + 1
            "#,
        )
        .bind(thread_id)
        .bind(user_id)
        .bind(now)
        .execute(&*self.pool)
        .await
        .context("Failed to add thread participant")?;

        // 更新话题的参与者数量
        sqlx::query(
            r#"
            UPDATE threads
            SET participant_count = (
                SELECT COUNT(DISTINCT user_id) 
                FROM thread_participants 
                WHERE thread_id = $1
            )
            WHERE id = $1
            "#,
        )
        .bind(thread_id)
        .execute(&*self.pool)
        .await
        .context("Failed to update thread participant count")?;

        Ok(())
    }

    #[instrument(skip(self), fields(thread_id = %thread_id))]
    async fn get_participants(&self, thread_id: &str) -> Result<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT user_id
            FROM thread_participants
            WHERE thread_id = $1
            ORDER BY last_participated_at DESC
            "#,
        )
        .bind(thread_id)
        .fetch_all(&*self.pool)
        .await
        .context("Failed to get thread participants")?;

        Ok(rows.into_iter().map(|row| row.get("user_id")).collect())
    }
}
