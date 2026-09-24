//! # PostgreSQL Thread Repository
//!
//! PostgreSQL持久化层实现，用于话题（Thread）管理
//!
//! `threads` / `thread_participants` 主键含 `tenant_id`：每条查询都从 `ctx` 取租户
//! （缺省按 `normalize_tenant_id` 归一到 `"0"`），跨租户的同名 thread_id 互不可见。

use std::sync::Arc;

use chrono::Utc;
use flare_im_contracts::utils::normalize_tenant_id;
use flare_server_core::error::{ErrorCode, Result, map_infra_error};
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

    fn tenant_of(ctx: &flare_server_core::context::Context) -> String {
        normalize_tenant_id(ctx.tenant_id().unwrap_or_default())
    }

    fn thread_from_row(row: &sqlx::postgres::PgRow) -> Thread {
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
    }
}

impl ThreadRepository for PostgresThreadRepository {
    #[instrument(skip(self, ctx), fields(conversation_id = %conversation_id, root_message_id = %root_message_id))]
    async fn create_thread(
        &self,
        ctx: &flare_server_core::context::Context,
        conversation_id: &str,
        root_message_id: &str,
        title: Option<&str>,
        creator_id: &str,
    ) -> Result<String> {
        let tenant_id = Self::tenant_of(ctx);
        let thread_id = root_message_id.to_string(); // 话题ID通常等于根消息ID
        let now = Utc::now();

        sqlx::query(
            r#"
            INSERT INTO threads (
                tenant_id, id, conversation_id, root_message_id, title, creator_id,
                reply_count, participant_count, is_pinned, is_locked, is_archived,
                created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, 0, 1, FALSE, FALSE, FALSE, $7, $7)
            ON CONFLICT (tenant_id, id) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(&tenant_id)
        .bind(&thread_id)
        .bind(conversation_id)
        .bind(root_message_id)
        .bind(title)
        .bind(creator_id)
        .bind(now)
        .fetch_optional(&*self.pool)
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to create thread"))?;

        // 添加创建者为参与者
        self.add_participant(ctx, &thread_id, creator_id).await?;

        Ok(thread_id)
    }

    #[instrument(skip(self, ctx), fields(conversation_id = %conversation_id))]
    async fn list_threads(
        &self,
        ctx: &flare_server_core::context::Context,
        conversation_id: &str,
        limit: i32,
        offset: i32,
        include_archived: bool,
        sort_order: ThreadSortOrder,
    ) -> Result<(Vec<Thread>, i32)> {
        let tenant_id = Self::tenant_of(ctx);

        // 构建排序子句
        let order_by = match sort_order {
            ThreadSortOrder::UpdatedDesc => "last_reply_at DESC NULLS LAST, updated_at DESC",
            ThreadSortOrder::UpdatedAsc => "last_reply_at ASC NULLS FIRST, updated_at ASC",
            ThreadSortOrder::ReplyCountDesc => "reply_count DESC, last_reply_at DESC NULLS LAST",
        };

        // 构建WHERE子句
        let where_clause = if include_archived {
            "WHERE tenant_id = $1 AND conversation_id = $2"
        } else {
            "WHERE tenant_id = $1 AND conversation_id = $2 AND is_archived = FALSE"
        };

        // 查询总数
        let total_count: i64 = sqlx::query_scalar(&format!(
            r#"
                SELECT COUNT(*) FROM threads {}
                "#,
            where_clause
        ))
        .bind(&tenant_id)
        .bind(conversation_id)
        .fetch_one(&*self.pool)
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to count threads"))?;

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
            LIMIT $3 OFFSET $4
            "#,
            where_clause, order_by
        ))
        .bind(&tenant_id)
        .bind(conversation_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&*self.pool)
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to list threads"))?;

        let threads: Vec<Thread> = rows.iter().map(Self::thread_from_row).collect();

        Ok((threads, total_count as i32))
    }

    #[instrument(skip(self, ctx), fields(thread_id = %thread_id))]
    async fn get_thread(
        &self,
        ctx: &flare_server_core::context::Context,
        thread_id: &str,
    ) -> Result<Option<Thread>> {
        let tenant_id = Self::tenant_of(ctx);
        let row = sqlx::query(
            r#"
            SELECT
                id, conversation_id, root_message_id, title, creator_id,
                reply_count, last_reply_at, last_reply_id, last_reply_user_id,
                participant_count, is_pinned, is_locked, is_archived,
                created_at, updated_at, extra
            FROM threads
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(thread_id)
        .fetch_optional(&*self.pool)
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to get thread"))?;

        Ok(row.as_ref().map(Self::thread_from_row))
    }

    #[instrument(skip(self, ctx), fields(thread_id = %thread_id))]
    async fn update_thread(
        &self,
        ctx: &flare_server_core::context::Context,
        thread_id: &str,
        title: Option<&str>,
        is_pinned: Option<bool>,
        is_locked: Option<bool>,
        is_archived: Option<bool>,
    ) -> Result<()> {
        use sqlx::QueryBuilder;

        let tenant_id = Self::tenant_of(ctx);
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

        query.push(" WHERE tenant_id = ");
        query.push_bind(tenant_id);
        query.push(" AND id = ");
        query.push_bind(thread_id);

        query
            .build()
            .execute(&*self.pool)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to update thread"))?;

        Ok(())
    }

    #[instrument(skip(self, ctx), fields(thread_id = %thread_id))]
    async fn delete_thread(
        &self,
        ctx: &flare_server_core::context::Context,
        thread_id: &str,
    ) -> Result<()> {
        let tenant_id = Self::tenant_of(ctx);
        sqlx::query("DELETE FROM threads WHERE tenant_id = $1 AND id = $2")
            .bind(&tenant_id)
            .bind(thread_id)
            .execute(&*self.pool)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to delete thread"))?;

        Ok(())
    }

    #[instrument(skip(self, ctx), fields(thread_id = %thread_id))]
    async fn increment_reply_count(
        &self,
        ctx: &flare_server_core::context::Context,
        thread_id: &str,
        reply_message_id: &str,
        reply_user_id: &str,
    ) -> Result<()> {
        let tenant_id = Self::tenant_of(ctx);
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
            WHERE tenant_id = $4 AND id = $5
            "#,
        )
        .bind(now)
        .bind(reply_message_id)
        .bind(reply_user_id)
        .bind(&tenant_id)
        .bind(thread_id)
        .execute(&*self.pool)
        .await
        .map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "Failed to increment thread reply count",
            )
        })?;

        // 更新参与者信息
        self.add_participant(ctx, thread_id, reply_user_id).await?;

        Ok(())
    }

    #[instrument(skip(self, ctx), fields(thread_id = %thread_id, user_id = %user_id))]
    async fn add_participant(
        &self,
        ctx: &flare_server_core::context::Context,
        thread_id: &str,
        user_id: &str,
    ) -> Result<()> {
        let tenant_id = Self::tenant_of(ctx);
        let now = Utc::now();

        // 插入或更新参与者
        sqlx::query(
            r#"
            INSERT INTO thread_participants (tenant_id, thread_id, user_id, first_participated_at, last_participated_at, reply_count)
            VALUES ($1, $2, $3, $4, $4, 0)
            ON CONFLICT (tenant_id, thread_id, user_id)
            DO UPDATE SET
                last_participated_at = $4,
                reply_count = thread_participants.reply_count + 1
            "#,
        )
        .bind(&tenant_id)
        .bind(thread_id)
        .bind(user_id)
        .bind(now)
        .execute(&*self.pool)
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to add thread participant"))?;

        // 更新话题的参与者数量
        sqlx::query(
            r#"
            UPDATE threads
            SET participant_count = (
                SELECT COUNT(DISTINCT user_id)
                FROM thread_participants
                WHERE tenant_id = $1 AND thread_id = $2
            )
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(thread_id)
        .execute(&*self.pool)
        .await
        .map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "Failed to update thread participant count",
            )
        })?;

        Ok(())
    }

    #[instrument(skip(self, ctx), fields(thread_id = %thread_id))]
    async fn get_participants(
        &self,
        ctx: &flare_server_core::context::Context,
        thread_id: &str,
    ) -> Result<Vec<String>> {
        let tenant_id = Self::tenant_of(ctx);
        let rows = sqlx::query(
            r#"
            SELECT user_id
            FROM thread_participants
            WHERE tenant_id = $1 AND thread_id = $2
            ORDER BY last_participated_at DESC
            "#,
        )
        .bind(&tenant_id)
        .bind(thread_id)
        .fetch_all(&*self.pool)
        .await
        .map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "Failed to get thread participants",
            )
        })?;

        Ok(rows.into_iter().map(|row| row.get("user_id")).collect())
    }
}
