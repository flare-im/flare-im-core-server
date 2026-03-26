//! # PostgreSQL Conversation Repository
//!
//! PostgreSQL持久化层实现，用于会话元数据存储

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use tracing::info;

use crate::config::ConversationConfig;
use crate::domain::model::{
    Conversation, ConversationBootstrapResult, ConversationFilter, ConversationParticipant, ConversationSort, ConversationSummary,
};
use crate::domain::repository::ConversationRepository;
use flare_im_core::utils::calculate_unread_count;

/// 会话查询行结构（与 init_v2 一致：visibility 为 INT）
#[derive(sqlx::FromRow)]
#[allow(dead_code)]
struct ConversationRow {
    conversation_id: String,
    conversation_type: String,
    business_type: String,
    display_name: Option<String>,
    attributes: serde_json::Value,
    visibility: i32,
    lifecycle_state: String,
    updated_at: DateTime<Utc>,
}

/// PostgreSQL Conversation Repository实现
pub struct PostgresConversationRepository {
    pool: Arc<PgPool>,
    config: Arc<ConversationConfig>,
}

impl PostgresConversationRepository {
    /// 创建PostgreSQL Conversation Repository
    pub fn new(pool: Arc<PgPool>, config: Arc<ConversationConfig>) -> Self {
        Self { pool, config }
    }

}

impl ConversationRepository for PostgresConversationRepository {
    async fn load_bootstrap(
        &self,
        ctx: &flare_server_core::context::Context,
        client_cursor: &HashMap<String, i64>,
    ) -> Result<ConversationBootstrapResult> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let user_id = ctx.user_id().ok_or_else(|| anyhow::anyhow!("user_id is required in context"))?;
        // 1. 从user_sync_cursor表加载用户的光标映射
        let cursor_rows = sqlx::query(
            r#"
            SELECT conversation_id, last_synced_ts
            FROM user_sync_cursor
            WHERE tenant_id = $1 AND user_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(user_id)
        .fetch_all(&*self.pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to load user cursors: {}", e))?;

        let mut server_cursor: HashMap<String, i64> = cursor_rows
            .into_iter()
            .map(|row| {
                let conversation_id: String = row.get("conversation_id");
                let ts: i64 = row.get("last_synced_ts");
                (conversation_id, ts)
            })
            .collect();

        // merge client cursor hints to ensure we cover requested conversations
        for (conversation_id, ts) in client_cursor {
            server_cursor.entry(conversation_id.clone()).or_insert(*ts);
        }

        // 2. 从conversations和conversation_participants表查询用户参与的会话（包含未读数信息）
        let session_rows = sqlx::query(
            r#"
            SELECT DISTINCT
                s.conversation_id,
                s.conversation_type,
                s.business_type,
                s.display_name,
                s.attributes,
                s.visibility,
                s.lifecycle_state,
                s.updated_at,
                s.last_message_seq,
                COALESCE(sp.last_read_seq, 0) as last_read_seq,
                COALESCE(sp.unread_count, 0) as unread_count
            FROM conversations s
            INNER JOIN conversation_participants sp ON s.tenant_id = sp.tenant_id AND s.conversation_id = sp.conversation_id
            WHERE s.tenant_id = $1
              AND sp.tenant_id = $1
              AND sp.user_id = $2
              AND s.lifecycle_state != 'deleted'
            ORDER BY s.updated_at DESC
            "#,
        )
        .bind(tenant_id)
        .bind(user_id)
        .fetch_all(&*self.pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to load user conversations: {}", e))?;

        let mut summaries = Vec::new();

        for row in session_rows {
            let conversation_id: String = row.get("conversation_id");
            let conversation_type: Option<String> = row.get("conversation_type");
            let business_type: Option<String> = row.get("business_type");
            let display_name: Option<String> = row.get("display_name");
            let attributes: Option<serde_json::Value> = row.get("attributes");
            let updated_at: DateTime<Utc> = row.get("updated_at");

            // 从数据库读取未读数相关字段
            let last_message_seq: Option<i64> = row.get("last_message_seq");
            let last_read_seq: i64 = row.get("last_read_seq");
            let unread_count: i32 = row.get("unread_count");

            let attributes: HashMap<String, String> = attributes
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or_default();

            // 注释：最后一条消息信息将在ApplicationService层通过MessageProvider补充
            // 当前实现使用updated_at作为server_cursor_ts的fallback
            let server_cursor_ts = server_cursor
                .get(&conversation_id)
                .copied()
                .or_else(|| Some(updated_at.timestamp_millis()));

            // 计算未读数：基于 last_message_seq - last_read_seq（init_v2 列名）
            // 如果数据库中的 unread_count 已更新，直接使用；否则计算
            let calculated_unread = if last_message_seq.is_some() {
                // 使用工具函数计算未读数
                calculate_unread_count(last_message_seq, last_read_seq)
            } else {
                unread_count // 使用数据库中的值
            };

            let summary = ConversationSummary {
                conversation_id,
                conversation_type,
                business_type,
                last_message_id: None,           // 将在ApplicationService层补充
                last_message_time: None,         // 将在ApplicationService层补充
                last_sender_id: None,            // 将在ApplicationService层补充
                last_message_type: None,         // 将在ApplicationService层补充
                last_content_type: None,         // 将在ApplicationService层补充
                unread_count: calculated_unread, // 基于 seq 计算的未读数
                metadata: attributes,
                server_cursor_ts,
                display_name,
            };

            summaries.push(summary);
        }

        // 按server_cursor_ts降序排序
        summaries.sort_by(|a, b| {
            let at = a.server_cursor_ts.unwrap_or_default();
            let bt = b.server_cursor_ts.unwrap_or_default();
            bt.cmp(&at)
        });

        Ok(ConversationBootstrapResult {
            summaries,
            recent_messages: Vec::new(),
            cursor_map: server_cursor,
            policy: self.config.default_policy.clone(),
        })
    }

    async fn update_cursor(&self, ctx: &flare_server_core::context::Context, conversation_id: &str, ts: i64) -> Result<()> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let user_id = ctx.user_id().ok_or_else(|| anyhow::anyhow!("user_id is required in context"))?;
        sqlx::query(
            r#"
            INSERT INTO user_sync_cursor (tenant_id, user_id, conversation_id, last_synced_ts, updated_at)
            VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
            ON CONFLICT (tenant_id, user_id, conversation_id)
            DO UPDATE SET last_synced_ts = $4, updated_at = CURRENT_TIMESTAMP
            "#,
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(conversation_id)
        .bind(ts)
        .execute(&*self.pool)
        .await
        .context("Failed to update cursor")?;

        Ok(())
    }

    async fn create_conversation(&self, ctx: &flare_server_core::context::Context, session: &Conversation) -> Result<()> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let mut tx = self.pool.begin().await?;

        // 插入会话记录（幂等：ON CONFLICT DO NOTHING，支持异步 conversation.ensure 事件并发消费）
        let result = sqlx::query(
            r#"
            INSERT INTO conversations (
                tenant_id, conversation_id, conversation_type, business_type, display_name,
                attributes, visibility, lifecycle_state, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (tenant_id, conversation_id) DO NOTHING
            "#,
        )
        .bind(tenant_id)
        .bind(&session.conversation_id)
        .bind(&session.conversation_type)
        .bind(&session.business_type)
        .bind(&session.display_name)
        .bind(serde_json::to_value(&session.attributes)?)
        .bind(session.visibility.as_proto())
        .bind(session.lifecycle_state.as_str())
        .execute(&mut *tx)
        .await
        .context("Failed to create conversation")?;
        if result.rows_affected() > 0 {
            info!(conversation_id = %session.conversation_id, "Conversation row inserted");
        }

        // 插入参与者记录（使用 ON CONFLICT 处理重复插入）
        for participant in &session.participants {
            sqlx::query(
                r#"
                INSERT INTO conversation_participants (
                    tenant_id, conversation_id, user_id, roles, muted, pinned, attributes, created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (tenant_id, conversation_id, user_id) 
                DO UPDATE SET
                    roles = EXCLUDED.roles,
                    muted = EXCLUDED.muted,
                    pinned = EXCLUDED.pinned,
                    attributes = EXCLUDED.attributes,
                    updated_at = CURRENT_TIMESTAMP
                "#,
            )
            .bind(tenant_id)
            .bind(&session.conversation_id)
            .bind(&participant.user_id)
            .bind(&participant.roles)
            .bind(participant.muted)
            .bind(participant.pinned)
            .bind(serde_json::to_value(&participant.attributes)?)
            .execute(&mut *tx)
            .await
            .context("Failed to create participant")?;
        }

        tx.commit().await?;
        info!(conversation_id = %session.conversation_id, "Conversation created");
        Ok(())
    }

    async fn get_conversation(&self, ctx: &flare_server_core::context::Context, conversation_id: &str) -> Result<Option<Conversation>> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let row = sqlx::query(
            r#"
            SELECT conversation_id, conversation_type, business_type, display_name,
                   attributes, visibility, lifecycle_state,
                   created_at, updated_at
            FROM conversations
            WHERE tenant_id = $1 AND conversation_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(conversation_id)
        .fetch_optional(&*self.pool)
        .await
        .context("Failed to get conversation")?;

        let Some(row) = row else {
            return Ok(None);
        };

        let conversation_id: String = row.get("conversation_id");
        let conversation_type: String = row.get("conversation_type");
        let business_type: String = row.get("business_type");
        let display_name: Option<String> = row.get("display_name");
        let attributes: Option<serde_json::Value> = row.get("attributes");
        let visibility_int: i32 = row.get("visibility");
        let lifecycle_state: String = row.get("lifecycle_state");
        let created_at: DateTime<Utc> = row.get("created_at");
        let updated_at: DateTime<Utc> = row.get("updated_at");

        let attributes: HashMap<String, String> = attributes
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();

        let visibility = crate::domain::model::ConversationVisibility::from_proto(visibility_int);

        let lifecycle_state = match lifecycle_state.as_str() {
            "active" => crate::domain::model::ConversationLifecycleState::Active,
            "suspended" => crate::domain::model::ConversationLifecycleState::Suspended,
            "archived" => crate::domain::model::ConversationLifecycleState::Archived,
            "deleted" => crate::domain::model::ConversationLifecycleState::Deleted,
            _ => crate::domain::model::ConversationLifecycleState::Unspecified,
        };

        // 查询参与者
        let participant_rows = sqlx::query(
            r#"
            SELECT user_id, roles, muted, pinned, attributes
            FROM conversation_participants
            WHERE tenant_id = $1 AND conversation_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(&conversation_id)
        .fetch_all(&*self.pool)
        .await
        .context("Failed to get participants")?;

        let mut participants = Vec::new();
        for p_row in participant_rows {
            let user_id: String = p_row.get("user_id");
            let roles: Vec<String> = p_row.get("roles");
            let muted: bool = p_row.get("muted");
            let pinned: bool = p_row.get("pinned");
            let attributes: Option<serde_json::Value> = p_row.get("attributes");
            let attributes: HashMap<String, String> = attributes
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or_default();

            participants.push(ConversationParticipant {
                user_id,
                roles,
                muted,
                pinned,
                attributes,
            });
        }

        Ok(Some(Conversation {
            tenant_id: tenant_id.to_string(),
            conversation_id,
            conversation_type,
            business_type,
            display_name,
            attributes,
            participants,
            visibility,
            lifecycle_state,
            policy: None,
            created_at,
            updated_at,
        }))
    }

    async fn update_conversation(&self, ctx: &flare_server_core::context::Context, session: &Conversation) -> Result<()> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        sqlx::query(
            r#"
            UPDATE conversations
            SET display_name = $1,
                attributes = $2,
                visibility = $3,
                lifecycle_state = $4,
                updated_at = CURRENT_TIMESTAMP
            WHERE tenant_id = $5 AND conversation_id = $6
            "#,
        )
        .bind(&session.display_name)
        .bind(serde_json::to_value(&session.attributes)?)
        .bind(session.visibility.as_proto())
        .bind(session.lifecycle_state.as_str())
        .bind(tenant_id)
        .bind(&session.conversation_id)
        .execute(&*self.pool)
        .await
        .context("Failed to update conversation")?;

        info!(conversation_id = %session.conversation_id, "Conversation updated");
        Ok(())
    }

    async fn delete_conversation(&self, ctx: &flare_server_core::context::Context, conversation_id: &str, hard_delete: bool) -> Result<()> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        if hard_delete {
            // 物理删除（级联删除参与者）
            sqlx::query("DELETE FROM conversations WHERE tenant_id = $1 AND conversation_id = $2")
                .bind(tenant_id)
                .bind(conversation_id)
                .execute(&*self.pool)
                .await
                .context("Failed to delete conversation")?;
        } else {
            // 软删除（更新生命周期状态）
            sqlx::query(
                r#"
                UPDATE conversations
                SET lifecycle_state = 'deleted', updated_at = CURRENT_TIMESTAMP
                WHERE tenant_id = $1 AND conversation_id = $2
                "#,
            )
            .bind(tenant_id)
            .bind(conversation_id)
            .execute(&*self.pool)
            .await
            .context("Failed to delete conversation")?;
        }

        info!(conversation_id = %conversation_id, hard_delete = hard_delete, "Conversation deleted");
        Ok(())
    }

    async fn manage_participants(
        &self,
        ctx: &flare_server_core::context::Context,
        conversation_id: &str,
        to_add: &[ConversationParticipant],
        to_remove: &[String],
        role_updates: &[(String, Vec<String>)],
    ) -> Result<Vec<ConversationParticipant>> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let mut tx = self.pool.begin().await?;

        // 添加参与者
        for participant in to_add {
            sqlx::query(
                r#"
                INSERT INTO conversation_participants (
                    tenant_id, conversation_id, user_id, roles, muted, pinned, attributes, created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (tenant_id, conversation_id, user_id)
                DO UPDATE SET
                    roles = $4,
                    muted = $5,
                    pinned = $6,
                    attributes = $7,
                    updated_at = CURRENT_TIMESTAMP
                "#,
            )
            .bind(tenant_id)
            .bind(conversation_id)
            .bind(&participant.user_id)
            .bind(&participant.roles)
            .bind(participant.muted)
            .bind(participant.pinned)
            .bind(serde_json::to_value(&participant.attributes)?)
            .execute(&mut *tx)
            .await
            .context("Failed to add participant")?;
        }

        // 删除参与者
        for user_id in to_remove {
            sqlx::query("DELETE FROM conversation_participants WHERE tenant_id = $1 AND conversation_id = $2 AND user_id = $3")
                .bind(tenant_id)
                .bind(conversation_id)
                .bind(user_id)
                .execute(&mut *tx)
                .await
                .context("Failed to remove participant")?;
        }

        // 更新角色
        for (user_id, roles) in role_updates {
            sqlx::query(
                r#"
                UPDATE conversation_participants
                SET roles = $1, updated_at = CURRENT_TIMESTAMP
                WHERE tenant_id = $2 AND conversation_id = $3 AND user_id = $4
                "#,
            )
            .bind(roles)
            .bind(tenant_id)
            .bind(conversation_id)
            .bind(user_id)
            .execute(&mut *tx)
            .await
            .context("Failed to update participant roles")?;
        }

        tx.commit().await?;

        // 返回更新后的参与者列表
        let participant_rows = sqlx::query(
            r#"
            SELECT user_id, roles, muted, pinned, attributes
            FROM conversation_participants
            WHERE tenant_id = $1 AND conversation_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(conversation_id)
        .fetch_all(&*self.pool)
        .await
        .context("Failed to get participants")?;

        let mut participants = Vec::new();
        for p_row in participant_rows {
            let user_id: String = p_row.get("user_id");
            let roles: Vec<String> = p_row.get("roles");
            let muted: bool = p_row.get("muted");
            let pinned: bool = p_row.get("pinned");
            let attributes: Option<serde_json::Value> = p_row.get("attributes");
            let attributes: HashMap<String, String> = attributes
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or_default();

            participants.push(ConversationParticipant {
                user_id,
                roles,
                muted,
                pinned,
                attributes,
            });
        }

        Ok(participants)
    }

    async fn batch_acknowledge(&self, ctx: &flare_server_core::context::Context, cursors: &[(String, i64)]) -> Result<()> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let user_id = ctx.user_id().ok_or_else(|| anyhow::anyhow!("user_id is required in context"))?;
        let mut tx = self.pool.begin().await?;

        for (conversation_id, ts) in cursors {
            sqlx::query(
                r#"
                INSERT INTO user_sync_cursor (tenant_id, user_id, conversation_id, last_synced_ts, updated_at)
                VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
                ON CONFLICT (tenant_id, user_id, conversation_id)
                DO UPDATE SET last_synced_ts = $4, updated_at = CURRENT_TIMESTAMP
                "#,
            )
            .bind(tenant_id)
            .bind(user_id)
            .bind(conversation_id)
            .bind(*ts)
            .execute(&mut *tx)
            .await
            .context("Failed to acknowledge cursor")?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn search_conversations(
        &self,
        ctx: &flare_server_core::context::Context,
        filters: &[ConversationFilter],
        sort: &[ConversationSort],
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<ConversationSummary>, usize)> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let user_id = ctx.user_id();
        // 构建基础查询
        let mut query = String::from(
            r#"
            SELECT DISTINCT
                s.conversation_id,
                s.conversation_type,
                s.business_type,
                s.display_name,
                s.attributes,
                s.visibility,
                s.lifecycle_state,
                s.updated_at
            FROM conversations s
            "#,
        );

        // 如果指定了user_id，需要JOIN conversation_participants表
        if user_id.is_some() {
            query.push_str("INNER JOIN conversation_participants sp ON s.tenant_id = sp.tenant_id AND s.conversation_id = sp.conversation_id\n");
        }

        // 构建WHERE子句
        let mut conditions = Vec::new();
        let mut bind_index = 1;

        // 添加 tenant_id 过滤（必需）
        conditions.push(format!("s.tenant_id = ${}", bind_index));
        bind_index += 1;

        if user_id.is_some() {
            conditions.push(format!("sp.tenant_id = ${}", bind_index));
            bind_index += 1;
            conditions.push(format!("sp.user_id = ${}", bind_index));
            bind_index += 1;
        }

        // 应用过滤器
        for filter in filters {
            if filter.conversation_type.is_some() {
                conditions.push(format!("s.conversation_type = ${}", bind_index));
                bind_index += 1;
            }
            if filter.business_type.is_some() {
                conditions.push(format!("s.business_type = ${}", bind_index));
                bind_index += 1;
            }
            if filter.lifecycle_state.is_some() {
                conditions.push(format!("s.lifecycle_state = ${}", bind_index));
                bind_index += 1;
            }
            if filter.visibility.is_some() {
                conditions.push(format!("s.visibility = ${}", bind_index));
                bind_index += 1;
            }
            if filter.participant_user_id.is_some() {
                if !query.contains("conversation_participants") {
                    query.push_str(
                        "INNER JOIN conversation_participants sp2 ON s.tenant_id = sp2.tenant_id AND s.conversation_id = sp2.conversation_id\n",
                    );
                }
                conditions.push(format!("sp2.tenant_id = ${}", bind_index));
                bind_index += 1;
                conditions.push(format!("sp2.user_id = ${}", bind_index));
                bind_index += 1;
            }
        }

        // 默认过滤：排除已删除的会话
        conditions.push("s.lifecycle_state != 'deleted'".to_string());

        if !conditions.is_empty() {
            query.push_str("WHERE ");
            query.push_str(&conditions.join(" AND "));
        }

        // 构建ORDER BY子句
        if sort.is_empty() {
            query.push_str(" ORDER BY s.updated_at DESC");
        } else {
            let mut order_clauses = Vec::new();
            for s in sort {
                let direction = if s.ascending { "ASC" } else { "DESC" };
                let field = match s.field.as_str() {
                    "created_at" => "s.created_at",
                    "updated_at" => "s.updated_at",
                    "conversation_type" => "s.conversation_type",
                    "business_type" => "s.business_type",
                    _ => "s.updated_at", // 默认字段
                };
                order_clauses.push(format!("{} {}", field, direction));
            }
            query.push_str(" ORDER BY ");
            query.push_str(&order_clauses.join(", "));
        }

        // 添加LIMIT和OFFSET
        query.push_str(&format!(
            " LIMIT ${} OFFSET ${}",
            bind_index,
            bind_index + 1
        ));

        // 执行查询（使用query而不是query_as，因为动态SQL构建）
        let mut query_builder = sqlx::query(&query);

        // 首先绑定 tenant_id（必需）
        query_builder = query_builder.bind(tenant_id);

        if let Some(uid) = user_id {
            query_builder = query_builder.bind(tenant_id); // sp.tenant_id
            query_builder = query_builder.bind(uid); // sp.user_id
        }

        // 绑定过滤器参数
        for filter in filters {
            if let Some(ref st) = filter.conversation_type {
                query_builder = query_builder.bind(st);
            }
            if let Some(ref bt) = filter.business_type {
                query_builder = query_builder.bind(bt);
            }
            if let Some(ref ls) = filter.lifecycle_state {
                query_builder = query_builder.bind(ls.as_str());
            }
            if let Some(ref vis) = filter.visibility {
                query_builder = query_builder.bind(vis.as_proto());
            }
            if let Some(ref pid) = filter.participant_user_id {
                query_builder = query_builder.bind(tenant_id); // sp2.tenant_id
                query_builder = query_builder.bind(pid); // sp2.user_id
            }
        }

        query_builder = query_builder.bind(limit as i64).bind(offset as i64);

        let rows = query_builder
            .fetch_all(&*self.pool)
            .await
            .context("Failed to search conversations")?;

        // 转换为ConversationSummary
        let summaries: Vec<ConversationSummary> = rows
            .into_iter()
            .map(|row| {
                let conversation_id: String = row.get("conversation_id");
                let conversation_type: String = row.get("conversation_type");
                let business_type: String = row.get("business_type");
                let display_name: Option<String> = row.get("display_name");
                let attributes: Option<serde_json::Value> = row.get("attributes");
                let updated_at: DateTime<Utc> = row.get("updated_at");

                let attributes: HashMap<String, String> = attributes
                    .and_then(|v| serde_json::from_value(v).ok())
                    .unwrap_or_default();
                let server_cursor_ts = Some(updated_at.timestamp_millis());

                ConversationSummary {
                    conversation_id,
                    conversation_type: Some(conversation_type),
                    business_type: Some(business_type),
                    last_message_id: None,
                    last_message_time: None,
                    last_sender_id: None,
                    last_message_type: None,
                    last_content_type: None,
                    unread_count: 0, // 将在ApplicationService层通过MessageProvider精确计算
                    metadata: attributes,
                    server_cursor_ts,
                    display_name,
                }
            })
            .collect();

        // 查询总数（用于分页）
        // 注意：总数查询可能较慢，生产环境建议：
        // 1. 使用Redis缓存查询结果（TTL 5-10分钟）
        // 2. 使用近似值（如通过采样估算）
        // 3. 对于大用户，考虑使用分页而不显示总数
        let count_query = query.replace("SELECT DISTINCT", "SELECT COUNT(DISTINCT s.conversation_id)");
        let count_query = count_query.split("LIMIT").next().unwrap_or(&count_query);
        let mut count_builder = sqlx::query_scalar::<_, i64>(count_query);

        if let Some(uid) = user_id {
            count_builder = count_builder.bind(uid);
        }

        // 绑定过滤器参数（与上面相同）
        for filter in filters {
            if let Some(ref st) = filter.conversation_type {
                count_builder = count_builder.bind(st);
            }
            if let Some(ref bt) = filter.business_type {
                count_builder = count_builder.bind(bt);
            }
            if let Some(ref ls) = filter.lifecycle_state {
                count_builder = count_builder.bind(ls.as_str());
            }
            if let Some(ref vis) = filter.visibility {
                count_builder = count_builder.bind(vis.as_proto());
            }
            if let Some(ref pid) = filter.participant_user_id {
                count_builder = count_builder.bind(pid);
            }
        }

        let total = count_builder.fetch_one(&*self.pool).await.unwrap_or(0) as usize;

        Ok((summaries, total))
    }

    async fn mark_as_read(&self, ctx: &flare_server_core::context::Context, conversation_id: &str, seq: i64) -> Result<()> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let user_id = ctx.user_id().ok_or_else(|| anyhow::anyhow!("user_id is required in context"))?;
        // 更新 conversation_participants 的 last_read_seq 和 unread_count（init_v2 列名）
        sqlx::query(
            r#"
            UPDATE conversation_participants sp
            SET
                last_read_seq = $1,
                unread_count = GREATEST(0, COALESCE((
                    SELECT last_message_seq FROM conversations WHERE tenant_id = $2 AND conversation_id = $3
                ), 0) - $1),
                updated_at = CURRENT_TIMESTAMP
            WHERE sp.tenant_id = $2 AND sp.conversation_id = $3 AND sp.user_id = $4
            "#,
        )
        .bind(seq)
        .bind(tenant_id)
        .bind(conversation_id)
        .bind(user_id)
        .execute(&*self.pool)
        .await
        .context("Failed to mark as read")?;

        info!(
            user_id = %user_id,
            conversation_id = %conversation_id,
            seq,
            "Marked messages as read"
        );

        Ok(())
    }

    async fn get_last_message_seq(
        &self,
        ctx: &flare_server_core::context::Context,
        conversation_id: &str,
    ) -> Result<Option<i64>> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let row = sqlx::query(
            r#"SELECT last_message_seq FROM conversations WHERE tenant_id = $1 AND conversation_id = $2"#,
        )
        .bind(tenant_id)
        .bind(conversation_id)
        .fetch_optional(&*self.pool)
        .await
        .context("Failed to get last_message_seq")?;
        Ok(row.and_then(|r| r.get("last_message_seq")))
    }

    async fn get_unread_count(&self, ctx: &flare_server_core::context::Context, conversation_id: &str) -> Result<i32> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let user_id = ctx.user_id().ok_or_else(|| anyhow::anyhow!("user_id is required in context"))?;
        // 从 conversation_participants 表读取未读数
        let row = sqlx::query(
            r#"
            SELECT COALESCE(sp.unread_count, 0) as unread_count
            FROM conversation_participants sp
            WHERE sp.tenant_id = $1 AND sp.conversation_id = $2 AND sp.user_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(conversation_id)
        .bind(user_id)
        .fetch_optional(&*self.pool)
        .await
        .context("Failed to get unread count")?;

        let unread_count = if let Some(row) = row {
            row.get("unread_count")
        } else {
            0
        };

        Ok(unread_count)
    }
}
