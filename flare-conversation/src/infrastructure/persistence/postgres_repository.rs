//! # PostgreSQL Conversation Repository
//!
//! PostgreSQL持久化层实现，用于会话元数据存储

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result, map_infra_error, require_user_id};
use sqlx::{PgPool, Row};
use tracing::debug;

use crate::config::ConversationConfig;
use crate::domain::model::{
    Conversation, ConversationBootstrapResult, ConversationFilter, ConversationParticipant,
    ConversationParticipantsPage, ConversationSort, ConversationSummary, ConversationType,
};
use crate::domain::repository::ConversationRepository;

/// 会话查询行结构（与 init_v2 一致：visibility 为 INT）
#[derive(sqlx::FromRow)]
#[allow(dead_code)]
struct ConversationRow {
    conversation_id: String,
    conversation_type: i32,
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

    /// 单聊库中 `channel_id` 为空：按当前用户从 participants 解析对端，仅写入内存摘要（不下发进 DB 列）。
    async fn fill_single_chat_channel_ids(
        pool: &PgPool,
        tenant_id: &str,
        current_user_id: &str,
        summaries: &mut [ConversationSummary],
    ) -> Result<()> {
        let need_peer: Vec<String> = summaries
            .iter()
            .filter(|s| {
                if !s.channel_id.is_empty() {
                    return false;
                }
                match s.conversation_type {
                    ConversationType::Single => true,
                    ConversationType::Unspecified => s.conversation_id.starts_with("1A"),
                    _ => false,
                }
            })
            .map(|s| s.conversation_id.clone())
            .collect();

        if need_peer.is_empty() {
            return Ok(());
        }

        let rows = sqlx::query(
            r#"
            SELECT cp.conversation_id::text AS conversation_id, cp.user_id::text AS user_id
            FROM conversation_participants cp
            WHERE cp.tenant_id = $1
              AND cp.conversation_id = ANY($2)
              AND cp.user_id <> $3
            "#,
        )
        .bind(tenant_id)
        .bind(&need_peer)
        .bind(current_user_id)
        .fetch_all(pool)
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "fill single chat channel_id"))?;

        let mut peer_by_cid: HashMap<String, String> = HashMap::new();
        for row in rows {
            let cid: String = row.get("conversation_id");
            let uid: String = row.get("user_id");
            peer_by_cid.entry(cid).or_insert(uid);
        }

        for s in summaries.iter_mut() {
            if s.channel_id.is_empty()
                && let Some(peer) = peer_by_cid.get(&s.conversation_id)
            {
                s.channel_id.clone_from(peer);
                if s.display_name
                    .as_deref()
                    .unwrap_or_default()
                    .trim()
                    .is_empty()
                {
                    s.display_name = Some(peer.clone());
                }
            }
        }

        Ok(())
    }

    /// 摘要只带非单聊成员预览和版本，完整成员由独立成员同步按需/空闲拉取。
    async fn fill_non_single_member_preview(
        pool: &PgPool,
        tenant_id: &str,
        summaries: &mut [ConversationSummary],
    ) -> Result<()> {
        let need_participants: Vec<String> = summaries
            .iter()
            .filter(|s| !matches!(s.conversation_type, ConversationType::Single))
            .map(|s| s.conversation_id.clone())
            .collect();

        if need_participants.is_empty() {
            return Ok(());
        }

        let rows = sqlx::query(
            r#"
            SELECT
                cp.conversation_id::text AS conversation_id,
                cp.user_id::text AS user_id,
                COALESCE(cp.roles, ARRAY[]::text[]) AS roles,
                COALESCE(cp.muted, false) AS muted,
                COALESCE(cp.pinned, false) AS pinned,
                COALESCE(cp.attributes, '{}'::jsonb) AS attributes,
                COALESCE(cp.nickname, '') AS nickname
            FROM conversation_participants cp
            WHERE cp.tenant_id = $1
              AND cp.conversation_id = ANY($2)
              AND NOT COALESCE(cp.is_deleted, false)
              AND cp.quit_at IS NULL
            ORDER BY cp.conversation_id, cp.joined_at ASC, cp.user_id ASC
            "#,
        )
        .bind(tenant_id)
        .bind(&need_participants)
        .fetch_all(pool)
        .await
        .map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "fill conversation participants",
            )
        })?;

        let mut by_cid: HashMap<String, Vec<ConversationParticipant>> = HashMap::new();
        for row in rows {
            let cid: String = row.get("conversation_id");
            let mut attributes: HashMap<String, String> = row
                .get::<serde_json::Value, _>("attributes")
                .as_object()
                .map(|obj| {
                    obj.iter()
                        .map(|(k, v)| {
                            (
                                k.clone(),
                                v.as_str()
                                    .map(ToOwned::to_owned)
                                    .unwrap_or_else(|| v.to_string()),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            let nickname: String = row.get("nickname");
            if !nickname.trim().is_empty() {
                attributes.insert("nickname".to_string(), nickname);
            }
            by_cid
                .entry(cid)
                .or_default()
                .push(ConversationParticipant {
                    user_id: row.get("user_id"),
                    roles: row.get::<Vec<String>, _>("roles"),
                    muted: row.get("muted"),
                    pinned: row.get("pinned"),
                    attributes,
                });
        }

        for summary in summaries.iter_mut() {
            if let Some(participants) = by_cid.remove(&summary.conversation_id) {
                summary
                    .metadata
                    .insert("member_count".to_string(), participants.len().to_string());
                summary.participant_version = participants.len() as u64;
                summary.member_preview = participants.into_iter().take(10).collect();
            }
        }

        Ok(())
    }
}

impl ConversationRepository for PostgresConversationRepository {
    async fn load_bootstrap(
        &self,
        ctx: &flare_server_core::context::Context,
        client_cursor: &HashMap<String, i64>,
    ) -> Result<ConversationBootstrapResult> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let user_id = require_user_id(ctx)?;
        // 1. 从 user_sync_cursor 表加载按会话消息 seq 游标。
        // last_synced_ts 只服务于 __conversations__ 这种列表时间游标，不能参与单会话消息游标。
        let cursor_rows = sqlx::query(
            r#"
            SELECT
                conversation_id,
                CASE
                    WHEN conversation_id = '__conversations__'
                    THEN COALESCE(last_synced_ts, 0)
                    ELSE GREATEST(
                        COALESCE(last_synced_seq, 0),
                        CASE
                            WHEN COALESCE(last_synced_ts, 0) < 1000000000000
                            THEN COALESCE(last_synced_ts, 0)
                            ELSE 0
                        END
                    )
                END AS sync_cursor
            FROM user_sync_cursor
            WHERE tenant_id = $1 AND user_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(&user_id)
        .fetch_all(&*self.pool)
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to load user cursors"))?;

        let mut server_cursor: HashMap<String, i64> = cursor_rows
            .into_iter()
            .map(|row| {
                let conversation_id: String = row.get("conversation_id");
                let cursor: i64 = row.get("sync_cursor");
                (conversation_id, cursor)
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
                GREATEST(
                    s.updated_at,
                    sp.joined_at,
                    sp.updated_at,
                    COALESCE(message_tail.last_message_at, s.updated_at)
                ) AS effective_updated_at,
                GREATEST(
                    COALESCE(s.last_message_seq, 0),
                    COALESCE(message_tail.max_seq, 0)
                ) AS last_message_seq,
                COALESCE(s.channel_id, '') as channel_id,
                COALESCE(sp.last_read_seq, 0) as last_read_seq,
                GREATEST(
                    COALESCE(sp.unread_count, 0),
                    COALESCE(unread_tail.unread_count, 0)
                ) as unread_count,
                COALESCE(sp.muted, false) as muted,
                COALESCE(sp.pinned, false) as pinned,
                COALESCE(sp.is_archived, false) as is_archived,
                COALESCE(sp.settings_version, 0) as settings_version,
                sp.draft as draft,
                -- visible_after_seq 是历史可见边界，不是同步游标。
                -- 当前服务端未持久化用户级清空历史边界，不能用 last_sync_seq / user_sync_cursor 代替。
                0::BIGINT AS visible_after_seq,
                CASE
                    WHEN COALESCE(peer.max_peer_read_seq, 0) <= GREATEST(
                        COALESCE(s.last_message_seq, 0),
                        COALESCE(message_tail.max_seq, 0),
                        0
                    )
                    THEN COALESCE(peer.max_peer_read_seq, 0)
                    ELSE 0
                END AS peer_read_seq
            FROM conversations s
            INNER JOIN conversation_participants sp ON s.tenant_id = sp.tenant_id AND s.conversation_id = sp.conversation_id
            LEFT JOIN LATERAL (
                SELECT MAX(COALESCE(sp2.last_read_seq, 0)) AS max_peer_read_seq
                FROM conversation_participants sp2
                WHERE sp2.tenant_id = s.tenant_id
                  AND sp2.conversation_id = s.conversation_id
                  AND sp2.user_id <> $2
                  AND NOT COALESCE(sp2.is_deleted, false)
            ) peer ON TRUE
            LEFT JOIN LATERAL (
                SELECT MAX(m.seq) AS max_seq, MAX(m.timestamp) AS last_message_at
                FROM messages m
                WHERE m.tenant_id = s.tenant_id
                  AND m.conversation_id = s.conversation_id
            ) message_tail ON TRUE
            LEFT JOIN LATERAL (
                SELECT COUNT(1)::INT AS unread_count
                FROM messages m
                WHERE m.tenant_id = s.tenant_id
                  AND m.conversation_id = s.conversation_id
                  AND m.seq > COALESCE(sp.last_read_seq, 0)
                  AND m.sender_id <> $2
                  AND COALESCE(m.status, 1) NOT IN (6, 7, 8)
            ) unread_tail ON TRUE
            WHERE s.tenant_id = $1
              AND sp.tenant_id = $1
              AND sp.user_id = $2
              AND s.lifecycle_state != 'deleted'
              AND NOT COALESCE(sp.is_deleted, false)
            ORDER BY effective_updated_at DESC
            "#,
        )
        .bind(tenant_id)
        .bind(&user_id)
        .fetch_all(&*self.pool)
        .await
        .map_err(|e| {
            map_infra_error(e, ErrorCode::DatabaseError, "Failed to load user conversations")
        })?;

        let mut summaries = Vec::new();

        for row in session_rows {
            let conversation_id: String = row.get("conversation_id");
            let conversation_type: Option<i32> = row.get("conversation_type");
            let business_type: Option<String> = row.get("business_type");
            let display_name: Option<String> = row.get("display_name");
            let attributes: Option<serde_json::Value> = row.get("attributes");
            let effective_updated_at: DateTime<Utc> = row.get("effective_updated_at");

            // 从数据库读取未读数相关字段
            let last_message_seq: Option<i64> = row.get("last_message_seq");
            let channel_id: String = row.get("channel_id");
            let last_read_seq: i64 = row.get("last_read_seq");
            let unread_count: i32 = row.get("unread_count");
            let is_muted: bool = row.get("muted");
            let is_pinned: bool = row.get("pinned");
            let is_archived: bool = row.get("is_archived");
            let settings_version: i64 = row.get("settings_version");
            let draft: Option<String> = row.get("draft");
            let visible_after_seq: i64 = row.get("visible_after_seq");
            let peer_read_seq: i64 = row.get("peer_read_seq");

            let attributes: HashMap<String, String> = attributes
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or_default();
            let mut attributes = attributes;
            attributes.insert(
                "peer_read_seq".to_string(),
                peer_read_seq.max(0).to_string(),
            );
            attributes.insert("is_muted".to_string(), is_muted.to_string());
            attributes.insert("is_pinned".to_string(), is_pinned.to_string());
            attributes.insert("is_archived".to_string(), is_archived.to_string());
            attributes.insert(
                "user_settings_version".to_string(),
                settings_version.max(0).to_string(),
            );
            if let Some(d) = draft.as_ref().filter(|v| !v.is_empty()) {
                attributes.insert("draft".to_string(), d.clone());
            }

            // 注释：最后一条消息信息将在ApplicationService层通过MessageProvider补充
            // server_cursor_ts 必须表达「会话列表行更新时间」，供 sync snapshot 做增量过滤。
            // user_sync_cursor 同时存放单会话消息 seq，不能覆盖这里，否则会把 seq(如 884)
            // 当作毫秒时间游标，导致客户端误判“没有会话数据”。
            let server_cursor_ts = Some(effective_updated_at.timestamp_millis());

            let summary = ConversationSummary {
                conversation_id,
                conversation_type: conversation_type
                    .map(ConversationType::from_int)
                    .unwrap_or(ConversationType::Unspecified),
                business_type,
                last_message_id: None,   // 将在ApplicationService层补充
                last_message_time: None, // 将在ApplicationService层补充
                last_sender_id: None,    // 将在ApplicationService层补充
                last_message_type: None, // 将在ApplicationService层补充
                last_content_type: None, // 将在ApplicationService层补充
                last_message_preview: None,
                unread_count,
                last_read_seq,
                metadata: attributes,
                server_cursor_ts,
                display_name,
                last_message_seq,
                channel_id,
                participant_version: 0,
                member_preview: Vec::new(),
                is_muted,
                is_pinned,
                is_archived,
                settings_version: settings_version.max(0) as u64,
                draft,
                visible_after_seq: visible_after_seq.max(0),
            };

            summaries.push(summary);
        }

        Self::fill_single_chat_channel_ids(&self.pool, tenant_id, &user_id, &mut summaries).await?;
        Self::fill_non_single_member_preview(&self.pool, tenant_id, &mut summaries).await?;

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

    async fn update_cursor(
        &self,
        ctx: &flare_server_core::context::Context,
        conversation_id: &str,
        sync_seq: i64,
    ) -> Result<()> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let user_id = require_user_id(ctx)?;
        if conversation_id == "__conversations__" {
            sqlx::query(
                r#"
                INSERT INTO user_sync_cursor (tenant_id, user_id, conversation_id, last_synced_seq, last_synced_ts, updated_at)
                VALUES ($1, $2, $3, 0, $4, CURRENT_TIMESTAMP)
                ON CONFLICT (tenant_id, user_id, conversation_id)
                DO UPDATE SET
                    last_synced_seq = 0,
                    last_synced_ts = GREATEST(COALESCE(user_sync_cursor.last_synced_ts, 0), EXCLUDED.last_synced_ts),
                    updated_at = CURRENT_TIMESTAMP
                "#,
            )
            .bind(tenant_id)
            .bind(&user_id)
            .bind(conversation_id)
            .bind(sync_seq)
            .execute(&*self.pool)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to update conversation list cursor"))?;
        } else {
            let synced_at_ms = Utc::now().timestamp_millis();
            sqlx::query(
                r#"
                INSERT INTO user_sync_cursor (tenant_id, user_id, conversation_id, last_synced_seq, last_synced_ts, updated_at)
                VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
                ON CONFLICT (tenant_id, user_id, conversation_id)
                DO UPDATE SET
                    last_synced_seq = GREATEST(COALESCE(user_sync_cursor.last_synced_seq, 0), EXCLUDED.last_synced_seq),
                    last_synced_ts = GREATEST(COALESCE(user_sync_cursor.last_synced_ts, 0), EXCLUDED.last_synced_ts),
                    updated_at = CURRENT_TIMESTAMP
                "#,
            )
            .bind(tenant_id)
            .bind(&user_id)
            .bind(conversation_id)
            .bind(sync_seq)
            .bind(synced_at_ms)
            .execute(&*self.pool)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to update message sync cursor"))?;
        }

        Ok(())
    }

    async fn create_conversation(
        &self,
        ctx: &flare_server_core::context::Context,
        session: &Conversation,
    ) -> Result<()> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        if session.conversation_id.trim().is_empty() {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "conversation_id is required",
            )
            .build_error());
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "begin transaction"))?;

        // 插入会话记录（幂等：ON CONFLICT DO NOTHING，支持异步 conversation.ensure 事件并发消费）
        let result = sqlx::query(
            r#"
            INSERT INTO conversations (
                tenant_id, conversation_id, conversation_type, business_type, display_name,
                attributes, visibility, lifecycle_state, channel_id, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (tenant_id, conversation_id) DO NOTHING
            "#,
        )
        .bind(tenant_id)
        .bind(&session.conversation_id)
        .bind(session.conversation_type.as_int())
        .bind(&session.business_type)
        .bind(&session.display_name)
        .bind(serde_json::to_value(&session.attributes).map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::SerializationError,
                "serialize session attributes",
            )
        })?)
        .bind(session.visibility.as_proto())
        .bind(session.lifecycle_state.as_str())
        .bind(&session.channel_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            map_infra_error(e, ErrorCode::DatabaseError, "Failed to create conversation")
        })?;
        if result.rows_affected() > 0 {
            debug!(conversation_id = %session.conversation_id, "Conversation row inserted");
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
                    is_deleted = FALSE,
                    updated_at = CURRENT_TIMESTAMP
                "#,
            )
            .bind(tenant_id)
            .bind(&session.conversation_id)
            .bind(&participant.user_id)
            .bind(&participant.roles)
            .bind(participant.muted)
            .bind(participant.pinned)
            .bind(
                serde_json::to_value(&participant.attributes).map_err(|e| {
                    map_infra_error(e, ErrorCode::SerializationError, "serialize participant attributes")
                })?,
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to create participant"))?;
        }

        tx.commit()
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "commit transaction"))?;
        debug!(conversation_id = %session.conversation_id, "Conversation created");
        Ok(())
    }

    async fn get_conversation(
        &self,
        ctx: &flare_server_core::context::Context,
        conversation_id: &str,
    ) -> Result<Option<Conversation>> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let row = sqlx::query(
            r#"
            SELECT conversation_id, conversation_type, business_type, display_name,
                   attributes, visibility, lifecycle_state, channel_id,
                   created_at, updated_at
            FROM conversations
            WHERE tenant_id = $1 AND conversation_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(conversation_id)
        .fetch_optional(&*self.pool)
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to get conversation"))?;

        let Some(row) = row else {
            return Ok(None);
        };

        let conversation_id: String = row.get("conversation_id");
        let conversation_type_raw: i32 = row.get("conversation_type");
        let conversation_type = ConversationType::from_int(conversation_type_raw);
        let business_type: String = row.get("business_type");
        let channel_id: String = row.get("channel_id");
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
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to get participants"))?;

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
            channel_id,
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

    async fn update_conversation(
        &self,
        ctx: &flare_server_core::context::Context,
        session: &Conversation,
    ) -> Result<()> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        sqlx::query(
            r#"
            UPDATE conversations
            SET display_name = $1,
                attributes = $2,
                visibility = $3,
                lifecycle_state = $4,
                channel_id = $5,
                updated_at = CURRENT_TIMESTAMP
            WHERE tenant_id = $6 AND conversation_id = $7
            "#,
        )
        .bind(&session.display_name)
        .bind(serde_json::to_value(&session.attributes).map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::SerializationError,
                "serialize session attributes",
            )
        })?)
        .bind(session.visibility.as_proto())
        .bind(session.lifecycle_state.as_str())
        .bind(&session.channel_id)
        .bind(tenant_id)
        .bind(&session.conversation_id)
        .execute(&*self.pool)
        .await
        .map_err(|e| {
            map_infra_error(e, ErrorCode::DatabaseError, "Failed to update conversation")
        })?;

        debug!(conversation_id = %session.conversation_id, "Conversation updated");
        Ok(())
    }

    async fn delete_conversation(
        &self,
        ctx: &flare_server_core::context::Context,
        conversation_id: &str,
        hard_delete: bool,
    ) -> Result<()> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        if hard_delete {
            // 物理删除：消息/事件与元数据一并清理，避免同 conversation_id 重建后 sync 拉回旧历史。
            let mut tx =
                self.pool.begin().await.map_err(|e| {
                    map_infra_error(e, ErrorCode::DatabaseError, "begin transaction")
                })?;

            for stmt in [
                "DELETE FROM pinned_messages WHERE tenant_id = $1 AND conversation_id = $2",
                "DELETE FROM marked_messages WHERE tenant_id = $1 AND conversation_id = $2",
                "DELETE FROM events WHERE tenant_id = $1 AND conversation_id = $2",
                "DELETE FROM messages WHERE tenant_id = $1 AND conversation_id = $2",
            ] {
                sqlx::query(stmt)
                    .bind(tenant_id)
                    .bind(conversation_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| {
                        map_infra_error(
                            e,
                            ErrorCode::DatabaseError,
                            "Failed to purge conversation messages",
                        )
                    })?;
            }

            sqlx::query("DELETE FROM conversations WHERE tenant_id = $1 AND conversation_id = $2")
                .bind(tenant_id)
                .bind(conversation_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    map_infra_error(e, ErrorCode::DatabaseError, "Failed to delete conversation")
                })?;

            tx.commit().await.map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::DatabaseError,
                    "commit conversation hard delete",
                )
            })?;
        } else {
            // 用户侧软删除：只隐藏当前用户的会话视图，并把消息同步游标推进到当前尾部。
            // 这保留全局会话与对端视图；同一用户重新加入/重建会话后不会重新拉回删除前历史。
            let user_id = require_user_id(ctx)?;
            let mut tx = self.pool.begin().await.map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::DatabaseError,
                    "begin conversation soft delete",
                )
            })?;

            sqlx::query(
                r#"
                WITH tail AS (
                    SELECT COALESCE(last_message_seq, 0) AS max_seq
                    FROM conversations
                    WHERE tenant_id = $1 AND conversation_id = $2
                ),
                participant AS (
                    UPDATE conversation_participants sp
                    SET
                        is_deleted = TRUE,
                        is_archived = FALSE,
                        draft = NULL,
                        unread_count = 0,
                        last_read_seq = GREATEST(COALESCE(sp.last_read_seq, 0), (SELECT max_seq FROM tail)),
                        last_sync_seq = GREATEST(COALESCE(sp.last_sync_seq, 0), (SELECT max_seq FROM tail)),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE sp.tenant_id = $1
                      AND sp.conversation_id = $2
                      AND sp.user_id = $3
                    RETURNING (SELECT max_seq FROM tail) AS max_seq
                )
                INSERT INTO user_sync_cursor (
                    tenant_id, user_id, conversation_id, last_synced_seq, last_synced_ts, updated_at
                )
                SELECT $1, $3, $2, GREATEST(COALESCE(max_seq, 0), 0), (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000)::BIGINT, CURRENT_TIMESTAMP
                FROM participant
                ON CONFLICT (tenant_id, user_id, conversation_id)
                DO UPDATE SET
                    last_synced_seq = GREATEST(COALESCE(user_sync_cursor.last_synced_seq, 0), EXCLUDED.last_synced_seq),
                    last_synced_ts = GREATEST(COALESCE(user_sync_cursor.last_synced_ts, 0), EXCLUDED.last_synced_ts),
                    updated_at = CURRENT_TIMESTAMP
                "#,
            )
            .bind(tenant_id)
            .bind(conversation_id)
            .bind(&user_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::DatabaseError, "Failed to soft delete conversation")
            })?;

            tx.commit().await.map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::DatabaseError,
                    "commit conversation soft delete",
                )
            })?;
        }

        debug!(conversation_id = %conversation_id, hard_delete = hard_delete, "Conversation deleted");
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
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "begin transaction"))?;

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
                    is_deleted = FALSE,
                    quit_at = NULL,
                    updated_at = CURRENT_TIMESTAMP
                "#,
            )
            .bind(tenant_id)
            .bind(conversation_id)
            .bind(&participant.user_id)
            .bind(&participant.roles)
            .bind(participant.muted)
            .bind(participant.pinned)
            .bind(
                serde_json::to_value(&participant.attributes).map_err(|e| {
                    map_infra_error(e, ErrorCode::SerializationError, "serialize participant attributes")
                })?,
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to add participant"))?;
        }

        // 删除参与者
        for user_id in to_remove {
            sqlx::query("DELETE FROM conversation_participants WHERE tenant_id = $1 AND conversation_id = $2 AND user_id = $3")
                .bind(tenant_id)
                .bind(conversation_id)
                .bind(user_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to remove participant"))?;
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
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::DatabaseError,
                    "Failed to update participant roles",
                )
            })?;
        }

        tx.commit()
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "commit transaction"))?;

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
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to get participants"))?;

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

    async fn list_conversation_participants(
        &self,
        ctx: &flare_server_core::context::Context,
        conversation_id: &str,
        cursor: Option<&str>,
        limit: i32,
        include_removed: bool,
    ) -> Result<ConversationParticipantsPage> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let user_id = require_user_id(ctx)?;
        let conversation_id = conversation_id.trim();
        if conversation_id.is_empty() {
            return Err(flare_server_core::error::ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "conversation_id is required",
            )
            .build_error());
        }
        let offset = cursor
            .and_then(|v| v.trim().parse::<i64>().ok())
            .unwrap_or_default()
            .max(0);
        let limit = limit.clamp(1, 500);

        let membership_exists: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT 1::BIGINT
            FROM conversation_participants
            WHERE tenant_id = $1
              AND conversation_id = $2
              AND user_id = $3
              AND NOT COALESCE(is_deleted, false)
              AND quit_at IS NULL
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(conversation_id)
        .bind(&user_id)
        .fetch_optional(&*self.pool)
        .await
        .map_err(|e| {
            map_infra_error(e, ErrorCode::DatabaseError, "check participant membership")
        })?;
        if membership_exists.is_none() {
            return Err(flare_server_core::error::ErrorBuilder::new(
                ErrorCode::MessageNotFound,
                "conversation not found",
            )
            .build_error());
        }

        let active_filter = if include_removed {
            ""
        } else {
            "AND NOT COALESCE(is_deleted, false) AND quit_at IS NULL"
        };
        let count_sql = format!(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM conversation_participants
            WHERE tenant_id = $1
              AND conversation_id = $2
              {active_filter}
            "#
        );
        let total: i64 = sqlx::query_scalar(&count_sql)
            .bind(tenant_id)
            .bind(conversation_id)
            .fetch_one(&*self.pool)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "count participants"))?;

        let version_sql = r#"
            SELECT COALESCE(EXTRACT(EPOCH FROM MAX(updated_at)) * 1000, 0)::BIGINT
            FROM conversation_participants
            WHERE tenant_id = $1
              AND conversation_id = $2
        "#;
        let participant_version: i64 = sqlx::query_scalar(version_sql)
            .bind(tenant_id)
            .bind(conversation_id)
            .fetch_one(&*self.pool)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "participant version"))?;

        let list_sql = format!(
            r#"
            SELECT
                user_id::text AS user_id,
                COALESCE(roles, ARRAY[]::text[]) AS roles,
                COALESCE(muted, false) AS muted,
                COALESCE(pinned, false) AS pinned,
                COALESCE(attributes, '{{}}'::jsonb) AS attributes,
                joined_at,
                COALESCE(nickname, '') AS nickname
            FROM conversation_participants
            WHERE tenant_id = $1
              AND conversation_id = $2
              {active_filter}
            ORDER BY joined_at ASC, user_id ASC
            LIMIT $3 OFFSET $4
            "#
        );
        let rows = sqlx::query(&list_sql)
            .bind(tenant_id)
            .bind(conversation_id)
            .bind(limit as i64)
            .bind(offset)
            .fetch_all(&*self.pool)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "list participants"))?;

        let participants = rows
            .into_iter()
            .map(|row| {
                let mut attributes: HashMap<String, String> = row
                    .get::<serde_json::Value, _>("attributes")
                    .as_object()
                    .map(|obj| {
                        obj.iter()
                            .map(|(k, v)| {
                                (
                                    k.clone(),
                                    v.as_str()
                                        .map(ToOwned::to_owned)
                                        .unwrap_or_else(|| v.to_string()),
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let nickname: String = row.get("nickname");
                if !nickname.trim().is_empty() {
                    attributes.insert("nickname".to_string(), nickname.clone());
                }
                ConversationParticipant {
                    user_id: row.get("user_id"),
                    roles: row.get("roles"),
                    muted: row.get("muted"),
                    pinned: row.get("pinned"),
                    attributes,
                }
            })
            .collect::<Vec<_>>();
        let next_offset = offset + participants.len() as i64;
        let has_more = next_offset < total;

        Ok(ConversationParticipantsPage {
            conversation_id: conversation_id.to_string(),
            participants,
            next_cursor: if has_more {
                Some(next_offset.to_string())
            } else {
                None
            },
            has_more,
            participant_version: participant_version.max(0) as u64,
            member_count: total.max(0) as i32,
        })
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
        let mut query = if user_id.is_some() {
            String::from(
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
                COALESCE(s.channel_id, '') as channel_id,
                COALESCE(s.last_message_seq, 0) as last_message_seq,
                COALESCE(sp.unread_count, 0) as unread_count,
                COALESCE(sp.last_read_seq, 0) as last_read_seq
            FROM conversations s
            "#,
            )
        } else {
            String::from(
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
                COALESCE(s.channel_id, '') as channel_id,
                COALESCE(s.last_message_seq, 0) as last_message_seq,
                0 as unread_count,
                0 as last_read_seq
            FROM conversations s
            "#,
            )
        };

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
            conditions.push("NOT COALESCE(sp.is_deleted, false)".to_string());
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
                query_builder = query_builder.bind(st.as_int());
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

        let rows = query_builder.fetch_all(&*self.pool).await.map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "Failed to search conversations",
            )
        })?;

        // 转换为ConversationSummary
        let mut summaries: Vec<ConversationSummary> = rows
            .into_iter()
            .map(|row| {
                let conversation_id: String = row.get("conversation_id");
                let conversation_type: i32 = row.get("conversation_type");
                let business_type: String = row.get("business_type");
                let display_name: Option<String> = row.get("display_name");
                let attributes: Option<serde_json::Value> = row.get("attributes");
                let updated_at: DateTime<Utc> = row.get("updated_at");
                let channel_id: String = row.get("channel_id");
                let last_message_seq: i64 = row.get("last_message_seq");
                let unread_count: i32 = row.get("unread_count");
                let last_read_seq: i64 = row.get("last_read_seq");

                let attributes: HashMap<String, String> = attributes
                    .and_then(|v| serde_json::from_value(v).ok())
                    .unwrap_or_default();
                let server_cursor_ts = Some(updated_at.timestamp_millis());

                ConversationSummary {
                    conversation_id,
                    conversation_type: ConversationType::from_int(conversation_type),
                    business_type: Some(business_type),
                    last_message_id: None,
                    last_message_time: None,
                    last_sender_id: None,
                    last_message_type: None,
                    last_content_type: None,
                    last_message_preview: None,
                    unread_count,
                    last_read_seq: last_read_seq.max(0),
                    metadata: attributes,
                    server_cursor_ts,
                    display_name,
                    last_message_seq: Some(last_message_seq.max(0)),
                    channel_id,
                    participant_version: 0,
                    member_preview: Vec::new(),
                    is_muted: false,
                    is_pinned: false,
                    is_archived: false,
                    settings_version: 0,
                    draft: None,
                    visible_after_seq: 0,
                }
            })
            .collect();

        if let Some(uid) = user_id {
            Self::fill_single_chat_channel_ids(&self.pool, tenant_id, uid, &mut summaries).await?;
        }
        Self::fill_non_single_member_preview(&self.pool, tenant_id, &mut summaries).await?;

        // 查询总数（用于分页）
        // 注意：总数查询可能较慢，生产环境建议：
        // 1. 使用Redis缓存查询结果（TTL 5-10分钟）
        // 2. 使用近似值（如通过采样估算）
        // 3. 对于大用户，考虑使用分页而不显示总数
        let count_query = query.replace(
            "SELECT DISTINCT",
            "SELECT COUNT(DISTINCT s.conversation_id)",
        );
        let count_query = count_query.split("LIMIT").next().unwrap_or(&count_query);
        let mut count_builder = sqlx::query_scalar::<_, i64>(count_query);

        count_builder = count_builder.bind(tenant_id);

        if let Some(uid) = user_id {
            count_builder = count_builder.bind(tenant_id); // sp.tenant_id
            count_builder = count_builder.bind(uid);
        }

        // 绑定过滤器参数（与上面相同）
        for filter in filters {
            if let Some(ref st) = filter.conversation_type {
                count_builder = count_builder.bind(st.as_int());
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
                count_builder = count_builder.bind(tenant_id); // sp2.tenant_id
                count_builder = count_builder.bind(pid);
            }
        }

        let total = count_builder.fetch_one(&*self.pool).await.unwrap_or(0) as usize;

        Ok((summaries, total))
    }

    async fn mark_as_read(
        &self,
        ctx: &flare_server_core::context::Context,
        conversation_id: &str,
        seq: i64,
    ) -> Result<()> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let user_id = require_user_id(ctx)?;
        // 更新 conversation_participants 的 last_read_seq 和 unread_count（init_v2 列名）
        let updated = sqlx::query(
            r#"
            WITH conv_state AS (
                SELECT
                    COALESCE(sp.last_read_seq, 0) AS prev_read_seq,
                    COALESCE(sp.unread_count, 0) AS prev_unread_count,
                    GREATEST(
                        COALESCE(c.last_message_seq, 0),
                        COALESCE(mx.max_seq, 0),
                        $1
                    ) AS max_seq
                FROM conversation_participants sp
                LEFT JOIN conversations c
                    ON c.tenant_id = sp.tenant_id
                   AND c.conversation_id = sp.conversation_id
                LEFT JOIN LATERAL (
                    SELECT m.seq AS max_seq
                    FROM messages m
                    WHERE m.tenant_id = sp.tenant_id
                      AND m.conversation_id = sp.conversation_id
                    ORDER BY m.seq DESC
                    LIMIT 1
                ) mx ON TRUE
                WHERE sp.tenant_id = $2
                  AND sp.conversation_id = $3
                  AND sp.user_id = $4
            ),
            target AS (
                SELECT
                    prev_read_seq,
                    prev_unread_count,
                    max_seq,
                    LEAST(
                        GREATEST(
                            prev_read_seq,
                            $1
                        ),
                        max_seq
                    ) AS next_read_seq
                FROM conv_state
            )
            UPDATE conversation_participants sp
            SET
                last_read_seq = target.next_read_seq,
                unread_count = CASE
                    WHEN target.next_read_seq <= target.prev_read_seq THEN GREATEST(target.prev_unread_count, 0)
                    WHEN target.next_read_seq >= target.max_seq THEN 0
                    ELSE GREATEST(
                        target.prev_unread_count - COALESCE((
                            SELECT COUNT(1)::INT
                            FROM messages m
                            WHERE m.tenant_id = $2
                              AND m.conversation_id = $3
                              AND m.seq > target.prev_read_seq
                              AND m.seq <= target.next_read_seq
                              AND m.sender_id <> $4
                              AND COALESCE(m.status, 1) NOT IN (6, 7, 8)
                        ), 0),
                        0
                    )
                END,
                updated_at = CURRENT_TIMESTAMP
            FROM target
            WHERE sp.tenant_id = $2
              AND sp.conversation_id = $3
              AND sp.user_id = $4
            RETURNING
                target.prev_read_seq AS prev_read_seq,
                target.next_read_seq AS next_read_seq,
                target.max_seq AS max_seq,
                target.prev_unread_count AS prev_unread_count,
                sp.unread_count AS next_unread_count
            "#,
        )
        .bind(seq)
        .bind(tenant_id)
        .bind(conversation_id)
        .bind(&user_id)
        .fetch_optional(&*self.pool)
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to mark as read"))?;

        if let Some(row) = updated {
            let prev_read_seq = row.get::<i64, _>("prev_read_seq");
            let next_read_seq = row.get::<i64, _>("next_read_seq");
            let max_seq = row.get::<i64, _>("max_seq");
            let prev_unread_count = row.get::<i32, _>("prev_unread_count");
            let next_unread_count = row.get::<i32, _>("next_unread_count");
            debug!(
                user_id = %user_id,
                conversation_id = %conversation_id,
                seq,
                prev_read_seq,
                next_read_seq,
                max_seq,
                prev_unread_count,
                next_unread_count,
                "Marked messages as read"
            );
        } else {
            debug!(
                user_id = %user_id,
                conversation_id = %conversation_id,
                seq,
                "Marked messages as read skipped (participant not found)"
            );
        }

        Ok(())
    }

    async fn update_user_settings(
        &self,
        ctx: &flare_server_core::context::Context,
        patch: &crate::domain::model::UpdateConversationUserSettingsPatch,
    ) -> Result<crate::domain::model::ConversationUserSettings> {
        use crate::domain::model::ConversationUserSettings;

        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let user_id = require_user_id(ctx)?;

        let has_patch = patch.is_pinned.is_some()
            || patch.is_muted.is_some()
            || patch.is_archived.is_some()
            || patch.draft.is_some();
        if !has_patch {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "at least one user setting field is required",
            )
            .build_error());
        }

        let row = sqlx::query(
            r#"
            UPDATE conversation_participants sp
            SET
                pinned = CASE WHEN $5::bool IS NOT NULL THEN $5 ELSE sp.pinned END,
                muted = CASE WHEN $6::bool IS NOT NULL THEN $6 ELSE sp.muted END,
                is_archived = CASE WHEN $7::bool IS NOT NULL THEN $7 ELSE sp.is_archived END,
                draft = CASE
                    WHEN $8::text IS NULL THEN sp.draft
                    WHEN $8 = '' THEN NULL
                    ELSE $8
                END,
                settings_version = sp.settings_version + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE sp.tenant_id = $1
              AND sp.conversation_id = $2
              AND sp.user_id = $3
              AND NOT COALESCE(sp.is_deleted, false)
              AND ($4 = 0 OR sp.settings_version = $4)
            RETURNING sp.pinned, sp.muted, sp.is_archived, sp.draft, sp.settings_version
            "#,
        )
        .bind(tenant_id)
        .bind(&patch.conversation_id)
        .bind(&user_id)
        .bind(patch.base_settings_version as i64)
        .bind(patch.is_pinned)
        .bind(patch.is_muted)
        .bind(patch.is_archived)
        .bind(patch.draft.as_deref())
        .fetch_optional(&*self.pool)
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "update user settings"))?;

        let Some(row) = row else {
            if patch.base_settings_version > 0 {
                return Err(ErrorBuilder::new(
                    ErrorCode::HttpConflict,
                    "user settings version conflict or participant not found",
                )
                .build_error());
            }
            return Err(ErrorBuilder::new(
                ErrorCode::HttpNotFound,
                "conversation participant not found",
            )
            .build_error());
        };

        Ok(ConversationUserSettings {
            is_pinned: row.get("pinned"),
            is_muted: row.get("muted"),
            is_archived: row.get("is_archived"),
            draft: row.get::<Option<String>, _>("draft"),
            settings_version: row.get::<i64, _>("settings_version").max(0) as u64,
        })
    }

    async fn apply_message_event(
        &self,
        ctx: &flare_server_core::context::Context,
        conversation_id: &str,
        sender_id: &str,
        seq: i64,
        status: i32,
    ) -> Result<()> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let precise_unread_threshold = self.config.large_conversation_precise_unread_threshold;

        sqlx::query(
            r#"
            WITH conv_upd AS (
                UPDATE conversations c
                SET
                    last_message_seq = GREATEST(COALESCE(c.last_message_seq, 0), $1),
                    updated_at = CURRENT_TIMESTAMP
                WHERE c.tenant_id = $2 AND c.conversation_id = $3
                RETURNING 1
            ),
            member_stats AS (
                SELECT COUNT(*)::INT AS member_count
                FROM conversation_participants
                WHERE tenant_id = $2
                  AND conversation_id = $3
                  AND NOT COALESCE(is_deleted, false)
            )
            UPDATE conversation_participants sp
            SET
                unread_count = CASE
                    WHEN sp.user_id = $4 THEN COALESCE(sp.unread_count, 0)
                    WHEN $5 IN (6, 7, 8) THEN COALESCE(sp.unread_count, 0)
                    WHEN COALESCE(sp.last_read_seq, 0) >= $1 THEN COALESCE(sp.unread_count, 0)
                    ELSE COALESCE(sp.unread_count, 0) + 1
                END,
                updated_at = CURRENT_TIMESTAMP
            FROM member_stats
            WHERE sp.tenant_id = $2
              AND sp.conversation_id = $3
              AND NOT COALESCE(sp.is_deleted, false)
              AND ($6 <= 0 OR member_stats.member_count <= $6)
            "#,
        )
        .bind(seq)
        .bind(tenant_id)
        .bind(conversation_id)
        .bind(sender_id)
        .bind(status)
        .bind(precise_unread_threshold)
        .execute(&*self.pool)
        .await
        .map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "Failed to apply message event for unread update",
            )
        })?;

        Ok(())
    }

    async fn get_unread_count(
        &self,
        ctx: &flare_server_core::context::Context,
        conversation_id: &str,
    ) -> Result<i32> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let user_id = require_user_id(ctx)?;
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
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to get unread count"))?;

        let unread_count = if let Some(row) = row {
            row.get("unread_count")
        } else {
            0
        };

        Ok(unread_count)
    }
}
