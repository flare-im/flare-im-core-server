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

/// 会话摘要读时补全:解码最后一条消息的 content(BYTEA proto `MessageContent`)→
/// (预览文本, content_type 标签)。文本类取正文,媒体/卡片等取占位标签;解码失败或空 → (None, None)。
/// 仅在读会话列表时按会话取最后一行解码,不新增落库列。
fn last_message_preview_from_content(bytes: Option<&[u8]>) -> (Option<String>, Option<String>) {
    use flare_proto::common::{MessageContent, message_content::Content};
    use prost::Message as _;
    let bytes = match bytes {
        Some(b) if !b.is_empty() => b,
        _ => return (None, None),
    };
    let content = match MessageContent::decode(bytes) {
        Ok(v) => v,
        Err(_) => return (None, None),
    };
    let (preview, kind): (String, &str) = match content.content {
        Some(Content::Text(t)) => (t.text, "text"),
        Some(Content::RichText(_)) => ("[富文本]".to_string(), "rich_text"),
        Some(Content::Image(_)) | Some(Content::ImageGroup(_)) => ("[图片]".to_string(), "image"),
        Some(Content::Video(_)) => ("[视频]".to_string(), "video"),
        Some(Content::Audio(_)) => ("[语音]".to_string(), "audio"),
        Some(Content::File(_)) => ("[文件]".to_string(), "file"),
        Some(Content::Location(_)) => ("[位置]".to_string(), "location"),
        Some(Content::Sticker(_)) | Some(Content::Emoji(_)) => ("[表情]".to_string(), "sticker"),
        Some(Content::Card(_)) | Some(Content::AppCard(_)) | Some(Content::LinkCard(_)) => {
            ("[卡片]".to_string(), "card")
        }
        Some(Content::Quote(_)) => ("[引用]".to_string(), "quote"),
        Some(Content::Forward(_)) => ("[转发]".to_string(), "forward"),
        Some(Content::Notification(_)) | Some(Content::System(_)) => {
            ("[系统消息]".to_string(), "system")
        }
        Some(_) => ("[消息]".to_string(), "other"),
        None => return (None, None),
    };
    let preview = if preview.trim().is_empty() {
        None
    } else {
        Some(preview)
    };
    (preview, Some(kind.to_string()))
}

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

        // 仅取每会话前 N 条做预览（LATERAL + LIMIT），绝不全量加载成员——10 万群下全量 SELECT 会返回
        // O(成员) 行(实测单次 18 万行/1.5s),每个客户端 ListConversations/同步都触发,直接打垮 Postgres。
        // 真实人数走单独的 COUNT 聚合(索引扫描,不物化行)。完整成员由独立成员同步按需拉取。
        const MEMBER_PREVIEW_LIMIT: i64 = 10;
        let rows = sqlx::query(
            r#"
            SELECT
                p.conversation_id::text AS conversation_id,
                p.user_id::text AS user_id,
                COALESCE(p.roles, ARRAY[]::text[]) AS roles,
                COALESCE(p.muted, false) AS muted,
                COALESCE(p.pinned, false) AS pinned,
                COALESCE(p.attributes, '{}'::jsonb) AS attributes,
                COALESCE(p.nickname, '') AS nickname
            FROM unnest($2::text[]) AS c(conversation_id)
            CROSS JOIN LATERAL (
                SELECT cp.*
                FROM conversation_participants cp
                WHERE cp.tenant_id = $1
                  AND cp.conversation_id = c.conversation_id
                  AND NOT COALESCE(cp.is_deleted, false)
                  AND cp.quit_at IS NULL
                ORDER BY cp.joined_at ASC, cp.user_id ASC
                LIMIT $3
            ) p
            "#,
        )
        .bind(tenant_id)
        .bind(&need_participants)
        .bind(MEMBER_PREVIEW_LIMIT)
        .fetch_all(pool)
        .await
        .map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "fill conversation member preview",
            )
        })?;

        // 真实成员数：按会话 COUNT 聚合（索引扫描，不把 O(成员) 行拉回应用层）。
        let count_rows = sqlx::query(
            r#"
            SELECT cp.conversation_id::text AS conversation_id, COUNT(*)::bigint AS member_count
            FROM conversation_participants cp
            WHERE cp.tenant_id = $1
              AND cp.conversation_id = ANY($2)
              AND NOT COALESCE(cp.is_deleted, false)
              AND cp.quit_at IS NULL
            GROUP BY cp.conversation_id
            "#,
        )
        .bind(tenant_id)
        .bind(&need_participants)
        .fetch_all(pool)
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "count conversation members"))?;
        let mut count_by_cid: HashMap<String, i64> = HashMap::new();
        for row in count_rows {
            count_by_cid.insert(row.get("conversation_id"), row.get("member_count"));
        }

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
            // 真实人数取 COUNT 结果；预览取 LATERAL 的前 N 条。
            let member_count = count_by_cid
                .get(&summary.conversation_id)
                .copied()
                .unwrap_or(0)
                .max(0) as u64;
            if member_count > 0 {
                summary
                    .metadata
                    .insert("member_count".to_string(), member_count.to_string());
                summary.participant_version = member_count;
            }
            if let Some(participants) = by_cid.remove(&summary.conversation_id) {
                // LATERAL 查询已按 MEMBER_PREVIEW_LIMIT 截断，这里仅防御性对齐同一常量。
                summary.member_preview = participants
                    .into_iter()
                    .take(MEMBER_PREVIEW_LIMIT as usize)
                    .collect();
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
        updated_after_ms: i64,
    ) -> Result<ConversationBootstrapResult> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let user_id = require_user_id(ctx)?;
        // 增量过滤边界（None=全量）。EXISTS 探针走 idx_messages_conversation_ts，
        // 使热启/重连列表同步的存储成本从 O(全账号会话×LATERAL) 降到 O(变化)。
        let updated_after: Option<chrono::DateTime<chrono::Utc>> = (updated_after_ms > 0)
            .then(|| chrono::DateTime::<chrono::Utc>::from_timestamp_millis(updated_after_ms))
            .flatten();
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
                END AS peer_read_seq,
                message_tail.last_message_id AS last_message_id,
                message_tail.last_sender_id AS last_sender_id,
                message_tail.last_message_type AS last_message_type,
                message_tail.last_message_at AS last_message_at,
                message_tail.last_content AS last_content
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
                -- 取会话最后一条消息行(seq 最大)。seq 单调即为最新,兼作 max_seq/last_message_at,
                -- 并带出发送者/类型/content 供读时补全会话摘要预览。
                SELECT m.seq AS max_seq, m.timestamp AS last_message_at,
                       m.server_id AS last_message_id, m.sender_id AS last_sender_id,
                       m.message_type AS last_message_type, m.content AS last_content
                FROM messages m
                WHERE m.tenant_id = s.tenant_id
                  AND m.conversation_id = s.conversation_id
                ORDER BY m.seq DESC
                LIMIT 1
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
              -- 增量过滤：任一构成 effective_updated_at 的来源晚于边界即命中；
              -- EXISTS 探针不引用 LATERAL 输出，可被规划器下推在 LATERAL 之前裁剪行。
              AND (
                  $3::timestamptz IS NULL
                  OR s.updated_at > $3
                  OR sp.joined_at > $3
                  OR sp.updated_at > $3
                  OR EXISTS (
                      SELECT 1 FROM messages mx
                      WHERE mx.tenant_id = s.tenant_id
                        AND mx.conversation_id = s.conversation_id
                        AND mx.timestamp > $3
                  )
              )
            ORDER BY effective_updated_at DESC
            "#,
        )
        .bind(tenant_id)
        .bind(&user_id)
        .bind(updated_after)
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

            // 读时补全会话最后一条消息(替代注释中未落地的 ApplicationService/MessageProvider)。
            let last_message_id: Option<String> = row.get("last_message_id");
            let last_sender_id: Option<String> = row.get("last_sender_id");
            let last_message_type: Option<i32> = row.get("last_message_type");
            let last_message_time: Option<DateTime<Utc>> = row.get("last_message_at");
            let last_content: Option<Vec<u8>> = row.get("last_content");
            let (last_message_preview, last_content_type) =
                last_message_preview_from_content(last_content.as_deref());

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
                last_message_id,
                last_message_time,
                last_sender_id,
                last_message_type,
                last_content_type,
                last_message_preview,
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

        // 批量插入参与者（多行 VALUES 分块 + ON CONFLICT 幂等）。
        // 逐行 INSERT 在万级/十万级群是建群的致命瓶颈：N 次网络往返 + 长事务。改为分块多行插入,
        // 每块 ≤PARTICIPANT_INSERT_CHUNK 行 × 7 参数 < PG 65535 参数上限,把往返从 O(成员) 降到 O(成员/块)。
        if !session.participants.is_empty() {
            use sqlx::QueryBuilder;
            const PARTICIPANT_INSERT_CHUNK: usize = 5000;
            for chunk in session.participants.chunks(PARTICIPANT_INSERT_CHUNK) {
                let mut qb = QueryBuilder::new(
                    "INSERT INTO conversation_participants \
                     (tenant_id, conversation_id, user_id, roles, muted, pinned, attributes, created_at, updated_at) ",
                );
                qb.push_values(chunk, |mut b, participant| {
                    let attrs = serde_json::to_value(&participant.attributes)
                        .unwrap_or_else(|_| serde_json::json!({}));
                    b.push_bind(tenant_id)
                        .push_bind(&session.conversation_id)
                        .push_bind(&participant.user_id)
                        .push_bind(&participant.roles)
                        .push_bind(participant.muted)
                        .push_bind(participant.pinned)
                        .push_bind(attrs)
                        .push("CURRENT_TIMESTAMP")
                        .push("CURRENT_TIMESTAMP");
                });
                qb.push(
                    " ON CONFLICT (tenant_id, conversation_id, user_id) DO UPDATE SET \
                     roles = EXCLUDED.roles, muted = EXCLUDED.muted, pinned = EXCLUDED.pinned, \
                     attributes = EXCLUDED.attributes, is_deleted = FALSE, updated_at = CURRENT_TIMESTAMP",
                );
                qb.build().execute(&mut *tx).await.map_err(|e| {
                    map_infra_error(
                        e,
                        ErrorCode::DatabaseError,
                        "Failed to batch insert participants",
                    )
                })?;
            }
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
        // 受信内部调用（Service/System actor，如网关读扩散成员订阅 bootstrap）跳过"调用者须为成员"的鉴权，
        // 仅按 tenant + conversation 范围读取成员；用户态调用仍要求 user_id 且必须是会话成员。
        let is_internal = ctx
            .actor()
            .map(|actor| {
                matches!(
                    actor.actor_type,
                    flare_server_core::context::ActorType::Service
                        | flare_server_core::context::ActorType::System
                )
            })
            .unwrap_or(false);
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

        if !is_internal {
            let user_id = require_user_id(ctx)?;
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

#[cfg(test)]
mod last_message_preview_tests {
    use super::last_message_preview_from_content;
    use flare_proto::common::{ImageContent, MessageContent, TextContent, message_content::Content};
    use prost::Message as _;

    fn encode(content: Content) -> Vec<u8> {
        MessageContent {
            content: Some(content),
        }
        .encode_to_vec()
    }

    #[test]
    fn text_content_yields_text_preview() {
        let bytes = encode(Content::Text(TextContent {
            text: "hello preview".to_string(),
            mentions: vec![],
        }));
        let (preview, kind) = last_message_preview_from_content(Some(&bytes));
        assert_eq!(preview.as_deref(), Some("hello preview"));
        assert_eq!(kind.as_deref(), Some("text"));
    }

    #[test]
    fn media_content_yields_label() {
        let bytes = encode(Content::Image(ImageContent::default()));
        let (preview, kind) = last_message_preview_from_content(Some(&bytes));
        assert_eq!(preview.as_deref(), Some("[图片]"));
        assert_eq!(kind.as_deref(), Some("image"));
    }

    #[test]
    fn empty_or_invalid_content_yields_none() {
        assert_eq!(last_message_preview_from_content(None), (None, None));
        assert_eq!(last_message_preview_from_content(Some(&[])), (None, None));
        // 无效 protobuf(非法 wire type)→ 解码失败 → None。
        assert_eq!(
            last_message_preview_from_content(Some(&[0xff, 0xff])),
            (None, None)
        );
    }
}
