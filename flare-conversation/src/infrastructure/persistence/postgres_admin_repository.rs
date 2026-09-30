//! 管理员只读仓储（PostgreSQL）：按租户分页列会话、读详情、分页读成员。
//!
//! 租户由调用方显式传入（接口层取自已验证的 admin 令牌），这里不读调用上下文，也不做
//! 「调用者须为成员」的校验。与会话仓储共用同一个连接池。
//!
//! 两个游标都是不透明文本，只由本模块编码/解码：
//! - 会话：`c1:{updated_at 微秒|n}:{conversation_id}`，按 `updated_at DESC NULLS FIRST,
//!   conversation_id DESC` 翻页，与 `idx_conversations_tenant_updated` 同序；
//! - 成员：`p1:{joined_at 微秒|n}:{total}:{user_id}`，按 `joined_at ASC NULLS LAST, user_id ASC`
//!   翻页，与 `idx_conversation_participants_conv_joined` 同序；成员总数只在首页计算，随游标带走。
//!
//! `n` 表示上一页最后一行的时间列为 NULL（列有默认值，正常写路径不会出现，但读路径不能因此崩溃）。
//! 解析失败返回 `InvalidParameter`，不退回首页，避免管理视图静默重读。

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result, map_infra_error};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use crate::domain::model::{
    AdminConversation, AdminConversationPage, Conversation, ConversationLifecycleState,
    ConversationParticipant, ConversationParticipantsPage, ConversationType,
    ConversationVisibility,
};
use crate::domain::repository::ConversationAdminRepository;

/// 会话列表每页默认条数与上限。
pub const ADMIN_CONVERSATION_PAGE_DEFAULT: i64 = 50;
pub const ADMIN_CONVERSATION_PAGE_MAX: i64 = 200;
/// 成员列表每页默认条数与上限。
pub const ADMIN_PARTICIPANT_PAGE_DEFAULT: i32 = 200;
pub const ADMIN_PARTICIPANT_PAGE_MAX: i32 = 500;

pub fn clamp_admin_conversation_limit(limit: i64) -> i64 {
    if limit <= 0 {
        ADMIN_CONVERSATION_PAGE_DEFAULT
    } else {
        limit.min(ADMIN_CONVERSATION_PAGE_MAX)
    }
}

pub fn clamp_admin_participant_limit(limit: i32) -> i32 {
    if limit <= 0 {
        ADMIN_PARTICIPANT_PAGE_DEFAULT
    } else {
        limit.min(ADMIN_PARTICIPANT_PAGE_MAX)
    }
}

fn invalid_cursor(kind: &str) -> FlareError {
    ErrorBuilder::new(
        ErrorCode::InvalidParameter,
        format!("invalid {kind} cursor"),
    )
    .build_error()
}

fn encode_time(at: Option<DateTime<Utc>>) -> String {
    at.map(|at| at.timestamp_micros().to_string())
        .unwrap_or_else(|| "n".to_string())
}

/// `Ok(None)` = NULL；`Err` = 损坏。
fn decode_time(raw: &str) -> std::result::Result<Option<DateTime<Utc>>, ()> {
    if raw == "n" {
        return Ok(None);
    }
    let micros = raw.parse::<i64>().map_err(|_| ())?;
    DateTime::<Utc>::from_timestamp_micros(micros)
        .map(Some)
        .ok_or(())
}

/// 会话游标：上一页最后一行的 `(updated_at, conversation_id)`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminConversationCursor {
    pub updated_at: Option<DateTime<Utc>>,
    pub conversation_id: String,
}

impl AdminConversationCursor {
    pub fn encode(&self) -> String {
        format!(
            "c1:{}:{}",
            encode_time(self.updated_at),
            self.conversation_id
        )
    }

    /// 空串 = 首页。
    pub fn decode(raw: Option<&str>) -> Result<Option<Self>> {
        let Some(raw) = raw.filter(|raw| !raw.is_empty()) else {
            return Ok(None);
        };
        let rest = raw
            .strip_prefix("c1:")
            .ok_or_else(|| invalid_cursor("conversation"))?;
        let (time, conversation_id) = rest
            .split_once(':')
            .ok_or_else(|| invalid_cursor("conversation"))?;
        if conversation_id.is_empty() {
            return Err(invalid_cursor("conversation"));
        }
        let updated_at = decode_time(time).map_err(|_| invalid_cursor("conversation"))?;
        Ok(Some(Self {
            updated_at,
            conversation_id: conversation_id.to_string(),
        }))
    }
}

/// 成员游标：上一页最后一行的 `(joined_at, user_id)` 与首页算出的总数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminParticipantCursor {
    pub joined_at: Option<DateTime<Utc>>,
    pub total: i64,
    pub user_id: String,
}

impl AdminParticipantCursor {
    pub fn encode(&self) -> String {
        format!(
            "p1:{}:{}:{}",
            encode_time(self.joined_at),
            self.total,
            self.user_id
        )
    }

    pub fn decode(raw: Option<&str>) -> Result<Option<Self>> {
        let Some(raw) = raw.filter(|raw| !raw.is_empty()) else {
            return Ok(None);
        };
        let rest = raw
            .strip_prefix("p1:")
            .ok_or_else(|| invalid_cursor("participant"))?;
        // user_id 放最后并用 splitn 取剩余部分：含冒号的 user_id 也能原样还原。
        let mut parts = rest.splitn(3, ':');
        let joined_at = parts
            .next()
            .ok_or(())
            .and_then(decode_time)
            .map_err(|_| invalid_cursor("participant"))?;
        let total = parts
            .next()
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|total| *total >= 0)
            .ok_or_else(|| invalid_cursor("participant"))?;
        let user_id = parts
            .next()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid_cursor("participant"))?;
        Ok(Some(Self {
            joined_at,
            total,
            user_id: user_id.to_string(),
        }))
    }
}

fn attributes_map(value: Option<serde_json::Value>) -> HashMap<String, String> {
    value
        .and_then(|value| value.as_object().cloned())
        .map(|object| {
            object
                .into_iter()
                .map(|(key, value)| {
                    let value = value
                        .as_str()
                        .map(ToOwned::to_owned)
                        .unwrap_or_else(|| value.to_string());
                    (key, value)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn lifecycle_state(raw: &str) -> ConversationLifecycleState {
    match raw {
        "active" => ConversationLifecycleState::Active,
        "suspended" => ConversationLifecycleState::Suspended,
        "archived" => ConversationLifecycleState::Archived,
        "deleted" => ConversationLifecycleState::Deleted,
        _ => ConversationLifecycleState::Unspecified,
    }
}

const CONVERSATION_COLUMNS: &str = r#"
    conversation_id, conversation_type, business_type, display_name, attributes,
    visibility, lifecycle_state, channel_id, created_at, updated_at"#;

fn conversation_from_row(tenant_id: &str, row: &PgRow) -> Conversation {
    let created_at: Option<DateTime<Utc>> = row.get("created_at");
    let updated_at: Option<DateTime<Utc>> = row.get("updated_at");
    let created_at = created_at.unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    Conversation {
        tenant_id: tenant_id.to_string(),
        conversation_id: row.get("conversation_id"),
        conversation_type: ConversationType::from_int(row.get("conversation_type")),
        business_type: row.get("business_type"),
        channel_id: row.get("channel_id"),
        display_name: row.get("display_name"),
        attributes: attributes_map(row.get("attributes")),
        participants: Vec::new(),
        visibility: ConversationVisibility::from_proto(row.get("visibility")),
        lifecycle_state: lifecycle_state(row.get::<String, _>("lifecycle_state").as_str()),
        policy: None,
        created_at,
        updated_at: updated_at.unwrap_or(created_at),
    }
}

pub struct PostgresConversationAdminRepository {
    pool: Arc<PgPool>,
}

impl PostgresConversationAdminRepository {
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }

    /// 当前有效成员数（未删除、未退出），按会话聚合。
    async fn active_member_counts(
        &self,
        tenant_id: &str,
        conversation_ids: &[String],
    ) -> Result<HashMap<String, i64>> {
        if conversation_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let rows = sqlx::query(
            r#"
            SELECT conversation_id, COUNT(*)::BIGINT AS member_count
            FROM conversation_participants
            WHERE tenant_id = $1
              AND conversation_id = ANY($2)
              AND NOT COALESCE(is_deleted, false)
              AND quit_at IS NULL
            GROUP BY conversation_id
            "#,
        )
        .bind(tenant_id)
        .bind(conversation_ids)
        .fetch_all(&*self.pool)
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "admin count members"))?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get("conversation_id"), row.get("member_count")))
            .collect())
    }

    async fn conversation_exists(&self, tenant_id: &str, conversation_id: &str) -> Result<bool> {
        let exists: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT 1::BIGINT FROM conversations
            WHERE tenant_id = $1 AND conversation_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(conversation_id)
        .fetch_optional(&*self.pool)
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "admin check conversation"))?;
        Ok(exists.is_some())
    }
}

#[async_trait::async_trait]
impl ConversationAdminRepository for PostgresConversationAdminRepository {
    async fn admin_list_conversations(
        &self,
        tenant_id: &str,
        cursor: Option<&str>,
        limit: i64,
        include_deleted: bool,
    ) -> Result<AdminConversationPage> {
        let cursor = AdminConversationCursor::decode(cursor)?;
        let limit = clamp_admin_conversation_limit(limit);
        // 与 idx_conversations_tenant_updated(tenant_id, updated_at DESC) 同序（DESC 默认 NULLS FIRST），
        // 所以 NULL 行排在最前：非 NULL 游标之后不会再有 NULL 行，NULL 游标之后先读完剩余 NULL 行。
        // 每个分支只绑定自己引用的参数：PostgreSQL 不接受多绑或跳号的参数。
        let keyset = match &cursor {
            None => "",
            Some(AdminConversationCursor {
                updated_at: Some(_),
                ..
            }) => "AND (updated_at < $4 OR (updated_at = $4 AND conversation_id < $5))",
            Some(AdminConversationCursor {
                updated_at: None, ..
            }) => "AND ((updated_at IS NULL AND conversation_id < $4) OR updated_at IS NOT NULL)",
        };
        let sql = format!(
            r#"
            SELECT {CONVERSATION_COLUMNS}
            FROM conversations
            WHERE tenant_id = $1
              AND ($2 OR lifecycle_state <> 'deleted')
              {keyset}
            ORDER BY updated_at DESC NULLS FIRST, conversation_id DESC
            LIMIT $3
            "#
        );
        let query = sqlx::query(&sql)
            .bind(tenant_id)
            .bind(include_deleted)
            .bind(limit + 1);
        let query = match cursor {
            None => query,
            Some(AdminConversationCursor {
                updated_at: Some(updated_at),
                conversation_id,
            }) => query.bind(updated_at).bind(conversation_id),
            Some(AdminConversationCursor {
                updated_at: None,
                conversation_id,
            }) => query.bind(conversation_id),
        };
        let mut rows = query.fetch_all(&*self.pool).await.map_err(|e| {
            map_infra_error(e, ErrorCode::DatabaseError, "admin list conversations")
        })?;

        let has_more = rows.len() as i64 > limit;
        rows.truncate(limit as usize);
        let next_cursor = if has_more {
            rows.last().map(|row| {
                AdminConversationCursor {
                    updated_at: row.get("updated_at"),
                    conversation_id: row.get("conversation_id"),
                }
                .encode()
            })
        } else {
            None
        };
        let conversations: Vec<Conversation> = rows
            .iter()
            .map(|row| conversation_from_row(tenant_id, row))
            .collect();
        let ids: Vec<String> = conversations
            .iter()
            .map(|c| c.conversation_id.clone())
            .collect();
        let counts = self.active_member_counts(tenant_id, &ids).await?;
        Ok(AdminConversationPage {
            conversations: conversations
                .into_iter()
                .map(|conversation| AdminConversation {
                    member_count: counts
                        .get(&conversation.conversation_id)
                        .copied()
                        .unwrap_or(0),
                    conversation,
                })
                .collect(),
            next_cursor,
            has_more,
        })
    }

    async fn admin_get_conversation(
        &self,
        tenant_id: &str,
        conversation_id: &str,
    ) -> Result<Option<AdminConversation>> {
        let sql = format!(
            r#"
            SELECT {CONVERSATION_COLUMNS}
            FROM conversations
            WHERE tenant_id = $1 AND conversation_id = $2
            "#
        );
        let row = sqlx::query(&sql)
            .bind(tenant_id)
            .bind(conversation_id)
            .fetch_optional(&*self.pool)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "admin get conversation"))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let conversation = conversation_from_row(tenant_id, &row);
        let member_count = self
            .active_member_counts(
                tenant_id,
                std::slice::from_ref(&conversation.conversation_id),
            )
            .await?
            .get(&conversation.conversation_id)
            .copied()
            .unwrap_or(0);
        Ok(Some(AdminConversation {
            conversation,
            member_count,
        }))
    }

    async fn admin_list_participants(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        cursor: Option<&str>,
        limit: i32,
        include_removed: bool,
    ) -> Result<Option<ConversationParticipantsPage>> {
        let cursor = AdminParticipantCursor::decode(cursor)?;
        if !self.conversation_exists(tenant_id, conversation_id).await? {
            return Ok(None);
        }
        let limit = clamp_admin_participant_limit(limit);
        let active_filter = if include_removed {
            ""
        } else {
            "AND NOT COALESCE(is_deleted, false) AND quit_at IS NULL"
        };
        let total = match &cursor {
            Some(cursor) => cursor.total,
            None => {
                let sql = format!(
                    r#"
                    SELECT COUNT(*)::BIGINT
                    FROM conversation_participants
                    WHERE tenant_id = $1 AND conversation_id = $2
                      {active_filter}
                    "#
                );
                sqlx::query_scalar::<_, i64>(&sql)
                    .bind(tenant_id)
                    .bind(conversation_id)
                    .fetch_one(&*self.pool)
                    .await
                    .map_err(|e| {
                        map_infra_error(e, ErrorCode::DatabaseError, "admin count participants")
                    })?
            }
        };
        // 与 idx_conversation_participants_conv_joined(tenant_id, conversation_id, joined_at, user_id)
        // 同序（ASC 默认 NULLS LAST）：NULL 行排在最后。每个分支只绑定自己引用的参数。
        let keyset = match &cursor {
            None => "",
            Some(AdminParticipantCursor {
                joined_at: Some(_), ..
            }) => "AND ((joined_at, user_id) > ($4, $5) OR joined_at IS NULL)",
            Some(AdminParticipantCursor {
                joined_at: None, ..
            }) => "AND joined_at IS NULL AND user_id > $4",
        };
        let sql = format!(
            r#"
            SELECT user_id::text AS user_id,
                   COALESCE(roles, ARRAY[]::text[]) AS roles,
                   COALESCE(muted, false) AS muted,
                   COALESCE(pinned, false) AS pinned,
                   COALESCE(attributes, '{{}}'::jsonb) AS attributes,
                   joined_at,
                   COALESCE(nickname, '') AS nickname,
                   COALESCE(visible_from_seq, 0) AS visible_from_seq,
                   COALESCE(mention_only, false) AS mention_only
            FROM conversation_participants
            WHERE tenant_id = $1 AND conversation_id = $2
              {active_filter}
              {keyset}
            ORDER BY joined_at ASC NULLS LAST, user_id ASC
            LIMIT $3
            "#
        );
        let query = sqlx::query(&sql)
            .bind(tenant_id)
            .bind(conversation_id)
            .bind(i64::from(limit) + 1);
        let query = match cursor {
            None => query,
            Some(AdminParticipantCursor {
                joined_at: Some(joined_at),
                user_id,
                ..
            }) => query.bind(joined_at).bind(user_id),
            Some(AdminParticipantCursor {
                joined_at: None,
                user_id,
                ..
            }) => query.bind(user_id),
        };
        let mut rows = query
            .fetch_all(&*self.pool)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "admin list participants"))?;

        let has_more = rows.len() > limit as usize;
        rows.truncate(limit as usize);
        let next_cursor = if has_more {
            rows.last().map(|row| {
                AdminParticipantCursor {
                    joined_at: row.get("joined_at"),
                    total,
                    user_id: row.get("user_id"),
                }
                .encode()
            })
        } else {
            None
        };
        let participants = rows
            .iter()
            .map(|row| {
                let mut attributes = attributes_map(row.get("attributes"));
                let nickname: String = row.get("nickname");
                if !nickname.trim().is_empty() {
                    attributes.insert("nickname".to_string(), nickname);
                }
                ConversationParticipant {
                    user_id: row.get("user_id"),
                    roles: row.get("roles"),
                    muted: row.get("muted"),
                    pinned: row.get("pinned"),
                    attributes,
                    visible_from_seq: row.get("visible_from_seq"),
                    mention_only: row.get("mention_only"),
                }
            })
            .collect();
        Ok(Some(ConversationParticipantsPage {
            conversation_id: conversation_id.to_string(),
            participants,
            next_cursor,
            has_more,
            participant_version: 0,
            member_count: i32::try_from(total).unwrap_or(i32::MAX),
        }))
    }
}

#[cfg(test)]
mod cursor_tests {
    use super::*;

    #[test]
    fn conversation_cursor_round_trips_including_null_time_and_colons() {
        let at = DateTime::<Utc>::from_timestamp_micros(1_700_000_000_123_456).unwrap();
        for cursor in [
            AdminConversationCursor {
                updated_at: Some(at),
                conversation_id: "2ABC".into(),
            },
            AdminConversationCursor {
                updated_at: None,
                conversation_id: "id:with:colons".into(),
            },
        ] {
            let encoded = cursor.encode();
            assert_eq!(
                AdminConversationCursor::decode(Some(&encoded)).unwrap(),
                Some(cursor)
            );
        }
        assert_eq!(AdminConversationCursor::decode(None).unwrap(), None);
        assert_eq!(AdminConversationCursor::decode(Some("")).unwrap(), None);
    }

    #[test]
    fn participant_cursor_round_trips_and_carries_total() {
        let at = DateTime::<Utc>::from_timestamp_micros(1_700_000_000_000_001).unwrap();
        for cursor in [
            AdminParticipantCursor {
                joined_at: Some(at),
                total: 42,
                user_id: "u:1".into(),
            },
            AdminParticipantCursor {
                joined_at: None,
                total: 0,
                user_id: "u2".into(),
            },
        ] {
            let encoded = cursor.encode();
            assert_eq!(
                AdminParticipantCursor::decode(Some(&encoded)).unwrap(),
                Some(cursor)
            );
        }
    }

    #[test]
    fn corrupt_cursors_are_rejected_instead_of_restarting() {
        for raw in [
            "x",
            "c1:",
            "c1:abc:id",
            "c1:123:",
            "c1:123",
            "k:1:2:3:u",
            "p1:1:2:u",
        ] {
            let error = AdminConversationCursor::decode(Some(raw)).unwrap_err();
            assert_eq!(error.code(), Some(ErrorCode::InvalidParameter), "{raw}");
        }
        for raw in [
            "x",
            "p1:",
            "p1:1:-1:u",
            "p1:1:2:",
            "p1:n:x:u",
            "c1:1:id",
            "12",
        ] {
            let error = AdminParticipantCursor::decode(Some(raw)).unwrap_err();
            assert_eq!(error.code(), Some(ErrorCode::InvalidParameter), "{raw}");
        }
    }

    #[test]
    fn page_limits_have_defaults_and_ceilings() {
        assert_eq!(clamp_admin_conversation_limit(0), 50);
        assert_eq!(clamp_admin_conversation_limit(-3), 50);
        assert_eq!(clamp_admin_conversation_limit(10_000), 200);
        assert_eq!(clamp_admin_participant_limit(0), 200);
        assert_eq!(clamp_admin_participant_limit(7), 7);
        assert_eq!(clamp_admin_participant_limit(10_000), 500);
    }
}
