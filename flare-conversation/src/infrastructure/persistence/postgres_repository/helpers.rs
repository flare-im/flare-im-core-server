//! 私有辅助查询：调用者成员资格核对、成员聚合与未读阈值判定。
//!
//! 从 postgres_repository.rs 拆出；这些是固有 impl 的私有方法，
//! 提升 pub(super) 供主文件的 trait impl 调用。

use flare_server_core::error::{ErrorCode, Result, map_infra_error, require_user_id};

use crate::domain::model::ConversationParticipant;

use super::{PARTICIPANT_COLUMNS, participant_from_row, PostgresConversationRepository};

impl PostgresConversationRepository {
    /// 用户态成员读的前置条件：调用者自己是该会话的成员（未退出；隐藏了会话的也算），否则按
    /// "会话不存在"拒绝。读哪些行（在席还是成员）由各读接口的口径决定，与调用者核对无关：隐藏
    /// 了会话的人照样能发已读回执、点表情，这些事件的收件人读取会带着他的身份来，只是读出的
    /// 收件人按在席口径不含隐藏者。
    pub(super) async fn ensure_caller_participant(
        &self,
        ctx: &flare_server_core::context::Context,
        tenant_id: &str,
        conversation_id: &str,
    ) -> Result<()> {
        let user_id = require_user_id(ctx)?;
        let membership_exists: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT 1::BIGINT
            FROM conversation_participants
            WHERE tenant_id = $1
              AND conversation_id = $2
              AND user_id = $3
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
        Ok(())
    }
}

impl PostgresConversationRepository {
    /// `user_ids` 中仍是成员（未退出；隐藏了会话的也算）的参与者，主键上的 ANY 查找：O(ids)。
    pub(super) async fn members_among(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        user_ids: &[String],
    ) -> Result<Vec<ConversationParticipant>> {
        let sql = format!(
            r#"
            SELECT {PARTICIPANT_COLUMNS}
            FROM conversation_participants
            WHERE tenant_id = $1
              AND conversation_id = $2
              AND user_id = ANY($3)
              AND quit_at IS NULL
            ORDER BY joined_at ASC, user_id ASC
            "#
        );
        let rows = sqlx::query(&sql)
            .bind(tenant_id)
            .bind(conversation_id)
            .bind(user_ids)
            .fetch_all(&*self.pool)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "find members"))?;
        Ok(rows.iter().map(participant_from_row).collect())
    }

    /// 会话成员是否超过精确未读阈值(`large_conversation_precise_unread_threshold`):有界计数,
    /// 最多读「阈值+1」行即停(LIMIT)。不对全群 COUNT(*)(O(成员) 扫描,实测 10 万群病态计划
    /// 钉核),也不依赖不可靠的 conversations.member_count(实测大群该列为 0)。超阈值的会话不物化
    /// 每个成员的未读。阈值为 0 时不分大小群。
    pub(super) async fn above_precise_unread_threshold(
        &self,
        tenant_id: &str,
        conversation_id: &str,
    ) -> Result<bool> {
        let precise_unread_threshold = self.config.large_conversation_precise_unread_threshold;
        if precise_unread_threshold <= 0 {
            return Ok(false);
        }
        let capped: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM (
                SELECT 1
                FROM conversation_participants
                WHERE tenant_id = $1
                  AND conversation_id = $2
                  AND NOT COALESCE(is_deleted, false)
                LIMIT $3
            ) capped
            "#,
        )
        .bind(tenant_id)
        .bind(conversation_id)
        .bind(i64::from(precise_unread_threshold) + 1)
        .fetch_one(&*self.pool)
        .await
        .map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "Failed to probe conversation size",
            )
        })?;
        Ok(capped > i64::from(precise_unread_threshold))
    }
}

