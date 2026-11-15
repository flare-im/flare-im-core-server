//! 用户级通知设置（`user_notification_settings`）与会话通知订阅者
//! （`conversation_participants.notify_all`）的 PostgreSQL 实现。

use flare_server_core::error::{ErrorBuilder, ErrorCode, Result, map_infra_error};
use sqlx::Row;
use sqlx::postgres::PgRow;

use super::{PostgresConversationRepository, required_tenant};
use crate::domain::model::{
    MultiDeviceAlertPolicy, NotificationPreview, QuietHours, QuietHoursChange,
    UserNotificationSettings, UserNotificationSettingsPatch,
};
use crate::domain::repository::NotificationSettingsRepository;

const SETTINGS_COLUMNS: &str = "quiet_start_minute, quiet_end_minute, quiet_weekdays, \
     quiet_allow_mentions, preview, multi_device, settings_version";

/// 写入时各列的新值：`$3` 为真时整体替换勿扰四列（取消时写 NULL 与默认值），否则保留；预览与
/// 多端策略为 NULL 时保留。插入与按版本更新共用这一段。
const SETTINGS_ASSIGNMENTS: &str = r#"
    quiet_start_minute = CASE WHEN $3::boolean THEN $4::smallint ELSE s.quiet_start_minute END,
    quiet_end_minute = CASE WHEN $3::boolean THEN $5::smallint ELSE s.quiet_end_minute END,
    quiet_weekdays = CASE WHEN $3::boolean THEN $6::smallint ELSE s.quiet_weekdays END,
    quiet_allow_mentions = CASE WHEN $3::boolean THEN $7::boolean ELSE s.quiet_allow_mentions END,
    preview = COALESCE($8::smallint, s.preview),
    multi_device = COALESCE($9::smallint, s.multi_device),
    settings_version = s.settings_version + 1,
    updated_at = CURRENT_TIMESTAMP"#;

/// 存储行 → 领域值。列上的 CHECK 保证取值在范围内；万一读到范围外的值按默认处理，不让一行坏数据
/// 拖垮整批推送判定。
fn settings_from_row(row: &PgRow) -> UserNotificationSettings {
    let start: Option<i16> = row.get("quiet_start_minute");
    let end: Option<i16> = row.get("quiet_end_minute");
    let quiet_hours = match (start, end) {
        (Some(start), Some(end)) => Some(QuietHours {
            start_minute: u32::try_from(start).unwrap_or_default(),
            end_minute: u32::try_from(end).unwrap_or_default(),
            weekdays: u32::try_from(row.get::<i16, _>("quiet_weekdays")).unwrap_or_default(),
            allow_mentions: row.get("quiet_allow_mentions"),
        }),
        _ => None,
    };
    UserNotificationSettings {
        quiet_hours,
        preview: NotificationPreview::from_wire(i32::from(row.get::<i16, _>("preview")))
            .unwrap_or_default(),
        multi_device: MultiDeviceAlertPolicy::from_wire(i32::from(
            row.get::<i16, _>("multi_device"),
        ))
        .unwrap_or_default(),
        settings_version: u64::try_from(row.get::<i64, _>("settings_version")).unwrap_or_default(),
    }
}

fn smallint(value: u32) -> Result<i16> {
    i16::try_from(value).map_err(|_| {
        ErrorBuilder::new(
            ErrorCode::InvalidParameter,
            "notification setting out of range",
        )
        .build_error()
    })
}

fn wire_smallint(value: i32) -> Result<i16> {
    i16::try_from(value).map_err(|_| {
        ErrorBuilder::new(
            ErrorCode::InvalidParameter,
            "notification setting out of range",
        )
        .build_error()
    })
}

/// 勿扰四列的写入值：(是否替换, 开始, 结束, 星期, 放行点名)。
type QuietColumns = (bool, Option<i16>, Option<i16>, i16, bool);

fn quiet_columns(change: QuietHoursChange) -> Result<QuietColumns> {
    Ok(match change {
        QuietHoursChange::Keep => (false, None, None, 0, false),
        QuietHoursChange::Clear => (true, None, None, 0, false),
        QuietHoursChange::Set(quiet) => (
            true,
            Some(smallint(quiet.start_minute)?),
            Some(smallint(quiet.end_minute)?),
            smallint(quiet.weekdays)?,
            quiet.allow_mentions,
        ),
    })
}

impl NotificationSettingsRepository for PostgresConversationRepository {
    async fn get_user_notification_settings(
        &self,
        ctx: &flare_server_core::context::Context,
        user_id: &str,
    ) -> Result<Option<UserNotificationSettings>> {
        let tenant_id = required_tenant(ctx)?;
        let row = sqlx::query(&format!(
            "SELECT {SETTINGS_COLUMNS} FROM user_notification_settings \
             WHERE tenant_id = $1 AND user_id = $2"
        ))
        .bind(tenant_id)
        .bind(user_id)
        .fetch_optional(&*self.pool)
        .await
        .map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "read user notification settings",
            )
        })?;
        Ok(row.as_ref().map(settings_from_row))
    }

    async fn batch_get_user_notification_settings(
        &self,
        ctx: &flare_server_core::context::Context,
        user_ids: &[String],
    ) -> Result<Vec<(String, UserNotificationSettings)>> {
        let tenant_id = required_tenant(ctx)?;
        if user_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(&format!(
            "SELECT user_id, {SETTINGS_COLUMNS} FROM user_notification_settings \
             WHERE tenant_id = $1 AND user_id = ANY($2)"
        ))
        .bind(tenant_id)
        .bind(user_ids)
        .fetch_all(&*self.pool)
        .await
        .map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "batch read user notification settings",
            )
        })?;
        Ok(rows
            .iter()
            .map(|row| (row.get::<String, _>("user_id"), settings_from_row(row)))
            .collect())
    }

    async fn update_user_notification_settings(
        &self,
        ctx: &flare_server_core::context::Context,
        user_id: &str,
        patch: &UserNotificationSettingsPatch,
    ) -> Result<UserNotificationSettings> {
        let tenant_id = required_tenant(ctx)?;
        let (replace_quiet, start, end, weekdays, allow_mentions) =
            quiet_columns(patch.quiet_hours)?;
        let preview = patch
            .preview
            .map(|preview| wire_smallint(preview.wire()))
            .transpose()?;
        let multi_device = patch
            .multi_device
            .map(|policy| wire_smallint(policy.wire()))
            .transpose()?;

        // 不校验版本：没有行就以默认值为底插入（版本 1），有行就按补丁改并加一。
        // 校验版本：只改版本相符的那一行；没存过的用户当前版本是 0，非 0 的基准版本必然冲突。
        let sql = if patch.base_settings_version == 0 {
            format!(
                r#"
                INSERT INTO user_notification_settings AS s (
                    tenant_id, user_id, quiet_start_minute, quiet_end_minute, quiet_weekdays,
                    quiet_allow_mentions, preview, multi_device, settings_version, updated_at
                )
                VALUES (
                    $1, $2, $4::smallint, $5::smallint, $6::smallint, $7::boolean,
                    COALESCE($8::smallint, 0), COALESCE($9::smallint, 0), 1, CURRENT_TIMESTAMP
                )
                ON CONFLICT (tenant_id, user_id) DO UPDATE SET {SETTINGS_ASSIGNMENTS}
                RETURNING {SETTINGS_COLUMNS}
                "#
            )
        } else {
            format!(
                r#"
                UPDATE user_notification_settings AS s SET {SETTINGS_ASSIGNMENTS}
                WHERE s.tenant_id = $1 AND s.user_id = $2 AND s.settings_version = $10
                RETURNING {SETTINGS_COLUMNS}
                "#
            )
        };
        let mut query = sqlx::query(&sql)
            .bind(tenant_id)
            .bind(user_id)
            .bind(replace_quiet)
            .bind(start)
            .bind(end)
            .bind(weekdays)
            .bind(allow_mentions)
            .bind(preview)
            .bind(multi_device);
        if patch.base_settings_version != 0 {
            query = query.bind(i64::try_from(patch.base_settings_version).unwrap_or(i64::MAX));
        }
        let row = query.fetch_optional(&*self.pool).await.map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DatabaseError,
                "update user notification settings",
            )
        })?;
        let Some(row) = row else {
            return Err(ErrorBuilder::new(
                ErrorCode::HttpConflict,
                "user notification settings version conflict",
            )
            .build_error());
        };
        Ok(settings_from_row(&row))
    }

    async fn list_notification_subscribers(
        &self,
        ctx: &flare_server_core::context::Context,
        conversation_id: &str,
        after_user_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>> {
        let tenant_id = required_tenant(ctx)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        // 部分索引 idx_conversation_participants_notify_all 只收 notify_all 为真的行，按 user_id
        // 有序；空串小于任何非空的 user_id，第一页也走同一个范围条件。
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT user_id::text
            FROM conversation_participants
            WHERE tenant_id = $1
              AND conversation_id = $2
              AND notify_all
              AND user_id > $3
              AND quit_at IS NULL
              AND NOT COALESCE(is_deleted, false)
              AND NOT COALESCE(muted, false)
              AND NOT mention_only
            ORDER BY user_id ASC
            LIMIT $4
            "#,
        )
        .bind(tenant_id)
        .bind(conversation_id)
        .bind(after_user_id.unwrap_or(""))
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&*self.pool)
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "list notification subscribers"))
    }
}
