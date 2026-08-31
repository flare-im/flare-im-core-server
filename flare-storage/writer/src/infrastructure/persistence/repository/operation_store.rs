use base64::Engine;
use chrono::Utc;
use flare_proto::common::{Event, EventType, MessageStatus};
use flare_server_core::error::Result;
use prost::Message as ProstMessage;
use serde_json::{Value, json};
use sqlx::{Pool, Postgres, Row};

const MESSAGE_PIN_SCOPE_SELF: i32 = 1;

/// FSM 状态名 → `messages.status`（proto `MessageStatus` 的枚举值）。
///
/// 取值一律从 proto 枚举现算，不写字面量：这张表原先把 RECALLED 映射成 6，
/// 而 6 是 DELETED、5 才是 RECALLED；读写两侧用的是同一套错值，服务端内部
/// 自洽，于是没人发现。暴露它的是客户端——SDK 按 `status == RECALLED` 判
/// `is_recalled`，从服务端全量同步时读到 6 判定为 false，撤回过的消息会带着
/// 原文重新显示（换设备、重装、清缓存后必现）。
///
/// 硬删与软删都落 DELETED：proto 只有一个 DELETED，删除的作用域由
/// MessageDeleteEvent 承载。软删实际走 `update_message_visibility`，压根不改
/// 这一列，"DELETED_SOFT" 只是保持映射完整。
fn fsm_state_to_status_int(fsm_state: &str) -> i32 {
    let status = match fsm_state.to_uppercase().as_str() {
        "CREATED" | "INIT" => MessageStatus::Created,
        "SENT" | "EDITED" | "DELIVERED" | "READ" => MessageStatus::Sent,
        "FAILED" => MessageStatus::Failed,
        "RECALLED" => MessageStatus::Recalled,
        "DELETED_HARD" | "DELETED_SOFT" => MessageStatus::Deleted,
        _ => MessageStatus::Sent,
    };
    status as i32
}

/// init_v2: message_visibility.visibility_status 为 INT（0=VISIBLE, 1=HIDDEN, 2=DELETED）
fn visibility_status_to_int(s: &str) -> i32 {
    match s.to_uppercase().as_str() {
        "HIDDEN" => 1,
        "DELETED" => 2,
        _ => 0,
    }
}

/// init_v2: marked_messages.mark_type 为 INT（1=IMPORTANT, 2=TODO, 3=DONE, 4=CUSTOM）
fn mark_type_to_int(s: &str) -> i32 {
    match s.to_uppercase().as_str() {
        "IMPORTANT" => 1,
        "TODO" => 2,
        "DONE" => 3,
        "CUSTOM" => 4,
        _ => 1,
    }
}

fn event_type_to_db_string(v: i32) -> &'static str {
    match EventType::try_from(v) {
        Ok(EventType::EventMessageRecall) => "EVENT_MESSAGE_RECALL",
        Ok(EventType::EventMessageEdit) => "EVENT_MESSAGE_EDIT",
        Ok(EventType::EventMessageDelete) => "EVENT_MESSAGE_DELETE",
        Ok(EventType::EventReadReceipt) => "EVENT_READ_RECEIPT",
        Ok(EventType::EventReaction) => "EVENT_REACTION",
        Ok(EventType::EventPin) => "EVENT_PIN",
        Ok(EventType::EventUnpin) => "EVENT_UNPIN",
        Ok(EventType::EventMark) => "EVENT_MARK",
        Ok(EventType::EventUnmark) => "EVENT_UNMARK",
        _ => "EVENT_TYPE_UNSPECIFIED",
    }
}

pub struct OperationStore {
    pool: Pool<Postgres>,
}

impl OperationStore {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    /// init_v2: messages.status 为 INT（MessageStatus 枚举值）
    pub async fn update_message_fsm_state(
        &self,
        tenant_id: &str,
        message_id: &str,
        fsm_state: &str,
        _recall_reason: Option<&str>,
    ) -> Result<()> {
        let status_int = fsm_state_to_status_int(fsm_state);
        sqlx::query(
            r#"
            UPDATE messages SET status = $1
            WHERE tenant_id = $2 AND (server_id = $3 OR client_msg_id = $3)
            "#,
        )
        .bind(status_int)
        .bind(tenant_id)
        .bind(message_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// init_v2: messages 仅更新 content、extra；extra 内编辑元数据为 camelCase（`currentEditVersion` 等）
    #[allow(clippy::too_many_arguments)]
    pub async fn update_message_content(
        &self,
        tenant_id: &str,
        message_id: &str,
        new_content: &[u8],
        editor_id: &str,
        reason: Option<&str>,
        content_text_for_extra: Option<&str>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query(
            r#"
            SELECT content, extra, server_id FROM messages
            WHERE tenant_id = $2 AND (server_id = $1 OR client_msg_id = $1)
            "#,
        )
        .bind(message_id)
        .bind(tenant_id)
        .fetch_optional(&mut *tx)
        .await?;

        let (mut extra, real_server_id) = match row {
            Some(r) => (r.get::<Value, _>("extra"), r.get::<String, _>("server_id")),
            None => {
                tx.rollback().await?;
                return Err(flare_server_core::error::FlareError::system(format!(
                    "Message not found: {} (tenant_id: {}).",
                    message_id, tenant_id
                )));
            }
        };

        if !extra.is_object() {
            extra = Value::Object(serde_json::Map::new());
        }

        let next_version_row = sqlx::query_scalar::<_, i32>(
            r#"
            SELECT COALESCE(MAX(edit_version), 0) + 1 FROM message_edit_history
            WHERE tenant_id = $1 AND message_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(&real_server_id)
        .fetch_optional(&mut *tx)
        .await?;
        // 版本号完全由本表权威推导：调用方曾传入的 edit_version 恒为 0（发布端无法在
        // 异步扇出下同步分配），只作下限从无实际作用，已随 proto 死字段一并移除。
        let final_edit_version = next_version_row.unwrap_or(1).max(1);

        if let Value::Object(ref mut map) = extra {
            if let Some(text) = content_text_for_extra {
                map.insert("contentText".to_string(), Value::String(text.to_string()));
            }
            // 与编排/Reader 约定一致：同步下行时 SDK 用 extra 识别「已编辑」（proto 无独立 EDITED 状态位）
            map.insert(
                "messageFsmState".to_string(),
                Value::String("EDITED".to_string()),
            );
            map.insert(
                "currentEditVersion".to_string(),
                Value::String(final_edit_version.to_string()),
            );
        }

        sqlx::query(
            r#"
            UPDATE messages SET content = $1, extra = $2
            WHERE server_id = $3 AND tenant_id = $4
            "#,
        )
        .bind(new_content)
        .bind(&extra)
        .bind(&real_server_id)
        .bind(tenant_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO message_edit_history (tenant_id, message_id, edit_version, content, editor_id, reason, show_edited_mark)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (tenant_id, message_id, edit_version) DO NOTHING
            "#,
        )
        .bind(tenant_id)
        .bind(&real_server_id)
        .bind(final_edit_version)
        .bind(new_content)
        .bind(editor_id)
        .bind(reason)
        .bind(true)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    /// init_v2: message_visibility.visibility_status 为 INT（0=VISIBLE, 1=HIDDEN, 2=DELETED）
    pub async fn update_message_visibility(
        &self,
        tenant_id: &str,
        message_id: &str,
        user_id: &str,
        scope: i32,
        visibility_status: &str,
    ) -> Result<()> {
        let status_int = visibility_status_to_int(visibility_status);
        sqlx::query(
            r#"
            INSERT INTO message_visibility (tenant_id, message_id, user_id, scope, visibility_status, changed_at)
            VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
            ON CONFLICT (tenant_id, message_id, user_id, scope)
            DO UPDATE SET visibility_status = EXCLUDED.visibility_status, changed_at = EXCLUDED.changed_at
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .bind(user_id)
        .bind(scope)
        .bind(status_int)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn record_message_read(
        &self,
        tenant_id: &str,
        message_id: &str,
        user_id: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO message_read_records (tenant_id, message_id, user_id, read_at)
            VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
            ON CONFLICT (tenant_id, message_id, user_id) DO UPDATE SET read_at = EXCLUDED.read_at
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .bind(user_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_message_reaction(
        &self,
        tenant_id: &str,
        message_id: &str,
        emoji: &str,
        user_id: &str,
        add: bool,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        let reaction_row = sqlx::query(
            r#"SELECT user_ids, count FROM message_reactions WHERE tenant_id = $1 AND message_id = $2 AND emoji = $3"#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .bind(emoji)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(row) = reaction_row {
            let mut user_ids: Vec<String> = row.get("user_ids");
            let mut count: i32 = row.get("count");

            if add {
                if !user_ids.contains(&user_id.to_string()) {
                    user_ids.push(user_id.to_string());
                    count += 1;
                }
            } else {
                user_ids.retain(|id| id != user_id);
                count = count.max(0) - 1;
            }

            if count > 0 {
                sqlx::query(
                    r#"UPDATE message_reactions SET user_ids = $1, count = $2, last_updated = CURRENT_TIMESTAMP WHERE message_id = $3 AND emoji = $4 AND tenant_id = $5"#,
                )
                .bind(&user_ids)
                .bind(count)
                .bind(message_id)
                .bind(emoji)
                .bind(tenant_id)
                .execute(&mut *tx)
                .await?;
            } else {
                sqlx::query(r#"DELETE FROM message_reactions WHERE message_id = $1 AND emoji = $2 AND tenant_id = $3"#)
                    .bind(message_id)
                    .bind(emoji)
                    .bind(tenant_id)
                    .execute(&mut *tx)
                    .await?;
            }
        } else if add {
            sqlx::query(
                r#"INSERT INTO message_reactions (tenant_id, message_id, emoji, user_ids, count, last_updated) VALUES ($1, $2, $3, $4, 1, CURRENT_TIMESTAMP)"#,
            )
            .bind(tenant_id)
            .bind(message_id)
            .bind(emoji)
            .bind(vec![user_id.to_string()])
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn pin_message(
        &self,
        tenant_id: &str,
        message_id: &str,
        conversation_id: &str,
        user_id: &str,
        scope: i32,
        pin: bool,
        expire_at: Option<chrono::DateTime<chrono::Utc>>,
        reason: Option<&str>,
    ) -> Result<()> {
        let owner_user_id = if scope == MESSAGE_PIN_SCOPE_SELF {
            user_id
        } else {
            ""
        };
        if pin {
            sqlx::query(
                r#"
                INSERT INTO pinned_messages (tenant_id, message_id, conversation_id, pinned_by, scope, owner_user_id, pinned_at, expire_at, reason)
                VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, $7, $8)
                ON CONFLICT (tenant_id, conversation_id, message_id, scope, owner_user_id)
                DO UPDATE SET pinned_by = EXCLUDED.pinned_by, pinned_at = EXCLUDED.pinned_at, expire_at = EXCLUDED.expire_at, reason = EXCLUDED.reason
                "#,
            )
            .bind(tenant_id)
            .bind(message_id)
            .bind(conversation_id)
            .bind(user_id)
            .bind(scope)
            .bind(owner_user_id)
            .bind(expire_at)
            .bind(reason)
            .execute(&self.pool)
            .await?;
        } else {
            sqlx::query(r#"DELETE FROM pinned_messages WHERE tenant_id = $1 AND message_id = $2 AND conversation_id = $3 AND scope = $4 AND owner_user_id = $5"#)
                .bind(tenant_id)
                .bind(message_id)
                .bind(conversation_id)
                .bind(scope)
                .bind(owner_user_id)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    /// init_v2: marked_messages.mark_type 为 INT（1=IMPORTANT, 2=TODO, 3=DONE, 4=CUSTOM）
    #[allow(clippy::too_many_arguments)]
    pub async fn mark_message(
        &self,
        tenant_id: &str,
        message_id: &str,
        conversation_id: &str,
        user_id: &str,
        mark_type: &str,
        color: Option<&str>,
        add: bool,
    ) -> Result<()> {
        let mark_type_int = mark_type_to_int(mark_type);
        if add {
            sqlx::query(
                r#"
                INSERT INTO marked_messages (tenant_id, message_id, user_id, conversation_id, mark_type, color, marked_at)
                VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
                ON CONFLICT (tenant_id, message_id, user_id, mark_type)
                DO UPDATE SET color = EXCLUDED.color, marked_at = EXCLUDED.marked_at
                "#,
            )
            .bind(tenant_id)
            .bind(message_id)
            .bind(user_id)
            .bind(conversation_id)
            .bind(mark_type_int)
            .bind(color)
            .execute(&self.pool)
            .await?;
        } else {
            sqlx::query(r#"DELETE FROM marked_messages WHERE tenant_id = $1 AND message_id = $2 AND user_id = $3 AND mark_type = $4"#)
                .bind(tenant_id)
                .bind(message_id)
                .bind(user_id)
                .bind(mark_type_int)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    /// 追加事件到操作历史。operator_id 由领域侧从 metadata 注入传入（proto Event 无此字段）。
    pub async fn append_event(
        &self,
        tenant_id: &str,
        message_id: &str,
        event: &Event,
        operator_id: &str,
    ) -> Result<()> {
        let ts = if event.created_at > 0 {
            chrono::DateTime::<Utc>::from_timestamp_millis(event.created_at)
                .unwrap_or_else(Utc::now)
        } else {
            Utc::now()
        };
        let mut buf = Vec::new();
        event.encode(&mut buf)?;
        let encoded = base64::engine::general_purpose::STANDARD.encode(&buf);
        let operation_data_json = json!({ "event_base64": encoded });

        sqlx::query(
            r#"
            INSERT INTO message_operation_history (
                tenant_id, message_id, operation_type, operator_id, target_user_id,
                operation_data, timestamp, metadata
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .bind(event_type_to_db_string(event.r#type))
        .bind(operator_id)
        .bind("")
        .bind(operation_data_json)
        .bind(ts)
        .bind(Value::Object(serde_json::Map::new()))
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 撤回必须落 RECALLED，不能落 DELETED。
    ///
    /// 这两个值只差 1，而且读写两侧一起写错时服务端完全自洽——只有从服务端
    /// 全量同步的客户端会把撤回过的消息当成正常消息重新显示出来。断言写死
    /// 期望值（而不是同样用枚举现算），否则枚举变了这条测试会跟着变，等于没测。
    #[test]
    fn recall_maps_to_recalled_not_deleted() {
        assert_eq!(fsm_state_to_status_int("RECALLED"), 5);
        assert_ne!(
            fsm_state_to_status_int("RECALLED"),
            fsm_state_to_status_int("DELETED_HARD"),
            "撤回与删除必须是可区分的两个状态"
        );
    }

    #[test]
    fn delete_maps_to_deleted_within_the_proto_enum() {
        // proto MessageStatus 只有一个 DELETED=6，没有 7/8；
        // 硬删软删的区别由 MessageDeleteEvent 承载，不靠 status 编码。
        assert_eq!(fsm_state_to_status_int("DELETED_HARD"), 6);
        assert_eq!(fsm_state_to_status_int("DELETED_SOFT"), 6);
    }

    #[test]
    fn failed_is_not_collapsed_into_sent() {
        assert_eq!(fsm_state_to_status_int("FAILED"), 4);
        assert_eq!(fsm_state_to_status_int("SENT"), 2);
    }

    #[test]
    fn every_mapped_state_stays_inside_the_proto_enum() {
        // 越界值（此前的 7、8）会让按 proto 解码 status 的一侧拿到未知变体。
        let valid = [
            MessageStatus::Created as i32,
            MessageStatus::Sent as i32,
            MessageStatus::Persisted as i32,
            MessageStatus::Failed as i32,
            MessageStatus::Recalled as i32,
            MessageStatus::Deleted as i32,
        ];
        for state in [
            "CREATED",
            "INIT",
            "SENT",
            "EDITED",
            "DELIVERED",
            "READ",
            "FAILED",
            "RECALLED",
            "DELETED_HARD",
            "DELETED_SOFT",
            "SOMETHING_UNKNOWN",
        ] {
            let mapped = fsm_state_to_status_int(state);
            assert!(
                valid.contains(&mapped),
                "{state} 映射成了 proto 枚举之外的 {mapped}"
            );
        }
    }
}
