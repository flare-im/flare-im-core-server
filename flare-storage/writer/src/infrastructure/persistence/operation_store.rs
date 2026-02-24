use anyhow::Result;
use chrono::Utc;
use flare_im_core::utils::timestamp_to_datetime;
use flare_proto::common::MessageContent;
use flare_proto::MessageContentExt;
use prost::Message as ProstMessage;
use serde_json::{json, Value};
use sqlx::{Pool, Postgres, QueryBuilder, Row};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

pub struct OperationStore {
    pool: Pool<Postgres>,
}

impl OperationStore {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub async fn update_message_fsm_state(
        &self,
        _tenant_id: &str, // 保留参数用于接口一致性，但 UPDATE 时不需要作为条件（唯一索引已保证）
        message_id: &str,
        fsm_state: &str,
        recall_reason: Option<&str>,
    ) -> Result<()> {
        let mut query = QueryBuilder::new("UPDATE messages SET status = ");
        query.push_bind(fsm_state);
        query.push(", fsm_state_changed_at = CURRENT_TIMESTAMP");
        query.push(", updated_at = CURRENT_TIMESTAMP");

        if let Some(reason) = recall_reason {
            query.push(", recall_reason = ");
            query.push_bind(reason);
        }

        // 支持通过 server_id 或 client_msg_id 更新
        query.push(" WHERE server_id = ");
        query.push_bind(message_id);
        query.push(" OR client_msg_id = ");
        query.push_bind(message_id);

        query.build().execute(&self.pool).await?;

        Ok(())
    }

    pub async fn update_message_content(
        &self,
        tenant_id: &str, // INSERT 时需要，UPDATE 时不需要作为条件
        message_id: &str,
        new_content: &MessageContent,
        edit_version: i32,
        editor_id: &str,
        reason: Option<&str>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        // 查询时不使用 tenant_id 进行数据隔离，因为 tenant_id 不再强制要求
        tracing::info!(
            message_id = %message_id,
            "🔍 查询消息以获取当前编辑版本，支持 server_id 或 client_msg_id"
        );
        
        let current_message_row = sqlx::query(
            r#"
            SELECT content, current_edit_version, extra, server_id
            FROM messages
            WHERE (server_id = $1 OR client_msg_id = $1)
            "#,
        )
        .bind(message_id)
        .fetch_optional(&mut *tx)
        .await?;

        let (current_edit_version, mut extra, real_server_id) = match current_message_row {
            Some(row) => {
                let version = row.get::<i32, _>("current_edit_version");
                let extra = row.get::<Value, _>("extra");
                let server_id = row.get::<String, _>("server_id");
                tracing::info!(
                    message_id = %message_id,
                    real_server_id = %server_id,
                    current_edit_version = version,
                    "✅ 找到消息，当前编辑版本: {}",
                    version
                );
                (version, extra, server_id)
            }
            None => {
                tx.rollback().await?;
                tracing::error!(
                    tenant_id = %tenant_id,
                    message_id = %message_id,
                    "❌ 消息不存在，无法编辑。请确保 target_message_id 是服务端返回的 server_msg_id 或 client_msg_id"
                );
                return Err(anyhow::anyhow!("Message not found: {} (tenant_id: {}).", message_id, tenant_id));
            }
        };

        // **关键修复**：如果传入的 edit_version 无效（<= 0 或 <= current_edit_version），自动计算为 current_edit_version + 1
        let final_edit_version = if edit_version <= 0 || edit_version <= current_edit_version {
            current_edit_version + 1
        } else {
                edit_version
        };

        // **关键修复**：如果是文本消息，同步更新 extra 字段中的 content_text
        // 这样客户端在获取列表或搜索时能看到最新的文本内容
        if let Some(flare_proto::common::message_content::Content::Text(text)) = &new_content.content {
            if let Value::Object(ref mut map) = extra {
                map.insert("content_text".to_string(), Value::String(text.text.clone()));
                tracing::info!(
                    message_id = %message_id,
                    "📝 更新 extra.content_text 为新文本"
                );
            }
        }

        tracing::info!(
            message_id = %message_id,
            real_server_id = %real_server_id,
            current_edit_version = current_edit_version,
            provided_edit_version = edit_version,
            final_edit_version = final_edit_version,
            "📝 计算编辑版本号"
        );

        // 使用统一的编码方法（高性能、一致性）
        let new_content_bytes = new_content.encode_to_bytes()?;

        // UPDATE 使用 real_server_id
        sqlx::query(
            r#"
            UPDATE messages
            SET 
                content = $1,
                status = 'EDITED',
                fsm_state_changed_at = CURRENT_TIMESTAMP,
                current_edit_version = $2,
                last_edited_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP,
                extra = $3
            WHERE server_id = $4
            "#,
        )
        .bind(&new_content_bytes)
        .bind(final_edit_version)  // **关键修复**：使用计算后的版本号
        .bind(&extra)
        .bind(&real_server_id)
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
        .bind(&real_server_id) // 使用 real_server_id
        .bind(final_edit_version)  // **关键修复**：使用计算后的版本号
        .bind(&new_content_bytes)
        .bind(editor_id)
        .bind(reason)
        .bind(true)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(())
    }

    pub async fn update_message_visibility(
        &self,
        tenant_id: &str, // INSERT 时需要，ON CONFLICT 会自动处理
        message_id: &str,
        user_id: &str,
        visibility_status: &str,
    ) -> Result<()> {
        // INSERT 时携带 tenant_id，ON CONFLICT 使用唯一约束 (tenant_id, message_id, user_id)
        sqlx::query(
            r#"
            INSERT INTO message_visibility (tenant_id, message_id, user_id, visibility_status, changed_at)
            VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
            ON CONFLICT (tenant_id, message_id, user_id) 
            DO UPDATE SET 
                visibility_status = EXCLUDED.visibility_status,
                changed_at = EXCLUDED.changed_at,
                updated_at = CURRENT_TIMESTAMP
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .bind(user_id)
        .bind(visibility_status)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn record_message_read(
        &self,
        tenant_id: &str, // INSERT 时需要，ON CONFLICT 会自动处理
        message_id: &str,
        user_id: &str,
    ) -> Result<()> {
        // INSERT 时携带 tenant_id，ON CONFLICT 使用唯一约束 (tenant_id, message_id, user_id)
        sqlx::query(
            r#"
            INSERT INTO message_read_records (tenant_id, message_id, user_id, read_at)
            VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
            ON CONFLICT (tenant_id, message_id, user_id) 
            DO UPDATE SET read_at = EXCLUDED.read_at
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
            r#"
            SELECT user_ids, count
            FROM message_reactions
            WHERE tenant_id = $1 AND message_id = $2 AND emoji = $3
            "#,
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
                // UPDATE 时不需要 tenant_id 作为条件（唯一约束已保证）
                sqlx::query(
                    r#"
                    UPDATE message_reactions
                    SET user_ids = $1, count = $2, last_updated = CURRENT_TIMESTAMP
                    WHERE message_id = $3 AND emoji = $4
                    "#,
                )
                .bind(&user_ids)
                .bind(count)
                .bind(message_id)
                .bind(emoji)
                .execute(&mut *tx)
                .await?;
            } else {
                // DELETE 时不需要 tenant_id 作为条件（唯一约束已保证）
                sqlx::query(
                    r#"
                    DELETE FROM message_reactions
                    WHERE message_id = $1 AND emoji = $2
                    "#,
                )
                .bind(message_id)
                .bind(emoji)
                .execute(&mut *tx)
                .await?;
            }
        } else if add {
            sqlx::query(
                r#"
                INSERT INTO message_reactions (tenant_id, message_id, emoji, user_ids, count, last_updated)
                VALUES ($1, $2, $3, $4, 1, CURRENT_TIMESTAMP)
                "#,
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

    pub async fn pin_message(
        &self,
        tenant_id: &str, // INSERT 时需要，DELETE 时不需要作为条件
        message_id: &str,
        conversation_id: &str,
        user_id: &str,
        pin: bool,
        expire_at: Option<chrono::DateTime<chrono::Utc>>,
        reason: Option<&str>,
    ) -> Result<()> {
        if pin {
            // INSERT 时携带 tenant_id，ON CONFLICT 使用唯一约束 (tenant_id, conversation_id, message_id)
            sqlx::query(
                r#"
                INSERT INTO pinned_messages (tenant_id, message_id, conversation_id, pinned_by, pinned_at, expire_at, reason)
                VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, $5, $6)
                ON CONFLICT (tenant_id, conversation_id, message_id) 
                DO UPDATE SET 
                    pinned_by = EXCLUDED.pinned_by,
                    pinned_at = EXCLUDED.pinned_at,
                    expire_at = EXCLUDED.expire_at,
                    reason = EXCLUDED.reason
                "#,
            )
            .bind(tenant_id)
            .bind(message_id)
            .bind(conversation_id)
            .bind(user_id)
            .bind(expire_at)
            .bind(reason)
            .execute(&self.pool)
            .await?;
        } else {
            // DELETE 时不需要 tenant_id 作为条件（唯一约束已保证）
            sqlx::query(
                r#"
                DELETE FROM pinned_messages
                WHERE message_id = $1 AND conversation_id = $2
                "#,
            )
            .bind(message_id)
            .bind(conversation_id)
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

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
        if add {
            sqlx::query(
                r#"
                INSERT INTO marked_messages (tenant_id, message_id, user_id, conversation_id, mark_type, color, marked_at)
                VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
                ON CONFLICT (tenant_id, message_id, user_id, mark_type) 
                DO UPDATE SET 
                    color = EXCLUDED.color,
                    marked_at = EXCLUDED.marked_at,
                    updated_at = CURRENT_TIMESTAMP
                "#,
            )
            .bind(tenant_id)
            .bind(message_id)
            .bind(user_id)
            .bind(conversation_id)
            .bind(mark_type)
            .bind(color)
            .execute(&self.pool)
            .await?;
        } else {
            // DELETE 时不需要 tenant_id 作为条件（唯一约束已保证）
            sqlx::query(
                r#"
                DELETE FROM marked_messages
                WHERE message_id = $1 AND user_id = $2 AND mark_type = $3
                "#,
            )
            .bind(message_id)
            .bind(user_id)
            .bind(mark_type)
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

    pub async fn append_operation(
        &self,
        tenant_id: &str,
        message_id: &str,
        operation: &flare_proto::common::MessageOperation,
    ) -> Result<()> {
        let operation_timestamp = operation
            .timestamp
            .as_ref()
            .and_then(|ts| timestamp_to_datetime(ts))
            .unwrap_or_else(|| Utc::now());

        // 将 operation_data 转换为 JSONB
        // 由于 protobuf 生成的类型没有实现 Serialize，我们使用 protobuf 的 encode 方法
        let operation_data_json = if let Some(op_data) = &operation.operation_data {
            // 将 protobuf 消息编码为 bytes，然后转换为 base64 字符串存储
            use flare_proto::common::message_operation::OperationData;
            let mut buf = Vec::new();
            match op_data {
                OperationData::Recall(data) => {
                    data.encode(&mut buf)?;
                }
                OperationData::Edit(data) => {
                    data.encode(&mut buf)?;
                }
                OperationData::Delete(data) => {
                    data.encode(&mut buf)?;
                }
                OperationData::Read(data) => {
                    data.encode(&mut buf)?;
                }
                OperationData::Reaction(data) => {
                    data.encode(&mut buf)?;
                }
                OperationData::Pin(data) => {
                    data.encode(&mut buf)?;
                }
                OperationData::Mark(data) => {
                    data.encode(&mut buf)?;
                }
                OperationData::Unmark(data) => {
                    data.encode(&mut buf)?;
                }
            }
            // 使用 base64 编码存储
            json!({
                "encoded": BASE64.encode(&buf),
                "type": format!("{:?}", op_data)
            })
        } else {
            Value::Null
        };

        sqlx::query(
            r#"
            INSERT INTO message_operation_history (
                tenant_id, message_id, operation_type, operator_id, target_user_id,
                operation_data, show_notice, notice_text, timestamp, metadata
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .bind(operation.operation_type as i32)
        .bind(&operation.operator_id)
        .bind(&operation.target_user_id)
        .bind(operation_data_json)
        .bind(operation.show_notice)
        .bind(&operation.notice_text)
        .bind(operation_timestamp)
        .bind(json!(&operation.metadata))
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

