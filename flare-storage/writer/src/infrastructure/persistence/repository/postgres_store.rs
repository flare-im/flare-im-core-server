use base64::Engine;
use flare_im_contracts::Ctx;
use flare_im_contracts::utils::normalize_tenant_id;
use flare_proto::common::{
    ContentVisibility, MessageContent, MessageRetentionLifecycle, MessageRetentionPolicy,
    MessageRetentionState, RetentionMode,
};
use flare_server_core::error::Result;
use prost::Message as ProstMessage;
use serde_json::to_value;
use sqlx::{Pool, Postgres, Row, postgres::PgPoolOptions};
use tracing::{instrument, warn};

use super::operation_store;
use crate::convert;

use crate::config::StorageWriterConfig;
use crate::domain::model::{Event, Message};
use crate::domain::repository::{
    ArchiveStoreRepository, MessageWriteLedgerRepository, MessageWriteStage,
};

const LEGACY_BURN_STATUS_INIT: i32 = 1;
const LEGACY_BURN_STATUS_READ: i32 = 2;
const LEGACY_BURN_STATUS_BURN_PENDING: i32 = 3;
const LEGACY_BURN_STATUS_BURNED: i32 = 4;
const LEGACY_BURN_STATUS_HARD_DELETED: i32 = 5;

/// 与 deploy/init.sql messages 表结构对齐（无 receiver_id，与 common/message.proto 一致）
#[derive(sqlx::FromRow)]
#[allow(dead_code)]
struct MessageRow {
    tenant_id: String,
    server_id: String,
    conversation_id: String,
    client_msg_id: Option<String>,
    sender_id: String,
    sender_name: Option<String>,
    sender_avatar: Option<String>,
    channel_id: Option<String>,
    thread_id: Option<String>,
    source: i32,
    seq: i64,
    timestamp: chrono::DateTime<chrono::Utc>,
    conversation_type: i32,
    message_type: i32,
    content: Option<Vec<u8>>,
    status: i32,
    burn_enabled: bool,
    burn_after_read_seconds: Option<i64>,
    burn_status: i32,
    first_read_at: Option<i64>,
    burn_at: Option<i64>,
    burned_at: Option<i64>,
    offline_push_info: Option<serde_json::Value>,
    extra: Option<serde_json::Value>,
    extensions: Option<serde_json::Value>,
    created_at: chrono::DateTime<chrono::Utc>,
    persisted_at: Option<chrono::DateTime<chrono::Utc>>,
    delivered_at: Option<chrono::DateTime<chrono::Utc>>,
}

pub struct PostgresMessageStore {
    pool: Pool<Postgres>,
    operation_store: operation_store::OperationStore,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DueBurnMessageRow {
    pub tenant_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub burn_at: i64,
}

impl PostgresMessageStore {
    pub async fn new(config: &StorageWriterConfig) -> Result<Option<Self>> {
        let url = match &config.postgres_url {
            Some(url) => url,
            None => return Ok(None),
        };

        // 优化连接池配置（根据配置参数）
        let pool = PgPoolOptions::new()
            .max_connections(config.postgres_max_connections)
            .min_connections(config.postgres_min_connections)
            .acquire_timeout(std::time::Duration::from_secs(
                config.postgres_acquire_timeout_seconds,
            ))
            .idle_timeout(Some(std::time::Duration::from_secs(
                config.postgres_idle_timeout_seconds,
            )))
            .max_lifetime(Some(std::time::Duration::from_secs(
                config.postgres_max_lifetime_seconds,
            )))
            .test_before_acquire(true) // 获取连接前测试连接是否有效
            .connect(url)
            .await?;

        Ok(Some(Self::from_pool(pool)))
    }

    /// 插件实体绑定的登记（同一个连接池）。
    pub fn plugin_entity_bindings(
        &self,
    ) -> super::plugin_entity_bindings::PostgresPluginEntityBindings {
        super::plugin_entity_bindings::PostgresPluginEntityBindings::new(self.pool.clone())
    }

    /// 用已有连接池构造（真库用例等）。
    pub fn from_pool(pool: Pool<Postgres>) -> Self {
        let operation_store = operation_store::OperationStore::new(pool.clone());
        Self {
            pool,
            operation_store,
        }
    }
}

impl MessageWriteLedgerRepository for PostgresMessageStore {
    fn mark_stage<'a>(
        &'a self,
        ctx: &'a Ctx,
        tenant_id: &'a str,
        message_id: &'a str,
        stage: MessageWriteStage,
        error: Option<&'a str>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let _ = ctx;
            let timestamp_column = stage.timestamp_column();
            let sql = format!(
                r#"
                UPDATE message_write_ledger
                SET write_state = $3,
                    {timestamp_column} = COALESCE({timestamp_column}, CURRENT_TIMESTAMP),
                    last_error = $4::TEXT,
                    failed_at = CASE
                        WHEN $4::TEXT IS NULL THEN failed_at
                        ELSE CURRENT_TIMESTAMP
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE tenant_id = $1
                  AND server_id = $2
                "#
            );

            let result = sqlx::query(&sql)
                .bind(tenant_id)
                .bind(message_id)
                .bind(stage.as_str())
                .bind(error)
                .execute(&self.pool)
                .await?;

            if result.rows_affected() == 0 {
                tracing::trace!(
                    tenant_id = %tenant_id,
                    message_id = %message_id,
                    stage = %stage.as_str(),
                    "Message write ledger stage skipped because row was not found"
                );
            }

            Ok(())
        })
    }
}

impl PostgresMessageStore {
    /// 获取数据库连接池（用于创建 ConversationRepository）
    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }

    pub async fn scan_due_burn_messages(
        &self,
        tenant_id: &str,
        now: i64,
        limit: i64,
    ) -> Result<Vec<DueBurnMessageRow>> {
        let limit = limit.clamp(1, 1000);
        let rows = sqlx::query_as::<_, DueBurnMessageRow>(
            r#"
            SELECT tenant_id, conversation_id, server_id AS message_id, burn_at
            FROM messages
            WHERE tenant_id = $1
              AND burn_status = $2
              AND burn_at IS NOT NULL
              AND burn_at <= $3
            ORDER BY burn_at ASC
            LIMIT $4
            "#,
        )
        .bind(tenant_id)
        .bind(LEGACY_BURN_STATUS_BURN_PENDING)
        .bind(now)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// 根据消息ID查询消息（内部辅助方法），返回领域模型（init_v2 列 + common/message.proto）
    async fn get_message_by_id(&self, message_id: &str) -> Result<Option<Message>> {
        use serde_json::Value as JsonValue;

        let row = sqlx::query_as::<_, MessageRow>(
            r#"
            SELECT tenant_id, server_id, conversation_id, client_msg_id, sender_id, sender_name, sender_avatar,
                channel_id, thread_id, source, seq, timestamp, conversation_type, message_type, content,
                status, burn_enabled, burn_after_read_seconds, burn_status, first_read_at, burn_at,
                burned_at, offline_push_info, extra, extensions, created_at, persisted_at, delivered_at
            FROM messages WHERE server_id = $1 LIMIT 1
            "#,
        )
        .bind(message_id)
        .fetch_optional(&self.pool)
        .await?;

        let row = match row {
            Some(r) => r,
            None => return Ok(None),
        };

        let extra_value: JsonValue = row.extra.unwrap_or_else(|| serde_json::json!({}));
        let extra_map = extra_value
            .as_object()
            .cloned()
            .unwrap_or_else(serde_json::Map::new);

        let content = row
            .content
            .clone()
            .filter(|bytes| !bytes.is_empty())
            .and_then(|bytes| MessageContent::decode(bytes.as_slice()).ok());

        let timestamp = Some(prost_types::Timestamp {
            seconds: row.timestamp.timestamp(),
            nanos: row.timestamp.timestamp_subsec_nanos() as i32,
        });

        let offline_push_info = row.offline_push_info.as_ref().and_then(|v| {
            use flare_proto::common::OfflinePushInfo;
            let o = v.as_object()?;
            Some(OfflinePushInfo {
                title: o
                    .get("title")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string(),
                body: o
                    .get("body")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string(),
                sound: o
                    .get("sound")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string(),
                badge: o.get("badge").and_then(|b| b.as_bool()).unwrap_or(false),
                payload: o
                    .get("payload")
                    .and_then(|p| p.as_str())
                    .unwrap_or("")
                    .to_string(),
            })
        });

        let extensions: std::collections::HashMap<String, Vec<u8>> = row
            .extensions
            .as_ref()
            .and_then(|v| v.as_object())
            .map(|o| {
                o.iter()
                    .filter_map(|(k, v)| {
                        let s = v.as_str()?;
                        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                            .ok()
                            .map(|bytes| (k.clone(), bytes))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut extra: std::collections::HashMap<String, String> = extra_map
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
            .collect();
        extra.insert("tenant_id".to_string(), row.tenant_id.clone());

        let msg = Message {
            server_id: row.server_id,
            conversation_id: row.conversation_id,
            client_msg_id: row.client_msg_id.unwrap_or_default(),
            sender_id: row.sender_id,
            sender_name: row.sender_name.unwrap_or_default(),
            sender_avatar: row.sender_avatar.unwrap_or_default(),
            source: row.source,
            conversation_seq: row.seq as u64,
            timestamp,
            conversation_type: row.conversation_type,
            message_type: row.message_type,
            message_seq: None,
            channel_id: row.channel_id.unwrap_or_default(),
            thread_id: row.thread_id,
            content,
            status: row.status,
            retention_policy: None,
            retention_state: legacy_retention_state(
                row.burn_status,
                row.first_read_at,
                row.burn_at,
                row.burned_at,
            ),
            offline_push_info,
            extra,
            extensions,
        };
        Ok(Some(msg))
    }
}

fn legacy_retention_state(
    legacy_status: i32,
    first_read_at: Option<i64>,
    burn_at: Option<i64>,
    burned_at: Option<i64>,
) -> Option<MessageRetentionState> {
    let lifecycle = match legacy_status {
        LEGACY_BURN_STATUS_BURN_PENDING => MessageRetentionLifecycle::Scheduled,
        LEGACY_BURN_STATUS_BURNED => MessageRetentionLifecycle::Expired,
        LEGACY_BURN_STATUS_HARD_DELETED => MessageRetentionLifecycle::Purged,
        _ => return None,
    };
    let content_visibility = match lifecycle {
        MessageRetentionLifecycle::Purged => ContentVisibility::Purged,
        MessageRetentionLifecycle::Expired => ContentVisibility::Redacted,
        _ => ContentVisibility::Available,
    };
    Some(MessageRetentionState {
        lifecycle: lifecycle as i32,
        content_visibility: content_visibility as i32,
        first_triggered_at: first_read_at,
        expire_at: burn_at,
        expired_at: burned_at,
        purged_at: if lifecycle == MessageRetentionLifecycle::Purged {
            burned_at
        } else {
            None
        },
        triggered_by_user_id: None,
    })
}

fn retention_mode(policy: &MessageRetentionPolicy) -> RetentionMode {
    RetentionMode::try_from(policy.mode).unwrap_or(RetentionMode::Unspecified)
}

fn retention_lifecycle(state: &MessageRetentionState) -> MessageRetentionLifecycle {
    MessageRetentionLifecycle::try_from(state.lifecycle)
        .unwrap_or(MessageRetentionLifecycle::Unspecified)
}

fn retention_enabled(
    policy: Option<&MessageRetentionPolicy>,
    state: Option<&MessageRetentionState>,
) -> bool {
    let policy_enabled = policy
        .map(|policy| {
            !matches!(
                retention_mode(policy),
                RetentionMode::Unspecified | RetentionMode::None
            )
        })
        .unwrap_or(false);
    policy_enabled || state.is_some()
}

fn legacy_burn_status(state: Option<&MessageRetentionState>) -> i32 {
    match state.map(retention_lifecycle) {
        Some(MessageRetentionLifecycle::Scheduled) => LEGACY_BURN_STATUS_BURN_PENDING,
        Some(MessageRetentionLifecycle::Expired) => LEGACY_BURN_STATUS_BURNED,
        Some(MessageRetentionLifecycle::Purged) => LEGACY_BURN_STATUS_HARD_DELETED,
        Some(MessageRetentionLifecycle::Active) => LEGACY_BURN_STATUS_READ,
        _ => LEGACY_BURN_STATUS_INIT,
    }
}

fn legacy_retention_columns(
    message: &Message,
) -> (
    bool,
    Option<i64>,
    i32,
    Option<i64>,
    Option<i64>,
    Option<i64>,
) {
    let policy = message.retention_policy.as_ref();
    let state = message.retention_state.as_ref();
    let first_triggered_at = state.and_then(|state| state.first_triggered_at);
    let expire_at = state
        .and_then(|state| state.expire_at)
        .or_else(|| policy.and_then(|policy| policy.expire_at));
    let expired_at = state.and_then(|state| state.expired_at.or(state.purged_at));
    let expire_after_seconds = policy
        .and_then(|policy| policy.expire_after_seconds)
        .filter(|seconds| *seconds > 0);

    (
        retention_enabled(policy, state),
        expire_after_seconds,
        legacy_burn_status(state),
        first_triggered_at,
        expire_at,
        expired_at,
    )
}

fn message_content_bytes(content: Option<&MessageContent>) -> Option<Vec<u8>> {
    content.map(|content| content.encode_to_vec())
}

fn offline_push_info_to_json(
    opt: Option<&flare_proto::common::OfflinePushInfo>,
) -> serde_json::Value {
    let o = match opt {
        Some(o) => o,
        None => return serde_json::json!({}),
    };
    serde_json::json!({
        "title": o.title,
        "body": o.body,
        "sound": o.sound,
        "badge": o.badge,
        "payload": o.payload,
    })
}

fn extensions_to_json(map: &std::collections::HashMap<String, Vec<u8>>) -> serde_json::Value {
    use serde_json::Value as J;
    let o: serde_json::Map<String, J> = map
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                J::String(base64::engine::general_purpose::STANDARD.encode(v)),
            )
        })
        .collect();
    J::Object(o)
}

fn resolve_tenant_id(
    ctx: &Ctx,
    message_id: &str,
    extra: &serde_json::Map<String, serde_json::Value>,
) -> Result<String> {
    if let Some(tenant_id) = ctx
        .tenant_id()
        .filter(|tenant_id| !tenant_id.trim().is_empty())
    {
        return Ok(normalize_tenant_id(tenant_id));
    }

    extra
        .get("tenant_id")
        .and_then(|value| value.as_str())
        .filter(|tenant_id| !tenant_id.trim().is_empty())
        .map(normalize_tenant_id)
        .ok_or_else(|| {
            flare_server_core::error::FlareError::system(format!(
                "tenant_id is required for message {}",
                message_id
            ))
        })
}

/// 发送幂等账本里一条消息要认领的键与它的结果。
struct LedgerClaim {
    tenant_id: String,
    sender_id: String,
    client_msg_id: String,
    conversation_id: String,
    server_id: String,
    seq: i64,
}

impl LedgerClaim {
    /// 没有 `client_msg_id` 的消息无从按发送去重，不入账。
    fn of(
        tenant_id: &str,
        sender_id: &str,
        client_msg_id: Option<&str>,
        conversation_id: &str,
        server_id: &str,
        seq: i64,
    ) -> Option<Self> {
        let client_msg_id = client_msg_id.filter(|id| !id.trim().is_empty())?;
        Some(Self {
            tenant_id: tenant_id.to_string(),
            sender_id: sender_id.to_string(),
            client_msg_id: client_msg_id.to_string(),
            conversation_id: conversation_id.to_string(),
            server_id: server_id.to_string(),
            seq,
        })
    }
}

/// 账本行写入时消息已落库：重发拿回的结果按已持久化回复。
const LEDGER_DURABILITY: i16 = flare_proto::common::SendAckDurability::Persisted as i16;

/// 在写消息的事务里认领发送幂等账本键，返回账本的结论。
///
/// 先 `INSERT … ON CONFLICT DO NOTHING` 认领；没认领到的键另起一条语句读出占用者（READ COMMITTED 下，
/// 冲突等待期间别的事务提交的行要到新语句才看得见；同一批里先认领的行本事务也看得见）：
/// - 占用者就是这条消息（重投；或写入台账里已没有它而账本还在）：照常写入；
/// - 占用者在另一会话：重发不会换会话，这是 `client_msg_id` 被复用，照常写入，账本仍指向先到的那条；
/// - 占用者是同一会话的另一条消息：同一次发送的第二条，不写入。
async fn claim_send_idempotency(
    conn: &mut sqlx::PgConnection,
    claims: &[LedgerClaim],
) -> Result<crate::domain::repository::ArchiveOutcome> {
    use crate::domain::repository::{ArchiveOutcome, LedgerDuplicate};
    use std::collections::{HashMap, HashSet};

    let mut outcome = ArchiveOutcome::default();
    if claims.is_empty() {
        return Ok(outcome);
    }
    let column = |pick: fn(&LedgerClaim) -> &String| -> Vec<String> {
        claims.iter().map(|claim| pick(claim).clone()).collect()
    };
    let tenant_ids = column(|claim| &claim.tenant_id);
    let sender_ids = column(|claim| &claim.sender_id);
    let client_msg_ids = column(|claim| &claim.client_msg_id);
    let seqs: Vec<i64> = claims.iter().map(|claim| claim.seq).collect();

    let claimed: Vec<(String,)> = sqlx::query_as(
        r#"
        INSERT INTO message_idempotency_ledger (
            tenant_id, sender_id, client_msg_id, conversation_id, server_id,
            conversation_seq, durability, created_at
        )
        SELECT tenant_id, sender_id, client_msg_id, conversation_id, server_id,
               conversation_seq, $7, CURRENT_TIMESTAMP
          FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::bigint[])
               WITH ORDINALITY AS claim(
                   tenant_id, sender_id, client_msg_id, conversation_id, server_id,
                   conversation_seq, position
               )
         ORDER BY position
        ON CONFLICT (tenant_id, sender_id, client_msg_id) DO NOTHING
        RETURNING server_id
        "#,
    )
    .bind(&tenant_ids)
    .bind(&sender_ids)
    .bind(&client_msg_ids)
    .bind(column(|claim| &claim.conversation_id))
    .bind(column(|claim| &claim.server_id))
    .bind(&seqs)
    .bind(LEDGER_DURABILITY)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        flare_server_core::error::FlareError::system(format!(
            "claim message idempotency ledger failed batch_size={}: {err}",
            claims.len()
        ))
    })?;
    let claimed: HashSet<String> = claimed.into_iter().map(|(server_id,)| server_id).collect();
    if claimed.len() == claims.len() {
        return Ok(outcome);
    }

    let unclaimed: Vec<&LedgerClaim> = claims
        .iter()
        .filter(|claim| !claimed.contains(&claim.server_id))
        .collect();
    let occupants: Vec<(String, String, String, String, String)> = sqlx::query_as(
        r#"
        SELECT ledger.tenant_id, ledger.sender_id, ledger.client_msg_id,
               ledger.conversation_id, ledger.server_id
          FROM message_idempotency_ledger AS ledger
          JOIN UNNEST($1::text[], $2::text[], $3::text[]) AS claim(tenant_id, sender_id, client_msg_id)
            ON ledger.tenant_id = claim.tenant_id
           AND ledger.sender_id = claim.sender_id
           AND ledger.client_msg_id = claim.client_msg_id
        "#,
    )
    .bind(
        unclaimed
            .iter()
            .map(|claim| claim.tenant_id.clone())
            .collect::<Vec<_>>(),
    )
    .bind(
        unclaimed
            .iter()
            .map(|claim| claim.sender_id.clone())
            .collect::<Vec<_>>(),
    )
    .bind(
        unclaimed
            .iter()
            .map(|claim| claim.client_msg_id.clone())
            .collect::<Vec<_>>(),
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        flare_server_core::error::FlareError::system(format!(
            "read message idempotency ledger occupants failed: {err}"
        ))
    })?;
    let occupants: HashMap<(String, String, String), (String, String)> = occupants
        .into_iter()
        .map(
            |(tenant_id, sender_id, client_msg_id, conversation_id, server_id)| {
                (
                    (tenant_id, sender_id, client_msg_id),
                    (conversation_id, server_id),
                )
            },
        )
        .collect();

    for claim in unclaimed {
        let key = (
            claim.tenant_id.clone(),
            claim.sender_id.clone(),
            claim.client_msg_id.clone(),
        );
        // 读不到占用者：认领与读取之间已被保留期清理删掉，按新消息写入。
        let Some((conversation_id, server_id)) = occupants.get(&key) else {
            continue;
        };
        if *server_id == claim.server_id {
            continue;
        }
        if *conversation_id != claim.conversation_id {
            outcome.reused_client_msg_ids += 1;
            continue;
        }
        outcome.duplicates.push(LedgerDuplicate {
            server_id: claim.server_id.clone(),
            original_server_id: server_id.clone(),
        });
    }
    Ok(outcome)
}

/// 删一批过期账本行：先锁定至多 `$2` 行再按主键删，多个 Writer 实例同时清理时互相跳过已锁的行。
const DELETE_EXPIRED_LEDGER_SQL: &str = r#"
WITH batch AS MATERIALIZED (
    SELECT tenant_id, sender_id, client_msg_id
      FROM message_idempotency_ledger
     WHERE created_at < $1
     ORDER BY created_at
     LIMIT $2
       FOR UPDATE SKIP LOCKED
)
DELETE FROM message_idempotency_ledger AS ledger
 USING batch
 WHERE ledger.tenant_id = batch.tenant_id
   AND ledger.sender_id = batch.sender_id
   AND ledger.client_msg_id = batch.client_msg_id
"#;

impl crate::domain::repository::IdempotencyLedgerRetention for PostgresMessageStore {
    async fn delete_created_before(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
        limit: u32,
    ) -> Result<u64> {
        let result = sqlx::query(DELETE_EXPIRED_LEDGER_SQL)
            .bind(cutoff)
            .bind(i64::from(limit.max(1)))
            .execute(&self.pool)
            .await
            .map_err(|err| {
                flare_server_core::error::FlareError::system(format!(
                    "delete expired message idempotency ledger rows failed: {err}"
                ))
            })?;
        Ok(result.rows_affected())
    }
}

impl PostgresMessageStore {
    /// 单条归档：认领发送幂等账本 → 写入台账 → 消息行，同一事务。判为重复的消息什么也不写。
    #[instrument(skip(self, ctx, message), fields(message_id = %message.server_id, conversation_id = %message.conversation_id))]
    async fn archive_one(
        &self,
        ctx: &Ctx,
        message: &Message,
    ) -> Result<crate::domain::repository::ArchiveOutcome> {
        use crate::infrastructure::persistence::helpers::*;

        let proto_msg = convert::message_to_proto(message);
        let created_at_dt = get_message_timestamp(&proto_msg);
        let content_bytes = message_content_bytes(proto_msg.content.as_ref());
        let extra_value = build_extra_value(&message.extra)?;
        let seq = i64::try_from(proto_msg.conversation_seq).map_err(|_| {
            flare_server_core::error::FlareError::system(format!(
                "invalid seq overflow for message {}",
                proto_msg.server_id
            ))
        })?;
        if seq <= 0 {
            return Err(flare_server_core::error::FlareError::system(format!(
                "invalid seq={}, message must be assigned in orchestrator: {}",
                seq, proto_msg.server_id
            )));
        }

        let tenant_id = resolve_tenant_id(ctx, &proto_msg.server_id, &extra_value)?;
        let (burn_enabled, burn_after_read_seconds, burn_status, first_read_at, burn_at, burned_at) =
            legacy_retention_columns(message);

        let mut tx = self.pool.begin().await?;
        let claims: Vec<LedgerClaim> = LedgerClaim::of(
            &tenant_id,
            &proto_msg.sender_id,
            Some(&proto_msg.client_msg_id),
            &proto_msg.conversation_id,
            &proto_msg.server_id,
            seq,
        )
        .into_iter()
        .collect();
        let outcome = claim_send_idempotency(&mut tx, &claims).await?;
        if outcome.is_duplicate(&proto_msg.server_id) {
            // 什么也没写：先到的那条已经入账。
            tx.commit().await?;
            return Ok(outcome);
        }

        let ledger_result = sqlx::query(
            r#"
            INSERT INTO message_write_ledger (
                tenant_id, server_id, conversation_id, seq
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (tenant_id, server_id) DO NOTHING
            "#,
        )
        .bind(&tenant_id)
        .bind(&proto_msg.server_id)
        .bind(&proto_msg.conversation_id)
        .bind(seq)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            tracing::error!(
                error = %err,
                message_id = %proto_msg.server_id,
                conversation_id = %proto_msg.conversation_id,
                seq,
                "insert message write ledger failed"
            );
            flare_server_core::error::FlareError::system(format!(
                "insert message write ledger failed message_id={} conversation_id={} seq={}: {err}",
                proto_msg.server_id, proto_msg.conversation_id, seq
            ))
        })?;

        if ledger_result.rows_affected() == 0 {
            tx.commit().await?;
            tracing::trace!(
                tenant_id = %tenant_id,
                message_id = %proto_msg.server_id,
                conversation_id = %proto_msg.conversation_id,
                seq = seq,
                "Message archive skipped by durable write ledger"
            );
            return Ok(outcome);
        }

        sqlx::query(
            r#"
            INSERT INTO messages (
                tenant_id, server_id, conversation_id, client_msg_id, sender_id, sender_name, sender_avatar,
                channel_id, thread_id, source, seq, timestamp, conversation_type, message_type, content,
                status, burn_enabled, burn_after_read_seconds, burn_status, first_read_at, burn_at,
                burned_at, offline_push_info, extra, extensions, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)
            ON CONFLICT (tenant_id, server_id, created_at) DO NOTHING
            "#,
        )
        .bind(&tenant_id)
        .bind(&proto_msg.server_id)
        .bind(&proto_msg.conversation_id)
        .bind(if proto_msg.client_msg_id.is_empty() { None::<String> } else { Some(proto_msg.client_msg_id.clone()) })
        .bind(&proto_msg.sender_id)
        .bind(if message.sender_name.is_empty() { None::<String> } else { Some(message.sender_name.clone()) })
        .bind(if message.sender_avatar.is_empty() { None::<String> } else { Some(message.sender_avatar.clone()) })
        .bind(if proto_msg.channel_id.is_empty() { None::<String> } else { Some(proto_msg.channel_id.clone()) })
        .bind(proto_msg.thread_id.clone().filter(|thread_id| !thread_id.is_empty()))
        .bind(proto_msg.source)
        .bind(seq)
        .bind(created_at_dt)
        .bind(proto_msg.conversation_type)
        .bind(proto_msg.message_type)
        .bind(content_bytes)
        .bind(proto_msg.status)
        .bind(burn_enabled)
        .bind(burn_after_read_seconds)
        .bind(burn_status)
        .bind(first_read_at)
        .bind(burn_at)
        .bind(burned_at)
        .bind(offline_push_info_to_json(proto_msg.offline_push_info.as_ref()))
        .bind(to_value(&extra_value)?)
        .bind(extensions_to_json(&proto_msg.extensions))
        .bind(created_at_dt)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            tracing::error!(
                error = %err,
                message_id = %proto_msg.server_id,
                conversation_id = %proto_msg.conversation_id,
                seq,
                "insert message archive failed"
            );
            flare_server_core::error::FlareError::system(format!(
                "insert message archive failed message_id={} conversation_id={} seq={}: {err}",
                proto_msg.server_id,
                proto_msg.conversation_id,
                seq
            ))
        })?;

        sqlx::query(
            r#"
            UPDATE message_write_ledger
            SET write_state = $3,
                archive_persisted_at = COALESCE(archive_persisted_at, CURRENT_TIMESTAMP),
                last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE tenant_id = $1
              AND server_id = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(&proto_msg.server_id)
        .bind(MessageWriteStage::ArchivePersisted.as_str())
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            tracing::error!(
                error = %err,
                message_id = %proto_msg.server_id,
                "update message write ledger archive stage failed"
            );
            flare_server_core::error::FlareError::system(format!(
                "update message write ledger archive stage failed message_id={}: {err}",
                proto_msg.server_id
            ))
        })?;

        if let Some(proof) =
            ctx.get_data::<flare_im_service_kit::audit_effects::write_proof::VerifiedWrite>()
        {
            proof
                .record(
                    &mut tx,
                    &tenant_id,
                    &proto_msg.server_id,
                    &proto_msg.conversation_id,
                )
                .await
                .map_err(|_| {
                    flare_server_core::error::FlareError::system("machine commit audit unavailable")
                })?;
        }
        tx.commit().await?;
        Ok(outcome)
    }
}

impl ArchiveStoreRepository for PostgresMessageStore {
    async fn store_archive(&self, ctx: &Ctx, message: &Message) -> Result<()> {
        self.archive_one(ctx, message).await.map(|_| ())
    }

    #[instrument(skip(self), fields(tenant_id, message_id, fsm_state))]
    async fn update_message_fsm_state(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        fsm_state: &str,
        recall_reason: Option<&str>,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        self.operation_store
            .update_message_fsm_state(tenant_id, message_id, fsm_state, recall_reason)
            .await
    }

    #[instrument(skip(self), fields(tenant_id, message_id, editor_id))]
    async fn update_message_content(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        new_content: &[u8],
        editor_id: &str,
        reason: Option<&str>,
        content_text_for_extra: Option<&str>,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        self.operation_store
            .update_message_content(
                tenant_id,
                message_id,
                new_content,
                editor_id,
                reason,
                content_text_for_extra,
            )
            .await
    }

    #[instrument(skip(self), fields(tenant_id, message_id, user_id, visibility_status))]
    async fn update_message_visibility(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        user_id: &str,
        scope: i32,
        visibility_status: &str,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        self.operation_store
            .update_message_visibility(tenant_id, message_id, user_id, scope, visibility_status)
            .await
    }

    #[instrument(skip(self), fields(tenant_id, message_id, user_id))]
    async fn record_message_read(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        user_id: &str,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        self.operation_store
            .record_message_read(tenant_id, message_id, user_id)
            .await
    }

    #[instrument(skip(self), fields(tenant_id, message_id, reader_id = ?reader_id, burn_at))]
    async fn schedule_message_burn(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        reader_id: Option<&str>,
        first_read_at: i64,
        burn_at: i64,
    ) -> Result<bool> {
        let _ = ctx;
        let result = sqlx::query(
            r#"
            UPDATE messages
            SET first_read_at = COALESCE(first_read_at, $3),
                burn_at = COALESCE(burn_at, $4),
                burn_status = $5
            WHERE tenant_id = $1
              AND server_id = $2
              AND burn_enabled = TRUE
              AND burn_status IN ($6, $7)
              AND burned_at IS NULL
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .bind(first_read_at)
        .bind(burn_at)
        .bind(LEGACY_BURN_STATUS_BURN_PENDING)
        .bind(LEGACY_BURN_STATUS_INIT)
        .bind(LEGACY_BURN_STATUS_READ)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() > 0 {
            if let Some(reader_id) = reader_id {
                let read_at = chrono::DateTime::from_timestamp(first_read_at, 0)
                    .unwrap_or_else(chrono::Utc::now);
                // 主写（标记已读）已经成功，这条是派生的读记录：失败不该把整个操作回滚成
                // 「未读」，但也绝不能静默——否则「阅后即焚」到期时会因为查不到读记录而
                // 不焚，且没有任何线索。失败可见，语义不变。
                if let Err(err) = sqlx::query(
                    r#"
                    INSERT INTO message_read_records (tenant_id, message_id, user_id, read_at)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (tenant_id, message_id, user_id)
                    DO UPDATE SET read_at = LEAST(message_read_records.read_at, EXCLUDED.read_at)
                    "#,
                )
                .bind(tenant_id)
                .bind(message_id)
                .bind(reader_id)
                .bind(read_at)
                .execute(&self.pool)
                .await
                {
                    warn!(
                        %tenant_id, %message_id, %reader_id, error = %err,
                        "写 message_read_records 失败：已读标记已生效，但读记录缺失，\
                         阅后即焚的到期判定会因此看不到这次阅读"
                    );
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[instrument(skip(self), fields(tenant_id, message_id, reader_id = ?reader_id))]
    async fn schedule_message_burn_after_read(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        reader_id: Option<&str>,
        first_read_at: i64,
    ) -> Result<Option<i64>> {
        let _ = ctx;
        let row = sqlx::query(
            r#"
            UPDATE messages
            SET first_read_at = COALESCE(first_read_at, $3),
                burn_at = COALESCE(
                    burn_at,
                    $3 + GREATEST(COALESCE(burn_after_read_seconds, 0), 1)
                ),
                burn_status = $4
            WHERE tenant_id = $1
              AND server_id = $2
              AND burn_enabled = TRUE
              AND COALESCE(burn_after_read_seconds, 0) > 0
              AND burn_status IN ($5, $6)
              AND burned_at IS NULL
            RETURNING burn_at
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .bind(first_read_at)
        .bind(LEGACY_BURN_STATUS_BURN_PENDING)
        .bind(LEGACY_BURN_STATUS_INIT)
        .bind(LEGACY_BURN_STATUS_READ)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        let burn_at = row.try_get::<i64, _>("burn_at").unwrap_or(first_read_at);
        if let Some(reader_id) = reader_id {
            let read_at =
                chrono::DateTime::from_timestamp(first_read_at, 0).unwrap_or_else(chrono::Utc::now);
            let _ = sqlx::query(
                r#"
                INSERT INTO message_read_records (tenant_id, message_id, user_id, read_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (tenant_id, message_id, user_id)
                DO UPDATE SET read_at = LEAST(message_read_records.read_at, EXCLUDED.read_at)
                "#,
            )
            .bind(tenant_id)
            .bind(message_id)
            .bind(reader_id)
            .bind(read_at)
            .execute(&self.pool)
            .await;
        }
        Ok(Some(burn_at))
    }

    #[instrument(skip(self), fields(tenant_id, message_id, burned_at))]
    async fn mark_message_burned(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        burned_at: i64,
    ) -> Result<bool> {
        let _ = ctx;
        let result = sqlx::query(
            r#"
            UPDATE messages
            SET burn_status = $3,
                burned_at = COALESCE(burned_at, $4),
                content = '\x'::bytea,
                offline_push_info = NULL,
                extensions = '{}'::jsonb
            WHERE tenant_id = $1
              AND server_id = $2
              AND burn_enabled = TRUE
              AND burn_status = $5
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .bind(LEGACY_BURN_STATUS_BURNED)
        .bind(burned_at)
        .bind(LEGACY_BURN_STATUS_BURN_PENDING)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() > 0 {
            let burned_at_dt =
                chrono::DateTime::from_timestamp(burned_at, 0).unwrap_or_else(chrono::Utc::now);
            // 消息本体已标记焚毁（上面那条带 ?），这里是回填读记录上的焚毁时间。
            // 失败不改变「已焚毁」这个事实，但静默会让读记录与消息状态长期不一致，
            // 而这是隐私功能——不一致必须留下痕迹。
            if let Err(err) = sqlx::query(
                r#"
                UPDATE message_read_records
                SET burned_at = COALESCE(burned_at, $3)
                WHERE tenant_id = $1 AND message_id = $2
                "#,
            )
            .bind(tenant_id)
            .bind(message_id)
            .bind(burned_at_dt)
            .execute(&self.pool)
            .await
            {
                warn!(
                    %tenant_id, %message_id, error = %err,
                    "回填 message_read_records.burned_at 失败：消息已焚毁，读记录仍显示未焚"
                );
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[instrument(skip(self), fields(tenant_id, message_id, hard_deleted_at))]
    async fn mark_message_hard_deleted(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        hard_deleted_at: i64,
    ) -> Result<bool> {
        let _ = (ctx, hard_deleted_at);
        let result = sqlx::query(
            r#"
            UPDATE messages
            SET burn_status = $3,
                content = '\x'::bytea,
                offline_push_info = NULL,
                extensions = '{}'::jsonb
            WHERE tenant_id = $1
              AND server_id = $2
              AND burn_enabled = TRUE
              AND burn_status IN ($4, $5)
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .bind(LEGACY_BURN_STATUS_HARD_DELETED)
        .bind(LEGACY_BURN_STATUS_BURNED)
        .bind(LEGACY_BURN_STATUS_BURN_PENDING)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    #[instrument(skip(self), fields(tenant_id, message_id, emoji, user_id))]
    async fn upsert_message_reaction(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        emoji: &str,
        user_id: &str,
        add: bool,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        self.operation_store
            .upsert_message_reaction(tenant_id, message_id, emoji, user_id, add)
            .await
    }

    #[instrument(skip(self, state), fields(tenant_id, conversation_id, message_id = %state.server_msg_id, version = state.version))]
    async fn upsert_message_plugin_state(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        conversation_id: &str,
        state: &crate::domain::model::PluginStatePayload,
    ) -> Result<crate::domain::repository::PluginStateWrite> {
        let _ = ctx; // 上下文用于日志追踪
        self.operation_store
            .upsert_message_plugin_state(tenant_id, conversation_id, state)
            .await
    }

    #[instrument(
        skip(self),
        fields(tenant_id, message_id, conversation_id, user_id, pin)
    )]
    async fn pin_message(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        conversation_id: &str,
        user_id: &str,
        scope: i32,
        pin: bool,
        expire_at: Option<chrono::DateTime<chrono::Utc>>,
        reason: Option<&str>,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        self.operation_store
            .pin_message(
                tenant_id,
                message_id,
                conversation_id,
                user_id,
                scope,
                pin,
                expire_at,
                reason,
            )
            .await
    }

    #[instrument(
        skip(self),
        fields(tenant_id, message_id, conversation_id, user_id, mark_type)
    )]
    async fn mark_message(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        conversation_id: &str,
        user_id: &str,
        mark_type: &str,
        color: Option<&str>,
        add: bool,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        self.operation_store
            .mark_message(
                tenant_id,
                message_id,
                conversation_id,
                user_id,
                mark_type,
                color,
                add,
            )
            .await
    }

    #[instrument(skip(self, event), fields(tenant_id, message_id, operator_id = %event.operator_id))]
    async fn append_event(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        event: &Event,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        let proto_event = convert::event_to_proto(event);
        if tenant_id.is_empty() {
            return Err(flare_server_core::error::FlareError::system(
                "tenant_id is required".to_string(),
            ));
        }
        self.operation_store
            .append_event(
                tenant_id,
                message_id,
                &proto_event,
                event.operator_id.as_str(),
            )
            .await
    }

    #[instrument(skip(self), fields(message_id))]
    async fn get_message(&self, ctx: &Ctx, message_id: &str) -> Result<Option<Message>> {
        let _ = ctx; // 上下文用于日志追踪
        // 调用内部辅助方法
        self.get_message_by_id(message_id).await
    }

    /// 批量存储消息（优化性能）
    ///
    /// 使用 TimescaleDB 优化的批量插入策略：
    /// - 小批量（<=10）：逐个插入（简单可靠）
    /// - 中批量（11-500）：使用 VALUES 多行插入（单事务，性能较好）
    /// - 大批量（>500）：分批处理，每批最多 500 条（避免单次事务过大）
    ///
    /// 批量大小自适应：
    /// - 根据消息大小动态调整批量大小
    /// - 避免单次事务过大导致超时或内存问题
    async fn store_archive_batch(&self, ctx: &Ctx, messages: &[Message]) -> Result<()> {
        self.archive_messages(ctx, messages).await.map(|_| ())
    }

    /// 每个事务都先认领发送幂等账本，判为重复的消息不写入，同批其余消息照常写入；各事务的结论合并返回。
    #[instrument(skip(self, ctx, messages), fields(batch_size = messages.len()))]
    async fn archive_messages(
        &self,
        ctx: &Ctx,
        messages: &[Message],
    ) -> Result<crate::domain::repository::ArchiveOutcome> {
        let mut outcome = crate::domain::repository::ArchiveOutcome::default();
        if messages.is_empty() {
            return Ok(outcome);
        }

        // 小批量：逐个插入（简单且可靠）
        if messages.len() <= 10 {
            for message in messages {
                outcome.merge(self.archive_one(ctx, message).await?);
            }
            return Ok(outcome);
        }

        // 计算平均消息大小（用于自适应批量大小）
        let avg_message_size = messages
            .iter()
            .map(|m| {
                let content_size = m.content.as_ref().map(|c| c.encoded_len()).unwrap_or(0);
                let extra_size = serde_json::to_string(&m.extra).unwrap_or_default().len();
                content_size + extra_size + 200
            })
            .sum::<usize>()
            / messages.len();

        // 自适应批量大小：
        // - 小消息（<1KB）：每批最多 500 条
        // - 中等消息（1-10KB）：每批最多 200 条
        // - 大消息（>10KB）：每批最多 50 条
        let optimal_batch_size = if avg_message_size < 1024 {
            500
        } else if avg_message_size < 10 * 1024 {
            200
        } else {
            50
        };

        // 如果消息数量超过最优批量大小，分批处理
        if messages.len() > optimal_batch_size {
            let chunks: Vec<_> = messages.chunks(optimal_batch_size).collect();
            tracing::trace!(
                total_messages = messages.len(),
                optimal_batch_size = optimal_batch_size,
                chunks = chunks.len(),
                avg_message_size = avg_message_size,
                "Splitting batch into {} chunks for optimal performance",
                chunks.len()
            );

            for chunk in chunks {
                outcome.merge(self.store_archive_batch_values(ctx, chunk).await?);
            }
            return Ok(outcome);
        }

        // 中批量：使用 VALUES 多行插入（单事务，性能较好）
        self.store_archive_batch_values(ctx, messages).await
    }
}

impl PostgresMessageStore {
    /// 使用 VALUES 多行插入进行批量存储（优化版本）
    ///
    /// 此方法使用 sqlx::QueryBuilder 构建批量 INSERT 语句，
    /// 利用 TimescaleDB 的批量插入优化，性能比循环插入提升 10-100 倍
    ///
    /// 错误处理和重试：
    /// - 事务失败时自动重试（最多 3 次）
    /// - 使用指数退避策略
    ///
    /// 每次尝试都在同一事务里先认领发送幂等账本，判为重复的行不进写入台账、也不写消息行。
    async fn store_archive_batch_values(
        &self,
        ctx: &Ctx,
        messages: &[Message],
    ) -> Result<crate::domain::repository::ArchiveOutcome> {
        use sqlx::QueryBuilder;
        use std::collections::HashSet;
        use std::time::Duration;

        use crate::infrastructure::persistence::helpers::*;

        let prepared_data: Vec<_> = messages
            .iter()
            .map(|message| -> Result<_> {
                let proto_msg = convert::message_to_proto(message);
                let created_at_dt = get_message_timestamp(&proto_msg);
                let extra_value = build_extra_value(&message.extra)?;
                let seq = i64::try_from(proto_msg.conversation_seq).map_err(|_| {
                    flare_server_core::error::FlareError::system(format!(
                        "invalid seq overflow for message {}",
                        proto_msg.server_id
                    ))
                })?;
                if seq <= 0 {
                    return Err(flare_server_core::error::FlareError::system(format!(
                        "invalid seq={}, message must be assigned in orchestrator: {}",
                        seq, proto_msg.server_id
                    )));
                }
                let tenant_id = resolve_tenant_id(ctx, &proto_msg.server_id, &extra_value)?;
                let (
                    burn_enabled,
                    burn_after_read_seconds,
                    burn_status,
                    first_read_at,
                    burn_at,
                    burned_at,
                ) = legacy_retention_columns(message);

                Ok((
                    tenant_id,
                    proto_msg.server_id.clone(),
                    proto_msg.conversation_id.clone(),
                    if proto_msg.client_msg_id.is_empty() {
                        None
                    } else {
                        Some(proto_msg.client_msg_id.clone())
                    },
                    proto_msg.sender_id.clone(),
                    if message.sender_name.is_empty() {
                        None
                    } else {
                        Some(message.sender_name.clone())
                    },
                    if message.sender_avatar.is_empty() {
                        None
                    } else {
                        Some(message.sender_avatar.clone())
                    },
                    if proto_msg.channel_id.is_empty() {
                        None
                    } else {
                        Some(proto_msg.channel_id.clone())
                    },
                    proto_msg
                        .thread_id
                        .clone()
                        .filter(|thread_id| !thread_id.is_empty()),
                    proto_msg.source,
                    seq,
                    created_at_dt,
                    proto_msg.conversation_type,
                    proto_msg.message_type,
                    message_content_bytes(proto_msg.content.as_ref()),
                    proto_msg.status,
                    burn_enabled,
                    burn_after_read_seconds,
                    burn_status,
                    first_read_at,
                    burn_at,
                    burned_at,
                    offline_push_info_to_json(proto_msg.offline_push_info.as_ref()),
                    to_value(&extra_value).unwrap_or_default(),
                    extensions_to_json(&proto_msg.extensions),
                    created_at_dt,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let claims: Vec<LedgerClaim> = prepared_data
            .iter()
            .filter_map(|row| {
                LedgerClaim::of(&row.0, &row.4, row.3.as_deref(), &row.2, &row.1, row.10)
            })
            .collect();

        // 重试机制（最多 3 次）
        let max_retries = 3;
        let mut last_error: Option<flare_server_core::error::FlareError> = None;

        for attempt in 0..max_retries {
            // 使用事务确保原子性
            let mut tx = match self.pool.begin().await {
                Ok(tx) => tx,
                Err(e) => {
                    last_error = Some(flare_server_core::error::FlareError::from(e));
                    if attempt < max_retries - 1 {
                        // 指数退避：1s, 2s, 4s
                        let backoff = Duration::from_millis(1000 * (1 << attempt));
                        tracing::warn!(
                            attempt = attempt + 1,
                            backoff_ms = backoff.as_millis(),
                            "Failed to begin transaction, retrying after backoff"
                        );
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    return Err(last_error.unwrap_or_else(|| {
                        flare_server_core::error::FlareError::system(
                            "Failed to begin transaction after retries".to_string(),
                        )
                    }));
                }
            };

            let outcome = match claim_send_idempotency(&mut tx, &claims).await {
                Ok(outcome) => outcome,
                Err(e) => {
                    last_error = Some(e);
                    let _ = tx.rollback().await;
                    if attempt < max_retries - 1 {
                        let backoff = Duration::from_millis(1000 * (1 << attempt));
                        tracing::warn!(
                            attempt = attempt + 1,
                            backoff_ms = backoff.as_millis(),
                            "Failed to claim message idempotency ledger batch, retrying after backoff"
                        );
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    return Err(last_error.unwrap_or_else(|| {
                        flare_server_core::error::FlareError::system(
                            "Failed to claim message idempotency ledger batch after retries"
                                .to_string(),
                        )
                    }));
                }
            };
            // 判为重复的行整行跳过；同批其余行照常写入。
            let candidates: Vec<_> = prepared_data
                .iter()
                .filter(|row| !outcome.is_duplicate(&row.1))
                .collect();
            if candidates.is_empty() {
                tx.commit().await?;
                return Ok(outcome);
            }

            let mut ledger_builder: QueryBuilder<Postgres> = QueryBuilder::new(
                r#"
                INSERT INTO message_write_ledger (
                    tenant_id, server_id, conversation_id, seq
                )
                "#,
            );
            ledger_builder.push_values(&candidates, |mut b, row| {
                let row = *row;
                b.push_bind(&row.0); // tenant_id
                b.push_bind(&row.1); // server_id
                b.push_bind(&row.2); // conversation_id
                b.push_bind(row.10); // seq
            });
            ledger_builder.push(
                " ON CONFLICT (tenant_id, server_id) DO NOTHING RETURNING tenant_id, server_id",
            );

            let inserted_ledger_rows: Vec<(String, String)> = match ledger_builder
                .build_query_as()
                .fetch_all(&mut *tx)
                .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    last_error = Some(flare_server_core::error::FlareError::from(e));
                    let _ = tx.rollback().await;
                    if attempt < max_retries - 1 {
                        let backoff = Duration::from_millis(1000 * (1 << attempt));
                        tracing::warn!(
                            attempt = attempt + 1,
                            backoff_ms = backoff.as_millis(),
                            "Failed to insert message write ledger batch, retrying after backoff"
                        );
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    return Err(last_error.unwrap_or_else(|| {
                        flare_server_core::error::FlareError::system(
                            "Failed to insert message write ledger batch after retries".to_string(),
                        )
                    }));
                }
            };

            if inserted_ledger_rows.is_empty() {
                tx.commit().await?;
                tracing::trace!(
                    batch_size = messages.len(),
                    "All batch messages were skipped by durable write ledger"
                );
                return Ok(outcome);
            }

            let inserted_keys: HashSet<(String, String)> =
                inserted_ledger_rows.into_iter().collect();
            let rows_to_insert: Vec<_> = candidates
                .iter()
                .copied()
                .filter(|row| inserted_keys.contains(&(row.0.clone(), row.1.clone())))
                .collect();

            let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
                r#"
                INSERT INTO messages (
                    tenant_id, server_id, conversation_id, client_msg_id, sender_id, sender_name, sender_avatar,
                    channel_id, thread_id, source, seq, timestamp, conversation_type, message_type, content,
                    status, burn_enabled, burn_after_read_seconds, burn_status, first_read_at, burn_at,
                    burned_at, offline_push_info, extra, extensions, created_at
                )
                "#,
            );

            query_builder.push_values(&rows_to_insert, |mut b, row| {
                let row = *row;
                b.push_bind(&row.0); // tenant_id
                b.push_bind(&row.1); // server_id
                b.push_bind(&row.2); // conversation_id
                b.push_bind(&row.3); // client_msg_id
                b.push_bind(&row.4); // sender_id
                b.push_bind(&row.5); // sender_name
                b.push_bind(&row.6); // sender_avatar
                b.push_bind(&row.7); // channel_id
                b.push_bind(&row.8); // thread_id
                b.push_bind(row.9); // source
                b.push_bind(row.10); // seq
                b.push_bind(row.11); // timestamp
                b.push_bind(row.12); // conversation_type
                b.push_bind(row.13); // message_type
                b.push_bind(&row.14); // content
                b.push_bind(row.15); // status
                b.push_bind(row.16); // burn_enabled
                b.push_bind(row.17); // burn_after_read_seconds
                b.push_bind(row.18); // burn_status
                b.push_bind(row.19); // first_read_at
                b.push_bind(row.20); // burn_at
                b.push_bind(row.21); // burned_at
                b.push_bind(&row.22); // offline_push_info
                b.push_bind(&row.23); // extra
                b.push_bind(&row.24); // extensions
                b.push_bind(row.25); // created_at
            });

            query_builder.push(" ON CONFLICT (tenant_id, server_id, created_at) DO NOTHING");

            // 执行批量插入
            match query_builder.build().execute(&mut *tx).await {
                Ok(_) => {
                    for row in &rows_to_insert {
                        sqlx::query(
                            r#"
                            UPDATE message_write_ledger
                            SET write_state = $3,
                                archive_persisted_at = COALESCE(archive_persisted_at, CURRENT_TIMESTAMP),
                                last_error = NULL,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE tenant_id = $1
                              AND server_id = $2
                            "#,
                        )
                        .bind(&row.0)
                        .bind(&row.1)
                        .bind(MessageWriteStage::ArchivePersisted.as_str())
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| {
                            flare_server_core::error::FlareError::system(format!(
                                "update message write ledger archive stage failed message_id={} attempt={}: {e}",
                                row.1,
                                attempt + 1
                            ))
                        })?;
                    }

                    if let Some(proof)=ctx.get_data::<flare_im_service_kit::audit_effects::write_proof::VerifiedWrite>() {
                        for row in &rows_to_insert {proof.record(&mut tx,&row.0,&row.1,&row.2).await.map_err(|_|flare_server_core::error::FlareError::system("machine commit audit unavailable"))?;}
                    }
                    // 提交事务
                    match tx.commit().await {
                        Ok(_) => {
                            tracing::info!(
                                batch_size = rows_to_insert.len(),
                                attempt = attempt + 1,
                                "Successfully batch inserted {} messages into TimescaleDB using VALUES",
                                rows_to_insert.len()
                            );
                            return Ok(outcome);
                        }
                        Err(e) => {
                            last_error = Some(flare_server_core::error::FlareError::from(e));
                            if attempt < max_retries - 1 {
                                let backoff = Duration::from_millis(1000 * (1 << attempt));
                                tracing::warn!(
                                    attempt = attempt + 1,
                                    backoff_ms = backoff.as_millis(),
                                    "Failed to commit transaction, retrying after backoff"
                                );
                                tokio::time::sleep(backoff).await;
                                continue;
                            }
                        }
                    }
                }
                Err(e) => {
                    last_error = Some(flare_server_core::error::FlareError::system(format!(
                        "batch insert message archive failed batch_size={} attempt={}: {e}",
                        messages.len(),
                        attempt + 1
                    )));
                    // 回滚事务
                    let _ = tx.rollback().await;
                    if attempt < max_retries - 1 {
                        let backoff = Duration::from_millis(1000 * (1 << attempt));
                        tracing::warn!(
                            attempt = attempt + 1,
                            backoff_ms = backoff.as_millis(),
                            "Failed to execute batch insert, retrying after backoff"
                        );
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                }
            }
        }

        // 所有重试都失败
        Err(flare_server_core::error::FlareError::system(format!(
            "Failed to batch insert {} messages after {} attempts: {}",
            messages.len(),
            max_retries,
            last_error
                .map(|e| e.to_string())
                .unwrap_or_else(|| "Unknown error".to_string())
        )))
    }
}
