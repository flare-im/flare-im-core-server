//! 事件流写入（init_v2：event_type INT，payload BYTEA 单列）

use flare_im_contracts::Ctx;
use flare_proto::common::event::Payload;
use flare_server_core::error::{ErrorCode, FlareError, Result, map_infra_error};
use sqlx::{Pool, Postgres, QueryBuilder, Row};
use std::collections::{HashMap, HashSet};
use tracing::{debug, instrument, warn};

use crate::config::StorageWriterConfig;
use crate::domain::repository::{EventStreamRepository, StoredEventMatch};

fn encode_payload_bytes(payload: &Payload) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    match payload {
        Payload::Message(m) => encode_proto_payload(m, &mut buf, "message")?,
        Payload::Recall(r) => encode_proto_payload(r, &mut buf, "recall")?,
        Payload::Edit(e) => encode_proto_payload(e, &mut buf, "edit")?,
        Payload::Delete(d) => encode_proto_payload(d, &mut buf, "delete")?,
        Payload::Read(r) => encode_proto_payload(r, &mut buf, "read")?,
        Payload::Reaction(r) => encode_proto_payload(r, &mut buf, "reaction")?,
        Payload::Pin(p) => encode_proto_payload(p, &mut buf, "pin")?,
        Payload::Unpin(u) => encode_proto_payload(u, &mut buf, "unpin")?,
        Payload::Mark(m) => encode_proto_payload(m, &mut buf, "mark")?,
        Payload::Unmark(u) => encode_proto_payload(u, &mut buf, "unmark")?,
        Payload::RetentionScheduled(b) => encode_proto_payload(b, &mut buf, "retention_scheduled")?,
        Payload::RetentionExpired(b) => encode_proto_payload(b, &mut buf, "retention_expired")?,
        Payload::RetentionPurged(b) => encode_proto_payload(b, &mut buf, "retention_purged")?,
        Payload::PluginState(s) => encode_proto_payload(s, &mut buf, "plugin_state")?,
        _ => {}
    }
    Ok(buf)
}

fn encode_proto_payload<M: prost::Message>(
    value: &M,
    buf: &mut Vec<u8>,
    label: &str,
) -> Result<()> {
    value
        .encode(buf)
        .map_err(|err| FlareError::serialization_error(format!("encode {label} payload: {err}")))
}

fn db_error(operation: &str, err: sqlx::Error) -> FlareError {
    map_infra_error(err, ErrorCode::DatabaseError, operation)
}

/// 同一 `(tenant_id, conversation_id, seq)` 上已有一行时，它与本次要写的行的关系。
///
/// 除 [`ExistingEvent::Conflict`] 外都按重投接受（保留已有行，不覆盖）：
///
/// - 字节比较不够：prost 按 `HashMap` 的迭代序编码 map 字段（`Message.attributes` 等），同一事件
///   每次从 MQ 解出再编码，字节可能不同，重投会被当成冲突，重试几次后进 DLQ。内容一律按「解码后相同」
///   比较；字节哈希同样受编码顺序影响，不能当内容指纹。
/// - 升级窗口：`915b5c2` 之前的 Writer 以领域枚举的判别值落类型号（反应落成 11 等），同一事件由新旧
///   Writer 各处理一次时类型号不同、载荷相同。操作事件在应用前按同一口径判重（`match_stored_event`），
///   两次投递同时处理（如滚动升级时新旧实例并存）才会在写入时走到这里。只认这一种旧编码
///   （[`EventStreamInsertRow::legacy_event_type`]），别的类型不一致仍是冲突。已有的旧类型号不在这里
///   纠正（存量数据另行处理）。
/// - 消息事件：服务端消息 ID 全局唯一，同一 `server_id`、`client_msg_id`、发送者、会话与序号且内容解码后
///   相同即同一条消息的重投；attributes 等元数据的差异（跨版本重投时 Writer 的规整可能变了）不算冲突。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExistingEvent {
    /// 类型与载荷字节都相同。
    Identical,
    /// 类型相同，载荷字节不同但解码后相同。
    SameContent,
    /// 已有行是修复前的类型号，载荷与本次相同（字节或解码后）。
    LegacyTypeTag,
    /// 同一条消息，内容相同，元数据不同。
    SameMessage,
    /// 同一个位置上的另一个事件。
    Conflict,
}

/// `accept_legacy_type_tag`：已有行来自数据库时为 true；同一批内两行都由本版本编码，不接受旧类型号。
fn match_existing_event(
    existing_event_type: i32,
    existing_payload: &[u8],
    row: &EventStreamInsertRow,
    accept_legacy_type_tag: bool,
) -> ExistingEvent {
    if existing_event_type == row.event_type && existing_payload == row.payload.as_slice() {
        return ExistingEvent::Identical;
    }
    // 旧类型号只在有载荷时认：空载荷没有内容可比，旧号又可能恰好是另一个 proto 类型（会话更新的旧号 7
    // 即 proto 的会话删除）。
    let legacy_type_tag = accept_legacy_type_tag
        && !row.payload.is_empty()
        && existing_event_type != row.event_type
        && existing_event_type == row.legacy_event_type;
    if existing_event_type != row.event_type && !legacy_type_tag {
        return ExistingEvent::Conflict;
    }
    if existing_payload == row.payload.as_slice() {
        return ExistingEvent::LegacyTypeTag;
    }
    // 两边都按本次的类型解：旧类型号的行，载荷本来就是同一种内层消息的编码。
    let (Some(existing), Some(new)) = (
        decode_event_payload(row.event_type, existing_payload),
        decode_event_payload(row.event_type, &row.payload),
    ) else {
        return ExistingEvent::Conflict;
    };
    if existing == new {
        return if legacy_type_tag {
            ExistingEvent::LegacyTypeTag
        } else {
            ExistingEvent::SameContent
        };
    }
    match (&existing, &new) {
        (Payload::Message(existing), Payload::Message(new))
            if !legacy_type_tag && same_message(existing, new) =>
        {
            ExistingEvent::SameMessage
        }
        _ => ExistingEvent::Conflict,
    }
}

/// 同一条消息的重投：服务端消息 ID（全局唯一，非空）、`client_msg_id`、发送者、会话、会话序号与
/// 消息类型相同，内容解码后相同。其余字段（attributes、扩展、展示名、状态等）不参与判断。
fn same_message(
    existing: &flare_proto::common::Message,
    new: &flare_proto::common::Message,
) -> bool {
    !new.server_id.is_empty()
        && existing.server_id == new.server_id
        && existing.client_msg_id == new.client_msg_id
        && existing.sender_id == new.sender_id
        && existing.conversation_id == new.conversation_id
        && existing.conversation_seq == new.conversation_seq
        && existing.message_type == new.message_type
        && existing.content == new.content
}

/// 同一位置上的另一个事件：重试不会改变已有行，按不可重试的冲突（`HttpConflict`）报错。
fn event_stream_conflict(row: &EventStreamInsertRow, existing_event_type: i32) -> FlareError {
    flare_server_core::flare_err!(
        ErrorCode::HttpConflict,
        format!(
            "event stream conflict tenant_id={} conversation_id={} seq={} existing_type={} new_type={}",
            row.tenant_id, row.conversation_id, row.seq, existing_event_type, row.event_type
        )
    )
}

/// 按 proto 类型号解 `events.payload`（与 Reader `event_stream_row::decode_payload_oneof` 同一张表）。
/// 解不开或类型号未知时为 `None`，调用方按冲突处理。
fn decode_event_payload(event_type: i32, payload: &[u8]) -> Option<Payload> {
    use flare_proto::common::EventType as T;

    fn decode<M: prost::Message + Default>(payload: &[u8]) -> Option<M> {
        M::decode(payload).ok()
    }

    Some(match T::try_from(event_type).ok()? {
        T::EventMessage => Payload::Message(decode(payload)?),
        T::EventMessageRecall => Payload::Recall(decode(payload)?),
        T::EventMessageEdit => Payload::Edit(decode(payload)?),
        T::EventMessageDelete => Payload::Delete(decode(payload)?),
        T::EventReadReceipt => Payload::Read(decode(payload)?),
        T::EventConversationUpdate => Payload::Conversation(decode(payload)?),
        T::EventConversationDelete => Payload::ConversationDelete(decode(payload)?),
        T::EventReaction => Payload::Reaction(decode(payload)?),
        T::EventPin => Payload::Pin(decode(payload)?),
        T::EventUnpin => Payload::Unpin(decode(payload)?),
        T::EventMark => Payload::Mark(decode(payload)?),
        T::EventUnmark => Payload::Unmark(decode(payload)?),
        T::EventMessageRetentionScheduled => Payload::RetentionScheduled(decode(payload)?),
        T::EventMessageRetentionExpired => Payload::RetentionExpired(decode(payload)?),
        T::EventMessageRetentionPurged => Payload::RetentionPurged(decode(payload)?),
        T::EventCustom => Payload::Custom(decode(payload)?),
        T::EventMessagePluginState => Payload::PluginState(decode(payload)?),
        T::Unspecified => return None,
    })
}

const EVENT_STREAM_BATCH_SIZE: usize = 500;

/// `events` 表的一行（Writer 侧编码）。Reader `event_stream_row::proto_event_from_events_row`
/// 是它的逆，两侧的往返由 `tests/event_codec_contract.rs` 钉住。
#[derive(Debug, Clone)]
pub struct EventStreamInsertRow {
    pub tenant_id: String,
    pub conversation_id: String,
    pub seq: i64,
    /// proto `flare.common.v1.EventType` 的取值。
    pub event_type: i32,
    /// `915b5c2` 之前的 Writer 给同一事件落的类型号（领域枚举的判别值），只用于识别升级窗口内的重投。
    pub legacy_event_type: i32,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub operator_id: String,
    pub request_id: Option<String>,
    pub event_seq: Option<i64>,
    /// `Event.payload` oneof 内层消息的 prost 编码。
    pub payload: Vec<u8>,
}

impl EventStreamInsertRow {
    fn key(&self) -> (String, String, i64) {
        (
            self.tenant_id.clone(),
            self.conversation_id.clone(),
            self.seq,
        )
    }
}

/// 领域事件 → `events` 行；没有会话 ID 的事件不落事件流（`None`）。
pub fn event_to_insert_row(
    event: &crate::domain::model::Event,
) -> Result<Option<EventStreamInsertRow>> {
    let tenant_id = event.tenant_id.clone();
    let proto_ev = crate::convert::event_to_proto(event);
    let conversation_id = proto_ev.conversation_id.clone();
    if conversation_id.is_empty() {
        return Ok(None);
    }

    let created_at = if proto_ev.created_at > 0 {
        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(proto_ev.created_at)
            .unwrap_or_else(chrono::Utc::now)
    } else {
        chrono::Utc::now()
    };
    let payload = proto_ev
        .payload
        .as_ref()
        .map(encode_payload_bytes)
        .transpose()?
        .unwrap_or_default();

    Ok(Some(EventStreamInsertRow {
        tenant_id,
        conversation_id,
        seq: proto_ev.conversation_seq as i64,
        event_type: proto_ev.r#type,
        legacy_event_type: event.r#type as i32,
        created_at,
        operator_id: event.operator_id.clone(),
        request_id: proto_ev.request_id,
        event_seq: event.event_seq.map(|u| u as i64),
        payload,
    }))
}

pub struct PostgresEventStreamStore {
    pool: Pool<sqlx::Postgres>,
}

impl PostgresEventStreamStore {
    pub fn new(pool: Pool<sqlx::Postgres>) -> Self {
        Self { pool }
    }

    pub async fn from_config(config: &StorageWriterConfig) -> Result<Option<Self>> {
        let url = match &config.postgres_url {
            Some(u) => u,
            None => return Ok(None),
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(config.postgres_max_connections)
            .connect(url)
            .await
            .map_err(|err| db_error("connect event stream postgres", err))?;
        Ok(Some(Self::new(pool)))
    }

    /// 同一 `(tenant_id, conversation_id, seq)` 上已有行的类型号与载荷。
    async fn load_existing_event(
        &self,
        row: &EventStreamInsertRow,
    ) -> Result<Option<(i32, Vec<u8>)>> {
        let existing = sqlx::query(
            r#"
            SELECT event_type, payload FROM events
            WHERE tenant_id = $1 AND conversation_id = $2 AND seq = $3
            LIMIT 1
            "#,
        )
        .bind(&row.tenant_id)
        .bind(&row.conversation_id)
        .bind(row.seq)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| db_error("load existing event stream row", err))?;

        let Some(existing) = existing else {
            return Ok(None);
        };
        let existing_event_type: i32 = existing
            .try_get("event_type")
            .map_err(|err| db_error("decode event_type from event stream row", err))?;
        let existing_payload: Option<Vec<u8>> = existing
            .try_get("payload")
            .map_err(|err| db_error("decode payload from event stream row", err))?;
        Ok(Some((
            existing_event_type,
            existing_payload.unwrap_or_default(),
        )))
    }

    /// 已有行与本次的行比较，按重复接受的几种情况记日志；冲突时返回 [`ExistingEvent::Conflict`]，由调用方报错。
    fn classify_existing_event(
        row: &EventStreamInsertRow,
        existing_event_type: i32,
        existing_payload: &[u8],
    ) -> ExistingEvent {
        let verdict = match_existing_event(existing_event_type, existing_payload, row, true);
        match verdict {
            ExistingEvent::Identical => {}
            ExistingEvent::SameContent => debug!(
                tenant_id = %row.tenant_id,
                conversation_id = %row.conversation_id,
                seq = row.seq,
                "event stream redelivery with re-encoded payload; keeping the existing row"
            ),
            ExistingEvent::LegacyTypeTag | ExistingEvent::SameMessage => warn!(
                tenant_id = %row.tenant_id,
                conversation_id = %row.conversation_id,
                seq = row.seq,
                existing_type = existing_event_type,
                new_type = row.event_type,
                ?verdict,
                "event stream redelivery differs from the existing row; accepted as a duplicate, existing row kept"
            ),
            // 冲突由调用方报错并记日志。
            ExistingEvent::Conflict => {}
        }
        verdict
    }

    async fn verify_conflicting_event(&self, row: &EventStreamInsertRow) -> Result<()> {
        if let Some((existing_event_type, existing_payload)) = self.load_existing_event(row).await?
            && Self::classify_existing_event(row, existing_event_type, &existing_payload)
                == ExistingEvent::Conflict
        {
            return Err(event_stream_conflict(row, existing_event_type));
        }
        Ok(())
    }

    async fn append_event_rows(&self, rows: &[EventStreamInsertRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut query: QueryBuilder<Postgres> = QueryBuilder::new(
            r#"
            INSERT INTO events (
                tenant_id, conversation_id, seq, event_type, created_at, operator_id, request_id, event_seq, payload
            )
            "#,
        );

        query.push_values(rows, |mut b, row| {
            b.push_bind(&row.tenant_id);
            b.push_bind(&row.conversation_id);
            b.push_bind(row.seq);
            b.push_bind(row.event_type);
            b.push_bind(row.created_at);
            b.push_bind(&row.operator_id);
            b.push_bind(&row.request_id);
            b.push_bind(row.event_seq);
            b.push_bind(&row.payload);
        });
        query.push(
            " ON CONFLICT (tenant_id, conversation_id, seq) DO NOTHING RETURNING tenant_id, conversation_id, seq",
        );

        let inserted: Vec<(String, String, i64)> = query
            .build_query_as()
            .fetch_all(&self.pool)
            .await
            .map_err(|err| db_error("append event stream batch", err))?;
        let inserted_keys: HashSet<(String, String, i64)> = inserted.into_iter().collect();

        for row in rows {
            if !inserted_keys.contains(&row.key()) {
                self.verify_conflicting_event(row).await?;
            }
        }

        Ok(())
    }
}

impl EventStreamRepository for PostgresEventStreamStore {
    #[instrument(skip(self, event), fields(tenant_id = %event.tenant_id, conversation_id = %event.conversation_id, seq = %event.seq))]
    async fn append_event_to_stream(
        &self,
        ctx: &Ctx,
        event: &crate::domain::model::Event,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        let Some(row) = event_to_insert_row(event)? else {
            return Ok(());
        };

        let insert_result = sqlx::query(
            r#"
            INSERT INTO events (
                tenant_id, conversation_id, seq, event_type, created_at, operator_id, request_id, event_seq, payload
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (tenant_id, conversation_id, seq) DO NOTHING
            "#,
        )
        .bind(&row.tenant_id)
        .bind(&row.conversation_id)
        .bind(row.seq)
        .bind(row.event_type)
        .bind(row.created_at)
        .bind(&row.operator_id)
        .bind(&row.request_id)
        .bind(row.event_seq)
        .bind(&row.payload)
        .execute(&self.pool)
        .await
        .map_err(|err| db_error("append event stream row", err))?;

        if insert_result.rows_affected() == 0 {
            self.verify_conflicting_event(&row).await?;
        }

        Ok(())
    }

    #[instrument(skip(self, events), fields(batch_size = events.len()))]
    async fn append_events_to_stream(
        &self,
        ctx: &Ctx,
        events: &[crate::domain::model::Event],
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        if events.is_empty() {
            return Ok(());
        }

        let mut rows = Vec::with_capacity(events.len());
        for event in events {
            if let Some(row) = event_to_insert_row(event)? {
                rows.push(row);
            }
        }
        if rows.is_empty() {
            return Ok(());
        }

        let mut seen: HashMap<_, &EventStreamInsertRow> = HashMap::with_capacity(rows.len());
        for row in &rows {
            if let Some(previous) = seen.insert(row.key(), row)
                && match_existing_event(previous.event_type, &previous.payload, row, false)
                    == ExistingEvent::Conflict
            {
                return Err(flare_server_core::error::FlareError::system(format!(
                    "event stream batch contains conflicting duplicate tenant_id={} conversation_id={} seq={}",
                    row.tenant_id, row.conversation_id, row.seq
                )));
            }
        }

        for chunk in rows.chunks(EVENT_STREAM_BATCH_SIZE) {
            self.append_event_rows(chunk).await?;
        }

        Ok(())
    }

    #[instrument(skip(self, event), fields(tenant_id = %event.tenant_id, conversation_id = %event.conversation_id, seq = %event.seq))]
    async fn match_stored_event(
        &self,
        ctx: &Ctx,
        event: &crate::domain::model::Event,
    ) -> Result<StoredEventMatch> {
        let _ = ctx; // 上下文用于日志追踪
        // 不落事件流的事件（没有会话 ID）无从判重，与写入侧一致。
        let Some(row) = event_to_insert_row(event)? else {
            return Ok(StoredEventMatch::Absent);
        };
        let Some((existing_event_type, existing_payload)) = self.load_existing_event(&row).await?
        else {
            return Ok(StoredEventMatch::Absent);
        };
        Ok(
            match Self::classify_existing_event(&row, existing_event_type, &existing_payload) {
                ExistingEvent::Conflict => StoredEventMatch::Conflict {
                    existing_event_type,
                },
                _ => StoredEventMatch::Duplicate,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::{
        BurnScheduledPayload, Event, EventPayload, EventType, MarkPayload, PinPayload,
        ReactionPayload, RecallPayload, UnmarkPayload, UnpinPayload,
    };
    use flare_proto::common;

    fn operation_event(r#type: EventType, payload: EventPayload) -> Event {
        Event {
            tenant_id: "tenant-a".to_string(),
            conversation_id: "conv-a".to_string(),
            seq: 42,
            r#type,
            created_at: None,
            operator_id: "u1".to_string(),
            event_seq: Some(7),
            request_id: None,
            payload: Some(payload),
        }
    }

    /// `events` 行的 `event_type` 必须是内层载荷对应的 proto 类型：Reader 按这个数字选择解码的
    /// 消息类型（`event.proto` 中 `EVENT_REACTION = 8` 对 `ReactionEvent`，依此类推）。
    fn assert_row<M>(event: &Event, event_type: i32, expected: M)
    where
        M: prost::Message + Default + PartialEq + std::fmt::Debug,
    {
        let row = event_to_insert_row(event)
            .expect("encode")
            .expect("row for a conversation event");
        assert_eq!(row.event_type, event_type, "{:?}", event.r#type);
        assert_eq!(
            M::decode(row.payload.as_slice()).expect("payload decodes as the declared type"),
            expected,
            "{:?}",
            event.r#type
        );
    }

    /// 写入侧的行能按 proto 类型解回原内容（W10-S1 F1）。修复前 `event_type` 是领域枚举的判别值：
    /// 反应落成 11（`EVENT_MARK`），Reader 解 `MarkEvent` 失败；置顶落成 12（`EVENT_UNMARK`），
    /// 会被静默解成取消标记。撤回的数字两边本来一致，作对照。
    #[test]
    fn operation_rows_decode_as_their_declared_proto_type() {
        assert_row(
            &operation_event(
                EventType::Reaction,
                EventPayload::Reaction(ReactionPayload {
                    server_msg_id: "m1".to_string(),
                    user_id: "u1".to_string(),
                    emoji: "thumbsup".to_string(),
                    action: 1,
                }),
            ),
            8,
            common::ReactionEvent {
                server_msg_id: "m1".to_string(),
                user_id: "u1".to_string(),
                emoji: "thumbsup".to_string(),
                action: 1,
            },
        );
        assert_row(
            &operation_event(
                EventType::Pin,
                EventPayload::Pin(PinPayload {
                    server_msg_id: "m1".to_string(),
                    pinned_by: "u1".to_string(),
                    reason: Some("keep".to_string()),
                    expire_at: None,
                    scope: 1,
                }),
            ),
            9,
            common::PinEvent {
                server_msg_id: "m1".to_string(),
                pinned_by: "u1".to_string(),
                reason: Some("keep".to_string()),
                expire_at: None,
                scope: 1,
            },
        );
        assert_row(
            &operation_event(
                EventType::Unpin,
                EventPayload::Unpin(UnpinPayload {
                    server_msg_id: "m1".to_string(),
                    unpinned_by: "u1".to_string(),
                    scope: 1,
                }),
            ),
            10,
            common::UnpinEvent {
                server_msg_id: "m1".to_string(),
                unpinned_by: "u1".to_string(),
                scope: 1,
            },
        );
        assert_row(
            &operation_event(
                EventType::Mark,
                EventPayload::Mark(MarkPayload {
                    server_msg_id: "m1".to_string(),
                    user_id: "u1".to_string(),
                    mark_type: 2,
                    color: "#FF0000".to_string(),
                }),
            ),
            11,
            common::MarkEvent {
                server_msg_id: "m1".to_string(),
                user_id: "u1".to_string(),
                mark_type: 2,
                color: "#FF0000".to_string(),
            },
        );
        assert_row(
            &operation_event(
                EventType::Unmark,
                EventPayload::Unmark(UnmarkPayload {
                    server_msg_id: "m1".to_string(),
                    user_id: "u1".to_string(),
                    mark_type: 2,
                }),
            ),
            12,
            common::UnmarkEvent {
                server_msg_id: "m1".to_string(),
                user_id: "u1".to_string(),
                mark_type: 2,
            },
        );
        assert_row(
            &operation_event(
                EventType::MessageRecall,
                EventPayload::Recall(RecallPayload {
                    server_msg_id: "m1".to_string(),
                    reason: "typo".to_string(),
                    time_limit_seconds: None,
                    allow_admin_recall: None,
                }),
            ),
            2,
            common::MessageRecallEvent {
                server_msg_id: "m1".to_string(),
                reason: "typo".to_string(),
                time_limit_seconds: None,
                allow_admin_recall: None,
            },
        );

        let scheduled = operation_event(
            EventType::MessageBurnScheduled,
            EventPayload::BurnScheduled(BurnScheduledPayload {
                tenant_id: "tenant-a".to_string(),
                conversation_id: "conv-a".to_string(),
                message_id: "m1".to_string(),
                server_id: "m1".to_string(),
                seq: None,
                reader_id: Some("u2".to_string()),
                burn_at: 1_700_000_000_000,
                event_time: 1_699_999_990_000,
            }),
        );
        let Some(common::event::Payload::RetentionScheduled(expected)) =
            crate::convert::event_to_proto(&scheduled).payload
        else {
            panic!("burn-scheduled payload converts to RetentionScheduled");
        };
        assert_eq!(expected.server_msg_id, "m1");
        assert_row(&scheduled, 13, expected);
    }

    fn row_for(event: &Event) -> EventStreamInsertRow {
        event_to_insert_row(event)
            .expect("encode")
            .expect("row for a conversation event")
    }

    fn reaction(emoji: &str) -> Event {
        operation_event(
            EventType::Reaction,
            EventPayload::Reaction(ReactionPayload {
                server_msg_id: "m1".to_string(),
                user_id: "u1".to_string(),
                emoji: emoji.to_string(),
                action: 1,
            }),
        )
    }

    fn text_message(server_id: &str, client_msg_id: &str, text: &str) -> Event {
        let message = crate::domain::model::Message {
            server_id: server_id.to_string(),
            conversation_id: "conv-a".to_string(),
            client_msg_id: client_msg_id.to_string(),
            sender_id: "u1".to_string(),
            conversation_seq: 42,
            message_type: 1,
            content: Some(common::MessageContent {
                content: Some(common::message_content::Content::Text(
                    common::TextContent {
                        text: text.to_string(),
                        mentions: Vec::new(),
                    },
                )),
            }),
            extra: [
                ("tenant_id", "tenant-a"),
                ("business_type", "chat"),
                ("source_app", "web"),
                ("trace", "t-1"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
            ..Default::default()
        };
        Event {
            tenant_id: "tenant-a".to_string(),
            conversation_id: "conv-a".to_string(),
            seq: 42,
            r#type: EventType::Message,
            created_at: None,
            operator_id: String::new(),
            event_seq: None,
            request_id: None,
            payload: Some(EventPayload::Message(Box::new(message))),
        }
    }

    fn proto_message(row: &EventStreamInsertRow) -> common::Message {
        <common::Message as prost::Message>::decode(row.payload.as_slice()).expect("message")
    }

    /// 同一条消息换一种 map 条目顺序的编码：protobuf 允许 map 条目任意顺序、字段拼接即合并，
    /// 与 prost 按 `HashMap` 迭代序编码时两次投递得到的字节差异同类。保证与 `row.payload` 不同。
    fn reencoded_with_other_map_order(row: &EventStreamInsertRow) -> Vec<u8> {
        let message = proto_message(row);
        let mut entries: Vec<_> = message.attributes.clone().into_iter().collect();
        assert!(entries.len() >= 2, "needs two map entries to reorder");
        entries.sort();
        let mut base = message.clone();
        base.attributes.clear();
        let encode = |entries: &[(String, String)]| {
            let mut bytes = prost::Message::encode_to_vec(&base);
            for (key, value) in entries {
                let entry = common::Message {
                    attributes: [(key.clone(), value.clone())].into_iter().collect(),
                    ..Default::default()
                };
                bytes.extend(prost::Message::encode_to_vec(&entry));
            }
            bytes
        };
        let ascending = encode(&entries);
        let reencoded = if ascending == row.payload {
            entries.reverse();
            encode(&entries)
        } else {
            ascending
        };
        assert_ne!(reencoded, row.payload);
        assert_eq!(
            <common::Message as prost::Message>::decode(reencoded.as_slice()).expect("decodes"),
            message
        );
        reencoded
    }

    #[test]
    fn identical_rows_are_duplicates_and_content_changes_are_conflicts() {
        let row = row_for(&reaction("thumbsup"));
        assert_eq!(
            match_existing_event(row.event_type, &row.payload, &row, true),
            ExistingEvent::Identical
        );
        let other = row_for(&reaction("heart"));
        assert_eq!(
            match_existing_event(other.event_type, &other.payload, &row, true),
            ExistingEvent::Conflict
        );
        // 类型不同、载荷相同（撤回两侧类型号一致，没有旧编码可言）。
        let recall = row_for(&operation_event(
            EventType::MessageRecall,
            EventPayload::Recall(RecallPayload {
                server_msg_id: "m1".to_string(),
                reason: String::new(),
                time_limit_seconds: None,
                allow_admin_recall: None,
            }),
        ));
        assert_eq!(recall.legacy_event_type, recall.event_type);
        assert_eq!(
            match_existing_event(3, &recall.payload, &recall, true),
            ExistingEvent::Conflict
        );
        // 载荷解不开按冲突处理。
        assert_eq!(
            match_existing_event(row.event_type, &[0xff, 0xff, 0xff], &row, true),
            ExistingEvent::Conflict
        );
    }

    /// 升级窗口（W12-R）：修复前的 Writer 以领域判别值落类型号，同一事件升级后重投时类型号不同、
    /// 载荷相同。修复前按冲突进 DLQ；现在按重复接受。旧类型号照修复前的值写死。
    #[test]
    fn pre_fix_rows_of_the_same_event_are_accepted_as_duplicates() {
        let cases = [
            (reaction("thumbsup"), 8, 11),
            (
                operation_event(
                    EventType::Pin,
                    EventPayload::Pin(PinPayload {
                        server_msg_id: "m1".to_string(),
                        pinned_by: "u1".to_string(),
                        reason: None,
                        expire_at: None,
                        scope: 1,
                    }),
                ),
                9,
                12,
            ),
            (
                operation_event(
                    EventType::Unpin,
                    EventPayload::Unpin(UnpinPayload {
                        server_msg_id: "m1".to_string(),
                        unpinned_by: "u1".to_string(),
                        scope: 1,
                    }),
                ),
                10,
                13,
            ),
            (
                operation_event(
                    EventType::Mark,
                    EventPayload::Mark(MarkPayload {
                        server_msg_id: "m1".to_string(),
                        user_id: "u1".to_string(),
                        mark_type: 2,
                        color: String::new(),
                    }),
                ),
                11,
                14,
            ),
            (
                operation_event(
                    EventType::Unmark,
                    EventPayload::Unmark(UnmarkPayload {
                        server_msg_id: "m1".to_string(),
                        user_id: "u1".to_string(),
                        mark_type: 2,
                    }),
                ),
                12,
                15,
            ),
        ];
        for (event, proto_type, pre_fix_type) in cases {
            let row = row_for(&event);
            assert_eq!(
                (row.event_type, row.legacy_event_type),
                (proto_type, pre_fix_type)
            );
            assert_eq!(
                match_existing_event(pre_fix_type, &row.payload, &row, true),
                ExistingEvent::LegacyTypeTag,
                "{:?}",
                event.r#type
            );
            // 只认这一种旧编码：同一批内两行都是本版本编码，不接受；旧类型号但内容不同仍是冲突。
            assert_eq!(
                match_existing_event(pre_fix_type, &row.payload, &row, false),
                ExistingEvent::Conflict
            );
            assert_eq!(
                match_existing_event(pre_fix_type + 1, &row.payload, &row, true),
                ExistingEvent::Conflict
            );
        }

        let row = row_for(&reaction("thumbsup"));
        let other = row_for(&reaction("heart"));
        assert_eq!(
            match_existing_event(11, &other.payload, &row, true),
            ExistingEvent::Conflict
        );

        // 无载荷的事件不认旧类型号：会话更新的旧号 7 正是 proto 的会话删除。
        let mut conversation_update = reaction("unused");
        conversation_update.r#type = EventType::ConversationUpdate;
        conversation_update.payload = None;
        let row = row_for(&conversation_update);
        assert_eq!(
            (row.event_type, row.legacy_event_type, row.payload.len()),
            (6, 7, 0)
        );
        assert_eq!(
            match_existing_event(7, &[], &row, true),
            ExistingEvent::Conflict
        );
    }

    /// 同一条消息再次编码时 map 条目顺序可能不同（prost 按 `HashMap` 迭代序编码），字节比较会把重投
    /// 当冲突、重试后进 DLQ（W12-R）。解码后相同即重复。
    #[test]
    fn a_redelivered_message_with_reordered_map_entries_is_a_duplicate() {
        let row = row_for(&text_message("srv-1", "cli-1", "hello"));
        let existing = reencoded_with_other_map_order(&row);
        assert_eq!(
            match_existing_event(row.event_type, &existing, &row, true),
            ExistingEvent::SameContent
        );
        assert_eq!(
            match_existing_event(row.event_type, &existing, &row, false),
            ExistingEvent::SameContent
        );
    }

    /// 同一条消息（server_id、client_msg_id、发送者、会话、序号相同，内容解码后相同）只差元数据时
    /// 按重复接受；任一身份字段或内容不同仍是冲突。
    #[test]
    fn the_same_message_with_other_metadata_is_a_duplicate_but_another_message_is_a_conflict() {
        let row = row_for(&text_message("srv-1", "cli-1", "hello"));
        let with_metadata = |edit: &dyn Fn(&mut common::Message)| {
            let mut message = proto_message(&row);
            edit(&mut message);
            prost::Message::encode_to_vec(&message)
        };

        let other_metadata = with_metadata(&|m| {
            m.attributes
                .insert("x-tenant-id".to_string(), "tenant-a ".to_string());
            m.sender_name = "Alice".to_string();
        });
        assert_eq!(
            match_existing_event(row.event_type, &other_metadata, &row, true),
            ExistingEvent::SameMessage
        );

        for (label, existing) in [
            (
                "server_id",
                with_metadata(&|m| m.server_id = "srv-2".to_string()),
            ),
            (
                "client_msg_id",
                with_metadata(&|m| m.client_msg_id = "cli-2".to_string()),
            ),
            (
                "sender_id",
                with_metadata(&|m| m.sender_id = "u2".to_string()),
            ),
            (
                "conversation_seq",
                with_metadata(&|m| m.conversation_seq = 43),
            ),
            ("message_type", with_metadata(&|m| m.message_type = 2)),
            (
                "content",
                row_for(&text_message("srv-1", "cli-1", "bye")).payload,
            ),
        ] {
            assert_eq!(
                match_existing_event(row.event_type, &existing, &row, true),
                ExistingEvent::Conflict,
                "{label}"
            );
        }

        // 服务端消息 ID 为空时不能据此认定同一条消息。
        let anonymous = row_for(&text_message("", "cli-1", "hello"));
        let existing = {
            let mut message = proto_message(&anonymous);
            message.sender_name = "Alice".to_string();
            prost::Message::encode_to_vec(&message)
        };
        assert_eq!(
            match_existing_event(anonymous.event_type, &existing, &anonymous, true),
            ExistingEvent::Conflict
        );
    }
}
