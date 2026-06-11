//! 事件流写入（init_v2：event_type INT，payload BYTEA 单列）

use flare_im_contracts::Ctx;
use flare_proto::common::event::Payload;
use flare_server_core::error::{ErrorCode, FlareError, Result, map_infra_error};
use sqlx::{Pool, Postgres, QueryBuilder, Row};
use std::collections::{HashMap, HashSet};
use tracing::instrument;

use crate::config::StorageWriterConfig;
use crate::domain::repository::EventStreamRepository;

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

fn event_conflict_matches(
    existing_event_type: i32,
    existing_payload: &[u8],
    new_event_type: i32,
    new_payload: &[u8],
) -> bool {
    existing_event_type == new_event_type && existing_payload == new_payload
}

const EVENT_STREAM_BATCH_SIZE: usize = 500;

#[derive(Clone)]
struct EventStreamInsertRow {
    tenant_id: String,
    conversation_id: String,
    seq: i64,
    event_type: i32,
    created_at: chrono::DateTime<chrono::Utc>,
    operator_id: String,
    request_id: Option<String>,
    event_seq: Option<i64>,
    payload: Vec<u8>,
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

fn event_to_insert_row(
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

    async fn verify_conflicting_event(&self, row: &EventStreamInsertRow) -> Result<()> {
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

        if let Some(existing) = existing {
            let existing_event_type: i32 = existing
                .try_get("event_type")
                .map_err(|err| db_error("decode event_type from event stream row", err))?;
            let existing_payload: Option<Vec<u8>> = existing
                .try_get("payload")
                .map_err(|err| db_error("decode payload from event stream row", err))?;
            let existing_payload = existing_payload.unwrap_or_default();
            if !event_conflict_matches(
                existing_event_type,
                &existing_payload,
                row.event_type,
                &row.payload,
            ) {
                return Err(flare_server_core::error::FlareError::system(format!(
                    "event stream conflict tenant_id={} conversation_id={} seq={} existing_type={} new_type={}",
                    row.tenant_id,
                    row.conversation_id,
                    row.seq,
                    existing_event_type,
                    row.event_type
                )));
            }
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

        let mut seen = HashMap::with_capacity(rows.len());
        for row in &rows {
            let key = row.key();
            if let Some((event_type, payload)) = seen.insert(key, (row.event_type, &row.payload))
                && (!event_conflict_matches(event_type, payload, row.event_type, &row.payload))
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

    #[instrument(skip(self), fields(tenant_id, conversation_id, seq))]
    async fn event_exists(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        conversation_id: &str,
        seq: i64,
    ) -> Result<bool> {
        let _ = ctx; // 上下文用于日志追踪
        let row = sqlx::query(
            r#"
            SELECT 1 FROM events
            WHERE tenant_id = $1 AND conversation_id = $2 AND seq = $3
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(conversation_id)
        .bind(seq)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| db_error("check event stream existence", err))?;
        Ok(row.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_conflict_matches_only_when_type_and_payload_match() {
        assert!(event_conflict_matches(1, b"payload-a", 1, b"payload-a"));
        assert!(!event_conflict_matches(1, b"payload-a", 2, b"payload-a"));
        assert!(!event_conflict_matches(1, b"payload-a", 1, b"payload-b"));
    }
}
