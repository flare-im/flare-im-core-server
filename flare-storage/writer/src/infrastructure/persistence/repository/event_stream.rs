//! 事件流写入（init_v2：event_type INT，payload BYTEA 单列）

use anyhow::Result;
use flare_proto::common::event::Payload;
use flare_server_core::context::Ctx;
use prost::Message as _;
use sqlx::Pool;
use tracing::instrument;

use crate::config::StorageWriterConfig;
use crate::domain::repository::EventStreamRepository;

fn encode_payload_bytes(payload: &Payload) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    match payload {
        Payload::Message(m) => m.encode(&mut buf)?,
        Payload::Recall(r) => r.encode(&mut buf)?,
        Payload::Edit(e) => e.encode(&mut buf)?,
        Payload::Delete(d) => d.encode(&mut buf)?,
        Payload::Read(r) => r.encode(&mut buf)?,
        Payload::Reaction(r) => r.encode(&mut buf)?,
        Payload::Pin(p) => p.encode(&mut buf)?,
        Payload::Unpin(u) => u.encode(&mut buf)?,
        Payload::Mark(m) => m.encode(&mut buf)?,
        Payload::Unmark(u) => u.encode(&mut buf)?,
        _ => {}
    }
    Ok(buf)
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
            .await?;
        Ok(Some(Self::new(pool)))
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
        let tenant_id = event.tenant_id.as_str();
        let proto_ev = crate::convert::event_to_proto(event);
        let conversation_id = proto_ev.conversation_id.as_str();
        if conversation_id.is_empty() {
            return Ok(());
        }
        let seq = proto_ev.seq as i64;
        let event_type_int = proto_ev.r#type;
        let created_at = proto_ev
            .created_at
            .as_ref()
            .and_then(flare_im_core::utils::timestamp_to_datetime)
            .unwrap_or_else(chrono::Utc::now);
        let operator_id = event.operator_id.as_str();
        let request_id: Option<&str> = proto_ev.request_id.as_deref();
        let event_seq: Option<i64> = proto_ev.event_seq.map(|u| u as i64);
        let payload_bytes = proto_ev
            .payload
            .as_ref()
            .map(encode_payload_bytes)
            .transpose()?
            .unwrap_or_default();

        sqlx::query(
            r#"
            INSERT INTO events (
                tenant_id, conversation_id, seq, event_type, created_at, operator_id, request_id, event_seq, payload
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (tenant_id, conversation_id, seq) DO NOTHING
            "#,
        )
        .bind(tenant_id)
        .bind(conversation_id)
        .bind(seq)
        .bind(event_type_int)
        .bind(created_at)
        .bind(operator_id)
        .bind(request_id)
        .bind(event_seq)
        .bind(payload_bytes)
        .execute(&self.pool)
        .await?;

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
        .await?;
        Ok(row.is_some())
    }
}
