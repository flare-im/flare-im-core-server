//! `tenant_client_policy` (migrations/0023) and `tenant_notice_outbox`. The row and the notices its
//! watermarks owe commit together; `flare.*` notices leave through the outbox only.
use std::sync::Arc;
use std::time::Duration;

use flare_server_core::error::{FlareError, Result};
use sqlx::{PgPool, Postgres, Row, Transaction};

use crate::domain::model::{
    ClientPolicyNotice, ClientPolicyRecord, ClientPolicyWatermarks, TenantProjectionOutcome,
};
use crate::domain::repository::tenant_notice_outbox::{PendingTenantNotice, TenantNoticeOutbox};

fn unavailable(_: impl std::fmt::Debug) -> FlareError {
    FlareError::system("client policy projection storage unavailable")
}

fn outbox_unavailable(_: impl std::fmt::Debug) -> FlareError {
    FlareError::system("tenant notice outbox unavailable")
}

fn interval_ms(delay: Duration) -> i64 {
    i64::try_from(delay.as_millis()).unwrap_or(i64::MAX)
}

/// `last_error` holds at most 512 characters.
fn bounded(note: &str) -> String {
    note.chars().take(512).collect()
}

fn bigint(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn unsigned(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

pub struct PostgresClientPolicy {
    pub pool: Arc<PgPool>,
}

impl PostgresClientPolicy {
    /// Writes `record` and queues the notices it owes, or changes nothing when it is not newer.
    pub async fn upsert_if_newer(
        &self,
        record: &ClientPolicyRecord,
    ) -> Result<TenantProjectionOutcome> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let previous = sqlx::query(
            "SELECT version, access_serial, release_generation FROM tenant_client_policy \
             WHERE tenant_id = $1 FOR UPDATE",
        )
        .bind(&record.tenant_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?
        .map(|row| ClientPolicyWatermarks {
            version: unsigned(row.get("version")),
            access_serial: unsigned(row.get("access_serial")),
            release_generation: unsigned(row.get("release_generation")),
        });
        if let Some(previous) = previous.filter(|previous| previous.version >= record.version) {
            tx.rollback().await.map_err(unavailable)?;
            return Ok(TenantProjectionOutcome {
                accepted: false,
                current_version: previous.version,
            });
        }
        let build_policies = serde_json::to_value(&record.build_policies)
            .map_err(|_| FlareError::system("client policy encoding failed"))?;
        // The version check repeats here for two first projections racing past the row lock.
        let written: Option<i64> = sqlx::query_scalar(
            r#"
            INSERT INTO tenant_client_policy AS p
                (tenant_id, version, access_serial, release_generation, build_policies, updated_at)
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (tenant_id) DO UPDATE SET
                version = EXCLUDED.version,
                access_serial = GREATEST(p.access_serial, EXCLUDED.access_serial),
                release_generation = GREATEST(p.release_generation, EXCLUDED.release_generation),
                build_policies = EXCLUDED.build_policies,
                updated_at = NOW()
            WHERE p.version < EXCLUDED.version
            RETURNING version
            "#,
        )
        .bind(&record.tenant_id)
        .bind(bigint(record.version))
        .bind(bigint(record.access_serial))
        .bind(bigint(record.release_generation))
        .bind(build_policies)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        if written.is_none() {
            let current: i64 =
                sqlx::query_scalar("SELECT version FROM tenant_client_policy WHERE tenant_id = $1")
                    .bind(&record.tenant_id)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(unavailable)?;
            tx.rollback().await.map_err(unavailable)?;
            return Ok(TenantProjectionOutcome {
                accepted: false,
                current_version: unsigned(current),
            });
        }
        for notice in record.notices_since(previous) {
            enqueue(&mut tx, &record.tenant_id, notice).await?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(TenantProjectionOutcome {
            accepted: true,
            current_version: record.version,
        })
    }

    pub async fn versions(&self, app_ids: &[String]) -> Result<Vec<(String, u64)>> {
        let rows = sqlx::query(
            "SELECT tenant_id, version FROM tenant_client_policy \
             WHERE cardinality($1::text[]) = 0 OR tenant_id = ANY($1) ORDER BY tenant_id",
        )
        .bind(app_ids)
        .fetch_all(&*self.pool)
        .await
        .map_err(unavailable)?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get("tenant_id"), unsigned(row.get("version"))))
            .collect())
    }
}

/// A newer version takes over a pending row and is due at once.
async fn enqueue(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    notice: ClientPolicyNotice,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO tenant_notice_outbox AS o (tenant_id, topic, version)
        VALUES ($1, $2, $3)
        ON CONFLICT (tenant_id, topic) DO UPDATE SET
            version = GREATEST(o.version, EXCLUDED.version),
            attempts = 0,
            next_attempt_at = CURRENT_TIMESTAMP,
            last_error = NULL
        "#,
    )
    .bind(tenant_id)
    .bind(notice.topic)
    .bind(bigint(notice.version))
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

pub struct PostgresTenantNoticeOutbox {
    pub pool: Arc<PgPool>,
}

#[async_trait::async_trait]
impl TenantNoticeOutbox for PostgresTenantNoticeOutbox {
    async fn claim_due(&self, limit: u32, lease: Duration) -> Result<Vec<PendingTenantNotice>> {
        let rows: Vec<(String, String, i64, i32)> = sqlx::query_as(
            r#"
            UPDATE tenant_notice_outbox o
            SET attempts = o.attempts + 1,
                next_attempt_at = CURRENT_TIMESTAMP + ($2::bigint * INTERVAL '1 millisecond')
            FROM (
                SELECT tenant_id, topic FROM tenant_notice_outbox
                WHERE next_attempt_at <= CURRENT_TIMESTAMP
                ORDER BY next_attempt_at
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            ) due
            WHERE o.tenant_id = due.tenant_id AND o.topic = due.topic
            RETURNING o.tenant_id, o.topic, o.version, o.attempts
            "#,
        )
        .bind(i64::from(limit))
        .bind(interval_ms(lease))
        .fetch_all(&*self.pool)
        .await
        .map_err(outbox_unavailable)?;
        Ok(rows
            .into_iter()
            .map(
                |(tenant_id, topic, version, attempts)| PendingTenantNotice {
                    tenant_id,
                    topic,
                    version: unsigned(version),
                    attempts,
                },
            )
            .collect())
    }

    async fn complete(&self, notice: &PendingTenantNotice) -> Result<()> {
        sqlx::query(
            "DELETE FROM tenant_notice_outbox WHERE tenant_id = $1 AND topic = $2 AND version = $3",
        )
        .bind(&notice.tenant_id)
        .bind(&notice.topic)
        .bind(bigint(notice.version))
        .execute(&*self.pool)
        .await
        .map_err(outbox_unavailable)?;
        Ok(())
    }

    async fn retry_after(
        &self,
        notice: &PendingTenantNotice,
        delay: Duration,
        error: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE tenant_notice_outbox \
             SET next_attempt_at = CURRENT_TIMESTAMP + ($4::bigint * INTERVAL '1 millisecond'), \
                 last_error = $5 \
             WHERE tenant_id = $1 AND topic = $2 AND version = $3",
        )
        .bind(&notice.tenant_id)
        .bind(&notice.topic)
        .bind(bigint(notice.version))
        .bind(interval_ms(delay))
        .bind(bounded(error))
        .execute(&*self.pool)
        .await
        .map_err(outbox_unavailable)?;
        Ok(())
    }
}
