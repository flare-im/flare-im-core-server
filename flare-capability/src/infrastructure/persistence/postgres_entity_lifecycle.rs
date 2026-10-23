//! `plugin_entity_lifecycle_outbox` (migrations/0018): Capability adds `post_failed` rows, Storage
//! Writer adds `recalled` rows with the recall itself, and Capability's worker claims and ends them.
use crate::domain::repository::entity_lifecycle::{
    EntityLifecycleOutbox, LifecycleOutcome, LifecycleTask,
};
use flare_server_core::error::{FlareError, Result};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;

pub struct PostgresEntityLifecycleOutbox {
    pub pool: Arc<PgPool>,
}

fn unavailable(_: impl std::fmt::Debug) -> FlareError {
    FlareError::system("plugin entity lifecycle outbox unavailable")
}

fn interval_ms(delay: Duration) -> i64 {
    i64::try_from(delay.as_millis()).unwrap_or(i64::MAX)
}

/// `last_error` holds at most 512 characters.
fn bounded(note: &str) -> String {
    note.chars().take(512).collect()
}

impl PostgresEntityLifecycleOutbox {
    /// Fails when the table or its `unconfirmed` column is absent (migration 0018 not applied).
    pub async fn check_schema(&self) -> Result<()> {
        sqlx::query(
            "SELECT id, tenant_id, plugin_id, contribution_id, entity, reason, server_msg_id, status, \
             attempts, next_attempt_at, last_error, created_at, unconfirmed \
             FROM plugin_entity_lifecycle_outbox LIMIT 0",
        )
        .execute(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl EntityLifecycleOutbox for PostgresEntityLifecycleOutbox {
    async fn enqueue(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        contribution_id: &str,
        entity: &str,
        reason: &str,
        server_msg_id: Option<&str>,
    ) -> Result<()> {
        // A confirmed row replaces a pending unconfirmed one of the same entity: due at once.
        sqlx::query(
            "INSERT INTO plugin_entity_lifecycle_outbox AS o \
             (tenant_id, plugin_id, contribution_id, entity, reason, server_msg_id) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (tenant_id, plugin_id, entity, reason) DO UPDATE \
             SET unconfirmed = FALSE, \
                 next_attempt_at = LEAST(o.next_attempt_at, CURRENT_TIMESTAMP), \
                 updated_at = CURRENT_TIMESTAMP \
             WHERE o.unconfirmed AND o.status = 'pending'",
        )
        .bind(tenant_id)
        .bind(plugin_id)
        .bind(contribution_id)
        .bind(entity)
        .bind(reason)
        .bind(server_msg_id)
        .execute(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        Ok(())
    }

    async fn enqueue_unconfirmed(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        contribution_id: &str,
        entity: &str,
        verify_after: Duration,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO plugin_entity_lifecycle_outbox \
             (tenant_id, plugin_id, contribution_id, entity, reason, unconfirmed, next_attempt_at) \
             VALUES ($1, $2, $3, $4, 'post_failed', TRUE, \
                     CURRENT_TIMESTAMP + ($5::bigint * INTERVAL '1 millisecond')) \
             ON CONFLICT (tenant_id, plugin_id, entity, reason) DO NOTHING",
        )
        .bind(tenant_id)
        .bind(plugin_id)
        .bind(contribution_id)
        .bind(entity)
        .bind(interval_ms(verify_after))
        .execute(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        Ok(())
    }

    async fn post_failed(&self, tenant_id: &str, plugin_id: &str, entity: &str) -> Result<bool> {
        // An unconfirmed row counts once the worker ended it (delivered after it found no card).
        let found: Option<i64> = sqlx::query_scalar(
            "SELECT id FROM plugin_entity_lifecycle_outbox \
             WHERE tenant_id = $1 AND plugin_id = $2 AND entity = $3 AND reason = 'post_failed' \
               AND (NOT unconfirmed OR status <> 'pending')",
        )
        .bind(tenant_id)
        .bind(plugin_id)
        .bind(entity)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        Ok(found.is_some())
    }

    async fn claim_due(&self, limit: u32, lease: Duration) -> Result<Vec<LifecycleTask>> {
        // The row lock of the inner SELECT keeps two workers from claiming one row; the lease
        // (next_attempt_at moved forward) hands it back if this worker dies before ending it.
        #[allow(clippy::type_complexity)]
        let rows: Vec<(
            i64,
            String,
            String,
            String,
            String,
            String,
            Option<String>,
            i32,
            bool,
            i64,
        )> = sqlx::query_as(
            r#"
            UPDATE plugin_entity_lifecycle_outbox o
            SET attempts = o.attempts + 1,
                next_attempt_at = CURRENT_TIMESTAMP + ($2::bigint * INTERVAL '1 millisecond'),
                updated_at = CURRENT_TIMESTAMP
            FROM (
                SELECT id FROM plugin_entity_lifecycle_outbox
                WHERE status = 'pending' AND next_attempt_at <= CURRENT_TIMESTAMP
                ORDER BY next_attempt_at, id
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            ) due
            WHERE o.id = due.id
            RETURNING o.id, o.tenant_id, o.plugin_id, o.contribution_id, o.entity, o.reason,
                      o.server_msg_id, o.attempts, o.unconfirmed,
                      (EXTRACT(EPOCH FROM o.created_at) * 1000)::bigint
            "#,
        )
        .bind(i64::from(limit))
        .bind(interval_ms(lease))
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    tenant_id,
                    plugin_id,
                    contribution_id,
                    entity,
                    reason,
                    server_msg_id,
                    attempts,
                    unconfirmed,
                    created_at_ms,
                )| LifecycleTask {
                    id,
                    tenant_id,
                    plugin_id,
                    contribution_id,
                    entity,
                    reason,
                    server_msg_id,
                    attempts,
                    unconfirmed,
                    created_at_ms,
                },
            )
            .collect())
    }

    async fn complete(&self, id: i64, outcome: LifecycleOutcome, note: Option<&str>) -> Result<()> {
        sqlx::query(
            "UPDATE plugin_entity_lifecycle_outbox \
             SET status = $2, last_error = COALESCE($3, last_error), updated_at = CURRENT_TIMESTAMP \
             WHERE id = $1 AND status = 'pending'",
        )
        .bind(id)
        .bind(outcome.as_str())
        .bind(note.map(bounded))
        .execute(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        Ok(())
    }

    async fn retry_after(&self, id: i64, delay: Duration, error: &str) -> Result<()> {
        sqlx::query(
            "UPDATE plugin_entity_lifecycle_outbox \
             SET next_attempt_at = CURRENT_TIMESTAMP + ($2::bigint * INTERVAL '1 millisecond'), \
                 last_error = $3, updated_at = CURRENT_TIMESTAMP \
             WHERE id = $1 AND status = 'pending'",
        )
        .bind(id)
        .bind(interval_ms(delay))
        .bind(bounded(error))
        .execute(self.pool.as_ref())
        .await
        .map_err(unavailable)?;
        Ok(())
    }
}
