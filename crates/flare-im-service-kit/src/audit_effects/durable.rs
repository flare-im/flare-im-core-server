//! Source-owned evidence: enqueue in the business transaction, publish after commit.
//! Attribution is an awaited task scope, never an unsigned actor or a spawned child task.
use super::{AuditEffects, EFFECT_EVENT, EffectEvent, SUBJECT_PREFIX, now_ms};
use flare_server_core::{
    SignedActorType, VerifiedContext,
    context::{Context, Ctx},
    mq::producer::Producer,
};
use sqlx::{PgConnection, PgPool, Row};
use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

#[derive(Clone)]
struct Attribution {
    request_id: String,
    app: String,
    actor: String,
    permission: Option<String>,
    source: &'static str,
}
tokio::task_local! { static STAFF: Attribution; }

/// Not authorization: the normal verified RPC permission/scope checks must run first.
pub trait StaffAudit: Future + Sized {
    fn staff_audit(self, ctx: &Ctx, source: &'static str) -> impl Future<Output = Self::Output> {
        let attribution = ctx
            .get_data::<VerifiedContext>()
            .and_then(VerifiedContext::claims)
            .filter(|claims| claims.actor_type == SignedActorType::Staff)
            .map(|claims| Attribution {
                request_id: claims.request_id.clone(),
                app: claims.tenant_id.clone(),
                actor: claims.actor_id.clone(),
                permission: claims.permission.clone(),
                source,
            });
        async move {
            match attribution {
                Some(attribution) => STAFF.scope(attribution, self).await,
                None => self.await,
            }
        }
    }
}
impl<F: Future> StaffAudit for F {}

/// A missing Staff scope is an ordinary non-Staff command, not an invented operator.
/// An attribution mismatch fails the *same* transaction closed.
pub async fn record(
    tx: &mut PgConnection,
    app: &str,
    aggregate: &'static str,
    id: &str,
    action: &'static str,
) -> sqlx::Result<()> {
    let Some(payload) = pending_evidence(app, aggregate, id, action)? else {
        return Ok(());
    };
    sqlx::query(
        "INSERT INTO core_audit_outbox(event_id,app_id,payload) VALUES($1::uuid,$2,$3::jsonb)",
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(app)
    .bind(payload)
    .execute(tx)
    .await?;
    Ok(())
}

/// Snapshot attribution for an owned durable command. This is NOT an effect yet:
/// the owner inserts it into the outbox only in its confirmed-completion transaction.
pub fn pending_evidence(
    app: &str,
    aggregate: &'static str,
    id: &str,
    action: &'static str,
) -> sqlx::Result<Option<String>> {
    let Ok(actor) = STAFF.try_with(Clone::clone) else {
        return Ok(None);
    };
    if actor.app != app || app.is_empty() || app == "*" {
        return Err(sqlx::Error::Protocol("audit application mismatch".into()));
    }
    let payload = serde_json::to_string(&EffectEvent {
        event: EFFECT_EVENT,
        request_id: &actor.request_id,
        at_ms: now_ms(),
        scope: app,
        source: actor.source,
        actor_id: &actor.actor,
        actor_type: "staff",
        permission: actor.permission.as_deref(),
        aggregate,
        aggregate_id: id,
        action,
        outcome: "committed",
    })
    .map_err(|error| sqlx::Error::Encode(Box::new(error)))?;
    Ok(Some(payload))
}

#[async_trait::async_trait]
pub trait EvidencePublisher: Send + Sync {
    /// Success means the broker acknowledged the stable event, not merely accepted a task.
    async fn publish(&self, id: &str, app: &str, payload: Vec<u8>) -> Result<(), ()>;
}
#[async_trait::async_trait]
impl EvidencePublisher for AuditEffects {
    async fn publish(&self, id: &str, app: &str, payload: Vec<u8>) -> Result<(), ()> {
        let producer = self.producer.as_ref().ok_or(())?;
        let ctx: Ctx = Arc::new(Context::with_request_id(id));
        producer
            .send(
                &ctx,
                &format!("{SUBJECT_PREFIX}.{app}"),
                Some(id),
                payload,
                Some(HashMap::from([(
                    "x-message-id".into(),
                    format!("audit:{id}"),
                )])),
            )
            .await
            .map_err(|_| ())
    }
}

/// One row per bounded attempt. Cancellation/crash rolls the transaction back;
/// redelivery preserves the event ID and the exact stored wire bytes.
pub async fn drain_one(pool: &PgPool, publisher: &dyn EvidencePublisher) -> sqlx::Result<bool> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET LOCAL statement_timeout='2s'")
        .execute(&mut *tx)
        .await?;
    let row = sqlx::query("SELECT event_id::text AS event_id,app_id,payload::text AS payload FROM core_audit_outbox WHERE delivered_at IS NULL AND next_at<=clock_timestamp() ORDER BY next_at,event_id LIMIT 1 FOR UPDATE SKIP LOCKED")
        .fetch_optional(&mut *tx).await?;
    let Some(row) = row else { return Ok(false) };
    let id: String = row.get("event_id");
    let app: String = row.get("app_id");
    let payload: String = row.get("payload");
    if matches!(
        tokio::time::timeout(
            Duration::from_secs(2),
            publisher.publish(&id, &app, payload.into_bytes())
        )
        .await,
        Ok(Ok(()))
    ) {
        sqlx::query(
            "UPDATE core_audit_outbox SET delivered_at=clock_timestamp() WHERE event_id=$1::uuid",
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;
    } else {
        sqlx::query("UPDATE core_audit_outbox SET attempts=LEAST(attempts+1,1000000),next_at=clock_timestamp()+make_interval(secs=>LEAST(60,power(2,LEAST(attempts,6)))::double precision) WHERE event_id=$1::uuid")
            .bind(id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(true)
}

impl AuditEffects {
    pub async fn run_durable(self, pool: Arc<PgPool>, shutdown: impl Future<Output = ()>) {
        tokio::pin!(shutdown);
        let mut cleanup = tokio::time::Instant::now();
        loop {
            let work = tokio::select! {
                _=&mut shutdown => break,
                result=tokio::time::timeout(Duration::from_secs(3),drain_one(&pool,&self)) => {
                    if !matches!(result, Ok(Ok(_))) { tracing::warn!("durable Core audit dispatch deferred"); }
                    matches!(result, Ok(Ok(true)))
                }
            };
            if tokio::time::Instant::now() >= cleanup {
                // Never expire pending evidence. Bound the maintenance batch and wall time.
                tokio::select! {
                    _=&mut shutdown => break,
                    _=tokio::time::timeout(Duration::from_secs(2), sqlx::query("DELETE FROM core_audit_outbox WHERE event_id IN (SELECT event_id FROM core_audit_outbox WHERE delivered_at<clock_timestamp()-interval '30 days' LIMIT 256)").execute(&*pool)) => {}
                }
                cleanup = tokio::time::Instant::now() + Duration::from_secs(3600);
            }
            if work {
                tokio::task::yield_now().await;
            } else {
                tokio::select! { _=&mut shutdown => break, _=tokio::time::sleep(Duration::from_millis(100)) => {} }
            }
        }
    }
}
