//! PG owns operator commands; Redis indexes are conditional projections. Completion
//! requires the addressed gateway's acknowledgement, never PUBLISH's subscriber count.
use super::*;
use flare_im_service_kit::audit_effects::AuditEffects;
use flare_server_core::error::FlareError;
use serde_json::Value;
use sqlx::Row;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Target {
    app: String,
    user: String,
    record: RedisConnectionRecord,
    reason: String,
}
impl RedisConversationRepository {
    pub fn with_command_pool(mut self, pool: Arc<sqlx::PgPool>) -> Self {
        self.commands = Some(pool);
        self
    }
    pub(super) async fn prepare_command(
        &self,
        app: &str,
        user: &str,
        record: &RedisConnectionRecord,
        reason: &str,
        evidence: &str,
    ) -> Result<String> {
        let pool = self
            .commands
            .as_ref()
            .ok_or_else(|| FlareError::system("operator command store unavailable"))?;
        let evidence: Value = serde_json::from_str(evidence)
            .map_err(|_| FlareError::system("invalid kick evidence"))?;
        let work=async {
            let mut tx=pool.begin().await?;
            sqlx::query("SET LOCAL lock_timeout='250ms'").execute(&mut *tx).await?;
            sqlx::query("SET LOCAL statement_timeout='2s'").execute(&mut *tx).await?;
            let id=uuid::Uuid::new_v4().to_string();
            let target=serde_json::to_value(Target{app:app.into(),user:user.into(),record:record.clone(),reason:reason.into()}).map_err(|e|sqlx::Error::Encode(Box::new(e)))?;
            let id:String=sqlx::query_scalar("INSERT INTO online_kick_commands(id,app_id,request_id,physical_id,target,evidence) VALUES($1::uuid,$2,$3,$4,$5,$6) ON CONFLICT(app_id,request_id,physical_id) DO UPDATE SET id=online_kick_commands.id RETURNING id::text")
                .bind(id).bind(app).bind(evidence["request_id"].as_str().unwrap_or_default()).bind(&record.connection_id).bind(target).bind(&evidence).fetch_one(&mut *tx).await?;
            tx.commit().await?;
            Ok::<_,sqlx::Error>(id)
        }.await;
        work.map_err(|_| FlareError::system("kick command transaction unavailable"))
    }
    pub async fn advance_command(&self, id: &str) -> Result<bool> {
        let pool = self
            .commands
            .as_ref()
            .ok_or_else(|| FlareError::system("operator command store unavailable"))?;
        let work=async {
            // Targets/evidence are immutable. Network delivery must never hold the
            // source transaction or its row lock; concurrent exact-target retries
            // are harmless, and completion is fenced below after delivery.
            let row=sqlx::query("SELECT target,completed_at FROM online_kick_commands WHERE id=$1::uuid").bind(id).fetch_one(&**pool).await?;
            if row.try_get::<Option<DateTime<Utc>>,_>("completed_at")?.is_some() {return Ok::<_,sqlx::Error>(true)}
            let target:Target=serde_json::from_value(row.try_get("target")?).map_err(|e|sqlx::Error::Decode(Box::new(e)))?;
            let completed=matches!(tokio::time::timeout(Duration::from_secs(2),self.confirm_target(id,&target)).await,Ok(Ok(())));
            let mut tx=pool.begin().await?;
            sqlx::query("SET LOCAL lock_timeout='250ms'").execute(&mut *tx).await?;
            sqlx::query("SET LOCAL statement_timeout='2s'").execute(&mut *tx).await?;
            let row=sqlx::query("SELECT app_id,target,evidence,completed_at FROM online_kick_commands WHERE id=$1::uuid FOR UPDATE").bind(id).fetch_one(&mut *tx).await?;
            if row.try_get::<Option<DateTime<Utc>>,_>("completed_at")?.is_some() {return Ok::<_,sqlx::Error>(true)}
            if completed {
                let mut evidence:Value=row.try_get("evidence")?;
                evidence["at_ms"]=chrono::Utc::now().timestamp_millis().into();
                sqlx::query("INSERT INTO core_audit_outbox(event_id,app_id,payload) VALUES($1::uuid,$2,$3)").bind(id).bind(&target.app).bind(evidence).execute(&mut *tx).await?;
                sqlx::query("UPDATE online_kick_commands SET completed_at=clock_timestamp() WHERE id=$1::uuid").bind(id).execute(&mut *tx).await?;
            } else {
                sqlx::query("UPDATE online_kick_commands SET attempts=LEAST(attempts+1,1000000),next_at=clock_timestamp()+make_interval(secs=>LEAST(60,power(2,LEAST(attempts,6)))::double precision) WHERE id=$1::uuid").bind(id).execute(&mut *tx).await?;
            }
            tx.commit().await?;
            Ok(completed)
        }.await;
        work.map_err(|_| FlareError::system("kick completion transaction unavailable"))
    }
    async fn confirm_target(&self, id: &str, t: &Target) -> Result<()> {
        let mut kick = ConnectionManager::new(self.kick_client.as_ref().clone()).await?;
        let receipt_key = format!("{}:receipt:{id}", self.config.kick_channel);
        let signal = serde_json::json!({"tenant_id":t.app,"user_id":t.user,"device_id":t.record.device_id,"connection_id":t.record.connection_id,"reason":t.reason,"command_id":id,"target_gateway":t.record.gateway_id});
        let _: i64 = kick
            .publish(
                &self.config.kick_channel,
                serde_json::to_string(&signal)
                    .map_err(|_| FlareError::system("invalid kick target"))?,
            )
            .await?;
        for _ in 0..30 {
            let receipt: Option<String> = kick.get(&receipt_key).await?;
            if receipt.as_deref() == Some(t.record.connection_id.as_str()) {
                let mut conn = self.connection().await?;
                let _: Option<String> = redis::Script::new(REMOVE_FOR_KICK)
                    .key(Self::connection_key(&t.app, &t.user))
                    .key(Self::tenant_sessions_key(&t.app))
                    .arg(&t.record.conversation_id)
                    .arg(&t.app)
                    .arg(tenant_session_member_prefix(&t.user))
                    .arg(&t.record.connection_id)
                    .invoke_async(&mut conn)
                    .await?;
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        Err(FlareError::system("physical target confirmation pending"))
    }
    pub async fn run_commands(
        self: Arc<Self>,
        pool: Arc<sqlx::PgPool>,
        publisher: AuditEffects,
        shutdown: impl std::future::Future<Output = ()>,
    ) {
        tokio::pin!(shutdown);
        let delivery = async {
            if publisher.enabled() {
                publisher
                    .run_durable(pool.clone(), std::future::pending())
                    .await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(delivery);
        loop {
            tokio::select! {
                _=&mut shutdown=>break,
                _=&mut delivery=>break,
                _=tokio::time::sleep(Duration::from_millis(100))=>{
                    let result=tokio::time::timeout(Duration::from_secs(3),async {
                        let id:Option<String>=sqlx::query_scalar("SELECT id::text FROM online_kick_commands WHERE completed_at IS NULL AND next_at<=clock_timestamp() ORDER BY next_at LIMIT 1").fetch_optional(&*pool).await?;
                        if let Some(id)=id {let _=self.advance_command(&id).await;}
                        Ok::<_,sqlx::Error>(())
                    }).await;
                    if !matches!(result,Ok(Ok(()))) {tracing::warn!("operator kick reconciliation deferred");}
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "kick_commands_pg_tests.rs"]
mod functional;
