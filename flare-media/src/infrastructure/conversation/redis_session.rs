use std::sync::Arc;

use crate::error::{ErrorCode, Result, map_infra_error};
use chrono::{Duration, Utc};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use tokio::sync::Mutex;

use crate::domain::model::{UploadSession, UploadSessionStatus};
use crate::domain::repository::UploadSessionStore;

#[derive(Clone)]
pub struct RedisUploadSessionStore {
    namespace: String,
    ttl: Duration,
    connection: Arc<Mutex<ConnectionManager>>,
}

impl RedisUploadSessionStore {
    pub async fn new(
        redis_url: &str,
        namespace: impl Into<String>,
        ttl_seconds: i64,
    ) -> Result<Self> {
        let client = redis::Client::open(redis_url)
            .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, "redis client open"))?;
        let connection = client
            .get_connection_manager()
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, "redis connection manager"))?;
        Ok(Self {
            namespace: namespace.into(),
            ttl: Duration::seconds(ttl_seconds.max(60)),
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    fn key(&self, upload_id: &str) -> String {
        format!("{}:{}", self.namespace, upload_id)
    }

    fn ensure_session_defaults(session: &mut UploadSession) {
        let now = Utc::now();
        if session.created_at.timestamp() == 0 {
            session.created_at = now;
        }
        session.updated_at = now;
        if session.status == UploadSessionStatus::Completed
            || session.status == UploadSessionStatus::Aborted
        {
            session.expires_at = now + Duration::hours(1);
        }
    }

    fn ttl_for_session(&self, session: &UploadSession) -> u64 {
        let now = Utc::now();
        let diff = (session.expires_at - now).num_seconds();
        let clamped = diff.max(60).min(self.ttl.num_seconds());
        clamped as u64
    }
}

#[async_trait::async_trait]
impl UploadSessionStore for RedisUploadSessionStore {
    async fn create_session(&self, session: &UploadSession) -> Result<()> {
        let mut session = session.clone();
        Self::ensure_session_defaults(&mut session);
        let payload = serde_json::to_string(&session).map_err(|e| {
            map_infra_error(e, ErrorCode::SerializationError, "serialize upload session")
        })?;
        let mut conn = self.connection.lock().await;
        let _: () = conn
            .set_ex(
                self.key(&session.upload_id),
                payload,
                self.ttl_for_session(&session),
            )
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::NetworkError, "failed to create upload session in redis")
            })?;
        Ok(())
    }

    async fn get_session(&self, upload_id: &str) -> Result<Option<UploadSession>> {
        let mut conn = self.connection.lock().await;
        let payload: Option<String> = conn
            .get(self.key(upload_id))
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::NetworkError,
                    "failed to fetch upload session from redis",
                )
            })?;
        if let Some(payload) = payload {
            let session: UploadSession = serde_json::from_str(&payload).map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::DeserializationError,
                    "failed to deserialize upload session",
                )
            })?;
            if session.expires_at < Utc::now() {
                drop(conn);
                self.delete_session(upload_id).await.ok();
                return Ok(None);
            }
            Ok(Some(session))
        } else {
            Ok(None)
        }
    }

    async fn upsert_session(&self, session: &UploadSession) -> Result<()> {
        let mut session = session.clone();
        Self::ensure_session_defaults(&mut session);
        let payload = serde_json::to_string(&session).map_err(|e| {
            map_infra_error(e, ErrorCode::SerializationError, "serialize upload session")
        })?;
        let mut conn = self.connection.lock().await;
        let _: () = conn
            .set_ex(
                self.key(&session.upload_id),
                payload,
                self.ttl_for_session(&session),
            )
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::NetworkError, "failed to upsert upload session in redis")
            })?;
        Ok(())
    }

    async fn delete_session(&self, upload_id: &str) -> Result<()> {
        let mut conn = self.connection.lock().await;
        let _: () = conn
            .del(self.key(upload_id))
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::NetworkError,
                    "failed to delete upload session from redis",
                )
            })?;
        Ok(())
    }
}
