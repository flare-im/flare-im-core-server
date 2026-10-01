use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::RwLock;

use crate::domain::model::UploadSession;
use crate::domain::repository::UploadSessionStore;
use flare_server_core::error::Result;

/// 进程内上传会话存储（开发/单机兜底）。
///
/// 说明：
/// - 仅用于未配置 Redis 的场景；
/// - 服务重启后会话会丢失，不适合多副本生产环境。
#[derive(Default)]
pub struct MemoryUploadSessionStore {
    sessions: Arc<RwLock<HashMap<String, Entry>>>,
}

struct Entry {
    session: UploadSession,
    /// 可被回收领取的时刻：写入时为会话过期时刻，领取后为租期。
    claimable_after: DateTime<Utc>,
}

impl Entry {
    fn new(session: &UploadSession) -> Self {
        Self {
            claimable_after: session.expires_at,
            session: session.clone(),
        }
    }
}

impl MemoryUploadSessionStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// 测试用：不论是否过期，读出存储里的记录本身。
    #[cfg(test)]
    pub(crate) async fn stored(&self, upload_id: &str) -> Option<UploadSession> {
        self.sessions
            .read()
            .await
            .get(upload_id)
            .map(|entry| entry.session.clone())
    }
}

#[async_trait::async_trait]
impl UploadSessionStore for MemoryUploadSessionStore {
    async fn create_session(&self, session: &UploadSession) -> Result<()> {
        let mut guard = self.sessions.write().await;
        guard.insert(session.upload_id.clone(), Entry::new(session));
        Ok(())
    }

    async fn get_session(&self, upload_id: &str) -> Result<Option<UploadSession>> {
        let guard = self.sessions.read().await;
        let now = Utc::now();
        Ok(guard
            .get(upload_id)
            .filter(|entry| entry.session.expires_at >= now)
            .map(|entry| entry.session.clone()))
    }

    async fn upsert_session(&self, session: &UploadSession) -> Result<()> {
        let mut guard = self.sessions.write().await;
        guard.insert(session.upload_id.clone(), Entry::new(session));
        Ok(())
    }

    async fn delete_session(&self, upload_id: &str) -> Result<()> {
        let mut guard = self.sessions.write().await;
        guard.remove(upload_id);
        Ok(())
    }

    async fn claim_expired_sessions(
        &self,
        expired_before: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<UploadSession>> {
        let mut guard = self.sessions.write().await;
        let mut claimed = Vec::new();
        for entry in guard.values_mut() {
            if claimed.len() >= limit {
                break;
            }
            if entry.claimable_after < expired_before {
                entry.claimable_after = lease_until;
                claimed.push(entry.session.clone());
            }
        }
        Ok(claimed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::UploadSessionStatus;
    use chrono::Duration;

    fn session(upload_id: &str, expires_at: DateTime<Utc>) -> UploadSession {
        UploadSession {
            upload_id: upload_id.to_string(),
            tenant_id: "tenant-a".to_string(),
            file_id: None,
            file_name: "a.txt".to_string(),
            mime_type: "text/plain".to_string(),
            file_type: "FILE_TYPE_FILE".to_string(),
            chunk_size: 1,
            total_size: Some(1),
            uploaded_size: 0,
            uploaded_chunks: Vec::new(),
            uploaded_parts: Vec::new(),
            transport_kind: None,
            storage_upload_id: None,
            bucket: None,
            object_key: None,
            single_part_upload_url: None,
            file_fingerprint: None,
            head_tail_sha256: None,
            full_sha256: None,
            total_parts: None,
            user_id: "owner".to_string(),
            namespace: None,
            business_tag: None,
            trace_id: None,
            metadata: HashMap::new(),
            status: UploadSessionStatus::Pending,
            expires_at,
            created_at: expires_at,
            updated_at: expires_at,
        }
    }

    /// 修复前内存兜底存储永不过期：过期会话仍能续传、完成，且从不释放。
    #[tokio::test]
    async fn expired_sessions_are_hidden_and_claimed_once_per_lease() {
        let store = MemoryUploadSessionStore::new();
        let now = Utc::now();
        store
            .create_session(&session("expired", now - Duration::minutes(10)))
            .await
            .unwrap();
        store
            .create_session(&session("live", now + Duration::minutes(10)))
            .await
            .unwrap();
        assert!(store.get_session("expired").await.unwrap().is_none());
        assert!(store.get_session("live").await.unwrap().is_some());

        let lease = now + Duration::minutes(5);
        let claimed = store.claim_expired_sessions(now, lease, 10).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].upload_id, "expired");
        assert!(
            store
                .claim_expired_sessions(now, lease, 10)
                .await
                .unwrap()
                .is_empty()
        );
        let again = store
            .claim_expired_sessions(lease + Duration::seconds(1), lease, 10)
            .await
            .unwrap();
        assert_eq!(again.len(), 1);
        store.delete_session("expired").await.unwrap();
        assert!(
            store
                .claim_expired_sessions(now + Duration::hours(1), now, 10)
                .await
                .unwrap()
                .iter()
                .all(|s| s.upload_id == "live")
        );
    }
}
