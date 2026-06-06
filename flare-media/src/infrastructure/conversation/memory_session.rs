use std::collections::HashMap;
use std::sync::Arc;

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
    sessions: Arc<RwLock<HashMap<String, UploadSession>>>,
}

impl MemoryUploadSessionStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl UploadSessionStore for MemoryUploadSessionStore {
    async fn create_session(&self, session: &UploadSession) -> Result<()> {
        let mut guard = self.sessions.write().await;
        guard.insert(session.upload_id.clone(), session.clone());
        Ok(())
    }

    async fn get_session(&self, upload_id: &str) -> Result<Option<UploadSession>> {
        let guard = self.sessions.read().await;
        Ok(guard.get(upload_id).cloned())
    }

    async fn upsert_session(&self, session: &UploadSession) -> Result<()> {
        let mut guard = self.sessions.write().await;
        guard.insert(session.upload_id.clone(), session.clone());
        Ok(())
    }

    async fn delete_session(&self, upload_id: &str) -> Result<()> {
        let mut guard = self.sessions.write().await;
        guard.remove(upload_id);
        Ok(())
    }
}
