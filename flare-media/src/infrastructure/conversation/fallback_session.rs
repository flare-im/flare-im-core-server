use std::sync::Arc;

use tracing::warn;

use crate::domain::model::UploadSession;
use crate::domain::repository::UploadSessionStore;
use crate::error::Result;

/// Redis 优先、内存兜底的上传会话存储。
///
/// 行为：
/// - create/upsert/delete 先走 primary，失败则降级到 fallback；
/// - get 优先 primary，若 primary 返回 None 或报错，则尝试 fallback。
pub struct FallbackUploadSessionStore {
    primary: Arc<dyn UploadSessionStore>,
    fallback: Arc<dyn UploadSessionStore>,
}

impl FallbackUploadSessionStore {
    pub fn new(primary: Arc<dyn UploadSessionStore>, fallback: Arc<dyn UploadSessionStore>) -> Self {
        Self { primary, fallback }
    }
}

#[async_trait::async_trait]
impl UploadSessionStore for FallbackUploadSessionStore {
    async fn create_session(&self, session: &UploadSession) -> Result<()> {
        if let Err(err) = self.primary.create_session(session).await {
            warn!(
                error = %err,
                upload_id = %session.upload_id,
                "primary upload session store create failed, fallback to secondary store"
            );
            return self.fallback.create_session(session).await;
        }
        Ok(())
    }

    async fn get_session(&self, upload_id: &str) -> Result<Option<UploadSession>> {
        match self.primary.get_session(upload_id).await {
            Ok(Some(session)) => Ok(Some(session)),
            Ok(None) => self.fallback.get_session(upload_id).await,
            Err(err) => {
                warn!(
                    error = %err,
                    upload_id = upload_id,
                    "primary upload session store get failed, fallback to secondary store"
                );
                self.fallback.get_session(upload_id).await
            }
        }
    }

    async fn upsert_session(&self, session: &UploadSession) -> Result<()> {
        if let Err(err) = self.primary.upsert_session(session).await {
            warn!(
                error = %err,
                upload_id = %session.upload_id,
                "primary upload session store upsert failed, fallback to secondary store"
            );
            return self.fallback.upsert_session(session).await;
        }
        Ok(())
    }

    async fn delete_session(&self, upload_id: &str) -> Result<()> {
        let primary_result = self.primary.delete_session(upload_id).await;
        let fallback_result = self.fallback.delete_session(upload_id).await;

        if let Err(err) = primary_result {
            warn!(
                error = %err,
                upload_id = upload_id,
                "primary upload session store delete failed, secondary delete result will be used"
            );
        }

        fallback_result
    }
}
