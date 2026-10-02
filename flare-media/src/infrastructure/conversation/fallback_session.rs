use std::sync::Arc;

use chrono::{DateTime, Utc};
use tracing::warn;

use crate::domain::model::UploadSession;
use crate::domain::repository::{SessionUpdate, UploadSessionStore};
use flare_server_core::error::Result;

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
    pub fn new(
        primary: Arc<dyn UploadSessionStore>,
        fallback: Arc<dyn UploadSessionStore>,
    ) -> Self {
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

    /// 与 get 相同：记录在 primary 就在 primary 上改，primary 没有或出错时改 fallback 里的。
    /// `apply` 的拒绝不是存储故障，原样返回，不再去 fallback 改一遍。
    async fn update_session(
        &self,
        upload_id: &str,
        apply: &SessionUpdate<'_>,
    ) -> Result<Option<UploadSession>> {
        let rejected = std::sync::atomic::AtomicBool::new(false);
        let tracked = |session: &mut UploadSession| {
            apply(session).inspect_err(|_| {
                rejected.store(true, std::sync::atomic::Ordering::Relaxed);
            })
        };
        match self.primary.update_session(upload_id, &tracked).await {
            Ok(Some(session)) => Ok(Some(session)),
            Ok(None) => self.fallback.update_session(upload_id, apply).await,
            Err(err) if rejected.load(std::sync::atomic::Ordering::Relaxed) => Err(err),
            Err(err) => {
                warn!(
                    error = %err,
                    upload_id = upload_id,
                    "primary upload session store update failed, fallback to secondary store"
                );
                // fallback 里也没有这条记录时报 primary 的错误，而不是「会话不存在」。
                match self.fallback.update_session(upload_id, apply).await? {
                    Some(session) => Ok(Some(session)),
                    None => Err(err),
                }
            }
        }
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

    /// 两边都领：primary 写失败时会话落在 fallback，同样要回收。primary 出错不挡 fallback。
    async fn claim_expired_sessions(
        &self,
        expired_before: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<UploadSession>> {
        let mut claimed = match self
            .primary
            .claim_expired_sessions(expired_before, lease_until, limit)
            .await
        {
            Ok(claimed) => claimed,
            Err(err) => {
                warn!(
                    error = %err,
                    "primary upload session store claim failed, reclaiming the secondary store only"
                );
                Vec::new()
            }
        };
        let remaining = limit.saturating_sub(claimed.len());
        if remaining > 0 {
            claimed.extend(
                self.fallback
                    .claim_expired_sessions(expired_before, lease_until, remaining)
                    .await?,
            );
        }
        Ok(claimed)
    }
}
