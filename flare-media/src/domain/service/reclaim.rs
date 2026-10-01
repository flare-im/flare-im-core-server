//! 过期上传会话的回收：会话占着的存储侧资源（单次 PUT 已写入的对象、未完成的分片上传、
//! 网关分块的本地目录）在会话过期后释放，记录随之删除。由 Media 进程里的定时任务驱动，
//! 多副本同时运行时靠会话存储的领取租约分工。
//!
//! 与完成直传的并发：完成在合并前先续期会话，续期后的会话领不到；领取后才续期的，释放前
//! 重读记录时会发现并跳过。过期会话对完成不可见，所以剩下的窗口只有一种：某次完成在会话
//! 过期前读到它，却在续期之前停顿了 5 分钟以上（宽限期），续期又恰好落在本任务重读之后。
use super::*;
use chrono::DateTime;

/// 会话过期后再等这么久才回收，不和临近过期才发起的完成请求（含大文件分片合并）抢对象。
pub const UPLOAD_SESSION_RECLAIM_GRACE_SECONDS: i64 = 300;
/// 一次领取的租期：释放失败或进程中途退出时，租期过后由任一副本重新领取。
pub const UPLOAD_SESSION_RECLAIM_LEASE_SECONDS: i64 = 300;
/// 每轮至多领取的会话数。
pub const UPLOAD_SESSION_RECLAIM_BATCH: usize = 100;
/// 定时任务的轮询间隔。
pub const UPLOAD_SESSION_RECLAIM_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// 一轮回收的结果，供日志与测试使用。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct UploadReclaimReport {
    /// 资源已释放、记录已删除的会话数。
    pub reclaimed: usize,
    /// 释放或删除失败、留待租期过后重试的会话数。
    pub failed: usize,
    /// 领取之后又被续期（例如完成直传开始合并）、这一轮不动的会话数。
    pub renewed: usize,
    /// 本机上没有对应活跃会话、已删除的过期分块目录数。
    pub stale_chunk_dirs: usize,
}

impl MediaService {
    /// 回收一轮：领取过期超过宽限期的会话，释放资源后删除记录；再清本机过期的分块目录。
    pub async fn reclaim_expired_upload_sessions(
        &self,
        now: DateTime<Utc>,
    ) -> Result<UploadReclaimReport> {
        let mut report = UploadReclaimReport::default();
        let Some(store) = &self.upload_conversation_store else {
            return Ok(report);
        };
        let expired_before = now - Duration::seconds(UPLOAD_SESSION_RECLAIM_GRACE_SECONDS);
        let sessions = store
            .claim_expired_sessions(
                expired_before,
                now + Duration::seconds(UPLOAD_SESSION_RECLAIM_LEASE_SECONDS),
                UPLOAD_SESSION_RECLAIM_BATCH,
            )
            .await?;
        for claimed in sessions {
            // 领到的是领取那一刻的快照。完成直传在开始合并前会续期会话，提交分片也会续期：
            // 释放前重读记录，续期过的不动（它不再过期，之后按新的过期时刻再回收）；其余按
            // 最新记录释放，已完成的会话、已登记的文件都不删对象。
            let session = match store.get_session_record(&claimed.upload_id).await {
                Ok(Some(current)) if current.expires_at >= expired_before => {
                    report.renewed += 1;
                    continue;
                }
                Ok(current) => current.unwrap_or(claimed),
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        upload_id = %claimed.upload_id,
                        "failed to re-read a claimed upload session; retrying after the lease"
                    );
                    report.failed += 1;
                    continue;
                }
            };
            let released = match self.release_upload_resources(&session).await {
                Ok(()) => store.delete_session(&session.upload_id).await,
                Err(err) => Err(err),
            };
            match released {
                Ok(()) => report.reclaimed += 1,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        upload_id = %session.upload_id,
                        "failed to reclaim expired upload session; retrying after the lease"
                    );
                    report.failed += 1;
                }
            }
        }
        report.stale_chunk_dirs = self.sweep_stale_chunk_dirs(now).await;
        Ok(report)
    }

    /// 释放一个会话占着的存储侧资源。
    pub(super) async fn release_upload_resources(&self, session: &UploadSession) -> Result<()> {
        let Some(kind) = session.transport_kind else {
            // 网关分块：分块目录只是中间数据，完成后本该删掉，任何状态下都可以删。
            return self.remove_chunk_dir(&session.upload_id).await;
        };
        let (Some(object_repo), Some(object_key)) =
            (self.object_repo.as_ref(), session.object_key.as_deref())
        else {
            return Ok(());
        };
        match kind {
            // abort 只作用于未完成的分片上传，已合并的对象不受影响，所以先 abort 再单独判断
            // 合并出的对象：完成时大小不符而没删掉的，或合并后登记失败的，都没有文件记录。
            DirectUploadTransportKind::MultipartPut => {
                if let Some(storage_upload_id) = session.storage_upload_id.as_deref() {
                    object_repo
                        .abort_multipart_upload(object_key, storage_upload_id)
                        .await?;
                }
                self.delete_unregistered_object(session, object_key).await
            }
            DirectUploadTransportKind::SinglePut => {
                self.delete_unregistered_object(session, object_key).await
            }
        }
    }

    /// 直传对象就是文件本身：会话已完成、或文件已经登记（完成时删会话失败留下的记录）时，
    /// 它属于那个文件，不能删。其余情况删除；对象不存在时 S3 的删除同样成功。
    async fn delete_unregistered_object(
        &self,
        session: &UploadSession,
        object_key: &str,
    ) -> Result<()> {
        if session.status == UploadSessionStatus::Completed
            || self.upload_persisted(session).await?
        {
            return Ok(());
        }
        let Some(object_repo) = self.object_repo.as_ref() else {
            return Ok(());
        };
        object_repo.delete_object(object_key).await
    }

    /// 会话对应的文件是否已登记。核对不了（没有元数据库、会话缺租户或 file_id）时按已登记
    /// 处理：宁可留下对象，也不删掉可能已经属于某个文件的数据。
    async fn upload_persisted(&self, session: &UploadSession) -> Result<bool> {
        let (Some(metadata_store), Some(file_id)) =
            (self.metadata_store.as_ref(), session.file_id.as_deref())
        else {
            return Ok(true);
        };
        if validate_tenant(&session.tenant_id).is_err() {
            return Ok(true);
        }
        let ctx = Context::root().with_tenant_id(&session.tenant_id);
        Ok(metadata_store.load_metadata(&ctx, file_id).await?.is_some())
    }

    async fn remove_chunk_dir(&self, upload_id: &str) -> Result<()> {
        // 不是规范 UUID 的 id 从来没有对应的目录，也绝不拿它拼路径。
        if self.session_dir(upload_id).is_err() {
            return Ok(());
        }
        self.cleanup_chunks(upload_id).await
    }

    /// 分块目录落在收到分块的那个副本本地，领到会话的副本未必有它；会话记录也可能早已
    /// 过期消失。所以每个副本各自清理本机目录：超过会话有效期加宽限期没有写入、且会话
    /// 存储里没有活跃会话的规范 UUID 目录才删。
    async fn sweep_stale_chunk_dirs(&self, now: DateTime<Utc>) -> usize {
        let idle_after = Duration::seconds(
            self.config.chunk_ttl_seconds.max(60) + UPLOAD_SESSION_RECLAIM_GRACE_SECONDS,
        );
        let Ok(mut entries) = fs::read_dir(&self.config.chunk_root_dir).await else {
            return 0;
        };
        let mut removed = 0;
        while let Ok(Some(entry)) = entries.next_entry().await {
            let Some(upload_id) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if self.session_dir(&upload_id).is_err() {
                continue;
            }
            let Ok(metadata) = entry.metadata().await else {
                continue;
            };
            let idle = metadata
                .modified()
                .ok()
                .map(DateTime::<Utc>::from)
                .is_some_and(|modified| modified + idle_after < now);
            if !metadata.is_dir() || !idle {
                continue;
            }
            if let Some(store) = &self.upload_conversation_store
                && !matches!(store.get_session(&upload_id).await, Ok(None))
            {
                // 会话仍有效（例如只重传了已有分块来续期），或会话存储暂时读不到：留着。
                continue;
            }
            match fs::remove_dir_all(entry.path()).await {
                Ok(()) => removed += 1,
                Err(err) => tracing::warn!(
                    error = %err,
                    upload_id = %upload_id,
                    "failed to remove stale chunk directory"
                ),
            }
        }
        removed
    }
}

#[cfg(test)]
#[path = "reclaim_tests.rs"]
mod tests;
