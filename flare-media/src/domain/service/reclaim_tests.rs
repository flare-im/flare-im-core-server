//! 直传大小约束与过期上传会话回收：真实领域编排，内存端口。存储侧按 signed headers 拒绝
//! 超长 PUT 由 S3 实现与存储负责（`s3.rs` 断言签名里有 content-length），这里覆盖完成时的
//! 兜底核对与回收任务。
use super::super::isolation_tests::{Metadata, ctx};
use super::*;
use crate::domain::repository::{MediaMetadataStore, MediaObjectRepository, UploadSessionStore};
use crate::infrastructure::conversation::memory_session::MemoryUploadSessionStore;
use flare_grpc_proto::media::UploadFileMetadata;
use std::sync::{Arc, Mutex};

const STORAGE_UPLOAD_ID: &str = "storage-upload-1";

/// 记录调用的私有桶：对象大小由测试按“客户端实际写了多少”设定。
#[derive(Default)]
struct RecordingBucket {
    objects: Mutex<HashMap<String, i64>>,
    deleted: Mutex<Vec<String>>,
    aborted: Mutex<Vec<(String, String)>>,
    /// (object_key, content_length, expires_in)
    presigned_puts: Mutex<Vec<(String, i64, i64)>>,
}

impl RecordingBucket {
    fn client_wrote(&self, key: &str, size: i64) {
        self.objects.lock().unwrap().insert(key.to_string(), size);
    }
    fn holds(&self, key: &str) -> bool {
        self.objects.lock().unwrap().contains_key(key)
    }
}

#[async_trait::async_trait]
impl MediaObjectRepository for RecordingBucket {
    async fn put_object(&self, _context: &UploadContext<'_>) -> Result<String> {
        Err(flare_server_core::flare_err!(
            ErrorCode::InternalError,
            "direct upload never streams through Media"
        ))
    }
    async fn delete_object(&self, object_path: &str) -> Result<()> {
        self.objects.lock().unwrap().remove(object_path);
        self.deleted.lock().unwrap().push(object_path.to_string());
        Ok(())
    }
    async fn presign_object(&self, object_path: &str, _expires_in: i64) -> Result<String> {
        Ok(format!(
            "https://bucket.test/{object_path}?X-Amz-Signature=get"
        ))
    }
    async fn presign_put_object(
        &self,
        object_path: &str,
        _content_type: &str,
        content_length: i64,
        expires_in: i64,
    ) -> Result<String> {
        self.presigned_puts.lock().unwrap().push((
            object_path.to_string(),
            content_length,
            expires_in,
        ));
        Ok(format!(
            "https://bucket.test/{object_path}?X-Amz-Signature=put"
        ))
    }
    async fn create_multipart_upload(
        &self,
        _object_path: &str,
        _content_type: &str,
    ) -> Result<String> {
        Ok(STORAGE_UPLOAD_ID.to_string())
    }
    async fn presign_upload_part(
        &self,
        object_path: &str,
        _upload_id: &str,
        part_number: u32,
        _expires_in: i64,
    ) -> Result<String> {
        Ok(format!(
            "https://bucket.test/{object_path}?partNumber={part_number}"
        ))
    }
    async fn complete_multipart_upload(
        &self,
        _object_path: &str,
        _upload_id: &str,
        _parts: &[UploadedPartRecord],
    ) -> Result<()> {
        // 合并后的对象大小由测试事先用 client_wrote 设定（分片地址不绑定长度）。
        Ok(())
    }
    async fn abort_multipart_upload(&self, object_path: &str, upload_id: &str) -> Result<()> {
        self.aborted
            .lock()
            .unwrap()
            .push((object_path.to_string(), upload_id.to_string()));
        Ok(())
    }
    async fn stat_object(&self, object_path: &str) -> Result<ObjectStat> {
        let size = self.objects.lock().unwrap().get(object_path).copied();
        size.map(|size| ObjectStat {
            size: Some(size),
            etag: None,
        })
        .ok_or_else(|| {
            flare_server_core::flare_err!(ErrorCode::NetworkError, "failed to stat object")
        })
    }
    fn base_url(&self) -> Option<String> {
        Some("https://bucket.test".to_string())
    }
    fn cdn_base_url(&self) -> Option<String> {
        None
    }
    fn use_presigned_urls(&self) -> bool {
        true
    }
    fn bucket_name(&self) -> Option<String> {
        Some("media".to_string())
    }
}

/// 会话有效期（秒）；单次 PUT 地址的默认有效期刻意比它长。
const SESSION_TTL: i64 = 600;
const DEFAULT_URL_TTL: i64 = 3600;

struct Fixture {
    service: MediaService,
    bucket: Arc<RecordingBucket>,
    metadata: Arc<Metadata>,
    sessions: Arc<MemoryUploadSessionStore>,
    chunks: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let bucket = Arc::new(RecordingBucket::default());
        let metadata = Arc::new(Metadata::default());
        let sessions = Arc::new(MemoryUploadSessionStore::new());
        let chunks = std::env::temp_dir().join(format!("flare-media-reclaim-{}", Uuid::new_v4()));
        let service = MediaService::new(
            Some(bucket.clone()),
            Some(metadata.clone()),
            None,
            None,
            Some(sessions.clone()),
            None,
            MediaDomainConfig::new(
                DEFAULT_URL_TTL,
                None,
                60,
                chunks.clone(),
                SESSION_TTL,
                8 * 1024 * 1024,
            ),
        );
        Self {
            service,
            bucket,
            metadata,
            sessions,
            chunks,
        }
    }

    async fn initiate(&self, size: i64) -> DirectUploadSessionState {
        self.service
            .initiate_direct_upload(
                &owner(),
                &UploadFileMetadata {
                    file_name: "photo.png".to_string(),
                    mime_type: "image/png".to_string(),
                    file_size: size,
                    ..Default::default()
                },
                8 * 1024 * 1024,
                None,
                None,
                None,
            )
            .await
            .unwrap()
    }

    /// 回收时刻：会话有效期加宽限期之后。
    fn after_expiry(&self) -> chrono::DateTime<Utc> {
        Utc::now() + Duration::seconds(SESSION_TTL + UPLOAD_SESSION_RECLAIM_GRACE_SECONDS + 5)
    }

    async fn stored_session(&self, upload_id: &str) -> Option<UploadSession> {
        self.sessions.stored(upload_id).await
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.chunks);
    }
}

fn owner() -> Context {
    ctx("tenant-a", "owner")
}

#[tokio::test]
async fn single_put_url_signs_the_declared_size_and_never_outlives_the_session() {
    let f = Fixture::new();
    let state = f.initiate(5).await;
    assert_eq!(state.transport_kind, DirectUploadTransportKind::SinglePut);
    let presigned = f.bucket.presigned_puts.lock().unwrap().clone();
    assert_eq!(presigned.len(), 1);
    let (key, content_length, expires_in) = &presigned[0];
    assert_eq!(key, &state.object_key);
    assert_eq!(*content_length, 5);
    assert!(
        *expires_in <= SESSION_TTL && *expires_in > 0,
        "single PUT url lives {expires_in}s, session {SESSION_TTL}s"
    );
}

#[tokio::test]
async fn completion_refuses_a_single_put_larger_than_declared_and_deletes_it() {
    let f = Fixture::new();
    let state = f.initiate(5).await;
    // 存储不校验 signed headers 时，客户端能写进比声明大的对象。
    f.bucket.client_wrote(&state.object_key, 50 * 1024 * 1024);
    let err = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
    assert!(!f.bucket.holds(&state.object_key));
    assert!(
        f.metadata
            .load_metadata(&owner(), &state.file_id)
            .await
            .unwrap()
            .is_none()
    );
    // 会话还在：在地址有效期内按声明大小重传即可完成。
    f.bucket.client_wrote(&state.object_key, 5);
    let saved = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap();
    assert_eq!(saved.file_size, 5);
    assert!(f.bucket.holds(&state.object_key));
}

#[tokio::test]
async fn completion_refuses_a_merged_multipart_object_of_another_size() {
    let f = Fixture::new();
    let declared = 20 * 1024 * 1024;
    let state = f.initiate(declared).await;
    assert_eq!(
        state.transport_kind,
        DirectUploadTransportKind::MultipartPut
    );
    let parts: Vec<UploadedPartRecord> = (1..=state.total_parts)
        .map(|part_number| UploadedPartRecord {
            part_number,
            etag: format!("etag-{part_number}"),
            size: state.part_size,
            sha256: None,
        })
        .collect();
    f.service
        .commit_direct_upload_parts(&owner(), &state.upload_id, &parts)
        .await
        .unwrap();
    f.bucket.client_wrote(&state.object_key, declared * 3);
    let err = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
    assert!(!f.bucket.holds(&state.object_key));
    assert!(
        f.metadata
            .load_metadata(&owner(), &state.file_id)
            .await
            .unwrap()
            .is_none()
    );
    // 已合并的分片上传无法重来，会话随之结束。
    assert!(f.stored_session(&state.upload_id).await.is_none());
}

#[tokio::test]
async fn aborting_a_single_put_deletes_the_object_already_written() {
    let f = Fixture::new();
    let state = f.initiate(5).await;
    f.bucket.client_wrote(&state.object_key, 5);
    f.service
        .abort_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap();
    assert!(!f.bucket.holds(&state.object_key));
    assert!(f.stored_session(&state.upload_id).await.is_none());
}

#[tokio::test]
async fn expired_sessions_release_storage_and_their_records() {
    let f = Fixture::new();
    // 单次 PUT 写了对象却没完成。
    let abandoned_put = f.initiate(5).await;
    f.bucket.client_wrote(&abandoned_put.object_key, 5);
    // 分片上传开了没完成。
    let abandoned_multipart = f.initiate(20 * 1024 * 1024).await;
    // 已完成登记、但删会话失败留下的记录：对象属于文件，不能删。
    let persisted = f.initiate(5).await;
    f.bucket.client_wrote(&persisted.object_key, 5);
    f.service
        .complete_direct_upload(&owner(), &persisted.upload_id)
        .await
        .unwrap();
    let mut leftover = f.stored_session(&abandoned_put.upload_id).await.unwrap();
    leftover.upload_id = persisted.upload_id.clone();
    leftover.file_id = Some(persisted.file_id.clone());
    leftover.object_key = Some(persisted.object_key.clone());
    f.sessions.upsert_session(&leftover).await.unwrap();
    // 网关分块：本机目录里有分块。
    let chunked = f
        .service
        .initiate_multipart_upload(
            &owner(),
            MultipartUploadInit {
                file_name: "x.txt".to_string(),
                mime_type: "text/plain".to_string(),
                file_type: "file".to_string(),
                file_size: Some(1),
                chunk_size: 256 * 1024,
                user_id: "owner".to_string(),
                namespace: None,
                business_tag: None,
                trace_id: None,
                metadata: HashMap::new(),
            },
        )
        .await
        .unwrap();
    f.service
        .upload_multipart_chunk(
            &owner(),
            MultipartChunkPayload {
                upload_id: chunked.upload_id.clone(),
                chunk_index: 0,
                bytes: vec![1],
            },
        )
        .await
        .unwrap();
    let chunk_dir = f.service.session_dir(&chunked.upload_id).unwrap();
    assert!(chunk_dir.exists());

    // 还在有效期内：什么都不动。
    let early = f
        .service
        .reclaim_expired_upload_sessions(Utc::now())
        .await
        .unwrap();
    assert_eq!(early, UploadReclaimReport::default());
    assert!(f.bucket.holds(&abandoned_put.object_key));

    let report = f
        .service
        .reclaim_expired_upload_sessions(f.after_expiry())
        .await
        .unwrap();
    assert_eq!(report.reclaimed, 4, "{report:?}");
    assert_eq!(report.failed, 0, "{report:?}");
    assert!(!f.bucket.holds(&abandoned_put.object_key));
    assert_eq!(
        f.bucket.aborted.lock().unwrap().clone(),
        vec![(
            abandoned_multipart.object_key.clone(),
            STORAGE_UPLOAD_ID.to_string()
        )]
    );
    assert!(f.bucket.holds(&persisted.object_key));
    assert!(!chunk_dir.exists());
    for upload_id in [
        &abandoned_put.upload_id,
        &abandoned_multipart.upload_id,
        &persisted.upload_id,
        &chunked.upload_id,
    ] {
        assert!(f.stored_session(upload_id).await.is_none(), "{upload_id}");
    }
    // 第二轮没有可回收的。
    let again = f
        .service
        .reclaim_expired_upload_sessions(f.after_expiry())
        .await
        .unwrap();
    assert_eq!(again.reclaimed, 0);
}

/// 存储暂时不可用：会话留着，租期过后重新领取并完成回收。
#[tokio::test]
async fn failed_release_keeps_the_session_for_a_later_round() {
    struct FailingDeletes(RecordingBucket, Mutex<bool>);
    #[async_trait::async_trait]
    impl MediaObjectRepository for FailingDeletes {
        async fn put_object(&self, context: &UploadContext<'_>) -> Result<String> {
            self.0.put_object(context).await
        }
        async fn delete_object(&self, object_path: &str) -> Result<()> {
            if *self.1.lock().unwrap() {
                return Err(flare_server_core::flare_err!(
                    ErrorCode::NetworkError,
                    "storage unavailable"
                ));
            }
            self.0.delete_object(object_path).await
        }
        async fn presign_object(&self, object_path: &str, expires_in: i64) -> Result<String> {
            self.0.presign_object(object_path, expires_in).await
        }
        async fn presign_put_object(
            &self,
            object_path: &str,
            content_type: &str,
            content_length: i64,
            expires_in: i64,
        ) -> Result<String> {
            self.0
                .presign_put_object(object_path, content_type, content_length, expires_in)
                .await
        }
        fn base_url(&self) -> Option<String> {
            self.0.base_url()
        }
        fn cdn_base_url(&self) -> Option<String> {
            None
        }
        fn use_presigned_urls(&self) -> bool {
            true
        }
    }
    let bucket = Arc::new(FailingDeletes(RecordingBucket::default(), Mutex::new(true)));
    let sessions = Arc::new(MemoryUploadSessionStore::new());
    let chunks = std::env::temp_dir().join(format!("flare-media-reclaim-{}", Uuid::new_v4()));
    let service = MediaService::new(
        Some(bucket.clone()),
        Some(Arc::new(Metadata::default())),
        None,
        None,
        Some(sessions.clone()),
        None,
        MediaDomainConfig::new(
            DEFAULT_URL_TTL,
            None,
            60,
            chunks.clone(),
            SESSION_TTL,
            8 * 1024 * 1024,
        ),
    );
    let state = service
        .initiate_direct_upload(
            &owner(),
            &UploadFileMetadata {
                file_name: "a.png".to_string(),
                mime_type: "image/png".to_string(),
                file_size: 5,
                ..Default::default()
            },
            0,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    bucket.0.client_wrote(&state.object_key, 5);
    let at = Utc::now() + Duration::seconds(SESSION_TTL + UPLOAD_SESSION_RECLAIM_GRACE_SECONDS + 5);
    let report = service.reclaim_expired_upload_sessions(at).await.unwrap();
    assert_eq!((report.reclaimed, report.failed), (0, 1));
    assert!(bucket.0.holds(&state.object_key));
    // 同一租期内不会被重复领取。
    let report = service.reclaim_expired_upload_sessions(at).await.unwrap();
    assert_eq!((report.reclaimed, report.failed), (0, 0));
    *bucket.1.lock().unwrap() = false;
    let later = at + Duration::seconds(UPLOAD_SESSION_RECLAIM_LEASE_SECONDS * 2 + 5);
    let report = service
        .reclaim_expired_upload_sessions(later)
        .await
        .unwrap();
    assert_eq!((report.reclaimed, report.failed), (1, 0));
    assert!(!bucket.0.holds(&state.object_key));
    let _ = std::fs::remove_dir_all(&chunks);
}

/// 分块目录落在收到分块的副本本地：会话记录被别的副本回收或早已消失后，本机仍按目录
/// 闲置时间清理；有活跃会话的、名字不是规范 UUID 的目录不动。
#[tokio::test]
async fn stale_chunk_directories_without_a_live_session_are_swept_locally() {
    let f = Fixture::new();
    let orphan = Uuid::new_v4().to_string();
    std::fs::create_dir_all(f.chunks.join(&orphan)).unwrap();
    std::fs::write(f.chunks.join(&orphan).join("000000.part"), b"x").unwrap();
    let foreign = f.chunks.join("not-a-session");
    std::fs::create_dir_all(&foreign).unwrap();
    // 活跃会话：目录旧，但会话刚续过期（例如只重传了已有分块）。
    let live = f
        .service
        .initiate_multipart_upload(
            &owner(),
            MultipartUploadInit {
                file_name: "x.txt".to_string(),
                mime_type: "text/plain".to_string(),
                file_type: "file".to_string(),
                file_size: Some(1),
                chunk_size: 256 * 1024,
                user_id: "owner".to_string(),
                namespace: None,
                business_tag: None,
                trace_id: None,
                metadata: HashMap::new(),
            },
        )
        .await
        .unwrap();
    let at = f.after_expiry();
    let mut session = f.stored_session(&live.upload_id).await.unwrap();
    session.expires_at = at + Duration::hours(1);
    f.sessions.upsert_session(&session).await.unwrap();

    let report = f.service.reclaim_expired_upload_sessions(at).await.unwrap();
    assert_eq!(report.stale_chunk_dirs, 1, "{report:?}");
    assert!(!f.chunks.join(&orphan).exists());
    assert!(foreign.exists());
    assert!(f.service.session_dir(&live.upload_id).unwrap().exists());
}
