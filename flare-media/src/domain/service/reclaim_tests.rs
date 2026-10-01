//! 直传大小约束与过期上传会话回收：真实领域编排，内存端口。存储侧按 signed headers 拒绝
//! 长度不符的单次 PUT 与分片由 S3 实现与存储负责（`s3.rs` 断言签名里有 content-length），
//! 这里覆盖每片签入的长度、完成时的兜底核对与错误优先级，以及回收任务。
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
    /// (part_number, content_length)
    presigned_parts: Mutex<Vec<(u32, i64)>>,
    /// 每次合并分片的对象 key。
    completed: Mutex<Vec<String>>,
    /// 为真时删除对象失败（存储暂时不可用）。
    fail_deletes: Mutex<bool>,
    /// 设了就在合并里停住：先通知第一个，等第二个通知了才合并（模拟耗时的大文件合并）。
    merge_gate: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
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
        if *self.fail_deletes.lock().unwrap() {
            return Err(flare_server_core::flare_err!(
                ErrorCode::NetworkError,
                "storage unavailable"
            ));
        }
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
        content_length: i64,
        _expires_in: i64,
    ) -> Result<String> {
        self.presigned_parts
            .lock()
            .unwrap()
            .push((part_number, content_length));
        Ok(format!(
            "https://bucket.test/{object_path}?partNumber={part_number}"
        ))
    }
    async fn complete_multipart_upload(
        &self,
        object_path: &str,
        _upload_id: &str,
        _parts: &[UploadedPartRecord],
    ) -> Result<()> {
        let gate = self.merge_gate.lock().unwrap().take();
        if let Some((entered, release)) = gate {
            entered.notify_one();
            release.notified().await;
        }
        // 与 S3 一致：已合并或已中止的分片上传不能再合并（存储回 NoSuchUpload，S3 实现映射
        // 成参数错误）。
        let gone = self
            .completed
            .lock()
            .unwrap()
            .iter()
            .any(|key| key == object_path)
            || self
                .aborted
                .lock()
                .unwrap()
                .iter()
                .any(|(key, _)| key == object_path);
        if gone {
            return Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "multipart upload no longer exists; start a new upload"
            ));
        }
        // 合并后的对象大小由测试事先用 client_wrote 设定（模拟不校验 signed headers 的存储）。
        self.completed.lock().unwrap().push(object_path.to_string());
        Ok(())
    }
    async fn abort_multipart_upload(&self, object_path: &str, upload_id: &str) -> Result<()> {
        self.aborted
            .lock()
            .unwrap()
            .push((object_path.to_string(), upload_id.to_string()));
        Ok(())
    }
    async fn stat_object(&self, object_path: &str) -> Result<Option<ObjectStat>> {
        let size = self.objects.lock().unwrap().get(object_path).copied();
        Ok(size.map(|size| ObjectStat {
            size: Some(size),
            etag: None,
        }))
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
        Self::with(
            |metadata| metadata as Arc<dyn MediaMetadataStore>,
            |sessions| sessions as Arc<dyn UploadSessionStore>,
        )
    }

    /// 服务经包装后的端口访问存储，测试仍直接读写里面的内存存储。
    fn with(
        metadata_port: impl FnOnce(Arc<Metadata>) -> Arc<dyn MediaMetadataStore>,
        sessions_port: impl FnOnce(Arc<MemoryUploadSessionStore>) -> Arc<dyn UploadSessionStore>,
    ) -> Self {
        let bucket = Arc::new(RecordingBucket::default());
        let metadata = Arc::new(Metadata::default());
        let sessions = Arc::new(MemoryUploadSessionStore::new());
        let chunks = std::env::temp_dir().join(format!("flare-media-reclaim-{}", Uuid::new_v4()));
        let service = MediaService::new(
            Some(bucket.clone()),
            Some(metadata_port(metadata.clone())),
            None,
            None,
            Some(sessions_port(sessions.clone())),
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

/// 提交全部分片（ETag 由测试编造；合并由假存储完成）。
async fn commit_all_parts(f: &Fixture, state: &DirectUploadSessionState) {
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
}

/// 每片地址签入这一片应有的长度：前几片是 part_size，最后一片是余下的字节，合计等于
/// 声明大小。超大的分片在上传时就被存储拒绝，不必等到完成时才发现。
#[tokio::test]
async fn multipart_part_urls_sign_each_part_length() {
    let f = Fixture::new();
    let part_size = 8 * 1024 * 1024;
    let declared = 2 * part_size + 4 * 1024 * 1024 + 7;
    let state = f.initiate(declared).await;
    assert_eq!(
        state.transport_kind,
        DirectUploadTransportKind::MultipartPut
    );
    assert_eq!((state.part_size, state.total_parts), (part_size, 3));
    // 乱序、分批请求也按片号算长度。
    for batch in [&[3u32, 1][..], &[2][..]] {
        f.service
            .presign_direct_upload_parts(&owner(), &state.upload_id, batch, 0)
            .await
            .unwrap();
    }
    let mut signed = f.bucket.presigned_parts.lock().unwrap().clone();
    signed.sort();
    assert_eq!(
        signed,
        vec![
            (1, part_size),
            (2, part_size),
            (3, declared - 2 * part_size)
        ]
    );
    assert_eq!(
        signed.iter().map(|(_, length)| length).sum::<i64>(),
        declared
    );
}

/// 完成早于 PUT：对象不存在是调用方错误（400），不是存储不可用（503）；会话保留，
/// PUT 之后照常完成。
#[tokio::test]
async fn completing_a_single_put_before_the_put_is_a_bad_request() {
    let f = Fixture::new();
    let state = f.initiate(5).await;
    let err = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::InvalidParameter), "{err}");
    assert!(f.stored_session(&state.upload_id).await.is_some());
    // 空对象同样是调用方错误。
    f.bucket.client_wrote(&state.object_key, 0);
    let err = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::InvalidParameter), "{err}");
    f.bucket.client_wrote(&state.object_key, 5);
    let saved = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap();
    assert_eq!(saved.file_size, 5);
}

/// 大小不符时删除写错的对象失败：答复仍是大小不符（400），不被存储故障（503）盖住；
/// 会话保留，对象由回收任务在会话过期后删除。
#[tokio::test]
async fn a_failed_delete_does_not_mask_a_single_put_size_mismatch() {
    let f = Fixture::new();
    let state = f.initiate(5).await;
    f.bucket.client_wrote(&state.object_key, 50);
    *f.bucket.fail_deletes.lock().unwrap() = true;
    let err = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::InvalidParameter), "{err}");
    assert!(f.bucket.holds(&state.object_key));
    assert!(f.stored_session(&state.upload_id).await.is_some());

    *f.bucket.fail_deletes.lock().unwrap() = false;
    let report = f
        .service
        .reclaim_expired_upload_sessions(f.after_expiry())
        .await
        .unwrap();
    assert_eq!((report.reclaimed, report.failed), (1, 0));
    assert!(!f.bucket.holds(&state.object_key));
}

/// 分片合并后大小不符、删除对象失败：答复仍是 400；会话标为中止并保留，不能再完成，
/// 回收任务到期后删掉合并出的对象和记录。
#[tokio::test]
async fn a_failed_delete_does_not_mask_a_merged_size_mismatch_and_reclaim_removes_the_object() {
    let f = Fixture::new();
    let declared = 20 * 1024 * 1024;
    let state = f.initiate(declared).await;
    commit_all_parts(&f, &state).await;
    f.bucket.client_wrote(&state.object_key, declared * 3);
    *f.bucket.fail_deletes.lock().unwrap() = true;
    let err = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::InvalidParameter), "{err}");
    assert!(f.bucket.holds(&state.object_key));
    let kept = f.stored_session(&state.upload_id).await.unwrap();
    assert_eq!(kept.status, UploadSessionStatus::Aborted);

    // 已合并的分片上传不能重来：再次完成直接拒绝，不再碰存储。
    *f.bucket.fail_deletes.lock().unwrap() = false;
    let err = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::InvalidParameter), "{err}");
    assert_eq!(f.bucket.completed.lock().unwrap().len(), 1);
    assert!(f.bucket.holds(&state.object_key));

    let report = f
        .service
        .reclaim_expired_upload_sessions(f.after_expiry())
        .await
        .unwrap();
    assert_eq!((report.reclaimed, report.failed), (1, 0));
    assert!(!f.bucket.holds(&state.object_key));
    assert!(f.stored_session(&state.upload_id).await.is_none());
    assert!(
        f.metadata
            .load_metadata(&owner(), &state.file_id)
            .await
            .unwrap()
            .is_none()
    );
}

/// 合并成功并已登记的分片文件：会话记录残留（删会话失败）时，回收只 abort，不删文件对象。
#[tokio::test]
async fn reclaim_keeps_a_registered_multipart_object() {
    let f = Fixture::new();
    let declared = 20 * 1024 * 1024;
    let state = f.initiate(declared).await;
    commit_all_parts(&f, &state).await;
    f.bucket.client_wrote(&state.object_key, declared);
    let pending = f.stored_session(&state.upload_id).await.unwrap();
    f.service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap();
    // 模拟完成时删会话失败：记录还在，状态仍是完成前的样子。
    f.sessions.upsert_session(&pending).await.unwrap();

    let report = f
        .service
        .reclaim_expired_upload_sessions(f.after_expiry())
        .await
        .unwrap();
    assert_eq!((report.reclaimed, report.failed), (1, 0));
    assert!(f.bucket.holds(&state.object_key));
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

/// 元数据端口：前 `fail_saves` 次登记失败（数据库暂时不可用），其余照常。
struct FlakyMetadata {
    inner: Arc<Metadata>,
    fail_saves: Mutex<u32>,
}

impl FlakyMetadata {
    fn port(fail_saves: u32) -> impl FnOnce(Arc<Metadata>) -> Arc<dyn MediaMetadataStore> {
        move |inner| {
            Arc::new(Self {
                inner,
                fail_saves: Mutex::new(fail_saves),
            })
        }
    }
}

#[async_trait::async_trait]
impl MediaMetadataStore for FlakyMetadata {
    async fn save_metadata(&self, ctx: &Context, metadata: &MediaFileMetadata) -> Result<()> {
        {
            let mut left = self.fail_saves.lock().unwrap();
            if *left > 0 {
                *left -= 1;
                return Err(flare_server_core::flare_err!(
                    ErrorCode::DatabaseError,
                    "database unavailable"
                ));
            }
        }
        self.inner.save_metadata(ctx, metadata).await
    }
    async fn load_metadata(&self, ctx: &Context, id: &str) -> Result<Option<MediaFileMetadata>> {
        self.inner.load_metadata(ctx, id).await
    }
    async fn load_by_hash(&self, ctx: &Context, hash: &str) -> Result<Option<MediaFileMetadata>> {
        self.inner.load_by_hash(ctx, hash).await
    }
    async fn delete_metadata(&self, ctx: &Context, id: &str) -> Result<()> {
        self.inner.delete_metadata(ctx, id).await
    }
    async fn list_orphaned_assets(
        &self,
        ctx: &Context,
        before: chrono::DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<MediaFileMetadata>> {
        self.inner.list_orphaned_assets(ctx, before, limit).await
    }
    async fn claim_orphaned_asset(
        &self,
        ctx: &Context,
        id: &str,
        before: chrono::DateTime<Utc>,
    ) -> Result<Option<Box<dyn crate::domain::repository::OrphanAssetClaim>>> {
        self.inner.claim_orphaned_asset(ctx, id, before).await
    }
    async fn update_status(
        &self,
        ctx: &Context,
        id: &str,
        status: MediaAssetStatus,
        grace_expires_at: Option<chrono::DateTime<Utc>>,
    ) -> Result<()> {
        self.inner
            .update_status(ctx, id, status, grace_expires_at)
            .await
    }
}

/// 会话端口：把会话标为中止的写入前 `fail_aborted_upserts` 次失败，并计数；
/// `renew_after_claim` 设了就在下一次领取之后把领到的会话续期到该时刻（模拟紧接着领取
/// 开始的一次完成）。
struct FlakySessions {
    inner: Arc<MemoryUploadSessionStore>,
    fail_aborted_upserts: Mutex<u32>,
    aborted_upserts: Mutex<u32>,
    renew_after_claim: Mutex<Option<chrono::DateTime<Utc>>>,
}

impl FlakySessions {
    fn new(inner: Arc<MemoryUploadSessionStore>) -> Self {
        Self {
            inner,
            fail_aborted_upserts: Mutex::new(0),
            aborted_upserts: Mutex::new(0),
            renew_after_claim: Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl UploadSessionStore for FlakySessions {
    async fn create_session(&self, session: &UploadSession) -> Result<()> {
        self.inner.create_session(session).await
    }
    async fn get_session(&self, upload_id: &str) -> Result<Option<UploadSession>> {
        self.inner.get_session(upload_id).await
    }
    async fn get_session_record(&self, upload_id: &str) -> Result<Option<UploadSession>> {
        self.inner.get_session_record(upload_id).await
    }
    async fn upsert_session(&self, session: &UploadSession) -> Result<()> {
        if session.status == UploadSessionStatus::Aborted {
            *self.aborted_upserts.lock().unwrap() += 1;
            let mut left = self.fail_aborted_upserts.lock().unwrap();
            if *left > 0 {
                *left -= 1;
                return Err(flare_server_core::flare_err!(
                    ErrorCode::NetworkError,
                    "session store unavailable"
                ));
            }
        }
        self.inner.upsert_session(session).await
    }
    async fn delete_session(&self, upload_id: &str) -> Result<()> {
        self.inner.delete_session(upload_id).await
    }
    async fn claim_expired_sessions(
        &self,
        expired_before: chrono::DateTime<Utc>,
        lease_until: chrono::DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<UploadSession>> {
        let claimed = self
            .inner
            .claim_expired_sessions(expired_before, lease_until, limit)
            .await?;
        let renew = self.renew_after_claim.lock().unwrap().take();
        if let Some(expires_at) = renew {
            for session in &claimed {
                let mut renewed = session.clone();
                renewed.expires_at = expires_at;
                self.inner.upsert_session(&renewed).await?;
            }
        }
        Ok(claimed)
    }
}

/// 用 [`FlakySessions`] 作会话端口的夹具，同时交回这个端口供测试布置故障。
fn fixture_with_flaky_sessions() -> (Fixture, Arc<FlakySessions>) {
    let mut port = None;
    let f = Fixture::with(
        |metadata| metadata as Arc<dyn MediaMetadataStore>,
        |inner| {
            let flaky = Arc::new(FlakySessions::new(inner));
            port = Some(flaky.clone());
            flaky
        },
    );
    (f, port.unwrap())
}

/// 分片合并成功、登记失败（数据库暂时不可用）后客户端重试完成：存储里已没有这个分片
/// 上传（S3 回 NoSuchUpload），合并出的对象却在。重试按这个对象核对大小并补登记；修复前
/// 重试永远失败（NoSuchUpload 经 NetworkError 回 503）。
#[tokio::test]
async fn a_retried_completion_registers_the_object_merged_before_registration_failed() {
    let f = Fixture::with(FlakyMetadata::port(1), |sessions| {
        sessions as Arc<dyn UploadSessionStore>
    });
    let declared = 20 * 1024 * 1024;
    let state = f.initiate(declared).await;
    commit_all_parts(&f, &state).await;
    f.bucket.client_wrote(&state.object_key, declared);

    let err = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap_err();
    assert_ne!(err.code(), Some(ErrorCode::InvalidParameter), "{err}");
    assert_eq!(f.bucket.completed.lock().unwrap().len(), 1);
    assert!(
        f.metadata
            .load_metadata(&owner(), &state.file_id)
            .await
            .unwrap()
            .is_none()
    );
    let kept = f.stored_session(&state.upload_id).await.unwrap();
    assert_eq!(kept.status, UploadSessionStatus::Pending);

    let saved = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap();
    assert_eq!(saved.file_id, state.file_id);
    assert_eq!(saved.file_size, declared);
    // 第二次合并被存储拒绝（已合并），没有再合并一次。
    assert_eq!(f.bucket.completed.lock().unwrap().len(), 1);
    assert!(f.bucket.holds(&state.object_key));
    assert!(
        f.metadata
            .load_metadata(&owner(), &state.file_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(f.stored_session(&state.upload_id).await.is_none());
}

/// 存储里的分片上传已不在（例如被桶生命周期规则中止）、也没有合并出的对象：再试也不会
/// 成功，回 400 让客户端重新发起上传，而不是可重试的 503。
#[tokio::test]
async fn completing_a_multipart_upload_the_store_dropped_asks_for_a_new_upload() {
    let f = Fixture::new();
    let declared = 20 * 1024 * 1024;
    let state = f.initiate(declared).await;
    commit_all_parts(&f, &state).await;
    f.bucket
        .aborted
        .lock()
        .unwrap()
        .push((state.object_key.clone(), STORAGE_UPLOAD_ID.to_string()));
    let err = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::InvalidParameter), "{err}");
    assert!(
        f.metadata
            .load_metadata(&owner(), &state.file_id)
            .await
            .unwrap()
            .is_none()
    );
}

/// 已完成登记、只是删会话失败（或答复丢了）后重试完成：答复已登记的文件，不再碰存储，
/// 也不重写登记，否则其间加上的引用计数会被清掉。
#[tokio::test]
async fn retrying_a_completed_upload_answers_the_registered_file_without_rewriting_it() {
    let f = Fixture::new();
    let state = f.initiate(5).await;
    f.bucket.client_wrote(&state.object_key, 5);
    let pending = f.stored_session(&state.upload_id).await.unwrap();
    let first = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap();
    f.sessions.upsert_session(&pending).await.unwrap();
    let mut referenced = f
        .metadata
        .load_metadata(&owner(), &state.file_id)
        .await
        .unwrap()
        .unwrap();
    referenced.reference_count = 2;
    f.metadata
        .save_metadata(&owner(), &referenced)
        .await
        .unwrap();

    let again = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap();
    assert_eq!(again.file_id, first.file_id);
    assert_eq!(again.reference_count, 2);
    let stored = f
        .metadata
        .load_metadata(&owner(), &state.file_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.reference_count, 2);
    assert!(f.stored_session(&state.upload_id).await.is_none());
    assert!(f.bucket.holds(&state.object_key));
}

/// 零字节文件：声明大小为 0 的单次 PUT 写入 0 字节对象即可完成，与经 Media 中转的上传
/// 一致；修复前完成时回 400「对象缺失或为空」，零字节文件永远传不上去。
#[tokio::test]
async fn a_zero_byte_single_put_completes() {
    let f = Fixture::new();
    let state = f.initiate(0).await;
    assert_eq!(state.transport_kind, DirectUploadTransportKind::SinglePut);
    let presigned = f.bucket.presigned_puts.lock().unwrap().clone();
    assert_eq!(presigned.len(), 1);
    assert_eq!(presigned[0].1, 0, "the PUT url signs content-length 0");

    // 还没 PUT：对象不存在仍是调用方错误，会话保留。
    let err = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::InvalidParameter), "{err}");
    assert!(f.stored_session(&state.upload_id).await.is_some());

    f.bucket.client_wrote(&state.object_key, 0);
    let saved = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap();
    assert_eq!(saved.file_size, 0);
    assert!(f.bucket.holds(&state.object_key));
    assert!(
        f.metadata
            .load_metadata(&owner(), &state.file_id)
            .await
            .unwrap()
            .is_some()
    );
}

/// 声明 0 字节却写进了内容：与其他大小不符一样回 400 并删掉对象。
#[tokio::test]
async fn a_zero_byte_declaration_refuses_a_non_empty_object() {
    let f = Fixture::new();
    let state = f.initiate(0).await;
    f.bucket.client_wrote(&state.object_key, 3);
    let err = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::InvalidParameter), "{err}");
    assert!(!f.bucket.holds(&state.object_key));
    assert!(
        f.metadata
            .load_metadata(&owner(), &state.file_id)
            .await
            .unwrap()
            .is_none()
    );
}

/// 合并后大小不符、删对象失败时把会话标为中止；这次写入失败会重试，而不是被忽略。
#[tokio::test]
async fn marking_a_merged_mismatch_aborted_retries_a_failed_write() {
    let (f, port) = fixture_with_flaky_sessions();
    *port.fail_aborted_upserts.lock().unwrap() = 1;
    let declared = 20 * 1024 * 1024;
    let state = f.initiate(declared).await;
    commit_all_parts(&f, &state).await;
    f.bucket.client_wrote(&state.object_key, declared * 3);
    *f.bucket.fail_deletes.lock().unwrap() = true;

    let err = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::InvalidParameter), "{err}");
    assert_eq!(*port.aborted_upserts.lock().unwrap(), 2);
    let kept = f.stored_session(&state.upload_id).await.unwrap();
    assert_eq!(kept.status, UploadSessionStatus::Aborted);
}

/// 标记中止一直写不进去：答复仍是大小不符的 400；会话停在待完成，再次完成仍被大小核对
/// 拒绝、不会登记，回收任务到期后删掉这个未登记的对象。
#[tokio::test]
async fn an_unrecorded_abort_still_refuses_completion_and_reclaim_removes_the_object() {
    let (f, port) = fixture_with_flaky_sessions();
    *port.fail_aborted_upserts.lock().unwrap() = u32::MAX;
    let declared = 20 * 1024 * 1024;
    let state = f.initiate(declared).await;
    commit_all_parts(&f, &state).await;
    f.bucket.client_wrote(&state.object_key, declared * 3);
    *f.bucket.fail_deletes.lock().unwrap() = true;

    for round in 1..=2 {
        let err = f
            .service
            .complete_direct_upload(&owner(), &state.upload_id)
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            Some(ErrorCode::InvalidParameter),
            "round {round}: {err}"
        );
        assert_eq!(
            *port.aborted_upserts.lock().unwrap(),
            round * super::super::direct::ABORT_MARK_ATTEMPTS
        );
        let kept = f.stored_session(&state.upload_id).await.unwrap();
        assert_eq!(kept.status, UploadSessionStatus::Pending);
        assert!(f.bucket.holds(&state.object_key));
        assert!(
            f.metadata
                .load_metadata(&owner(), &state.file_id)
                .await
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(f.bucket.completed.lock().unwrap().len(), 1);

    *f.bucket.fail_deletes.lock().unwrap() = false;
    let report = f
        .service
        .reclaim_expired_upload_sessions(f.after_expiry())
        .await
        .unwrap();
    assert_eq!((report.reclaimed, report.failed), (1, 0), "{report:?}");
    assert!(!f.bucket.holds(&state.object_key));
    assert!(f.stored_session(&state.upload_id).await.is_none());
}

/// 会话临近过期时开始完成、合并耗时超过过期时刻加宽限期：完成在合并前先续期，回收任务
/// 领不到它，不会中止分片上传，也不会删掉刚合并的对象。
#[tokio::test]
async fn reclaim_leaves_a_session_whose_completion_is_merging() {
    let f = Fixture::new();
    let declared = 20 * 1024 * 1024;
    let state = f.initiate(declared).await;
    commit_all_parts(&f, &state).await;
    f.bucket.client_wrote(&state.object_key, declared);
    let mut about_to_expire = f.stored_session(&state.upload_id).await.unwrap();
    about_to_expire.expires_at = Utc::now() + Duration::seconds(30);
    f.sessions.upsert_session(&about_to_expire).await.unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *f.bucket.merge_gate.lock().unwrap() = Some((entered.clone(), release.clone()));
    let at =
        about_to_expire.expires_at + Duration::seconds(UPLOAD_SESSION_RECLAIM_GRACE_SECONDS + 5);

    let owner = owner();
    let (completed, report) = tokio::join!(
        f.service.complete_direct_upload(&owner, &state.upload_id),
        async {
            entered.notified().await;
            let report = f.service.reclaim_expired_upload_sessions(at).await.unwrap();
            release.notify_one();
            report
        }
    );
    assert_eq!(report.reclaimed, 0, "{report:?}");
    assert!(f.bucket.aborted.lock().unwrap().is_empty());
    let saved = completed.unwrap();
    assert_eq!(saved.file_size, declared);
    assert!(f.bucket.holds(&state.object_key));
    assert!(
        f.metadata
            .load_metadata(&owner, &state.file_id)
            .await
            .unwrap()
            .is_some()
    );
}

/// 领取之后会话才被续期（一次完成紧接着领取开始）：回收任务释放前重读记录，看到续期就
/// 不动它（不中止分片上传、不删对象、不删记录），之后照常完成。
#[tokio::test]
async fn reclaim_skips_a_session_renewed_after_it_was_claimed() {
    let (f, port) = fixture_with_flaky_sessions();
    let declared = 20 * 1024 * 1024;
    let state = f.initiate(declared).await;
    commit_all_parts(&f, &state).await;
    f.bucket.client_wrote(&state.object_key, declared);
    let at = f.after_expiry();
    *port.renew_after_claim.lock().unwrap() = Some(at + Duration::seconds(SESSION_TTL));

    let report = f.service.reclaim_expired_upload_sessions(at).await.unwrap();
    assert_eq!(
        (report.reclaimed, report.renewed, report.failed),
        (0, 1, 0),
        "{report:?}"
    );
    assert!(f.bucket.aborted.lock().unwrap().is_empty());
    assert!(f.bucket.holds(&state.object_key));
    assert!(f.stored_session(&state.upload_id).await.is_some());

    let saved = f
        .service
        .complete_direct_upload(&owner(), &state.upload_id)
        .await
        .unwrap();
    assert_eq!(saved.file_size, declared);
}
