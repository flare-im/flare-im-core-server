//! Deterministic domain regression: real upload/read/session/cleanup orchestration,
//! in-memory ports. PostgreSQL/S3 and HTTP end-to-end gates remain separate.
use super::*;
use crate::domain::repository::{
    MediaLocalStore, MediaMetadataStore, OrphanAssetClaim, UploadSessionStore,
};
use std::sync::{Arc, Mutex};

type MetadataMap = Arc<Mutex<HashMap<(String, String), MediaFileMetadata>>>;

#[derive(Default)]
pub(crate) struct Metadata(pub(crate) MetadataMap);

/// In-memory claim: the entry is removed at claim time and restored unless committed.
struct MemoryClaim {
    map: MetadataMap,
    key: (String, String),
    asset: Option<MediaFileMetadata>,
}

#[async_trait::async_trait]
impl OrphanAssetClaim for MemoryClaim {
    fn asset(&self) -> &MediaFileMetadata {
        self.asset
            .as_ref()
            .expect("claim holds its asset until commit")
    }
    async fn commit(mut self: Box<Self>) -> Result<()> {
        self.asset = None;
        Ok(())
    }
}

impl Drop for MemoryClaim {
    fn drop(&mut self) {
        if let Some(asset) = self.asset.take() {
            self.map.lock().unwrap().insert(self.key.clone(), asset);
        }
    }
}

#[async_trait::async_trait]
impl MediaMetadataStore for Metadata {
    async fn save_metadata(&self, ctx: &Context, value: &MediaFileMetadata) -> Result<()> {
        self.0.lock().unwrap().insert(
            (require_tenant(ctx)?.to_string(), value.file_id.clone()),
            value.clone(),
        );
        Ok(())
    }
    async fn load_metadata(&self, ctx: &Context, id: &str) -> Result<Option<MediaFileMetadata>> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .get(&(require_tenant(ctx)?.to_string(), id.to_string()))
            .cloned())
    }
    async fn load_by_hash(&self, ctx: &Context, hash: &str) -> Result<Option<MediaFileMetadata>> {
        let tenant = require_tenant(ctx)?;
        Ok(self
            .0
            .lock()
            .unwrap()
            .iter()
            .find(|((owner, _), value)| owner == tenant && value.sha256.as_deref() == Some(hash))
            .map(|(_, value)| value.clone()))
    }
    async fn delete_metadata(&self, ctx: &Context, id: &str) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .remove(&(require_tenant(ctx)?.to_string(), id.to_string()));
        Ok(())
    }
    async fn list_orphaned_assets(
        &self,
        ctx: &Context,
        before: chrono::DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<MediaFileMetadata>> {
        let tenant = require_tenant(ctx)?;
        Ok(self
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|((owner, _), value)| {
                owner == tenant
                    && value.reference_count == 0
                    && value.status == MediaAssetStatus::Pending
                    && value.grace_expires_at.is_some_and(|at| at <= before)
            })
            .take(limit as usize)
            .map(|(_, value)| value.clone())
            .collect())
    }
    async fn claim_orphaned_asset(
        &self,
        ctx: &Context,
        id: &str,
        before: chrono::DateTime<Utc>,
    ) -> Result<Option<Box<dyn OrphanAssetClaim>>> {
        let key = (require_tenant(ctx)?.to_string(), id.to_string());
        let mut map = self.0.lock().unwrap();
        let orphan = map.get(&key).is_some_and(|value| {
            value.reference_count == 0
                && value.status == MediaAssetStatus::Pending
                && value.grace_expires_at.is_some_and(|at| at <= before)
        });
        if !orphan {
            return Ok(None);
        }
        let asset = map.remove(&key);
        Ok(Some(Box::new(MemoryClaim {
            map: self.0.clone(),
            key,
            asset,
        })))
    }
    async fn update_status(
        &self,
        ctx: &Context,
        id: &str,
        status: MediaAssetStatus,
        grace: Option<chrono::DateTime<Utc>>,
    ) -> Result<()> {
        if let Some(value) = self
            .0
            .lock()
            .unwrap()
            .get_mut(&(require_tenant(ctx)?.to_string(), id.to_string()))
        {
            value.status = status;
            value.grace_expires_at = grace;
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct Files(pub(crate) Mutex<HashMap<String, Vec<u8>>>);

#[async_trait::async_trait]
impl MediaLocalStore for Files {
    async fn write(&self, context: &UploadContext<'_>) -> Result<String> {
        let key = format!(
            "{}/{}",
            crate::domain::model::tenant_storage_segment(context.tenant_id),
            context.file_id
        );
        self.0
            .lock()
            .unwrap()
            .insert(key.clone(), context.payload.to_vec());
        Ok(key)
    }
    async fn read(&self, key: &str) -> Result<Vec<u8>> {
        self.0.lock().unwrap().get(key).cloned().ok_or_else(|| {
            flare_server_core::flare_err!(ErrorCode::MessageNotFound, "file not found")
        })
    }
    async fn delete(&self, key: &str) -> Result<()> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
    fn base_url(&self) -> Option<String> {
        Some("https://media.test/files".to_string())
    }
}

#[derive(Default)]
pub(crate) struct Sessions(pub(crate) Mutex<HashMap<String, UploadSession>>);

#[async_trait::async_trait]
impl UploadSessionStore for Sessions {
    async fn create_session(&self, session: &UploadSession) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .insert(session.upload_id.clone(), session.clone());
        Ok(())
    }
    async fn get_session(&self, id: &str) -> Result<Option<UploadSession>> {
        Ok(self.0.lock().unwrap().get(id).cloned())
    }
    async fn upsert_session(&self, session: &UploadSession) -> Result<()> {
        self.create_session(session).await
    }
    async fn delete_session(&self, id: &str) -> Result<()> {
        self.0.lock().unwrap().remove(id);
        Ok(())
    }
}

struct Fixture {
    service: MediaService,
    metadata: Arc<Metadata>,
    files: Arc<Files>,
    sessions: Arc<Sessions>,
    temporary: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let metadata = Arc::new(Metadata::default());
        let files = Arc::new(Files::default());
        let sessions = Arc::new(Sessions::default());
        let temporary = std::env::temp_dir().join(format!("flare-media-sec05-{}", Uuid::new_v4()));
        let service = MediaService::new(
            None,
            Some(metadata.clone()),
            None,
            None,
            Some(sessions.clone()),
            Some(files.clone()),
            MediaDomainConfig::new(3600, None, 60, temporary.clone(), 3600, 8 * 1024 * 1024),
        );
        Self {
            service,
            metadata,
            files,
            sessions,
            temporary,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.temporary).expect("remove this test's temporary chunks");
    }
}

fn ctx(tenant: &str, user: &str) -> Context {
    Context::root().with_tenant_id(tenant).with_user_id(user)
}

#[tokio::test]
async fn upload_download_and_presign_enforce_tenant_and_authenticated_owner() {
    let f = Fixture::new();
    let a = ctx("tenant-a", "same-user");
    let b = ctx("tenant-b", "same-user");
    let other = ctx("tenant-a", "other-user");
    let data = b"tenant-owned bytes";
    let input = UploadContext {
        tenant_id: "tenant-a",
        file_id: "file",
        file_name: "file.txt",
        mime_type: "text/plain",
        file_size: data.len() as i64,
        payload: data,
        file_category: "file".to_string(),
        user_id: "same-user",
        trace_id: None,
        namespace: None,
        business_tag: None,
        metadata: HashMap::from([
            ("tenant_id".to_string(), "tenant-b".to_string()),
            (
                STORAGE_PATH_METADATA_KEY.to_string(),
                "tenant-b/stolen".to_string(),
            ),
            (
                STORAGE_BUCKET_METADATA_KEY.to_string(),
                "foreign".to_string(),
            ),
        ]),
    };
    let saved = f.service.store_media_file(&a, input).await.unwrap();
    assert_eq!(saved.owner_id.as_deref(), Some("same-user"));
    assert_eq!(
        saved.metadata.get("tenant_id").map(String::as_str),
        Some("tenant-a")
    );
    assert_eq!(saved.storage_path(), Some("tenant-a/file"));
    assert!(saved.storage_bucket().is_none());
    assert_eq!(
        f.service.download_local_file(&a, "file").await.unwrap(),
        data
    );
    for forbidden in [&b, &other, &Context::root()] {
        assert!(f.service.get_metadata(forbidden, "file").await.is_err());
        assert!(
            f.service
                .create_presigned_url(forbidden, "file", 60)
                .await
                .is_err()
        );
        assert!(
            f.service
                .download_local_file(forbidden, "file")
                .await
                .is_err()
        );
    }
    // An old storage key is not rebuilt/prefixed on read.
    let mut legacy = saved;
    legacy.storage_path = Some("old-layout/object.txt".to_string());
    f.files
        .0
        .lock()
        .unwrap()
        .insert("old-layout/object.txt".to_string(), data.to_vec());
    f.metadata.save_metadata(&a, &legacy).await.unwrap();
    assert_eq!(
        f.service.download_local_file(&a, "file").await.unwrap(),
        data
    );
    // Historical private objects with unknown ownership remain unavailable.
    legacy.owner_id = None;
    f.metadata.save_metadata(&a, &legacy).await.unwrap();
    assert!(f.service.get_metadata(&a, "file").await.is_err());
    legacy.access_type = FileAccessType::Public;
    f.metadata.save_metadata(&a, &legacy).await.unwrap();
    assert!(f.service.get_metadata(&other, "file").await.is_ok());
    assert!(f.service.delete_media_file(&other, "file").await.is_err());
    assert!(f.service.get_metadata(&b, "file").await.is_err());
}

#[tokio::test]
async fn cleanup_only_deletes_selected_tenant_and_honors_limit() {
    let f = Fixture::new();
    for (tenant, id) in [("a", "same-file"), ("a", "second-file"), ("b", "same-file")] {
        let asset = MediaFileMetadata {
            file_id: id.to_string(),
            reference_count: 0,
            status: MediaAssetStatus::Pending,
            grace_expires_at: Some(Utc::now() - Duration::seconds(10)),
            storage_path: Some(format!("{tenant}/{id}")),
            metadata: HashMap::from([(
                MEDIA_LIFECYCLE_SCOPE_METADATA_KEY.to_string(),
                MESSAGE_MEDIA_LIFECYCLE_SCOPE.to_string(),
            )]),
            ..Default::default()
        };
        f.metadata
            .save_metadata(&ctx(tenant, "admin"), &asset)
            .await
            .unwrap();
        f.files
            .0
            .lock()
            .unwrap()
            .insert(format!("{tenant}/{id}"), vec![1]);
    }
    assert_eq!(
        f.service
            .cleanup_orphaned_assets(&ctx("a", "admin"), 1)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        f.metadata
            .list_orphaned_assets(&ctx("a", "admin"), Utc::now(), 100)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        f.metadata
            .load_metadata(&ctx("b", "admin"), "same-file")
            .await
            .unwrap()
            .is_some()
    );
    assert!(f.files.0.lock().unwrap().contains_key("b/same-file"));
    assert!(
        f.service
            .cleanup_orphaned_assets(&Context::root(), 1)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn multipart_requires_session_tenant_and_owner_before_io_or_abort() {
    let f = Fixture::new();
    let a = ctx("a", "owner");
    let initial = f
        .service
        .initiate_multipart_upload(
            &a,
            MultipartUploadInit {
                file_name: "x.txt".to_string(),
                mime_type: "text/plain".to_string(),
                file_type: "file".to_string(),
                file_size: Some(1),
                chunk_size: 256 * 1024,
                user_id: "spoofed".to_string(),
                namespace: None,
                business_tag: None,
                trace_id: None,
                metadata: HashMap::new(),
            },
        )
        .await
        .unwrap();
    let id = &initial.upload_id;
    let mut session = f.sessions.get_session(id).await.unwrap().unwrap();
    assert_eq!(session.tenant_id, "a");
    assert_eq!(session.user_id, "owner");
    for wrong in [ctx("b", "owner"), ctx("a", "other")] {
        assert!(f.service.abort_multipart_upload(&wrong, id).await.is_err());
        assert!(
            f.service
                .complete_multipart_upload(&wrong, id)
                .await
                .is_err()
        );
        assert!(
            f.service
                .upload_multipart_chunk(
                    &wrong,
                    MultipartChunkPayload {
                        upload_id: id.clone(),
                        chunk_index: 0,
                        bytes: vec![1]
                    }
                )
                .await
                .is_err()
        );
    }
    session.tenant_id.clear();
    f.sessions.upsert_session(&session).await.unwrap();
    assert!(f.service.abort_multipart_upload(&a, id).await.is_err());
    assert!(f.sessions.get_session(id).await.unwrap().is_some());
    // Direct upload uses the same stored ownership guard before status/abort.
    session.tenant_id = "a".to_string();
    session.transport_kind = Some(DirectUploadTransportKind::SinglePut);
    f.sessions.upsert_session(&session).await.unwrap();
    assert!(
        f.service
            .get_direct_upload_status(&ctx("b", "owner"), id)
            .await
            .is_err()
    );
    assert!(
        f.service
            .abort_direct_upload(&ctx("b", "owner"), id)
            .await
            .is_err()
    );
    assert!(f.service.get_direct_upload_status(&a, id).await.is_ok());
    // Missing sessions never reach cleanup; invalid IDs cannot form chunk paths.
    f.service
        .abort_multipart_upload(&a, "../outside")
        .await
        .unwrap();
    assert!(f.service.session_dir("../outside").is_err());
    assert!(f.service.session_dir("/tmp").is_err());
}

#[test]
fn tenant_key_encoding_is_injective_and_cannot_escape_its_segment() {
    use crate::domain::model::tenant_storage_segment as encode;
    assert_eq!(encode("123"), "123");
    assert_eq!(encode("tenant-A"), "tenant-A");
    assert_eq!(encode("../other"), "%2E%2E%2Fother");
    assert_ne!(encode("a/b"), encode("a%2Fb"));
    assert_ne!(encode("A"), encode("a"));
}
