//! `upload-url` 只给出调用方租户下、由服务端生成的对象位置（SEC-05）。
//! 修复前它原样采用调用方的 key（缺省时是 `uploads/<uuid>`），首段不是租户。
use super::*;
use crate::domain::repository::MediaObjectRepository;
use std::sync::Arc;

const ORIGIN: &str = "https://edge.test/flare-media";

/// 只提供位置信息的对象存储，key 布局沿用 trait 默认的 `{tenant}/{name}`。
/// 写入与签名都不该被调用：upload-url 不碰存储。
struct LocationOnlyBucket;

#[async_trait::async_trait]
impl MediaObjectRepository for LocationOnlyBucket {
    async fn put_object(&self, _context: &UploadContext<'_>) -> Result<String> {
        Err(flare_server_core::flare_err!(
            ErrorCode::InternalError,
            "upload-url must not write objects"
        ))
    }
    async fn delete_object(&self, _object_path: &str) -> Result<()> {
        Err(flare_server_core::flare_err!(
            ErrorCode::InternalError,
            "upload-url must not delete objects"
        ))
    }
    async fn presign_object(&self, _object_path: &str, _expires_in: i64) -> Result<String> {
        Err(flare_server_core::flare_err!(
            ErrorCode::InternalError,
            "upload-url must not sign reads"
        ))
    }
    fn base_url(&self) -> Option<String> {
        Some(ORIGIN.to_string())
    }
    fn cdn_base_url(&self) -> Option<String> {
        None
    }
    fn use_presigned_urls(&self) -> bool {
        true
    }
    fn bucket_name(&self) -> Option<String> {
        Some("flare-media".to_string())
    }
}

struct Fixture {
    service: MediaService,
    temporary: std::path::PathBuf,
}

impl Fixture {
    fn new(object_repo: Option<ObjectRepositoryRef>) -> Self {
        let temporary =
            std::env::temp_dir().join(format!("flare-media-upload-url-{}", Uuid::new_v4()));
        let service = MediaService::new(
            object_repo,
            None,
            None,
            None,
            None,
            None,
            MediaDomainConfig::new(3600, None, 60, temporary.clone(), 3600, 8 * 1024 * 1024),
        );
        Self { service, temporary }
    }

    fn with_bucket() -> Self {
        Self::new(Some(Arc::new(LocationOnlyBucket)))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.temporary).expect("remove this test's temporary chunks");
    }
}

fn caller(tenant: &str) -> Context {
    Context::root().with_tenant_id(tenant).with_user_id("owner")
}

fn first_segment(key: &str) -> &str {
    key.split('/').next().unwrap_or_default()
}

/// 安全套件的对照请求（空 bucket、空 key）拿到的 key 首段是调用方租户，且每次都是新位置。
#[test]
fn server_generated_key_starts_with_the_caller_tenant() {
    let f = Fixture::with_bucket();
    let (url, key) = f
        .service
        .generate_upload_url(&caller("1200000000000000333"), Some(""), Some(""))
        .unwrap();
    assert_eq!(first_segment(&key), "1200000000000000333", "{key}");
    let name = key.rsplit('/').next().unwrap();
    assert!(Uuid::parse_str(name).is_ok(), "{key}");
    assert_eq!(url, format!("{ORIGIN}/{key}"));

    let (_, again) = f
        .service
        .generate_upload_url(&caller("1200000000000000333"), None, None)
        .unwrap();
    assert_ne!(again, key);

    // 与配置桶同名的 bucket 不算越界。
    let (_, same_bucket) = f
        .service
        .generate_upload_url(&caller("tenant-b"), Some("flare-media"), Some("  "))
        .unwrap();
    assert_eq!(first_segment(&same_bucket), "tenant-b", "{same_bucket}");
}

/// 租户段做的是可逆编码，带斜杠或 `..` 的租户 id 也只占首段，爬不到别的租户下面。
#[test]
fn tenant_segment_is_encoded_and_never_splits() {
    for fixture in [Fixture::with_bucket(), Fixture::new(None)] {
        let (_, key) = fixture
            .service
            .generate_upload_url(&caller("../tenant-b"), None, None)
            .unwrap();
        assert_eq!(first_segment(&key), "%2E%2E%2Ftenant-b", "{key}");
        assert!(!key.contains("../"), "{key}");
    }
}

/// 没有对象存储时同样按租户分段，退回的相对地址也落在租户下。
#[test]
fn without_an_object_store_the_key_is_still_tenant_scoped() {
    let f = Fixture::new(None);
    let (url, key) = f
        .service
        .generate_upload_url(&caller("tenant-a"), None, None)
        .unwrap();
    assert!(key.starts_with("tenant-a/uploads/"), "{key}");
    assert_eq!(url, format!("/media/{key}"));
}

/// 调用方点名的 key（含别的租户前缀）、别的桶、缺租户上下文都在生成地址之前被拒。
#[test]
fn caller_keys_foreign_buckets_and_missing_tenants_are_rejected() {
    let f = Fixture::with_bucket();
    for key in [
        "uploads/test.jpg",
        "tenant-b/media/documents/x.txt",
        "tenant-a/x",
    ] {
        let err = f
            .service
            .generate_upload_url(&caller("tenant-a"), None, Some(key))
            .unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter), "{key}");
    }

    let err = f
        .service
        .generate_upload_url(&caller("tenant-a"), Some("media-bucket"), None)
        .unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::PermissionDenied));

    for ctx in [
        Context::root().with_user_id("owner"),
        caller("*"),
        caller(" tenant-a"),
    ] {
        let err = f.service.generate_upload_url(&ctx, None, None).unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::PermissionDenied));
    }
}
