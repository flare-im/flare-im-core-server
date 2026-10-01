//! 对外 CDN 地址只能是绝对 http(s) URL：空基址不能拼出裸 key，历史行的裸 key 读取时丢弃。
use super::isolation_tests::Metadata;
use super::*;
use crate::domain::repository::{MediaMetadataStore, MediaObjectRepository};
use std::sync::Arc;

const SIGNED_ORIGIN: &str = "https://edge.test/flare-media";

/// 私有桶：`put_object` 返回租户前缀 key，预签名给出指向对外端点的绝对地址。
struct SignedBucket;

#[async_trait::async_trait]
impl MediaObjectRepository for SignedBucket {
    async fn put_object(&self, context: &UploadContext<'_>) -> Result<String> {
        Ok(format!(
            "{}/media/documents/{}.txt",
            context.tenant_id, context.file_id
        ))
    }
    async fn delete_object(&self, _object_path: &str) -> Result<()> {
        Ok(())
    }
    async fn presign_object(&self, object_path: &str, _expires_in: i64) -> Result<String> {
        Ok(format!(
            "{SIGNED_ORIGIN}/{object_path}?X-Amz-Signature=unit-test"
        ))
    }
    fn base_url(&self) -> Option<String> {
        Some(SIGNED_ORIGIN.to_string())
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
    metadata: Arc<Metadata>,
    temporary: std::path::PathBuf,
}

impl Fixture {
    fn new(cdn_base_url: Option<&str>) -> Self {
        let metadata = Arc::new(Metadata::default());
        let temporary = std::env::temp_dir().join(format!("flare-media-cdn-{}", Uuid::new_v4()));
        let service = MediaService::new(
            Some(Arc::new(SignedBucket)),
            Some(metadata.clone()),
            None,
            None,
            None,
            None,
            MediaDomainConfig::new(
                3600,
                cdn_base_url.map(str::to_string),
                60,
                temporary.clone(),
                3600,
                8 * 1024 * 1024,
            ),
        );
        Self {
            service,
            metadata,
            temporary,
        }
    }

    async fn upload(&self, ctx: &Context, file_id: &str) -> MediaFileMetadata {
        let data = b"cdn url bytes";
        self.service
            .store_media_file(
                ctx,
                UploadContext {
                    tenant_id: "tenant-a",
                    file_id,
                    file_name: "note.txt",
                    mime_type: "text/plain",
                    file_size: data.len() as i64,
                    payload: data,
                    file_category: "file".to_string(),
                    user_id: "owner",
                    trace_id: None,
                    namespace: None,
                    business_tag: None,
                    metadata: HashMap::new(),
                },
            )
            .await
            .expect("owner upload")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.temporary).expect("remove this test's temporary chunks");
    }
}

fn owner() -> Context {
    super::isolation_tests::ctx("tenant-a", "owner")
}

fn assert_signed(url: &str, key: &str) {
    assert!(
        url.starts_with(&format!("{SIGNED_ORIGIN}/{key}?")),
        "expected a signed absolute url for {key}, got {url}"
    );
}

/// base.toml 的 `${FLARE_S3_CDN_BASE_URL:-}` 展开成空串时，上传不能把裸 key 记成 cdn_url，
/// 取地址时 cdn_url 为空、url 是绝对签名地址。缺 scheme 的基址同理。
#[tokio::test]
async fn blank_cdn_base_never_yields_a_relative_cdn_url() {
    for blank in ["", "   ", "cdn.example.com/media"] {
        let f = Fixture::new(Some(blank));
        let saved = f.upload(&owner(), "blank-cdn").await;
        let key = saved.storage_path().expect("object key").to_string();
        assert_eq!(key, "tenant-a/media/documents/blank-cdn.txt");
        assert_eq!(saved.cdn_url, "", "persisted cdn_url for base {blank:?}");

        let presigned = f
            .service
            .create_presigned_url(&owner(), "blank-cdn", 60)
            .await
            .unwrap();
        assert_eq!(presigned.cdn_url, "", "cdn_url for base {blank:?}");
        assert_signed(&presigned.url, &key);
    }
}

/// 修复前落库的历史行：cdn_url 是裸 key。读取时丢弃它，回退到签名 URL。
#[tokio::test]
async fn legacy_bare_key_cdn_url_is_dropped_on_read() {
    let f = Fixture::new(None);
    let mut saved = f.upload(&owner(), "legacy").await;
    let key = saved.storage_path().expect("object key").to_string();
    for legacy in [
        key.clone(),
        format!("/{key}"),
        format!("//{key}"),
        format!("https:{key}"),
        "cdn.example.com/media/x.txt".to_string(),
        format!(" {SIGNED_ORIGIN}/{key}"),
    ] {
        saved.cdn_url = legacy.clone();
        f.metadata.save_metadata(&owner(), &saved).await.unwrap();

        let presigned = f
            .service
            .create_presigned_url(&owner(), "legacy", 60)
            .await
            .unwrap();
        assert_eq!(presigned.cdn_url, "", "legacy cdn_url {legacy:?}");
        assert_signed(&presigned.url, &key);

        let loaded = f.service.get_metadata(&owner(), "legacy").await.unwrap();
        assert_eq!(loaded.client_cdn_url(), "", "legacy cdn_url {legacy:?}");
    }
}

/// 真配了 CDN 时行为不变：新上传与已有的绝对 cdn_url 都照常返回。
#[tokio::test]
async fn configured_absolute_cdn_is_kept() {
    let f = Fixture::new(Some("https://cdn.example.com/media/"));
    let saved = f.upload(&owner(), "with-cdn").await;
    let key = saved.storage_path().expect("object key").to_string();
    assert_eq!(
        saved.cdn_url,
        format!("https://cdn.example.com/media/{key}")
    );
    let presigned = f
        .service
        .create_presigned_url(&owner(), "with-cdn", 60)
        .await
        .unwrap();
    assert_eq!(presigned.cdn_url, saved.cdn_url);
    assert_signed(&presigned.url, &key);
}

#[test]
fn only_absolute_http_urls_count_as_client_urls() {
    for accepted in [
        "https://cdn.example.com/a/b.txt",
        "http://127.0.0.1:18080/flare-media/a?X-Amz-Signature=x",
        "HTTPS://CDN.EXAMPLE.COM/a",
    ] {
        assert!(is_absolute_http_url(accepted), "{accepted}");
    }
    for rejected in [
        "",
        "   ",
        "1200000000000000333/media/documents/2026/09/30/id.txt",
        "/flare-media/a.txt",
        "//cdn.example.com/a.txt",
        "https:cdn.example.com/a.txt",
        "https:/cdn.example.com/a.txt",
        "ftp://cdn.example.com/a.txt",
        "javascript:alert(1)",
        " https://cdn.example.com/a.txt",
        "https://cdn.example.com/a b.txt",
        "https://cdn.example.com/a\n.txt",
        "https://",
    ] {
        assert!(!is_absolute_http_url(rejected), "{rejected:?}");
    }
}
