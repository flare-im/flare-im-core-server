//! 上传类拒绝经 gRPC 往返后仍要能被网关识别成调用方错误（404/400），而不是 Unknown/500。
//! 网关的映射只看往返后的错误码，所以码本身就是 Media 与网关之间的契约。
use super::isolation_tests::Sessions;
use super::*;
use std::sync::Arc;

const MAX_CHUNK: i64 = 16;

struct Fixture {
    service: MediaService,
    temporary: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temporary =
            std::env::temp_dir().join(format!("flare-media-upload-reject-{}", Uuid::new_v4()));
        let service = MediaService::new(
            None,
            None,
            None,
            None,
            Some(Arc::new(Sessions::default())),
            None,
            MediaDomainConfig::new(3600, None, 60, temporary.clone(), 3600, MAX_CHUNK),
        );
        Self { service, temporary }
    }

    async fn session(&self, ctx: &Context) -> String {
        self.service
            .initiate_multipart_upload(
                ctx,
                MultipartUploadInit {
                    file_name: "x.txt".to_string(),
                    mime_type: "text/plain".to_string(),
                    file_type: "file".to_string(),
                    file_size: Some(MAX_CHUNK),
                    chunk_size: MAX_CHUNK,
                    user_id: String::new(),
                    namespace: None,
                    business_tag: None,
                    trace_id: None,
                    metadata: HashMap::new(),
                },
            )
            .await
            .unwrap()
            .upload_id
    }

    async fn chunk(&self, ctx: &Context, upload_id: &str, size: usize) -> tonic::Code {
        let err = self
            .service
            .upload_multipart_chunk(
                ctx,
                MultipartChunkPayload {
                    upload_id: upload_id.to_string(),
                    chunk_index: 0,
                    bytes: vec![7; size],
                },
            )
            .await
            .unwrap_err();
        tonic::Status::from(err).code()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.temporary).expect("remove this test's temporary chunks");
    }
}

fn ctx(tenant: &str, user: &str) -> Context {
    super::isolation_tests::ctx(tenant, user)
}

#[tokio::test]
async fn oversized_and_empty_chunks_are_invalid_arguments() {
    let f = Fixture::new();
    let owner = ctx("tenant-a", "owner");
    let upload_id = f.session(&owner).await;
    // 修复前超限分片是 MessageTooLarge，没有对应的 gRPC 码，往返后成了 Unknown。
    assert_eq!(
        f.chunk(&owner, &upload_id, MAX_CHUNK as usize + 1).await,
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        f.chunk(&owner, &upload_id, 0).await,
        tonic::Code::InvalidArgument
    );
}

#[tokio::test]
async fn sessions_of_other_callers_are_not_found() {
    let f = Fixture::new();
    let upload_id = f.session(&ctx("tenant-a", "owner")).await;
    for stranger in [ctx("tenant-a", "other"), ctx("tenant-b", "owner")] {
        assert_eq!(
            f.chunk(&stranger, &upload_id, 1).await,
            tonic::Code::NotFound
        );
    }
    let missing = Uuid::new_v4().to_string();
    assert_eq!(
        f.chunk(&ctx("tenant-a", "owner"), &missing, 1).await,
        tonic::Code::NotFound
    );
}
