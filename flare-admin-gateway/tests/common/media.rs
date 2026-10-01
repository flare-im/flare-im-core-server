//! 媒资管理路由（SEC-05 迁入）的替身 Media 服务与夹具。
//!
//! 与 Conversation / Online / Capability 不同，Media 不认转发的 admin 令牌，只认 Cell 私钥
//! 签的内部上下文（`x-flare-ctx`）。替身记下每次调用收到的签名上下文、`authorization`
//! 与全部 `x-*` 元数据，测试再用 Cell 公钥验签，断言身份只来自网关验过的 admin 令牌。
//!
//! 替身按输入给出固定回答：`denied` / `down` / `missing` / `unsupported` / `rejected`
//! 分别回 PERMISSION_DENIED / UNAVAILABLE / NOT_FOUND / UNKNOWN（真实 Media 对未实现的
//! 对象清单与 ACL 就回 UNKNOWN）/ UNAUTHENTICATED，错误文案都带 `id-secret-1`，用来断言
//! 网关不回显。清理接口只有数字字段，故用 `limit` 取值 403/503/500/401 选择同样的失败。

use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::http::Request;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_admin_gateway::interface::http::{MediaContextSigner, create_admin_router};
use flare_grpc_proto::media::media_service_server::{MediaService, MediaServiceServer};
use flare_grpc_proto::media::*;
use flare_server_core::{SignedContextLayer, SignedContextSigner, SignedContextVerifier};
use tonic::transport::Channel;
use tonic::{Request as GrpcRequest, Response as GrpcResponse, Status, Streaming};

use super::{Console, EMPLOYEE, TENANT, WRITE_HEADERS, clients};

pub const SECRET_MARKER: &str = "id-secret-1";

/// 一次 Media 调用：方法名、`x-flare-ctx` 的全部取值、`authorization`、全部 `x-*` 元数据、请求摘要。
#[derive(Debug, Clone)]
pub struct MediaCall {
    pub method: &'static str,
    pub signed_context: Vec<String>,
    pub authorization: Vec<String>,
    pub x_metadata: Vec<(String, String)>,
    pub request: String,
}

#[derive(Clone, Default)]
pub struct MediaRecorder(Arc<Mutex<Vec<MediaCall>>>);

impl MediaRecorder {
    fn record<T>(&self, method: &'static str, request: &GrpcRequest<T>, summary: String) {
        let metadata = request.metadata();
        let values = |name: &str| -> Vec<String> {
            metadata
                .get_all(name)
                .iter()
                .map(|value| value.to_str().unwrap_or("<non-ascii>").to_string())
                .collect()
        };
        let x_metadata = metadata
            .keys()
            .filter_map(|key| match key {
                tonic::metadata::KeyRef::Ascii(key) => Some(key.as_str().to_string()),
                tonic::metadata::KeyRef::Binary(_) => None,
            })
            .filter(|key| key.starts_with("x-"))
            .flat_map(|key| {
                values(&key)
                    .into_iter()
                    .map(move |value| (key.clone(), value))
            })
            .collect();
        self.0.lock().unwrap().push(MediaCall {
            method,
            signed_context: values("x-flare-ctx"),
            authorization: values("authorization"),
            x_metadata,
            request: summary,
        });
    }

    pub fn calls(&self) -> Vec<MediaCall> {
        self.0.lock().unwrap().clone()
    }

    pub fn clear(&self) {
        self.0.lock().unwrap().clear();
    }
}

fn failure(key: &str) -> Option<Status> {
    let detail = format!("backend detail {SECRET_MARKER}");
    match key {
        "denied" => Some(Status::permission_denied(detail)),
        "down" => Some(Status::unavailable(detail)),
        "missing" => Some(Status::not_found(detail)),
        "unsupported" => Some(Status::unknown(detail)),
        "rejected" => Some(Status::unauthenticated(detail)),
        _ => None,
    }
}

fn cleanup_failure(limit: u32) -> Option<Status> {
    failure(match limit {
        403 => "denied",
        503 => "down",
        500 => "unsupported",
        401 => "rejected",
        _ => "",
    })
}

fn unimplemented<T>() -> Result<GrpcResponse<T>, Status> {
    Err(Status::unimplemented("not used by admin-gateway"))
}

fn file(file_id: &str) -> FileInfo {
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("owner_id".to_string(), "u-owner".to_string());
    FileInfo {
        file_id: file_id.to_string(),
        file_name: format!("{file_id}.png"),
        mime_type: "image/png".to_string(),
        size: 2048,
        url: format!("https://s3.internal/{TENANT}/{file_id}?X-Amz-Signature=sig"),
        cdn_url: format!("https://cdn.internal/{TENANT}/{file_id}"),
        created_at: Some((UNIX_EPOCH + Duration::from_secs(1_700_000_000)).into()),
        metadata,
        reference_count: 1,
        status: "active".to_string(),
        sha256: "ab".repeat(32),
        md5: "cd".repeat(16),
        grace_expires_at: None,
        bucket: "media".to_string(),
        object_key: format!("{TENANT}/2026/{file_id}.png"),
        tenant: TENANT.to_string(),
    }
}

#[derive(Clone, Default)]
pub struct MediaFake {
    pub recorder: MediaRecorder,
}

#[tonic::async_trait]
impl MediaService for MediaFake {
    async fn cleanup_orphaned_assets(
        &self,
        request: GrpcRequest<CleanupOrphanedAssetsRequest>,
    ) -> Result<GrpcResponse<CleanupOrphanedAssetsResponse>, Status> {
        let r = *request.get_ref();
        let summary = format!("limit={} dry_run={}", r.limit, r.dry_run);
        self.recorder
            .record("CleanupOrphanedAssets", &request, summary);
        if let Some(status) = cleanup_failure(r.limit) {
            return Err(status);
        }
        Ok(GrpcResponse::new(CleanupOrphanedAssetsResponse {
            file_ids: vec!["f-orphan-1".to_string(), "f-orphan-2".to_string()],
            success: true,
            error_message: String::new(),
            scanned: 2,
        }))
    }

    async fn set_object_acl(
        &self,
        request: GrpcRequest<SetObjectAclRequest>,
    ) -> Result<GrpcResponse<()>, Status> {
        let r = request.get_ref().clone();
        let entries: Vec<String> = r
            .entries
            .iter()
            .map(|entry| format!("{}:{}", entry.principal, entry.permissions.join("+")))
            .collect();
        let summary = format!("file_id={} entries={}", r.file_id, entries.join(","));
        self.recorder.record("SetObjectAcl", &request, summary);
        if let Some(status) = failure(&r.file_id) {
            return Err(status);
        }
        Ok(GrpcResponse::new(()))
    }

    async fn list_objects(
        &self,
        request: GrpcRequest<ListObjectsRequest>,
    ) -> Result<GrpcResponse<ListObjectsResponse>, Status> {
        let r = request.get_ref().clone();
        let summary = format!(
            "bucket={} prefix={} pagination={} filters={} sort={}",
            r.bucket,
            r.prefix,
            r.pagination.is_some(),
            r.filters.len(),
            r.sort.len()
        );
        self.recorder.record("ListObjects", &request, summary);
        if let Some(status) = failure(&r.bucket) {
            return Err(status);
        }
        Ok(GrpcResponse::new(ListObjectsResponse {
            files: vec![file("f-1"), file("f-2")],
            pagination: None,
        }))
    }

    async fn describe_bucket(
        &self,
        request: GrpcRequest<DescribeBucketRequest>,
    ) -> Result<GrpcResponse<DescribeBucketResponse>, Status> {
        let bucket = request.get_ref().bucket.clone();
        self.recorder
            .record("DescribeBucket", &request, format!("bucket={bucket}"));
        if let Some(status) = failure(&bucket) {
            return Err(status);
        }
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("tenant_prefix".to_string(), format!("{TENANT}/"));
        Ok(GrpcResponse::new(DescribeBucketResponse {
            bucket,
            region: "local".to_string(),
            storage_class: "STANDARD".to_string(),
            versioning_enabled: false,
            metadata,
        }))
    }

    async fn upload_file(
        &self,
        _: GrpcRequest<Streaming<UploadFileRequest>>,
    ) -> Result<GrpcResponse<UploadFileResponse>, Status> {
        unimplemented()
    }
    async fn initiate_multipart_upload(
        &self,
        _: GrpcRequest<InitiateMultipartUploadRequest>,
    ) -> Result<GrpcResponse<InitiateMultipartUploadResponse>, Status> {
        unimplemented()
    }
    async fn upload_multipart_chunk(
        &self,
        _: GrpcRequest<UploadMultipartChunkRequest>,
    ) -> Result<GrpcResponse<UploadMultipartChunkResponse>, Status> {
        unimplemented()
    }
    async fn complete_multipart_upload(
        &self,
        _: GrpcRequest<CompleteMultipartUploadRequest>,
    ) -> Result<GrpcResponse<UploadFileResponse>, Status> {
        unimplemented()
    }
    async fn abort_multipart_upload(
        &self,
        _: GrpcRequest<AbortMultipartUploadRequest>,
    ) -> Result<GrpcResponse<AbortMultipartUploadResponse>, Status> {
        unimplemented()
    }
    async fn initiate_direct_upload(
        &self,
        _: GrpcRequest<InitiateDirectUploadRequest>,
    ) -> Result<GrpcResponse<InitiateDirectUploadResponse>, Status> {
        unimplemented()
    }
    async fn get_direct_upload_status(
        &self,
        _: GrpcRequest<GetDirectUploadStatusRequest>,
    ) -> Result<GrpcResponse<GetDirectUploadStatusResponse>, Status> {
        unimplemented()
    }
    async fn presign_direct_upload_parts(
        &self,
        _: GrpcRequest<PresignDirectUploadPartsRequest>,
    ) -> Result<GrpcResponse<PresignDirectUploadPartsResponse>, Status> {
        unimplemented()
    }
    async fn commit_direct_upload_parts(
        &self,
        _: GrpcRequest<CommitDirectUploadPartsRequest>,
    ) -> Result<GrpcResponse<CommitDirectUploadPartsResponse>, Status> {
        unimplemented()
    }
    async fn complete_direct_upload(
        &self,
        _: GrpcRequest<CompleteDirectUploadRequest>,
    ) -> Result<GrpcResponse<UploadFileResponse>, Status> {
        unimplemented()
    }
    async fn abort_direct_upload(
        &self,
        _: GrpcRequest<AbortDirectUploadRequest>,
    ) -> Result<GrpcResponse<AbortMultipartUploadResponse>, Status> {
        unimplemented()
    }
    async fn create_reference(
        &self,
        _: GrpcRequest<CreateReferenceRequest>,
    ) -> Result<GrpcResponse<CreateReferenceResponse>, Status> {
        unimplemented()
    }
    async fn delete_reference(
        &self,
        _: GrpcRequest<DeleteReferenceRequest>,
    ) -> Result<GrpcResponse<DeleteReferenceResponse>, Status> {
        unimplemented()
    }
    async fn list_references(
        &self,
        _: GrpcRequest<ListReferencesRequest>,
    ) -> Result<GrpcResponse<ListReferencesResponse>, Status> {
        unimplemented()
    }
    async fn get_file_url(
        &self,
        _: GrpcRequest<GetFileUrlRequest>,
    ) -> Result<GrpcResponse<GetFileUrlResponse>, Status> {
        unimplemented()
    }
    async fn get_file_info(
        &self,
        _: GrpcRequest<GetFileInfoRequest>,
    ) -> Result<GrpcResponse<GetFileInfoResponse>, Status> {
        unimplemented()
    }

    type DownloadFileStream = tonic::codegen::BoxStream<DownloadFileChunk>;

    async fn download_file(
        &self,
        _: GrpcRequest<DownloadFileRequest>,
    ) -> Result<GrpcResponse<Self::DownloadFileStream>, Status> {
        unimplemented()
    }
    async fn delete_file(
        &self,
        _: GrpcRequest<DeleteFileRequest>,
    ) -> Result<GrpcResponse<DeleteFileResponse>, Status> {
        unimplemented()
    }
    async fn process_image(
        &self,
        _: GrpcRequest<ProcessImageRequest>,
    ) -> Result<GrpcResponse<ProcessImageResponse>, Status> {
        unimplemented()
    }
    async fn process_video(
        &self,
        _: GrpcRequest<ProcessVideoRequest>,
    ) -> Result<GrpcResponse<ProcessVideoResponse>, Status> {
        unimplemented()
    }
    async fn generate_upload_url(
        &self,
        _: GrpcRequest<GenerateUploadUrlRequest>,
    ) -> Result<GrpcResponse<GenerateUploadUrlResponse>, Status> {
        unimplemented()
    }
}

/// Cell 内部上下文密钥对：私钥给网关签，公钥给 Media（及测试）验。
pub fn cell_keys(seed: u8) -> (SignedContextSigner, SignedContextVerifier) {
    let key = SigningKey::from_bytes(&[seed; 32]);
    let private = key.to_pkcs8_pem(Default::default()).unwrap();
    let public = key
        .verifying_key()
        .to_public_key_pem(Default::default())
        .unwrap();
    (
        SignedContextSigner::from_private_key(&private).unwrap(),
        SignedContextVerifier::from_public_key(&public).unwrap(),
    )
}

/// 起一个只绑 127.0.0.1 的替身 Media。`media_verifier` 为 `Some` 时套上 Media 实际使用的
/// `SignedContextLayer`（签名不对就在层内 UNAUTHENTICATED，替身 handler 不会被调用）。
pub async fn spawn_media(
    media_verifier: Option<SignedContextVerifier>,
) -> (std::net::SocketAddr, MediaRecorder) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    assert!(address.ip().is_loopback());
    let fake = MediaFake::default();
    let recorder = fake.recorder.clone();
    let incoming = tonic::transport::server::TcpIncoming::from(listener);
    let mut server = tonic::transport::Server::builder();
    match media_verifier {
        Some(verifier) => {
            tokio::spawn(
                server
                    .add_service(
                        SignedContextLayer::new(verifier).layer(MediaServiceServer::new(fake)),
                    )
                    .serve_with_incoming(incoming),
            );
        }
        None => {
            tokio::spawn(
                server
                    .add_service(MediaServiceServer::new(fake))
                    .serve_with_incoming(incoming),
            );
        }
    }
    (address, recorder)
}

pub struct MediaHarness {
    pub router: Router,
    pub media: MediaRecorder,
    pub console: Console,
    pub token: String,
    /// 与网关签名私钥配对的 Cell 公钥。
    pub cell: SignedContextVerifier,
}

/// `signer=false` 模拟网关缺 `FLARE_INTERNAL_CTX_PRIVATE_KEY`；`media_verifier` 见 [`spawn_media`]。
pub async fn media_harness(
    signer: bool,
    media_verifier: Option<SignedContextVerifier>,
) -> MediaHarness {
    let (address, media) = spawn_media(media_verifier).await;
    let media_channel = Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_lazy();
    let unused = Channel::from_static("http://127.0.0.1:9").connect_lazy();
    let (cell_signer, cell) = cell_keys(62);
    let console = Console::new();
    let router = create_admin_router(
        Arc::new(clients(unused.clone(), media_channel, unused)),
        None,
    )
    .layer(axum::Extension(Arc::new(console.verifier.clone())))
    .layer(axum::Extension(MediaContextSigner(
        signer.then_some(cell_signer),
    )));
    let token = console.admin_token(EMPLOYEE, TENANT);
    MediaHarness {
        router,
        media,
        console,
        token,
        cell,
    }
}

pub const CLEANUP: &str = "/api/v1/admin/medias/cleanup-orphaned-assets";
pub const OBJECT_ACL: &str = "/api/v1/admin/medias/object-acl";
pub const OBJECTS: &str = "/api/v1/admin/medias/objects";
pub const BUCKET: &str = "/api/v1/admin/medias/bucket";

pub fn cleanup_body(limit: u32) -> String {
    format!(r#"{{"limit":{limit},"dry_run":false}}"#)
}

pub fn acl_body(file_id: &str) -> String {
    format!(
        r#"{{"file_id":"{file_id}","entries":[{{"principal":"role:auditor","permissions":["read"]}},{{"principal":"user:u-owner","permissions":["read","write"]}}]}}"#
    )
}

/// SEC-05 迁入的四条媒资路由：(方法, 路径与查询, 请求体)。写操作是两个 POST。
pub fn media_routes() -> Vec<(&'static str, String, Option<String>)> {
    vec![
        ("POST", CLEANUP.to_string(), Some(cleanup_body(50))),
        ("GET", format!("{OBJECTS}?bucket=media&prefix=2026/"), None),
        ("POST", OBJECT_ACL.to_string(), Some(acl_body("f-1"))),
        ("GET", format!("{BUCKET}?bucket=media"), None),
    ]
}

/// 带 JSON 请求体的请求构造器；`body=None` 时与 [`super::request`] 相同。
pub fn media_request(
    method: &str,
    path: &str,
    token: Option<&str>,
    tenant: Option<&str>,
    extra: &[(&str, &str)],
    body: Option<&str>,
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    if let Some(tenant) = tenant {
        builder = builder.header("x-tenant-id", tenant);
    }
    for (name, value) in extra {
        builder = builder.header(*name, *value);
    }
    match body {
        Some(body) => builder
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    }
}

/// 管理员请求：写路由自动带齐审计头。
pub fn media_admin_request(
    method: &str,
    path: &str,
    token: &str,
    body: Option<&str>,
) -> Request<Body> {
    let extra = if method == "POST" { WRITE_HEADERS } else { &[] };
    media_request(method, path, Some(token), Some(TENANT), extra, body)
}
