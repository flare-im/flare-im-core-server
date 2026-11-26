use flare_server_core::error::{FlareError, Result};
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, set_signed_context_token,
};
use tokio_stream::{self as stream};
use tonic::Code;
use tonic::transport::{Channel, Uri};
use tracing::warn;

use crate::clients::Ctx;
use flare_grpc_proto::media::media_service_client::MediaServiceClient;
use flare_grpc_proto::media::*;

/// Users get a fresh Cell token per RPC. Staff forwards the original verified gateway
/// assertion, including its permission and lifetime; admin-gateway never re-signs employees.
#[derive(Clone)]
pub struct MediaCaller {
    ctx: Ctx,
    authority: MediaAuthority,
}
#[derive(Clone)]
enum MediaAuthority {
    User {
        signer: SignedContextSigner,
        identity: SignedContextIdentity,
    },
    Staff(crate::staff_context::VerifiedStaff),
}

impl MediaCaller {
    /// API Gateway 已验证令牌的终端用户；租户与用户只取自令牌主体。
    pub fn user(ctx: Ctx, signer: SignedContextSigner, tenant_id: &str, user_id: &str) -> Self {
        Self::new(ctx, signer, SignedActorType::User, tenant_id, user_id)
    }

    /// Requires an already verified Staff gateway assertion. No unsigned actor conversion.
    pub fn staff(ctx: Ctx) -> Result<Self> {
        let rejected = || {
            FlareError::localized(
                flare_server_core::error::ErrorCode::AuthenticationRequired,
                "verified gateway staff required for Media",
            )
        };
        let claims = crate::staff_context::staff_claims_of(&ctx).ok_or_else(rejected)?;
        if claims.kid != flare_server_core::auth::kid::GATEWAY {
            return Err(rejected());
        }
        let staff = crate::staff_context::VerifiedStaff::from_verified(claims, ctx.clone())
            .map_err(|_| rejected())?;
        if staff.token().is_empty() {
            return Err(rejected());
        }
        Ok(Self {
            ctx,
            authority: MediaAuthority::Staff(staff),
        })
    }

    fn new(
        ctx: Ctx,
        signer: SignedContextSigner,
        actor_type: SignedActorType,
        tenant_id: &str,
        actor_id: &str,
    ) -> Self {
        let request_id = Some(ctx.request_id())
            .filter(|id| {
                !id.is_empty()
                    && id.len() <= 128
                    && id.trim() == *id
                    && !id.chars().any(char::is_control)
            })
            .map(str::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let identity = SignedContextIdentity::new(actor_type, actor_id, tenant_id, request_id);
        Self {
            ctx,
            authority: MediaAuthority::User { signer, identity },
        }
    }

    pub fn ctx(&self) -> &Ctx {
        &self.ctx
    }

    pub fn actor_id(&self) -> &str {
        match &self.authority {
            MediaAuthority::User { identity, .. } => &identity.actor_id,
            MediaAuthority::Staff(staff) => &staff.actor_id,
        }
    }

    fn request<T>(&self, payload: T) -> Result<tonic::Request<T>> {
        let (signer, identity) = match &self.authority {
            MediaAuthority::User { signer, identity } => (signer, identity),
            MediaAuthority::Staff(staff) => {
                return crate::staff_context::staff_traced_request(
                    payload,
                    staff.token(),
                    &self.ctx,
                );
            }
        };
        let token = signer
            .sign(identity)
            .map_err(|_| FlareError::system("media caller identity signing failed"))?;
        let mut request = flare_server_core::request_with_context(payload, &self.ctx);
        set_signed_context_token(&mut request, &token)?;
        Ok(request)
    }
}

/// MediaService gRPC 客户端封装
#[derive(Clone)]
pub struct MediaServiceClientWrapper {
    client: MediaServiceClient<Channel>,
    current_url: String,
    fallback_url: Option<String>,
    fallback_client: std::sync::Arc<tokio::sync::OnceCell<MediaServiceClient<Channel>>>,
}

impl MediaServiceClientWrapper {
    async fn reconnect_to(&mut self, url: &str) -> Result<()> {
        let client = self
            .fallback_client
            .get_or_try_init(|| async {
                let uri: Uri = url.parse().map_err(|err| {
                    FlareError::system(format!("invalid media grpc uri {url}: {err}"))
                })?;
                let channel = Channel::builder(uri).connect().await.map_err(|err| {
                    FlareError::system(format!("connect media grpc {url}: {err}"))
                })?;
                Ok::<_, FlareError>(MediaServiceClient::new(channel))
            })
            .await?;
        self.client = client.clone();
        self.current_url = url.to_owned();
        Ok(())
    }

    fn client(&self) -> MediaServiceClient<Channel> {
        self.fallback_client.get().unwrap_or(&self.client).clone()
    }

    /// 流式上传文件
    pub async fn upload_file(
        &mut self,
        caller: &MediaCaller,
        requests: Vec<UploadFileRequest>,
    ) -> Result<UploadFileResponse> {
        let output = stream::iter(requests);
        let response = self.client().upload_file(caller.request(output)?).await?;
        Ok(response.into_inner())
    }

    /// 初始化分片上传
    pub async fn initiate_multipart_upload(
        &mut self,
        caller: &MediaCaller,
        request: InitiateMultipartUploadRequest,
    ) -> Result<InitiateMultipartUploadResponse> {
        match self
            .client()
            .initiate_multipart_upload(caller.request(request.clone())?)
            .await
        {
            Ok(resp) => Ok(resp.into_inner()),
            Err(status) if status.code() == Code::Unimplemented => {
                if let Some(fallback_url) = self.fallback_url.clone() {
                    warn!(
                        operation = "initiate_multipart_upload",
                        current_url = %self.current_url,
                        fallback_url = %fallback_url,
                        error = %status,
                        "media grpc returned UNIMPLEMENTED, retrying with fallback endpoint"
                    );
                    self.reconnect_to(&fallback_url).await?;
                    let response = self
                        .client()
                        .initiate_multipart_upload(caller.request(request)?)
                        .await?;
                    Ok(response.into_inner())
                } else {
                    Err(status.into())
                }
            }
            Err(status) => Err(status.into()),
        }
    }

    /// 上传分片
    pub async fn upload_multipart_chunk(
        &mut self,
        caller: &MediaCaller,
        request: UploadMultipartChunkRequest,
    ) -> Result<UploadMultipartChunkResponse> {
        match self
            .client()
            .upload_multipart_chunk(caller.request(request.clone())?)
            .await
        {
            Ok(resp) => Ok(resp.into_inner()),
            Err(status) if status.code() == Code::Unimplemented => {
                if let Some(fallback_url) = self.fallback_url.clone() {
                    warn!(
                        operation = "upload_multipart_chunk",
                        current_url = %self.current_url,
                        fallback_url = %fallback_url,
                        error = %status,
                        "media grpc returned UNIMPLEMENTED, retrying with fallback endpoint"
                    );
                    self.reconnect_to(&fallback_url).await?;
                    let response = self
                        .client()
                        .upload_multipart_chunk(caller.request(request)?)
                        .await?;
                    Ok(response.into_inner())
                } else {
                    Err(status.into())
                }
            }
            Err(status) => Err(status.into()),
        }
    }

    /// 完成分片上传
    pub async fn complete_multipart_upload(
        &mut self,
        caller: &MediaCaller,
        request: CompleteMultipartUploadRequest,
    ) -> Result<UploadFileResponse> {
        match self
            .client()
            .complete_multipart_upload(caller.request(request.clone())?)
            .await
        {
            Ok(resp) => Ok(resp.into_inner()),
            Err(status) if status.code() == Code::Unimplemented => {
                if let Some(fallback_url) = self.fallback_url.clone() {
                    warn!(
                        operation = "complete_multipart_upload",
                        current_url = %self.current_url,
                        fallback_url = %fallback_url,
                        error = %status,
                        "media grpc returned UNIMPLEMENTED, retrying with fallback endpoint"
                    );
                    self.reconnect_to(&fallback_url).await?;
                    let response = self
                        .client()
                        .complete_multipart_upload(caller.request(request)?)
                        .await?;
                    Ok(response.into_inner())
                } else {
                    Err(status.into())
                }
            }
            Err(status) => Err(status.into()),
        }
    }

    /// 中止分片上传
    pub async fn abort_multipart_upload(
        &mut self,
        caller: &MediaCaller,
        request: AbortMultipartUploadRequest,
    ) -> Result<AbortMultipartUploadResponse> {
        match self
            .client()
            .abort_multipart_upload(caller.request(request.clone())?)
            .await
        {
            Ok(resp) => Ok(resp.into_inner()),
            Err(status) if status.code() == Code::Unimplemented => {
                if let Some(fallback_url) = self.fallback_url.clone() {
                    warn!(
                        operation = "abort_multipart_upload",
                        current_url = %self.current_url,
                        fallback_url = %fallback_url,
                        error = %status,
                        "media grpc returned UNIMPLEMENTED, retrying with fallback endpoint"
                    );
                    self.reconnect_to(&fallback_url).await?;
                    let response = self
                        .client()
                        .abort_multipart_upload(caller.request(request)?)
                        .await?;
                    Ok(response.into_inner())
                } else {
                    Err(status.into())
                }
            }
            Err(status) => Err(status.into()),
        }
    }

    pub async fn initiate_direct_upload(
        &mut self,
        caller: &MediaCaller,
        request: InitiateDirectUploadRequest,
    ) -> Result<InitiateDirectUploadResponse> {
        let response = self
            .client()
            .initiate_direct_upload(caller.request(request)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn get_admin_upload_policy(
        &mut self,
        caller: &MediaCaller,
        request: AdminUploadPolicyRequest,
    ) -> Result<AdminUploadPolicyResponse> {
        Ok(self
            .client()
            .get_admin_upload_policy(caller.request(request)?)
            .await?
            .into_inner())
    }

    pub async fn admin_initiate_direct_upload(
        &mut self,
        caller: &MediaCaller,
        request: AdminInitiateDirectUploadRequest,
    ) -> Result<InitiateDirectUploadResponse> {
        Ok(self
            .client()
            .admin_initiate_direct_upload(caller.request(request)?)
            .await?
            .into_inner())
    }

    pub async fn get_direct_upload_status(
        &mut self,
        caller: &MediaCaller,
        request: GetDirectUploadStatusRequest,
    ) -> Result<GetDirectUploadStatusResponse> {
        let response = self
            .client()
            .get_direct_upload_status(caller.request(request)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn presign_direct_upload_parts(
        &mut self,
        caller: &MediaCaller,
        request: PresignDirectUploadPartsRequest,
    ) -> Result<PresignDirectUploadPartsResponse> {
        let response = self
            .client()
            .presign_direct_upload_parts(caller.request(request)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn commit_direct_upload_parts(
        &mut self,
        caller: &MediaCaller,
        request: CommitDirectUploadPartsRequest,
    ) -> Result<CommitDirectUploadPartsResponse> {
        let response = self
            .client()
            .commit_direct_upload_parts(caller.request(request)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn complete_direct_upload(
        &mut self,
        caller: &MediaCaller,
        request: CompleteDirectUploadRequest,
    ) -> Result<UploadFileResponse> {
        let response = self
            .client()
            .complete_direct_upload(caller.request(request)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn abort_direct_upload(
        &mut self,
        caller: &MediaCaller,
        request: AbortDirectUploadRequest,
    ) -> Result<AbortMultipartUploadResponse> {
        let response = self
            .client()
            .abort_direct_upload(caller.request(request)?)
            .await?;
        Ok(response.into_inner())
    }

    /// 创建新的客户端连接
    pub fn from_channel(channel: Channel, fallback_url: Option<String>) -> Self {
        Self {
            client: MediaServiceClient::new(channel),
            current_url: String::new(),
            fallback_url,
            fallback_client: std::sync::Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    /// 生成上传 URL
    pub async fn generate_upload_url(
        &mut self,
        caller: &MediaCaller,
        request: GenerateUploadUrlRequest,
    ) -> Result<GenerateUploadUrlResponse> {
        let response = self
            .client()
            .generate_upload_url(caller.request(request)?)
            .await?;
        Ok(response.into_inner())
    }

    /// 获取文件 URL（必须显式传播认证上下文）
    pub async fn get_file_url(
        &mut self,
        caller: &MediaCaller,
        request: GetFileUrlRequest,
    ) -> Result<GetFileUrlResponse> {
        let response = self.client().get_file_url(caller.request(request)?).await?;
        Ok(response.into_inner())
    }

    /// 获取文件信息（必须显式传播认证上下文）
    pub async fn get_file_info(
        &mut self,
        caller: &MediaCaller,
        request: GetFileInfoRequest,
    ) -> Result<GetFileInfoResponse> {
        let response = self
            .client()
            .get_file_info(caller.request(request)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn download_file(
        &mut self,
        caller: &MediaCaller,
        request: DownloadFileRequest,
    ) -> Result<tonic::Streaming<DownloadFileChunk>> {
        let response = self
            .client()
            .download_file(caller.request(request)?)
            .await?;
        Ok(response.into_inner())
    }

    /// 删除文件
    pub async fn delete_file(
        &mut self,
        caller: &MediaCaller,
        request: DeleteFileRequest,
    ) -> Result<DeleteFileResponse> {
        let response = self.client().delete_file(caller.request(request)?).await?;
        Ok(response.into_inner())
    }

    /// 创建引用
    pub async fn create_reference(
        &mut self,
        caller: &MediaCaller,
        request: CreateReferenceRequest,
    ) -> Result<CreateReferenceResponse> {
        let response = self
            .client()
            .create_reference(caller.request(request)?)
            .await?;
        Ok(response.into_inner())
    }

    /// 删除引用
    pub async fn delete_reference(
        &mut self,
        caller: &MediaCaller,
        request: DeleteReferenceRequest,
    ) -> Result<DeleteReferenceResponse> {
        let response = self
            .client()
            .delete_reference(caller.request(request)?)
            .await?;
        Ok(response.into_inner())
    }

    /// 列出引用
    pub async fn list_references(
        &mut self,
        caller: &MediaCaller,
        request: ListReferencesRequest,
    ) -> Result<ListReferencesResponse> {
        let response = self
            .client()
            .list_references(caller.request(request)?)
            .await?;
        Ok(response.into_inner())
    }

    /// 列出对象
    pub async fn list_objects(
        &mut self,
        caller: &MediaCaller,
        request: ListObjectsRequest,
    ) -> Result<ListObjectsResponse> {
        let response = self.client().list_objects(caller.request(request)?).await?;
        Ok(response.into_inner())
    }

    /// 图片处理
    pub async fn process_image(
        &mut self,
        caller: &MediaCaller,
        request: ProcessImageRequest,
    ) -> Result<ProcessImageResponse> {
        let response = self
            .client()
            .process_image(caller.request(request)?)
            .await?;
        Ok(response.into_inner())
    }

    /// 视频处理
    pub async fn process_video(
        &mut self,
        caller: &MediaCaller,
        request: ProcessVideoRequest,
    ) -> Result<ProcessVideoResponse> {
        let response = self
            .client()
            .process_video(caller.request(request)?)
            .await?;
        Ok(response.into_inner())
    }

    /// 清理孤儿资源
    pub async fn cleanup_orphaned_assets(
        &mut self,
        caller: &MediaCaller,
        request: CleanupOrphanedAssetsRequest,
    ) -> Result<CleanupOrphanedAssetsResponse> {
        let response = self
            .client()
            .cleanup_orphaned_assets(caller.request(request)?)
            .await?;
        Ok(response.into_inner())
    }

    /// 设置对象 ACL
    pub async fn set_object_acl(
        &mut self,
        caller: &MediaCaller,
        request: SetObjectAclRequest,
    ) -> Result<()> {
        self.client()
            .set_object_acl(caller.request(request)?)
            .await?;
        Ok(())
    }

    /// 描述桶
    pub async fn describe_bucket(
        &mut self,
        caller: &MediaCaller,
        request: DescribeBucketRequest,
    ) -> Result<DescribeBucketResponse> {
        let response = self
            .client()
            .describe_bucket(caller.request(request)?)
            .await?;
        Ok(response.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_server_core::SignedContextVerifier;
    use flare_server_core::context::Context;
    use flare_server_core::grpc::middleware::signed_context::SIGNED_CONTEXT_HEADER;

    fn keys() -> (SignedContextSigner, SignedContextVerifier) {
        let key = SigningKey::from_bytes(&[41; 32]);
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

    #[tokio::test]
    #[ignore] // 需要实际的 gRPC 服务
    async fn test_media_client_connection() {
        let channel = Channel::from_static("http://127.0.0.1:60081").connect_lazy();
        let _client = MediaServiceClientWrapper::from_channel(channel, None);
    }

    #[test]
    fn every_media_rpc_carries_a_fresh_token_for_the_chosen_identity_only() {
        let (signer, verifier) = keys();
        // Legacy identity fields in the request Context are never what Media trusts.
        let ctx: Ctx = std::sync::Arc::new(
            Context::with_request_id("req-1")
                .with_tenant_id("victim-tenant")
                .with_user_id("victim-user"),
        );
        let caller = MediaCaller::user(ctx, signer.clone(), "tenant-a", "user-a");
        let first = caller.request(()).unwrap();
        let second = caller.request(()).unwrap();
        let token = |request: &tonic::Request<()>| {
            request
                .metadata()
                .get(SIGNED_CONTEXT_HEADER)
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned()
        };
        let claims = verifier.verify(&token(&first)).unwrap();
        assert_eq!(claims.actor_type, SignedActorType::User);
        assert_eq!(
            (claims.tenant_id.as_str(), claims.actor_id.as_str()),
            ("tenant-a", "user-a")
        );
        assert_eq!(claims.request_id, "req-1");
        assert_ne!(verifier.verify(&token(&second)).unwrap().jti, claims.jti);

        assert!(
            MediaCaller::staff(std::sync::Arc::new(Context::with_request_id("forged"))).is_err()
        );
    }

    #[test]
    fn staff_forwards_the_verified_assertion_without_minting_another_identity() {
        let (signer, verifier) = keys();
        let signer = signer.with_kid("gateway").unwrap();
        let verifier = verifier.require_staff_kid(["gateway"]);
        let token = signer
            .sign(
                &SignedContextIdentity::new(
                    SignedActorType::Staff,
                    "employee",
                    "tenant-a",
                    "staff-request",
                )
                .with_staff_grant(None, Some("core:medias:write".into()), 7),
            )
            .unwrap();
        let mut carrier = tonic::Request::new(());
        set_signed_context_token(&mut carrier, &token).unwrap();
        let ctx =
            flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata(
                carrier.metadata(),
                &verifier,
            )
            .unwrap();
        let caller = MediaCaller::staff(ctx).unwrap();
        for _ in 0..2 {
            let request = caller.request(()).unwrap();
            assert_eq!(
                request
                    .metadata()
                    .get(SIGNED_CONTEXT_HEADER)
                    .unwrap()
                    .to_str()
                    .unwrap(),
                token
            );
        }
        assert_eq!(caller.actor_id(), "employee");
    }

    #[test]
    fn invalid_identity_fails_before_any_rpc() {
        let (signer, _) = keys();
        let caller = MediaCaller::user(
            std::sync::Arc::new(Context::with_request_id("req")),
            signer,
            "",
            "user-a",
        );
        assert!(caller.request(()).is_err());
    }
    async fn serve(router: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (url, task)
    }

    fn grpc_response(status: &'static str) -> axum::response::Response {
        axum::http::Response::builder()
            .header("content-type", "application/grpc")
            .header("grpc-status", status)
            .body(axum::body::Body::from(vec![0_u8; 5]))
            .unwrap()
    }

    fn caller() -> MediaCaller {
        MediaCaller::user(
            std::sync::Arc::new(Context::with_request_id("parallel-rpc")),
            keys().0,
            "app",
            "user",
        )
    }

    #[tokio::test]
    async fn slow_upload_does_not_block_a_query_or_hold_client_ownership_after_cancellation() {
        let entered = std::sync::Arc::new(tokio::sync::Notify::new());
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        let (signal, gate) = (entered.clone(), release.clone());
        let router = axum::Router::new().fallback(move |request: axum::extract::Request| {
            let (signal, gate) = (signal.clone(), gate.clone());
            async move {
                if request.uri().path().ends_with("/UploadFile") {
                    signal.notify_one();
                    gate.notified().await;
                }
                grpc_response("0")
            }
        });
        let (url, server) = serve(router).await;
        let shared = std::sync::Arc::new(tokio::sync::Mutex::new(
            MediaServiceClientWrapper::from_channel(
                Channel::from_shared(url).unwrap().connect().await.unwrap(),
                None,
            ),
        ));
        let upload_client = shared.clone();
        let upload = tokio::spawn(async move {
            let mut client = upload_client.lock().await.clone();
            client.upload_file(&caller(), vec![]).await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let mut client = shared.lock().await.clone();
            client
                .get_file_url(&caller(), GetFileUrlRequest::default())
                .await
        })
        .await
        .unwrap();
        result.unwrap();
        upload.abort();
        assert!(upload.await.unwrap_err().is_cancelled());
        let mut client = shared.lock().await.clone();
        client
            .get_file_url(&caller(), GetFileUrlRequest::default())
            .await
            .unwrap();
        release.notify_one();
        server.abort();
    }

    #[tokio::test]
    async fn endpoint_fallback_is_shared_with_clones_created_before_the_switch() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let fallback_calls = Arc::new(AtomicUsize::new(0));
        let first = primary_calls.clone();
        let (primary, primary_server) = serve(axum::Router::new().fallback(move || {
            first.fetch_add(1, Ordering::SeqCst);
            async { grpc_response("12") }
        }))
        .await;
        let next = fallback_calls.clone();
        let (fallback, fallback_server) = serve(axum::Router::new().fallback(move || {
            next.fetch_add(1, Ordering::SeqCst);
            async { grpc_response("0") }
        }))
        .await;
        let mut client = MediaServiceClientWrapper::from_channel(
            Channel::from_shared(primary)
                .unwrap()
                .connect()
                .await
                .unwrap(),
            Some(fallback),
        );
        let mut old_clone = client.clone();
        client
            .initiate_multipart_upload(&caller(), InitiateMultipartUploadRequest::default())
            .await
            .unwrap();
        old_clone
            .get_file_url(&caller(), GetFileUrlRequest::default())
            .await
            .unwrap();
        assert_eq!(primary_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fallback_calls.load(Ordering::SeqCst), 2);
        primary_server.abort();
        fallback_server.abort();
    }
}
