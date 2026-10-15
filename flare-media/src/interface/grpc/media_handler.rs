use std::pin::Pin;
use std::sync::Arc;

use flare_grpc_proto::media::media_service_server::MediaService;
use flare_grpc_proto::media::media_service_server::MediaServiceServer;
use flare_grpc_proto::media::upload_file_request;
use flare_grpc_proto::media::{
    AbortDirectUploadRequest, AbortMultipartUploadRequest, AbortMultipartUploadResponse,
    CleanupOrphanedAssetsRequest, CleanupOrphanedAssetsResponse, CommitDirectUploadPartsRequest,
    CommitDirectUploadPartsResponse, CompleteDirectUploadRequest, CompleteMultipartUploadRequest,
    CreateReferenceRequest, CreateReferenceResponse, DeleteFileRequest, DeleteFileResponse,
    DeleteReferenceRequest, DeleteReferenceResponse, DescribeBucketRequest, DescribeBucketResponse,
    DownloadFileChunk, DownloadFileRequest, GenerateUploadUrlRequest, GenerateUploadUrlResponse,
    GetDirectUploadStatusRequest, GetDirectUploadStatusResponse, GetFileInfoRequest,
    GetFileInfoResponse, GetFileUrlRequest, GetFileUrlResponse, InitiateDirectUploadRequest,
    InitiateDirectUploadResponse, InitiateMultipartUploadRequest, InitiateMultipartUploadResponse,
    ListObjectsRequest, ListObjectsResponse, ListReferencesRequest, ListReferencesResponse,
    PresignDirectUploadPartsRequest, PresignDirectUploadPartsResponse, ProcessImageRequest,
    ProcessImageResponse, ProcessVideoRequest, ProcessVideoResponse, SetObjectAclRequest,
    UploadFileRequest, UploadFileResponse, UploadMultipartChunkRequest,
    UploadMultipartChunkResponse,
};
use flare_im_service_kit::audit_effects::durable::StaffAudit;
use flare_server_core::auth::core_admin::permission::{MEDIAS_READ, MEDIAS_WRITE};
use flare_server_core::context::Ctx;
use flare_server_core::error::grpc::IntoGrpc;
use flare_server_core::{
    SignedContextLayer, SignedContextService, SignedContextVerifier, VerifiedContext,
};
use prost_types::Timestamp;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};
use tracing::instrument;

use crate::application::handlers::{MediaCommandHandler, MediaQueryHandler};
use crate::application::utils::{to_proto_file_info, to_proto_reference};
use crate::domain::model::MediaReferenceScope;

#[derive(Clone)]
pub struct MediaGrpcHandler {
    command_handler: Arc<MediaCommandHandler>,
    query_handler: Arc<MediaQueryHandler>,
}

impl MediaGrpcHandler {
    pub fn new(
        command_handler: Arc<MediaCommandHandler>,
        query_handler: Arc<MediaQueryHandler>,
    ) -> Self {
        Self {
            command_handler,
            query_handler,
        }
    }
}

#[tonic::async_trait]
impl MediaService for MediaGrpcHandler {
    type DownloadFileStream = Pin<
        Box<dyn tokio_stream::Stream<Item = Result<DownloadFileChunk, Status>> + Send + 'static>,
    >;

    #[instrument(skip(self, request))]
    async fn upload_file(
        &self,
        request: Request<tonic::Streaming<UploadFileRequest>>,
    ) -> Result<Response<UploadFileResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let tenant_id = upload_tenant(&ctx)?;

        let mut stream = request.into_inner();
        let first = stream
            .next()
            .await
            .ok_or_else(|| status_internal("upload stream empty"))?
            .map_err(status_internal)?;

        let (mut upload_metadata, mut payload) = match first.request {
            Some(upload_file_request::Request::Metadata(metadata)) => (metadata, Vec::new()),
            _ => return Err(status_internal("first upload frame must contain metadata")),
        };

        // 将 tenant_id 添加到 metadata 中（如果未设置）
        if !upload_metadata.metadata.contains_key("tenant_id") {
            upload_metadata
                .metadata
                .insert("tenant_id".to_string(), tenant_id);
        }

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(status_internal)?;
            match chunk.request {
                Some(upload_file_request::Request::ChunkData(data)) => {
                    payload.extend_from_slice(&data);
                }
                Some(upload_file_request::Request::Metadata(_)) => {
                    return Err(status_internal("metadata frame must only appear once"));
                }
                None => {}
            }
        }

        let metadata = self
            .command_handler
            .handle_upload_file(&ctx, upload_metadata, payload)
            .await
            .into_grpc()?;
        // 上传完成后，返回预签名URL
        let presigned = self
            .query_handler
            .handle_get_file_url(
                &ctx,
                flare_grpc_proto::media::GetFileUrlRequest {
                    file_id: metadata.file_id.clone(),
                    expires_in: 0, // 使用服务默认TTL
                    download: false,
                    response_headers: Default::default(),
                    retention_protected: false,
                    content_visibility: flare_proto::common::ContentVisibility::Available as i32,
                },
            )
            .await
            .into_grpc()?;
        Ok(Response::new(UploadFileResponse {
            file_id: metadata.file_id.clone(),
            url: presigned.url,
            cdn_url: presigned.cdn_url,
            info: Some(to_proto_file_info(&metadata)),
            success: true,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn initiate_multipart_upload(
        &self,
        request: Request<InitiateMultipartUploadRequest>,
    ) -> Result<Response<InitiateMultipartUploadResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();
        let session = self
            .command_handler
            .handle_initiate_multipart_upload(&ctx, req)
            .await
            .into_grpc()?;

        Ok(Response::new(InitiateMultipartUploadResponse {
            upload_id: session.upload_id,
            chunk_size: session.chunk_size,
            expires_at: Some(to_proto_timestamp(session.expires_at)),
            success: true,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn upload_multipart_chunk(
        &self,
        request: Request<UploadMultipartChunkRequest>,
    ) -> Result<Response<UploadMultipartChunkResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();
        let chunk_index = req.chunk_index;
        let session = self
            .command_handler
            .handle_upload_multipart_chunk(&ctx, req)
            .await
            .into_grpc()?;

        Ok(Response::new(UploadMultipartChunkResponse {
            upload_id: session.upload_id,
            chunk_index,
            uploaded_size: session.uploaded_size as u64,
            expires_at: Some(to_proto_timestamp(session.expires_at)),
            success: true,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn complete_multipart_upload(
        &self,
        request: Request<CompleteMultipartUploadRequest>,
    ) -> Result<Response<UploadFileResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();
        let metadata = self
            .command_handler
            .handle_complete_multipart_upload(&ctx, req)
            .await
            .into_grpc()?;
        // 完成分片上传后也返回预签名URL
        let presigned = self
            .query_handler
            .handle_get_file_url(
                &ctx,
                flare_grpc_proto::media::GetFileUrlRequest {
                    file_id: metadata.file_id.clone(),
                    expires_in: 0, // 使用服务默认TTL
                    download: false,
                    response_headers: Default::default(),
                    retention_protected: false,
                    content_visibility: flare_proto::common::ContentVisibility::Available as i32,
                },
            )
            .await
            .into_grpc()?;
        Ok(Response::new(UploadFileResponse {
            file_id: metadata.file_id.clone(),
            url: presigned.url,
            cdn_url: presigned.cdn_url,
            info: Some(to_proto_file_info(&metadata)),
            success: true,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn abort_multipart_upload(
        &self,
        request: Request<AbortMultipartUploadRequest>,
    ) -> Result<Response<AbortMultipartUploadResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();
        self.command_handler
            .handle_abort_multipart_upload(&ctx, req)
            .await
            .into_grpc()?;

        Ok(Response::new(AbortMultipartUploadResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn initiate_direct_upload(
        &self,
        request: Request<InitiateDirectUploadRequest>,
    ) -> Result<Response<InitiateDirectUploadResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();
        let session = self
            .command_handler
            .handle_initiate_direct_upload(&ctx, req)
            .await
            .into_grpc()?;
        Ok(Response::new(to_initiate_direct_upload_response(session)))
    }

    #[instrument(skip(self, request))]
    async fn get_direct_upload_status(
        &self,
        request: Request<GetDirectUploadStatusRequest>,
    ) -> Result<Response<GetDirectUploadStatusResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();
        let response = self
            .query_handler
            .handle_get_direct_upload_status(&ctx, req)
            .await
            .into_grpc()?;
        Ok(Response::new(response))
    }

    #[instrument(skip(self, request))]
    async fn presign_direct_upload_parts(
        &self,
        request: Request<PresignDirectUploadPartsRequest>,
    ) -> Result<Response<PresignDirectUploadPartsResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();
        let response = self
            .query_handler
            .handle_presign_direct_upload_parts(&ctx, req)
            .await
            .into_grpc()?;
        Ok(Response::new(response))
    }

    #[instrument(skip(self, request))]
    async fn commit_direct_upload_parts(
        &self,
        request: Request<CommitDirectUploadPartsRequest>,
    ) -> Result<Response<CommitDirectUploadPartsResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();
        let session = self
            .command_handler
            .handle_commit_direct_upload_parts(&ctx, req)
            .await
            .into_grpc()?;
        Ok(Response::new(CommitDirectUploadPartsResponse {
            committed_parts: session
                .uploaded_parts
                .iter()
                .map(|part| part.part_number)
                .collect(),
            uploaded_size: session.uploaded_size,
            success: true,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn complete_direct_upload(
        &self,
        request: Request<CompleteDirectUploadRequest>,
    ) -> Result<Response<UploadFileResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();
        let metadata = self
            .command_handler
            .handle_complete_direct_upload(&ctx, req)
            .await
            .into_grpc()?;
        let presigned = self
            .query_handler
            .handle_get_file_url(
                &ctx,
                flare_grpc_proto::media::GetFileUrlRequest {
                    file_id: metadata.file_id.clone(),
                    expires_in: 0,
                    download: false,
                    response_headers: Default::default(),
                    retention_protected: false,
                    content_visibility: flare_proto::common::ContentVisibility::Available as i32,
                },
            )
            .await
            .into_grpc()?;
        Ok(Response::new(UploadFileResponse {
            file_id: metadata.file_id.clone(),
            url: presigned.url,
            cdn_url: presigned.cdn_url,
            info: Some(to_proto_file_info(&metadata)),
            success: true,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn abort_direct_upload(
        &self,
        request: Request<AbortDirectUploadRequest>,
    ) -> Result<Response<AbortMultipartUploadResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();
        self.command_handler
            .handle_abort_direct_upload(&ctx, req)
            .await
            .into_grpc()?;
        Ok(Response::new(AbortMultipartUploadResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn create_reference(
        &self,
        request: Request<CreateReferenceRequest>,
    ) -> Result<Response<CreateReferenceResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();
        let metadata = req.metadata;

        if req.file_id.is_empty() {
            return Err(status_invalid_argument("file_id is required"));
        }
        if req.owner_id.is_empty() {
            return Err(status_invalid_argument("owner_id is required"));
        }

        let namespace = if req.namespace.is_empty() {
            metadata
                .get("namespace")
                .cloned()
                .unwrap_or_else(|| req.owner_id.clone())
        } else {
            req.namespace.clone()
        };

        let business_tag = if req.business_tag.is_empty() {
            metadata.get("business_tag").cloned()
        } else {
            Some(req.business_tag.clone())
        };

        let scope = MediaReferenceScope {
            namespace,
            owner_id: req.owner_id.clone(),
            business_tag,
        };

        let file_metadata = self
            .command_handler
            .handle_attach_reference(&ctx, &req.file_id, scope, metadata)
            .await
            .into_grpc()?;

        Ok(Response::new(CreateReferenceResponse {
            info: Some(to_proto_file_info(&file_metadata)),
            success: true,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn delete_reference(
        &self,
        request: Request<DeleteReferenceRequest>,
    ) -> Result<Response<DeleteReferenceResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();

        if req.file_id.is_empty() {
            return Err(status_invalid_argument("file_id is required"));
        }

        let reference_id = if req.reference_id.is_empty() {
            None
        } else {
            Some(req.reference_id.as_str())
        };

        let metadata = self
            .command_handler
            .handle_release_reference(&ctx, &req.file_id, reference_id)
            .await
            .into_grpc()?;

        Ok(Response::new(DeleteReferenceResponse {
            info: Some(to_proto_file_info(&metadata)),
            success: true,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn list_references(
        &self,
        request: Request<ListReferencesRequest>,
    ) -> Result<Response<ListReferencesResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();

        if req.file_id.is_empty() {
            return Err(status_invalid_argument("file_id is required"));
        }

        let references = self
            .query_handler
            .handle_list_references(&ctx, &req.file_id)
            .await
            .into_grpc()?;

        let references_proto = references.iter().map(to_proto_reference).collect();

        Ok(Response::new(ListReferencesResponse {
            references: references_proto,
            pagination: None,
            success: true,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn cleanup_orphaned_assets(
        &self,
        request: Request<CleanupOrphanedAssetsRequest>,
    ) -> Result<Response<CleanupOrphanedAssetsResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        require_media_admin(&ctx, MEDIAS_WRITE)?;
        let req = request.into_inner();
        if req.dry_run {
            return Err(Status::invalid_argument(
                "media cleanup dry run is not supported",
            ));
        }

        let cleaned = self
            .command_handler
            .handle_cleanup_orphaned_assets(&ctx, req.limit)
            .staff_audit(&ctx, "flare-media")
            .await
            .into_grpc()?;

        let scanned = cleaned.len() as u32;
        Ok(Response::new(CleanupOrphanedAssetsResponse {
            file_ids: cleaned,
            success: true,
            error_message: String::new(),
            scanned,
        }))
    }

    #[instrument(skip(self, request))]
    async fn get_file_url(
        &self,
        request: Request<GetFileUrlRequest>,
    ) -> Result<Response<GetFileUrlResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();
        let presigned = self
            .query_handler
            .handle_get_file_url(&ctx, req)
            .await
            .into_grpc()?;
        Ok(Response::new(GetFileUrlResponse {
            url: presigned.url,
            cdn_url: presigned.cdn_url,
            expires_at: Some(to_proto_timestamp(presigned.expires_at)),
            success: true,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn get_file_info(
        &self,
        request: Request<GetFileInfoRequest>,
    ) -> Result<Response<GetFileInfoResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();
        let metadata = self
            .query_handler
            .handle_get_file_info(&ctx, &req.file_id)
            .await
            .into_grpc()?;
        Ok(Response::new(GetFileInfoResponse {
            info: Some(to_proto_file_info(&metadata)),
            success: true,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn download_file(
        &self,
        request: Request<DownloadFileRequest>,
    ) -> Result<Response<Self::DownloadFileStream>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();
        let chunks = self
            .query_handler
            .handle_download_file(&ctx, req)
            .await
            .into_grpc()?;
        let stream = tokio_stream::iter(chunks.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream) as Self::DownloadFileStream))
    }

    #[instrument(skip(self, request))]
    async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> Result<Response<DeleteFileResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();
        self.command_handler
            .handle_delete_file(&ctx, req)
            .await
            .into_grpc()?;
        Ok(Response::new(DeleteFileResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn process_image(
        &self,
        request: Request<ProcessImageRequest>,
    ) -> Result<Response<ProcessImageResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();
        let result = self
            .command_handler
            .handle_process_image(&ctx, req)
            .await
            .into_grpc()?;
        Ok(Response::new(ProcessImageResponse {
            processed_file_id: result.file_id,
            url: result.url,
            cdn_url: result.cdn_url,
            success: true,
            error_message: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn process_video(
        &self,
        request: Request<ProcessVideoRequest>,
    ) -> Result<Response<ProcessVideoResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();
        let result = self
            .command_handler
            .handle_process_video(&ctx, req)
            .await
            .into_grpc()?;
        Ok(Response::new(ProcessVideoResponse {
            processed_file_id: result.file_id,
            url: result.url,
            cdn_url: result.cdn_url,
            success: true,
            error_message: String::new(),
        }))
    }

    async fn set_object_acl(
        &self,
        request: Request<SetObjectAclRequest>,
    ) -> Result<Response<()>, Status> {
        let ctx = verified_ctx(&request)?;
        require_media_admin(&ctx, MEDIAS_WRITE)?;
        let req = request.into_inner();
        self.command_handler
            .handle_set_object_acl(&ctx, req)
            .await
            .into_grpc()?;
        Ok(Response::new(()))
    }

    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        require_media_admin(&ctx, MEDIAS_READ)?;
        let req = request.into_inner();
        let response = self
            .query_handler
            .handle_list_objects(&ctx, req)
            .await
            .into_grpc()?;
        Ok(Response::new(response))
    }

    async fn generate_upload_url(
        &self,
        request: Request<GenerateUploadUrlRequest>,
    ) -> Result<Response<GenerateUploadUrlResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        let req = request.into_inner();
        let response = self
            .query_handler
            .handle_generate_upload_url(&ctx, req)
            .await
            .into_grpc()?;
        Ok(Response::new(response))
    }

    async fn describe_bucket(
        &self,
        request: Request<DescribeBucketRequest>,
    ) -> Result<Response<DescribeBucketResponse>, Status> {
        let ctx = verified_ctx(&request)?;
        require_media_admin(&ctx, MEDIAS_READ)?;
        let req = request.into_inner();
        let response = self
            .query_handler
            .handle_describe_bucket(&ctx, req)
            .await
            .into_grpc()?;
        Ok(Response::new(response))
    }
}

/// Media gRPC 服务端组合：只有持 Cell 签名内部上下文（`x-flare-ctx`）的调用方
/// 能进入 handler；缺失、伪造、过期或重复的令牌在层内直接 UNAUTHENTICATED，
/// 调用方自带的 `x-tenant-id` / `x-user-id` 等旧 metadata 会被剥离。
pub fn authenticated_media_service(
    handler: MediaGrpcHandler,
    verifier: SignedContextVerifier,
) -> SignedContextService<MediaServiceServer<MediaGrpcHandler>> {
    SignedContextLayer::new(verifier.require_staff_kid([flare_server_core::auth::kid::GATEWAY]))
        .layer(MediaServiceServer::new(handler))
}

/// 身份只来自签名层注入的已验上下文。即便 handler 被错误地挂在没有签名层的
/// Server 上，也不回落到调用方 metadata，而是拒绝。
fn verified_ctx<T>(request: &Request<T>) -> Result<Ctx, Status> {
    request
        .extensions()
        .get::<Ctx>()
        .filter(|ctx| {
            ctx.get_data::<VerifiedContext>()
                .and_then(VerifiedContext::claims)
                .is_some()
        })
        .cloned()
        .ok_or_else(|| Status::unauthenticated("signed context required"))
}

/// 上传的租户只取签名上下文：缺租户即 `PERMISSION_DENIED`，在读上传流之前拒绝，不再按租户
/// "0" 写进上传元数据（W12-T）。与媒资仓储的 `require_tenant` 同一判定，显式的 "0" 照常。
fn upload_tenant(ctx: &Ctx) -> Result<String, Status> {
    crate::domain::service::require_tenant(ctx)
        .map(str::to_string)
        .into_grpc()
}

/// 管理操作只接受具体应用的 Staff 与当前路由权限；有效 User/System/TenantAdmin 也不能代用。
fn require_media_admin(ctx: &Ctx, permission: &str) -> Result<(), Status> {
    crate::domain::service::require_tenant(ctx).into_grpc()?;
    flare_server_core::require_staff_permission(ctx, permission).map(|_| ())
}

#[cfg(test)]
#[path = "media_handler_auth_tests.rs"]
mod auth_tests;

fn status_internal<E: std::fmt::Display>(err: E) -> Status {
    Status::internal(err.to_string())
}

fn status_invalid_argument(message: impl Into<String>) -> Status {
    Status::invalid_argument(message.into())
}

fn to_proto_timestamp(value: chrono::DateTime<chrono::Utc>) -> Timestamp {
    Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

fn to_initiate_direct_upload_response(
    state: crate::domain::model::DirectUploadSessionState,
) -> InitiateDirectUploadResponse {
    InitiateDirectUploadResponse {
        upload_id: state.upload_id,
        file_id: state.file_id,
        transport_kind: match state.transport_kind {
            crate::domain::model::DirectUploadTransportKind::SinglePut => {
                flare_grpc_proto::media::DirectUploadTransportKind::SinglePut as i32
            }
            crate::domain::model::DirectUploadTransportKind::MultipartPut => {
                flare_grpc_proto::media::DirectUploadTransportKind::MultipartPut as i32
            }
        },
        bucket: state.bucket,
        object_key: state.object_key,
        storage_upload_id: state.storage_upload_id.unwrap_or_default(),
        part_size: state.part_size,
        total_parts: state.total_parts,
        upload_url: state.upload_url.unwrap_or_default(),
        expires_at: Some(to_proto_timestamp(state.expires_at)),
        success: true,
        error_message: String::new(),
    }
}
