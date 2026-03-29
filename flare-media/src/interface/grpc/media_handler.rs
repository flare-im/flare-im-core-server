use std::sync::Arc;

use flare_proto::common::StatusOnlyResponse;
use flare_proto::media::media_service_server::MediaService;
use flare_proto::media::upload_file_request;
use flare_proto::media::{
    AbortMultipartUploadRequest, AbortMultipartUploadResponse, CleanupOrphanedAssetsRequest,
    CleanupOrphanedAssetsResponse, CompleteMultipartUploadRequest, CreateReferenceRequest,
    CreateReferenceResponse, DeleteFileRequest, DeleteFileResponse, DeleteReferenceRequest,
    DeleteReferenceResponse, DescribeBucketRequest, DescribeBucketResponse,
    GenerateUploadUrlRequest, GenerateUploadUrlResponse, GetFileInfoRequest, GetFileInfoResponse,
    GetFileUrlRequest, GetFileUrlResponse, InitiateMultipartUploadRequest,
    InitiateMultipartUploadResponse, ListObjectsRequest, ListObjectsResponse,
    ListReferencesRequest, ListReferencesResponse, ProcessImageRequest, ProcessImageResponse,
    ProcessVideoRequest, ProcessVideoResponse, SetObjectAclRequest, UploadFileRequest,
    UploadFileResponse, UploadMultipartChunkRequest, UploadMultipartChunkResponse,
};
use flare_server_core::error::ok_status;
use flare_server_core::utils::require_ctx_from_request;
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
    #[instrument(skip(self, request))]
    async fn upload_file(
        &self,
        request: Request<tonic::Streaming<UploadFileRequest>>,
    ) -> Result<Response<UploadFileResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let tenant_id = ctx.tenant_id().unwrap_or("0").to_string();

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
        if !tenant_id.is_empty() && !upload_metadata.metadata.contains_key("tenant_id") {
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
            .map_err(status_internal)?;
        // 上传完成后，返回预签名URL
        let presigned = self
            .query_handler
            .handle_get_file_url(
                &ctx,
                flare_proto::media::GetFileUrlRequest {
                    file_id: metadata.file_id.clone(),
                    expires_in: 0, // 使用服务默认TTL
                    download: false,
                    response_headers: Default::default(),
                },
            )
            .await
            .map_err(status_internal)?;
        Ok(Response::new(UploadFileResponse {
            file_id: metadata.file_id.clone(),
            url: presigned.url,
            cdn_url: presigned.cdn_url,
            info: Some(to_proto_file_info(&metadata)),
            success: true,
            error_message: String::new(),
            status: Some(ok_status()),
        }))
    }

    #[instrument(skip(self, request))]
    async fn initiate_multipart_upload(
        &self,
        request: Request<InitiateMultipartUploadRequest>,
    ) -> Result<Response<InitiateMultipartUploadResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        let session = self
            .command_handler
            .handle_initiate_multipart_upload(&ctx, req)
            .await
            .map_err(status_internal)?;

        Ok(Response::new(InitiateMultipartUploadResponse {
            upload_id: session.upload_id,
            chunk_size: session.chunk_size,
            expires_at: Some(to_proto_timestamp(session.expires_at)),
            success: true,
            error_message: String::new(),
            status: Some(ok_status()),
        }))
    }

    #[instrument(skip(self, request))]
    async fn upload_multipart_chunk(
        &self,
        request: Request<UploadMultipartChunkRequest>,
    ) -> Result<Response<UploadMultipartChunkResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        let chunk_index = req.chunk_index;
        let session = self
            .command_handler
            .handle_upload_multipart_chunk(&ctx, req)
            .await
            .map_err(status_internal)?;

        Ok(Response::new(UploadMultipartChunkResponse {
            upload_id: session.upload_id,
            chunk_index,
            uploaded_size: session.uploaded_size as u64,
            expires_at: Some(to_proto_timestamp(session.expires_at)),
            success: true,
            error_message: String::new(),
            status: Some(ok_status()),
        }))
    }

    #[instrument(skip(self, request))]
    async fn complete_multipart_upload(
        &self,
        request: Request<CompleteMultipartUploadRequest>,
    ) -> Result<Response<UploadFileResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        let metadata = self
            .command_handler
            .handle_complete_multipart_upload(&ctx, req)
            .await
            .map_err(status_internal)?;
        // 完成分片上传后也返回预签名URL
        let presigned = self
            .query_handler
            .handle_get_file_url(
                &ctx,
                flare_proto::media::GetFileUrlRequest {
                    file_id: metadata.file_id.clone(),
                    expires_in: 0, // 使用服务默认TTL
                    download: false,
                    response_headers: Default::default(),
                },
            )
            .await
            .map_err(status_internal)?;
        Ok(Response::new(UploadFileResponse {
            file_id: metadata.file_id.clone(),
            url: presigned.url,
            cdn_url: presigned.cdn_url,
            info: Some(to_proto_file_info(&metadata)),
            success: true,
            error_message: String::new(),
            status: Some(ok_status()),
        }))
    }

    #[instrument(skip(self, request))]
    async fn abort_multipart_upload(
        &self,
        request: Request<AbortMultipartUploadRequest>,
    ) -> Result<Response<AbortMultipartUploadResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        self.command_handler
            .handle_abort_multipart_upload(&ctx, req)
            .await
            .map_err(status_internal)?;

        Ok(Response::new(AbortMultipartUploadResponse {
            success: true,
            error_message: String::new(),
            status: Some(ok_status()),
        }))
    }

    #[instrument(skip(self, request))]
    async fn create_reference(
        &self,
        request: Request<CreateReferenceRequest>,
    ) -> Result<Response<CreateReferenceResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let tenant_id = ctx.tenant_id().unwrap_or("0").to_string();
        let req = request.into_inner();

        // 将 tenant_id 添加到 metadata 中（如果未设置）
        let mut metadata = req.metadata;
        if !metadata.contains_key("tenant_id") {
            metadata.insert("tenant_id".to_string(), tenant_id.clone());
        }

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
            .map_err(status_internal)?;

        Ok(Response::new(CreateReferenceResponse {
            info: Some(to_proto_file_info(&file_metadata)),
            success: true,
            error_message: String::new(),
            status: Some(ok_status()),
        }))
    }

    #[instrument(skip(self, request))]
    async fn delete_reference(
        &self,
        request: Request<DeleteReferenceRequest>,
    ) -> Result<Response<DeleteReferenceResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
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
            .map_err(status_internal)?;

        Ok(Response::new(DeleteReferenceResponse {
            info: Some(to_proto_file_info(&metadata)),
            success: true,
            error_message: String::new(),
            status: Some(ok_status()),
        }))
    }

    #[instrument(skip(self, request))]
    async fn list_references(
        &self,
        request: Request<ListReferencesRequest>,
    ) -> Result<Response<ListReferencesResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        if req.file_id.is_empty() {
            return Err(status_invalid_argument("file_id is required"));
        }

        let references = self
            .query_handler
            .handle_list_references(&ctx, &req.file_id)
            .await
            .map_err(status_internal)?;

        let references_proto = references
            .iter()
            .map(|reference| to_proto_reference(reference))
            .collect();

        Ok(Response::new(ListReferencesResponse {
            references: references_proto,
            pagination: None,
            success: true,
            error_message: String::new(),
            status: Some(ok_status()),
        }))
    }

    #[instrument(skip(self, request))]
    async fn cleanup_orphaned_assets(
        &self,
        request: Request<CleanupOrphanedAssetsRequest>,
    ) -> Result<Response<CleanupOrphanedAssetsResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let _req = request.into_inner();

        let cleaned = self
            .command_handler
            .handle_cleanup_orphaned_assets(&ctx)
            .await
            .map_err(status_internal)?;

        let scanned = cleaned.len() as u32;
        Ok(Response::new(CleanupOrphanedAssetsResponse {
            file_ids: cleaned,
            success: true,
            error_message: String::new(),
            scanned,
            status: Some(ok_status()),
        }))
    }

    #[instrument(skip(self, request))]
    async fn get_file_url(
        &self,
        request: Request<GetFileUrlRequest>,
    ) -> Result<Response<GetFileUrlResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        let presigned = self
            .query_handler
            .handle_get_file_url(&ctx, req)
            .await
            .map_err(status_internal)?;
        Ok(Response::new(GetFileUrlResponse {
            url: presigned.url,
            cdn_url: presigned.cdn_url,
            expires_at: Some(to_proto_timestamp(presigned.expires_at)),
            success: true,
            error_message: String::new(),
            status: Some(ok_status()),
        }))
    }

    #[instrument(skip(self, request))]
    async fn get_file_info(
        &self,
        request: Request<GetFileInfoRequest>,
    ) -> Result<Response<GetFileInfoResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        let metadata = self
            .query_handler
            .handle_get_file_info(&ctx, &req.file_id)
            .await
            .map_err(status_internal)?;
        Ok(Response::new(GetFileInfoResponse {
            info: Some(to_proto_file_info(&metadata)),
            success: true,
            error_message: String::new(),
            status: Some(ok_status()),
        }))
    }

    #[instrument(skip(self, request))]
    async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> Result<Response<DeleteFileResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        self.command_handler
            .handle_delete_file(&ctx, req)
            .await
            .map_err(status_internal)?;
        Ok(Response::new(DeleteFileResponse {
            success: true,
            error_message: String::new(),
            status: Some(ok_status()),
        }))
    }

    #[instrument(skip(self, request))]
    async fn process_image(
        &self,
        request: Request<ProcessImageRequest>,
    ) -> Result<Response<ProcessImageResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        let result = self
            .command_handler
            .handle_process_image(&ctx, req)
            .await
            .map_err(status_internal)?;
        Ok(Response::new(ProcessImageResponse {
            processed_file_id: result.file_id,
            url: result.url,
            cdn_url: result.cdn_url,
            success: true,
            error_message: String::new(),
            status: Some(ok_status()),
        }))
    }

    #[instrument(skip(self, request))]
    async fn process_video(
        &self,
        request: Request<ProcessVideoRequest>,
    ) -> Result<Response<ProcessVideoResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        let result = self
            .command_handler
            .handle_process_video(&ctx, req)
            .await
            .map_err(status_internal)?;
        Ok(Response::new(ProcessVideoResponse {
            processed_file_id: result.file_id,
            url: result.url,
            cdn_url: result.cdn_url,
            success: true,
            error_message: String::new(),
            status: Some(ok_status()),
        }))
    }

    async fn set_object_acl(
        &self,
        _request: Request<SetObjectAclRequest>,
    ) -> Result<Response<StatusOnlyResponse>, Status> {
        Err(unimplemented_status("SetObjectAcl"))
    }

    async fn list_objects(
        &self,
        _request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        Err(unimplemented_status("ListObjects"))
    }

    async fn generate_upload_url(
        &self,
        _request: Request<GenerateUploadUrlRequest>,
    ) -> Result<Response<GenerateUploadUrlResponse>, Status> {
        Err(unimplemented_status("GenerateUploadUrl"))
    }

    async fn describe_bucket(
        &self,
        _request: Request<DescribeBucketRequest>,
    ) -> Result<Response<DescribeBucketResponse>, Status> {
        Err(unimplemented_status("DescribeBucket"))
    }
}

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

fn unimplemented_status(method: &str) -> Status {
    Status::unimplemented(format!("{method} is not implemented yet"))
}
