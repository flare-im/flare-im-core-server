//! 查询处理器（查询侧）- 部分查询直接调用基础设施层，包含业务逻辑的查询使用领域服务
//!
//! 在 CQRS 架构中，查询侧通常直接调用基础设施层（仓储实现），
//! 因为查询是只读操作，不涉及业务逻辑，不需要经过领域层。
//!
//! 注意：`get_metadata` 和 `create_presigned_url` 包含业务逻辑（缓存、默认值处理、URL构建），
//! 因此仍需要通过领域服务处理。

use std::sync::Arc;

use flare_grpc_proto::media::{
    DescribeBucketRequest, DescribeBucketResponse, DownloadFileChunk, DownloadFileRequest,
    GenerateUploadUrlRequest, GenerateUploadUrlResponse, GetDirectUploadStatusRequest,
    GetDirectUploadStatusResponse, GetFileUrlRequest, ListObjectsRequest, ListObjectsResponse,
    PresignDirectUploadPartsRequest, PresignDirectUploadPartsResponse, PresignedUploadPart,
};
use flare_server_core::context::Context;
use flare_server_core::error::{ErrorCode, Result};

use crate::domain::model::{
    DirectUploadSessionState, MediaFileMetadata, MediaReference, PresignedUrl,
};
use crate::domain::service::MediaService;

/// 媒体查询处理器（查询侧）
///
/// 简单查询直接使用基础设施层，包含业务逻辑的查询使用领域服务
pub struct MediaQueryHandler {
    // 包含业务逻辑的查询使用领域服务
    domain_service: Arc<MediaService>,
}

impl MediaQueryHandler {
    pub fn new(domain_service: Arc<MediaService>) -> Self {
        Self { domain_service }
    }

    /// 获取文件信息（包含缓存逻辑，使用领域服务）
    pub async fn handle_get_file_info(
        &self,
        ctx: &Context,
        file_id: &str,
    ) -> Result<MediaFileMetadata> {
        self.domain_service.get_metadata(ctx, file_id).await
    }

    /// 获取文件URL（包含默认值处理和URL构建逻辑，使用领域服务）
    pub async fn handle_get_file_url(
        &self,
        ctx: &Context,
        request: GetFileUrlRequest,
    ) -> Result<PresignedUrl> {
        if request.retention_protected
            && matches!(
                flare_proto::common::ContentVisibility::try_from(request.content_visibility),
                Ok(flare_proto::common::ContentVisibility::Hidden)
                    | Ok(flare_proto::common::ContentVisibility::Redacted)
                    | Ok(flare_proto::common::ContentVisibility::Purged)
            )
        {
            return Err(flare_server_core::flare_err!(
                ErrorCode::PermissionDenied,
                "retention-protected message attachment url is forbidden"
            ));
        }
        let mut expires_in = i64::from(request.expires_in);
        if request.retention_protected {
            expires_in = if expires_in > 0 {
                expires_in.min(300)
            } else {
                300
            };
        }
        self.domain_service
            .create_presigned_url(ctx, &request.file_id, expires_in)
            .await
    }

    pub async fn handle_download_file(
        &self,
        ctx: &Context,
        request: DownloadFileRequest,
    ) -> Result<Vec<DownloadFileChunk>> {
        let bytes = self
            .domain_service
            .download_local_file(ctx, &request.file_id)
            .await?;
        const CHUNK_SIZE: usize = 64 * 1024;
        Ok(bytes
            .chunks(CHUNK_SIZE)
            .map(|chunk| DownloadFileChunk {
                chunk_data: chunk.to_vec(),
            })
            .collect())
    }

    pub async fn handle_generate_upload_url(
        &self,
        _ctx: &Context,
        request: GenerateUploadUrlRequest,
    ) -> Result<GenerateUploadUrlResponse> {
        let (upload_url, object_key) = self.domain_service.generate_upload_url(
            Some(request.bucket.as_str()),
            Some(request.object_key.as_str()),
        );
        Ok(GenerateUploadUrlResponse {
            upload_url,
            object_key,
        })
    }

    pub async fn handle_list_objects(
        &self,
        ctx: &Context,
        request: ListObjectsRequest,
    ) -> Result<ListObjectsResponse> {
        let files = self
            .domain_service
            .list_objects(ctx, &request.bucket, &request.prefix)
            .await?;
        Ok(ListObjectsResponse {
            files: files
                .iter()
                .map(crate::application::utils::to_proto_file_info)
                .collect(),
            pagination: None,
        })
    }

    pub async fn handle_describe_bucket(
        &self,
        _ctx: &Context,
        request: DescribeBucketRequest,
    ) -> Result<DescribeBucketResponse> {
        let (bucket, region, storage_class, versioning_enabled, metadata) = self
            .domain_service
            .describe_bucket(Some(request.bucket.as_str()));
        Ok(DescribeBucketResponse {
            bucket,
            region,
            storage_class,
            versioning_enabled,
            metadata,
        })
    }

    pub async fn handle_get_direct_upload_status(
        &self,
        ctx: &Context,
        request: GetDirectUploadStatusRequest,
    ) -> Result<GetDirectUploadStatusResponse> {
        let state = self
            .domain_service
            .get_direct_upload_status(ctx, &request.upload_id)
            .await?;
        Ok(to_direct_upload_status_response(state))
    }

    pub async fn handle_presign_direct_upload_parts(
        &self,
        ctx: &Context,
        request: PresignDirectUploadPartsRequest,
    ) -> Result<PresignDirectUploadPartsResponse> {
        let expires_in = i64::from(request.expires_in);
        let parts = self
            .domain_service
            .presign_direct_upload_parts(ctx, &request.upload_id, &request.part_numbers, expires_in)
            .await?;
        Ok(PresignDirectUploadPartsResponse {
            parts: parts
                .into_iter()
                .map(|part| PresignedUploadPart {
                    part_number: part.part_number,
                    upload_url: part.upload_url,
                    headers: part.headers,
                })
                .collect(),
            success: true,
            error_message: String::new(),
        })
    }

    /// 列出文件引用（通过领域服务）
    pub async fn handle_list_references(
        &self,
        ctx: &Context,
        file_id: &str,
    ) -> Result<Vec<MediaReference>> {
        self.domain_service.list_references(ctx, file_id).await
    }

    pub fn to_proto_file_info(
        &self,
        metadata: &MediaFileMetadata,
    ) -> flare_grpc_proto::media::FileInfo {
        crate::application::utils::to_proto_file_info(metadata)
    }
}

fn to_direct_upload_status_response(
    state: DirectUploadSessionState,
) -> GetDirectUploadStatusResponse {
    GetDirectUploadStatusResponse {
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
        total_size: state.total_size,
        total_parts: state.total_parts,
        uploaded_parts: state
            .uploaded_parts
            .into_iter()
            .map(|part| flare_grpc_proto::media::UploadedPartInfo {
                part_number: part.part_number,
                etag: part.etag,
                size: part.size,
                sha256: part.sha256.unwrap_or_default(),
            })
            .collect(),
        expires_at: Some(prost_types::Timestamp {
            seconds: state.expires_at.timestamp(),
            nanos: state.expires_at.timestamp_subsec_nanos() as i32,
        }),
        success: true,
        error_message: String::new(),
    }
}
