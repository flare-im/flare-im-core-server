use anyhow::Result;
use std::sync::Arc;
use tokio_stream::{self as stream};
use tonic::Code;
use tonic::transport::{Channel, Uri};
use tracing::warn;

use crate::context::Ctx;
use flare_grpc_proto::media::media_service_client::MediaServiceClient;
use flare_grpc_proto::media::*;

/// MediaService gRPC 客户端封装
#[derive(Clone)]
pub struct MediaServiceClientWrapper {
    client: MediaServiceClient<Channel>,
    current_url: String,
    fallback_url: Option<String>,
}

impl MediaServiceClientWrapper {
    const DEFAULT_MEDIA_ENDPOINT: &'static str = "http://localhost:60081";

    fn request_with_ctx<T>(ctx: &Ctx, payload: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(payload);
        ctx.inject_to_grpc_metadata(request.metadata_mut());
        request
    }

    async fn reconnect_to(&mut self, url: &str) -> Result<()> {
        let uri: Uri = url.parse()?;
        let channel = Channel::builder(uri).connect().await?;
        self.client = MediaServiceClient::new(channel);
        self.current_url = url.to_string();
        Ok(())
    }

    /// 流式上传文件
    pub async fn upload_file(
        &mut self,
        requests: Vec<UploadFileRequest>,
    ) -> Result<UploadFileResponse> {
        let output = stream::iter(requests);
        let response = self.client.upload_file(output).await?;
        Ok(response.into_inner())
    }

    /// 初始化分片上传
    pub async fn initiate_multipart_upload(
        &mut self,
        request: InitiateMultipartUploadRequest,
    ) -> Result<InitiateMultipartUploadResponse> {
        match self.client.initiate_multipart_upload(request.clone()).await {
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
                    let response = self.client.initiate_multipart_upload(request).await?;
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
        request: UploadMultipartChunkRequest,
    ) -> Result<UploadMultipartChunkResponse> {
        match self.client.upload_multipart_chunk(request.clone()).await {
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
                    let response = self.client.upload_multipart_chunk(request).await?;
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
        request: CompleteMultipartUploadRequest,
    ) -> Result<UploadFileResponse> {
        match self.client.complete_multipart_upload(request.clone()).await {
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
                    let response = self.client.complete_multipart_upload(request).await?;
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
        request: AbortMultipartUploadRequest,
    ) -> Result<AbortMultipartUploadResponse> {
        match self.client.abort_multipart_upload(request.clone()).await {
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
                    let response = self.client.abort_multipart_upload(request).await?;
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
        request: InitiateDirectUploadRequest,
    ) -> Result<InitiateDirectUploadResponse> {
        let response = self.client.initiate_direct_upload(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_direct_upload_status(
        &mut self,
        request: GetDirectUploadStatusRequest,
    ) -> Result<GetDirectUploadStatusResponse> {
        let response = self.client.get_direct_upload_status(request).await?;
        Ok(response.into_inner())
    }

    pub async fn presign_direct_upload_parts(
        &mut self,
        request: PresignDirectUploadPartsRequest,
    ) -> Result<PresignDirectUploadPartsResponse> {
        let response = self.client.presign_direct_upload_parts(request).await?;
        Ok(response.into_inner())
    }

    pub async fn commit_direct_upload_parts(
        &mut self,
        request: CommitDirectUploadPartsRequest,
    ) -> Result<CommitDirectUploadPartsResponse> {
        let response = self.client.commit_direct_upload_parts(request).await?;
        Ok(response.into_inner())
    }

    pub async fn complete_direct_upload(
        &mut self,
        request: CompleteDirectUploadRequest,
    ) -> Result<UploadFileResponse> {
        let response = self.client.complete_direct_upload(request).await?;
        Ok(response.into_inner())
    }

    pub async fn abort_direct_upload(
        &mut self,
        request: AbortDirectUploadRequest,
    ) -> Result<AbortMultipartUploadResponse> {
        let response = self.client.abort_direct_upload(request).await?;
        Ok(response.into_inner())
    }

    /// 创建新的客户端连接
    pub async fn new(url: &str) -> Result<Self> {
        let uri: Uri = url.parse()?;
        let channel = Channel::builder(uri).connect().await?;
        let client = MediaServiceClient::new(channel);
        let fallback_url = if url != Self::DEFAULT_MEDIA_ENDPOINT {
            Some(Self::DEFAULT_MEDIA_ENDPOINT.to_string())
        } else {
            None
        };
        Ok(Self {
            client,
            current_url: url.to_string(),
            fallback_url,
        })
    }

    /// 生成上传 URL
    pub async fn generate_upload_url(
        &mut self,
        request: GenerateUploadUrlRequest,
    ) -> Result<GenerateUploadUrlResponse> {
        let response = self.client.generate_upload_url(request).await?;
        Ok(response.into_inner())
    }

    /// 获取文件 URL
    pub async fn get_file_url(&mut self, request: GetFileUrlRequest) -> Result<GetFileUrlResponse> {
        let response = self.client.get_file_url(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_file_url_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: GetFileUrlRequest,
    ) -> Result<GetFileUrlResponse> {
        let response = self
            .client
            .get_file_url(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }

    /// 获取文件信息
    pub async fn get_file_info(
        &mut self,
        request: GetFileInfoRequest,
    ) -> Result<GetFileInfoResponse> {
        let response = self.client.get_file_info(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_file_info_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: GetFileInfoRequest,
    ) -> Result<GetFileInfoResponse> {
        let response = self
            .client
            .get_file_info(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn download_file_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: DownloadFileRequest,
    ) -> Result<tonic::Streaming<DownloadFileChunk>> {
        let response = self
            .client
            .download_file(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }

    /// 删除文件
    pub async fn delete_file(&mut self, request: DeleteFileRequest) -> Result<DeleteFileResponse> {
        let response = self.client.delete_file(request).await?;
        Ok(response.into_inner())
    }

    /// 创建引用
    pub async fn create_reference(
        &mut self,
        request: CreateReferenceRequest,
    ) -> Result<CreateReferenceResponse> {
        let response = self.client.create_reference(request).await?;
        Ok(response.into_inner())
    }

    /// 删除引用
    pub async fn delete_reference(
        &mut self,
        request: DeleteReferenceRequest,
    ) -> Result<DeleteReferenceResponse> {
        let response = self.client.delete_reference(request).await?;
        Ok(response.into_inner())
    }

    /// 列出引用
    pub async fn list_references(
        &mut self,
        request: ListReferencesRequest,
    ) -> Result<ListReferencesResponse> {
        let response = self.client.list_references(request).await?;
        Ok(response.into_inner())
    }

    /// 列出对象
    pub async fn list_objects(
        &mut self,
        request: ListObjectsRequest,
    ) -> Result<ListObjectsResponse> {
        let response = self.client.list_objects(request).await?;
        Ok(response.into_inner())
    }

    /// 图片处理
    pub async fn process_image(
        &mut self,
        request: ProcessImageRequest,
    ) -> Result<ProcessImageResponse> {
        let response = self.client.process_image(request).await?;
        Ok(response.into_inner())
    }

    /// 视频处理
    pub async fn process_video(
        &mut self,
        request: ProcessVideoRequest,
    ) -> Result<ProcessVideoResponse> {
        let response = self.client.process_video(request).await?;
        Ok(response.into_inner())
    }

    /// 清理孤儿资源
    pub async fn cleanup_orphaned_assets(
        &mut self,
        request: CleanupOrphanedAssetsRequest,
    ) -> Result<CleanupOrphanedAssetsResponse> {
        let response = self.client.cleanup_orphaned_assets(request).await?;
        Ok(response.into_inner())
    }

    /// 设置对象 ACL
    pub async fn set_object_acl(&mut self, request: SetObjectAclRequest) -> Result<()> {
        self.client.set_object_acl(request).await?;
        Ok(())
    }

    /// 描述桶
    pub async fn describe_bucket(
        &mut self,
        request: DescribeBucketRequest,
    ) -> Result<DescribeBucketResponse> {
        let response = self.client.describe_bucket(request).await?;
        Ok(response.into_inner())
    }
}

/// gRPC 客户端管理器
pub struct GrpcClients {
    pub media: Arc<tokio::sync::Mutex<MediaServiceClientWrapper>>,
}

impl GrpcClients {
    /// 初始化所有 gRPC 客户端
    pub async fn new(media_service_url: &str) -> Result<Self> {
        let media = MediaServiceClientWrapper::new(media_service_url).await?;

        Ok(Self {
            media: Arc::new(tokio::sync::Mutex::new(media)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // 需要实际的 gRPC 服务
    async fn test_media_client_connection() {
        let client = MediaServiceClientWrapper::new("http://localhost:60081").await;
        assert!(client.is_ok());
    }
}
