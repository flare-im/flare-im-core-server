use anyhow::Result;
use std::sync::Arc;
use tonic::transport::{Channel, Uri};

use flare_grpc_proto::media::media_service_client::MediaServiceClient;
use flare_grpc_proto::media::*;

/// MediaService gRPC 客户端封装
#[derive(Clone)]
pub struct MediaServiceClientWrapper {
    client: MediaServiceClient<Channel>,
}

impl MediaServiceClientWrapper {
    /// 创建新的客户端连接
    pub async fn new(url: &str) -> Result<Self> {
        let uri: Uri = url.parse()?;
        let channel = Channel::builder(uri).connect().await?;
        let client = MediaServiceClient::new(channel);
        Ok(Self { client })
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
    pub async fn get_file_url(
        &mut self,
        request: GetFileUrlRequest,
    ) -> Result<GetFileUrlResponse> {
        let response = self.client.get_file_url(request).await?;
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

    /// 删除文件
    pub async fn delete_file(
        &mut self,
        request: DeleteFileRequest,
    ) -> Result<DeleteFileResponse> {
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
    pub async fn new(
        media_service_url: &str,
    ) -> Result<Self> {
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
        let client = MediaServiceClientWrapper::new("http://localhost:50051").await;
        assert!(client.is_ok());
    }
}
