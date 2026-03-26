//! 媒体服务客户端

use futures_util::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use flare_proto::media::media_service_client::MediaServiceClient;
use flare_proto::media::*;

use flare_server_core::discovery::ServiceClient;

/// gRPC媒体服务客户端
pub struct GrpcMediaClient {
    /// 服务客户端（用于服务发现）
    service_client: Option<Arc<Mutex<ServiceClient>>>,
    /// 服务名称
    service_name: String,
    /// 直连地址（当没有服务发现时使用）
    direct_address: Option<String>,
}

impl GrpcMediaClient {
    /// 创建新的gRPC媒体服务客户端
    pub fn new(service_name: String) -> Self {
        Self {
            service_client: None,
            service_name,
            direct_address: None,
        }
    }

    /// 使用服务客户端创建gRPC媒体服务客户端
    pub fn with_service_client(service_client: ServiceClient, service_name: String) -> Self {
        Self {
            service_client: Some(Arc::new(Mutex::new(service_client))),
            service_name,
            direct_address: None,
        }
    }

    /// 使用直接地址创建gRPC媒体服务客户端
    pub fn with_direct_address(direct_address: String, service_name: String) -> Self {
        Self {
            service_client: None,
            service_name,
            direct_address: Some(direct_address),
        }
    }

    /// 获取gRPC客户端
    async fn get_client(&self) -> Result<MediaServiceClient<Channel>, Status> {
        if let Some(service_client) = &self.service_client {
            let mut client = service_client.lock().await;
            let channel = client.get_channel().await.map_err(|e| {
                Status::unavailable(format!(
                    "Failed to get channel from service discovery: {}",
                    e
                ))
            })?;
            Ok(MediaServiceClient::new(channel))
        } else if let Some(ref address) = self.direct_address {
            let channel = Channel::from_shared(address.clone())
                .map_err(|e| Status::invalid_argument(format!("Invalid address: {}", e)))?
                .connect()
                .await
                .map_err(|e| {
                    Status::unavailable(format!("Failed to connect to {}: {}", address, e))
                })?;
            Ok(MediaServiceClient::new(channel))
        } else {
            // 使用服务名称进行直连（假设服务名称可以直接解析）
            let channel = Channel::from_shared(self.service_name.clone())
                .map_err(|e| Status::invalid_argument(format!("Invalid service name: {}", e)))?
                .connect()
                .await
                .map_err(|e| {
                    Status::unavailable(format!(
                        "Failed to connect to {}: {}",
                        self.service_name, e
                    ))
                })?;
            Ok(MediaServiceClient::new(channel))
        }
    }

    /// 上传文件（流式）
    pub async fn upload_file(
        &self,
        request: Request<tonic::Streaming<UploadFileRequest>>,
    ) -> Result<Response<UploadFileResponse>, Status> {
        // 收集流中的所有元素，处理Result类型
        let stream = request.into_inner();
        let items: Vec<Result<UploadFileRequest, Status>> = stream.collect().await;

        // 检查是否有错误
        let upload_requests: Result<Vec<UploadFileRequest>, Status> = items.into_iter().collect();
        let upload_requests = upload_requests?;

        // 创建一个新的流
        let new_stream = tokio_stream::iter(upload_requests);

        let mut client = self.get_client().await?;
        client.upload_file(new_stream).await
    }

    /// 初始化分片上传
    pub async fn initiate_multipart_upload(
        &self,
        request: Request<InitiateMultipartUploadRequest>,
    ) -> Result<Response<InitiateMultipartUploadResponse>, Status> {
        let mut client = self.get_client().await?;
        client.initiate_multipart_upload(request).await
    }

    /// 上传单个分片
    pub async fn upload_multipart_chunk(
        &self,
        request: Request<UploadMultipartChunkRequest>,
    ) -> Result<Response<UploadMultipartChunkResponse>, Status> {
        let mut client = self.get_client().await?;
        client.upload_multipart_chunk(request).await
    }

    /// 完成分片上传
    pub async fn complete_multipart_upload(
        &self,
        request: Request<CompleteMultipartUploadRequest>,
    ) -> Result<Response<UploadFileResponse>, Status> {
        let mut client = self.get_client().await?;
        client.complete_multipart_upload(request).await
    }

    /// 取消分片上传
    pub async fn abort_multipart_upload(
        &self,
        request: Request<AbortMultipartUploadRequest>,
    ) -> Result<Response<AbortMultipartUploadResponse>, Status> {
        let mut client = self.get_client().await?;
        client.abort_multipart_upload(request).await
    }

    /// 创建媒资引用
    pub async fn create_reference(
        &self,
        request: Request<CreateReferenceRequest>,
    ) -> Result<Response<CreateReferenceResponse>, Status> {
        let mut client = self.get_client().await?;
        client.create_reference(request).await
    }

    /// 删除媒资引用
    pub async fn delete_reference(
        &self,
        request: Request<DeleteReferenceRequest>,
    ) -> Result<Response<DeleteReferenceResponse>, Status> {
        let mut client = self.get_client().await?;
        client.delete_reference(request).await
    }

    /// 列出媒资引用
    pub async fn list_references(
        &self,
        request: Request<ListReferencesRequest>,
    ) -> Result<Response<ListReferencesResponse>, Status> {
        let mut client = self.get_client().await?;
        client.list_references(request).await
    }

    /// 清理孤立媒资
    pub async fn cleanup_orphaned_assets(
        &self,
        request: Request<CleanupOrphanedAssetsRequest>,
    ) -> Result<Response<CleanupOrphanedAssetsResponse>, Status> {
        let mut client = self.get_client().await?;
        client.cleanup_orphaned_assets(request).await
    }

    /// 获取文件URL
    pub async fn get_file_url(
        &self,
        request: Request<GetFileUrlRequest>,
    ) -> Result<Response<GetFileUrlResponse>, Status> {
        let mut client = self.get_client().await?;
        client.get_file_url(request).await
    }

    /// 获取文件信息
    pub async fn get_file_info(
        &self,
        request: Request<GetFileInfoRequest>,
    ) -> Result<Response<GetFileInfoResponse>, Status> {
        let mut client = self.get_client().await?;
        client.get_file_info(request).await
    }

    /// 删除文件
    pub async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> Result<Response<DeleteFileResponse>, Status> {
        let mut client = self.get_client().await?;
        client.delete_file(request).await
    }

    /// 处理图片
    pub async fn process_image(
        &self,
        request: Request<ProcessImageRequest>,
    ) -> Result<Response<ProcessImageResponse>, Status> {
        let mut client = self.get_client().await?;
        client.process_image(request).await
    }

    /// 处理视频
    pub async fn process_video(
        &self,
        request: Request<ProcessVideoRequest>,
    ) -> Result<Response<ProcessVideoResponse>, Status> {
        let mut client = self.get_client().await?;
        client.process_video(request).await
    }

    /// 设置对象ACL
    pub async fn set_object_acl(
        &self,
        request: Request<SetObjectAclRequest>,
    ) -> Result<Response<flare_proto::common::StatusOnlyResponse>, Status> {
        let mut client = self.get_client().await?;
        client.set_object_acl(request).await
    }

    /// 列出对象
    pub async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        let mut client = self.get_client().await?;
        client.list_objects(request).await
    }

    /// 生成上传URL
    pub async fn generate_upload_url(
        &self,
        request: Request<GenerateUploadUrlRequest>,
    ) -> Result<Response<GenerateUploadUrlResponse>, Status> {
        let mut client = self.get_client().await?;
        client.generate_upload_url(request).await
    }

    /// 描述存储桶
    pub async fn describe_bucket(
        &self,
        request: Request<DescribeBucketRequest>,
    ) -> Result<Response<DescribeBucketResponse>, Status> {
        let mut client = self.get_client().await?;
        client.describe_bucket(request).await
    }
}
