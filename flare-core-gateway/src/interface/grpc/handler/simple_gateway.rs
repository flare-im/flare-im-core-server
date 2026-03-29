//! # Gateway gRPC 代理
//!
//! 透明转发到后端服务（本 crate 仅此一种实现）。
//! 职责：
//! - 媒体服务代理 (media.proto)
//! - Hook管理代理 (hooks.proto)
//! - 消息操作代理 (message.proto)
//! - 用户在线状态代理 (online.proto)

use std::sync::Arc;
use tonic::{Request, Response, Status};

// 媒体服务
use flare_proto::media::media_service_server::MediaService;
use flare_proto::media::*;

// Hook服务
use flare_proto::hooks::hook_service_server::HookService;
use flare_proto::hooks::*;

// 消息服务（仅发送侧）
use flare_proto::message::message_send_service_server::MessageSendService;
use flare_proto::message::*;

// 在线状态服务（已合并为 OnlineService）
use flare_proto::signaling::online::online_service_server::OnlineService;
use flare_proto::signaling::online::*;

use crate::infrastructure::hook::GrpcHookClient;
use crate::infrastructure::media::GrpcMediaClient;
use crate::infrastructure::message::GrpcMessageClient;
use crate::infrastructure::online::GrpcOnlineClient;

/// 简单网关处理器
#[derive(Clone)]
pub struct SimpleGatewayHandler {
    /// 媒体服务客户端
    media_client: Arc<GrpcMediaClient>,
    /// Hook服务客户端
    hook_client: Arc<GrpcHookClient>,
    /// 消息服务客户端
    message_client: Arc<GrpcMessageClient>,
    /// 在线状态服务客户端
    online_client: Arc<GrpcOnlineClient>,
}

impl SimpleGatewayHandler {
    /// 创建简单网关处理器
    pub fn new(
        media_client: Arc<GrpcMediaClient>,
        hook_client: Arc<GrpcHookClient>,
        message_client: Arc<GrpcMessageClient>,
        online_client: Arc<GrpcOnlineClient>,
    ) -> Self {
        Self {
            media_client,
            hook_client,
            message_client,
            online_client,
        }
    }
}

#[tonic::async_trait]
impl MediaService for SimpleGatewayHandler {
    /// 上传文件（流式）
    async fn upload_file(
        &self,
        request: Request<tonic::Streaming<UploadFileRequest>>,
    ) -> Result<Response<UploadFileResponse>, Status> {
        // 代理到真实的媒体服务
        (*self.media_client).upload_file(request).await
    }

    /// 初始化分片上传
    async fn initiate_multipart_upload(
        &self,
        request: Request<InitiateMultipartUploadRequest>,
    ) -> Result<Response<InitiateMultipartUploadResponse>, Status> {
        self.media_client.initiate_multipart_upload(request).await
    }

    /// 上传单个分片
    async fn upload_multipart_chunk(
        &self,
        request: Request<UploadMultipartChunkRequest>,
    ) -> Result<Response<UploadMultipartChunkResponse>, Status> {
        self.media_client.upload_multipart_chunk(request).await
    }

    /// 完成分片上传
    async fn complete_multipart_upload(
        &self,
        request: Request<CompleteMultipartUploadRequest>,
    ) -> Result<Response<UploadFileResponse>, Status> {
        self.media_client.complete_multipart_upload(request).await
    }

    /// 取消分片上传
    async fn abort_multipart_upload(
        &self,
        request: Request<AbortMultipartUploadRequest>,
    ) -> Result<Response<AbortMultipartUploadResponse>, Status> {
        self.media_client.abort_multipart_upload(request).await
    }

    /// 创建媒资引用
    async fn create_reference(
        &self,
        request: Request<CreateReferenceRequest>,
    ) -> Result<Response<CreateReferenceResponse>, Status> {
        self.media_client.create_reference(request).await
    }

    /// 删除媒资引用
    async fn delete_reference(
        &self,
        request: Request<DeleteReferenceRequest>,
    ) -> Result<Response<DeleteReferenceResponse>, Status> {
        self.media_client.delete_reference(request).await
    }

    /// 列出媒资引用
    async fn list_references(
        &self,
        request: Request<ListReferencesRequest>,
    ) -> Result<Response<ListReferencesResponse>, Status> {
        self.media_client.list_references(request).await
    }

    /// 清理孤立媒资
    async fn cleanup_orphaned_assets(
        &self,
        request: Request<CleanupOrphanedAssetsRequest>,
    ) -> Result<Response<CleanupOrphanedAssetsResponse>, Status> {
        self.media_client.cleanup_orphaned_assets(request).await
    }

    /// 获取文件URL
    async fn get_file_url(
        &self,
        request: Request<GetFileUrlRequest>,
    ) -> Result<Response<GetFileUrlResponse>, Status> {
        self.media_client.get_file_url(request).await
    }

    /// 获取文件信息
    async fn get_file_info(
        &self,
        request: Request<GetFileInfoRequest>,
    ) -> Result<Response<GetFileInfoResponse>, Status> {
        self.media_client.get_file_info(request).await
    }

    /// 删除文件
    async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> Result<Response<DeleteFileResponse>, Status> {
        self.media_client.delete_file(request).await
    }

    /// 处理图片
    async fn process_image(
        &self,
        request: Request<ProcessImageRequest>,
    ) -> Result<Response<ProcessImageResponse>, Status> {
        self.media_client.process_image(request).await
    }

    /// 处理视频
    async fn process_video(
        &self,
        request: Request<ProcessVideoRequest>,
    ) -> Result<Response<ProcessVideoResponse>, Status> {
        self.media_client.process_video(request).await
    }

    /// 设置对象ACL
    async fn set_object_acl(
        &self,
        request: Request<SetObjectAclRequest>,
    ) -> Result<Response<flare_proto::common::StatusOnlyResponse>, Status> {
        self.media_client.set_object_acl(request).await
    }

    /// 列出对象
    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        self.media_client.list_objects(request).await
    }

    /// 生成上传URL
    async fn generate_upload_url(
        &self,
        request: Request<GenerateUploadUrlRequest>,
    ) -> Result<Response<GenerateUploadUrlResponse>, Status> {
        self.media_client.generate_upload_url(request).await
    }

    /// 描述存储桶
    async fn describe_bucket(
        &self,
        request: Request<DescribeBucketRequest>,
    ) -> Result<Response<DescribeBucketResponse>, Status> {
        self.media_client.describe_bucket(request).await
    }
}

#[tonic::async_trait]
impl HookService for SimpleGatewayHandler {
    /// 创建Hook配置
    async fn create_hook_config(
        &self,
        request: Request<CreateHookConfigRequest>,
    ) -> Result<Response<CreateHookConfigResponse>, Status> {
        self.hook_client.create_hook_config(request).await
    }

    /// 获取Hook配置
    async fn get_hook_config(
        &self,
        request: Request<GetHookConfigRequest>,
    ) -> Result<Response<GetHookConfigResponse>, Status> {
        self.hook_client.get_hook_config(request).await
    }

    /// 更新Hook配置
    async fn update_hook_config(
        &self,
        request: Request<UpdateHookConfigRequest>,
    ) -> Result<Response<UpdateHookConfigResponse>, Status> {
        self.hook_client.update_hook_config(request).await
    }

    /// 列出Hook配置
    async fn list_hook_configs(
        &self,
        request: Request<ListHookConfigsRequest>,
    ) -> Result<Response<ListHookConfigsResponse>, Status> {
        self.hook_client.list_hook_configs(request).await
    }

    /// 删除Hook配置
    async fn delete_hook_config(
        &self,
        request: Request<DeleteHookConfigRequest>,
    ) -> Result<Response<DeleteHookConfigResponse>, Status> {
        self.hook_client.delete_hook_config(request).await
    }

    /// 启用/禁用Hook
    async fn set_hook_status(
        &self,
        request: Request<SetHookStatusRequest>,
    ) -> Result<Response<SetHookStatusResponse>, Status> {
        self.hook_client.set_hook_status(request).await
    }

    /// 查询Hook执行统计
    async fn get_hook_statistics(
        &self,
        request: Request<GetHookStatisticsRequest>,
    ) -> Result<Response<GetHookStatisticsResponse>, Status> {
        self.hook_client.get_hook_statistics(request).await
    }

    /// 查询Hook执行历史
    async fn query_hook_executions(
        &self,
        request: Request<QueryHookExecutionsRequest>,
    ) -> Result<Response<QueryHookExecutionsResponse>, Status> {
        self.hook_client.query_hook_executions(request).await
    }
}

#[tonic::async_trait]
impl MessageSendService for SimpleGatewayHandler {
    /// 发送单条消息
    async fn send_message(
        &self,
        request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        self.message_client.send_message(request).await
    }

    /// 批量发送消息
    async fn batch_send_message(
        &self,
        request: Request<BatchSendMessageRequest>,
    ) -> Result<Response<BatchSendMessageResponse>, Status> {
        self.message_client.batch_send_message(request).await
    }

    /// 发送系统消息
    async fn send_system_message(
        &self,
        request: Request<SendSystemMessageRequest>,
    ) -> Result<Response<SendSystemMessageResponse>, Status> {
        self.message_client.send_system_message(request).await
    }

    /// 统一事件入口：ExecuteEventRequest → OperationResponse
    async fn execute_event(
        &self,
        request: Request<ExecuteEventRequest>,
    ) -> Result<Response<flare_proto::common::OperationResponse>, Status> {
        self.message_client.execute_event(request).await
    }

    async fn send_ack(
        &self,
        request: Request<SendAckRequest>,
    ) -> Result<Response<SendAckResponse>, Status> {
        self.message_client.send_ack(request).await
    }

    async fn send_custom_data(
        &self,
        request: Request<SendCustomDataRequest>,
    ) -> Result<Response<SendCustomDataResponse>, Status> {
        self.message_client.send_custom_data(request).await
    }
}

// 定义流类型以解决编译错误
type WatchPresenceStream = std::pin::Pin<
    Box<
        dyn futures::Stream<Item = Result<flare_proto::signaling::online::PresenceEvent, Status>>
            + Send
            + Sync
            + 'static,
    >,
>;
type SubscribeUserPresenceStream = std::pin::Pin<
    Box<
        dyn futures::Stream<
                Item = Result<flare_proto::signaling::online::UserPresenceEvent, Status>,
            > + Send
            + Sync
            + 'static,
    >,
>;

#[tonic::async_trait]
impl OnlineService for SimpleGatewayHandler {
    type WatchPresenceStream = WatchPresenceStream;
    type SubscribeUserPresenceStream = SubscribeUserPresenceStream;

    // ========== 在线会话管理 RPC ==========

    /// 用户登录
    async fn login(
        &self,
        request: Request<LoginRequest>,
    ) -> Result<Response<LoginResponse>, Status> {
        self.online_client.login(request).await
    }

    /// 用户登出
    async fn logout(
        &self,
        request: Request<LogoutRequest>,
    ) -> Result<Response<LogoutResponse>, Status> {
        self.online_client.logout(request).await
    }

    /// 心跳
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        self.online_client.heartbeat(request).await
    }

    /// 获取在线状态
    async fn get_online_status(
        &self,
        request: Request<GetOnlineStatusRequest>,
    ) -> Result<Response<GetOnlineStatusResponse>, Status> {
        self.online_client.get_online_status(request).await
    }

    /// 监听在线状态变化
    async fn watch_presence(
        &self,
        request: Request<WatchPresenceRequest>,
    ) -> Result<Response<Self::WatchPresenceStream>, Status> {
        let stream = self
            .online_client
            .watch_presence(request)
            .await?
            .into_inner();
        Ok(Response::new(Box::pin(stream)))
    }

    // ========== 用户在线状态 RPC ==========

    /// 查询用户在线状态
    async fn get_user_presence(
        &self,
        request: Request<GetUserPresenceRequest>,
    ) -> Result<Response<GetUserPresenceResponse>, Status> {
        self.online_client.get_user_presence(request).await
    }

    /// 批量查询在线状态
    async fn batch_get_user_presence(
        &self,
        request: Request<BatchGetUserPresenceRequest>,
    ) -> Result<Response<BatchGetUserPresenceResponse>, Status> {
        self.online_client.batch_get_user_presence(request).await
    }

    /// 订阅用户状态变化
    async fn subscribe_user_presence(
        &self,
        request: Request<SubscribeUserPresenceRequest>,
    ) -> Result<Response<Self::SubscribeUserPresenceStream>, Status> {
        let stream = self
            .online_client
            .subscribe_user_presence(request)
            .await?
            .into_inner();
        Ok(Response::new(Box::pin(stream)))
    }

    /// 列出用户设备
    async fn list_user_devices(
        &self,
        request: Request<ListUserDevicesRequest>,
    ) -> Result<Response<ListUserDevicesResponse>, Status> {
        self.online_client.list_user_devices(request).await
    }

    /// 踢出设备
    async fn kick_device(
        &self,
        request: Request<KickDeviceRequest>,
    ) -> Result<Response<KickDeviceResponse>, Status> {
        self.online_client.kick_device(request).await
    }

    /// 查询设备信息
    async fn get_device(
        &self,
        request: Request<GetDeviceRequest>,
    ) -> Result<Response<GetDeviceResponse>, Status> {
        self.online_client.get_device(request).await
    }
}
