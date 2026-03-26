//! 消息服务客户端

use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use flare_proto::message::message_send_service_client::MessageSendServiceClient;
use flare_proto::message::*;

use flare_server_core::discovery::ServiceClient;

/// gRPC消息服务客户端
pub struct GrpcMessageClient {
    /// 服务客户端（用于服务发现）
    service_client: Option<Arc<Mutex<ServiceClient>>>,
    /// 服务名称
    service_name: String,
    /// 直连地址（当没有服务发现时使用）
    direct_address: Option<String>,
}

impl GrpcMessageClient {
    /// 创建新的gRPC消息服务客户端
    pub fn new(service_name: String) -> Self {
        Self {
            service_client: None,
            service_name,
            direct_address: None,
        }
    }

    /// 使用服务客户端创建gRPC消息服务客户端
    pub fn with_service_client(service_client: ServiceClient, service_name: String) -> Self {
        Self {
            service_client: Some(Arc::new(Mutex::new(service_client))),
            service_name,
            direct_address: None,
        }
    }

    /// 使用直接地址创建gRPC消息服务客户端
    pub fn with_direct_address(direct_address: String, service_name: String) -> Self {
        Self {
            service_client: None,
            service_name,
            direct_address: Some(direct_address),
        }
    }

    /// 获取gRPC客户端
    async fn get_client(&self) -> Result<MessageSendServiceClient<Channel>, Status> {
        if let Some(service_client) = &self.service_client {
            let mut client = service_client.lock().await;
            let channel = client.get_channel().await.map_err(|e| {
                Status::unavailable(format!(
                    "Failed to get channel from service discovery: {}",
                    e
                ))
            })?;
            Ok(MessageSendServiceClient::new(channel))
        } else if let Some(ref address) = self.direct_address {
            let channel = Channel::from_shared(address.clone())
                .map_err(|e| Status::invalid_argument(format!("Invalid address: {}", e)))?
                .connect()
                .await
                .map_err(|e| {
                    Status::unavailable(format!("Failed to connect to {}: {}", address, e))
                })?;
            Ok(MessageSendServiceClient::new(channel))
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
            Ok(MessageSendServiceClient::new(channel))
        }
    }

    /// 发送单条消息
    pub async fn send_message(
        &self,
        request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        let mut client = self.get_client().await?;
        client.send_message(request).await
    }

    /// 批量发送消息
    pub async fn batch_send_message(
        &self,
        request: Request<BatchSendMessageRequest>,
    ) -> Result<Response<BatchSendMessageResponse>, Status> {
        let mut client = self.get_client().await?;
        client.batch_send_message(request).await
    }

    /// 发送系统消息
    pub async fn send_system_message(
        &self,
        request: Request<SendSystemMessageRequest>,
    ) -> Result<Response<SendSystemMessageResponse>, Status> {
        let mut client = self.get_client().await?;
        client.send_system_message(request).await
    }

    /// 统一事件入口：ExecuteEventRequest → OperationResponse（与 RouterUpstream.RouteEvent 对齐）
    pub async fn execute_event(
        &self,
        request: Request<ExecuteEventRequest>,
    ) -> Result<Response<flare_proto::common::OperationResponse>, Status> {
        let mut client = self.get_client().await?;
        client.execute_event(request).await
    }

    pub async fn send_ack(
        &self,
        request: Request<SendAckRequest>,
    ) -> Result<Response<SendAckResponse>, Status> {
        let mut client = self.get_client().await?;
        client.send_ack(request).await
    }

    pub async fn send_custom_data(
        &self,
        request: Request<SendCustomDataRequest>,
    ) -> Result<Response<SendCustomDataResponse>, Status> {
        let mut client = self.get_client().await?;
        client.send_custom_data(request).await
    }
}
