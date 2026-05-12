use anyhow::Result;
use tonic::transport::{Channel, Uri};

use flare_grpc_proto::message::message_action_service_client::MessageActionServiceClient;
use flare_grpc_proto::message::message_send_service_client::MessageSendServiceClient;
use flare_grpc_proto::message::*;

/// MessageSendService gRPC 客户端封装
#[derive(Clone)]
pub struct MessageSendServiceClientWrapper {
    client: MessageSendServiceClient<Channel>,
}

impl MessageSendServiceClientWrapper {
    /// 创建新的客户端连接
    pub async fn new(url: &str) -> Result<Self> {
        let uri: Uri = url.parse()?;
        let channel = Channel::builder(uri).connect().await?;
        let client = MessageSendServiceClient::new(channel);
        Ok(Self { client })
    }

    /// 发送消息
    pub async fn send_message(
        &mut self,
        request: SendMessageRequest,
    ) -> Result<SendMessageResponse> {
        let response = self.client.send_message(request).await?;
        Ok(response.into_inner())
    }

    /// 批量发送消息
    pub async fn batch_send_message(
        &mut self,
        request: BatchSendMessageRequest,
    ) -> Result<BatchSendMessageResponse> {
        let response = self.client.batch_send_message(request).await?;
        Ok(response.into_inner())
    }
}

/// MessageActionService gRPC 客户端封装
#[derive(Clone)]
pub struct MessageActionServiceClientWrapper {
    client: MessageActionServiceClient<Channel>,
}

impl MessageActionServiceClientWrapper {
    /// 创建新的客户端连接
    pub async fn new(url: &str) -> Result<Self> {
        let uri: Uri = url.parse()?;
        let channel = Channel::builder(uri).connect().await?;
        let client = MessageActionServiceClient::new(channel);
        Ok(Self { client })
    }

    /// 撤回消息
    pub async fn recall_message(
        &mut self,
        request: RecallMessageRequest,
    ) -> Result<RecallMessageResponse> {
        let response = self.client.recall_message(request).await?;
        Ok(response.into_inner())
    }

    /// 标记消息已读
    pub async fn mark_message_read(
        &mut self,
        request: MarkMessageReadRequest,
    ) -> Result<MarkMessageReadResponse> {
        let response = self.client.mark_message_read(request).await?;
        Ok(response.into_inner())
    }
}
