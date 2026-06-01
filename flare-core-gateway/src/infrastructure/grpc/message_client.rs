use anyhow::Result;
use tonic::transport::Channel;

use crate::context::Ctx;
use flare_grpc_proto::message::message_action_service_client::MessageActionServiceClient;
use flare_grpc_proto::message::message_send_service_client::MessageSendServiceClient;
use flare_grpc_proto::message::*;

/// MessageSendService gRPC 客户端封装
#[derive(Clone)]
pub struct MessageSendServiceClientWrapper {
    client: MessageSendServiceClient<Channel>,
}

impl MessageSendServiceClientWrapper {
    fn request_with_ctx<T>(ctx: &Ctx, payload: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(payload);
        ctx.inject_to_grpc_metadata(request.metadata_mut());
        request
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            client: MessageSendServiceClient::new(channel),
        }
    }

    /// 发送消息
    pub async fn send_message(
        &mut self,
        request: SendMessageRequest,
    ) -> Result<SendMessageResponse> {
        let response = self.client.send_message(request).await?;
        Ok(response.into_inner())
    }

    /// 发送消息（透传网关上下文）
    pub async fn send_message_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: SendMessageRequest,
    ) -> Result<SendMessageResponse> {
        let response = self
            .client
            .send_message(Self::request_with_ctx(ctx, request))
            .await?;
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

    /// 批量发送消息（透传网关上下文）
    pub async fn batch_send_message_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: BatchSendMessageRequest,
    ) -> Result<BatchSendMessageResponse> {
        let response = self
            .client
            .batch_send_message(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }
}

/// MessageActionService gRPC 客户端封装
#[derive(Clone)]
pub struct MessageActionServiceClientWrapper {
    client: MessageActionServiceClient<Channel>,
}

impl MessageActionServiceClientWrapper {
    fn request_with_ctx<T>(ctx: &Ctx, payload: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(payload);
        ctx.inject_to_grpc_metadata(request.metadata_mut());
        request
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            client: MessageActionServiceClient::new(channel),
        }
    }

    /// 撤回消息
    pub async fn recall_message(
        &mut self,
        request: RecallMessageRequest,
    ) -> Result<RecallMessageResponse> {
        let response = self.client.recall_message(request).await?;
        Ok(response.into_inner())
    }

    /// 撤回消息（透传网关上下文）
    pub async fn recall_message_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: RecallMessageRequest,
    ) -> Result<RecallMessageResponse> {
        let response = self
            .client
            .recall_message(Self::request_with_ctx(ctx, request))
            .await?;
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

    /// 标记消息已读（透传网关上下文）
    pub async fn mark_message_read_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: MarkMessageReadRequest,
    ) -> Result<MarkMessageReadResponse> {
        let response = self
            .client
            .mark_message_read(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }
}
