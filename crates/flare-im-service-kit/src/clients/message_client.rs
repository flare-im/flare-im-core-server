use flare_server_core::error::Result;
use tonic::transport::Channel;

use crate::clients::Ctx;
use flare_grpc_proto::message::message_action_service_client::MessageActionServiceClient;
use flare_grpc_proto::message::message_event_service_client::MessageEventServiceClient;
use flare_grpc_proto::message::message_send_service_client::MessageSendServiceClient;
use flare_grpc_proto::message::*;

/// MessageSendService gRPC 客户端封装
#[derive(Clone)]
pub struct MessageSendServiceClientWrapper {
    client: MessageSendServiceClient<Channel>,
}

impl MessageSendServiceClientWrapper {
    fn request_with_ctx<T>(ctx: &Ctx, payload: T) -> tonic::Request<T> {
        flare_server_core::request_with_context(payload, ctx)
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

    /// 发送系统消息
    pub async fn send_system_message(
        &mut self,
        request: SendSystemMessageRequest,
    ) -> Result<SendSystemMessageResponse> {
        let response = self.client.send_system_message(request).await?;
        Ok(response.into_inner())
    }

    /// 发送系统消息（透传网关上下文）
    pub async fn send_system_message_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: SendSystemMessageRequest,
    ) -> Result<SendSystemMessageResponse> {
        let response = self
            .client
            .send_system_message(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }

    /// 上行 ACK
    pub async fn send_ack(&mut self, request: SendAckRequest) -> Result<SendAckResponse> {
        let response = self.client.send_ack(request).await?;
        Ok(response.into_inner())
    }

    /// 上行 ACK（透传网关上下文）
    pub async fn send_ack_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: SendAckRequest,
    ) -> Result<SendAckResponse> {
        let response = self
            .client
            .send_ack(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }

    /// 上行 CustomData
    pub async fn send_custom_data(
        &mut self,
        request: SendCustomDataRequest,
    ) -> Result<SendCustomDataResponse> {
        let response = self.client.send_custom_data(request).await?;
        Ok(response.into_inner())
    }

    /// 上行 CustomData（透传网关上下文）
    pub async fn send_custom_data_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: SendCustomDataRequest,
    ) -> Result<SendCustomDataResponse> {
        let response = self
            .client
            .send_custom_data(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }
}

/// MessageEventService gRPC 客户端封装
#[derive(Clone)]
pub struct MessageEventServiceClientWrapper {
    client: MessageEventServiceClient<Channel>,
}

impl MessageEventServiceClientWrapper {
    fn request_with_ctx<T>(ctx: &Ctx, payload: T) -> tonic::Request<T> {
        flare_server_core::request_with_context(payload, ctx)
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            client: MessageEventServiceClient::new(channel),
        }
    }

    /// 执行操作事件
    pub async fn execute_event(&mut self, request: ExecuteEventRequest) -> Result<()> {
        self.client.execute_event(request).await?;
        Ok(())
    }

    /// 执行操作事件（透传网关上下文）
    pub async fn execute_event_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: ExecuteEventRequest,
    ) -> Result<()> {
        self.client
            .execute_event(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(())
    }
}

/// MessageActionService gRPC 客户端封装
#[derive(Clone)]
pub struct MessageActionServiceClientWrapper {
    client: MessageActionServiceClient<Channel>,
}

impl MessageActionServiceClientWrapper {
    fn request_with_ctx<T>(ctx: &Ctx, payload: T) -> tonic::Request<T> {
        flare_server_core::request_with_context(payload, ctx)
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
}
