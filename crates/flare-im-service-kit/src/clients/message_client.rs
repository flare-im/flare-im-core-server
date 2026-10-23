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

    /// Forward a gateway-authenticated identity to protected MessageIngest.
    ///
    /// 失败时原样返回 gRPC `Status`（保留状态码与 `details` 中的 `ErrorDetail`）：转成 `FlareError`
    /// 会把 `FAILED_PRECONDITION`（PreSend 策略拒绝）并进未知错误，调用方（API Gateway 的 HTTP
    /// 发送）要按状态码映射。
    pub async fn send_message_with_signed_context(
        &mut self,
        request: SendMessageRequest,
        token: &str,
    ) -> std::result::Result<SendMessageResponse, tonic::Status> {
        let mut request = tonic::Request::new(request);
        flare_server_core::grpc::client::set_signed_context_token(&mut request, token)?;
        Ok(self.client.send_message(request).await?.into_inner())
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

    /// 执行操作事件：带网关为已验证用户签的上下文（`signed`），Orchestrator 以签名为准。
    ///
    /// 失败时原样返回 gRPC `Status`（保留状态码与 `details` 中的 `ErrorDetail`）：转成 `FlareError`
    /// 会丢掉授权拒绝的原因码（如 `MESSAGE_OPERATION_NOT_MEMBER`），调用方要按状态码与原因映射。
    pub async fn execute_event_with_ctx(
        &mut self,
        ctx: &Ctx,
        signed: &str,
        request: ExecuteEventRequest,
    ) -> std::result::Result<(), tonic::Status> {
        let mut request = Self::request_with_ctx(ctx, request);
        flare_server_core::set_signed_context_token(&mut request, signed)?;
        self.client.execute_event(request).await?;
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

    /// 撤回消息：带网关为已验证用户签的上下文（`signed`），Orchestrator 以签名为准。失败时原样
    /// 返回 gRPC `Status`，同 [`MessageEventServiceClientWrapper::execute_event_with_ctx`]（撤回被拒的
    /// 原因如 `MESSAGE_OPERATION_NOT_SENDER`）。
    pub async fn recall_message_with_ctx(
        &mut self,
        ctx: &Ctx,
        signed: &str,
        request: RecallMessageRequest,
    ) -> std::result::Result<RecallMessageResponse, tonic::Status> {
        let mut request = Self::request_with_ctx(ctx, request);
        flare_server_core::set_signed_context_token(&mut request, signed)?;
        let response = self.client.recall_message(request).await?;
        Ok(response.into_inner())
    }
}
