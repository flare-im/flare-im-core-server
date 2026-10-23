use flare_server_core::error::Result;
use tonic::transport::Channel;

use crate::clients::Ctx;
use crate::staff_context::staff_context_request;
use flare_grpc_proto::conversation::conversation_admin_service_client::ConversationAdminServiceClient;
use flare_grpc_proto::conversation::conversation_manage_service_client::ConversationManageServiceClient;
use flare_grpc_proto::conversation::conversation_read_service_client::ConversationReadServiceClient;
use flare_grpc_proto::conversation::message_read_receipt_service_client::MessageReadReceiptServiceClient;
use flare_grpc_proto::conversation::*;

/// 会话读客户端。用户态方法带调用者上下文与签名身份（`x-flare-ctx`）；管理员方法
/// （`admin_*`）走同一通道上的 `ConversationAdminService`，只带 admin-gateway 验过后原样转发的
/// Staff `x-flare-ctx`（`staff_token`）。群聊已读回执（`*_message_read*`）走同一通道上的
/// `MessageReadReceiptService`，身份同用户态方法。
#[derive(Clone)]
pub struct ConversationReadServiceClientWrapper {
    client: ConversationReadServiceClient<Channel>,
    admin: ConversationAdminServiceClient<Channel>,
    read_receipts: MessageReadReceiptServiceClient<Channel>,
}

impl ConversationReadServiceClientWrapper {
    /// 元数据由 `ctx` 编码，须与签名同一身份（会话读接口拒绝不一致的元数据）。`signed` 是调用方
    /// 刚为已认证身份签的令牌；没有时转发 `ctx` 里入站验过签的令牌；都没有就只带元数据。
    fn request_with_ctx<T>(
        ctx: &Ctx,
        signed: Option<&str>,
        payload: T,
    ) -> Result<tonic::Request<T>> {
        let mut request = flare_server_core::request_with_context(payload, ctx);
        match signed {
            Some(token) => flare_server_core::set_signed_context_token(&mut request, token)?,
            None => {
                flare_server_core::forward_signed_context(&mut request, ctx)?;
            }
        }
        Ok(request)
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            client: ConversationReadServiceClient::new(channel.clone()),
            admin: ConversationAdminServiceClient::new(channel.clone()),
            read_receipts: MessageReadReceiptServiceClient::new(channel),
        }
    }

    pub async fn admin_list_conversations(
        &mut self,
        staff_token: &str,
        request: AdminListConversationsRequest,
    ) -> Result<AdminListConversationsResponse> {
        let response = self
            .admin
            .admin_list_conversations(staff_context_request(request, staff_token)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn admin_get_conversation(
        &mut self,
        staff_token: &str,
        request: AdminGetConversationRequest,
    ) -> Result<AdminGetConversationResponse> {
        let response = self
            .admin
            .admin_get_conversation(staff_context_request(request, staff_token)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn admin_list_participants(
        &mut self,
        staff_token: &str,
        request: AdminListParticipantsRequest,
    ) -> Result<AdminListParticipantsResponse> {
        let response = self
            .admin
            .admin_list_participants(staff_context_request(request, staff_token)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn list_conversations_with_ctx(
        &mut self,
        ctx: &Ctx,
        signed: Option<&str>,
        request: ListConversationsRequest,
    ) -> Result<ListConversationsResponse> {
        let response = self
            .client
            .list_conversations(Self::request_with_ctx(ctx, signed, request)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn get_conversation_detail_with_ctx(
        &mut self,
        ctx: &Ctx,
        signed: Option<&str>,
        request: GetConversationDetailRequest,
    ) -> Result<GetConversationDetailResponse> {
        let response = self
            .client
            .get_conversation_detail(Self::request_with_ctx(ctx, signed, request)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn list_conversation_participants_with_ctx(
        &mut self,
        ctx: &Ctx,
        signed: Option<&str>,
        request: ListConversationParticipantsRequest,
    ) -> Result<ListConversationParticipantsResponse> {
        let response = self
            .client
            .list_conversation_participants(Self::request_with_ctx(ctx, signed, request)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn get_message_read_summaries_with_ctx(
        &mut self,
        ctx: &Ctx,
        signed: Option<&str>,
        request: GetMessageReadSummariesRequest,
    ) -> Result<GetMessageReadSummariesResponse> {
        let response = self
            .read_receipts
            .get_message_read_summaries(Self::request_with_ctx(ctx, signed, request)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn list_message_readers_with_ctx(
        &mut self,
        ctx: &Ctx,
        signed: Option<&str>,
        request: ListMessageReadersRequest,
    ) -> Result<ListMessageReadersResponse> {
        let response = self
            .read_receipts
            .list_message_readers(Self::request_with_ctx(ctx, signed, request)?)
            .await?;
        Ok(response.into_inner())
    }
}

#[derive(Clone)]
pub struct ConversationManageServiceClientWrapper {
    client: ConversationManageServiceClient<Channel>,
}

impl ConversationManageServiceClientWrapper {
    fn request_with_ctx<T>(ctx: &Ctx, payload: T) -> tonic::Request<T> {
        flare_server_core::request_with_context(payload, ctx)
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            client: ConversationManageServiceClient::new(channel),
        }
    }

    pub async fn manage_participants_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: ManageParticipantsRequest,
    ) -> Result<ManageParticipantsResponse> {
        let response = self
            .client
            .manage_participants(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn update_conversation_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: UpdateConversationRequest,
    ) -> Result<UpdateConversationResponse> {
        let response = self
            .client
            .update_conversation(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }
}
