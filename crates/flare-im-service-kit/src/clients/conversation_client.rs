use flare_server_core::error::Result;
use tonic::transport::Channel;

use crate::clients::{Ctx, admin_bearer_request};
use flare_grpc_proto::conversation::conversation_admin_service_client::ConversationAdminServiceClient;
use flare_grpc_proto::conversation::conversation_manage_service_client::ConversationManageServiceClient;
use flare_grpc_proto::conversation::conversation_read_service_client::ConversationReadServiceClient;
use flare_grpc_proto::conversation::*;

/// 会话读客户端。用户态方法带调用者上下文；管理员方法（`admin_*`）走同一通道上的
/// `ConversationAdminService`，只带 admin-gateway 原样转发的 admin 令牌。
#[derive(Clone)]
pub struct ConversationReadServiceClientWrapper {
    client: ConversationReadServiceClient<Channel>,
    admin: ConversationAdminServiceClient<Channel>,
}

impl ConversationReadServiceClientWrapper {
    fn request_with_ctx<T>(ctx: &Ctx, payload: T) -> tonic::Request<T> {
        flare_server_core::request_with_context(payload, ctx)
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            client: ConversationReadServiceClient::new(channel.clone()),
            admin: ConversationAdminServiceClient::new(channel),
        }
    }

    pub async fn admin_list_conversations(
        &mut self,
        admin_token: &str,
        request: AdminListConversationsRequest,
    ) -> Result<AdminListConversationsResponse> {
        let response = self
            .admin
            .admin_list_conversations(admin_bearer_request(request, admin_token)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn admin_get_conversation(
        &mut self,
        admin_token: &str,
        request: AdminGetConversationRequest,
    ) -> Result<AdminGetConversationResponse> {
        let response = self
            .admin
            .admin_get_conversation(admin_bearer_request(request, admin_token)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn admin_list_participants(
        &mut self,
        admin_token: &str,
        request: AdminListParticipantsRequest,
    ) -> Result<AdminListParticipantsResponse> {
        let response = self
            .admin
            .admin_list_participants(admin_bearer_request(request, admin_token)?)
            .await?;
        Ok(response.into_inner())
    }

    pub async fn list_conversations_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: ListConversationsRequest,
    ) -> Result<ListConversationsResponse> {
        let response = self
            .client
            .list_conversations(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn get_conversation_detail_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: GetConversationDetailRequest,
    ) -> Result<GetConversationDetailResponse> {
        let response = self
            .client
            .get_conversation_detail(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn list_conversation_participants_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: ListConversationParticipantsRequest,
    ) -> Result<ListConversationParticipantsResponse> {
        let response = self
            .client
            .list_conversation_participants(Self::request_with_ctx(ctx, request))
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
