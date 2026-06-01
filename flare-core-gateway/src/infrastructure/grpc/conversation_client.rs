use anyhow::Result;
use tonic::transport::Channel;

use crate::context::Ctx;
use flare_grpc_proto::conversation::conversation_manage_service_client::ConversationManageServiceClient;
use flare_grpc_proto::conversation::conversation_read_service_client::ConversationReadServiceClient;
use flare_grpc_proto::conversation::*;

#[derive(Clone)]
pub struct ConversationReadServiceClientWrapper {
    client: ConversationReadServiceClient<Channel>,
}

impl ConversationReadServiceClientWrapper {
    fn request_with_ctx<T>(ctx: &Ctx, payload: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(payload);
        ctx.inject_to_grpc_metadata(request.metadata_mut());
        request
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            client: ConversationReadServiceClient::new(channel),
        }
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
        let mut request = tonic::Request::new(payload);
        ctx.inject_to_grpc_metadata(request.metadata_mut());
        request
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
