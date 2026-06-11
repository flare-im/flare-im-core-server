//! gRPC 出站适配器：发现下游 Channel 并调用 Conversation（读 + 游标写）/ Storage Reader。
//!
//! 本模块实现 application 层定义的 Port trait，使用 tonic 框架。

use flare_grpc_proto::conversation::conversation_manage_service_client::ConversationManageServiceClient;
use flare_grpc_proto::conversation::conversation_read_service_client::ConversationReadServiceClient;
use flare_grpc_proto::conversation::{
    ConversationBootstrapRequest, ConversationBootstrapResponse, GetConversationDetailRequest,
    GetConversationDetailResponse, ListConversationParticipantsRequest,
    ListConversationParticipantsResponse, UpdateConversationUserSettingsRequest,
    UpdateConversationUserSettingsResponse, UpdateCursorRequest,
};
use flare_grpc_proto::storage::storage_reader_service_client::StorageReaderServiceClient;
use flare_grpc_proto::storage::{
    GetConversationMessageHeadRequest, QueryConversationEventsRequest, QueryMessagesBySeqRequest,
};
use flare_im_contracts::Ctx;
use flare_im_contracts::service_names::{CONVERSATION, STORAGE_READER, get_service_name};
use flare_proto::Message;
use flare_server_core::client::request_with_context;
use flare_server_core::error::FlareError;
use tonic::transport::Channel;

use crate::application::error::{discovery_unavailable, flare_from_tonic_status};

use crate::application::ports::{
    ConversationEventReadPort, ConversationSyncPort, QueryEventsPage,
    StorageConversationMessageHead, StorageReadPort,
};

/// gRPC 同步适配器（基于 tonic）
///
/// 实现 application 层的 Port trait，通过 gRPC 调用下游服务。
#[derive(Clone, Copy, Default)]
pub struct GrpcSyncAdapters;

impl GrpcSyncAdapters {
    async fn create_channel(service_name: &str) -> Result<Channel, FlareError> {
        let fallback = flare_im_service_kit::discovery::default_static_grpc_fallback(service_name);
        flare_im_service_kit::discovery::connect_grpc_channel_with_fallback(service_name, fallback)
            .await
            .map_err(|e| discovery_unavailable(service_name, e))
    }

    async fn conversation_read_client() -> Result<ConversationReadServiceClient<Channel>, FlareError>
    {
        let name = get_service_name(CONVERSATION);
        let ch = Self::create_channel(&name).await?;
        Ok(ConversationReadServiceClient::new(ch))
    }

    async fn conversation_manage_client()
    -> Result<ConversationManageServiceClient<Channel>, FlareError> {
        let name = get_service_name(CONVERSATION);
        let ch = Self::create_channel(&name).await?;
        Ok(ConversationManageServiceClient::new(ch))
    }

    async fn storage_client() -> Result<StorageReaderServiceClient<Channel>, FlareError> {
        let name = get_service_name(STORAGE_READER);
        let ch = Self::create_channel(&name).await?;
        Ok(StorageReaderServiceClient::new(ch))
    }
}

impl ConversationSyncPort for GrpcSyncAdapters {
    async fn conversation_bootstrap(
        &self,
        ctx: &Ctx,
        req: ConversationBootstrapRequest,
    ) -> Result<ConversationBootstrapResponse, FlareError> {
        let mut client = Self::conversation_read_client().await?;
        let resp = client
            .conversation_bootstrap(request_with_context(req, ctx))
            .await
            .map_err(|e| flare_from_tonic_status(&e))?;
        Ok(resp.into_inner())
    }

    async fn update_read_cursor(
        &self,
        ctx: &Ctx,
        req: UpdateCursorRequest,
    ) -> Result<(), FlareError> {
        let mut client = Self::conversation_manage_client().await?;
        client
            .update_cursor(request_with_context(req, ctx))
            .await
            .map_err(|e| flare_from_tonic_status(&e))?;
        Ok(())
    }

    async fn conversation_detail(
        &self,
        ctx: &Ctx,
        req: GetConversationDetailRequest,
    ) -> Result<GetConversationDetailResponse, FlareError> {
        let mut client = Self::conversation_read_client().await?;
        let resp = client
            .get_conversation_detail(request_with_context(req, ctx))
            .await
            .map_err(|e| flare_from_tonic_status(&e))?;
        Ok(resp.into_inner())
    }

    async fn list_conversation_participants(
        &self,
        ctx: &Ctx,
        req: ListConversationParticipantsRequest,
    ) -> Result<ListConversationParticipantsResponse, FlareError> {
        let mut client = Self::conversation_read_client().await?;
        let resp = client
            .list_conversation_participants(request_with_context(req, ctx))
            .await
            .map_err(|e| flare_from_tonic_status(&e))?;
        Ok(resp.into_inner())
    }

    async fn update_conversation_user_settings(
        &self,
        ctx: &Ctx,
        req: UpdateConversationUserSettingsRequest,
    ) -> Result<UpdateConversationUserSettingsResponse, FlareError> {
        let mut client = Self::conversation_manage_client().await?;
        let resp = client
            .update_conversation_user_settings(request_with_context(req, ctx))
            .await
            .map_err(|e| flare_from_tonic_status(&e))?;
        Ok(resp.into_inner())
    }
}

impl StorageReadPort for GrpcSyncAdapters {
    async fn query_messages_by_seq(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        after_seq: i64,
        before_seq: i64,
        limit: i32,
        user_id: &str,
    ) -> Result<(Vec<Message>, i64), FlareError> {
        let mut client = Self::storage_client().await?;
        let resp = client
            .query_messages_by_seq(request_with_context(
                QueryMessagesBySeqRequest {
                    conversation_id: conversation_id.to_string(),
                    after_seq,
                    before_seq,
                    limit,
                    user_id: user_id.to_string(),
                    include_burned_placeholder: false,
                },
                ctx,
            ))
            .await
            .map_err(|e| flare_from_tonic_status(&e))?
            .into_inner();
        let last_seq = resp.last_seq;
        Ok((resp.messages, last_seq))
    }

    async fn get_conversation_message_head(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
    ) -> Result<StorageConversationMessageHead, FlareError> {
        let mut client = Self::storage_client().await?;
        let resp = client
            .get_conversation_message_head(request_with_context(
                GetConversationMessageHeadRequest {
                    conversation_id: conversation_id.to_string(),
                },
                ctx,
            ))
            .await
            .map_err(|e| flare_from_tonic_status(&e))?
            .into_inner();
        Ok(StorageConversationMessageHead {
            max_seq: resp.max_seq,
            last_message_id: resp.last_message_id,
            last_timestamp: resp.last_timestamp,
        })
    }
}

impl ConversationEventReadPort for GrpcSyncAdapters {
    async fn query_events_page(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        after_seq: i64,
        before_seq: i64,
        limit: i32,
        event_types: &[i32],
        include_deleted: bool,
    ) -> Result<QueryEventsPage, FlareError> {
        let _ = include_deleted;
        let mut client = Self::storage_client().await?;
        let resp = client
            .query_conversation_events(request_with_context(
                QueryConversationEventsRequest {
                    conversation_id: conversation_id.to_string(),
                    after_seq,
                    before_seq,
                    limit,
                    event_type_filter: event_types.to_vec(),
                },
                ctx,
            ))
            .await
            .map_err(|e| flare_from_tonic_status(&e))?
            .into_inner();
        Ok(QueryEventsPage {
            events: resp.events,
            last_seq: resp.last_seq,
            has_more: resp.has_more,
            next_cursor: resp.next_cursor,
        })
    }
}
