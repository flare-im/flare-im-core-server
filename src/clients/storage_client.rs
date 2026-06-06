use flare_server_core::error::Result;
use tonic::transport::Channel;

use crate::clients::Ctx;
use flare_grpc_proto::storage::storage_reader_service_client::StorageReaderServiceClient;
use flare_grpc_proto::storage::{
    ExportMessagesRequest, ExportMessagesResponse, GetMessageRequest, GetMessageResponse,
    QueryMessageEventsRequest, QueryMessageEventsResponse, QueryMessageWriteLedgerRequest,
    QueryMessageWriteLedgerResponse, SearchMessagesRequest, SearchMessagesResponse,
};

#[derive(Clone)]
pub struct StorageReaderServiceClientWrapper {
    client: StorageReaderServiceClient<Channel>,
}

impl StorageReaderServiceClientWrapper {
    fn request_with_ctx<T>(ctx: &Ctx, payload: T) -> tonic::Request<T> {
        flare_server_core::request_with_context(payload, ctx)
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            client: StorageReaderServiceClient::new(channel),
        }
    }

    pub async fn search_messages_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: SearchMessagesRequest,
    ) -> Result<SearchMessagesResponse> {
        let response = self
            .client
            .search_messages(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn get_message_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: GetMessageRequest,
    ) -> Result<GetMessageResponse> {
        let response = self
            .client
            .get_message(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn query_message_events_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: QueryMessageEventsRequest,
    ) -> Result<QueryMessageEventsResponse> {
        let response = self
            .client
            .query_message_events(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn query_message_write_ledger_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: QueryMessageWriteLedgerRequest,
    ) -> Result<QueryMessageWriteLedgerResponse> {
        let response = self
            .client
            .query_message_write_ledger(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }

    pub async fn export_messages_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: ExportMessagesRequest,
    ) -> Result<ExportMessagesResponse> {
        let response = self
            .client
            .export_messages(Self::request_with_ctx(ctx, request))
            .await?;
        Ok(response.into_inner())
    }
}
