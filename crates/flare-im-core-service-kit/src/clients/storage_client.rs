use flare_server_core::error::Result;
use tonic::transport::Channel;

use crate::clients::Ctx;
use crate::staff_context::staff_traced_request;
use flare_grpc_proto::storage::storage_reader_service_client::StorageReaderServiceClient;
use flare_grpc_proto::storage::{
    ExportMessagesRequest, ExportMessagesResponse, GetConversationMessageHeadRequest,
    GetConversationMessageHeadResponse, GetMessageRequest, GetMessageResponse,
    QueryMessageEventsRequest, QueryMessageEventsResponse, QueryMessageWriteLedgerRequest,
    QueryMessageWriteLedgerResponse, SearchMessagesRequest, SearchMessagesResponse,
};

#[derive(Clone)]
pub struct StorageReaderServiceClientWrapper {
    client: StorageReaderServiceClient<Channel>,
}

impl StorageReaderServiceClientWrapper {
    /// 调用方元数据，另把 `ctx` 里入站验过签的 `x-flare-ctx` 原样转发（Storage Reader 的读接口
    /// 以签名为准，两者一致）。入站没有签名时只带元数据。
    fn request_with_ctx<T>(ctx: &Ctx, payload: T) -> Result<tonic::Request<T>> {
        let mut request = flare_server_core::request_with_context(payload, ctx);
        flare_server_core::forward_signed_context(&mut request, ctx)?;
        Ok(request)
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            client: StorageReaderServiceClient::new(channel),
        }
    }

    /// 管理检索：只带 Staff `x-flare-ctx` 与请求号、追踪号（见 `staff_traced_request`）。
    pub async fn search_messages_as_admin(
        &mut self,
        staff_token: &str,
        ctx: &Ctx,
        request: SearchMessagesRequest,
    ) -> Result<SearchMessagesResponse> {
        let response = self
            .client
            .search_messages(staff_traced_request(request, staff_token, ctx)?)
            .await?;
        Ok(response.into_inner())
    }

    /// 管理检索：只带 Staff `x-flare-ctx` 与请求号、追踪号（见 `staff_traced_request`）。
    pub async fn get_message_as_admin(
        &mut self,
        staff_token: &str,
        ctx: &Ctx,
        request: GetMessageRequest,
    ) -> Result<GetMessageResponse> {
        let response = self
            .client
            .get_message(staff_traced_request(request, staff_token, ctx)?)
            .await?;
        Ok(response.into_inner())
    }

    /// 查询会话在持久化存储中的权威消息头（含 `max_seq`）。
    pub async fn get_conversation_message_head_with_ctx(
        &mut self,
        ctx: &Ctx,
        request: GetConversationMessageHeadRequest,
    ) -> Result<GetConversationMessageHeadResponse> {
        let response = self
            .client
            .get_conversation_message_head(Self::request_with_ctx(ctx, request)?)
            .await?;
        Ok(response.into_inner())
    }

    /// 管理检索：只带 Staff `x-flare-ctx` 与请求号、追踪号（见 `staff_traced_request`）。
    pub async fn query_message_events_as_admin(
        &mut self,
        staff_token: &str,
        ctx: &Ctx,
        request: QueryMessageEventsRequest,
    ) -> Result<QueryMessageEventsResponse> {
        let response = self
            .client
            .query_message_events(staff_traced_request(request, staff_token, ctx)?)
            .await?;
        Ok(response.into_inner())
    }

    /// 管理检索：只带 Staff `x-flare-ctx` 与请求号、追踪号（见 `staff_traced_request`）。
    pub async fn query_message_write_ledger_as_admin(
        &mut self,
        staff_token: &str,
        ctx: &Ctx,
        request: QueryMessageWriteLedgerRequest,
    ) -> Result<QueryMessageWriteLedgerResponse> {
        let response = self
            .client
            .query_message_write_ledger(staff_traced_request(request, staff_token, ctx)?)
            .await?;
        Ok(response.into_inner())
    }

    /// 管理检索：只带 Staff `x-flare-ctx` 与请求号、追踪号（见 `staff_traced_request`）。
    pub async fn export_messages_as_admin(
        &mut self,
        staff_token: &str,
        ctx: &Ctx,
        request: ExportMessagesRequest,
    ) -> Result<ExportMessagesResponse> {
        let response = self
            .client
            .export_messages(staff_traced_request(request, staff_token, ctx)?)
            .await?;
        Ok(response.into_inner())
    }
}
