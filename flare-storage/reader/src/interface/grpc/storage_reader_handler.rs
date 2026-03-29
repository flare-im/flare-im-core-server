use std::sync::Arc;

use chrono::{TimeZone, Utc};
use flare_proto::storage::storage_reader_service_server::StorageReaderService;
use flare_proto::storage::*;
use tonic::{Request, Response, Status};
use tracing::error;

use crate::application::handlers::{MessageStorageQueryHandler, QueryMessagesResult};
use crate::application::queries::{
    GetMessageQuery, ListMessageTagsQuery, QueryMessagesBySeqQuery, QueryMessagesQuery,
    SearchMessagesQuery,
};
use crate::convert::{filter_expression_from_proto, message_to_proto};
use crate::domain::repository::MessageStorage;
use flare_server_core::utils::extract_ctx_from_request_opt;

#[derive(Clone)]
pub struct StorageReaderGrpcHandler<M>
where
    M: MessageStorage + Send + Sync + Clone + 'static,
{
    query_handler: Arc<MessageStorageQueryHandler<M>>,
}

impl<M> StorageReaderGrpcHandler<M>
where
    M: MessageStorage + Send + Sync + Clone + 'static,
{
    pub async fn new(query_handler: Arc<MessageStorageQueryHandler<M>>) -> anyhow::Result<Self> {
        Ok(Self { query_handler })
    }
}

#[tonic::async_trait]
impl<M> StorageReaderService for StorageReaderGrpcHandler<M>
where
    M: MessageStorage + Send + Sync + Clone + 'static,
{
    async fn query_messages(
        &self,
        request: Request<QueryMessagesRequest>,
    ) -> Result<Response<QueryMessagesResponse>, Status> {
        // 从请求中提取上下文
        let ctx = extract_ctx_from_request_opt(&request)
            .unwrap_or_else(|| Arc::new(flare_server_core::context::Context::root()));

        let req = request.into_inner();
        let cursor_clone = req.cursor.clone();

        let query = QueryMessagesQuery {
            conversation_id: req.conversation_id,
            start_time: req.start_time,
            end_time: req.end_time,
            limit: req.limit,
            cursor: if req.cursor.is_empty() {
                None
            } else {
                Some(req.cursor)
            },
        };

        match self
            .query_handler
            .handle_query_messages_with_pagination(&ctx, query)
            .await
        {
            Ok(result) => {
                // 转换消息为 proto 类型
                let proto_messages: Vec<flare_proto::Message> = result
                    .messages
                    .into_iter()
                    .map(|msg| message_to_proto(&msg))
                    .collect();

                Ok(Response::new(QueryMessagesResponse {
                    messages: proto_messages,
                    next_cursor: result.next_cursor.clone(),
                    has_more: result.has_more,
                    pagination: Some(flare_proto::common::Pagination {
                        cursor: cursor_clone,
                        limit: req.limit,
                        has_more: result.has_more,
                        previous_cursor: String::new(),
                        total_size: result.total_size,
                    }),
                    status: Some(flare_server_core::error::ok_status()),
                }))
            }
            Err(err) => {
                error!(error = ?err, "Failed to query messages");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    async fn query_messages_by_seq(
        &self,
        request: Request<QueryMessagesBySeqRequest>,
    ) -> Result<Response<QueryMessagesBySeqResponse>, Status> {
        // 从请求中提取上下文
        let ctx = extract_ctx_from_request_opt(&request)
            .unwrap_or_else(|| Arc::new(flare_server_core::context::Context::root()));

        let req = request.into_inner();

        let query = QueryMessagesBySeqQuery {
            conversation_id: req.conversation_id,
            after_seq: req.after_seq,
            before_seq: if req.before_seq == 0 {
                None
            } else {
                Some(req.before_seq)
            },
            limit: req.limit,
            user_id: if req.user_id.is_empty() {
                None
            } else {
                Some(req.user_id)
            },
        };

        match self
            .query_handler
            .handle_query_messages_by_seq(&ctx, query)
            .await
        {
            Ok((messages, last_seq)) => {
                let message_count = messages.len() as i32;

                // 转换消息为 proto 类型
                let proto_messages: Vec<flare_proto::Message> = messages
                    .into_iter()
                    .map(|msg| message_to_proto(&msg))
                    .collect();

                // 构建基于 seq 的游标
                let next_cursor = proto_messages
                    .last()
                    .and_then(|msg| {
                        msg.extra
                            .get("seq")
                            .map(|seq_str| format!("seq:{}:{}", seq_str, msg.server_id))
                    })
                    .unwrap_or_default();
                let has_more = message_count >= req.limit;

                Ok(Response::new(QueryMessagesBySeqResponse {
                    messages: proto_messages,
                    next_cursor: next_cursor.clone(),
                    has_more,
                    last_seq: last_seq.unwrap_or(0),
                    status: Some(flare_server_core::error::ok_status()),
                }))
            }
            Err(err) => {
                error!(error = ?err, "Failed to query messages by seq");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    async fn search_messages(
        &self,
        request: Request<SearchMessagesRequest>,
    ) -> Result<Response<SearchMessagesResponse>, Status> {
        // 从请求中提取上下文
        let ctx = extract_ctx_from_request_opt(&request)
            .unwrap_or_else(|| Arc::new(flare_server_core::context::Context::root()));

        let req = request.into_inner();

        // 解析时间范围
        let (start_time, end_time) = if let Some(time_range) = &req.time_range {
            let start = time_range
                .start_time
                .as_ref()
                .and_then(|ts| Utc.timestamp_opt(ts.seconds, ts.nanos as u32).single());
            let end = time_range
                .end_time
                .as_ref()
                .and_then(|ts| Utc.timestamp_opt(ts.seconds, ts.nanos as u32).single());
            (start, end)
        } else {
            (None, None)
        };

        let query = SearchMessagesQuery {
            filters: req
                .filters
                .iter()
                .map(filter_expression_from_proto)
                .collect(),
            start_time: start_time.map(|dt| dt.timestamp()).unwrap_or(0),
            end_time: end_time.map(|dt| dt.timestamp()).unwrap_or(0),
            limit: req.pagination.as_ref().map(|p| p.limit).unwrap_or(200),
        };

        match self.query_handler.handle_search_messages(&ctx, query).await {
            Ok(messages) => {
                // 转换消息为 proto 类型
                let proto_messages: Vec<flare_proto::Message> = messages
                    .into_iter()
                    .map(|msg| message_to_proto(&msg))
                    .collect();

                let pagination = req.pagination.clone().map(|mut p| {
                    p.has_more = proto_messages.len() as i32 >= p.limit;
                    p
                });
                Ok(Response::new(SearchMessagesResponse {
                    messages: proto_messages,
                    pagination,
                    status: Some(flare_server_core::error::ok_status()),
                }))
            }
            Err(err) => {
                error!(error = ?err, "Failed to search messages");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    async fn get_message(
        &self,
        request: Request<GetMessageRequest>,
    ) -> Result<Response<GetMessageResponse>, Status> {
        // 从请求中提取上下文
        let ctx = extract_ctx_from_request_opt(&request)
            .unwrap_or_else(|| Arc::new(flare_server_core::context::Context::root()));

        let req = request.into_inner();

        let query = GetMessageQuery {
            message_id: req.message_id,
        };

        match self.query_handler.handle_get_message(&ctx, query).await {
            Ok(message) => {
                // 转换消息为 proto 类型
                let proto_message = message.map(|msg| message_to_proto(&msg));
                Ok(Response::new(GetMessageResponse {
                    message: proto_message,
                    status: Some(flare_server_core::error::ok_status()),
                }))
            }
            Err(err) => {
                error!(error = ?err, "Failed to get message");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    async fn list_message_tags(
        &self,
        request: Request<ListMessageTagsRequest>,
    ) -> Result<Response<ListMessageTagsResponse>, Status> {
        // 从请求中提取上下文
        let ctx = extract_ctx_from_request_opt(&request)
            .unwrap_or_else(|| Arc::new(flare_server_core::context::Context::root()));

        let _req = request.into_inner();

        let query = ListMessageTagsQuery {};

        match self
            .query_handler
            .handle_list_message_tags(&ctx, query)
            .await
        {
            Ok(tags) => Ok(Response::new(ListMessageTagsResponse {
                tags,
                status: Some(flare_server_core::error::ok_status()),
            })),
            Err(err) => {
                error!(error = ?err, "Failed to list message tags");
                Ok(Response::new(ListMessageTagsResponse {
                    tags: vec![],
                    status: Some(flare_server_core::error::ok_status()),
                }))
            }
        }
    }

    async fn export_messages(
        &self,
        request: Request<ExportMessagesRequest>,
    ) -> Result<Response<ExportMessagesResponse>, Status> {
        let _req = request.into_inner();
        // TODO: 实现导出消息功能
        Ok(Response::new(ExportMessagesResponse {
            export_task_id: format!("export_{}", uuid::Uuid::new_v4()),
            status: Some(flare_server_core::error::ok_status()),
        }))
    }

    async fn query_message_events(
        &self,
        _request: Request<QueryMessageEventsRequest>,
    ) -> Result<Response<QueryMessageEventsResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn query_message_edit_history(
        &self,
        _request: Request<QueryMessageEditHistoryRequest>,
    ) -> Result<Response<QueryMessageEditHistoryResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn query_message_read_list(
        &self,
        _request: Request<QueryMessageReadListRequest>,
    ) -> Result<Response<QueryMessageReadListResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn query_message_marks(
        &self,
        _request: Request<QueryMessageMarksRequest>,
    ) -> Result<Response<QueryMessageMarksResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn query_message_reactions(
        &self,
        _request: Request<QueryMessageReactionsRequest>,
    ) -> Result<Response<QueryMessageReactionsResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn query_conversation_events(
        &self,
        request: Request<QueryConversationEventsRequest>,
    ) -> Result<Response<QueryConversationEventsResponse>, Status> {
        let ctx = extract_ctx_from_request_opt(&request)
            .unwrap_or_else(|| Arc::new(flare_server_core::context::Context::root()));
        let req = request.into_inner();
        if req.conversation_id.trim().is_empty() {
            return Err(Status::invalid_argument("conversation_id is required"));
        }
        match self
            .query_handler
            .query_conversation_events_page(
                &ctx,
                &req.conversation_id,
                req.after_seq,
                req.before_seq,
                req.limit,
                req.event_type_filter,
            )
            .await
        {
            Ok((events, last_seq, has_more, next_cursor)) => {
                Ok(Response::new(QueryConversationEventsResponse {
                    events,
                    last_seq,
                    has_more,
                    next_cursor,
                    status: Some(flare_server_core::error::ok_status()),
                }))
            }
            Err(err) => {
                error!(error = ?err, "query_conversation_events failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    async fn get_conversation_message_head(
        &self,
        request: Request<GetConversationMessageHeadRequest>,
    ) -> Result<Response<GetConversationMessageHeadResponse>, Status> {
        let ctx = extract_ctx_from_request_opt(&request)
            .unwrap_or_else(|| Arc::new(flare_server_core::context::Context::root()));
        let req = request.into_inner();
        if req.conversation_id.trim().is_empty() {
            return Err(Status::invalid_argument("conversation_id is required"));
        }
        match self
            .query_handler
            .get_conversation_message_head_grpc(&ctx, &req.conversation_id)
            .await
        {
            Ok(Some((max_seq, last_message_id, last_timestamp))) => {
                Ok(Response::new(GetConversationMessageHeadResponse {
                    max_seq,
                    last_message_id,
                    last_timestamp,
                    status: Some(flare_server_core::error::ok_status()),
                }))
            }
            Ok(None) => Ok(Response::new(GetConversationMessageHeadResponse {
                max_seq: 0,
                last_message_id: String::new(),
                last_timestamp: None,
                status: Some(flare_server_core::error::ok_status()),
            })),
            Err(err) => {
                error!(error = ?err, "get_conversation_message_head failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }
}
