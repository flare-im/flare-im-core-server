use std::sync::Arc;

use chrono::{TimeZone, Utc};
use flare_proto::storage::storage_reader_service_server::StorageReaderService;
use flare_proto::storage::*;
use tonic::{Request, Response, Status};
use tracing::error;

use crate::application::handlers::{MessageStorageQueryHandler};
use crate::application::queries::{
    GetMessageQuery, ListMessageTagsQuery, QueryMessageOperationsQuery, QueryMessagesBySeqQuery, QueryMessagesQuery,
    SearchMessagesQuery,
};

#[derive(Clone)]
pub struct StorageReaderGrpcHandler {
    query_handler: Arc<MessageStorageQueryHandler>,
}

impl StorageReaderGrpcHandler {
    pub async fn new(
        query_handler: Arc<MessageStorageQueryHandler>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            query_handler,
        })
    }
}

#[tonic::async_trait]
impl StorageReaderService for StorageReaderGrpcHandler {
    async fn query_messages(
        &self,
        request: Request<QueryMessagesRequest>,
    ) -> Result<Response<QueryMessagesResponse>, Status> {
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
            .handle_query_messages_with_pagination(query)
            .await
        {
            Ok(result) => {
                Ok(Response::new(QueryMessagesResponse {
                    messages: result.messages,
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

        match self.query_handler.handle_query_messages_by_seq(query).await {
            Ok((messages, last_seq)) => {
                let message_count = messages.len() as i32;
                // 构建基于 seq 的游标
                let next_cursor = messages
                    .last()
                    .and_then(|msg| {
                        msg.extra
                            .get("seq")
                            .map(|seq_str| format!("seq:{}:{}", seq_str, msg.server_id))
                    })
                    .unwrap_or_default();
                let has_more = message_count >= req.limit;

                Ok(Response::new(
                    QueryMessagesBySeqResponse {
                        messages,
                        next_cursor: next_cursor.clone(),
                        has_more,
                        last_seq: last_seq.unwrap_or(0),
                        status: Some(flare_server_core::error::ok_status()),
                    },
                ))
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
            filters: req.filters,
            start_time: start_time.map(|dt| dt.timestamp()).unwrap_or(0),
            end_time: end_time.map(|dt| dt.timestamp()).unwrap_or(0),
            limit: req.pagination.as_ref().map(|p| p.limit).unwrap_or(200),
        };

        match self.query_handler.handle_search_messages(query).await {
            Ok(messages) => {
                let pagination = req.pagination.clone().map(|mut p| {
                    p.has_more = messages.len() as i32 >= p.limit;
                    p
                });
                Ok(Response::new(SearchMessagesResponse {
                    messages,
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
        let req = request.into_inner();
        let query = GetMessageQuery {
            message_id: req.message_id,
        };

        match self.query_handler.handle_get_message(query).await {
            Ok(message) => Ok(Response::new(GetMessageResponse {
                message,
                status: Some(flare_server_core::error::ok_status()),
            })),
            Err(err) => {
                error!(error = ?err, "Failed to get message");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    async fn query_message_operations(
        &self,
        request: Request<QueryMessageOperationsRequest>,
    ) -> Result<Response<QueryMessageOperationsResponse>, Status> {
        let req = request.into_inner();
        let query = QueryMessageOperationsQuery {
            message_id: req.message_id,
        };

        match self.query_handler.handle_query_message_operations(query).await {
            Ok(operations) => {
                let pagination = req.pagination.map(|mut p| {
                    p.has_more = operations.len() as i32 >= p.limit;
                    p
                }).or_else(|| Some(flare_proto::common::Pagination {
                    limit: operations.len() as i32,
                    has_more: false,
                    cursor: String::new(),
                    previous_cursor: String::new(),
                    total_size: operations.len() as i64,
                }));

                Ok(Response::new(QueryMessageOperationsResponse {
                    operations,
                    pagination,
                    status: Some(flare_server_core::error::ok_status()),
                }))
            }
            Err(err) => {
                error!(error = ?err, "Failed to query message operations");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    async fn list_message_tags(
        &self,
        request: Request<ListMessageTagsRequest>,
    ) -> Result<Response<ListMessageTagsResponse>, Status> {
        let _req = request.into_inner();
        let query = ListMessageTagsQuery {};

        match self.query_handler.handle_list_message_tags(query).await {
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
        let req = request.into_inner();
        // TODO: 实现导出消息功能
        Ok(Response::new(ExportMessagesResponse {
            export_task_id: format!("export_{}", uuid::Uuid::new_v4()),
            status: Some(flare_server_core::error::ok_status()),
        }))
    }
}