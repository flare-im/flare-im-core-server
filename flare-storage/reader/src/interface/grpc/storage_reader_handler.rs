use std::sync::Arc;

use chrono::{TimeZone, Utc};
use flare_grpc_proto::storage::storage_reader_service_server::StorageReaderService;
use flare_grpc_proto::storage::*;
use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status};
use tracing::error;

use crate::application::handlers::MessageStorageQueryHandler;
use crate::application::queries::{
    GetMessageQuery, ListMessageTagsQuery, QueryMessagesBySeqQuery, QueryMessagesQuery,
    SearchMessagesQuery,
};
use crate::convert::{
    edit_history_entry_to_proto, event_to_proto_or_default, event_type_from_proto_i32,
    filter_expression_from_proto, mark_entry_to_proto, message_to_proto, reaction_item_to_proto,
    read_list_entry_to_proto,
};
use crate::domain::model::{
    EventType, MessageExportTaskDraft, MessageWriteLedgerEntry as DomainMessageWriteLedgerEntry,
    MessageWriteLedgerQuery,
};
use crate::domain::repository::MessageStorage;
use flare_im_contracts::utils::extract_seq_from_message;
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
    pub async fn new(
        query_handler: Arc<MessageStorageQueryHandler<M>>,
    ) -> flare_server_core::error::Result<Self> {
        Ok(Self { query_handler })
    }
}

fn message_event_limit(pagination: Option<&flare_proto::common::Pagination>) -> i32 {
    pagination
        .map(|pagination| pagination.limit)
        .unwrap_or(100)
        .clamp(1, 500)
}

fn message_event_offset(pagination: Option<&flare_proto::common::Pagination>) -> i64 {
    pagination
        .and_then(|pagination| pagination.cursor.trim().parse::<i64>().ok())
        .unwrap_or_default()
        .max(0)
}

fn message_event_type_filter(event_types: &[i32]) -> Result<Option<Vec<EventType>>, Status> {
    if event_types.is_empty() {
        return Ok(None);
    }

    let mut filter = Vec::with_capacity(event_types.len());
    for event_type in event_types {
        let converted = event_type_from_proto_i32(*event_type);
        if converted == EventType::Unspecified {
            return Err(Status::invalid_argument(format!(
                "unknown message event type: {event_type}"
            )));
        }
        filter.push(converted);
    }
    Ok(Some(filter))
}

fn message_write_ledger_limit(pagination: Option<&flare_proto::common::Pagination>) -> i64 {
    pagination
        .map(|pagination| pagination.limit)
        .unwrap_or(100)
        .clamp(1, 500) as i64
}

fn message_write_ledger_offset(pagination: Option<&flare_proto::common::Pagination>) -> i64 {
    pagination
        .and_then(|pagination| pagination.cursor.trim().parse::<i64>().ok())
        .unwrap_or_default()
        .max(0)
}

fn optional_trimmed(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn optional_ledger_timestamp(
    value: i64,
    field: &str,
) -> Result<Option<chrono::DateTime<Utc>>, Status> {
    if value == 0 {
        return Ok(None);
    }
    Utc.timestamp_opt(value, 0)
        .single()
        .map(Some)
        .ok_or_else(|| Status::invalid_argument(format!("{field} is not a valid unix second")))
}

fn message_write_ledger_query(
    ctx: &flare_im_contracts::Ctx,
    req: QueryMessageWriteLedgerRequest,
) -> Result<MessageWriteLedgerQuery, Status> {
    let context_tenant_id = ctx
        .tenant_id()
        .map(str::trim)
        .filter(|tenant_id| !tenant_id.is_empty())
        .map(ToOwned::to_owned);
    let request_tenant_id = optional_trimmed(&req.tenant_id);
    let tenant_id = match (context_tenant_id, request_tenant_id) {
        (Some(context_tenant_id), Some(request_tenant_id))
            if context_tenant_id != request_tenant_id =>
        {
            return Err(Status::permission_denied(
                "tenant_id does not match authenticated context",
            ));
        }
        (Some(context_tenant_id), _) => context_tenant_id,
        (None, Some(request_tenant_id)) => request_tenant_id,
        (None, None) => return Err(Status::invalid_argument("tenant_id is required")),
    };

    let server_id = optional_trimmed(&req.server_id);
    let conversation_id = optional_trimmed(&req.conversation_id);
    let write_state = optional_trimmed(&req.write_state);
    let updated_after = optional_ledger_timestamp(req.updated_after, "updated_after")?;
    let updated_before = optional_ledger_timestamp(req.updated_before, "updated_before")?;
    if let (Some(after), Some(before)) = (updated_after.as_ref(), updated_before.as_ref())
        && before < after
    {
        return Err(Status::invalid_argument(
            "updated_before must be greater than or equal to updated_after",
        ));
    }

    if server_id.is_none()
        && conversation_id.is_none()
        && write_state.is_none()
        && !req.failed_only
        && updated_after.is_none()
        && updated_before.is_none()
    {
        return Err(Status::invalid_argument(
            "message write ledger query requires server_id, conversation_id, write_state, failed_only, or updated time range",
        ));
    }

    Ok(MessageWriteLedgerQuery {
        tenant_id,
        server_id,
        conversation_id,
        write_state,
        failed_only: req.failed_only,
        updated_after,
        updated_before,
        limit: message_write_ledger_limit(req.pagination.as_ref()),
        offset: message_write_ledger_offset(req.pagination.as_ref()),
    })
}

fn ledger_timestamp(value: chrono::DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

fn optional_ledger_timestamp_proto(
    value: Option<chrono::DateTime<Utc>>,
) -> Option<prost_types::Timestamp> {
    value.map(ledger_timestamp)
}

fn message_write_ledger_entry_to_proto(
    entry: DomainMessageWriteLedgerEntry,
) -> flare_grpc_proto::storage::MessageWriteLedgerEntry {
    flare_grpc_proto::storage::MessageWriteLedgerEntry {
        tenant_id: entry.tenant_id,
        server_id: entry.server_id,
        conversation_id: entry.conversation_id,
        seq: entry.seq,
        write_state: entry.write_state,
        archive_persisted_at: optional_ledger_timestamp_proto(entry.archive_persisted_at),
        storage_persisted_at: optional_ledger_timestamp_proto(entry.storage_persisted_at),
        wal_cleaned_at: optional_ledger_timestamp_proto(entry.wal_cleaned_at),
        ack_published_at: optional_ledger_timestamp_proto(entry.ack_published_at),
        failed_at: optional_ledger_timestamp_proto(entry.failed_at),
        last_error: entry.last_error.unwrap_or_default(),
        created_at: Some(ledger_timestamp(entry.created_at)),
        updated_at: Some(ledger_timestamp(entry.updated_at)),
    }
}

fn message_export_task_draft(
    ctx: &flare_im_contracts::Ctx,
    req: ExportMessagesRequest,
) -> Result<MessageExportTaskDraft, Status> {
    let tenant_id = ctx
        .tenant_id()
        .map(str::trim)
        .filter(|tenant_id| !tenant_id.is_empty())
        .ok_or_else(|| Status::invalid_argument("tenant_id is required"))?
        .to_string();
    let conversation_id = req.conversation_id.trim();
    if conversation_id.is_empty() {
        return Err(Status::invalid_argument("conversation_id is required"));
    }
    let time_range = req
        .time_range
        .ok_or_else(|| Status::invalid_argument("time_range is required"))?;
    let start_time = time_range
        .start_time
        .ok_or_else(|| Status::invalid_argument("time_range.start_time is required"))?;
    let end_time = time_range
        .end_time
        .ok_or_else(|| Status::invalid_argument("time_range.end_time is required"))?;
    if end_time <= start_time {
        return Err(Status::invalid_argument(
            "time_range.end_time must be greater than start_time",
        ));
    }

    let filters = serde_json::json!(
        req.filters
            .iter()
            .map(|filter| {
                serde_json::json!({
                    "field": filter.field,
                    "op": filter.op,
                    "values": filter.values,
                })
            })
            .collect::<Vec<_>>()
    );
    let request_id = ctx.request_id().to_string();
    let trace_id = ctx.trace_id().to_string();
    let task_id = stable_export_task_id(
        &tenant_id,
        conversation_id,
        start_time,
        end_time,
        &filters,
        &request_id,
    );

    Ok(MessageExportTaskDraft {
        task_id,
        tenant_id,
        conversation_id: conversation_id.to_string(),
        start_time,
        end_time,
        filters,
        requested_by: ctx.user_id().map(ToOwned::to_owned),
        request_id,
        trace_id,
    })
}

fn stable_export_task_id(
    tenant_id: &str,
    conversation_id: &str,
    start_time: i64,
    end_time: i64,
    filters: &serde_json::Value,
    request_id: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(tenant_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(conversation_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(start_time.to_be_bytes());
    hasher.update(end_time.to_be_bytes());
    hasher.update(b"\0");
    hasher.update(filters.to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(request_id.as_bytes());
    let digest = hasher.finalize();
    format!("export_{:x}", &digest)[..32].to_string()
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
            include_burned_placeholder: req.include_burned_placeholder,
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
        let requested_limit = req.limit.max(1);

        let query = QueryMessagesBySeqQuery {
            conversation_id: req.conversation_id,
            after_seq: req.after_seq,
            before_seq: if req.before_seq == 0 {
                None
            } else {
                Some(req.before_seq)
            },
            limit: requested_limit + 1,
            user_id: if req.user_id.is_empty() {
                None
            } else {
                Some(req.user_id)
            },
            include_burned_placeholder: req.include_burned_placeholder,
        };

        match self
            .query_handler
            .handle_query_messages_by_seq(&ctx, query)
            .await
        {
            Ok((messages, last_seq)) => {
                let mut messages = messages;
                let has_more = messages.len() as i32 > requested_limit;
                if has_more {
                    messages.truncate(requested_limit as usize);
                }
                let last_seq = messages
                    .last()
                    .and_then(|msg| extract_seq_from_message(&message_to_proto(msg)))
                    .or(last_seq)
                    .unwrap_or(0);

                // 转换消息为 proto 类型
                let proto_messages: Vec<flare_proto::Message> = messages
                    .into_iter()
                    .map(|msg| message_to_proto(&msg))
                    .collect();

                // 构建基于 seq 的游标
                let next_cursor = proto_messages
                    .last()
                    .and_then(|msg| {
                        msg.attributes
                            .get("seq")
                            .map(|seq_str| format!("seq:{}:{}", seq_str, msg.server_id))
                    })
                    .unwrap_or_default();

                Ok(Response::new(QueryMessagesBySeqResponse {
                    messages: proto_messages,
                    next_cursor: next_cursor.clone(),
                    has_more,
                    last_seq,
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
                .and_then(|ts| Utc.timestamp_millis_opt(*ts).single());
            let end = time_range
                .end_time
                .as_ref()
                .and_then(|ts| Utc.timestamp_millis_opt(*ts).single());
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
            Ok(tags) => Ok(Response::new(ListMessageTagsResponse { tags })),
            Err(err) => {
                error!(error = ?err, "Failed to list message tags");
                Ok(Response::new(ListMessageTagsResponse { tags: vec![] }))
            }
        }
    }

    async fn export_messages(
        &self,
        request: Request<ExportMessagesRequest>,
    ) -> Result<Response<ExportMessagesResponse>, Status> {
        let ctx = extract_ctx_from_request_opt(&request)
            .unwrap_or_else(|| Arc::new(flare_server_core::context::Context::root()));
        let draft = message_export_task_draft(&ctx, request.into_inner())?;

        match self
            .query_handler
            .storage()
            .create_message_export_task(&ctx, draft)
            .await
        {
            Ok(export_task_id) => Ok(Response::new(ExportMessagesResponse { export_task_id })),
            Err(err) => {
                error!(error = ?err, "create message export task failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    async fn query_message_events(
        &self,
        request: Request<QueryMessageEventsRequest>,
    ) -> Result<Response<QueryMessageEventsResponse>, Status> {
        let ctx = extract_ctx_from_request_opt(&request)
            .unwrap_or_else(|| Arc::new(flare_server_core::context::Context::root()));
        let req = request.into_inner();
        if req.message_id.trim().is_empty() {
            return Err(Status::invalid_argument("message_id is required"));
        }

        let limit = message_event_limit(req.pagination.as_ref());
        let offset = message_event_offset(req.pagination.as_ref());
        let event_type_filter = message_event_type_filter(&req.event_types)?;
        let event_type_filter = event_type_filter.as_deref();

        match self
            .query_handler
            .storage()
            .query_message_events(&ctx, &req.message_id, event_type_filter, limit, offset)
            .await
        {
            Ok((events, has_more)) => {
                let event_count = events.len() as i64;
                let next_cursor = if has_more {
                    (offset + event_count).to_string()
                } else {
                    String::new()
                };
                let previous_cursor = req
                    .pagination
                    .as_ref()
                    .map(|pagination| pagination.cursor.clone())
                    .unwrap_or_default();
                Ok(Response::new(QueryMessageEventsResponse {
                    events: events.iter().map(event_to_proto_or_default).collect(),
                    pagination: Some(flare_proto::common::Pagination {
                        cursor: next_cursor,
                        limit,
                        has_more,
                        previous_cursor,
                        total_size: 0,
                    }),
                }))
            }
            Err(err) => {
                error!(error = ?err, message_id = %req.message_id, "query_message_events failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    async fn query_message_write_ledger(
        &self,
        request: Request<QueryMessageWriteLedgerRequest>,
    ) -> Result<Response<QueryMessageWriteLedgerResponse>, Status> {
        let ctx = extract_ctx_from_request_opt(&request)
            .unwrap_or_else(|| Arc::new(flare_server_core::context::Context::root()));
        let req = request.into_inner();
        let offset = message_write_ledger_offset(req.pagination.as_ref());
        let query = message_write_ledger_query(&ctx, req)?;

        match self
            .query_handler
            .handle_query_message_write_ledger(&ctx, query.clone())
            .await
        {
            Ok((entries, has_more)) => {
                let entry_count = entries.len() as i64;
                let next_cursor = if has_more {
                    (offset + entry_count).to_string()
                } else {
                    String::new()
                };
                Ok(Response::new(QueryMessageWriteLedgerResponse {
                    entries: entries
                        .into_iter()
                        .map(message_write_ledger_entry_to_proto)
                        .collect(),
                    pagination: Some(flare_proto::common::Pagination {
                        cursor: next_cursor,
                        limit: query.limit as i32,
                        has_more,
                        previous_cursor: offset.to_string(),
                        total_size: entry_count,
                    }),
                }))
            }
            Err(err) => {
                error!(error = ?err, tenant_id = %query.tenant_id, "query_message_write_ledger failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    async fn query_message_edit_history(
        &self,
        request: Request<QueryMessageEditHistoryRequest>,
    ) -> Result<Response<QueryMessageEditHistoryResponse>, Status> {
        let ctx = extract_ctx_from_request_opt(&request)
            .unwrap_or_else(|| Arc::new(flare_server_core::context::Context::root()));
        let req = request.into_inner();
        if req.message_id.trim().is_empty() {
            return Err(Status::invalid_argument("message_id is required"));
        }
        let limit = message_event_limit(req.pagination.as_ref());
        let offset = message_event_offset(req.pagination.as_ref());

        match self
            .query_handler
            .storage()
            .query_message_edit_history(&ctx, &req.message_id)
            .await
        {
            Ok(items) => {
                let total_size = items.len() as i64;
                let page: Vec<_> = items
                    .into_iter()
                    .skip(offset as usize)
                    .take(limit as usize)
                    .collect();
                let has_more = offset + (page.len() as i64) < total_size;
                let next_cursor = if has_more {
                    (offset + page.len() as i64).to_string()
                } else {
                    String::new()
                };
                Ok(Response::new(QueryMessageEditHistoryResponse {
                    edit_history: page.iter().map(edit_history_entry_to_proto).collect(),
                    pagination: Some(flare_proto::common::Pagination {
                        cursor: next_cursor,
                        limit,
                        has_more,
                        previous_cursor: offset.to_string(),
                        total_size,
                    }),
                }))
            }
            Err(err) => {
                error!(error = ?err, message_id = %req.message_id, "query_message_edit_history failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    async fn query_message_read_list(
        &self,
        request: Request<QueryMessageReadListRequest>,
    ) -> Result<Response<QueryMessageReadListResponse>, Status> {
        let ctx = extract_ctx_from_request_opt(&request)
            .unwrap_or_else(|| Arc::new(flare_server_core::context::Context::root()));
        let req = request.into_inner();
        if req.message_id.trim().is_empty() {
            return Err(Status::invalid_argument("message_id is required"));
        }
        let limit = message_event_limit(req.pagination.as_ref());
        let offset = message_event_offset(req.pagination.as_ref());

        match self
            .query_handler
            .storage()
            .query_message_read_records(&ctx, &req.message_id)
            .await
        {
            Ok(items) => {
                let total_size = items.len() as i64;
                let page: Vec<_> = items
                    .into_iter()
                    .skip(offset as usize)
                    .take(limit as usize)
                    .collect();
                let has_more = offset + (page.len() as i64) < total_size;
                let next_cursor = if has_more {
                    (offset + page.len() as i64).to_string()
                } else {
                    String::new()
                };
                Ok(Response::new(QueryMessageReadListResponse {
                    read_list: page.iter().map(read_list_entry_to_proto).collect(),
                    pagination: Some(flare_proto::common::Pagination {
                        cursor: next_cursor,
                        limit,
                        has_more,
                        previous_cursor: offset.to_string(),
                        total_size,
                    }),
                }))
            }
            Err(err) => {
                error!(error = ?err, message_id = %req.message_id, "query_message_read_list failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    async fn query_message_marks(
        &self,
        request: Request<QueryMessageMarksRequest>,
    ) -> Result<Response<QueryMessageMarksResponse>, Status> {
        let ctx = extract_ctx_from_request_opt(&request)
            .unwrap_or_else(|| Arc::new(flare_server_core::context::Context::root()));
        let req = request.into_inner();
        if req.message_id.trim().is_empty() {
            return Err(Status::invalid_argument("message_id is required"));
        }
        let limit = message_event_limit(req.pagination.as_ref());
        let offset = message_event_offset(req.pagination.as_ref());

        match self
            .query_handler
            .storage()
            .query_message_marks(&ctx, &req.message_id)
            .await
        {
            Ok(items) => {
                let total_size = items.len() as i64;
                let page: Vec<_> = items
                    .into_iter()
                    .skip(offset as usize)
                    .take(limit as usize)
                    .collect();
                let has_more = offset + (page.len() as i64) < total_size;
                let next_cursor = if has_more {
                    (offset + page.len() as i64).to_string()
                } else {
                    String::new()
                };
                Ok(Response::new(QueryMessageMarksResponse {
                    marks: page.iter().map(mark_entry_to_proto).collect(),
                    pagination: Some(flare_proto::common::Pagination {
                        cursor: next_cursor,
                        limit,
                        has_more,
                        previous_cursor: offset.to_string(),
                        total_size,
                    }),
                }))
            }
            Err(err) => {
                error!(error = ?err, message_id = %req.message_id, "query_message_marks failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }

    async fn query_message_reactions(
        &self,
        request: Request<QueryMessageReactionsRequest>,
    ) -> Result<Response<QueryMessageReactionsResponse>, Status> {
        let ctx = extract_ctx_from_request_opt(&request)
            .unwrap_or_else(|| Arc::new(flare_server_core::context::Context::root()));
        let req = request.into_inner();
        if req.message_id.trim().is_empty() {
            return Err(Status::invalid_argument("message_id is required"));
        }

        match self
            .query_handler
            .storage()
            .query_message_reactions(&ctx, &req.message_id)
            .await
        {
            Ok(items) => {
                let reactions = items.iter().map(reaction_item_to_proto).collect();
                Ok(Response::new(QueryMessageReactionsResponse {
                    reactions,
                    pagination: req.pagination,
                }))
            }
            Err(err) => {
                error!(error = ?err, message_id = %req.message_id, "query_message_reactions failed");
                Err(Status::internal(err.to_string()))
            }
        }
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
                }))
            }
            Ok(None) => Ok(Response::new(GetConversationMessageHeadResponse {
                max_seq: 0,
                last_message_id: String::new(),
                last_timestamp: None,
            })),
            Err(err) => {
                error!(error = ?err, "get_conversation_message_head failed");
                Err(Status::internal(err.to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_server_core::Context;
    use std::sync::Arc;

    #[test]
    fn message_event_pagination_is_bounded_and_cursor_is_offset() {
        let pagination = flare_proto::common::Pagination {
            cursor: "40".to_string(),
            limit: 5_000,
            has_more: false,
            previous_cursor: String::new(),
            total_size: 0,
        };

        assert_eq!(message_event_limit(Some(&pagination)), 500);
        assert_eq!(message_event_offset(Some(&pagination)), 40);
    }

    #[test]
    fn message_event_type_filter_rejects_unknown_values() {
        let status = message_event_type_filter(&[999]).expect_err("unknown event type");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn message_export_task_draft_rejects_unbounded_request() {
        let ctx = Arc::new(Context::with_request_id("req-export").with_tenant_id("tenant-a"));

        let status = message_export_task_draft(&ctx, ExportMessagesRequest::default())
            .expect_err("bounded export request is required");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn message_export_task_draft_uses_stable_request_identity() {
        let ctx = Arc::new(Context::with_request_id("req-export").with_tenant_id("tenant-a"));
        let request = ExportMessagesRequest {
            conversation_id: "conv-a".to_string(),
            time_range: Some(flare_proto::common::TimeRange {
                start_time: Some(1_700_000_000),
                end_time: Some(1_700_060_000),
            }),
            filters: Vec::new(),
        };

        let first = message_export_task_draft(&ctx, request.clone()).expect("first draft");
        let second = message_export_task_draft(&ctx, request).expect("second draft");

        assert_eq!(first.task_id, second.task_id);
        assert!(first.task_id.starts_with("export_"));
        assert_eq!(first.tenant_id, "tenant-a");
        assert_eq!(first.conversation_id, "conv-a");
    }

    #[test]
    fn message_write_ledger_query_rejects_unbounded_scan() {
        let ctx = Arc::new(Context::with_request_id("req-ledger").with_tenant_id("tenant-a"));

        let status = message_write_ledger_query(&ctx, QueryMessageWriteLedgerRequest::default())
            .expect_err("bounded query is required");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn message_write_ledger_query_uses_context_tenant_and_caps_page() {
        let ctx = Arc::new(Context::with_request_id("req-ledger").with_tenant_id("tenant-a"));
        let request = QueryMessageWriteLedgerRequest {
            conversation_id: " conv-a ".to_string(),
            updated_after: 1_700_000_000,
            pagination: Some(flare_proto::common::Pagination {
                cursor: "40".to_string(),
                limit: 5_000,
                has_more: false,
                previous_cursor: String::new(),
                total_size: 0,
            }),
            ..Default::default()
        };

        let query = message_write_ledger_query(&ctx, request).expect("ledger query");

        assert_eq!(query.tenant_id, "tenant-a");
        assert_eq!(query.conversation_id.as_deref(), Some("conv-a"));
        assert_eq!(query.limit, 500);
        assert_eq!(query.offset, 40);
        assert!(query.updated_after.is_some());
    }

    #[test]
    fn message_write_ledger_query_rejects_tenant_override() {
        let ctx = Arc::new(Context::with_request_id("req-ledger").with_tenant_id("tenant-a"));
        let request = QueryMessageWriteLedgerRequest {
            tenant_id: "tenant-b".to_string(),
            server_id: "msg-a".to_string(),
            ..Default::default()
        };

        let status = message_write_ledger_query(&ctx, request).expect_err("tenant mismatch");

        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }
}
