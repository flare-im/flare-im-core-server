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
    filter_expression_from_proto, mark_entry_to_proto, message_into_proto, message_to_proto,
    reaction_item_to_proto, read_list_entry_to_proto,
};
use crate::domain::model::{
    EventType, MessageExportTaskDraft, MessageWriteLedgerEntry as DomainMessageWriteLedgerEntry,
    MessageWriteLedgerQuery,
};
use crate::domain::repository::MessageStorage;
use flare_im_contracts::utils::extract_seq_from_message;
use flare_im_service_kit::staff_context::{VerifiedStaff, require_staff};
use flare_server_core::SignedContextVerifier;
use flare_server_core::auth::core_admin::permission::{MESSAGES_EXPORT, MESSAGES_READ};
use flare_server_core::context::keys;
use flare_server_core::utils::extract_ctx_from_request_opt;
use flare_server_core::{SignedActorType, VerifiedContext};

#[derive(Clone)]
pub struct StorageReaderGrpcHandler<M>
where
    M: MessageStorage + Send + Sync + Clone + 'static,
{
    query_handler: Arc<MessageStorageQueryHandler<M>>,
    /// 验 admin-gateway 原样转发的 Staff `x-flare-ctx`（Cell 公钥集，Staff 钉死 `kid=gateway`）。
    /// 没有配置时管理检索一律 UNAUTHENTICATED。
    staff_verifier: Option<SignedContextVerifier>,
}

impl<M> StorageReaderGrpcHandler<M>
where
    M: MessageStorage + Send + Sync + Clone + 'static,
{
    pub async fn new(
        query_handler: Arc<MessageStorageQueryHandler<M>>,
        staff_verifier: Option<SignedContextVerifier>,
    ) -> flare_server_core::error::Result<Self> {
        Ok(Self {
            query_handler,
            staff_verifier,
        })
    }

    /// 管理检索（search、get、事件链、写入台账要求 `core:messages:read`，导出要求
    /// `core:messages:export`，只供 admin-gateway）的上下文：验过 admin-gateway 原样转发的 Staff
    /// `x-flare-ctx` 后，租户与操作人只取自签名，与其他 Core 服务的 admin 读一致。未签名的
    /// `x-tenant-id`、`x-user-id` 等元数据一概不认。
    fn admin_ctx<T>(
        &self,
        request: &Request<T>,
        permission: &str,
    ) -> Result<flare_server_core::context::Ctx, Status> {
        let verifier = self
            .staff_verifier
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("staff context verifier not configured"))?;
        let staff = require_staff(request, verifier, permission)?;
        Ok(admin_context(request.metadata(), &staff))
    }
}

/// 请求号与追踪号的长度上限：更长的值不用（请求号另生成），只为关联日志，不是身份。
const MAX_CORRELATION_ID_BYTES: usize = 128;

/// 由已验证的 Staff 上下文建上下文：租户与操作人来自签名；请求号与追踪号先取签名里的，再取
/// 元数据（导出任务按请求号去重，都没有时新生成一个）。
fn admin_context(
    metadata: &tonic::metadata::MetadataMap,
    staff: &VerifiedStaff,
) -> flare_server_core::context::Ctx {
    let correlation = |key: &str| {
        metadata
            .get(key)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty() && value.len() <= MAX_CORRELATION_ID_BYTES)
            .map(str::to_string)
    };
    let request_id = Some(staff.request_id.clone())
        .filter(|id| !id.is_empty())
        .or_else(|| correlation(keys::REQUEST_ID))
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let ctx = flare_server_core::context::Context::with_request_id(request_id)
        .with_tenant_id(&staff.tenant_id)
        .with_user_id(&staff.actor_id);
    Arc::new(
        match staff
            .trace_id
            .clone()
            .or_else(|| correlation(keys::TRACE_ID))
        {
            Some(trace_id) => ctx.with_trace_id(trace_id),
            None => ctx,
        },
    )
}

/// 按 seq 读与批量窗口里请求体的 `user_id` 是可见性下限（入群前的、为自己删除的消息不返回），
/// 不是谁在读。调用方是验过签的 User 时它只能是签名用户本人：填了别人回 PERMISSION_DENIED（以前
/// 同租户内可以指名他人、按别人的下限读），留空按签名用户补上（留空等于不加下限）。Service、
/// TenantAdmin 签名的调用（替用户读的服务）与读接口边界放行的未签名调用仍按请求体。
fn bound_read_user(
    ctx: &flare_server_core::context::Ctx,
    requested: String,
) -> Result<Option<String>, Status> {
    let signed_user = ctx
        .get_data::<VerifiedContext>()
        .and_then(VerifiedContext::claims)
        .filter(|claims| claims.actor_type == SignedActorType::User)
        .map(|claims| claims.actor_id.as_str());
    match signed_user {
        None => Ok((!requested.is_empty()).then_some(requested)),
        Some(user) if requested.is_empty() || requested == user => Ok(Some(user.to_string())),
        Some(_) => Err(Status::permission_denied(
            "user_id does not match the signed user",
        )),
    }
}

/// 失败的是哪一类读：按 id 取一条消息（或某条消息的事件、编辑历史、已读、标记、反应）时，
/// 「不存在」是对调用方有意义的答复；列表、检索、窗口、导出、账本、会话事件与会话头没有
/// 「不存在」——没有数据就是空结果，存储层在这些读上报不存在只能是内部不一致。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageRead {
    Lookup,
    Listing,
}

/// 存储失败 → gRPC 状态，只按错误码分类：数据库或缓存连不上回可重试的 UNAVAILABLE，超时回
/// DEADLINE_EXCEEDED，参数错误、无权限、不支持各回对应的码；不存在在按 id 取一条时回
/// NOT_FOUND，在列表类读上是 INTERNAL（见 [`StorageRead`]）；其余（数据库错误、解码失败、未分类的
/// 系统错误）是 INTERNAL。状态里只有固定说明：错误的原因、details 与系统错误的文本里可能有数据库
/// 报文、SQL 片段或请求里的取值，只进调用处的错误日志。
fn storage_failure(err: &flare_server_core::error::FlareError, read: StorageRead) -> Status {
    use flare_server_core::error::ErrorCode;
    match err.code() {
        Some(
            ErrorCode::ServiceUnavailable
            | ErrorCode::NetworkError
            | ErrorCode::NetworkUnreachable
            | ErrorCode::NetworkConnectionLost
            | ErrorCode::ConnectionFailed
            | ErrorCode::ConnectionRefused
            | ErrorCode::ConnectionClosed
            | ErrorCode::NotConnected,
        ) => Status::unavailable("message storage is temporarily unavailable"),
        Some(
            ErrorCode::OperationTimeout | ErrorCode::NetworkTimeout | ErrorCode::ConnectionTimeout,
        ) => Status::deadline_exceeded("message storage did not answer in time"),
        Some(ErrorCode::InvalidParameter | ErrorCode::BadRequest) => {
            Status::invalid_argument("message storage refused the request arguments")
        }
        Some(ErrorCode::MessageNotFound | ErrorCode::UserNotFound)
            if read == StorageRead::Lookup =>
        {
            Status::not_found("not found in message storage")
        }
        Some(ErrorCode::PermissionDenied) => {
            Status::permission_denied("message storage refused the tenant scope")
        }
        Some(ErrorCode::OperationNotSupported) => {
            Status::unimplemented("not supported by this message storage backend")
        }
        _ => Status::internal("message storage failed"),
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
                Err(storage_failure(&err, StorageRead::Listing))
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
        let user_id = bound_read_user(&ctx, req.user_id)?;
        let requested_limit = req.limit.max(1);
        let is_backfill_tail_page = crate::domain::repository::is_backfill_tail_page(
            req.after_seq,
            (req.before_seq != 0).then_some(req.before_seq),
        );

        let query = QueryMessagesBySeqQuery {
            conversation_id: req.conversation_id,
            after_seq: req.after_seq,
            before_seq: if req.before_seq == 0 {
                None
            } else {
                Some(req.before_seq)
            },
            limit: requested_limit + 1,
            user_id,
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
                    let requested_limit = requested_limit as usize;
                    if is_backfill_tail_page {
                        let overflow = messages.len().saturating_sub(requested_limit);
                        messages.drain(0..overflow);
                    } else {
                        messages.truncate(requested_limit);
                    }
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
                Err(storage_failure(&err, StorageRead::Listing))
            }
        }
    }

    async fn query_conversations_message_windows(
        &self,
        request: Request<QueryConversationsMessageWindowsRequest>,
    ) -> Result<Response<QueryConversationsMessageWindowsResponse>, Status> {
        let ctx = extract_ctx_from_request_opt(&request)
            .unwrap_or_else(|| Arc::new(flare_server_core::context::Context::root()));
        let req = request.into_inner();
        let user_id = bound_read_user(&ctx, req.user_id)?;
        let targets: Vec<(String, i64)> = req
            .targets
            .iter()
            .map(|t| (t.conversation_id.clone(), t.after_seq))
            .collect();

        match self
            .query_handler
            .handle_query_conversations_message_windows(
                &ctx,
                &targets,
                user_id.as_deref(),
                req.per_conversation_limit,
                req.newest_window,
                req.include_burned_placeholder,
            )
            .await
        {
            Ok(windows) => Ok(Response::new(QueryConversationsMessageWindowsResponse {
                windows: windows
                    .into_iter()
                    .map(|(conversation_id, messages)| {
                        // 按值转换：move 内容字节/字符串，整页免深拷贝。
                        let proto_messages: Vec<flare_proto::Message> =
                            messages.into_iter().map(message_into_proto).collect();
                        let last_seq = proto_messages
                            .last()
                            .and_then(extract_seq_from_message)
                            .unwrap_or(0);
                        ConversationMessageWindow {
                            conversation_id,
                            messages: proto_messages,
                            last_seq,
                        }
                    })
                    .collect(),
            })),
            Err(err) => {
                error!(error = ?err, "Failed to query conversations message windows");
                Err(storage_failure(&err, StorageRead::Listing))
            }
        }
    }

    async fn search_messages(
        &self,
        request: Request<SearchMessagesRequest>,
    ) -> Result<Response<SearchMessagesResponse>, Status> {
        let ctx = self.admin_ctx(&request, MESSAGES_READ)?;

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
                Err(storage_failure(&err, StorageRead::Listing))
            }
        }
    }

    async fn get_message(
        &self,
        request: Request<GetMessageRequest>,
    ) -> Result<Response<GetMessageResponse>, Status> {
        let ctx = self.admin_ctx(&request, MESSAGES_READ)?;

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
                Err(storage_failure(&err, StorageRead::Lookup))
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
            // 以前读失败时回空列表，调用方分不清「没有标签」与「存储故障」。
            Err(err) => {
                error!(error = ?err, "Failed to list message tags");
                Err(storage_failure(&err, StorageRead::Listing))
            }
        }
    }

    async fn export_messages(
        &self,
        request: Request<ExportMessagesRequest>,
    ) -> Result<Response<ExportMessagesResponse>, Status> {
        let ctx = self.admin_ctx(&request, MESSAGES_EXPORT)?;
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
                Err(storage_failure(&err, StorageRead::Listing))
            }
        }
    }

    async fn query_message_events(
        &self,
        request: Request<QueryMessageEventsRequest>,
    ) -> Result<Response<QueryMessageEventsResponse>, Status> {
        let ctx = self.admin_ctx(&request, MESSAGES_READ)?;
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
                Err(storage_failure(&err, StorageRead::Lookup))
            }
        }
    }

    async fn query_message_write_ledger(
        &self,
        request: Request<QueryMessageWriteLedgerRequest>,
    ) -> Result<Response<QueryMessageWriteLedgerResponse>, Status> {
        let ctx = self.admin_ctx(&request, MESSAGES_READ)?;
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
                Err(storage_failure(&err, StorageRead::Listing))
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
                Err(storage_failure(&err, StorageRead::Lookup))
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
                Err(storage_failure(&err, StorageRead::Lookup))
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
                Err(storage_failure(&err, StorageRead::Lookup))
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
                Err(storage_failure(&err, StorageRead::Lookup))
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
                Err(storage_failure(&err, StorageRead::Listing))
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
                Err(storage_failure(&err, StorageRead::Listing))
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

    // ---- 管理检索的验签与数据库不可用 ----

    use crate::infrastructure::persistence::optimized_postgres_store::OptimizedPostgresMessageStorageImpl;
    use crate::infrastructure::persistence::postgres_base::PostgresBaseStorage;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_im_service_kit::control_auth::{CORE_PROJECTION_AUDIENCE, PLATFORM_ISSUER};
    use flare_server_core::auth::{KeySet, kid};
    use flare_server_core::{SignedContextIdentity, SignedContextSigner};

    const GATEWAY_KEY: [u8; 32] = [47; 32];
    const HOOK_KEY: [u8; 32] = [48; 32];

    fn pem(seed: [u8; 32]) -> (String, String) {
        let key = SigningKey::from_bytes(&seed);
        (
            key.to_pkcs8_pem(Default::default()).unwrap().to_string(),
            key.verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap(),
        )
    }

    /// 网关钥与 hook 钥的公钥集，Staff 钉死 gateway。
    fn staff_verifier() -> SignedContextVerifier {
        let mut keys = KeySet::empty();
        keys.insert(kid::GATEWAY, &pem(GATEWAY_KEY).1).unwrap();
        keys.insert(kid::HOOK, &pem(HOOK_KEY).1).unwrap();
        SignedContextVerifier::from_key_set(keys).require_staff_kid([kid::GATEWAY])
    }

    fn staff_token(seed: [u8; 32], key_id: &str, tenant: &str, permission: &str) -> String {
        SignedContextSigner::from_private_key(&pem(seed).0)
            .unwrap()
            .with_kid(key_id)
            .unwrap()
            .sign(
                &SignedContextIdentity::new(
                    SignedActorType::Staff,
                    "employee-1",
                    tenant,
                    "req-staff",
                )
                .with_staff_grant(Some("org-1".into()), Some(permission.into()), 2)
                .with_trace_id("trace-staff"),
            )
            .unwrap()
    }

    /// 连一个不监听的回环端口：取连接立即失败，模拟数据库不可达（不需要真库）。
    fn unreachable_store_handler(
        verifier: Option<SignedContextVerifier>,
    ) -> StorageReaderGrpcHandler<OptimizedPostgresMessageStorageImpl> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_millis(500))
            .connect_lazy("postgres://reader@127.0.0.1:1/flare")
            .unwrap();
        let storage = Arc::new(OptimizedPostgresMessageStorageImpl::new(
            PostgresBaseStorage { pool, cache: None },
            None,
            None,
        ));
        StorageReaderGrpcHandler {
            query_handler: Arc::new(MessageStorageQueryHandler::new(storage)),
            staff_verifier: verifier,
        }
    }

    fn with_metadata<T>(payload: T, entries: &[(&'static str, &str)]) -> Request<T> {
        let mut request = Request::new(payload);
        for (key, value) in entries {
            request.metadata_mut().append(*key, value.parse().unwrap());
        }
        request
    }

    async fn codes_of(
        handler: &StorageReaderGrpcHandler<OptimizedPostgresMessageStorageImpl>,
        entries: &[(&'static str, &str)],
    ) -> [tonic::Code; 5] {
        [
            handler
                .search_messages(with_metadata(SearchMessagesRequest::default(), entries))
                .await
                .unwrap_err()
                .code(),
            handler
                .get_message(with_metadata(
                    GetMessageRequest {
                        message_id: "m1".into(),
                    },
                    entries,
                ))
                .await
                .unwrap_err()
                .code(),
            handler
                .query_message_events(with_metadata(
                    QueryMessageEventsRequest {
                        message_id: "m1".into(),
                        ..Default::default()
                    },
                    entries,
                ))
                .await
                .unwrap_err()
                .code(),
            handler
                .query_message_write_ledger(with_metadata(
                    QueryMessageWriteLedgerRequest {
                        conversation_id: "c1".into(),
                        ..Default::default()
                    },
                    entries,
                ))
                .await
                .unwrap_err()
                .code(),
            handler
                .export_messages(with_metadata(
                    ExportMessagesRequest {
                        conversation_id: "c1".into(),
                        ..Default::default()
                    },
                    entries,
                ))
                .await
                .unwrap_err()
                .code(),
        ]
    }

    /// 管理检索只认 admin-gateway 转发的 Staff 上下文：没有上下文、平台断言（旧 admin 受众或
    /// control 受众）、hook 钥签的 Staff 上下文都是 UNAUTHENTICATED，没配 Cell 公钥集时同样拒绝；
    /// 只带 `x-tenant-id` / `x-user-id` 元数据的调用不再能读任何租户。
    #[tokio::test]
    async fn admin_retrieval_requires_the_forwarded_staff_context() {
        let handler = unreachable_store_handler(Some(staff_verifier()));
        let unsigned = [("x-tenant-id", "tenant-a"), ("x-user-id", "employee-1")];
        let platform = flare_server_core::TokenService::from_ed25519_private_key(
            &pem([49; 32]).0,
            PLATFORM_ISSUER,
            60,
        )
        .unwrap();
        let admin = format!(
            "Bearer {}",
            platform
                .generate_with_ed25519("employee-1", Some("tenant-a"), "admin", "flare-core-admin")
                .unwrap()
        );
        let control = format!(
            "Bearer {}",
            platform
                .generate_with_ed25519(
                    "employee-1",
                    Some("tenant-a"),
                    "control",
                    CORE_PROJECTION_AUDIENCE
                )
                .unwrap()
        );
        let forged = staff_token(HOOK_KEY, kid::HOOK, "tenant-a", MESSAGES_READ);
        let mut refused: Vec<Vec<(&'static str, &str)>> = vec![unsigned.to_vec()];
        for bearer in [admin.as_str(), control.as_str()] {
            let mut entries = unsigned.to_vec();
            entries.push(("authorization", bearer));
            refused.push(entries);
        }
        refused.push(vec![("x-flare-ctx", forged.as_str())]);
        for entries in &refused {
            assert_eq!(
                codes_of(&handler, entries).await,
                [tonic::Code::Unauthenticated; 5],
                "{entries:?}"
            );
        }

        // 没配 Cell 公钥集：有效上下文也拒绝。
        let valid = staff_token(GATEWAY_KEY, kid::GATEWAY, "tenant-a", MESSAGES_READ);
        let status = unreachable_store_handler(None)
            .get_message(with_metadata(
                GetMessageRequest {
                    message_id: "m1".into(),
                },
                &[("x-flare-ctx", valid.as_str())],
            ))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    /// 权限码按 RPC 族复核：四条检索要 `core:messages:read`，导出要 `core:messages:export`；
    /// 别的码一律 PERMISSION_DENIED。通过鉴权的检索走到存储，连不上的库回 UNAVAILABLE；通过鉴权
    /// 的导出先过请求校验（这里的草稿缺时间范围，INVALID_ARGUMENT）。
    #[tokio::test]
    async fn admin_retrieval_checks_the_permission_code_per_rpc_family() {
        let handler = unreachable_store_handler(Some(staff_verifier()));
        let read = staff_token(GATEWAY_KEY, kid::GATEWAY, "tenant-a", MESSAGES_READ);
        let export = staff_token(GATEWAY_KEY, kid::GATEWAY, "tenant-a", MESSAGES_EXPORT);
        let other = staff_token(
            GATEWAY_KEY,
            kid::GATEWAY,
            "tenant-a",
            "core:conversations:read",
        );
        let denied = tonic::Code::PermissionDenied;
        let reached = tonic::Code::Unavailable;
        assert_eq!(
            codes_of(&handler, &[("x-flare-ctx", read.as_str())]).await,
            [reached, reached, reached, reached, denied]
        );
        assert_eq!(
            codes_of(&handler, &[("x-flare-ctx", export.as_str())]).await,
            [denied, denied, denied, denied, tonic::Code::InvalidArgument]
        );
        assert_eq!(
            codes_of(&handler, &[("x-flare-ctx", other.as_str())]).await,
            [denied; 5]
        );
    }

    /// 租户只取自签名：台账查询指名别的租户回 PERMISSION_DENIED，`x-tenant-id` 元数据改不了
    /// 签名租户；请求号与追踪号先取签名里的。
    #[tokio::test]
    async fn the_admin_tenant_comes_from_the_signed_context_only() {
        let handler = unreachable_store_handler(Some(staff_verifier()));
        let read = staff_token(GATEWAY_KEY, kid::GATEWAY, "tenant-a", MESSAGES_READ);
        let status = handler
            .query_message_write_ledger(with_metadata(
                QueryMessageWriteLedgerRequest {
                    tenant_id: "tenant-b".into(),
                    conversation_id: "c1".into(),
                    ..Default::default()
                },
                &[("x-flare-ctx", read.as_str()), ("x-tenant-id", "tenant-b")],
            ))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);

        let staff = flare_im_service_kit::staff_context::require_staff(
            &with_metadata((), &[("x-flare-ctx", read.as_str())]),
            &staff_verifier(),
            MESSAGES_READ,
        )
        .unwrap();
        let metadata = with_metadata(
            (),
            &[
                ("x-tenant-id", "tenant-b"),
                ("x-user-id", "intruder"),
                ("x-request-id", "req-7"),
                ("x-trace-id", "trace-7"),
            ],
        );
        let ctx = admin_context(metadata.metadata(), &staff);
        assert_eq!(ctx.tenant_id(), Some("tenant-a"));
        assert_eq!(ctx.user_id(), Some("employee-1"));
        // 签名里的请求号与追踪号优先于元数据。
        assert_eq!(ctx.request_id(), "req-staff");
        assert_eq!(ctx.trace_id(), "trace-staff");
    }

    /// 非 UNAVAILABLE 的存储失败也按错误码分类，状态里只有固定说明：数据库报文、SQL 片段、
    /// 请求里的取值与系统错误文本都不出现。修复前这些都是带 `err.to_string()` 原文的
    /// INTERNAL（系统错误的原文里就是数据库报文）。
    #[test]
    fn storage_failures_are_classified_and_never_carry_their_text() {
        use flare_server_core::error::{ErrorCode, FlareError, map_infra_error};
        use flare_server_core::flare_err;
        const RAW: &str = "relation \"messages\" leak-marker-77 at 10.0.0.7:5432";
        let cases: Vec<(FlareError, tonic::Code)> = vec![
            (
                FlareError::system(format!("Failed to query: {RAW}")),
                tonic::Code::Internal,
            ),
            (
                map_infra_error(RAW, ErrorCode::DatabaseError, format!("query {RAW}")),
                tonic::Code::Internal,
            ),
            (
                flare_err!(ErrorCode::DeserializationError, RAW),
                tonic::Code::Internal,
            ),
            (
                flare_err!(ErrorCode::InvalidParameter, format!("tenant {RAW}")),
                tonic::Code::InvalidArgument,
            ),
            (
                flare_err!(ErrorCode::BadRequest, RAW),
                tonic::Code::InvalidArgument,
            ),
            (
                flare_err!(ErrorCode::OperationTimeout, RAW),
                tonic::Code::DeadlineExceeded,
            ),
            (
                flare_err!(ErrorCode::MessageNotFound, RAW),
                tonic::Code::NotFound,
            ),
            (
                flare_err!(ErrorCode::PermissionDenied, RAW),
                tonic::Code::PermissionDenied,
            ),
            (
                flare_err!(ErrorCode::OperationNotSupported, RAW),
                tonic::Code::Unimplemented,
            ),
            (
                flare_err!(ErrorCode::ServiceUnavailable, RAW),
                tonic::Code::Unavailable,
            ),
            (
                flare_err!(ErrorCode::NetworkConnectionLost, RAW),
                tonic::Code::Unavailable,
            ),
        ];
        for (err, code) in cases {
            let status = storage_failure(&err, StorageRead::Lookup);
            assert_eq!(status.code(), code, "{err}");
            for leaked in ["leak-marker-77", "10.0.0.7", "relation", "系统错误"] {
                assert!(
                    !status.message().contains(leaked),
                    "{err} leaked {leaked}: {status:?}"
                );
            }
        }
    }

    /// 列表类读（列表、检索、窗口、导出、账本、会话事件与会话头）没有「不存在」：存储层报不存在
    /// 回 INTERNAL，而不是让调用方以为要找的东西不存在；其余分类与按 id 取一条相同。修复前所有读
    /// 都把 MessageNotFound 回 NOT_FOUND。
    #[test]
    fn not_found_is_only_an_answer_for_lookups() {
        use flare_server_core::error::ErrorCode;
        use flare_server_core::flare_err;
        let missing = flare_err!(ErrorCode::MessageNotFound, "message m1 at 10.0.0.7");
        assert_eq!(
            storage_failure(&missing, StorageRead::Lookup).code(),
            tonic::Code::NotFound
        );
        let status = storage_failure(&missing, StorageRead::Listing);
        assert_eq!(status.code(), tonic::Code::Internal, "{status:?}");
        assert!(!status.message().contains("10.0.0.7"), "{status:?}");
        for (code, expected) in [
            (ErrorCode::ServiceUnavailable, tonic::Code::Unavailable),
            (ErrorCode::OperationTimeout, tonic::Code::DeadlineExceeded),
            (ErrorCode::InvalidParameter, tonic::Code::InvalidArgument),
            (ErrorCode::PermissionDenied, tonic::Code::PermissionDenied),
            (ErrorCode::OperationNotSupported, tonic::Code::Unimplemented),
            (ErrorCode::DeserializationError, tonic::Code::Internal),
        ] {
            let err = flare_err!(code, "x");
            assert_eq!(
                storage_failure(&err, StorageRead::Listing).code(),
                expected,
                "{code:?}"
            );
        }
    }

    /// 读标签失败时回分好类的错误，而不是空列表（调用方分不清「没有标签」与「存储故障」）。
    #[tokio::test]
    async fn listing_tags_reports_storage_failures_instead_of_an_empty_list() {
        let handler = unreachable_store_handler(Some(staff_verifier()));
        let status = handler
            .list_message_tags(with_metadata(
                ListMessageTagsRequest::default(),
                &[("x-tenant-id", "tenant-a"), ("x-user-id", "u1")],
            ))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
        assert!(!status.message().contains("127.0.0.1"), "{status:?}");
    }

    /// 数据库不可达是可重试的 UNAVAILABLE（管理检索与其他读一样），且不带数据库错误文本；
    /// 修复前一律 INTERNAL，Admin Gateway 只能回 500。
    #[tokio::test]
    async fn an_unreachable_database_is_unavailable_not_internal() {
        let handler = unreachable_store_handler(Some(staff_verifier()));
        let staff = staff_token(GATEWAY_KEY, kid::GATEWAY, "tenant-a", MESSAGES_READ);
        let status = handler
            .get_message(with_metadata(
                GetMessageRequest {
                    message_id: "m1".into(),
                },
                &[("x-flare-ctx", staff.as_str())],
            ))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
        assert!(!status.message().contains("127.0.0.1"), "{status:?}");

        let status = handler
            .search_messages(with_metadata(
                SearchMessagesRequest::default(),
                &[("x-flare-ctx", staff.as_str())],
            ))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");

        let status = handler
            .query_message_write_ledger(with_metadata(
                QueryMessageWriteLedgerRequest {
                    conversation_id: "c1".into(),
                    updated_after: 1_700_000_000,
                    ..Default::default()
                },
                &[("x-flare-ctx", staff.as_str())],
            ))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");

        let status = handler
            .query_messages_by_seq(with_metadata(
                QueryMessagesBySeqRequest {
                    conversation_id: "c1".into(),
                    ..Default::default()
                },
                &[("x-tenant-id", "tenant-a"), ("x-user-id", "u1")],
            ))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
    }

    // ---- 按 seq 读与批量窗口：请求体 user_id 与签名用户绑定（W13-R2）----

    /// 经真实验签得到的上下文（带 `VerifiedContext`），与读接口边界放行后处理器拿到的相同。
    fn signed_ctx(actor_type: SignedActorType, actor: &str) -> flare_server_core::context::Ctx {
        let key = SigningKey::from_bytes(&[61; 32]);
        let signer = flare_server_core::SignedContextSigner::from_private_key(
            &key.to_pkcs8_pem(Default::default()).unwrap(),
        )
        .unwrap();
        let verifier = flare_server_core::SignedContextVerifier::from_public_key(
            &key.verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap(),
        )
        .unwrap();
        let token = signer
            .sign(&flare_server_core::SignedContextIdentity::new(
                actor_type, actor, "tenant-a", "req-bind",
            ))
            .unwrap();
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("x-flare-ctx", token.parse().unwrap());
        flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata(
            &metadata, &verifier,
        )
        .unwrap()
    }

    /// 签名的 User 只能按自己的可见性下限读：请求体填别人回 PERMISSION_DENIED，留空补成签名
    /// 用户；Service、TenantAdmin 签名与未签名的调用仍按请求体（留空即不加下限）。修复前请求体
    /// 原样生效，同租户的用户可以指名他人。
    #[test]
    fn signed_users_read_only_with_their_own_visibility_floor() {
        let alice = signed_ctx(SignedActorType::User, "alice");
        assert_eq!(
            bound_read_user(&alice, "alice".into()).unwrap().as_deref(),
            Some("alice")
        );
        assert_eq!(
            bound_read_user(&alice, String::new()).unwrap().as_deref(),
            Some("alice")
        );
        for other in ["bob", "alice ", "ALICE"] {
            let status = bound_read_user(&alice, other.into()).unwrap_err();
            assert_eq!(status.code(), tonic::Code::PermissionDenied, "{other:?}");
            assert!(!status.message().contains(other.trim()), "{status:?}");
        }

        for (actor_type, actor) in [
            (SignedActorType::Service, "flare-sync-orchestrator"),
            (SignedActorType::TenantAdmin, "admin-1"),
        ] {
            let ctx = signed_ctx(actor_type, actor);
            assert_eq!(
                bound_read_user(&ctx, "bob".into()).unwrap().as_deref(),
                Some("bob"),
                "{actor_type:?}"
            );
            assert_eq!(bound_read_user(&ctx, String::new()).unwrap(), None);
        }

        let unsigned: flare_server_core::context::Ctx = Arc::new(
            Context::with_request_id("req-legacy")
                .with_tenant_id("tenant-a")
                .with_user_id("alice"),
        );
        assert_eq!(
            bound_read_user(&unsigned, "bob".into()).unwrap().as_deref(),
            Some("bob")
        );
        assert_eq!(bound_read_user(&unsigned, String::new()).unwrap(), None);
    }
}
