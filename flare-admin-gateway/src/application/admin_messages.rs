use std::collections::HashMap;

use flare_grpc_proto::storage::{
    ExportMessagesRequest, ExportMessagesResponse, GetMessageRequest, GetMessageResponse,
    MessageWriteLedgerEntry as ProtoMessageWriteLedgerEntry, QueryMessageEventsRequest,
    QueryMessageEventsResponse, QueryMessageWriteLedgerRequest, QueryMessageWriteLedgerResponse,
    SearchMessagesRequest, SearchMessagesResponse,
};
use flare_proto::common::{
    Event, EventType as ProtoEventType, FilterExpression, FilterOperator, Message, Pagination,
    TimeRange, message_content,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

const DEFAULT_ADMIN_MESSAGE_QUERY_LIMIT: i32 = 100;
const MAX_ADMIN_MESSAGE_QUERY_LIMIT: i32 = 500;

#[derive(Debug, Clone, Default, Deserialize, Serialize, ToSchema)]
pub struct AdminMessageQueryHttpRequest {
    #[serde(default)]
    pub conversation_id: Option<String>,
    #[serde(default)]
    pub message_id: Option<String>,
    #[serde(default)]
    pub sender_id: Option<String>,
    #[serde(default)]
    pub channel_id: Option<String>,
    #[serde(default)]
    pub client_msg_id: Option<String>,
    #[serde(default)]
    pub message_type: Option<i32>,
    #[serde(default)]
    pub conversation_type: Option<i32>,
    #[serde(default)]
    pub source: Option<i32>,
    #[serde(default)]
    pub status: Option<i32>,
    #[serde(default)]
    pub is_recalled: Option<bool>,
    #[serde(default)]
    pub after_seq: Option<i64>,
    #[serde(default)]
    pub before_seq: Option<i64>,
    #[serde(default)]
    pub start_time: Option<i64>,
    #[serde(default)]
    pub end_time: Option<i64>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<i32>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminMessageQueryHttpResponse {
    pub messages: Vec<AdminMessageHttp>,
    pub has_more: bool,
    pub next_cursor: Option<String>,
    pub limit: i32,
    pub total_size: i64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, ToSchema)]
pub struct AdminMessageEventsQueryHttpRequest {
    #[serde(default)]
    pub event_types: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<i32>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, ToSchema)]
pub struct AdminMessageExportHttpRequest {
    pub conversation_id: String,
    #[serde(default)]
    pub sender_id: Option<String>,
    #[serde(default)]
    pub message_id: Option<String>,
    #[serde(default)]
    pub client_msg_id: Option<String>,
    #[serde(default)]
    pub channel_id: Option<String>,
    #[serde(default)]
    pub message_type: Option<i32>,
    #[serde(default)]
    pub source: Option<i32>,
    #[serde(default)]
    pub status: Option<i32>,
    #[serde(default)]
    pub is_recalled: Option<bool>,
    #[serde(default)]
    pub after_seq: Option<i64>,
    #[serde(default)]
    pub before_seq: Option<i64>,
    #[serde(default)]
    pub start_time: Option<i64>,
    #[serde(default)]
    pub end_time: Option<i64>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminMessageDetailHttpResponse {
    pub message: AdminMessageHttp,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminMessageEventsHttpResponse {
    pub events: Vec<AdminMessageEventHttp>,
    pub has_more: bool,
    pub next_cursor: Option<String>,
    pub limit: i32,
    pub total_size: i64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, ToSchema)]
pub struct AdminMessageWriteLedgerQueryHttpRequest {
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub server_id: Option<String>,
    #[serde(default)]
    pub conversation_id: Option<String>,
    #[serde(default)]
    pub write_state: Option<String>,
    #[serde(default)]
    pub failed_only: Option<bool>,
    #[serde(default)]
    pub updated_after: Option<i64>,
    #[serde(default)]
    pub updated_before: Option<i64>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<i32>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminMessageWriteLedgerHttpResponse {
    pub entries: Vec<AdminMessageWriteLedgerEntryHttp>,
    pub has_more: bool,
    pub next_cursor: Option<String>,
    pub limit: i32,
    pub total_size: i64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminMessageWriteLedgerEntryHttp {
    pub tenant_id: String,
    pub server_id: String,
    pub conversation_id: String,
    pub seq: i64,
    pub write_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archive_persisted_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_persisted_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_cleaned_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ack_published_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminMessageEventHttp {
    pub event_id: String,
    pub conversation_id: String,
    pub conversation_seq: u64,
    pub event_type: i32,
    pub created_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub payload_kind: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminMessageExportHttpResponse {
    pub export_task_id: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminMessageHttp {
    pub server_id: String,
    pub conversation_id: String,
    pub client_msg_id: String,
    pub sender_id: String,
    pub source: i32,
    pub conversation_seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_seq: Option<u64>,
    pub created_at: i64,
    pub conversation_type: i32,
    pub message_type: i32,
    pub status: i32,
    pub channel_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender_avatar: Option<String>,
    pub content_kind: String,
    pub attributes: HashMap<String, String>,
    pub extension_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminMessageQueryError {
    pub code: &'static str,
    pub message: String,
}

pub fn build_storage_search_request(
    query: &AdminMessageQueryHttpRequest,
) -> Result<SearchMessagesRequest, AdminMessageQueryError> {
    let filters = build_filters(query);
    if filters.is_empty() && query.start_time.is_none() && query.end_time.is_none() {
        return Err(AdminMessageQueryError {
            code: "ADMIN_MESSAGE_QUERY_FILTER_REQUIRED",
            message: "admin message query requires at least one indexed filter or time range"
                .to_string(),
        });
    }

    let limit = query_limit(query.limit);
    Ok(SearchMessagesRequest {
        filters,
        sort: Vec::new(),
        pagination: Some(Pagination {
            cursor: query.cursor.clone().unwrap_or_default(),
            limit,
            has_more: false,
            previous_cursor: String::new(),
            total_size: 0,
        }),
        time_range: Some(TimeRange {
            start_time: query.start_time,
            end_time: query.end_time,
        }),
    })
}

pub fn build_storage_get_message_request(
    message_id: &str,
) -> Result<GetMessageRequest, AdminMessageQueryError> {
    Ok(GetMessageRequest {
        message_id: required_trimmed_value(
            message_id,
            "ADMIN_MESSAGE_ID_REQUIRED",
            "message_id is required",
        )?,
    })
}

pub fn admin_message_query_response(
    response: SearchMessagesResponse,
    requested_limit: i32,
) -> AdminMessageQueryHttpResponse {
    let pagination = response.pagination;
    let has_more = pagination.as_ref().is_some_and(|value| value.has_more);
    let next_cursor = pagination
        .as_ref()
        .map(|value| value.cursor.trim().to_string())
        .filter(|value| !value.is_empty());
    let total_size = pagination
        .as_ref()
        .map(|value| value.total_size)
        .unwrap_or_default();

    AdminMessageQueryHttpResponse {
        messages: response
            .messages
            .into_iter()
            .map(AdminMessageHttp::from)
            .collect(),
        has_more,
        next_cursor,
        limit: query_limit(Some(requested_limit)),
        total_size,
    }
}

pub fn admin_message_detail_response(
    response: GetMessageResponse,
) -> Option<AdminMessageDetailHttpResponse> {
    response
        .message
        .map(|message| AdminMessageDetailHttpResponse {
            message: AdminMessageHttp::from(message),
        })
}

pub fn build_storage_message_events_request(
    message_id: &str,
    query: &AdminMessageEventsQueryHttpRequest,
) -> Result<QueryMessageEventsRequest, AdminMessageQueryError> {
    let limit = query_limit(query.limit);
    Ok(QueryMessageEventsRequest {
        message_id: required_trimmed_value(
            message_id,
            "ADMIN_MESSAGE_ID_REQUIRED",
            "message_id is required",
        )?,
        event_types: parse_event_types(query.event_types.as_deref())?,
        pagination: Some(Pagination {
            cursor: trimmed_optional(query.cursor.as_deref()).unwrap_or_default(),
            limit,
            has_more: false,
            previous_cursor: String::new(),
            total_size: 0,
        }),
    })
}

pub fn admin_message_events_response(
    response: QueryMessageEventsResponse,
    requested_limit: i32,
) -> AdminMessageEventsHttpResponse {
    let pagination = response.pagination;
    let has_more = pagination.as_ref().is_some_and(|value| value.has_more);
    let next_cursor = pagination
        .as_ref()
        .map(|value| value.cursor.trim().to_string())
        .filter(|value| !value.is_empty());
    let total_size = pagination
        .as_ref()
        .map(|value| value.total_size)
        .unwrap_or_default();

    AdminMessageEventsHttpResponse {
        events: response
            .events
            .into_iter()
            .map(AdminMessageEventHttp::from)
            .collect(),
        has_more,
        next_cursor,
        limit: query_limit(Some(requested_limit)),
        total_size,
    }
}

pub fn build_storage_write_ledger_request(
    query: &AdminMessageWriteLedgerQueryHttpRequest,
) -> Result<QueryMessageWriteLedgerRequest, AdminMessageQueryError> {
    let failed_only = query.failed_only.unwrap_or(false);
    if trimmed_optional(query.server_id.as_deref()).is_none()
        && trimmed_optional(query.conversation_id.as_deref()).is_none()
        && trimmed_optional(query.write_state.as_deref()).is_none()
        && !failed_only
        && query.updated_after.is_none()
        && query.updated_before.is_none()
    {
        return Err(AdminMessageQueryError {
            code: "ADMIN_MESSAGE_WRITE_LEDGER_FILTER_REQUIRED",
            message: "admin message write ledger query requires server_id, conversation_id, write_state, failed_only, or updated time range".to_string(),
        });
    }

    if let (Some(after), Some(before)) = (query.updated_after, query.updated_before)
        && before < after
    {
        return Err(AdminMessageQueryError {
            code: "ADMIN_MESSAGE_WRITE_LEDGER_TIME_RANGE_INVALID",
            message: "updated_before must be greater than or equal to updated_after".to_string(),
        });
    }

    let limit = query_limit(query.limit);
    Ok(QueryMessageWriteLedgerRequest {
        tenant_id: trimmed_optional(query.tenant_id.as_deref()).unwrap_or_default(),
        server_id: trimmed_optional(query.server_id.as_deref()).unwrap_or_default(),
        conversation_id: trimmed_optional(query.conversation_id.as_deref()).unwrap_or_default(),
        write_state: trimmed_optional(query.write_state.as_deref()).unwrap_or_default(),
        failed_only,
        updated_after: query.updated_after.unwrap_or_default(),
        updated_before: query.updated_before.unwrap_or_default(),
        pagination: Some(Pagination {
            cursor: trimmed_optional(query.cursor.as_deref()).unwrap_or_default(),
            limit,
            has_more: false,
            previous_cursor: String::new(),
            total_size: 0,
        }),
    })
}

pub fn admin_message_write_ledger_response(
    response: QueryMessageWriteLedgerResponse,
    requested_limit: i32,
) -> AdminMessageWriteLedgerHttpResponse {
    let pagination = response.pagination;
    let has_more = pagination.as_ref().is_some_and(|value| value.has_more);
    let next_cursor = pagination
        .as_ref()
        .map(|value| value.cursor.trim().to_string())
        .filter(|value| !value.is_empty());
    let total_size = pagination
        .as_ref()
        .map(|value| value.total_size)
        .unwrap_or_default();

    AdminMessageWriteLedgerHttpResponse {
        entries: response
            .entries
            .into_iter()
            .map(AdminMessageWriteLedgerEntryHttp::from)
            .collect(),
        has_more,
        next_cursor,
        limit: query_limit(Some(requested_limit)),
        total_size,
    }
}

pub fn build_storage_message_export_request(
    query: &AdminMessageExportHttpRequest,
) -> Result<ExportMessagesRequest, AdminMessageQueryError> {
    let conversation_id = required_trimmed_value(
        &query.conversation_id,
        "ADMIN_MESSAGE_EXPORT_CONVERSATION_REQUIRED",
        "conversation_id is required for admin message export",
    )?;
    let start_time = query.start_time.ok_or_else(|| AdminMessageQueryError {
        code: "ADMIN_MESSAGE_EXPORT_TIME_RANGE_REQUIRED",
        message: "start_time is required for bounded admin message export".to_string(),
    })?;
    let end_time = query.end_time.ok_or_else(|| AdminMessageQueryError {
        code: "ADMIN_MESSAGE_EXPORT_TIME_RANGE_REQUIRED",
        message: "end_time is required for bounded admin message export".to_string(),
    })?;
    if end_time <= start_time {
        return Err(AdminMessageQueryError {
            code: "ADMIN_MESSAGE_EXPORT_TIME_RANGE_INVALID",
            message: "end_time must be greater than start_time".to_string(),
        });
    }

    Ok(ExportMessagesRequest {
        conversation_id,
        time_range: Some(TimeRange {
            start_time: Some(start_time),
            end_time: Some(end_time),
        }),
        filters: build_export_filters(query),
    })
}

pub fn admin_message_export_response(
    response: ExportMessagesResponse,
) -> AdminMessageExportHttpResponse {
    AdminMessageExportHttpResponse {
        export_task_id: response.export_task_id,
    }
}

fn build_filters(query: &AdminMessageQueryHttpRequest) -> Vec<FilterExpression> {
    let mut filters = Vec::new();
    push_string_filter(
        &mut filters,
        "conversation_id",
        query.conversation_id.as_deref(),
    );
    push_string_filter(&mut filters, "message_id", query.message_id.as_deref());
    push_string_filter(&mut filters, "sender_id", query.sender_id.as_deref());
    push_string_filter(&mut filters, "channel_id", query.channel_id.as_deref());
    push_string_filter(
        &mut filters,
        "client_msg_id",
        query.client_msg_id.as_deref(),
    );
    push_i32_filter(&mut filters, "message_type", query.message_type);
    push_i32_filter(&mut filters, "conversation_type", query.conversation_type);
    push_i32_filter(&mut filters, "source", query.source);
    push_i32_filter(&mut filters, "status", query.status);
    push_bool_filter(&mut filters, "is_recalled", query.is_recalled);
    push_i64_filter(&mut filters, "after_seq", query.after_seq);
    push_i64_filter(&mut filters, "before_seq", query.before_seq);
    filters
}

fn build_export_filters(query: &AdminMessageExportHttpRequest) -> Vec<FilterExpression> {
    let mut filters = Vec::new();
    push_string_filter(&mut filters, "sender_id", query.sender_id.as_deref());
    push_string_filter(&mut filters, "message_id", query.message_id.as_deref());
    push_string_filter(
        &mut filters,
        "client_msg_id",
        query.client_msg_id.as_deref(),
    );
    push_string_filter(&mut filters, "channel_id", query.channel_id.as_deref());
    push_i32_filter(&mut filters, "message_type", query.message_type);
    push_i32_filter(&mut filters, "source", query.source);
    push_i32_filter(&mut filters, "status", query.status);
    push_bool_filter(&mut filters, "is_recalled", query.is_recalled);
    push_i64_filter(&mut filters, "after_seq", query.after_seq);
    push_i64_filter(&mut filters, "before_seq", query.before_seq);
    filters
}

fn push_string_filter(filters: &mut Vec<FilterExpression>, field: &str, value: Option<&str>) {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    filters.push(eq_filter(field, value));
}

fn push_i32_filter(filters: &mut Vec<FilterExpression>, field: &str, value: Option<i32>) {
    if let Some(value) = value {
        filters.push(eq_filter(field, value.to_string()));
    }
}

fn push_i64_filter(filters: &mut Vec<FilterExpression>, field: &str, value: Option<i64>) {
    if let Some(value) = value {
        filters.push(eq_filter(field, value.to_string()));
    }
}

fn push_bool_filter(filters: &mut Vec<FilterExpression>, field: &str, value: Option<bool>) {
    if let Some(value) = value {
        filters.push(eq_filter(field, value.to_string()));
    }
}

fn eq_filter(field: &str, value: impl Into<String>) -> FilterExpression {
    FilterExpression {
        field: field.to_string(),
        op: FilterOperator::Eq as i32,
        values: vec![value.into()],
    }
}

fn query_limit(limit: Option<i32>) -> i32 {
    limit
        .unwrap_or(DEFAULT_ADMIN_MESSAGE_QUERY_LIMIT)
        .clamp(1, MAX_ADMIN_MESSAGE_QUERY_LIMIT)
}

fn required_trimmed_value(
    value: &str,
    code: &'static str,
    message: &str,
) -> Result<String, AdminMessageQueryError> {
    let value = value.trim();
    if value.is_empty() {
        Err(AdminMessageQueryError {
            code,
            message: message.to_string(),
        })
    } else {
        Ok(value.to_string())
    }
}

fn trimmed_optional(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn parse_event_types(value: Option<&str>) -> Result<Vec<i32>, AdminMessageQueryError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(Vec::new());
    };

    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| {
            let parsed = item.parse::<i32>().map_err(|_| AdminMessageQueryError {
                code: "ADMIN_MESSAGE_EVENT_TYPE_INVALID",
                message: format!("event type `{item}` is not a valid integer"),
            })?;
            if parsed <= 0 {
                return Err(AdminMessageQueryError {
                    code: "ADMIN_MESSAGE_EVENT_TYPE_INVALID",
                    message: format!("event type `{item}` must be positive"),
                });
            }
            Ok(parsed)
        })
        .collect()
}

impl From<Message> for AdminMessageHttp {
    fn from(message: Message) -> Self {
        let extension_keys = message.extensions.keys().cloned().collect();
        Self {
            server_id: message.server_id,
            conversation_id: message.conversation_id,
            client_msg_id: message.client_msg_id,
            sender_id: message.sender_id,
            source: message.source,
            conversation_seq: message.conversation_seq,
            message_seq: message.message_seq,
            created_at: message.created_at,
            conversation_type: message.conversation_type,
            message_type: message.message_type,
            status: message.status,
            channel_id: message.channel_id,
            sender_name: optional_string(message.sender_name),
            sender_avatar: optional_string(message.sender_avatar),
            content_kind: content_kind(message.content.as_ref()),
            attributes: message.attributes,
            extension_keys,
        }
    }
}

impl From<Event> for AdminMessageEventHttp {
    fn from(event: Event) -> Self {
        Self {
            event_id: event.event_id,
            conversation_id: event.conversation_id,
            conversation_seq: event.conversation_seq,
            event_type: event.r#type,
            created_at: event.created_at,
            request_id: event.request_id,
            payload_kind: event_type_kind(event.r#type),
        }
    }
}

impl From<ProtoMessageWriteLedgerEntry> for AdminMessageWriteLedgerEntryHttp {
    fn from(entry: ProtoMessageWriteLedgerEntry) -> Self {
        Self {
            tenant_id: entry.tenant_id,
            server_id: entry.server_id,
            conversation_id: entry.conversation_id,
            seq: entry.seq,
            write_state: entry.write_state,
            archive_persisted_at: entry.archive_persisted_at.map(|ts| ts.seconds),
            storage_persisted_at: entry.storage_persisted_at.map(|ts| ts.seconds),
            wal_cleaned_at: entry.wal_cleaned_at.map(|ts| ts.seconds),
            ack_published_at: entry.ack_published_at.map(|ts| ts.seconds),
            failed_at: entry.failed_at.map(|ts| ts.seconds),
            last_error: optional_string(entry.last_error),
            created_at: entry.created_at.map(|ts| ts.seconds).unwrap_or_default(),
            updated_at: entry.updated_at.map(|ts| ts.seconds).unwrap_or_default(),
        }
    }
}

fn optional_string(value: String) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn event_type_kind(event_type: i32) -> String {
    match ProtoEventType::try_from(event_type) {
        Ok(ProtoEventType::EventMessage) => "message",
        Ok(ProtoEventType::EventMessageRecall) => "message_recall",
        Ok(ProtoEventType::EventMessageEdit) => "message_edit",
        Ok(ProtoEventType::EventMessageDelete) => "message_delete",
        Ok(ProtoEventType::EventReadReceipt) => "read_receipt",
        Ok(ProtoEventType::EventConversationUpdate) => "conversation_update",
        Ok(ProtoEventType::EventConversationDelete) => "conversation_delete",
        Ok(ProtoEventType::EventReaction) => "reaction",
        Ok(ProtoEventType::EventPin) => "pin",
        Ok(ProtoEventType::EventUnpin) => "unpin",
        Ok(ProtoEventType::EventMark) => "mark",
        Ok(ProtoEventType::EventUnmark) => "unmark",
        Ok(ProtoEventType::EventMessageRetentionScheduled) => "message_retention_scheduled",
        Ok(ProtoEventType::EventMessageRetentionExpired) => "message_retention_expired",
        Ok(ProtoEventType::EventMessageRetentionPurged) => "message_retention_purged",
        Ok(ProtoEventType::EventCustom) => "custom",
        Ok(ProtoEventType::Unspecified) | Err(_) => "unknown",
    }
    .to_string()
}

fn content_kind(content: Option<&flare_proto::common::MessageContent>) -> String {
    match content.and_then(|value| value.content.as_ref()) {
        Some(message_content::Content::Text(_)) => "text",
        Some(message_content::Content::Image(_)) => "image",
        Some(message_content::Content::Video(_)) => "video",
        Some(message_content::Content::Audio(_)) => "audio",
        Some(message_content::Content::File(_)) => "file",
        Some(message_content::Content::Location(_)) => "location",
        Some(message_content::Content::Card(_)) => "card",
        Some(message_content::Content::AppCard(_)) => "app_card",
        Some(message_content::Content::Sticker(_)) => "sticker",
        Some(message_content::Content::Emoji(_)) => "emoji",
        Some(message_content::Content::Quote(_)) => "quote",
        Some(message_content::Content::LinkCard(_)) => "link_card",
        Some(message_content::Content::Forward(_)) => "forward",
        Some(message_content::Content::Thread(_)) => "thread",
        Some(message_content::Content::RichText(_)) => "rich_text",
        Some(message_content::Content::ImageGroup(_)) => "image_group",
        Some(message_content::Content::System(_)) => "system",
        Some(message_content::Content::Notification(_)) => "notification",
        Some(message_content::Content::Custom(_)) => "custom",
        Some(message_content::Content::Placeholder(_)) => "placeholder",
        None => "unknown",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_grpc_proto::storage::{
        GetMessageResponse, MessageWriteLedgerEntry, QueryMessageEventsResponse,
        QueryMessageWriteLedgerResponse,
    };
    use flare_proto::common::{Event, Pagination};

    #[test]
    fn admin_message_query_builds_supported_filters_and_caps_limit() {
        let query = AdminMessageQueryHttpRequest {
            conversation_id: Some("conv-a".to_string()),
            message_id: Some("msg-a".to_string()),
            sender_id: Some("user-a".to_string()),
            status: Some(2),
            after_seq: Some(10),
            limit: Some(5_000),
            ..Default::default()
        };

        let request = build_storage_search_request(&query).expect("storage search request");

        assert_eq!(request.pagination.as_ref().expect("pagination").limit, 500);
        assert!(
            request
                .filters
                .iter()
                .any(|filter| filter.field == "conversation_id" && filter.values == ["conv-a"])
        );
        assert!(
            request
                .filters
                .iter()
                .any(|filter| filter.field == "message_id" && filter.values == ["msg-a"])
        );
        assert!(
            request
                .filters
                .iter()
                .any(|filter| filter.field == "after_seq" && filter.values == ["10"])
        );
    }

    #[test]
    fn admin_message_query_rejects_unbounded_scan() {
        let query = AdminMessageQueryHttpRequest::default();

        let error = build_storage_search_request(&query).expect_err("unbounded scan");

        assert_eq!(error.code, "ADMIN_MESSAGE_QUERY_FILTER_REQUIRED");
    }

    #[test]
    fn admin_message_detail_response_summarizes_message_without_payload() {
        let response = admin_message_detail_response(GetMessageResponse {
            message: Some(test_message()),
        })
        .expect("message detail");

        assert_eq!(response.message.server_id, "msg-a");
        assert_eq!(response.message.content_kind, "unknown");
        assert_eq!(response.message.extension_keys, ["payload_hash"]);
    }

    #[test]
    fn admin_message_events_request_caps_limit_and_parses_event_types() {
        let query = AdminMessageEventsQueryHttpRequest {
            event_types: Some("1,2, 8".to_string()),
            cursor: Some("40".to_string()),
            limit: Some(5_000),
        };

        let request = build_storage_message_events_request(" msg-a ", &query)
            .expect("message events request");

        assert_eq!(request.message_id, "msg-a");
        assert_eq!(request.event_types, [1, 2, 8]);
        let pagination = request.pagination.expect("pagination");
        assert_eq!(pagination.cursor, "40");
        assert_eq!(pagination.limit, 500);
    }

    #[test]
    fn admin_message_events_request_rejects_empty_message_id() {
        let error = build_storage_message_events_request("", &Default::default())
            .expect_err("message_id is required");

        assert_eq!(error.code, "ADMIN_MESSAGE_ID_REQUIRED");
    }

    #[test]
    fn admin_message_events_response_summarizes_event_payload() {
        let response = admin_message_events_response(
            QueryMessageEventsResponse {
                events: vec![Event {
                    conversation_id: "conv-a".to_string(),
                    conversation_seq: 7,
                    r#type: 2,
                    created_at: 1_700_000_000,
                    event_id: "event-a".to_string(),
                    request_id: Some("req-a".to_string()),
                    payload: None,
                }],
                pagination: Some(Pagination {
                    cursor: "next".to_string(),
                    limit: 10,
                    has_more: true,
                    previous_cursor: String::new(),
                    total_size: 20,
                }),
            },
            10,
        );

        assert_eq!(response.events[0].event_id, "event-a");
        assert_eq!(response.events[0].payload_kind, "message_recall");
        assert_eq!(response.next_cursor.as_deref(), Some("next"));
        assert!(response.has_more);
    }

    #[test]
    fn admin_message_write_ledger_request_rejects_unbounded_scan() {
        let error =
            build_storage_write_ledger_request(&AdminMessageWriteLedgerQueryHttpRequest::default())
                .expect_err("bounded ledger query is required");

        assert_eq!(error.code, "ADMIN_MESSAGE_WRITE_LEDGER_FILTER_REQUIRED");
    }

    #[test]
    fn admin_message_write_ledger_request_caps_limit_and_preserves_filters() {
        let request =
            build_storage_write_ledger_request(&AdminMessageWriteLedgerQueryHttpRequest {
                conversation_id: Some(" conv-a ".to_string()),
                write_state: Some("ack_publish_failed".to_string()),
                failed_only: Some(true),
                updated_after: Some(1_700_000_000),
                cursor: Some("25".to_string()),
                limit: Some(5_000),
                ..Default::default()
            })
            .expect("ledger request");

        assert_eq!(request.conversation_id, "conv-a");
        assert_eq!(request.write_state, "ack_publish_failed");
        assert!(request.failed_only);
        assert_eq!(request.updated_after, 1_700_000_000);
        let pagination = request.pagination.expect("pagination");
        assert_eq!(pagination.cursor, "25");
        assert_eq!(pagination.limit, 500);
    }

    #[test]
    fn admin_message_write_ledger_response_summarizes_entries() {
        let response = admin_message_write_ledger_response(
            QueryMessageWriteLedgerResponse {
                entries: vec![MessageWriteLedgerEntry {
                    tenant_id: "tenant-a".to_string(),
                    server_id: "msg-a".to_string(),
                    conversation_id: "conv-a".to_string(),
                    seq: 7,
                    write_state: "ack_publish_failed".to_string(),
                    last_error: "nats timeout".to_string(),
                    ..Default::default()
                }],
                pagination: Some(Pagination {
                    cursor: "next".to_string(),
                    limit: 10,
                    has_more: true,
                    previous_cursor: String::new(),
                    total_size: 1,
                }),
            },
            10,
        );

        assert_eq!(response.entries[0].server_id, "msg-a");
        assert_eq!(
            response.entries[0].last_error.as_deref(),
            Some("nats timeout")
        );
        assert_eq!(response.next_cursor.as_deref(), Some("next"));
        assert!(response.has_more);
    }

    #[test]
    fn admin_message_export_requires_conversation_and_time_range() {
        let error = build_storage_message_export_request(&AdminMessageExportHttpRequest::default())
            .expect_err("bounded export is required");
        assert_eq!(error.code, "ADMIN_MESSAGE_EXPORT_CONVERSATION_REQUIRED");

        let request = build_storage_message_export_request(&AdminMessageExportHttpRequest {
            conversation_id: "conv-a".to_string(),
            start_time: Some(1_700_000_000),
            end_time: Some(1_700_060_000),
            sender_id: Some("user-a".to_string()),
            status: Some(2),
            ..Default::default()
        })
        .expect("export request");

        assert_eq!(request.conversation_id, "conv-a");
        assert_eq!(
            request.time_range.expect("time range").start_time,
            Some(1_700_000_000)
        );
        assert!(
            request
                .filters
                .iter()
                .any(|filter| filter.field == "sender_id" && filter.values == ["user-a"])
        );
    }

    fn test_message() -> Message {
        let mut extensions = HashMap::new();
        extensions.insert("payload_hash".to_string(), Vec::new());
        Message {
            server_id: "msg-a".to_string(),
            conversation_id: "conv-a".to_string(),
            client_msg_id: "client-a".to_string(),
            sender_id: "user-a".to_string(),
            source: 1,
            conversation_seq: 7,
            created_at: 1_700_000_000,
            conversation_type: 1,
            message_type: 1,
            status: 1,
            channel_id: "default".to_string(),
            extensions,
            ..Default::default()
        }
    }
}
