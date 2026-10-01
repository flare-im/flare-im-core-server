//! 消息检索 5 条路由（Storage Reader）的下游失败映射：与 ADM-01、媒资路由同一口径，经
//! `admin_rpc_error` 类型化为 400/403/404/502/503/504，其余为各路由固定 reason 的 500；
//! 响应里不出现 Storage Reader 的错误文案。替身 Storage Reader 只绑 127.0.0.1。
mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::*;
use flare_admin_gateway::interface::http::create_admin_router;
use flare_grpc_proto::storage::storage_reader_service_server::{
    StorageReaderService, StorageReaderServiceServer,
};
use flare_grpc_proto::storage::*;
use flare_im_service_kit::clients::{
    ConversationManageServiceClientWrapper, ConversationReadServiceClientWrapper, GrpcClients,
    MediaServiceClientWrapper, MessageActionServiceClientWrapper, MessageEventServiceClientWrapper,
    MessageSendServiceClientWrapper, OnlineServiceClientWrapper, StorageReaderServiceClientWrapper,
};
use tonic::transport::Channel;
use tonic::{Request as GrpcRequest, Response as GrpcResponse, Status};

/// 下游错误文案里的标记：网关响应里出现即算回显。
const DOWNSTREAM_DETAIL: &str = "id-secret-1 at 10.0.0.7:5432";

/// 请求里带哪个键，替身就回哪种失败；`ok` 正常回答。
const FAILURES: &[(&str, tonic::Code, StatusCode, &str)] = &[
    (
        "k-denied",
        tonic::Code::PermissionDenied,
        StatusCode::FORBIDDEN,
        "FORBIDDEN",
    ),
    (
        "k-missing",
        tonic::Code::NotFound,
        StatusCode::NOT_FOUND,
        "NOT_FOUND",
    ),
    (
        "k-invalid",
        tonic::Code::InvalidArgument,
        StatusCode::BAD_REQUEST,
        "BAD_REQUEST",
    ),
    (
        "k-down",
        tonic::Code::Unavailable,
        StatusCode::SERVICE_UNAVAILABLE,
        "ADMIN_UPSTREAM_UNAVAILABLE",
    ),
    (
        "k-slow",
        tonic::Code::DeadlineExceeded,
        StatusCode::GATEWAY_TIMEOUT,
        "ADMIN_UPSTREAM_TIMEOUT",
    ),
    (
        "k-unauth",
        tonic::Code::Unauthenticated,
        StatusCode::BAD_GATEWAY,
        "ADMIN_UPSTREAM_REJECTED_ASSERTION",
    ),
];

/// Storage Reader 自己的内部错误（例如数据库失败）：500，reason 是各路由固定的。
const INTERNAL_KEY: &str = "k-internal";

fn failure_for(summary: &str) -> Option<Status> {
    if summary.contains(INTERNAL_KEY) {
        return Some(Status::internal(format!("db error {DOWNSTREAM_DETAIL}")));
    }
    FAILURES
        .iter()
        .find(|(key, ..)| summary.contains(key))
        .map(|(_, code, ..)| Status::new(*code, format!("downstream {DOWNSTREAM_DETAIL}")))
}

#[derive(Clone, Default)]
struct FakeStorageReader;

fn unused<T>() -> Result<GrpcResponse<T>, Status> {
    Err(Status::unimplemented("not used by admin-gateway"))
}

#[tonic::async_trait]
impl StorageReaderService for FakeStorageReader {
    async fn search_messages(
        &self,
        request: GrpcRequest<SearchMessagesRequest>,
    ) -> Result<GrpcResponse<SearchMessagesResponse>, Status> {
        if let Some(status) = failure_for(&format!("{:?}", request.get_ref())) {
            return Err(status);
        }
        Ok(GrpcResponse::new(SearchMessagesResponse::default()))
    }

    async fn get_message(
        &self,
        request: GrpcRequest<GetMessageRequest>,
    ) -> Result<GrpcResponse<GetMessageResponse>, Status> {
        if let Some(status) = failure_for(&request.get_ref().message_id) {
            return Err(status);
        }
        Ok(GrpcResponse::new(GetMessageResponse {
            message: Some(flare_proto::Message {
                server_id: request.get_ref().message_id.clone(),
                ..Default::default()
            }),
        }))
    }

    async fn query_message_events(
        &self,
        request: GrpcRequest<QueryMessageEventsRequest>,
    ) -> Result<GrpcResponse<QueryMessageEventsResponse>, Status> {
        if let Some(status) = failure_for(&request.get_ref().message_id) {
            return Err(status);
        }
        Ok(GrpcResponse::new(QueryMessageEventsResponse::default()))
    }

    async fn query_message_write_ledger(
        &self,
        request: GrpcRequest<QueryMessageWriteLedgerRequest>,
    ) -> Result<GrpcResponse<QueryMessageWriteLedgerResponse>, Status> {
        if let Some(status) = failure_for(&request.get_ref().conversation_id) {
            return Err(status);
        }
        Ok(GrpcResponse::new(QueryMessageWriteLedgerResponse::default()))
    }

    async fn export_messages(
        &self,
        request: GrpcRequest<ExportMessagesRequest>,
    ) -> Result<GrpcResponse<ExportMessagesResponse>, Status> {
        if let Some(status) = failure_for(&request.get_ref().conversation_id) {
            return Err(status);
        }
        Ok(GrpcResponse::new(ExportMessagesResponse {
            export_task_id: "export-1".to_string(),
        }))
    }

    async fn query_messages(
        &self,
        _: GrpcRequest<QueryMessagesRequest>,
    ) -> Result<GrpcResponse<QueryMessagesResponse>, Status> {
        unused()
    }
    async fn query_messages_by_seq(
        &self,
        _: GrpcRequest<QueryMessagesBySeqRequest>,
    ) -> Result<GrpcResponse<QueryMessagesBySeqResponse>, Status> {
        unused()
    }
    async fn query_conversations_message_windows(
        &self,
        _: GrpcRequest<QueryConversationsMessageWindowsRequest>,
    ) -> Result<GrpcResponse<QueryConversationsMessageWindowsResponse>, Status> {
        unused()
    }
    async fn query_message_edit_history(
        &self,
        _: GrpcRequest<QueryMessageEditHistoryRequest>,
    ) -> Result<GrpcResponse<QueryMessageEditHistoryResponse>, Status> {
        unused()
    }
    async fn query_message_read_list(
        &self,
        _: GrpcRequest<QueryMessageReadListRequest>,
    ) -> Result<GrpcResponse<QueryMessageReadListResponse>, Status> {
        unused()
    }
    async fn query_message_marks(
        &self,
        _: GrpcRequest<QueryMessageMarksRequest>,
    ) -> Result<GrpcResponse<QueryMessageMarksResponse>, Status> {
        unused()
    }
    async fn query_message_reactions(
        &self,
        _: GrpcRequest<QueryMessageReactionsRequest>,
    ) -> Result<GrpcResponse<QueryMessageReactionsResponse>, Status> {
        unused()
    }
    async fn list_message_tags(
        &self,
        _: GrpcRequest<ListMessageTagsRequest>,
    ) -> Result<GrpcResponse<ListMessageTagsResponse>, Status> {
        unused()
    }
    async fn query_conversation_events(
        &self,
        _: GrpcRequest<QueryConversationEventsRequest>,
    ) -> Result<GrpcResponse<QueryConversationEventsResponse>, Status> {
        unused()
    }
    async fn get_conversation_message_head(
        &self,
        _: GrpcRequest<GetConversationMessageHeadRequest>,
    ) -> Result<GrpcResponse<GetConversationMessageHeadResponse>, Status> {
        unused()
    }
}

struct MessagesHarness {
    router: axum::Router,
    token: String,
}

async fn messages_harness() -> MessagesHarness {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    assert!(address.ip().is_loopback());
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(StorageReaderServiceServer::new(FakeStorageReader))
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
    );
    let storage = Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_lazy();
    // 其余下游指向一个不会被调用的环回端口。
    let unused = Channel::from_static("http://127.0.0.1:9").connect_lazy();
    let clients = GrpcClients {
        media: Arc::new(tokio::sync::Mutex::new(
            MediaServiceClientWrapper::from_channel(unused.clone(), None),
        )),
        message_ingest_send: Arc::new(tokio::sync::Mutex::new(
            MessageSendServiceClientWrapper::from_channel(unused.clone()),
        )),
        message_event: Arc::new(tokio::sync::Mutex::new(
            MessageEventServiceClientWrapper::from_channel(unused.clone()),
        )),
        message_action: Arc::new(tokio::sync::Mutex::new(
            MessageActionServiceClientWrapper::from_channel(unused.clone()),
        )),
        conversation_read: Arc::new(tokio::sync::Mutex::new(
            ConversationReadServiceClientWrapper::from_channel(unused.clone()),
        )),
        conversation_manage: Arc::new(tokio::sync::Mutex::new(
            ConversationManageServiceClientWrapper::from_channel(unused.clone()),
        )),
        online: Arc::new(tokio::sync::Mutex::new(
            OnlineServiceClientWrapper::from_channel(unused),
        )),
        storage_reader: Arc::new(tokio::sync::Mutex::new(
            StorageReaderServiceClientWrapper::from_channel(storage),
        )),
    };
    let console = Console::new();
    let router = create_admin_router(Arc::new(clients), None)
        .layer(axum::Extension(Arc::new(console.verifier.clone())));
    let token = console.admin_token(EMPLOYEE, TENANT);
    MessagesHarness { router, token }
}

/// 5 条路由各自的请求：键放在替身据以判定失败的字段里。导出是写操作，带齐审计头。
fn message_request(route: usize, key: &str, token: &str) -> Request<Body> {
    let (method, path, body, write) = match route {
        0 => (
            "POST",
            "/api/v1/admin/messages/query".to_string(),
            Some(format!(r#"{{"conversation_id":"{key}"}}"#)),
            false,
        ),
        1 => ("GET", format!("/api/v1/admin/messages/{key}"), None, false),
        2 => (
            "GET",
            format!("/api/v1/admin/messages/{key}/events"),
            None,
            false,
        ),
        3 => (
            "POST",
            "/api/v1/admin/messages/write-ledger/query".to_string(),
            Some(format!(r#"{{"conversation_id":"{key}"}}"#)),
            false,
        ),
        4 => (
            "POST",
            "/api/v1/admin/messages/export".to_string(),
            Some(format!(
                r#"{{"conversation_id":"{key}","start_time":1700000000000,"end_time":1700000600000}}"#
            )),
            true,
        ),
        _ => unreachable!(),
    };
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("x-tenant-id", TENANT);
    if write {
        for (name, value) in WRITE_HEADERS {
            builder = builder.header(*name, *value);
        }
    }
    match body {
        Some(body) => builder
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    }
}

/// 每条路由失败时的固定 reason（Storage Reader 内部错误时用）。
const INTERNAL_REASONS: [&str; 5] = [
    "ADMIN_MESSAGE_QUERY_FAILED",
    "ADMIN_MESSAGE_DETAIL_FAILED",
    "ADMIN_MESSAGE_EVENTS_FAILED",
    "ADMIN_MESSAGE_WRITE_LEDGER_QUERY_FAILED",
    "ADMIN_MESSAGE_EXPORT_FAILED",
];

#[tokio::test]
async fn message_routes_reach_the_storage_reader_fake() {
    let h = messages_harness().await;
    for route in 0..INTERNAL_REASONS.len() {
        let (status, body) = send(&h.router, message_request(route, "ok", &h.token)).await;
        assert_eq!(status, StatusCode::OK, "route {route}: {body}");
    }
}

#[tokio::test]
async fn storage_reader_failures_are_typed_and_never_echoed() {
    let h = messages_harness().await;
    for (route, internal_reason) in INTERNAL_REASONS.iter().enumerate() {
        let cases = FAILURES
            .iter()
            .map(|(key, _, status, reason)| (*key, *status, *reason))
            .chain([(
                INTERNAL_KEY,
                StatusCode::INTERNAL_SERVER_ERROR,
                *internal_reason,
            )]);
        for (key, expected_status, expected_reason) in cases {
            let (status, body) = send(&h.router, message_request(route, key, &h.token)).await;
            assert_eq!(status, expected_status, "route {route} {key}: {body}");
            assert_eq!(
                body["reason"].as_str().unwrap_or_default(),
                expected_reason,
                "route {route} {key}: {body}"
            );
            let rendered = body.to_string();
            for leaked in ["id-secret-1", "10.0.0.7", "db error"] {
                assert!(
                    !rendered.contains(leaked),
                    "route {route} {key} echoed {leaked}: {rendered}"
                );
            }
        }
    }
}
