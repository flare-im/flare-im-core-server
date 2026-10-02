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

/// 请求里带这个键的检索停在替身里，直到测试放行（模拟一次慢查询）。
const HOLD_KEY: &str = "k-hold";

/// 替身收到的调用（`authorization` 与全部 `x-*` 元数据键）与慢查询的闸门。
#[derive(Default)]
struct FakeState {
    calls: std::sync::Mutex<Vec<(Vec<String>, Vec<String>)>>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[derive(Clone, Default)]
struct FakeStorageReader(Arc<FakeState>);

impl FakeStorageReader {
    async fn observe<T>(&self, request: &GrpcRequest<T>, key: &str) {
        let metadata = request.metadata();
        let authorization = metadata
            .get_all("authorization")
            .iter()
            .map(|value| value.to_str().unwrap_or("<non-ascii>").to_string())
            .collect();
        let mut x_metadata: Vec<String> = metadata
            .keys()
            .map(|key| match key {
                tonic::metadata::KeyRef::Ascii(key) => key.as_str().to_string(),
                tonic::metadata::KeyRef::Binary(key) => key.as_str().to_string(),
            })
            .filter(|key| key.starts_with("x-"))
            .collect();
        x_metadata.sort();
        self.0
            .calls
            .lock()
            .unwrap()
            .push((authorization, x_metadata));
        if key.contains(HOLD_KEY) {
            self.0.entered.notify_one();
            self.0.release.notified().await;
        }
    }
}

fn unused<T>() -> Result<GrpcResponse<T>, Status> {
    Err(Status::unimplemented("not used by admin-gateway"))
}

#[tonic::async_trait]
impl StorageReaderService for FakeStorageReader {
    async fn search_messages(
        &self,
        request: GrpcRequest<SearchMessagesRequest>,
    ) -> Result<GrpcResponse<SearchMessagesResponse>, Status> {
        let summary = format!("{:?}", request.get_ref());
        self.observe(&request, &summary).await;
        if let Some(status) = failure_for(&summary) {
            return Err(status);
        }
        Ok(GrpcResponse::new(SearchMessagesResponse::default()))
    }

    async fn get_message(
        &self,
        request: GrpcRequest<GetMessageRequest>,
    ) -> Result<GrpcResponse<GetMessageResponse>, Status> {
        self.observe(&request, &request.get_ref().message_id).await;
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
        self.observe(&request, &request.get_ref().message_id).await;
        if let Some(status) = failure_for(&request.get_ref().message_id) {
            return Err(status);
        }
        Ok(GrpcResponse::new(QueryMessageEventsResponse::default()))
    }

    async fn query_message_write_ledger(
        &self,
        request: GrpcRequest<QueryMessageWriteLedgerRequest>,
    ) -> Result<GrpcResponse<QueryMessageWriteLedgerResponse>, Status> {
        self.observe(&request, &request.get_ref().conversation_id)
            .await;
        if let Some(status) = failure_for(&request.get_ref().conversation_id) {
            return Err(status);
        }
        Ok(GrpcResponse::new(QueryMessageWriteLedgerResponse::default()))
    }

    async fn export_messages(
        &self,
        request: GrpcRequest<ExportMessagesRequest>,
    ) -> Result<GrpcResponse<ExportMessagesResponse>, Status> {
        self.observe(&request, &request.get_ref().conversation_id)
            .await;
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
    fake: FakeStorageReader,
}

async fn messages_harness() -> MessagesHarness {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    assert!(address.ip().is_loopback());
    let fake = FakeStorageReader::default();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(StorageReaderServiceServer::new(fake.clone()))
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
    MessagesHarness {
        router,
        token,
        fake,
    }
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

/// Storage Reader 只认转发的 admin 令牌：5 条路由都只带这个令牌，外加请求号与追踪号，不带
/// 未签名的租户、用户或操作人元数据。修复前带的是 `x-tenant-id`、`x-user-id` 等元数据，
/// 没有令牌。
#[tokio::test]
async fn message_routes_forward_only_the_admin_assertion_and_correlation_ids() {
    let h = messages_harness().await;
    for route in 0..INTERNAL_REASONS.len() {
        let (status, body) = send(&h.router, message_request(route, "ok", &h.token)).await;
        assert_eq!(status, StatusCode::OK, "route {route}: {body}");
    }
    let calls = h.fake.0.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), INTERNAL_REASONS.len());
    for (route, (authorization, x_metadata)) in calls.iter().enumerate() {
        assert_eq!(
            authorization,
            &vec![format!("Bearer {}", h.token)],
            "route {route}"
        );
        assert!(
            x_metadata
                .iter()
                .all(|key| key == "x-request-id" || key == "x-trace-id"),
            "route {route} sent unsigned metadata {x_metadata:?}"
        );
        assert!(
            x_metadata.contains(&"x-request-id".to_string()),
            "route {route}"
        );
    }
}

/// 一次慢检索不会挡住其他管理检索：客户端在锁里只取一份克隆，调用期间不占锁。修复前网关在
/// 锁里等整个 RPC，第二条检索要排在慢查询之后。
#[tokio::test]
async fn a_slow_storage_query_does_not_hold_up_other_admin_searches() {
    let h = messages_harness().await;
    let slow = tokio::spawn({
        let router = h.router.clone();
        let request = message_request(0, HOLD_KEY, &h.token);
        async move { send(&router, request).await }
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        h.fake.0.entered.notified(),
    )
    .await
    .expect("the slow search reached the storage reader");

    for route in 1..INTERNAL_REASONS.len() {
        let (status, body) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            send(&h.router, message_request(route, "ok", &h.token)),
        )
        .await
        .unwrap_or_else(|_| panic!("route {route} waited for the slow search"));
        assert_eq!(status, StatusCode::OK, "route {route}: {body}");
    }
    assert!(!slow.is_finished());
    h.fake.0.release.notify_one();
    let (status, body) = slow.await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// 带请求体的检索路由（检索、写入台账、导出）在请求体解析失败时的请求：`content_type` 为
/// `None` 时不带这个头。导出是写操作，带齐审计头。
fn raw_body_request(
    route: usize,
    content_type: Option<&str>,
    body: &str,
    token: &str,
) -> Request<Body> {
    let (path, write) = match route {
        0 => ("/api/v1/admin/messages/query", false),
        3 => ("/api/v1/admin/messages/write-ledger/query", false),
        4 => ("/api/v1/admin/messages/export", true),
        _ => unreachable!(),
    };
    let mut builder = Request::builder()
        .method("POST")
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("x-tenant-id", TENANT);
    if write {
        for (name, value) in WRITE_HEADERS {
            builder = builder.header(*name, *value);
        }
    }
    if let Some(content_type) = content_type {
        builder = builder.header("content-type", content_type);
    }
    builder.body(Body::from(body.to_string())).unwrap()
}

/// 请求里的取值：网关答复里出现即算回显。
const BODY_MARKER: &str = "leak-marker-77";

/// 字段类型不对（W8-S2 F1：检索字段写成数字）、JSON 写坏、顶层是字符串、空请求体、没声明
/// `application/json`：三条带请求体的路由都回网关自己的 400 信封 `ADMIN_INVALID_JSON_BODY`，
/// 不回 axum 默认的纯文本 422/400/415，不回显请求里的取值与解析器的说明，也不调用 Storage
/// Reader。修复前字段类型不对是 422，Control 只认 400/403/504，把它当成 Core 故障报 503。
#[tokio::test]
async fn malformed_message_bodies_are_typed_bad_requests_without_echo() {
    let h = messages_harness().await;
    let json = Some("application/json");
    let cases = [
        (json, r#"{"conversation_id":770077}"#.to_string()),
        (json, format!(r#"{{"conversation_id":["{BODY_MARKER}"]}}"#)),
        (json, format!(r#"{{"conversation_id":"{BODY_MARKER}""#)),
        (json, format!(r#""{BODY_MARKER}""#)),
        (json, String::new()),
        (
            Some("text/plain"),
            format!(r#"{{"conversation_id":"{BODY_MARKER}"}}"#),
        ),
        (None, format!(r#"{{"conversation_id":"{BODY_MARKER}"}}"#)),
    ];
    for route in [0, 3, 4] {
        for (content_type, body) in &cases {
            let (status, answer) = send(
                &h.router,
                raw_body_request(route, *content_type, body, &h.token),
            )
            .await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "route {route} {content_type:?} {body}: {answer}"
            );
            assert_eq!(answer["code"], 400, "route {route} {body}: {answer}");
            assert_eq!(
                answer["reason"], "ADMIN_INVALID_JSON_BODY",
                "route {route} {body}: {answer}"
            );
            let rendered = answer.to_string();
            for leaked in [BODY_MARKER, "770077", "invalid type", "line 1", "column"] {
                assert!(
                    !rendered.contains(leaked),
                    "route {route} {body} echoed {leaked}: {rendered}"
                );
            }
        }
    }
    assert!(h.fake.0.calls.lock().unwrap().is_empty());
}

/// 事件链的查询串类型不对：400 `ADMIN_INVALID_QUERY` 信封，不回显取值，不调用 Storage
/// Reader。修复前是 axum 的纯文本 400，带着解析器说明与取值。
#[tokio::test]
async fn malformed_event_query_strings_are_typed_bad_requests_without_echo() {
    let h = messages_harness().await;
    let request = Request::builder()
        .method("GET")
        .uri(format!(
            "/api/v1/admin/messages/ok/events?limit={BODY_MARKER}"
        ))
        .header("authorization", format!("Bearer {}", h.token))
        .header("x-tenant-id", TENANT)
        .body(Body::empty())
        .unwrap();
    let (status, answer) = send(&h.router, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{answer}");
    assert_eq!(answer["code"], 400, "{answer}");
    assert_eq!(answer["reason"], "ADMIN_INVALID_QUERY", "{answer}");
    assert!(!answer.to_string().contains(BODY_MARKER), "{answer}");
    assert!(h.fake.0.calls.lock().unwrap().is_empty());
}

/// 消息 id 百分号解码后不是 UTF-8：400 `ADMIN_INVALID_PATH` 信封，不回显取值，不调用 Storage
/// Reader。修复前是 axum 的纯文本 400，带着解码后的取值。
#[tokio::test]
async fn undecodable_message_ids_are_typed_bad_requests_without_echo() {
    let h = messages_harness().await;
    for uri in [
        format!("/api/v1/admin/messages/{BODY_MARKER}%FF"),
        format!("/api/v1/admin/messages/{BODY_MARKER}%FF/events"),
    ] {
        let request = Request::builder()
            .method("GET")
            .uri(&uri)
            .header("authorization", format!("Bearer {}", h.token))
            .header("x-tenant-id", TENANT)
            .body(Body::empty())
            .unwrap();
        let (status, answer) = send(&h.router, request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {answer}");
        assert_eq!(answer["code"], 400, "{uri}: {answer}");
        assert_eq!(answer["reason"], "ADMIN_INVALID_PATH", "{uri}: {answer}");
        assert!(!answer.to_string().contains(BODY_MARKER), "{answer}");
    }
    assert!(h.fake.0.calls.lock().unwrap().is_empty());
}

/// 请求体超过网关上限（`FLARE_ADMIN_GATEWAY_SERVER_MAX_BODY_SIZE`，即 `DefaultBodyLimit`）：
/// 400 `ADMIN_REQUEST_BODY_TOO_LARGE` 信封，而不是 axum 的纯文本 413。
#[tokio::test]
async fn an_oversized_message_body_is_a_typed_bad_request() {
    let h = messages_harness().await;
    let router = h
        .router
        .clone()
        .layer(axum::extract::DefaultBodyLimit::max(64));
    let body = format!(r#"{{"conversation_id":"{}"}}"#, "c".repeat(128));
    let (status, answer) = send(
        &router,
        raw_body_request(0, Some("application/json"), &body, &h.token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{answer}");
    assert_eq!(answer["reason"], "ADMIN_REQUEST_BODY_TOO_LARGE", "{answer}");
    assert!(h.fake.0.calls.lock().unwrap().is_empty());
}
