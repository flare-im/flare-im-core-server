//! 下游 gRPC 调用的请求超时（`GRPC_REQUEST_TIMEOUT_SECS`）与对外 CORS。
//!
//! 所有下游通道都指向一个只接连接、从不应答的端口：TCP 与 HTTP/2 握手照常，RPC 永远等不到
//! 应答。以前只有设备推送令牌接口限时，其余接口在这种下游上一直挂着；现在每次下游调用（含
//! 排队等客户端锁）都按请求超时放弃，回可重试的 503/504，不回显下游文本。
use super::*;
use crate::interface::http::MessageContextSigner;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use flare_im_service_kit::clients::*;
use flare_im_service_kit::{
    gateway_auth::TenantRuntimeGuard,
    tenant_runtime::{TenantAccessPolicy, TenantRuntimeCache},
};
use flare_server_core::{
    AuthError, AuthenticatedPrincipal, SignedContextSigner, TokenValidationRequest, TokenValidator,
};
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tower::ServiceExt;

/// 用例里的请求超时；路由外再套的上限远大于它，修复前请求一直挂着时用例按该上限失败。
const REQUEST_TIMEOUT: Duration = Duration::from_millis(400);
const HUNG: Duration = Duration::from_secs(5);

struct Validator;

#[async_trait::async_trait]
impl TokenValidator for Validator {
    async fn validate(
        &self,
        request: TokenValidationRequest,
    ) -> Result<AuthenticatedPrincipal, AuthError> {
        if request.token != "alice-a" {
            return Err(AuthError::InvalidToken("unknown test token".into()));
        }
        Ok(AuthenticatedPrincipal {
            user_id: "alice".into(),
            tenant_id: Some("tenant-a".into()),
            device_id: None,
            app_id: None,
            expires_at: None,
            scopes: vec![],
            metadata: Default::default(),
        })
    }
}

fn signer() -> SignedContextSigner {
    let key = SigningKey::from_bytes(&[71; 32]);
    let private = key.to_pkcs8_pem(Default::default()).unwrap();
    SignedContextSigner::from_private_key(&private).unwrap()
}

/// 接受连接后一直持有、从不读写的下游：调用方的 RPC 等不到任何应答。
async fn unresponsive_downstream() -> Channel {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_lazy()
}

async fn router(request_timeout: Option<Duration>) -> Router {
    let c = unresponsive_downstream().await;
    let clients = GrpcClients {
        media: Arc::new(Mutex::new(MediaServiceClientWrapper::from_channel(
            c.clone(),
            None,
        ))),
        message_ingest_send: Arc::new(Mutex::new(MessageSendServiceClientWrapper::from_channel(
            c.clone(),
        ))),
        message_event: Arc::new(Mutex::new(MessageEventServiceClientWrapper::from_channel(
            c.clone(),
        ))),
        message_action: Arc::new(Mutex::new(MessageActionServiceClientWrapper::from_channel(
            c.clone(),
        ))),
        conversation_read: Arc::new(Mutex::new(
            ConversationReadServiceClientWrapper::from_channel(c.clone()),
        )),
        conversation_manage: Arc::new(Mutex::new(
            ConversationManageServiceClientWrapper::from_channel(c.clone()),
        )),
        online: Arc::new(Mutex::new(OnlineServiceClientWrapper::from_channel(
            c.clone(),
        ))),
        storage_reader: Arc::new(Mutex::new(StorageReaderServiceClientWrapper::from_channel(
            c,
        ))),
        request_timeout,
    };
    create_public_router(Arc::new(clients))
        .layer(axum::Extension(
            Arc::new(Validator) as Arc<dyn TokenValidator>
        ))
        .layer(axum::Extension(TenantRuntimeGuard::new(
            TenantRuntimeCache::disabled(),
            TenantAccessPolicy::Lenient,
        )))
        .layer(axum::Extension(MessageContextSigner(Some(signer()))))
        .layer(public_cors_layer())
}

struct Answer {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Value,
}

async fn call(router: &Router, request: Request<Body>) -> Answer {
    let what = format!("{} {}", request.method(), request.uri());
    let response = tokio::time::timeout(HUNG, router.clone().oneshot(request))
        .await
        .unwrap_or_else(|_| panic!("{what} still hangs after {HUNG:?}: no request timeout"))
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    Answer {
        status,
        headers,
        body: serde_json::from_slice(&body).unwrap_or(Value::Null),
    }
}

fn get(uri: &str) -> Request<Body> {
    Request::get(uri)
        .header("authorization", "Bearer alice-a")
        .body(Body::empty())
        .unwrap()
}

fn post(uri: &str, body: Value) -> Request<Body> {
    Request::post(uri)
        .header("authorization", "Bearer alice-a")
        .header("content-type", "application/json")
        .header("origin", "https://app.example.test")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn assert_retryable(answer: &Answer, status: StatusCode, reason: &str, what: &str) {
    assert_eq!(answer.status, status, "{what}: {}", answer.body);
    assert_eq!(
        answer.body["reason"],
        json!(reason),
        "{what}: {}",
        answer.body
    );
    assert!(
        !answer.body.to_string().contains("timed out"),
        "{what} echoes the internal timeout text: {}",
        answer.body
    );
}

/// 每一类下游（会话、在线、消息动作、HTTP 发送、Media）卡住时都按请求超时返回：会话、在线与
/// 消息动作回 503 `SERVICE_UNAVAILABLE`，HTTP 发送同样回 503 并带这次的 `client_msg_id`（W12-T），
/// Media 照它原有的口径回 504 `MEDIA_TIMEOUT`。
#[tokio::test]
async fn every_downstream_family_ends_at_the_request_timeout_with_a_retryable_status() {
    let router = router(Some(REQUEST_TIMEOUT)).await;

    let started = Instant::now();
    let answer = call(&router, get("/api/v1/conversations")).await;
    assert_retryable(
        &answer,
        StatusCode::SERVICE_UNAVAILABLE,
        "SERVICE_UNAVAILABLE",
        "conversation list",
    );
    assert!(
        started.elapsed() >= REQUEST_TIMEOUT,
        "{:?}",
        started.elapsed()
    );

    let answer = call(&router, get("/api/v1/presence/users/alice")).await;
    assert_retryable(
        &answer,
        StatusCode::SERVICE_UNAVAILABLE,
        "SERVICE_UNAVAILABLE",
        "presence",
    );

    let answer = call(
        &router,
        post(
            "/api/v1/messages/recall",
            json!({"conversation_id": "group:1", "message_id": "m-1"}),
        ),
    )
    .await;
    assert_retryable(
        &answer,
        StatusCode::SERVICE_UNAVAILABLE,
        "SERVICE_UNAVAILABLE",
        "recall",
    );

    let answer = call(
        &router,
        post(
            "/api/v1/messages/send",
            json!({
                "conversation_id": "group:1",
                "content": {"text": "hi"},
                "message_type": 1,
                "client_msg_id": "cmid-timeout-1",
            }),
        ),
    )
    .await;
    assert_retryable(
        &answer,
        StatusCode::SERVICE_UNAVAILABLE,
        "SERVICE_UNAVAILABLE",
        "send",
    );
    assert_eq!(answer.body["client_msg_id"], json!("cmid-timeout-1"));
    assert_eq!(
        answer.headers.get(CLIENT_MSG_ID_HEADER_NAME).unwrap(),
        "cmid-timeout-1"
    );

    let answer = call(&router, get("/api/v1/medias/file-info?file_id=f-1")).await;
    assert_retryable(
        &answer,
        StatusCode::GATEWAY_TIMEOUT,
        "MEDIA_TIMEOUT",
        "media file info",
    );
}

/// 客户端调用期间一直持锁，限时把等锁算在内：同一客户端上排队的几个请求各自按时返回，
/// 而不是一个接一个等满超时。
#[tokio::test]
async fn requests_queued_on_one_client_all_end_near_the_request_timeout() {
    let router = router(Some(REQUEST_TIMEOUT)).await;
    let started = Instant::now();
    let mut queued = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let router = router.clone();
        queued.spawn(async move { call(&router, get("/api/v1/presence/users/alice")).await });
    }
    while let Some(answer) = queued.join_next().await {
        assert_retryable(
            &answer.unwrap(),
            StatusCode::SERVICE_UNAVAILABLE,
            "SERVICE_UNAVAILABLE",
            "queued presence",
        );
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed < REQUEST_TIMEOUT * 3,
        "4 queued requests took {elapsed:?}: the lock wait is not bounded"
    );
}

/// 浏览器跨源调用发送接口：应答暴露全部头（`*`，来源 `*`、不带凭据时有效），前端能读到
/// `x-client-msg-id`。预检放行 `authorization` 与 `content-type`。
#[tokio::test]
async fn browsers_can_read_the_client_msg_id_header_cross_origin() {
    let router = router(Some(REQUEST_TIMEOUT)).await;
    let answer = call(
        &router,
        post(
            "/api/v1/messages/send",
            json!({
                "conversation_id": "group:1",
                "content": {"text": "hi"},
                "message_type": 1,
                "client_msg_id": "cmid-cors-1",
            }),
        ),
    )
    .await;
    assert_eq!(
        answer.headers.get(CLIENT_MSG_ID_HEADER_NAME).unwrap(),
        "cmid-cors-1"
    );
    assert_eq!(
        answer
            .headers
            .get("access-control-allow-origin")
            .expect("cross-origin response"),
        "*"
    );
    let exposed = answer
        .headers
        .get("access-control-expose-headers")
        .expect("exposed headers")
        .to_str()
        .unwrap()
        .to_ascii_lowercase();
    assert!(
        exposed == "*"
            || exposed
                .split(',')
                .any(|name| name.trim() == CLIENT_MSG_ID_HEADER_NAME),
        "x-client-msg-id is not readable by browsers: {exposed}"
    );

    let preflight = Request::builder()
        .method("OPTIONS")
        .uri("/api/v1/messages/send")
        .header("origin", "https://app.example.test")
        .header("access-control-request-method", "POST")
        .header(
            "access-control-request-headers",
            "authorization,content-type",
        )
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(preflight).await.unwrap();
    assert!(response.status().is_success(), "{}", response.status());
    let allowed = response
        .headers()
        .get("access-control-allow-headers")
        .expect("allowed headers")
        .to_str()
        .unwrap()
        .to_ascii_lowercase();
    assert!(
        allowed == "*" || (allowed.contains("authorization") && allowed.contains("content-type")),
        "{allowed}"
    );
}

const CLIENT_MSG_ID_HEADER_NAME: &str = "x-client-msg-id";

/// 以后新增的接口同样要限时：处理函数里每次取下游客户端锁都紧跟在 `.bounded(async {` 之后
/// （取锁算在请求超时之内）。
#[test]
fn every_downstream_client_lock_in_handlers_is_inside_bounded() {
    for (file, source) in [
        ("message_handler.rs", include_str!("message_handler.rs")),
        (
            "conversation_handler.rs",
            include_str!("conversation_handler.rs"),
        ),
        ("presence_handler.rs", include_str!("presence_handler.rs")),
        (
            "media_handler/files.rs",
            include_str!("media_handler/files.rs"),
        ),
        (
            "media_handler/processing.rs",
            include_str!("media_handler/processing.rs"),
        ),
        (
            "media_handler/references.rs",
            include_str!("media_handler/references.rs"),
        ),
        (
            "media_handler/uploads.rs",
            include_str!("media_handler/uploads.rs"),
        ),
    ] {
        let lines: Vec<&str> = source.lines().collect();
        let mut locks = 0;
        for (index, line) in lines.iter().enumerate() {
            if line.contains("clients.") && line.contains(".lock().await") {
                locks += 1;
                assert!(
                    index > 0 && lines[index - 1].trim_end().ends_with(".bounded(async {"),
                    "{file}:{}: downstream client used without the request timeout",
                    index + 1
                );
            }
        }
        assert!(
            locks > 0,
            "{file}: no downstream calls found; update this list"
        );
    }
}
