//! 设备推送令牌接口与未知路由兜底的契约测试。
//!
//! 替身 Push Proxy 挂真实的 `SignedContextLayer`（与 Push Proxy 进程相同的准入层），
//! 按真实 Push Proxy 的规则校验 User 身份、规范租户与 user_id 归属，并以签名租户为键存令牌；
//! 测试断言到达下游的身份只来自 IM 令牌主体。
use super::*;
use crate::interface::http::MessageContextSigner;
use crate::interface::http::PushTokenClient;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_grpc_proto::push::push_service_server::{PushService, PushServiceServer};
use flare_grpc_proto::push::*;
use flare_im_service_kit::clients::*;
use flare_im_service_kit::{
    gateway_auth::TenantRuntimeGuard,
    tenant_runtime::{TenantAccessPolicy, TenantRuntimeCache},
};
use flare_server_core::{
    AuthError, AuthenticatedPrincipal, SignedActorType, SignedContextClaims, SignedContextLayer,
    SignedContextSigner, SignedContextVerifier, TokenValidationRequest, TokenValidator,
    VerifiedContext,
};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Mutex as StdMutex;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tower::ServiceExt;

/// 令牌字符串 → 主体。`alice-a` / `alice-b` 是同名用户在两个租户下的令牌；
/// `alice-default` 带的是非规范写法的默认租户 `default`。
struct Validator;

#[async_trait::async_trait]
impl TokenValidator for Validator {
    async fn validate(
        &self,
        request: TokenValidationRequest,
    ) -> Result<AuthenticatedPrincipal, AuthError> {
        let (tenant, device, scopes): (Option<&str>, Option<&str>, Vec<String>) =
            match request.token.as_str() {
                "alice-a" => (Some("tenant-a"), None, vec![]),
                "alice-b" => (Some("tenant-b"), None, vec![]),
                "alice-default" => (Some("default"), None, vec![]),
                "bound-d1" => (Some("tenant-a"), Some("d1"), vec![]),
                "unbound" => (None, None, vec![]),
                "blank-tenant" => (Some(""), None, vec![]),
                "admin-scope" => (Some("tenant-a"), None, vec!["gateway:admin".into()]),
                _ => return Err(AuthError::InvalidToken("unknown test token".into())),
            };
        Ok(AuthenticatedPrincipal {
            user_id: "alice".into(),
            tenant_id: tenant.map(str::to_owned),
            device_id: device.map(str::to_owned),
            app_id: None,
            expires_at: None,
            scopes,
            metadata: Default::default(),
        })
    }
}

#[derive(Debug, Clone)]
struct Call {
    rpc: &'static str,
    claims: SignedContextClaims,
    user_id: String,
    device_id: String,
    platform: String,
    provider: i32,
    token: String,
}

type StoreKey = (String, String, i32, String);

/// 按真实 Push Proxy 的规则工作的替身：只接受 User 签名身份，签名租户必须是规范写法
/// （`validate_actor_claims`：合法且 `normalize_tenant_id(t) == t`），请求 user_id 必须等于签名用户，
/// 令牌按（签名租户, 用户, 通道, 设备）存取。`deny_tenant` 模拟下游拒绝某租户。
#[derive(Clone, Default)]
struct FakePushProxy {
    calls: Arc<StdMutex<Vec<Call>>>,
    store: Arc<StdMutex<HashMap<StoreKey, String>>>,
    deny_tenant: Option<&'static str>,
}

impl FakePushProxy {
    fn admit<T>(&self, request: &tonic::Request<T>) -> Result<SignedContextClaims, tonic::Status> {
        let ctx = flare_server_core::require_ctx_from_request(request)?;
        let claims = ctx
            .get_data::<VerifiedContext>()
            .and_then(|marker| marker.claims())
            .cloned()
            .ok_or_else(|| tonic::Status::unauthenticated("signed identity required"))?;
        if claims.actor_type != SignedActorType::User {
            return Err(tonic::Status::permission_denied("user actor required"));
        }
        if !flare_im_contracts::domain::tenant::is_valid_tenant_id(&claims.tenant_id)
            || flare_im_contracts::utils::normalize_tenant_id(&claims.tenant_id) != claims.tenant_id
        {
            return Err(tonic::Status::permission_denied(
                "canonical signed tenant_id is required",
            ));
        }
        if self.deny_tenant == Some(claims.tenant_id.as_str()) {
            return Err(tonic::Status::permission_denied("tenant not permitted"));
        }
        Ok(claims)
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }
}

#[tonic::async_trait]
impl PushService for FakePushProxy {
    async fn push_message(
        &self,
        _: tonic::Request<PushMessageRequest>,
    ) -> Result<tonic::Response<PushMessageResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
    }

    async fn push_notification(
        &self,
        _: tonic::Request<PushNotificationRequest>,
    ) -> Result<tonic::Response<PushNotificationResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
    }

    async fn push_custom(
        &self,
        _: tonic::Request<PushCustomRequest>,
    ) -> Result<tonic::Response<PushCustomResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
    }

    async fn register_device_push_token(
        &self,
        request: tonic::Request<RegisterDevicePushTokenRequest>,
    ) -> Result<tonic::Response<RegisterDevicePushTokenResponse>, tonic::Status> {
        let claims = self.admit(&request)?;
        let req = request.into_inner();
        if req.user_id != claims.actor_id {
            return Err(tonic::Status::permission_denied("user_id mismatch"));
        }
        self.store.lock().unwrap().insert(
            (
                claims.tenant_id.clone(),
                req.user_id.clone(),
                req.provider,
                req.device_id.clone(),
            ),
            req.token.clone(),
        );
        self.calls.lock().unwrap().push(Call {
            rpc: "register",
            claims,
            user_id: req.user_id,
            device_id: req.device_id,
            platform: req.platform,
            provider: req.provider,
            token: req.token,
        });
        Ok(tonic::Response::new(RegisterDevicePushTokenResponse {
            success: true,
        }))
    }

    async fn unregister_device_push_token(
        &self,
        request: tonic::Request<UnregisterDevicePushTokenRequest>,
    ) -> Result<tonic::Response<UnregisterDevicePushTokenResponse>, tonic::Status> {
        let claims = self.admit(&request)?;
        let req = request.into_inner();
        if req.user_id != claims.actor_id {
            return Err(tonic::Status::permission_denied("user_id mismatch"));
        }
        self.store.lock().unwrap().remove(&(
            claims.tenant_id.clone(),
            req.user_id.clone(),
            req.provider,
            req.device_id.clone(),
        ));
        self.calls.lock().unwrap().push(Call {
            rpc: "unregister",
            claims,
            user_id: req.user_id,
            device_id: req.device_id,
            platform: String::new(),
            provider: req.provider,
            token: String::new(),
        });
        Ok(tonic::Response::new(UnregisterDevicePushTokenResponse {
            success: true,
        }))
    }

    async fn query_push_status(
        &self,
        _: tonic::Request<QueryPushStatusRequest>,
    ) -> Result<tonic::Response<QueryPushStatusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
    }
}

fn cell_keys() -> (SignedContextSigner, SignedContextVerifier) {
    let key = SigningKey::from_bytes(&[41; 32]);
    let private = key.to_pkcs8_pem(Default::default()).unwrap();
    let public = key
        .verifying_key()
        .to_public_key_pem(Default::default())
        .unwrap();
    (
        SignedContextSigner::from_private_key(&private).unwrap(),
        SignedContextVerifier::from_public_key(&public).unwrap(),
    )
}

async fn start_push_proxy(fake: FakePushProxy, verifier: SignedContextVerifier) -> Channel {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = SignedContextLayer::new(verifier).layer(PushServiceServer::new(fake));
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
    );
    Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_lazy()
}

/// 已绑定后立即释放的本机端口：连接必然被拒，模拟 Push Proxy 不可达。
async fn closed_port_channel() -> Channel {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_lazy()
}

fn router_with(push: Channel, signer: Option<SignedContextSigner>) -> Router {
    // 其它下游的惰性通道在这些用例里从不连接。
    let c = Channel::from_static("http://127.0.0.1:9").connect_lazy();
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
        request_timeout: None,
    };
    create_public_router(Arc::new(clients))
        .layer(axum::Extension(
            Arc::new(Validator) as Arc<dyn TokenValidator>
        ))
        .layer(axum::Extension(TenantRuntimeGuard::new(
            TenantRuntimeCache::disabled(),
            TenantAccessPolicy::Lenient,
        )))
        .layer(axum::Extension(MessageContextSigner(signer)))
        .layer(axum::Extension(PushTokenClient::from_channel(push)))
}

async fn signed_router(fake: FakePushProxy) -> Router {
    let (signer, verifier) = cell_keys();
    router_with(start_push_proxy(fake, verifier).await, Some(signer))
}

fn with_bearer(
    builder: axum::http::request::Builder,
    token: Option<&str>,
) -> axum::http::request::Builder {
    match token {
        Some(token) => builder.header("authorization", format!("Bearer {token}")),
        None => builder,
    }
}

fn put_request(token: Option<&str>, device_id: &str, body: Value) -> Request<Body> {
    with_bearer(
        Request::builder()
            .method("PUT")
            .uri(format!("/api/v1/devices/{device_id}/push-token"))
            .header("content-type", "application/json"),
        token,
    )
    .body(Body::from(body.to_string()))
    .unwrap()
}

fn delete_request(token: Option<&str>, device_id: &str, query: &str) -> Request<Body> {
    with_bearer(
        Request::builder()
            .method("DELETE")
            .uri(format!("/api/v1/devices/{device_id}/push-token?{query}")),
        token,
    )
    .body(Body::empty())
    .unwrap()
}

fn registration() -> Value {
    json!({"platform": "ios", "provider": "getui", "token": "tok-1"})
}

async fn send(router: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json = serde_json::from_slice(&body).unwrap_or_else(|_| {
        panic!(
            "{status} body is not JSON: {:?}",
            String::from_utf8_lossy(&body)
        )
    });
    (status, json)
}

fn assert_denied(result: &(StatusCode, Value), status: StatusCode, reason: &str) {
    assert_eq!(result.0, status, "{}", result.1);
    assert_eq!(result.1["code"], json!(status.as_u16()), "{}", result.1);
    assert_eq!(result.1["reason"], json!(reason), "{}", result.1);
}

#[tokio::test]
async fn register_and_unregister_reach_push_proxy_with_token_identity_only() {
    let fake = FakePushProxy::default();
    let router = signed_router(fake.clone()).await;

    let mut request = put_request(Some("alice-a"), "d1", registration());
    // 伪造的身份头不能改写签名身份。
    for (name, value) in [
        ("x-tenant-id", "tenant-b"),
        ("x-user-id", "mallory"),
        ("x-actor-type", "tenant_admin"),
    ] {
        request.headers_mut().insert(name, value.parse().unwrap());
    }
    let (status, body) = send(&router, request).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, json!({"code": 0, "data": {"success": true}}));

    let calls = fake.calls();
    assert_eq!(calls.len(), 1);
    let call = &calls[0];
    assert_eq!(call.rpc, "register");
    assert_eq!(call.claims.actor_type, SignedActorType::User);
    assert_eq!(call.claims.tenant_id, "tenant-a");
    assert_eq!(call.claims.actor_id, "alice");
    assert_eq!(call.claims.credential_id.as_deref(), Some("api-gateway"));
    assert_eq!(call.user_id, "alice");
    assert_eq!(call.device_id, "d1");
    assert_eq!(call.platform, "ios");
    assert_eq!(call.provider, DevicePushProvider::Getui as i32);
    assert_eq!(call.token, "tok-1");
    assert_eq!(fake.store.lock().unwrap().len(), 1);

    let (status, body) = send(
        &router,
        delete_request(Some("alice-a"), "d1", "provider=getui"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, json!({"code": 0, "data": {"success": true}}));
    let calls = fake.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].rpc, "unregister");
    assert_eq!(calls[1].claims.tenant_id, "tenant-a");
    assert_eq!(calls[1].user_id, "alice");
    assert_eq!(calls[1].device_id, "d1");
    assert!(fake.store.lock().unwrap().is_empty());
}

#[tokio::test]
async fn missing_or_malformed_bearer_is_401_envelope_before_push_proxy() {
    let fake = FakePushProxy::default();
    let router = signed_router(fake.clone()).await;

    let missing = send(&router, put_request(None, "d1", registration())).await;
    assert_denied(&missing, StatusCode::UNAUTHORIZED, "UNAUTHORIZED");
    let missing = send(&router, delete_request(None, "d1", "provider=getui")).await;
    assert_denied(&missing, StatusCode::UNAUTHORIZED, "UNAUTHORIZED");

    let mut basic = put_request(None, "d1", registration());
    basic
        .headers_mut()
        .insert("authorization", "Basic YWxpY2U6cHc=".parse().unwrap());
    assert_denied(
        &send(&router, basic).await,
        StatusCode::UNAUTHORIZED,
        "UNAUTHORIZED",
    );
    let unknown = send(&router, put_request(Some("forged"), "d1", registration())).await;
    assert_denied(&unknown, StatusCode::UNAUTHORIZED, "UNAUTHORIZED");

    assert!(fake.calls().is_empty());
}

#[tokio::test]
async fn caller_cannot_name_another_tenant_or_user() {
    let fake = FakePushProxy::default();
    let router = signed_router(fake.clone()).await;

    // 请求体/查询串里的身份字段一律拒收，而不是静默忽略。
    for extra in [
        json!({"tenantId": "tenant-b"}),
        json!({"userId": "bob"}),
        json!({"tenant_id": "tenant-b"}),
    ] {
        let mut body = registration();
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let result = send(&router, put_request(Some("alice-a"), "d1", body)).await;
        assert_denied(&result, StatusCode::BAD_REQUEST, "INVALID_REQUEST_BODY");
    }
    for query in [
        "provider=getui&tenantId=tenant-b",
        "provider=getui&userId=bob",
    ] {
        let result = send(&router, delete_request(Some("alice-a"), "d1", query)).await;
        assert_denied(&result, StatusCode::BAD_REQUEST, "INVALID_QUERY");
    }

    // 未绑定租户（或绑定为空串）的令牌带伪造租户头：发 RPC 前拒绝，空租户不能被规范化成租户 0。
    for token in ["unbound", "blank-tenant"] {
        let mut request = put_request(Some(token), "d1", registration());
        request
            .headers_mut()
            .insert("x-tenant-id", "tenant-a".parse().unwrap());
        let result = send(&router, request).await;
        assert_denied(&result, StatusCode::FORBIDDEN, "TENANT_REQUIRED");
    }
    assert!(fake.calls().is_empty());

    // 同一用户名在租户 B 的令牌只能作用于租户 B：租户 A 的登记不受影响。
    let (status, _) = send(&router, put_request(Some("alice-a"), "d1", registration())).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &router,
        delete_request(Some("alice-b"), "d1", "provider=getui"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let calls = fake.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].claims.tenant_id, "tenant-b");
    let store = fake.store.lock().unwrap();
    assert_eq!(store.len(), 1);
    assert!(store.contains_key(&(
        "tenant-a".to_string(),
        "alice".to_string(),
        DevicePushProvider::Getui as i32,
        "d1".to_string()
    )));
}

#[tokio::test]
async fn default_tenant_alias_is_signed_as_the_canonical_tenant() {
    // Push Proxy 只认规范租户：令牌里的 `default` 必须以 `0` 签发，否则真实下游回 403。
    let fake = FakePushProxy::default();
    let router = signed_router(fake.clone()).await;
    let (status, body) = send(
        &router,
        put_request(Some("alice-default"), "d1", registration()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = send(
        &router,
        delete_request(Some("alice-default"), "d1", "provider=getui"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let calls = fake.calls();
    assert_eq!(calls.len(), 2);
    for call in &calls {
        assert_eq!(call.claims.tenant_id, "0", "{}", call.rpc);
        assert_eq!(call.user_id, "alice");
    }
    assert!(fake.store.lock().unwrap().is_empty());
}

#[tokio::test]
async fn push_proxy_tenant_denial_is_a_403_envelope() {
    let fake = FakePushProxy {
        deny_tenant: Some("tenant-b"),
        ..Default::default()
    };
    let router = signed_router(fake.clone()).await;
    let result = send(&router, put_request(Some("alice-b"), "d1", registration())).await;
    assert_denied(&result, StatusCode::FORBIDDEN, "PERMISSION_DENIED");
    let result = send(
        &router,
        delete_request(Some("alice-b"), "d1", "provider=getui"),
    )
    .await;
    assert_denied(&result, StatusCode::FORBIDDEN, "PERMISSION_DENIED");
    assert!(fake.store.lock().unwrap().is_empty());
}

#[tokio::test]
async fn admin_unsigned_foreign_device_and_invalid_input_never_reach_push_proxy() {
    let fake = FakePushProxy::default();
    let router = signed_router(fake.clone()).await;

    let admin = send(
        &router,
        put_request(Some("admin-scope"), "d1", registration()),
    )
    .await;
    assert_denied(&admin, StatusCode::FORBIDDEN, "USER_REQUIRED");

    // 绑定 d1 的令牌不能改 d2 的推送令牌。
    let foreign = send(&router, put_request(Some("bound-d1"), "d2", registration())).await;
    assert_denied(&foreign, StatusCode::FORBIDDEN, "DEVICE_MISMATCH");
    let foreign = send(
        &router,
        delete_request(Some("bound-d1"), "d2", "provider=getui"),
    )
    .await;
    assert_denied(&foreign, StatusCode::FORBIDDEN, "DEVICE_MISMATCH");

    let bad_provider = send(
        &router,
        put_request(
            Some("alice-a"),
            "d1",
            json!({"platform": "ios", "provider": "apns", "token": "tok-1"}),
        ),
    )
    .await;
    assert_denied(
        &bad_provider,
        StatusCode::BAD_REQUEST,
        "PUSH_PROVIDER_INVALID",
    );
    let missing_provider = send(&router, delete_request(Some("alice-a"), "d1", "")).await;
    assert_denied(&missing_provider, StatusCode::BAD_REQUEST, "INVALID_QUERY");
    let bad_token = send(
        &router,
        put_request(
            Some("alice-a"),
            "d1",
            json!({"platform": "ios", "provider": "getui", "token": "  "}),
        ),
    )
    .await;
    assert_denied(&bad_token, StatusCode::BAD_REQUEST, "PUSH_TOKEN_INVALID");
    assert!(fake.calls().is_empty());

    let (_, verifier) = cell_keys();
    let unsigned = router_with(start_push_proxy(fake.clone(), verifier).await, None);
    let result = send(
        &unsigned,
        put_request(Some("alice-a"), "d1", registration()),
    )
    .await;
    assert_denied(
        &result,
        StatusCode::INTERNAL_SERVER_ERROR,
        "PUSH_SIGNER_UNAVAILABLE",
    );
    assert!(fake.calls().is_empty());

    // 设备绑定一致时照常放行。
    let (status, body) = send(&router, put_request(Some("bound-d1"), "d1", registration())).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(fake.calls().len(), 1);
}

#[tokio::test]
async fn unreachable_push_proxy_is_a_503_envelope() {
    let (signer, _) = cell_keys();
    let router = router_with(closed_port_channel().await, Some(signer));
    let result = send(&router, put_request(Some("alice-a"), "d1", registration())).await;
    assert_denied(
        &result,
        StatusCode::SERVICE_UNAVAILABLE,
        "UPSTREAM_UNAVAILABLE",
    );
}

#[tokio::test]
async fn unknown_routes_answer_with_the_core_not_found_envelope() {
    let router = signed_router(FakePushProxy::default()).await;
    for (method, path, token) in [
        ("GET", "/no-such-route", None),
        ("GET", "/api/v1/no-such-group", Some("alice-a")),
        ("GET", "/api/v1/medias/objects", Some("alice-a")),
        ("POST", "/api/v1/medias/cleanup-orphaned-assets", None),
        ("GET", "/api/v1/devices/d1/unknown", Some("alice-a")),
        ("PUT", "/api/v1/devices/d1", Some("alice-a")),
    ] {
        let response = router
            .clone()
            .oneshot(
                with_bearer(Request::builder().method(method).uri(path), token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {path}");
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("application/json"),
            "{method} {path}"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body)
            .unwrap_or_else(|_| panic!("{method} {path}: empty or non-JSON 404 body"));
        assert_eq!(json["code"], json!(404), "{method} {path}: {json}");
        assert_eq!(
            json["reason"],
            json!("NOT_FOUND"),
            "{method} {path}: {json}"
        );
        assert!(json.get("data").is_none(), "{method} {path}: {json}");
    }
}

#[tokio::test]
async fn openapi_documents_the_push_token_routes() {
    let spec = serde_json::to_value(ApiDoc::openapi()).unwrap();
    let path = &spec["paths"]["/api/v1/devices/{device_id}/push-token"];
    assert!(path["put"].is_object(), "{path}");
    assert!(path["delete"].is_object(), "{path}");
    let schemas = &spec["components"]["schemas"];
    assert!(schemas["RegisterPushTokenHttpRequest"].is_object());
    assert!(schemas["PushTokenHttpResponse"].is_object());
}
