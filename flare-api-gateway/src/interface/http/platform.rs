//! API Key partition: local primary credentials, strict runtime admission, Service actor.
//! No Control dependency, Social rules, end-user impersonation or business-table SQL.
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{ConnectInfo, Extension, Path, Query, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use flare_grpc_proto::conversation::{
    ListConversationParticipantsRequest, ListConversationParticipantsResponse,
    conversation_read_service_client::ConversationReadServiceClient,
};
use flare_im_service_kit::{
    credential_projection::{PostgresCredentialReader, connect_readonly_credential_pool},
    tenant_runtime::{TenantLookup, TenantRuntimeCache},
};
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner,
    auth::{
        api_key::ApiKeyPepper,
        api_key_policy::AuthorizedApiKey,
        circuit_breaker::{CircuitBreaker, Outcome},
        credential_budget::CredentialRequestBudget,
        credential_verifier::{CachedApiKeyVerifier, CredentialAuthError},
        trusted_proxy::TrustedProxyChain,
    },
};
use serde::{Deserialize, Serialize};
use std::{net::IpAddr, sync::Arc, time::Duration};
use tokio::time::timeout;
use tonic::transport::{Channel, Endpoint};

const SCOPE: &str = "platform.conversations.read";
const RPC_TIMEOUT: Duration = Duration::from_secs(2);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

pub struct PlatformConfig {
    pepper: ApiKeyPepper,
    proxies: TrustedProxyChain,
}
impl PlatformConfig {
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        fn optional(name: &str) -> anyhow::Result<Option<String>> {
            match std::env::var(name) {
                Ok(value) => Ok(Some(value)),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(_) => anyhow::bail!("invalid Platform environment configuration"),
            }
        }
        Self::parse(
            &optional("FLARE_PLATFORM_API_ENABLED")?.unwrap_or_else(|| "false".into()),
            optional("FLARE_API_KEY_PEPPER")?.as_deref(),
            &optional("FLARE_PLATFORM_TRUSTED_PROXY_CIDRS")?.unwrap_or_default(),
        )
    }
    fn parse(enabled: &str, pepper: Option<&str>, proxies: &str) -> anyhow::Result<Option<Self>> {
        match enabled {
            "false" => return Ok(None),
            "true" => {}
            _ => anyhow::bail!("invalid FLARE_PLATFORM_API_ENABLED"),
        }
        let pepper = ApiKeyPepper::from_base64url(pepper.unwrap_or(""))
            .map_err(|_| anyhow::anyhow!("invalid or missing FLARE_API_KEY_PEPPER"))?;
        let cidrs = if proxies.is_empty() {
            vec![]
        } else {
            proxies.split(',').map(|v| v.trim().to_owned()).collect()
        };
        let proxies = TrustedProxyChain::new(&cidrs)
            .map_err(|_| anyhow::anyhow!("invalid FLARE_PLATFORM_TRUSTED_PROXY_CIDRS"))?;
        Ok(Some(Self { pepper, proxies }))
    }
    pub async fn router(
        self,
        primary_url: &str,
        conversation_url: &str,
        signer: Arc<SignedContextSigner>,
    ) -> anyhow::Result<Router> {
        // Independent from client pools/channels/circuit/rate limits. Never a replica.
        let pool = connect_readonly_credential_pool(primary_url)
            .await
            .map_err(anyhow::Error::msg)?;
        let state = Arc::new(PlatformState {
            verifier: CachedApiKeyVerifier::new(Arc::new(PostgresCredentialReader::new(
                pool.clone(),
            ))),
            gate: TenantRuntimeCache::from_pool(pool),
            pepper: self.pepper,
            proxies: self.proxies,
            conversation: channel(conversation_url)?,
            signer,
            budget: CredentialRequestBudget::new(32),
            breaker: CircuitBreaker::new(
                "Platform Core",
                5,
                Duration::from_secs(2),
                Duration::from_millis(250),
            ),
        });
        Ok(router(state))
    }
}

fn channel(url: &str) -> anyhow::Result<Channel> {
    let uri = url
        .parse::<axum::http::Uri>()
        .map_err(|_| anyhow::anyhow!("invalid Platform conversation endpoint"))?;
    if !matches!(uri.scheme_str(), Some("http" | "https"))
        || uri
            .authority()
            .is_none_or(|authority| authority.as_str().contains('@'))
        || uri
            .path_and_query()
            .is_some_and(|path| path.as_str() != "/")
    {
        anyhow::bail!("Platform conversation endpoint must be a direct private HTTP(S) authority");
    }
    Ok(Endpoint::from_shared(url.to_owned())
        .map_err(|_| anyhow::anyhow!("invalid Platform conversation endpoint"))?
        .connect_timeout(Duration::from_millis(250))
        .timeout(RPC_TIMEOUT)
        .concurrency_limit(32)
        .buffer_size(32)
        .connect_lazy())
}
struct PlatformState {
    verifier: CachedApiKeyVerifier,
    gate: TenantRuntimeCache,
    pepper: ApiKeyPepper,
    proxies: TrustedProxyChain,
    conversation: Channel,
    signer: Arc<SignedContextSigner>,
    budget: CredentialRequestBudget,
    breaker: CircuitBreaker,
}
#[derive(Clone, Copy)]
struct SourceIp(IpAddr);
fn failure(status: StatusCode, code: &'static str) -> Response {
    let mut response = (status, Json(serde_json::json!({"error":{"code":code}}))).into_response();
    if matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
    ) {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, "1".parse().unwrap());
    }
    response
}
fn invalid() -> Response {
    failure(StatusCode::BAD_REQUEST, "INVALID_REQUEST")
}
fn unavailable() -> Response {
    failure(StatusCode::SERVICE_UNAVAILABLE, "PLATFORM_UNAVAILABLE")
}
fn single_header<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, ()> {
    let mut values = headers.get_all(name).iter();
    let first = values.next();
    if values.next().is_some() {
        return Err(());
    }
    first.map(|v| v.to_str().map_err(|_| ())).transpose()
}
fn router(state: Arc<PlatformState>) -> Router {
    let routes = Router::new()
        .route(
            "/apps/{app_id}/conversations/{conversation_id}/participants",
            get(participants),
        )
        .fallback(|| async { failure(StatusCode::NOT_FOUND, "NOT_FOUND") })
        .method_not_allowed_fallback(|| async {
            failure(StatusCode::METHOD_NOT_ALLOWED, "METHOD_NOT_ALLOWED")
        })
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(state, boundary));
    Router::new().nest("/platform/v1", routes)
}
async fn boundary(
    State(state): State<Arc<PlatformState>>,
    mut request: Request,
    next: Next,
) -> Response {
    let response = async {
        let Some(peer) = request
            .extensions()
            .get::<ConnectInfo<std::net::SocketAddr>>()
        else {
            return invalid();
        };
        let forwarded = match single_header(request.headers(), "x-forwarded-for") {
            Ok(value) => value,
            Err(_) => return invalid(),
        };
        let ip = match state.proxies.resolve(peer.0.ip(), forwarded) {
            Ok(ip) => ip,
            Err(_) => return invalid(),
        };
        if !state.budget.take_ip(ip) {
            return failure(StatusCode::TOO_MANY_REQUESTS, "PLATFORM_RATE_LIMITED");
        }
        let Ok(_permit) = state.budget.try_enter() else {
            return failure(StatusCode::SERVICE_UNAVAILABLE, "PLATFORM_OVERLOADED");
        };
        request.extensions_mut().insert(SourceIp(ip));
        match timeout(REQUEST_TIMEOUT, async {
            if request.uri().query().is_some_and(|q| q.len() > 2048) {
                return invalid();
            }
            let (parts, body) = request.into_parts();
            match to_bytes(body, 16 * 1024).await {
                Ok(body) if body.is_empty() => {
                    next.run(Request::from_parts(parts, Body::empty())).await
                }
                _ => invalid(),
            }
        })
        .await
        {
            Ok(response) => response,
            Err(_) => failure(StatusCode::GATEWAY_TIMEOUT, "PLATFORM_TIMEOUT"),
        }
    }
    .await;
    let mut response = response;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
        .headers_mut()
        .insert(header::PRAGMA, "no-cache".parse().unwrap());
    response
}
fn identifier(value: &str, app: bool) -> bool {
    !value.is_empty()
        && value.len() <= if app { 32 } else { 128 }
        && value.bytes().all(|b| {
            b.is_ascii_alphanumeric()
                || matches!(b, b'_' | b'-')
                || (!app && matches!(b, b'.' | b':' | b'@'))
        })
        && (!app || value == flare_im_contracts::utils::normalize_tenant_id(value))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ParticipantsQuery {
    #[serde(default = "default_limit")]
    limit: i32,
    #[serde(default)]
    cursor: String,
}
fn default_limit() -> i32 {
    50
}
fn cursor_valid(raw: &str) -> bool {
    if raw.is_empty() {
        return true;
    }
    if raw.len() > 512 || raw.trim() != raw || raw.chars().any(char::is_control) {
        return false;
    }
    let Some(rest) = raw.strip_prefix("k:") else {
        return false;
    };
    let mut parts = rest.splitn(4, ':');
    let mut numbers = [0; 3];
    for number in &mut numbers {
        let Some(raw) = parts.next() else {
            return false;
        };
        let Ok(value) = raw.parse::<i64>() else {
            return false;
        };
        if value < 0 || value.to_string() != raw {
            return false;
        }
        *number = value;
    }
    numbers[0] > 0
        && chrono::DateTime::from_timestamp_micros(numbers[0]).is_some()
        && parts
            .next()
            .is_some_and(|user| !user.is_empty() && user.len() <= 128 && user.trim() == user)
}
async fn authorize(
    state: &PlatformState,
    headers: &HeaderMap,
    app: &str,
    ip: IpAddr,
) -> Result<AuthorizedApiKey, Response> {
    let encoded = single_header(headers, "authorization")
        .ok()
        .flatten()
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|token| token.len() <= 256)
        .ok_or_else(|| failure(StatusCode::UNAUTHORIZED, "API_KEY_UNAUTHENTICATED"))?;
    let key = state
        .verifier
        .verify(encoded, &state.pepper, Some(app), SCOPE, ip)
        .await
        .map_err(|e| match e {
            CredentialAuthError::Unauthenticated => {
                failure(StatusCode::UNAUTHORIZED, "API_KEY_UNAUTHENTICATED")
            }
            CredentialAuthError::Forbidden => failure(StatusCode::FORBIDDEN, "API_KEY_FORBIDDEN"),
            CredentialAuthError::Unavailable => unavailable(),
        })?;
    if !state.budget.take_key(&key.key_id) {
        return Err(failure(
            StatusCode::TOO_MANY_REQUESTS,
            "PLATFORM_RATE_LIMITED",
        ));
    }
    match timeout(Duration::from_millis(250), state.gate.lookup(&key.app_id)).await {
        Ok(TenantLookup::Known(snapshot)) if snapshot.status.admits_connections() => Ok(key),
        Ok(TenantLookup::Known(_) | TenantLookup::Unknown) => {
            Err(failure(StatusCode::FORBIDDEN, "TENANT_INACTIVE"))
        }
        _ => Err(unavailable()),
    }
}
#[derive(Serialize)]
struct ParticipantData {
    user_id: String,
    roles: Vec<String>,
}
fn page_response(
    conversation: &str,
    limit: i32,
    page: ListConversationParticipantsResponse,
) -> Response {
    if page.conversation_id != conversation
        || page.participants.len() > limit as usize
        || !cursor_valid(&page.next_cursor)
        || page.has_more == page.next_cursor.is_empty()
    {
        return failure(StatusCode::BAD_GATEWAY, "UPSTREAM_INVALID_RESPONSE");
    }
    let participants: Vec<_> = page
        .participants
        .into_iter()
        .map(|p| ParticipantData {
            user_id: p.user_id,
            roles: p.roles,
        })
        .collect();
    Json(serde_json::json!({"data":{"conversation_id":conversation,"participants":participants,"next_cursor":page.next_cursor,"has_more":page.has_more}})).into_response()
}
async fn participants(
    State(state): State<Arc<PlatformState>>,
    headers: HeaderMap,
    Extension(SourceIp(ip)): Extension<SourceIp>,
    path: Result<Path<(String, String)>, axum::extract::rejection::PathRejection>,
    query: Result<Query<ParticipantsQuery>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let (Ok(Path((app, conversation))), Ok(Query(query))) = (path, query) else {
        return invalid();
    };
    if !identifier(&app, true)
        || !identifier(&conversation, false)
        || !(1..=100).contains(&query.limit)
        || !cursor_valid(&query.cursor)
    {
        return invalid();
    }
    let key = match authorize(&state, &headers, &app, ip).await {
        Ok(key) => key,
        Err(response) => return response,
    };
    let ticket = match state.breaker.try_admit() {
        Ok(ticket) => ticket,
        Err(_) => return failure(StatusCode::SERVICE_UNAVAILABLE, "UPSTREAM_UNAVAILABLE"),
    };
    let identity = SignedContextIdentity::new(
        SignedActorType::Service,
        key.service_account_id,
        key.app_id,
        uuid::Uuid::new_v4().to_string(),
    )
    .with_credential_id(key.key_id)
    .with_roles(vec![SCOPE.into()]);
    let token = match state.signer.sign(&identity) {
        Ok(token) => token,
        Err(_) => return unavailable(),
    };
    let mut request = tonic::Request::new(ListConversationParticipantsRequest {
        conversation_id: conversation.clone(),
        cursor: query.cursor,
        limit: query.limit,
        include_removed: false,
        ext: [("skip_meta".into(), "1".into())].into_iter().collect(),
    });
    if flare_server_core::set_signed_context_token(&mut request, &token).is_err() {
        return unavailable();
    }
    request.set_timeout(RPC_TIMEOUT);
    let result = timeout(
        RPC_TIMEOUT,
        ConversationReadServiceClient::new(state.conversation.clone())
            .max_decoding_message_size(1024 * 1024)
            .list_conversation_participants(request),
    )
    .await;
    match result {
        Ok(Ok(response)) => {
            let response = page_response(&conversation, query.limit, response.into_inner());
            ticket.settle(if response.status().is_success() {
                Outcome::Success
            } else {
                Outcome::Failure
            });
            response
        }
        Ok(Err(status))
            if matches!(
                status.code(),
                tonic::Code::PermissionDenied | tonic::Code::Unauthenticated
            ) =>
        {
            ticket.settle(Outcome::Success);
            failure(StatusCode::FORBIDDEN, "API_KEY_FORBIDDEN")
        }
        Ok(Err(status)) if status.code() == tonic::Code::NotFound => {
            ticket.settle(Outcome::Success);
            failure(StatusCode::NOT_FOUND, "NOT_FOUND")
        }
        _ => {
            ticket.settle(Outcome::Failure);
            failure(StatusCode::SERVICE_UNAVAILABLE, "UPSTREAM_UNAVAILABLE")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use ed25519_dalek::{SigningKey, pkcs8::EncodePrivateKey};
    use flare_im_contracts::domain::tenant::{TenantQuota, TenantRuntimeSnapshot, TenantStatus};
    use flare_im_service_kit::tenant_runtime::{TenantRuntimeCacheOptions, TenantRuntimeSource};
    use flare_server_core::auth::{
        api_key::{ApiKeyKind, ApiKeyToken},
        credential_snapshot::CredentialKeyRecord,
        credential_verifier::{CredentialReadError, LocalCredential, LocalCredentialReader},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower::ServiceExt;
    const PATH: &str = "/platform/v1/apps/app-a/conversations/same-conversation/participants";
    struct Reader {
        value: LocalCredential,
        fail: bool,
        calls: AtomicUsize,
    }
    #[async_trait]
    impl LocalCredentialReader for Reader {
        async fn lookup(&self, _: &str) -> Result<Option<LocalCredential>, CredentialReadError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(CredentialReadError::Unavailable)
            } else {
                Ok(Some(self.value.clone()))
            }
        }
    }
    struct Runtime {
        status: Option<TenantStatus>,
        fail: bool,
    }
    #[async_trait]
    impl TenantRuntimeSource for Runtime {
        async fn load(&self, app: &str) -> Result<Option<TenantRuntimeSnapshot>, String> {
            if self.fail {
                return Err("private database detail".into());
            }
            Ok(self.status.map(|status| TenantRuntimeSnapshot {
                tenant_id: app.into(),
                status,
                quota: TenantQuota::default(),
                version: 1,
            }))
        }
    }
    fn state(
        scope: &str,
        status: Option<TenantStatus>,
        db_fail: bool,
        gate_fail: bool,
    ) -> (Arc<PlatformState>, String, Arc<Reader>) {
        let pepper =
            ApiKeyPepper::from_base64url("KioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKio").unwrap();
        let token = ApiKeyToken::generate(ApiKeyKind::Live).unwrap();
        let record = CredentialKeyRecord {
            key_id: token.key_id().into(),
            service_account_id: "sa-native".into(),
            mode: 1,
            hash: token.digest(&pepper),
            pepper_fingerprint: pepper.fingerprint(),
            scopes: vec![scope.into()],
            expires_at_ms: 0,
            ip_allow: vec!["192.0.2.0/24".into()],
            revoked: false,
            service_account_enabled: true,
        };
        let reader = Arc::new(Reader {
            value: LocalCredential::new(&record.key_id.clone(), "app-a".into(), 1, record).unwrap(),
            fail: db_fail,
            calls: AtomicUsize::new(0),
        });
        let signing = SigningKey::from_bytes(&[11; 32])
            .to_pkcs8_pem(Default::default())
            .unwrap();
        let state = Arc::new(PlatformState {
            verifier: CachedApiKeyVerifier::new(reader.clone()),
            gate: TenantRuntimeCache::with_source(
                Arc::new(Runtime {
                    status,
                    fail: gate_fail,
                }),
                TenantRuntimeCacheOptions::default(),
            ),
            pepper,
            proxies: TrustedProxyChain::default(),
            conversation: channel("http://127.0.0.1:1").unwrap(),
            signer: Arc::new(SignedContextSigner::from_private_key(&signing).unwrap()),
            budget: CredentialRequestBudget::new(1),
            breaker: CircuitBreaker::new(
                "Platform test",
                2,
                Duration::from_secs(2),
                Duration::from_millis(250),
            ),
        });
        (state, token.into_issuance_token().to_string(), reader)
    }
    fn request(path: &str, token: Option<&str>) -> Request {
        let mut builder = axum::http::Request::builder().uri(path);
        if let Some(token) = token {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        let mut request = builder.body(Body::empty()).unwrap();
        request.extensions_mut().insert(ConnectInfo(
            "192.0.2.1:54321".parse::<std::net::SocketAddr>().unwrap(),
        ));
        request
    }
    async fn check(response: Response, status: StatusCode, code: &str) {
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(response.headers()[header::PRAGMA], "no-cache");
        let json: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(json, serde_json::json!({"error":{"code":code}}));
    }
    #[test]
    fn strict_inputs_do_not_reenter_legacy_offset_or_default_tenant() {
        for app in ["", "default", " app-a", "app:a", "app/a"] {
            assert!(!identifier(app, true));
        }
        assert!(identifier("app-a", true));
        assert!(identifier("group:uuid-a", false));
        for cursor in [
            "0",
            "99999999999999",
            "k:1:0:0:",
            "k:-1:0:0:u",
            "k:1:-1:0:u",
            "k:1:0:0:u\n",
            "k:01:0:0:u",
            "k:1:0:9223372036854775808:u",
        ] {
            assert!(!cursor_valid(cursor), "{cursor}");
        }
        assert!(cursor_valid(""));
        assert!(cursor_valid("k:1700000000000000:0:0:u:colon"));
        assert!(PlatformConfig::parse("false", None, "*").unwrap().is_none());
        for enabled in ["yes", "TRUE", ""] {
            assert!(PlatformConfig::parse(enabled, None, "").is_err());
        }
        assert!(PlatformConfig::parse("true", None, "").is_err());
        assert!(
            PlatformConfig::parse(
                "true",
                Some("KioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKio"),
                "0.0.0.0/0"
            )
            .is_err()
        );
    }
    #[tokio::test]
    async fn jwt_duplicate_and_caller_headers_never_reach_credential_store() {
        for bad in [
            "discovery://conversation",
            "http://user:secret@localhost:80",
            "http://localhost:80/private?token=secret",
        ] {
            assert!(channel(bad).is_err());
        }
        let (state, _, reader) = state(SCOPE, Some(TenantStatus::Active), false, false);
        let app = router(state);
        for token in [None, Some("user.jwt.token"), Some("staff.jwt.token")] {
            let mut req = request(PATH, token);
            req.headers_mut()
                .insert("x-tenant-id", "app-a".parse().unwrap());
            req.headers_mut()
                .insert("x-user-id", "superAdmin".parse().unwrap());
            check(
                app.clone().oneshot(req).await.unwrap(),
                StatusCode::UNAUTHORIZED,
                "API_KEY_UNAUTHENTICATED",
            )
            .await;
        }
        let mut req = request(PATH, Some("anything"));
        req.headers_mut()
            .append(header::AUTHORIZATION, "Bearer anything".parse().unwrap());
        check(
            app.oneshot(req).await.unwrap(),
            StatusCode::UNAUTHORIZED,
            "API_KEY_UNAUTHENTICATED",
        )
        .await;
        assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn app_scope_source_and_runtime_are_independent_gates() {
        for (scope, status, db_fail, gate_fail, status_code, code) in [
            (
                "platform.users.read",
                Some(TenantStatus::Active),
                false,
                false,
                StatusCode::FORBIDDEN,
                "API_KEY_FORBIDDEN",
            ),
            (
                SCOPE,
                None,
                false,
                false,
                StatusCode::FORBIDDEN,
                "TENANT_INACTIVE",
            ),
            (
                SCOPE,
                Some(TenantStatus::Suspended),
                false,
                false,
                StatusCode::FORBIDDEN,
                "TENANT_INACTIVE",
            ),
            (
                SCOPE,
                Some(TenantStatus::Deleting),
                false,
                false,
                StatusCode::FORBIDDEN,
                "TENANT_INACTIVE",
            ),
            (
                SCOPE,
                Some(TenantStatus::Active),
                true,
                false,
                StatusCode::SERVICE_UNAVAILABLE,
                "PLATFORM_UNAVAILABLE",
            ),
            (
                SCOPE,
                Some(TenantStatus::Active),
                false,
                true,
                StatusCode::SERVICE_UNAVAILABLE,
                "PLATFORM_UNAVAILABLE",
            ),
        ] {
            let (state, token, _) = state(scope, status, db_fail, gate_fail);
            check(
                router(state)
                    .oneshot(request(PATH, Some(&token)))
                    .await
                    .unwrap(),
                status_code,
                code,
            )
            .await;
        }
        let (state, token, _) = state(SCOPE, Some(TenantStatus::Active), false, false);
        check(
            router(state.clone())
                .oneshot(request(&PATH.replace("app-a", "app-b"), Some(&token)))
                .await
                .unwrap(),
            StatusCode::FORBIDDEN,
            "API_KEY_FORBIDDEN",
        )
        .await;
        let mut req = request(PATH, Some(&token));
        req.extensions_mut().insert(ConnectInfo(
            "198.51.100.1:54321"
                .parse::<std::net::SocketAddr>()
                .unwrap(),
        ));
        req.headers_mut()
            .insert("x-forwarded-for", "192.0.2.1".parse().unwrap());
        check(
            router(state).oneshot(req).await.unwrap(),
            StatusCode::FORBIDDEN,
            "API_KEY_FORBIDDEN",
        )
        .await;
    }
    #[tokio::test]
    async fn malformed_request_extraction_never_leaks_or_reads_projection() {
        let (state, _, reader) = state(SCOPE, Some(TenantStatus::Active), false, false);
        let app = router(state);
        for path in [
            format!("{PATH}?limit=0"),
            format!("{PATH}?limit=101"),
            format!("{PATH}?limit=1&limit=2"),
            format!("{PATH}?cursor=123"),
            format!("{PATH}?cursor=%FF"),
            format!("{PATH}?api_key=private"),
            PATH.replace("app-a", "default"),
            PATH.replace("same-conversation", "%FF"),
        ] {
            check(
                app.clone().oneshot(request(&path, None)).await.unwrap(),
                StatusCode::BAD_REQUEST,
                "INVALID_REQUEST",
            )
            .await;
        }
        let mut req = request(PATH, None);
        *req.body_mut() = Body::from("private body");
        check(
            app.clone().oneshot(req).await.unwrap(),
            StatusCode::BAD_REQUEST,
            "INVALID_REQUEST",
        )
        .await;
        let mut req = request(PATH, None);
        req.extensions_mut()
            .remove::<ConnectInfo<std::net::SocketAddr>>();
        check(
            app.clone().oneshot(req).await.unwrap(),
            StatusCode::BAD_REQUEST,
            "INVALID_REQUEST",
        )
        .await;
        let mut req = request(PATH, None);
        *req.method_mut() = axum::http::Method::POST;
        check(
            app.clone().oneshot(req).await.unwrap(),
            StatusCode::METHOD_NOT_ALLOWED,
            "METHOD_NOT_ALLOWED",
        )
        .await;
        check(
            app.oneshot(request("/platform/v1/unknown", None))
                .await
                .unwrap(),
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
        )
        .await;
        assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn independent_budget_overload_rate_and_downstream_failure_are_bounded() {
        let (state, token, _) = state(SCOPE, Some(TenantStatus::Active), false, false);
        let app = Router::new()
            .route("/client-health", get(|| async { StatusCode::OK }))
            .merge(router(state.clone()));
        let permit = state.budget.try_enter().unwrap();
        check(
            app.clone()
                .oneshot(request(PATH, Some(&token)))
                .await
                .unwrap(),
            StatusCode::SERVICE_UNAVAILABLE,
            "PLATFORM_OVERLOADED",
        )
        .await;
        assert_eq!(
            app.clone()
                .oneshot(request("/client-health", None))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        drop(permit);
        check(
            app.clone()
                .oneshot(request(PATH, Some(&token)))
                .await
                .unwrap(),
            StatusCode::SERVICE_UNAVAILABLE,
            "UPSTREAM_UNAVAILABLE",
        )
        .await;
        for _ in 0..60 {
            state.budget.take_ip("192.0.2.1".parse().unwrap());
        }
        check(
            app.clone()
                .oneshot(request(PATH, Some(&token)))
                .await
                .unwrap(),
            StatusCode::TOO_MANY_REQUESTS,
            "PLATFORM_RATE_LIMITED",
        )
        .await;
        assert_eq!(
            app.oneshot(request("/client-health", None))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }
    #[tokio::test]
    async fn stalled_body_timeout_releases_the_partition_permit() {
        let (state, _, _) = state(SCOPE, Some(TenantStatus::Active), false, false);
        let mut req = request(PATH, None);
        *req.body_mut() = Body::from_stream(futures::stream::pending::<
            Result<axum::body::Bytes, std::io::Error>,
        >());
        check(
            router(state.clone()).oneshot(req).await.unwrap(),
            StatusCode::GATEWAY_TIMEOUT,
            "PLATFORM_TIMEOUT",
        )
        .await;
        assert_eq!(state.budget.available_permits(), 1);
    }
    #[tokio::test]
    async fn upstream_page_is_a_strict_bounded_whitelist() {
        let page = ListConversationParticipantsResponse {
            conversation_id: "c1".into(),
            participants: vec![flare_proto::common::ConversationParticipant {
                user_id: "u1".into(),
                roles: vec!["member".into()],
                muted: true,
                pinned: true,
                attributes: [("private".into(), "secret".into())].into_iter().collect(),
                ..Default::default()
            }],
            member_count: 999,
            participant_version: u64::MAX,
            ..Default::default()
        };
        let response = page_response("c1", 1, page.clone());
        let value: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(
            value,
            serde_json::json!({"data":{"conversation_id":"c1","participants":[{"user_id":"u1","roles":["member"]}],"next_cursor":"","has_more":false}})
        );
        for bad in [
            ListConversationParticipantsResponse {
                conversation_id: "other".into(),
                ..page.clone()
            },
            ListConversationParticipantsResponse {
                participants: vec![page.participants[0].clone(); 2],
                ..page.clone()
            },
            ListConversationParticipantsResponse {
                has_more: true,
                ..page.clone()
            },
            ListConversationParticipantsResponse {
                next_cursor: "10000".into(),
                has_more: true,
                ..page
            },
        ] {
            assert_eq!(
                page_response("c1", 1, bad).status(),
                StatusCode::BAD_GATEWAY
            );
        }
    }
}
