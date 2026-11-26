//! 处理器构建逻辑

use std::sync::Arc;

use crate::application::handlers::{ConnectionHandler, SendHandler};
use crate::config::AccessGatewayConfig;
use crate::domain::ports::IConnectionPort;
use crate::domain::service::{
    SendAckDomainService, SendDataDomainService, SendEventDomainService, SendMessageDomainService,
    SyncPullLimiter, SyncPullRateLimitConfig, SyncService,
};
use crate::infrastructure::ports::{
    ConnectionContextResolver, RouterAckReportPort, RouterDataCommandPort, RouterEventCommandPort,
    RouterMessageCommandPort, SignalingRouteGrpcPool, StorageSyncGrpcPool, StorageSyncPort,
};
use crate::interface::link::LongConnectionHandler;
use flare_im_core_service_kit::tenant_runtime::TenantRuntimeCache;
use flare_server_core::auth::{
    AuthProviderConfig, AuthProviderMode, RedisTokenStore, TokenService, TokenValidator,
    TrustedIssuerConfig, build_http_hook_token_validator, build_token_validator,
};

type SharedAuthenticator = Arc<dyn flare_core::server::auth::Authenticator + Send + Sync>;

/// 构建认证器（token 校验 + 租户投影闸门）
pub async fn build_authenticator(
    config: &AccessGatewayConfig,
    tenant_runtime: TenantRuntimeCache,
    metrics: Arc<flare_im_core_service_kit::metrics::AccessGatewayMetrics>,
) -> flare_server_core::error::Result<SharedAuthenticator> {
    let core_tokens = match config.auth_provider.mode {
        AuthProviderMode::CoreJwt => Some(Arc::new(core_token_service(config)?)),
        AuthProviderMode::HttpHook => None,
    };
    let token_validator = connect_token_validator(
        &config.auth_provider,
        core_tokens,
        &config.trusted_token_issuers,
    )
    .map_err(|err| {
        flare_server_core::error::FlareError::system(format!(
            "failed to initialize access-gateway auth validator: {err}"
        ))
    })?;
    tracing::info!(
        auth_mode = ?config.auth_provider.mode,
        hook_fallback = config.auth_provider.mode == AuthProviderMode::CoreJwt
            && config.auth_provider.hook_url.is_some(),
        "access-gateway auth validator initialized"
    );

    Ok(Arc::new(
        crate::application::handlers::AuthHandler::new(
            token_validator,
            tenant_runtime,
            config.tenant_policy,
        )
        .with_metrics(metrics),
    ))
}

/// `core_jwt` 模式下本地验签用的 [`TokenService`]：core 密钥，配了 token store 时挂上撤销检查。
fn core_token_service(
    config: &AccessGatewayConfig,
) -> flare_server_core::error::Result<TokenService> {
    use tracing::{info, warn};

    let token_secret = config.token_secret.as_deref().ok_or_else(|| {
        flare_server_core::error::FlareError::system(
            "access-gateway token secret is required when auth.mode=core_jwt",
        )
    })?;
    let mut token_service = TokenService::new(
        token_secret.to_string(),
        config.token_issuer.clone(),
        config.token_ttl_seconds,
    );

    if let Some(store_url) = &config.token_store_redis_url {
        // 建连撤销检查复用 TokenService::validate_token 里的 is_revoked。
        // namespace 必须与 api-gateway 的 token_store 一致，否则读不到它写的撤销位。
        let store_result = match config.token_store_namespace.as_deref() {
            Some(ns) => RedisTokenStore::with_namespace(store_url, ns),
            None => RedisTokenStore::new(store_url),
        };
        match store_result {
            Ok(store) => {
                token_service = token_service.with_store(Arc::new(store));
                info!(
                    namespace = config.token_store_namespace.as_deref().unwrap_or("flare"),
                    "token store attached: connect-time revocation check enabled"
                );
            }
            Err(err) => {
                warn!(
                    ?err,
                    "Failed to initialize token store, proceeding without revocation support"
                );
            }
        }
    }

    Ok(token_service)
}

/// 长连接令牌校验器，与 API 网关的构造一致。
///
/// 有 core [`TokenService`]（`core_jwt` 模式）时走共享的 [`build_token_validator`]：先本地验
/// core JWT 与受信 issuer，本地不认、且配了 `auth.hook_url` 时再转问三方验证钩子；没配钩子只做
/// 本地校验。没有（`http_hook` 模式）时每枚令牌都交给钩子。
fn connect_token_validator(
    auth: &AuthProviderConfig,
    core_tokens: Option<Arc<TokenService>>,
    trusted_issuers: &[TrustedIssuerConfig],
) -> flare_server_core::error::Result<Arc<dyn TokenValidator>> {
    // 基建层的 build_* 返回 anyhow::Result；服务边界统一转为 server_core 错误体系。
    match core_tokens {
        Some(token_service) => {
            build_token_validator(auth, token_service, trusted_issuers).map_err(Into::into)
        }
        None => build_http_hook_token_validator(auth).map_err(Into::into),
    }
}

/// 构建长连接上行处理器
#[allow(clippy::too_many_arguments)]
pub fn build_long_connection_handler(
    connection_handler_app: Arc<ConnectionHandler>,
    connection_port: Arc<dyn IConnectionPort>,
    route_pool: Arc<SignalingRouteGrpcPool>,
    storage_sync_pool: Arc<StorageSyncGrpcPool>,
    sync_pull_rate_limit_config: SyncPullRateLimitConfig,
    tenant_runtime: TenantRuntimeCache,
    realtime_relay: Arc<crate::domain::service::RealtimeControlRelay>,
    realtime_broadcast: Arc<dyn crate::domain::service::IRealtimeBroadcastPort>,
    message_signer: Option<flare_server_core::SignedContextSigner>,
    delivery_watch: Arc<crate::domain::service::DeliveryWatch>,
) -> Arc<LongConnectionHandler> {
    let storage_sync = Arc::new(StorageSyncPort::new(
        storage_sync_pool,
        message_signer.clone(),
    ));
    let event_port: Arc<dyn crate::domain::ports::IEventCommandPort> = Arc::new(
        RouterEventCommandPort::new(route_pool.clone(), message_signer.clone()),
    );
    let message_port: Arc<dyn crate::domain::ports::IMessageCommandPort> = Arc::new(
        RouterMessageCommandPort::new(route_pool.clone(), message_signer),
    );
    let sync_port: Arc<dyn crate::domain::ports::ISyncPort> = storage_sync;

    let sync_service = Arc::new(
        SyncService::new(sync_port)
            .with_pull_limiter(Arc::new(SyncPullLimiter::new(sync_pull_rate_limit_config)))
            .with_tenant_runtime(tenant_runtime),
    );
    let send_event_service = Arc::new(SendEventDomainService::new(event_port));

    let data_port: Arc<dyn crate::domain::ports::IDataCommandPort> =
        Arc::new(RouterDataCommandPort::new(route_pool.clone()));
    let ack_port: Arc<dyn crate::domain::ports::IAckReportPort> =
        Arc::new(RouterAckReportPort::new(route_pool));
    let context_resolver: Arc<dyn crate::domain::ports::IContextResolver> =
        Arc::new(ConnectionContextResolver::new(connection_port));

    let send_handler = Arc::new(SendHandler::new(
        Arc::new(SendMessageDomainService::new(message_port)),
        send_event_service,
        Arc::new(SendDataDomainService::new(
            data_port,
            sync_service,
            realtime_relay,
            realtime_broadcast,
        )),
        Arc::new(SendAckDomainService::new(ack_port, delivery_watch)),
        context_resolver,
    ));

    Arc::new(LongConnectionHandler::new(
        connection_handler_app,
        send_handler,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use flare_server_core::auth::{AuthError, TokenValidationRequest};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;

    const CORE_SECRET: &str = "connect-validator-test-secret-longer-than-32-bytes";

    fn core_tokens() -> Arc<TokenService> {
        Arc::new(TokenService::new(CORE_SECRET, "flare-im-core", 3600))
    }

    fn core_token(tokens: &TokenService) -> String {
        tokens
            .generate_token("core-user", Some("device-a"), Some("tenant-a"))
            .expect("core token")
    }

    /// 社交签发的那类连接令牌：EdDSA 签名，本地 HS256 校验报 `InvalidAlgorithm`。
    fn social_token() -> String {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"EdDSA","typ":"JWT"}"#);
        let claims =
            URL_SAFE_NO_PAD.encode(br#"{"sub":"social-user","iss":"flare-im","exp":4102444800}"#);
        let signature = URL_SAFE_NO_PAD.encode([7u8; 64]);
        format!("{header}.{claims}.{signature}")
    }

    fn request(token: &str) -> TokenValidationRequest {
        TokenValidationRequest {
            token: token.to_string(),
            trace_id: None,
            request_id: None,
            path: None,
            method: None,
        }
    }

    /// 认可任何令牌的验证钩子，返回它的地址和被问的次数。
    async fn accepting_hook() -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/internal/auth/validate",
            listener.local_addr().unwrap()
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let counter = counter.clone();
                tokio::spawn(async move {
                    read_request(&mut socket).await;
                    counter.fetch_add(1, Ordering::SeqCst);
                    let body = r#"{"active":true,"userId":"social-user","tenantId":"tenant-a"}"#;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        (url, calls)
    }

    /// 读完一个 HTTP 请求（头 + Content-Length 指定的正文）再回应，免得半途关连接。
    async fn read_request(socket: &mut TcpStream) {
        let mut raw = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let read = socket.read(&mut chunk).await.unwrap_or(0);
            if read == 0 {
                return;
            }
            raw.extend_from_slice(&chunk[..read]);
            let text = String::from_utf8_lossy(&raw);
            let Some(head_end) = text.find("\r\n\r\n") else {
                continue;
            };
            let body_len = text[..head_end]
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
                .and_then(|(_, value)| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            if raw.len() >= head_end + 4 + body_len {
                return;
            }
        }
    }

    /// `core_jwt` 配了钩子：本地验不过的社交令牌转问钩子，钩子认可即放行；core 自签令牌
    /// 仍在本地验过，不去问钩子。
    #[tokio::test]
    async fn core_jwt_with_hook_accepts_tokens_the_hook_vouches_for() {
        let (hook_url, calls) = accepting_hook().await;
        let auth = AuthProviderConfig {
            mode: AuthProviderMode::CoreJwt,
            hook_url: Some(hook_url),
            hook_timeout_ms: 2000,
            ..Default::default()
        };
        let tokens = core_tokens();
        let validator = connect_token_validator(&auth, Some(tokens.clone()), &[]).unwrap();

        let principal = validator
            .validate(request(&social_token()))
            .await
            .expect("the hook vouches for the social token");
        assert_eq!(principal.user_id, "social-user");
        assert_eq!(principal.tenant_id.as_deref(), Some("tenant-a"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let principal = validator
            .validate(request(&core_token(&tokens)))
            .await
            .expect("core token");
        assert_eq!(principal.user_id, "core-user");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "core tokens never reach the hook"
        );
    }

    /// `core_jwt` 没配钩子：只做本地校验，社交令牌按无效拒绝，core 自签令牌照常放行。
    #[tokio::test]
    async fn core_jwt_without_hook_validates_locally_only() {
        let tokens = core_tokens();
        let validator =
            connect_token_validator(&AuthProviderConfig::default(), Some(tokens.clone()), &[])
                .unwrap();

        let err = validator
            .validate(request(&social_token()))
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::InvalidToken(_)), "{err}");

        let principal = validator
            .validate(request(&core_token(&tokens)))
            .await
            .expect("core token");
        assert_eq!(principal.user_id, "core-user");
    }
}
