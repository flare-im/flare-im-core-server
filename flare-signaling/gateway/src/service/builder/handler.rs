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
use flare_server_core::auth::{
    AuthProviderMode, RedisTokenStore, TokenService, build_core_jwt_token_validator,
    build_http_hook_token_validator,
};

type SharedAuthenticator = Arc<dyn flare_core::server::auth::Authenticator + Send + Sync>;

/// 构建认证器
pub async fn build_authenticator(
    config: &AccessGatewayConfig,
) -> flare_server_core::error::Result<SharedAuthenticator> {
    use tracing::{info, warn};

    let token_validator = match config.auth_provider.mode {
        AuthProviderMode::CoreJwt => {
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

            build_core_jwt_token_validator(Arc::new(token_service), &config.trusted_token_issuers)
        }
        AuthProviderMode::HttpHook => build_http_hook_token_validator(&config.auth_provider),
    }
    .map_err(|err| {
        flare_server_core::error::FlareError::system(format!(
            "failed to initialize access-gateway auth validator: {err}"
        ))
    })?;

    Ok(Arc::new(crate::application::handlers::AuthHandler::new(
        token_validator,
    )))
}

/// 构建长连接上行处理器
#[allow(clippy::too_many_arguments)]
pub fn build_long_connection_handler(
    connection_handler_app: Arc<ConnectionHandler>,
    connection_port: Arc<dyn IConnectionPort>,
    route_pool: Arc<SignalingRouteGrpcPool>,
    storage_sync_pool: Arc<StorageSyncGrpcPool>,
    sync_pull_rate_limit_config: SyncPullRateLimitConfig,
    conversation_subscriptions: Arc<crate::domain::service::ConversationSubscriptionRegistry>,
    push_port: Arc<dyn crate::domain::ports::IPushPort>,
) -> Arc<LongConnectionHandler> {
    let message_port: Arc<dyn crate::domain::ports::IMessageCommandPort> =
        Arc::new(RouterMessageCommandPort::new(route_pool.clone()));
    let event_port: Arc<dyn crate::domain::ports::IEventCommandPort> =
        Arc::new(RouterEventCommandPort::new(route_pool.clone()));
    let storage_sync = Arc::new(StorageSyncPort::new(storage_sync_pool));
    let sync_port: Arc<dyn crate::domain::ports::ISyncPort> = storage_sync;

    let sync_service = Arc::new(
        SyncService::new(sync_port)
            .with_pull_limiter(Arc::new(SyncPullLimiter::new(sync_pull_rate_limit_config))),
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
            conversation_subscriptions,
            push_port,
        )),
        Arc::new(SendAckDomainService::new(ack_port)),
        context_resolver,
    ));

    Arc::new(LongConnectionHandler::new(
        connection_handler_app,
        send_handler,
    ))
}
