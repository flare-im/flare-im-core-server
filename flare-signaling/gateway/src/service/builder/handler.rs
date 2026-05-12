//! 处理器构建逻辑

use std::sync::Arc;

use crate::application::handlers::{ConnectionHandler, SendHandler};
use crate::config::AccessGatewayConfig;
use crate::domain::ports::IConnectionPort;
use crate::domain::service::{
    SendAckDomainService, SendDataDomainService, SendEventDomainService, SendMessageDomainService,
    SyncService,
};
use crate::infrastructure::ports::{
    ConnectionContextResolver, RouterAckReportPort, RouterDataCommandPort, RouterEventCommandPort,
    RouterMessageCommandPort, SignalingRouteGrpcPool, StorageSyncGrpcPool, StorageSyncPort,
};
use crate::interface::link::LongConnectionHandler;
use flare_server_core::auth::{RedisTokenStore, TokenService};

/// 构建认证器
pub async fn build_authenticator(
    config: &AccessGatewayConfig,
) -> Arc<dyn flare_core::server::auth::Authenticator + Send + Sync> {
    use tracing::warn;

    let mut token_service = TokenService::new(
        config.token_secret.clone(),
        config.token_issuer.clone(),
        config.token_ttl_seconds,
    );

    if let Some(store_url) = &config.token_store_redis_url {
        match RedisTokenStore::new(store_url) {
            Ok(store) => {
                token_service = token_service.with_store(Arc::new(store));
            }
            Err(err) => {
                warn!(
                    ?err,
                    "Failed to initialize token store, proceeding without revocation support"
                );
            }
        }
    }

    Arc::new(crate::application::handlers::AuthHandler::new(Arc::new(
        token_service,
    )))
}

/// 构建长连接上行处理器
pub fn build_long_connection_handler(
    connection_handler_app: Arc<ConnectionHandler>,
    connection_port: Arc<dyn IConnectionPort>,
    route_pool: Arc<SignalingRouteGrpcPool>,
    storage_sync_pool: Arc<StorageSyncGrpcPool>,
) -> Arc<LongConnectionHandler> {
    let message_port: Arc<dyn crate::domain::ports::IMessageCommandPort> =
        Arc::new(RouterMessageCommandPort::new(route_pool.clone()));
    let event_port: Arc<dyn crate::domain::ports::IEventCommandPort> =
        Arc::new(RouterEventCommandPort::new(route_pool.clone()));
    let storage_sync = Arc::new(StorageSyncPort::new(storage_sync_pool));
    let sync_port: Arc<dyn crate::domain::ports::ISyncPort> = storage_sync;

    let sync_service = Arc::new(SyncService::new(sync_port));
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
        Arc::new(SendDataDomainService::new(data_port, sync_service)),
        Arc::new(SendAckDomainService::new(ack_port)),
        context_resolver,
    ));

    Arc::new(LongConnectionHandler::new(
        connection_handler_app,
        send_handler,
    ))
}
