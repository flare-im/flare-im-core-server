//! Wire 风格的依赖注入模块
//!
//! 提供简洁的依赖构建方法，类似 Go 的 Wire 框架

use std::sync::Arc;

use anyhow::Context;
use uuid::Uuid;

use crate::application::handlers::{ConnectionHandler, ConnectionQueryHandler, PushHandler};
use crate::config::{AccessGatewayConfig, PortConfig};
use crate::domain::model::ConnectionDomainServiceConfig;
use crate::domain::ports::{ConnectionQuery, IConnectionPort};
use crate::domain::service::{
    ConnectionDomainService, ConnectionQualityService, PushDomainService,
};
use crate::error::Result;
use crate::infrastructure::ports::{
    ConnectionRepository, ManagerConnectionQuery, PushRepository, SignalingRouteGrpcPool,
    StorageSyncGrpcPool,
};
use crate::interface::grpc::handler::AccessGatewayHandler;
use crate::service::builder::{
    build_authenticator, build_long_connection_handler, build_long_connection_server,
};
use flare_core::server::connection::{ConnectionManager, ConnectionManagerTrait};
use flare_im_core::metrics::AccessGatewayMetrics;
use flare_server_core::Config;
use tokio::sync::Mutex;

/// gRPC 服务集合
pub struct GrpcServices {
    pub access_gateway_handler: Arc<AccessGatewayHandler>,
    pub grpc_addr: std::net::SocketAddr,
}

/// 应用上下文 - 包含所有已初始化的服务
pub struct ApplicationContext {
    pub long_connection_server:
        Arc<tokio::sync::Mutex<Option<flare_core::server::builder::flare::FlareServer>>>,
    pub grpc_services: GrpcServices,
    pub push_domain_service: Arc<PushDomainService>,
    pub gateway_id: String,
    pub region: Option<String>,
}

/// 构建应用上下文
///
/// 按照 Wire 风格的依赖顺序构建所有组件
pub async fn initialize(
    app_config: &flare_im_core::config::FlareAppConfig,
    runtime_config: &Config,
    port_config: PortConfig,
) -> Result<ApplicationContext> {
    use tracing::{debug, info};

    // 1. 加载配置
    let access_config = Arc::new(AccessGatewayConfig::from_app_config(app_config));

    // 2. 获取 gateway_id 和 region
    let gateway_id = access_config
        .gateway_id
        .clone()
        .unwrap_or_else(|| format!("gateway-{}", &Uuid::new_v4().to_string()[..8]));
    let region = access_config.region.clone();

    info!(gateway_id = %gateway_id, "Gateway initialized");
    if let Some(ref r) = region {
        info!(region = %r, "Gateway region configured");
    }

    // 3. 初始化指标
    let metrics = Arc::new(AccessGatewayMetrics::new());
    debug!("Prometheus metrics initialized");

    // 4. 构建连接管理器
    let connection_manager = Arc::new(ConnectionManager::new());

    // 5. 连接查询和端口
    let connection_query: Arc<dyn ConnectionQuery> = Arc::new(ManagerConnectionQuery::new(
        connection_manager.clone() as Arc<dyn ConnectionManagerTrait>,
        access_config.default_tenant_id.clone(),
    ));

    let connection_port: Arc<dyn IConnectionPort> = Arc::new(ConnectionRepository::new(
        connection_manager.clone() as Arc<dyn ConnectionManagerTrait>,
        access_config.default_tenant_id.clone(),
    ));

    // 6. 连接领域服务
    let quality_service = Arc::new(ConnectionQualityService::new());
    let session_domain_service = Arc::new(ConnectionDomainService::new(
        connection_port.clone(),
        quality_service,
        ConnectionDomainServiceConfig {
            gateway_id: gateway_id.clone(),
        },
    ));

    let connection_handler_app = Arc::new(ConnectionHandler::new(
        session_domain_service.clone(),
        metrics.clone(),
        Arc::new(flare_im_core::abstractions::state::NoopConnectionStateNotifier),
        connection_port.clone(),
    ));

    // 7. Push 端口
    let push_handle_slot: Arc<Mutex<Option<Arc<dyn flare_core::server::handle::ServerHandle>>>> =
        Arc::new(Mutex::new(None));
    let push_port: Arc<dyn crate::domain::ports::IPushPort> =
        Arc::new(PushRepository::new(push_handle_slot.clone()));

    // 8. gRPC 连接池
    let route_grpc_pool = Arc::new(SignalingRouteGrpcPool::new());
    let storage_sync_pool = Arc::new(StorageSyncGrpcPool::new());

    // 9. 长连接处理器
    let connection_handler = build_long_connection_handler(
        connection_handler_app.clone(),
        connection_port.clone(),
        route_grpc_pool,
        storage_sync_pool,
    );

    // 10. 推送领域服务
    let push_domain_service = Arc::new(PushDomainService::new(push_port, connection_query.clone()));

    // 11. 认证器
    let authenticator = build_authenticator(&access_config).await;

    // 12. 长连接服务器
    debug!(ws_port = %port_config.ws_port, quic_port = %port_config.quic_port, "Building long connection server");
    let long_connection_server = build_long_connection_server(
        runtime_config,
        port_config.ws_port,
        port_config.quic_port,
        connection_manager.clone(),
        authenticator,
        connection_handler.clone(),
        access_config.clone(),
        push_handle_slot,
    )
    .await
    .with_context(|| "Failed to build long connection server")?;

    info!("Long connection server built successfully");

    // 13. gRPC 处理器
    debug!("Building gRPC handlers");
    let access_gateway_grpc_handler = Arc::new(AccessGatewayHandler::new(
        Arc::new(PushHandler::new(push_domain_service.clone())),
        Arc::new(ConnectionQueryHandler::new(connection_port.clone())),
    ));
    debug!("gRPC handlers built successfully");

    // 14. gRPC 地址
    let grpc_addr = format!(
        "{}:{}",
        runtime_config.server.address, port_config.grpc_port
    )
    .parse::<std::net::SocketAddr>()
    .with_context(|| "Invalid gRPC address")?;

    info!("Application context initialized successfully");
    Ok(ApplicationContext {
        long_connection_server,
        grpc_services: GrpcServices {
            access_gateway_handler: access_gateway_grpc_handler,
            grpc_addr,
        },
        push_domain_service: push_domain_service.clone(),
        gateway_id,
        region,
    })
}
