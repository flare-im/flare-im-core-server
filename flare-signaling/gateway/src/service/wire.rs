//! Wire 风格的依赖注入模块
//!
//! 类似 Go 的 Wire 框架，提供简单的依赖构建方法

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as AnyhowContext, Result};
use uuid::Uuid;

use crate::application::handlers::{
    AuthHandler, ConnectionHandler, ConnectionQueryHandler, PushHandler, SendHandler,
};
use crate::config::AccessGatewayConfig;
use crate::domain::model::ConnectionDomainServiceConfig;
use crate::domain::ports::{ConnectionQuery, IConnectionPort};
use crate::domain::service::{
    ConnectionDomainService, ConnectionQualityService, PushDomainService, SendAckDomainService,
    SendDataDomainService, SendEventDomainService, SendMessageDomainService, SyncService,
};
use crate::infrastructure::ports::{
    ConnectionContextResolver, ConnectionRepository, ManagerConnectionQuery, PushRepository,
    RouterAckReportPort, RouterDataCommandPort, RouterEventCommandPort, RouterMessageCommandPort,
    SignalingRouteGrpcPool, StorageSyncGrpcPool, StorageSyncPort,
};
use crate::interface::grpc::handler::AccessGatewayHandler;
use crate::interface::link::LongConnectionHandler;
use crate::service::service_manager::PortConfig;
use tokio::sync::Mutex;

// 注意：最新的 Flare 模式不再需要在 FlareServerBuilder 中配置中间件
// 中间件是客户端特性，服务端通过 ServerEventHandler 处理消息
use flare_core::server::builder::flare::{FlareServer, FlareServerBuilder};
use flare_core::server::connection::{ConnectionManager, ConnectionManagerTrait};
use flare_core::server::handle::{DefaultServerHandle, ServerHandle};
use flare_im_core::metrics::AccessGatewayMetrics;
use flare_server_core::Config;
use flare_server_core::auth::{RedisTokenStore, TokenService};

/// gRPC 服务集合
///
pub struct GrpcServices {
    pub access_gateway_handler: Arc<AccessGatewayHandler>,
    pub grpc_addr: std::net::SocketAddr,
}

/// 应用上下文 - 包含所有已初始化的服务
pub struct ApplicationContext {
    pub long_connection_server: Arc<tokio::sync::Mutex<Option<FlareServer>>>,
    pub grpc_services: GrpcServices,
    /// 推送领域服务（用于批量消息刷新）
    pub push_domain_service: Arc<crate::domain::service::push_domain_service::PushDomainService>,
    /// 网关 ID
    pub gateway_id: String,
    /// 地区
    pub region: Option<String>,
}

/// 构建应用上下文
///
/// 类似 Go Wire 的 Initialize 函数，按照依赖顺序构建所有组件
///
/// # 参数
/// * `app_config` - 应用配置
/// * `runtime_config` - 运行时配置
/// * `port_config` - 端口配置
///
/// # 返回
/// * `ApplicationContext` - 构建好的应用上下文
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

    // 6. 连接读模型（CQRS 查询侧，本地 ConnectionManager）
    let connection_query: Arc<dyn ConnectionQuery> = Arc::new(ManagerConnectionQuery::new(
        connection_manager.clone() as Arc<dyn ConnectionManagerTrait>,
        access_config.default_tenant_id.clone(),
    ));

    // 7. IConnectionPort：在线 RPC + 本地连接信息（供 ConnectionHandler / gRPC）
    let connection_port: Arc<dyn IConnectionPort> = Arc::new(ConnectionRepository::new(
        connection_manager.clone() as Arc<dyn ConnectionManagerTrait>,
        access_config.default_tenant_id.clone(),
    ));

    // 8–9. 连接领域服务 + 应用层连接处理器（State 模式：默认 NoopConnectionStateNotifier）
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

    // 10. 与 Flare `ServerHandle` 共享槽位：`setup_server_components` 注入后 `PushRepository` 即可下行
    let push_handle_slot: Arc<Mutex<Option<Arc<dyn ServerHandle>>>> = Arc::new(Mutex::new(None));
    let push_port: Arc<dyn crate::domain::ports::IPushPort> =
        Arc::new(PushRepository::new(push_handle_slot.clone()));

    // 11. Signaling Router / Conversation Sync gRPC 池（上行 Route* 与 DATA 同步）
    let route_grpc_pool = Arc::new(SignalingRouteGrpcPool::new());
    let storage_sync_pool = Arc::new(StorageSyncGrpcPool::new());

    // 12. 长连接处理器：ConnectionHandler + 上行 SendHandler（`IContextResolver` → `ConnectionContextResolver`）
    let connection_handler = build_long_connection_handler(
        connection_handler_app.clone(),
        connection_port.clone(),
        route_grpc_pool,
        storage_sync_pool,
    );

    // 13. 推送领域服务：读 `ConnectionQuery` + 写 `IPushPort`（ServerHandle 就绪后生效）
    let push_domain_service = Arc::new(PushDomainService::new(push_port, connection_query.clone()));

    // 14. 构建认证器
    let authenticator = build_authenticator(&access_config).await;

    // 15. 构建长连接服务器
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

    // 16. 构建 gRPC 处理器
    // 注意：SignalingService 由 flare-signaling/online 服务实现，Gateway 不再提供
    debug!("Building gRPC handlers");

    let access_gateway_grpc_handler = Arc::new(AccessGatewayHandler::new(
        Arc::new(PushHandler::new(push_domain_service.clone())),
        Arc::new(ConnectionQueryHandler::new(connection_port.clone())),
    ));
    debug!("gRPC handlers built successfully");

    // 17. gRPC 地址
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

/// 装配长连接上行：`SendHandler` + Router / Conversation Sync 适配端口
fn build_long_connection_handler(
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

/// 构建认证器
async fn build_authenticator(
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

    Arc::new(AuthHandler::new(Arc::new(token_service)))
}

/// 使用 Flare 模式构建服务器
///
/// Flare 模式特点：
/// - 只需实现 `ServerEventHandler` trait
/// - 自动消息路由和 ACK 处理
/// - 支持设备管理、认证、多协议等完整功能
/// - 连接数、心跳、认证超时等由 access_config 提供，便于扩容与稳定性调优
fn build_flare_server(
    ws_addr: String,
    quic_addr: Option<String>,
    connection_handler: Arc<LongConnectionHandler>,
    connection_manager: Arc<ConnectionManager>,
    device_manager: Arc<flare_core::server::device::DeviceManager>,
    authenticator: Arc<dyn flare_core::server::auth::Authenticator + Send + Sync>,
    compression_algorithm: flare_core::common::compression::CompressionAlgorithm,
    encryption_enabled: bool,
    access_config: &AccessGatewayConfig,
) -> Result<FlareServer> {
    use flare_core::common::config_types::{HeartbeatConfig, TransportProtocol};
    use flare_core::common::protocol::SerializationFormat;

    let event_handler: Arc<dyn flare_core::server::events::handler::ServerEventHandler> =
        connection_handler.clone();

    let mut builder = FlareServerBuilder::new(ws_addr.clone(), event_handler)
        .with_connection_manager(connection_manager)
        .with_device_manager(device_manager)
        .enable_auth()
        .with_authenticator(authenticator)
        .with_auth_timeout(Duration::from_secs(access_config.auth_timeout_secs))
        .with_max_connections(access_config.max_connections)
        .with_connection_timeout(Duration::from_secs(access_config.connection_timeout_secs))
        .with_heartbeat(HeartbeatConfig {
            interval: Duration::from_secs(access_config.heartbeat_interval_secs),
            timeout: Duration::from_secs(access_config.heartbeat_timeout_secs),
            enabled: true,
        })
        .with_default_format(SerializationFormat::Protobuf)
        .with_default_compression(compression_algorithm);

    // 可选：启用加密
    if encryption_enabled {
        builder = builder.with_default_encryption(
            flare_core::common::encryption::EncryptionAlgorithm::Aes256Gcm,
        );
    }

    // 协议配置
    if let Some(quic) = quic_addr {
        builder = builder
            .with_protocols(vec![TransportProtocol::WebSocket, TransportProtocol::QUIC])
            .with_protocol_address(TransportProtocol::WebSocket, ws_addr)
            .with_protocol_address(TransportProtocol::QUIC, quic);
    } else {
        builder = builder
            .with_protocols(vec![TransportProtocol::WebSocket])
            .with_protocol_address(TransportProtocol::WebSocket, ws_addr);
    }

    builder
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build FlareServer: {}", e))
}

/// 构建长连接服务器
async fn build_long_connection_server(
    runtime_config: &Config,
    ws_port: u16,
    quic_port: u16,
    connection_manager: Arc<ConnectionManager>,
    authenticator: Arc<dyn flare_core::server::auth::Authenticator + Send + Sync>,
    connection_handler: Arc<LongConnectionHandler>,
    access_config: Arc<AccessGatewayConfig>,
    push_handle_slot: Arc<Mutex<Option<Arc<dyn ServerHandle>>>>,
) -> Result<Arc<tokio::sync::Mutex<Option<FlareServer>>>> {
    use tracing::{error, info, warn};

    // 创建设备管理器（平台互斥策略：同一用户同一平台只能有一个设备在线）
    use flare_core::common::device::DeviceConflictStrategyBuilder;
    use flare_core::server::device::DeviceManager;
    let device_manager = Arc::new(DeviceManager::new(
        DeviceConflictStrategyBuilder::new()
            .platform_exclusive()
            .build(),
    ));

    let ws_addr = format!("{}:{}", runtime_config.server.address, ws_port);
    let quic_addr = format!("{}:{}", runtime_config.server.address, quic_port);

    // 配置压缩和加密（从配置读取）
    info!(
        compression_algorithm = ?access_config.compression_algorithm,
        enable_encryption = %access_config.enable_encryption,
        "Reading compression and encryption configuration"
    );

    let compression_algorithm =
        parse_compression_algorithm(access_config.compression_algorithm.as_deref());

    // 先注册加密器（如果启用），必须在构建服务器之前注册
    let encryption_config = setup_encryption_config(
        access_config.enable_encryption,
        access_config.encryption_key.as_deref(),
    )
    .await;

    info!(
        compression = ?compression_algorithm,
        encryption_enabled = %encryption_config.enabled,
        "Configuration parsed, building FlareServer"
    );

    let server = match build_flare_server(
        ws_addr.clone(),
        Some(quic_addr.clone()),
        connection_handler.clone(),
        connection_manager.clone(),
        device_manager.clone(),
        authenticator.clone(),
        compression_algorithm.clone(),
        encryption_config.enabled,
        access_config.as_ref(),
    ) {
        Ok(server) => server,
        Err(e) => {
            let error_msg = e.to_string();
            if error_msg.contains("Address already in use")
                || error_msg.contains("创建 QUIC 端点失败")
            {
                warn!(quic_addr = %quic_addr, "QUIC port unavailable, falling back to WebSocket-only mode");
                build_flare_server(
                    ws_addr.clone(),
                    None,
                    connection_handler.clone(),
                    connection_manager.clone(),
                    device_manager.clone(),
                    authenticator.clone(),
                    compression_algorithm,
                    encryption_config.enabled,
                    access_config.as_ref(),
                )?
            } else {
                error!(error = %e, "Failed to build FlareServer");
                return Err(anyhow::anyhow!("Failed to build server: {}", e));
            }
        }
    };

    // 设置 server handle、连接管理器，并注入 `PushRepository` 共用的 `ServerHandle` 槽位
    setup_server_components(
        &connection_handler,
        &connection_manager,
        Some(&push_handle_slot),
    )
    .await;

    // 启动服务器
    server.start().await.map_err(|e| {
        error!(error = %e, "Failed to start FlareServer");
        anyhow::anyhow!("Failed to start server: {}", e)
    })?;

    info!(ws_addr = %ws_addr, quic_addr = %quic_addr, "✅ Long connection server started");

    Ok(Arc::new(tokio::sync::Mutex::new(Some(server))))
}

/// 加密配置
struct EncryptionConfig {
    enabled: bool,
}

/// 解析压缩算法
fn parse_compression_algorithm(
    algorithm: Option<&str>,
) -> flare_core::common::compression::CompressionAlgorithm {
    use flare_core::common::compression::CompressionAlgorithm;

    let result = match algorithm {
        Some("gzip") => CompressionAlgorithm::Gzip,
        Some("zstd") => CompressionAlgorithm::Zstd,
        Some("none") | Some("") | None => CompressionAlgorithm::None,
        Some(other) => {
            tracing::warn!(algorithm = %other, "Unknown compression algorithm, using None");
            CompressionAlgorithm::None
        }
    };

    tracing::debug!(algorithm = ?algorithm, parsed = ?result, "Parsed compression algorithm");
    result
}

/// 配置加密（如果启用）
async fn setup_encryption_config(
    enable_encryption: bool,
    encryption_key: Option<&str>,
) -> EncryptionConfig {
    if !enable_encryption {
        return EncryptionConfig { enabled: false };
    }

    use flare_core::common::encryption::{Aes256GcmEncryptor, EncryptionUtil};
    use tracing::{info, warn};

    // 解析加密密钥（32字节）
    let key_bytes = encryption_key.and_then(|key| {
        if key.len() == 32 {
            // 直接32字符的字符串
            Some(key.as_bytes().to_vec())
        } else if key.len() == 64 {
            // hex 编码的 64 字符字符串（32字节）
            (0..32)
                .try_fold(Vec::new(), |mut acc, i| {
                    u8::from_str_radix(&key[i * 2..i * 2 + 2], 16).map(|b| {
                        acc.push(b);
                        acc
                    })
                })
                .ok()
        } else {
            None
        }
    });

    let encryption_key = key_bytes.unwrap_or_else(|| {
        warn!("Encryption key not set or invalid (expected 32 bytes or 64 hex chars), using default key (NOT SECURE FOR PRODUCTION)");
        b"01234567890123456789012345678901".to_vec() // 32 bytes for AES-256
    });

    match Aes256GcmEncryptor::new(&encryption_key) {
        Ok(encryptor) => {
            EncryptionUtil::register_custom(Arc::new(encryptor));
            info!("🔐 AES-256-GCM encryption enabled with custom key");
            EncryptionConfig { enabled: true }
        }
        Err(e) => {
            warn!(error = %e, "Failed to create encryption, encryption disabled");
            EncryptionConfig { enabled: false }
        }
    }
}

/// 设置服务器组件（ServerHandle 和 ConnectionManager）
async fn setup_server_components(
    connection_handler: &Arc<LongConnectionHandler>,
    connection_manager: &Arc<ConnectionManager>,
    push_handle_slot: Option<&Arc<Mutex<Option<Arc<dyn ServerHandle>>>>>,
) {
    use tracing::info;

    let manager_trait: Arc<dyn flare_core::server::connection::ConnectionManagerTrait> =
        connection_manager.clone();
    let server_handle: Arc<dyn ServerHandle> =
        Arc::new(DefaultServerHandle::new(manager_trait.clone()));

    if let Some(slot) = push_handle_slot {
        *slot.lock().await = Some(server_handle.clone());
    }

    connection_handler.set_server_handle(server_handle).await;
    connection_handler
        .set_connection_manager(manager_trait)
        .await;

    info!("✅ Server handle, connection manager, and push handle slot configured");
}
