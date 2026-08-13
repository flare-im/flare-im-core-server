//! 服务器构建逻辑

use std::sync::Arc;
use std::time::Duration;

use crate::config::AccessGatewayConfig;
use crate::interface::link::LongConnectionHandler;
use flare_core::server::builder::flare::{FlareServer, FlareServerBuilder};
use flare_core::server::connection::ConnectionManager;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tokio::sync::Mutex;

type PushHandleSlot = Arc<Mutex<Option<Arc<dyn flare_core::server::handle::ServerHandle>>>>;

/// 使用 Flare 模式构建服务器
///
/// Flare 模式特点：
/// - 只需实现 `ServerEventHandler` trait
/// - 自动消息路由和 ACK 处理
/// - 支持设备管理、认证、多协议等完整功能
#[allow(clippy::too_many_arguments)]
pub fn build_flare_server(
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
        .with_heartbeat(
            HeartbeatConfig::new()
                .with_interval(Duration::from_secs(access_config.heartbeat_interval_secs))
                .with_timeout(Duration::from_secs(access_config.heartbeat_timeout_secs)),
        )
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

    builder.build().map_err(|e| {
        ErrorBuilder::new(
            ErrorCode::InternalError,
            format!("Failed to build FlareServer: {}", e),
        )
        .build_error()
    })
}

/// 构建长连接服务器
#[allow(clippy::too_many_arguments)]
pub async fn build_long_connection_server(
    runtime_config: &flare_server_core::Config,
    ws_port: u16,
    quic_port: u16,
    connection_manager: Arc<ConnectionManager>,
    authenticator: Arc<dyn flare_core::server::auth::Authenticator + Send + Sync>,
    connection_handler: Arc<LongConnectionHandler>,
    access_config: Arc<AccessGatewayConfig>,
    push_handle_slot: PushHandleSlot,
) -> Result<Arc<tokio::sync::Mutex<Option<FlareServer>>>> {
    use tracing::{error, info, warn};

    // 创建设备管理器（平台互斥策略）
    use flare_core::common::device::DeviceConflictStrategyBuilder;
    use flare_core::server::device::DeviceManager;
    let device_manager = Arc::new(DeviceManager::new(
        DeviceConflictStrategyBuilder::new()
            .platform_exclusive()
            .build(),
    ));

    let ws_addr = format!("{}:{}", runtime_config.server.address, ws_port);
    let quic_addr = format!("{}:{}", runtime_config.server.address, quic_port);

    // 配置压缩和加密
    info!(
        compression_algorithm = ?access_config.compression_algorithm,
        enable_encryption = %access_config.enable_encryption,
        "Reading compression and encryption configuration"
    );

    let compression_algorithm =
        super::config::parse_compression_algorithm(access_config.compression_algorithm.as_deref());

    // 注册加密器（如果启用）
    let encryption_config = super::config::setup_encryption_config(
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
            // 判据优先看**端口是否真的占用**，而不是错误文本。
            //
            // 底层 io::Error 在 flare-core 里被格式化成了字符串，ErrorKind 丢失，
            // 所以这里原本只能匹配 "Address already in use"——那是**依赖系统 locale
            // 的英文文案**：换个语言环境或上游改一版措辞，这条降级就静默失效，
            // 网关不再退回 WS-only 而是直接起不来。
            // 端口探测与语言、与上游措辞都无关；文本匹配保留为兜底（探测与真正 bind
            // 之间存在时间窗，端口可能刚好在这期间被占）。
            let error_msg = e.to_string();
            if quic_port_unavailable(&quic_addr)
                || error_msg.contains("Address already in use")
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
                return Err(ErrorBuilder::new(
                    ErrorCode::InternalError,
                    format!("Failed to build server: {}", e),
                )
                .build_error());
            }
        }
    };

    // 设置 server handle
    setup_server_components(&connection_manager, Some(&push_handle_slot)).await;

    // 启动服务器
    server.start().await.map_err(|e| {
        error!(error = %e, "Failed to start FlareServer");
        ErrorBuilder::new(
            ErrorCode::InternalError,
            format!("Failed to start server: {}", e),
        )
        .build_error()
    })?;

    info!(ws_addr = %ws_addr, quic_addr = %quic_addr, "✅ Long connection server started");

    Ok(Arc::new(tokio::sync::Mutex::new(Some(server))))
}

/// 设置服务器组件
async fn setup_server_components(
    connection_manager: &Arc<ConnectionManager>,
    push_handle_slot: Option<&PushHandleSlot>,
) {
    use flare_core::server::handle::DefaultServerHandle;

    let handle = Arc::new(DefaultServerHandle::new(connection_manager.clone()
        as Arc<dyn flare_core::server::connection::ConnectionManagerTrait>));

    if let Some(slot) = push_handle_slot {
        let mut guard = slot.lock().await;
        *guard = Some(handle.clone());
    }
}

/// QUIC 用的 UDP 端口是否已被占用。
///
/// 只做一次绑定尝试并立刻释放：这是个与语言环境无关的结构化判据，
/// 用来替代对错误文案的字符串匹配。地址解析不了时返回 false —— 那不是
/// 「端口被占」，该让真正的构建流程去报出真实原因。
fn quic_port_unavailable(quic_addr: &str) -> bool {
    use std::net::{ToSocketAddrs, UdpSocket};

    let Ok(mut addrs) = quic_addr.to_socket_addrs() else {
        return false;
    };
    let Some(addr) = addrs.next() else {
        return false;
    };
    match UdpSocket::bind(addr) {
        Ok(_socket) => false, // 立刻 drop，端口随即释放
        Err(err) => err.kind() == std::io::ErrorKind::AddrInUse,
    }
}

#[cfg(test)]
mod quic_port_probe_tests {
    use super::quic_port_unavailable;
    use std::net::UdpSocket;

    #[test]
    fn reports_occupied_port_as_unavailable() {
        let held = UdpSocket::bind("127.0.0.1:0").expect("bind probe port");
        let addr = held.local_addr().expect("local addr").to_string();
        assert!(quic_port_unavailable(&addr), "端口被占用时应判定为不可用");
    }

    #[test]
    fn reports_free_port_as_available() {
        // 先占一个再释放，拿到一个几乎肯定空闲的端口号
        let addr = {
            let probe = UdpSocket::bind("127.0.0.1:0").expect("bind probe port");
            probe.local_addr().expect("local addr").to_string()
        };
        assert!(!quic_port_unavailable(&addr), "端口空闲时不该判定为占用");
    }

    #[test]
    fn unparseable_address_is_not_treated_as_occupied() {
        // 地址不合法不是「端口被占」，该让后续流程报出真实原因
        assert!(!quic_port_unavailable("not-an-address"));
    }
}
