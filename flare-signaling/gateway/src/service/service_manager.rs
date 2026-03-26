//! 统一服务管理器
//!
//! 负责管理所有服务（gRPC 和长连接）的生命周期，实现统一的启动和停止机制

use crate::service::wire::ApplicationContext;
use anyhow::Result;
use std::net::SocketAddr;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tracing::{error, info, warn};

/// 端口配置
#[derive(Debug, Clone)]
pub struct PortConfig {
    /// gRPC 端口（主端口，用于服务注册）
    pub grpc_port: u16,
    /// WebSocket 端口（自动计算：grpc_port - 2）
    pub ws_port: u16,
    /// QUIC 端口（自动计算：grpc_port - 1）
    pub quic_port: u16,
}

impl PortConfig {
    /// 从 gRPC 端口创建端口配置（当只指定了 gRPC 端口时）
    ///
    /// # 端口分配规则
    /// - WebSocket: grpc_port - 2
    /// - QUIC: grpc_port - 1
    /// - gRPC: grpc_port
    pub fn from_grpc_port(grpc_port: u16) -> Self {
        // 确保端口不会溢出
        let ws_port = grpc_port.saturating_sub(2);
        let quic_port = grpc_port.saturating_sub(1);

        Self {
            grpc_port,
            ws_port,
            quic_port,
        }
    }

    /// 从 WebSocket 端口创建端口配置（当指定了 PORT 环境变量时）
    ///
    /// # 端口分配规则
    /// - WebSocket: ws_port
    /// - QUIC: ws_port + 1
    /// - gRPC: grpc_port（需要单独指定）
    pub fn from_ws_port(ws_port: u16, grpc_port: u16) -> Self {
        let quic_port = ws_port.saturating_add(1);

        Self {
            grpc_port,
            ws_port,
            quic_port,
        }
    }

    /// 从环境变量或配置创建端口配置
    ///
    /// 优先级：
    /// 1. PORT + GRPC_PORT 环境变量（多网关部署场景）
    /// 2. GRPC_PORT 环境变量（只指定 gRPC 端口）
    /// 3. PORT 环境变量（PORT + 2 作为 gRPC 端口）
    /// 4. 配置中的端口 + 2（作为 gRPC 端口）
    pub fn from_env_or_config(config_port: u16) -> Self {
        // 检查是否同时指定了 PORT 和 GRPC_PORT（多网关部署场景）
        if let (Ok(env_port), Ok(env_grpc_port)) =
            (std::env::var("PORT"), std::env::var("GRPC_PORT"))
            && let (Ok(ws_port), Ok(grpc_port)) =
                (env_port.parse::<u16>(), env_grpc_port.parse::<u16>())
            {
                info!("使用环境变量 PORT={} 和 GRPC_PORT={}", ws_port, grpc_port);
                return Self::from_ws_port(ws_port, grpc_port);
            }

        // 只指定了 GRPC_PORT
        if let Ok(env_grpc_port) = std::env::var("GRPC_PORT")
            && let Ok(port) = env_grpc_port.parse::<u16>() {
                info!("使用环境变量 GRPC_PORT={} 作为 gRPC 端口", port);
                return Self::from_grpc_port(port);
            }

        // 只指定了 PORT
        if let Ok(env_port) = std::env::var("PORT")
            && let Ok(port) = env_port.parse::<u16>() {
                let grpc_port = port + 2;
                info!(
                    "使用环境变量 PORT={}，gRPC 端口 = {} (PORT + 2)",
                    port, grpc_port
                );
                return Self::from_ws_port(port, grpc_port);
            }

        // 默认：使用配置端口 + 2 作为 gRPC 端口
        let grpc_port = config_port + 2;
        Self::from_grpc_port(grpc_port)
    }
}

/// 服务管理器
///
/// 统一管理所有服务的生命周期，包括：
/// - gRPC 服务（AccessGateway）
/// - 长连接服务（WebSocket、QUIC）
pub struct ServiceManager {
    /// 应用上下文
    context: ApplicationContext,
    /// 端口配置
    port_config: PortConfig,
    /// 服务器地址
    address: String,
    /// gRPC 服务器任务句柄
    grpc_server_handle: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
    /// 停止信号发送器
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl ServiceManager {
    /// 创建服务管理器
    pub fn new(context: ApplicationContext, port_config: PortConfig, address: String) -> Self {
        Self {
            context,
            port_config,
            address,
            grpc_server_handle: None,
            shutdown_tx: None,
        }
    }

    /// 启动所有服务
    ///
    /// # 服务启动顺序
    /// 1. 长连接服务器（已在 create_context 中启动）
    /// 2. gRPC 服务器
    /// 3. 等待停止信号
    pub async fn start(&mut self) -> Result<()> {
        // 启动 gRPC 服务器
        self.start_grpc_server().await?;

        // 等待停止信号
        self.wait_for_shutdown().await?;

        Ok(())
    }

    /// 启动 gRPC 服务器
    async fn start_grpc_server(&mut self) -> Result<()> {
        let grpc_addr: SocketAddr = format!("{}:{}", self.address, self.port_config.grpc_port)
            .parse()
            .map_err(|err| anyhow::anyhow!("Invalid gRPC address: {}", err))?;

        let access_gateway_handler = self.context.grpc_services.access_gateway_handler.clone();

        info!("正在启动 gRPC 服务器: {}", grpc_addr);

        // 创建停止信号通道
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        self.shutdown_tx = Some(shutdown_tx);

        // 启动 gRPC 服务器任务
        let grpc_server_handle = tokio::spawn(async move {
            // 添加上下文中间件（自动提取和注入 TenantContext 和 RequestContext）
            use flare_server_core::middleware::ContextLayer;
            
            // 使用 graceful_shutdown 支持优雅停机
            // 使用 ContextLayer 包裹 Service
            
            let access_gateway_service = ContextLayer::new()
                .allow_missing()
                .layer(
                    flare_proto::access_gateway::access_gateway_server::AccessGatewayServer::new(
                        (*access_gateway_handler).clone(),
                    ),
                );
            
            let server_result = Server::builder()
                .add_service(access_gateway_service)
                .serve_with_shutdown(grpc_addr, async {
                    shutdown_rx.await.ok();
                    info!("收到停止信号，正在停止 gRPC 服务器...");
                })
                .await;

            match server_result {
                Ok(_) => {
                    info!("gRPC 服务器已停止");
                    Ok(())
                }
                Err(e) => {
                    error!(error = %e, "gRPC 服务器启动失败");
                    Err(e)
                }
            }
        });

        self.grpc_server_handle = Some(grpc_server_handle);

        // 等待一小段时间确保服务器启动
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        info!("✅ gRPC 服务器已启动: {}", grpc_addr);

        Ok(())
    }

    /// 等待停止信号并执行优雅停机
    async fn wait_for_shutdown(&mut self) -> Result<()> {
        info!("服务器运行中，按 Ctrl+C 停止...");

        // 等待 Ctrl+C 信号
        tokio::signal::ctrl_c().await?;

        info!("\n正在停止服务器...");

        // 执行优雅停机
        self.shutdown().await;

        info!("✅ 服务器已停止");
        Ok(())
    }

    /// 优雅停机
    ///
    /// 停止顺序：
    /// 1. 发送停止信号给 gRPC 服务器
    /// 2. 等待 gRPC 服务器停止
    /// 3. 停止长连接服务器
    /// 4. 注销服务注册
    async fn shutdown(&mut self) {
        // 1. 停止 gRPC 服务器
        if let Some(shutdown_tx) = self.shutdown_tx.take()
            && shutdown_tx.send(()).is_err() {
                warn!("停止信号发送失败（gRPC 服务器可能已停止）");
            }

        // 等待 gRPC 服务器停止
        if let Some(handle) = self.grpc_server_handle.take() {
            // 先克隆 handle 的引用，以便在超时情况下可以 abort
            let handle_for_abort = handle.abort_handle();

            match tokio::time::timeout(tokio::time::Duration::from_secs(5), handle).await {
                Ok(Ok(_)) => {
                    info!("gRPC 服务器已停止");
                }
                Ok(Err(e)) => {
                    warn!(error = %e, "gRPC 服务器停止时出错");
                }
                Err(_) => {
                    warn!("等待 gRPC 服务器停止超时，强制停止");
                    handle_for_abort.abort();
                }
            }
        }

        // 2. 停止长连接服务器
        if let Some(server) = self.context.long_connection_server.lock().await.take() {
            info!("正在停止长连接服务器...");
            if let Err(e) = server.stop().await {
                warn!(error = %e, "停止长连接服务器失败");
            } else {
                info!("长连接服务器已停止");
            }
        }

        // 3. 注销服务注册
        info!("正在注销服务注册...");
        // 服务注销由 ServiceRuntime 统一管理，这里不需要手动注销
    }
}
