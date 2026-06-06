//! 服务启动信息展示模块

use std::net::SocketAddr;

use crate::config::PortConfig;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};

/// 启动信息展示器
pub struct StartupInfo {
    /// 网关 ID
    pub gateway_id: String,
    /// 地区
    pub region: Option<String>,
    /// 端口配置
    pub port_config: PortConfig,
    /// 服务器地址
    pub address: String,
    /// gRPC 服务列表
    pub grpc_services: Vec<GrpcServiceInfo>,
}

/// gRPC 服务信息
#[derive(Debug, Clone)]
pub struct GrpcServiceInfo {
    /// 服务名称
    pub name: String,
    /// 服务描述
    pub description: String,
}

impl StartupInfo {
    /// 创建启动信息
    pub fn new(
        gateway_id: String,
        region: Option<String>,
        port_config: PortConfig,
        address: String,
    ) -> Self {
        Self {
            gateway_id,
            region,
            port_config,
            address,
            grpc_services: vec![GrpcServiceInfo {
                name: "AccessGateway".to_string(),
                description: "业务系统推送消息".to_string(),
            }],
        }
    }

    /// 打印启动信息
    pub fn print(&self) {
        use tracing::info;

        info!("");
        info!("╔════════════════════════════════════════════════════════════════╗");
        info!("║          Flare Access Gateway 服务启动成功                    ║");
        info!("╚════════════════════════════════════════════════════════════════╝");
        info!("");

        // 网关信息
        info!("📋 网关信息:");
        info!("   Gateway ID: {}", self.gateway_id);
        if let Some(ref region) = self.region {
            info!("   Region:     {}", region);
        }
        info!("");

        // gRPC 服务信息
        info!("🔌 gRPC 服务 (服务间调用，已注册到服务注册中心):");
        let grpc_addr = format!("{}:{}", self.address, self.port_config.grpc_port);
        info!("   gRPC 地址:  {}", grpc_addr);
        info!("");
        info!("   服务列表:");
        for service in &self.grpc_services {
            info!("     • {} - {}", service.name, service.description);
        }
        info!("");

        // 长连接服务信息
        info!("🌐 长连接服务 (客户端连接):");
        let ws_addr = format!("{}:{}", self.address, self.port_config.ws_port);
        let quic_addr = format!("{}:{}", self.address, self.port_config.quic_port);
        info!(
            "   WebSocket:  {} (ws://{} 或 wss://{})",
            ws_addr, ws_addr, ws_addr
        );
        info!("   QUIC:       {} (quic://{})", quic_addr, quic_addr);
        info!("");

        // 端口映射说明
        info!("📝 端口说明:");
        info!(
            "   • gRPC 端口 ({}) 用于服务间调用，已注册到服务注册中心",
            self.port_config.grpc_port
        );
        info!(
            "   • WebSocket 端口 ({}) 用于客户端 WebSocket 连接",
            self.port_config.ws_port
        );
        info!(
            "   • QUIC 端口 ({}) 用于客户端 QUIC 连接",
            self.port_config.quic_port
        );
        info!("");

        // 连接示例
        info!("💡 连接示例:");
        info!("   客户端连接 WebSocket:");
        info!("     ws://{}/ws", ws_addr);
        info!("   客户端连接 QUIC:");
        info!("     quic://{}", quic_addr);
        info!("   业务系统调用 gRPC:");
        info!("     grpc://{}", grpc_addr);
        info!("");

        info!("✅ 所有服务已就绪，等待客户端连接...");
        info!("");
    }

    /// 获取 gRPC 地址
    pub fn grpc_addr(&self) -> Result<SocketAddr> {
        format!("{}:{}", self.address, self.port_config.grpc_port)
            .parse()
            .map_err(|err| {
                ErrorBuilder::new(
                    ErrorCode::InvalidParameter,
                    format!("Invalid gRPC address: {}", err),
                )
                .build_error()
            })
    }

    /// 获取 WebSocket 地址
    pub fn ws_addr(&self) -> String {
        format!("{}:{}", self.address, self.port_config.ws_port)
    }

    /// 获取 QUIC 地址
    pub fn quic_addr(&self) -> String {
        format!("{}:{}", self.address, self.port_config.quic_port)
    }
}
