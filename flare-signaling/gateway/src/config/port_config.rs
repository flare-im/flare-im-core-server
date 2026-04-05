//! 端口配置模块
//!
//! 管理服务的端口分配和配置

use tracing::info;

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
            && let Ok(port) = env_grpc_port.parse::<u16>()
        {
            info!("使用环境变量 GRPC_PORT={} 作为 gRPC 端口", port);
            return Self::from_grpc_port(port);
        }

        // 只指定了 PORT
        if let Ok(env_port) = std::env::var("PORT")
            && let Ok(port) = env_port.parse::<u16>()
        {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_grpc_port() {
        let config = PortConfig::from_grpc_port(8080);
        assert_eq!(config.grpc_port, 8080);
        assert_eq!(config.ws_port, 8078);
        assert_eq!(config.quic_port, 8079);
    }

    #[test]
    fn test_from_ws_port() {
        let config = PortConfig::from_ws_port(8078, 8080);
        assert_eq!(config.grpc_port, 8080);
        assert_eq!(config.ws_port, 8078);
        assert_eq!(config.quic_port, 8079);
    }

    #[test]
    fn test_port_overflow() {
        let config = PortConfig::from_grpc_port(1);
        assert_eq!(config.grpc_port, 1);
        assert_eq!(config.ws_port, 0); // 防止溢出
        assert_eq!(config.quic_port, 0);
    }
}
