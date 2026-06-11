//! 应用启动器 - 负责依赖注入和服务启动

use crate::config::PortConfig;
use crate::service::startup::start_services_with_signals;
use flare_im_contracts::service_names::ACCESS_GATEWAY;
use flare_im_service_kit::FlareAppConfig;
use flare_server_core::error::Result;
use tracing::{error, info};

use super::wire;

/// 应用启动器
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点
    pub async fn run() -> Result<()> {
        Self::run_with_shutdown_signals(Vec::new()).await
    }

    pub async fn run_with_shutdown_signals(
        signals: flare_im_service_kit::RuntimeShutdownSignals,
    ) -> Result<()> {
        let config_path = flare_im_service_kit::resolve_config_path();
        info!(config_path = %config_path, "Loading configuration");
        let app_config = flare_im_service_kit::load_app_config_from_env();

        // 创建应用上下文
        info!("开始创建应用上下文...");
        let context = match Self::create_context(app_config).await {
            Ok(ctx) => {
                info!("✅ 应用上下文创建成功");
                ctx
            }
            Err(e) => {
                error!(error = %e, "❌ 应用上下文创建失败");
                return Err(e);
            }
        };

        // 获取运行时配置
        let service_cfg = app_config.access_gateway_service();
        let runtime_config =
            app_config.compose_service_config(&service_cfg.runtime, ACCESS_GATEWAY);

        // 端口配置：优先使用环境变量
        // 优先级：
        // 1. PORT + GRPC_PORT 环境变量（多网关部署场景，精确控制所有端口）
        // 2. GRPC_PORT 环境变量（只指定 gRPC 端口，自动计算 WebSocket 和 QUIC）
        // 3. PORT 环境变量（指定 WebSocket 端口，QUIC = PORT + 1，gRPC = PORT + 2）
        // 4. 配置端口 + 2（默认情况）
        let port_config = PortConfig::from_env_or_config(runtime_config.server.port);

        // 获取 gateway_id 和 region（从 context 中获取，需要在 create_context 中返回）
        let gateway_id = context.gateway_id.clone();
        let region = context.region.clone();

        // 使用 startup 模块启动服务（会打印详细的启动信息）
        start_services_with_signals(
            context,
            port_config,
            runtime_config.server.address.clone(),
            gateway_id,
            region,
            signals,
        )
        .await
    }

    /// 创建应用上下文
    pub async fn create_context(config: &FlareAppConfig) -> Result<wire::ApplicationContext> {
        use super::wire;

        let service_cfg = config.access_gateway_service();
        let runtime_config = config.compose_service_config(&service_cfg.runtime, ACCESS_GATEWAY);

        // 计算端口分配（使用统一的环境变量解析逻辑）
        let port_config = PortConfig::from_env_or_config(runtime_config.server.port);

        info!(
            "✅ 端口配置: gRPC {}:{}, WebSocket {}:{}, QUIC {}:{}",
            runtime_config.server.address,
            port_config.grpc_port,
            runtime_config.server.address,
            port_config.ws_port,
            runtime_config.server.address,
            port_config.quic_port
        );

        info!("继续构建应用上下文...");
        info!("   注意: 服务注册将在后台任务中异步执行");

        // 使用 Wire 风格的依赖注入构建应用上下文
        let context = wire::initialize(config, &runtime_config, port_config).await?;

        info!("✅ 应用上下文创建完成");

        // 服务注册将在 start_services 中通过 ServiceRuntime 统一管理
        // 这里不再需要后台任务注册

        Ok(context)
    }
}
