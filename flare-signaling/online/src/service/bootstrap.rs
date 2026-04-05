//! 应用启动器 - 负责依赖注入和服务启动

use std::net::SocketAddr;

use tracing::info;

use crate::service::wire::{self, ApplicationContext};
use crate::error::{map_infra_error, ErrorCode, Result};
use flare_im_core::service_names::SIGNALING_ONLINE;
use flare_core_runtime::ServiceRuntime;

/// 应用启动器
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点
    pub async fn run() -> Result<()> {
        use flare_im_core::{ServiceHelper, load_config};

        // 加载应用配置
        let app_config = load_config(Some("./config"));
        let service_config = app_config.signaling_online_service();

        info!("Parsing server address...");
        let address: SocketAddr =
            ServiceHelper::parse_server_addr(app_config, &service_config.runtime, SIGNALING_ONLINE)
                .map_err(|e| map_infra_error(e, ErrorCode::InvalidParameter, "invalid signaling online server address"))?;
        info!(address = %address, "Server address parsed successfully");

        // 使用 Wire 风格的依赖注入构建应用上下文
        let context = wire::initialize(app_config).await?;

        info!("ApplicationBootstrap created successfully");

        // 运行服务
        Self::run_with_context(context, address).await
    }

    /// 运行服务（带应用上下文）
    async fn run_with_context(context: ApplicationContext, address: SocketAddr) -> Result<()> {
        use flare_grpc_proto::signaling::online::online_service_server::OnlineServiceServer;
        use tonic::transport::Server;

        let online_handler = context.online_handler.clone();

        info!(
            address = %address,
            port = %address.port(),
            "Starting Signaling Online gRPC service..."
        );

        // 使用 ServiceRuntime 管理服务生命周期
        let address_clone = address;
        let runtime = flare_im_core::health::attach_runtime_health_checks(
            ServiceRuntime::new(SIGNALING_ONLINE)
                .with_address(address)
                .with_health_failure_action(flare_core_runtime::HealthFailureAction::GracefulShutdown)
                .add_spawn_with_shutdown("signaling-online-grpc", move |shutdown_rx| async move {
                    // 使用 ContextLayer 包裹 Service
                    use flare_server_core::middleware::ContextLayer;

                    let online_service = ContextLayer::new()
                        .allow_missing()
                        .layer(OnlineServiceServer::new(online_handler));

                    Server::builder()
                        .add_service(online_service)
                        .serve_with_shutdown(address_clone, async {
                            let _ = shutdown_rx.await;
                        })
                        .await
                        .map_err(|e| format!("gRPC server error: {}", e).into())
                }),
            SIGNALING_ONLINE,
        );

        // 运行服务（带服务注册）
        runtime
            .run_with_registration(|addr| {
                Box::pin(async move {
                    flare_im_core::discovery::register_runtime_service_only(
                        SIGNALING_ONLINE,
                        addr,
                        None,
                    )
                    .await
                })
            })
            .await
            .map_err(|e| map_infra_error(anyhow::anyhow!("{}", e), ErrorCode::InternalError, "Runtime error"))
    }
}
