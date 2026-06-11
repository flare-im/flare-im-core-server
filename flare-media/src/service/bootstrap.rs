//! 应用启动器 - 负责依赖注入和服务启动

use flare_server_core::error::Result;
use tracing::info;

use flare_im_contracts::service_names::MEDIA;

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
        let app_config = flare_im_service_kit::load_app_config_from_env();
        let service_config = app_config.media_service();
        let runtime_plan = flare_im_service_kit::build_service_runtime_plan(
            app_config,
            &service_config.runtime,
            MEDIA,
            "MEDIA",
            60081,
        )?;
        info!(address = %runtime_plan.address, "Server address parsed successfully");

        // 使用 Wire 风格的依赖注入构建应用上下文
        let context = wire::initialize(app_config).await?;

        info!("ApplicationBootstrap created successfully");

        // 运行服务
        Self::run_with_context(context, runtime_plan, signals).await
    }

    /// 运行服务（带应用上下文）
    async fn run_with_context(
        context: wire::ApplicationContext,
        runtime_plan: flare_im_service_kit::ImServiceRuntimePlan,
        signals: flare_im_service_kit::RuntimeShutdownSignals,
    ) -> Result<()> {
        use flare_grpc_proto::media::media_service_server::MediaServiceServer;
        use tonic::transport::Server;

        let address = runtime_plan.address;
        let service_name = runtime_plan.service_name.clone();
        let handler = context.handler.clone();

        info!(
            address = %address,
            port = %address.port(),
            "Starting Media gRPC service..."
        );

        let address_clone = address;
        let runtime = runtime_plan.service_runtime().add_spawn_with_shutdown(
            "media-grpc",
            move |shutdown_rx| async move {
                // 使用 ContextLayer 包裹 Service
                use flare_server_core::middleware::ContextLayer;

                let media_service = ContextLayer::new()
                    .allow_missing()
                    .layer(MediaServiceServer::new(handler));

                Server::builder()
                    .add_service(media_service)
                    .serve_with_shutdown(address_clone, async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .map_err(|e| format!("gRPC server error: {}", e).into())
            },
        );

        // 运行服务（带服务注册）
        runtime
            .run_with_registration_and_signals(
                |addr| {
                    Box::pin(async move {
                        flare_im_service_kit::discovery::register_runtime_service_only(
                            &service_name,
                            addr,
                            None,
                        )
                        .await
                    })
                },
                signals,
            )
            .await
            .map_err(flare_server_core::error::FlareError::from)
    }
}
