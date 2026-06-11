//! 应用启动器 - 负责依赖注入和服务启动

use flare_server_core::error::Result;
use tracing::info;

use crate::service::wire::{self, ApplicationContext};
use flare_im_contracts::service_names::SIGNALING_ROUTE;

/// 应用启动器
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点
    pub async fn run() -> Result<()> {
        let app_config = flare_im_service_kit::load_app_config_from_env();
        let service_config = app_config.signaling_route_service();
        let runtime_plan = flare_im_service_kit::build_service_runtime_plan(
            app_config,
            &service_config.runtime,
            SIGNALING_ROUTE,
            "SIGNALING_ROUTE",
            50062,
        )?;
        info!(address = %runtime_plan.address, "Server address parsed successfully");

        let context = wire::initialize(app_config).await?;

        info!("ApplicationBootstrap created successfully");

        Self::run_with_context(context, runtime_plan).await
    }

    /// 运行服务（带应用上下文）
    async fn run_with_context(
        context: ApplicationContext,
        runtime_plan: flare_im_service_kit::ImServiceRuntimePlan,
    ) -> Result<()> {
        use flare_grpc_proto::signaling::router::router_upstream_service_server::RouterUpstreamServiceServer;
        use tonic::transport::Server;

        let address = runtime_plan.address;
        let service_name = runtime_plan.service_name.clone();
        let upstream_handler = context.upstream_handler;

        info!(
            address = %address,
            port = %address.port(),
            "Starting Router gRPC service (upstream only; downstream push lives in flare-push-worker)"
        );

        let address_clone = address;
        let runtime = runtime_plan.service_runtime().add_spawn_with_shutdown(
            "router-grpc",
            move |shutdown_rx| async move {
                use flare_server_core::middleware::ContextLayer;

                let upstream_service = ContextLayer::new()
                    .allow_missing()
                    .layer(RouterUpstreamServiceServer::new(upstream_handler));

                Server::builder()
                    .add_service(upstream_service)
                    .serve_with_shutdown(address_clone, async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .map_err(|e| format!("gRPC server error: {}", e).into())
            },
        );

        runtime
            .run_with_registration(|addr| {
                Box::pin(async move {
                    flare_im_service_kit::discovery::register_runtime_service_only(
                        &service_name,
                        addr,
                        None,
                    )
                    .await
                })
            })
            .await
            .map_err(flare_server_core::error::FlareError::from)
    }
}
