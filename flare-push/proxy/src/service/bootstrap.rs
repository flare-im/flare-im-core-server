//! 应用启动：gRPC PushService 监听

use flare_server_core::error::Result;
use tracing::info;

use crate::service::wire::{self, ApplicationContext};
use flare_im_contracts::service_names::PUSH_PROXY;

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
        let service_config = app_config.push_proxy_service();
        let runtime_plan = flare_im_service_kit::build_service_runtime_plan(
            app_config,
            &service_config.runtime,
            PUSH_PROXY,
            "PUSH_PROXY",
            50090,
        )?;
        info!(address = %runtime_plan.address, "Push Proxy listen address");

        let context = wire::initialize(app_config).await?;
        Self::run_with_context(context, runtime_plan, signals).await
    }

    async fn run_with_context(
        context: ApplicationContext,
        runtime_plan: flare_im_service_kit::ImServiceRuntimePlan,
        signals: flare_im_service_kit::RuntimeShutdownSignals,
    ) -> Result<()> {
        use flare_grpc_proto::push::push_service_server::PushServiceServer;
        use tonic::transport::Server;

        let address = runtime_plan.address;
        let service_name = runtime_plan.service_name.clone();
        let handler = context.handler.clone();
        let address_clone = address;

        let runtime = runtime_plan.service_runtime().add_spawn_with_shutdown(
            "push-proxy-grpc",
            move |shutdown_rx| async move {
                use flare_server_core::middleware::ContextLayer;

                let svc = ContextLayer::new()
                    .allow_missing()
                    .layer(PushServiceServer::new(handler));

                Server::builder()
                    .add_service(svc)
                    .serve_with_shutdown(address_clone, async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .map_err(|e| format!("gRPC server error: {}", e).into())
            },
        );

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
