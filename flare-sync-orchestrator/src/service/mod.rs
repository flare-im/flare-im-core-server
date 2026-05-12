use anyhow::{Context, Result};
use flare_core_runtime::ServiceRuntime;
use flare_grpc_proto::sync::sync_service_server::SyncServiceServer;
use flare_im_core::service_names::SYNC_ORCHESTRATOR;
use std::net::SocketAddr;
use tonic::transport::Server;

use crate::interface::grpc::SyncOrchestratorGrpcHandler;

pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    pub async fn run() -> Result<()> {
        // 初始化全局配置，供 discovery/register_service_only 使用。
        let _ = flare_im_core::load_config(Some("config"));

        let host =
            std::env::var("SYNC_ORCHESTRATOR_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        let port = std::env::var("SYNC_ORCHESTRATOR_PORT")
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(60084);
        let address: SocketAddr = format!("{}:{}", host, port)
            .parse()
            .context("invalid sync orchestrator server address")?;

        let handler = SyncOrchestratorGrpcHandler::new();
        let runtime = flare_im_core::health::attach_runtime_health_checks(
            ServiceRuntime::new(SYNC_ORCHESTRATOR)
                .with_address(address)
                .with_health_failure_action(
                    flare_core_runtime::HealthFailureAction::GracefulShutdown,
                )
                .add_spawn_with_shutdown("sync-orchestrator-grpc", move |shutdown_rx| async move {
                    Server::builder()
                        .add_service(SyncServiceServer::new(handler))
                        .serve_with_shutdown(address, async {
                            let _ = shutdown_rx.await;
                        })
                        .await
                        .map_err(|e| format!("sync orchestrator grpc error: {}", e).into())
                }),
            SYNC_ORCHESTRATOR,
        );

        runtime
            .run_with_registration(|addr| {
                Box::pin(async move {
                    flare_im_core::discovery::register_runtime_service_only(
                        SYNC_ORCHESTRATOR,
                        addr,
                        None,
                    )
                    .await
                })
            })
            .await
    }
}
