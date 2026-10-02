use flare_grpc_proto::sync::sync_service_server::SyncServiceServer;
use flare_im_contracts::service_names::SYNC_ORCHESTRATOR;
use flare_server_core::error::Result;
use tonic::transport::Server;

use crate::interface::grpc::SyncOrchestratorGrpcHandler;

pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    pub async fn run() -> Result<()> {
        let app_config = flare_im_service_kit::load_app_config_from_env();
        let service_config = app_config.sync_orchestrator_service();
        let runtime_plan = flare_im_service_kit::build_service_runtime_plan(
            app_config,
            &service_config.runtime,
            SYNC_ORCHESTRATOR,
            "SYNC_ORCHESTRATOR",
            60084,
        )?;
        let address = runtime_plan.address;
        let service_name = runtime_plan.service_name.clone();
        let grpc_metrics =
            flare_im_service_kit::metrics::GrpcMetricsLayer::new(service_name.clone());

        // 同步是按用户读会话与消息的聚合入口：带 x-flare-ctx 就验签并以之为准，只收用户身份；
        // FLARE_REQUIRE_SIGNED_READ_CTX 打开时拒绝未签名调用。验过的令牌随下游读原样转发。
        let read_layer =
            flare_im_service_kit::signed_read::signed_read_layer_from_env(SYNC_ORCHESTRATOR)?
                .allow_actor_types([flare_server_core::SignedActorType::User]);
        let handler = SyncOrchestratorGrpcHandler::new();
        let runtime = runtime_plan.service_runtime().add_spawn_with_shutdown(
            "sync-orchestrator-grpc",
            move |shutdown_rx| async move {
                Server::builder()
                    .layer(grpc_metrics)
                    .add_service(read_layer.layer(SyncServiceServer::new(handler)))
                    .serve_with_shutdown(address, async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .map_err(|e| format!("sync orchestrator grpc error: {}", e).into())
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
