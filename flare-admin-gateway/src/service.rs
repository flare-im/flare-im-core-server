use axum::extract::DefaultBodyLimit;
use flare_core_runtime::ServiceRuntime;
use flare_im_service_kit::{
    clients::{CapabilityAdminClientWrapper, DownstreamGrpcResolver, GrpcClients},
    control_auth::ConsoleControlVerifier,
    gateway::{GatewayEnvScope, GatewaySettings, capability_downstream_from_env},
    service_names::ADMIN_GATEWAY,
};
use flare_server_core::SignedContextSigner;
use flare_server_core::error::{AnyhowContext, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{info, warn};

use crate::interface::http::{AdminCapabilityDownstream, MediaContextSigner, create_admin_router};

pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    pub async fn run() -> Result<()> {
        let app_config = flare_im_service_kit::load_config(Some("config"));
        let _tracing =
            flare_im_service_kit::tracing::init_tracing_from_config(Some(app_config.logging()))?;

        let settings = GatewaySettings::from_env_for(GatewayEnvScope::Admin)?;
        let admin_config = app_config.admin_gateway_service();
        let configured_address: SocketAddr =
            flare_im_service_kit::ServiceHelper::parse_server_addr(
                app_config,
                &admin_config.runtime,
                ADMIN_GATEWAY,
            )
            .context(
                "invalid admin-gateway listen address (check config/services/admin-gateway.toml)",
            )?;
        let address = match std::env::var("FLARE_ADMIN_GATEWAY_BIND_ADDRESS") {
            Ok(value) => value
                .parse::<SocketAddr>()
                .context("invalid FLARE_ADMIN_GATEWAY_BIND_ADDRESS")?,
            Err(std::env::VarError::NotPresent) => configured_address,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(flare_server_core::error::FlareError::system(
                    "FLARE_ADMIN_GATEWAY_BIND_ADDRESS has invalid encoding",
                ));
            }
        };
        info!(address = %address, "Admin Gateway HTTP listen address resolved");

        info!("Connecting Admin Gateway to downstream gRPC services...");
        info!(
            storage_reader_service_url = %settings.grpc.storage_reader_service_url,
            message_ingest_service_url = %settings.grpc.message_ingest_service_url,
            message_orchestrator_service_url = %settings.grpc.message_orchestrator_service_url,
            conversation_service_url = %settings.grpc.conversation_service_url,
            media_service_url = %settings.grpc.media_service_url,
            "Using downstream grpc endpoints for admin typed facade"
        );
        let clients =
            Arc::new(GrpcClients::new(Arc::new(app_config.clone()), &settings.grpc).await?);
        info!("Admin Gateway gRPC clients initialized");

        // 租户运行时、能力开关与插件（ADM-01）只经这条可选下游；URL 设为空串即关闭，
        // 对应三条路由回 503，其余管理接口不受影响。
        let capability = match capability_downstream_from_env(GatewayEnvScope::Admin)? {
            Some(config) => {
                info!(
                    capability_service_url = %config.service_url,
                    "Using capability downstream for admin tenant runtime, switches and plugins"
                );
                let resolver = DownstreamGrpcResolver::new(
                    Arc::new(app_config.clone()),
                    settings.grpc.clone(),
                );
                let client = CapabilityAdminClientWrapper::connect(&resolver, &config).await?;
                Some(AdminCapabilityDownstream { config, client })
            }
            None => {
                warn!("capability downstream disabled; admin capability routes answer 503");
                None
            }
        };

        let control_verifier = Arc::new(
            ConsoleControlVerifier::from_env()
                .context("invalid FLARE_CONSOLE_PUBLIC_KEY")?
                .context("FLARE_CONSOLE_PUBLIC_KEY required for admin gateway")?,
        );
        info!("Admin Gateway Console public-key verifier initialized");

        // Media admits only Cell-signed context; the Control-verified admin is signed here.
        let media_signer = match std::env::var("FLARE_INTERNAL_CTX_PRIVATE_KEY") {
            Ok(material) => Some(SignedContextSigner::from_private_key(&material).map_err(
                |_| {
                    flare_server_core::error::FlareError::system(
                        "invalid FLARE_INTERNAL_CTX_PRIVATE_KEY for admin gateway",
                    )
                },
            )?),
            Err(_) => {
                warn!("FLARE_INTERNAL_CTX_PRIVATE_KEY missing; admin media API fails closed");
                None
            }
        };

        let app = create_admin_router(clients, capability)
            .layer(flare_im_service_kit::metrics::HttpMetricsLayer::new(
                ADMIN_GATEWAY,
            ))
            .layer(DefaultBodyLimit::max(settings.server.max_body_size))
            .layer(axum::Extension(settings.clone()))
            .layer(axum::Extension(control_verifier))
            .layer(axum::Extension(MediaContextSigner(media_signer)))
            .layer(
                ServiceBuilder::new()
                    .layer(TraceLayer::new_for_http())
                    .layer(CorsLayer::permissive()),
            );

        let runtime = flare_im_service_kit::health::attach_runtime_health_checks(
            ServiceRuntime::new(ADMIN_GATEWAY)
                .with_address(address)
                .with_health_failure_action(
                    flare_core_runtime::HealthFailureAction::GracefulShutdown,
                )
                .add_spawn_with_shutdown("admin-gateway-http", move |shutdown_rx| async move {
                    let listener = TcpListener::bind(address).await?;
                    info!(address = %address, "Admin Gateway HTTP service is listening");

                    axum::serve(listener, app)
                        .with_graceful_shutdown(async {
                            let _ = shutdown_rx.await;
                        })
                        .await
                        .map_err(|e| e.into())
                }),
            ADMIN_GATEWAY,
        );
        let metrics = flare_im_service_kit::metrics::MetricsEndpointConfig::from_section(
            ADMIN_GATEWAY,
            admin_config.runtime.metrics.as_ref(),
        )?;
        let runtime =
            flare_im_service_kit::metrics::attach_metrics_endpoint(runtime, ADMIN_GATEWAY, metrics);

        Ok(runtime
            .run_with_registration(|addr| {
                Box::pin(async move {
                    flare_im_service_kit::discovery::register_runtime_service_only(
                        ADMIN_GATEWAY,
                        addr,
                        None,
                    )
                    .await
                })
            })
            .await?)
    }
}
