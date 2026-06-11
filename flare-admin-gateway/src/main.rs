use axum::extract::DefaultBodyLimit;
use flare_server_core::error::{AnyhowContext, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::info;

use flare_admin_gateway::interface::http::create_admin_router;
use flare_core_runtime::ServiceRuntime;
use flare_im_service_kit::{
    CoreGatewayServiceConfig,
    clients::GrpcClients,
    gateway::{GatewayEnvScope, GatewaySettings, require_secure_token_secret},
    service_names::ADMIN_GATEWAY,
};
use flare_server_core::{TokenService, auth::build_token_validator};

#[tokio::main]
async fn main() -> Result<()> {
    let app_config = flare_im_service_kit::load_config(Some("config"));
    flare_im_service_kit::tracing::init_tracing_from_config(Some(app_config.logging()));

    let settings = GatewaySettings::from_env_for(GatewayEnvScope::Admin)?;
    let admin_config = app_config.admin_gateway_service();
    let core_config = app_config.core_gateway_service();
    let address: SocketAddr = flare_im_service_kit::ServiceHelper::parse_server_addr(
        app_config,
        &admin_config.runtime,
        ADMIN_GATEWAY,
    )
    .context("invalid admin-gateway listen address (check config/services/admin_gateway.toml)")?;
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
    let clients = Arc::new(GrpcClients::new(Arc::new(app_config.clone()), &settings.grpc).await?);
    info!("Admin Gateway gRPC clients initialized");

    let token_service = Arc::new(TokenService::new(
        require_secure_token_secret(
            "FLARE_ADMIN_GATEWAY_TOKEN_SECRET",
            admin_config
                .token_secret
                .as_deref()
                .or(core_config.token_secret.as_deref()),
            "services.admin_gateway.token_secret",
        )?,
        admin_config
            .token_issuer
            .clone()
            .or_else(|| core_config.token_issuer.clone())
            .unwrap_or_else(|| "flare-im-core".to_string()),
        admin_config
            .token_ttl_seconds
            .or(core_config.token_ttl_seconds)
            .unwrap_or(3600),
    ));
    let trusted_issuers = trusted_admin_issuers(&admin_config, &core_config);
    let auth_validator =
        build_token_validator(&settings.auth, token_service.clone(), &trusted_issuers)
            .context("failed to initialize admin-gateway auth validator")?;
    info!(
        auth_mode = ?settings.auth.mode,
        trusted_issuer_count = trusted_issuers.len(),
        "Admin Gateway auth validator initialized"
    );

    let app = create_admin_router(clients)
        .layer(DefaultBodyLimit::max(settings.server.max_body_size))
        .layer(axum::Extension(settings.clone()))
        .layer(axum::Extension(auth_validator))
        .layer(axum::Extension(token_service))
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CorsLayer::permissive()),
        );

    let runtime = flare_im_service_kit::health::attach_runtime_health_checks(
        ServiceRuntime::new(ADMIN_GATEWAY)
            .with_address(address)
            .with_health_failure_action(flare_core_runtime::HealthFailureAction::GracefulShutdown)
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

fn trusted_admin_issuers(
    admin_config: &flare_im_service_kit::AdminGatewayServiceConfig,
    core_config: &CoreGatewayServiceConfig,
) -> Vec<flare_im_service_kit::config::TrustedTokenIssuerConfig> {
    if admin_config.trusted_token_issuers.is_empty() {
        core_config.trusted_token_issuers.clone()
    } else {
        admin_config.trusted_token_issuers.clone()
    }
}
