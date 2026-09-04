use axum::extract::DefaultBodyLimit;
use flare_core_runtime::ServiceRuntime;
use flare_im_service_kit::{
    clients::GrpcClients,
    gateway::{GatewayEnvScope, GatewaySettings, require_secure_token_secret},
    service_names::API_GATEWAY,
};
use flare_server_core::error::{AnyhowContext, Result};
use flare_server_core::{
    TokenService,
    auth::{build_token_issuer, build_token_validator},
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::info;

use crate::interface::http::create_public_router;

pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    pub async fn run() -> Result<()> {
        let app_config = flare_im_service_kit::load_config(Some("config"));
        flare_im_service_kit::tracing::init_tracing_from_config(Some(app_config.logging()));

        let settings = GatewaySettings::from_env_for(GatewayEnvScope::Api)?;
        let gateway_config = app_config.api_gateway_service();
        let address: SocketAddr = flare_im_service_kit::ServiceHelper::parse_server_addr(
            app_config,
            &gateway_config.runtime,
            API_GATEWAY,
        )
        .context("invalid api-gateway listen address (check config/services/api-gateway.toml)")?;
        info!(address = %address, "HTTP listen address resolved");

        info!("Connecting to gRPC services...");
        info!(
            media_service_url = %settings.grpc.media_service_url,
            message_ingest_service_url = %settings.grpc.message_ingest_service_url,
            message_orchestrator_service_url = %settings.grpc.message_orchestrator_service_url,
            conversation_service_url = %settings.grpc.conversation_service_url,
            storage_reader_service_url = %settings.grpc.storage_reader_service_url,
            "Using downstream grpc endpoints"
        );
        let clients =
            Arc::new(GrpcClients::new(Arc::new(app_config.clone()), &settings.grpc).await?);
        info!("gRPC clients initialized");

        let token_service = {
            let base = TokenService::new(
                require_secure_token_secret(
                    "FLARE_API_GATEWAY_TOKEN_SECRET",
                    gateway_config.token_secret.as_deref(),
                    "services.api_gateway.token_secret",
                )?,
                gateway_config
                    .token_issuer
                    .clone()
                    .unwrap_or_else(|| "flare-im-core".to_string()),
                gateway_config.token_ttl_seconds.unwrap_or(3600),
            );
            // 刷新令牌有效期（长效，支撑 7x24 免重登）。缺省 30 天。
            let refresh_ttl = gateway_config
                .refresh_token_ttl_seconds
                .unwrap_or(flare_server_core::auth::DEFAULT_REFRESH_TTL_SECS);
            Arc::new(base.with_refresh_ttl(refresh_ttl))
        };
        let auth_validator = build_token_validator(
            &settings.auth,
            token_service.clone(),
            &gateway_config.trusted_token_issuers,
        )
        .context("failed to initialize gateway auth validator")?;
        info!(auth_mode = ?settings.auth.mode, "Gateway auth validator initialized");
        let token_issuer = crate::interface::http::auth_handler::TokenIssuerHandle(
            build_token_issuer(&settings.auth, token_service.clone())
                .context("failed to initialize gateway token issuer")?,
        );
        if settings.auth.dev_issue {
            tracing::warn!(
                "AUTH_DEV_ISSUE is enabled: POST /api/v1/auth/tokens issues a token for ANY userId \
                 without credentials. Local/dev only — anyone reaching this gateway can impersonate any user."
            );
        }
        info!(
            app_credentials = settings.auth.app_credentials.len(),
            dev_issue = settings.auth.dev_issue,
            issuer_ready = token_issuer.0.is_some(),
            "Gateway token issuer initialized"
        );

        let app = create_public_router(clients)
            .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
            .layer(axum::Extension(settings.clone()))
            .layer(axum::Extension(auth_validator))
            .layer(axum::Extension(token_issuer))
            .layer(axum::Extension(token_service))
            .layer(
                ServiceBuilder::new()
                    .layer(TraceLayer::new_for_http())
                    .layer(CorsLayer::permissive()),
            );

        let runtime = flare_im_service_kit::health::attach_runtime_health_checks(
            ServiceRuntime::new(API_GATEWAY)
                .with_address(address)
                .with_health_failure_action(
                    flare_core_runtime::HealthFailureAction::GracefulShutdown,
                )
                .add_spawn_with_shutdown("gateway-http", move |shutdown_rx| async move {
                    let listener = TcpListener::bind(address).await?;
                    info!(address = %address, "✅ Gateway HTTP service is listening");

                    axum::serve(listener, app)
                        .with_graceful_shutdown(async {
                            let _ = shutdown_rx.await;
                        })
                        .await
                        .map_err(|e| e.into())
                }),
            API_GATEWAY,
        );

        Ok(runtime
            .run_with_registration(|addr| {
                Box::pin(async move {
                    flare_im_service_kit::discovery::register_runtime_service_only(
                        API_GATEWAY,
                        addr,
                        None,
                    )
                    .await
                })
            })
            .await?)
    }
}
