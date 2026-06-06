use axum::extract::DefaultBodyLimit;
use flare_server_core::error::{AnyhowContext, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::info;

use flare_core_gateway::interface::http::create_public_router;
use flare_core_runtime::ServiceRuntime;
use flare_im_core::{
    clients::GrpcClients,
    gateway::{GatewayEnvScope, GatewaySettings, require_secure_token_secret},
    service_names::CORE_GATEWAY,
};
use flare_server_core::{TokenService, auth::build_token_validator};

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化全局 FlareAppConfig（Consul 等服务注册依赖 `app_config()`）
    let app_config = flare_im_core::load_config(Some("config"));
    flare_im_core::tracing::init_tracing_from_config(Some(app_config.logging()));

    // gRPC/限流等仍走环境变量；HTTP 监听与仓库内 check_services.sh（50050）及 config/services/core_gateway.toml 对齐
    let settings = GatewaySettings::from_env_for(GatewayEnvScope::Core)?;
    let gateway_config = app_config.core_gateway_service();
    let address: SocketAddr = flare_im_core::ServiceHelper::parse_server_addr(
        app_config,
        &gateway_config.runtime,
        CORE_GATEWAY,
    )
    .context("invalid core-gateway listen address (check config/services/core_gateway.toml)")?;
    info!(address = %address, "HTTP listen address resolved");

    // 初始化 gRPC 客户端
    info!("Connecting to gRPC services...");
    info!(
        media_service_url = %settings.grpc.media_service_url,
        message_service_url = %settings.grpc.message_service_url,
        conversation_service_url = %settings.grpc.conversation_service_url,
        storage_reader_service_url = %settings.grpc.storage_reader_service_url,
        "Using downstream grpc endpoints"
    );
    let clients = Arc::new(GrpcClients::new(Arc::new(app_config.clone()), &settings.grpc).await?);
    info!("gRPC clients initialized");

    let token_service = Arc::new(TokenService::new(
        require_secure_token_secret(
            "FLARE_CORE_GATEWAY_TOKEN_SECRET",
            gateway_config.token_secret.as_deref(),
            "services.core_gateway.token_secret",
        )?,
        gateway_config
            .token_issuer
            .clone()
            .unwrap_or_else(|| "flare-im-core".to_string()),
        gateway_config.token_ttl_seconds.unwrap_or(3600),
    ));
    let auth_validator = build_token_validator(
        &settings.auth,
        token_service.clone(),
        &gateway_config.trusted_token_issuers,
    )
    .context("failed to initialize gateway auth validator")?;
    info!(auth_mode = ?settings.auth.mode, "Gateway auth validator initialized");

    // 创建路由
    let app = create_public_router(clients)
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
        .layer(axum::Extension(settings.clone()))
        .layer(axum::Extension(auth_validator))
        .layer(axum::Extension(token_service))
        .layer(
            ServiceBuilder::new()
                // 追踪
                .layer(TraceLayer::new_for_http())
                // CORS
                .layer(CorsLayer::permissive()),
        );

    // 使用 ServiceRuntime 管理 HTTP 服务
    let runtime = flare_im_core::health::attach_runtime_health_checks(
        ServiceRuntime::new(CORE_GATEWAY)
            .with_address(address)
            .with_health_failure_action(flare_core_runtime::HealthFailureAction::GracefulShutdown)
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
        CORE_GATEWAY,
    );

    // 运行时启动（支持服务注册）
    Ok(runtime
        .run_with_registration(|addr| {
            Box::pin(async move {
                flare_im_core::discovery::register_runtime_service_only(CORE_GATEWAY, addr, None)
                    .await
            })
        })
        .await?)
}
