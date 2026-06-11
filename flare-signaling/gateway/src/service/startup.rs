//! 服务启动模块
//!
//! 统一管理服务启动

use std::net::SocketAddr;

use crate::config::PortConfig;
use crate::service::display::StartupInfo;
use crate::service::wire::ApplicationContext;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tracing::{error, info};

/// 启动服务
pub async fn start_services(
    context: ApplicationContext,
    port_config: PortConfig,
    address: String,
    gateway_id: String,
    region: Option<String>,
) -> Result<()> {
    start_services_with_signals(
        context,
        port_config,
        address,
        gateway_id,
        region,
        Vec::new(),
    )
    .await
}

pub async fn start_services_with_signals(
    context: ApplicationContext,
    port_config: PortConfig,
    address: String,
    gateway_id: String,
    region: Option<String>,
    signals: flare_im_service_kit::RuntimeShutdownSignals,
) -> Result<()> {
    use flare_core_runtime::RuntimeConfig;
    use flare_grpc_proto::access_gateway::access_gateway_server::AccessGatewayServer;
    use flare_im_contracts::service_names::ACCESS_GATEWAY;
    use flare_server_core::middleware::ContextLayer;
    use tonic::transport::Server;

    // 创建并打印启动信息
    let startup_info = StartupInfo::new(
        gateway_id.clone(),
        region.clone(),
        port_config.clone(),
        address.clone(),
    );
    startup_info.print();

    // 解析 gRPC 地址
    let grpc_addr: SocketAddr = format!("{}:{}", address, port_config.grpc_port)
        .parse()
        .map_err(|err| {
            ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                format!("Invalid gRPC address: {}", err),
            )
            .build_error()
        })?;

    // 验证长连接服务器已启动
    {
        let server_guard = context.long_connection_server.lock().await;
        if server_guard.is_none() {
            error!("❌ 长连接服务器未启动");
            return Err(
                ErrorBuilder::new(ErrorCode::InternalError, "长连接服务器未启动").build_error(),
            );
        }
        info!("✅ 长连接服务器已在 wire.rs 中启动");
    }

    // 配置 Runtime
    let config = RuntimeConfig::default().with_shutdown_timeout(std::time::Duration::from_secs(10));

    // 创建 ServiceRuntime（使用简化模式 + 服务注册）
    let runtime = flare_im_service_kit::ImServiceRuntimePlan {
        service_name: ACCESS_GATEWAY.to_string(),
        address: grpc_addr,
    }
    .service_runtime()
    .with_config(config)
    // gRPC 服务任务
    .add_spawn_with_shutdown("grpc-server", move |shutdown_rx| {
        let handler = context.grpc_services.access_gateway_handler.clone();
        let addr = grpc_addr;

        async move {
            info!("🚀 Starting gRPC server on {}", addr);

            let service = ContextLayer::new()
                .allow_missing()
                .layer(AccessGatewayServer::new((*handler).clone()));

            let result = Server::builder()
                .add_service(service)
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.await;
                    info!("gRPC server shutdown signal received");
                })
                .await;

            match result {
                Ok(_) => {
                    info!("gRPC server stopped gracefully");
                    Ok(())
                }
                Err(e) => {
                    error!(error = %e, "gRPC server failed");
                    Err(format!("gRPC server error: {}", e).into())
                }
            }
        }
    })
    // 长连接服务器任务
    .add_spawn_with_shutdown("long-conn-server", move |shutdown_rx| {
        let server = context.long_connection_server.clone();

        async move {
            info!("✅ Long connection server is running");

            // 等待 shutdown 信号
            let _ = shutdown_rx.await;
            info!("Long connection server shutdown signal received");

            // 停止服务器
            if let Some(s) = server.lock().await.take() {
                info!("Stopping long connection server...");
                if let Err(e) = s.stop().await {
                    error!(error = %e, "Failed to stop long connection server");
                } else {
                    info!("Long connection server stopped gracefully");
                }
            }

            Ok(())
        }
    });

    // 运行服务（带服务注册）
    let gateway_id_for_reg = gateway_id.clone();
    let region_for_reg = region.clone();

    runtime
        .run_with_registration_and_signals(
            move |addr| {
                let gateway_id_clone = gateway_id_for_reg.clone();
                let region_clone = region_for_reg.clone();

                Box::pin(async move {
                    let registry = flare_im_service_kit::discovery::register_runtime_service_only(
                        ACCESS_GATEWAY,
                        addr,
                        Some(gateway_id_clone.clone()),
                    )
                    .await?;

                    if registry.is_some() {
                        info!(
                            "✅ Service registered: {} (instance_id={}, region={:?})",
                            ACCESS_GATEWAY, gateway_id_clone, region_clone
                        );
                    }

                    Ok(registry)
                })
            },
            signals,
        )
        .await?;

    Ok(())
}
