//! 服务启动模块
//!
//! 统一管理服务启动

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

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
    // 必须在 context 被 move 进各任务闭包之前取出来
    let metrics_config = context.metrics_config.clone();
    let usage = context.usage;
    let zone = context.zone.clone();
    let lease = context.lease.clone();

    // 就绪探针挂在指标端口（`/readyz`）：注册完成才就绪；停机一开始转 503，指标端口一直开到
    // 长连接服务器关完连接（`drained`），负载均衡在排空期间探测到的是 503 而不是连接被拒。
    let readiness = Arc::new(flare_im_service_kit::health::Readiness::from_env(
        ACCESS_GATEWAY,
    ));
    let (drained_tx, drained_rx) = tokio::sync::watch::channel(false);
    let long_conn_readiness = readiness.clone();

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
            error!("长连接服务器未启动");
            return Err(
                ErrorBuilder::new(ErrorCode::InternalError, "长连接服务器未启动").build_error(),
            );
        }
        info!("长连接服务器已在 wire.rs 中启动");
    }

    // 配置 Runtime
    let config = RuntimeConfig::default().with_shutdown_timeout(std::time::Duration::from_secs(10));

    // 创建 ServiceRuntime（使用简化模式 + 服务注册）。指标端点随共享运行时挂载：
    // 网关此前只在 wire.rs 打了一句 "Prometheus metrics initialized"，却从没真正起
    // serve_prometheus_metrics，连接数、推送成功/失败/耗时注册了却无处可读。
    let grpc_metrics = flare_im_service_kit::metrics::GrpcMetricsLayer::new(ACCESS_GATEWAY);
    let runtime = flare_im_service_kit::ImServiceRuntimePlan {
        service_name: ACCESS_GATEWAY.to_string(),
        address: grpc_addr,
        // 指标端口由下面带 `/readyz` 的出口接管，这里不再单独起一个。
        metrics: flare_im_service_kit::metrics::MetricsEndpointConfig::disabled(),
    }
    .service_runtime()
    .with_config(config)
    // gRPC 服务任务
    .add_spawn_with_shutdown("grpc-server", move |shutdown_rx| {
        let handler = context.grpc_services.access_gateway_handler.clone();
        let addr = grpc_addr;

        async move {
            info!("Starting gRPC server on {}", addr);

            let service = ContextLayer::new()
                .allow_missing()
                .layer(AccessGatewayServer::new((*handler).clone()));

            let result = Server::builder()
                .layer(grpc_metrics)
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
    // 用量 flush 循环：停机时补发当前窗口
    .add_spawn_with_shutdown("usage-flush", move |shutdown_rx| usage.run(shutdown_rx))
    // 长连接服务器任务
    .add_spawn_with_shutdown("long-conn-server", move |shutdown_rx| {
        let server = context.long_connection_server.clone();

        async move {
            info!("Long connection server is running");

            // 等待 shutdown 信号
            let _ = shutdown_rx.await;
            info!("Long connection server shutdown signal received");
            long_conn_readiness.mark_draining();

            // 停止服务器：按现有断开逻辑关闭全部连接（各连接向 Online 注销）
            if let Some(s) = server.lock().await.take() {
                info!("Stopping long connection server...");
                if let Err(e) = s.stop().await {
                    error!(error = %e, "Failed to stop long connection server");
                } else {
                    info!("Long connection server stopped gracefully");
                }
            }
            // 连接关完再放行租约删除与 `/readyz` 下线
            let _ = drained_tx.send(true);

            Ok(())
        }
    });
    let runtime = flare_im_service_kit::metrics::attach_metrics_endpoint_with_readiness(
        runtime,
        ACCESS_GATEWAY,
        metrics_config,
        readiness.clone(),
        wait_drained(drained_rx.clone(), DRAIN_WAIT),
    );
    // 网关租约续期；停机时等连接关完再删租约
    let runtime = match lease {
        Some(lease) => {
            let drained = wait_drained(drained_rx, DRAIN_WAIT);
            runtime.add_spawn_with_shutdown("gateway-lease", move |shutdown_rx| {
                lease.run(shutdown_rx, drained)
            })
        }
        None => runtime,
    };

    // 运行服务（带服务注册）
    let gateway_id_for_reg = gateway_id.clone();
    let instance_metadata = registration_metadata(region.as_deref(), zone.as_deref());

    runtime
        .run_with_registration(move |addr| {
            let gateway_id_clone = gateway_id_for_reg.clone();
            let metadata = instance_metadata.clone();

            Box::pin(async move {
                let registry =
                    flare_im_service_kit::discovery::register_runtime_service_only_with_metadata(
                        ACCESS_GATEWAY,
                        addr,
                        Some(gateway_id_clone.clone()),
                        (!metadata.is_empty()).then(|| metadata.clone()),
                    )
                    .await?;

                if registry.is_some() {
                    info!(
                        "Service registered: {} (instance_id={}, metadata={:?})",
                        ACCESS_GATEWAY, gateway_id_clone, metadata
                    );
                }
                // 所有任务已就绪、注册已完成：开始对外报就绪
                readiness.mark_ready();

                Ok(registry)
            })
        })
        .await?;

    Ok(())
}

/// 停机时等长连接服务器关完连接的上限：留在运行时 10s 停机时限之内，租约删除与指标端口收尾都还来得及。
const DRAIN_WAIT: std::time::Duration = std::time::Duration::from_secs(8);

/// 等到长连接服务器关完连接（或超过 `limit`）。
async fn wait_drained(mut drained: tokio::sync::watch::Receiver<bool>, limit: std::time::Duration) {
    let _ = tokio::time::timeout(limit, drained.wait_for(|done| *done)).await;
}

/// 写进服务发现实例元数据的 region / zone（未配置的不写）。
fn registration_metadata(region: Option<&str>, zone: Option<&str>) -> HashMap<String, String> {
    [("region", region), ("zone", zone)]
        .into_iter()
        .filter_map(|(key, value)| {
            value
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| (key.to_string(), value.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_metadata_carries_configured_region_and_zone() {
        let metadata = registration_metadata(Some("cn-north"), Some("az-1"));
        assert_eq!(metadata.get("region").map(String::as_str), Some("cn-north"));
        assert_eq!(metadata.get("zone").map(String::as_str), Some("az-1"));
        assert!(registration_metadata(None, Some(" ")).is_empty());
    }

    #[tokio::test]
    async fn drain_wait_returns_when_connections_are_closed_or_the_limit_passes() {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let waiting = tokio::spawn(wait_drained(rx.clone(), std::time::Duration::from_secs(5)));
        tx.send(true).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .expect("drained signal releases the wait")
            .unwrap();

        let (_tx, rx) = tokio::sync::watch::channel(false);
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            wait_drained(rx, std::time::Duration::from_millis(20)),
        )
        .await
        .expect("the limit bounds the wait");
    }
}
