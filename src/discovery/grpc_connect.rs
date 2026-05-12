//! 基于 [`crate::config::FlareAppConfig`] 与 [`super::create_discover_from_config`] 的统一 gRPC 连接与 [`GatewayRouter`] 构建。
//!
//! 有注册中心时通过 `flare_server_core::discovery::ServiceDiscover` / [`ServiceClient`] 解析；无注册中心时回退到静态 URI（本地开发）。
//! 禁止在业务 crate 中手写重复的 `create_discover` 双份与 `GatewayRouter::with_service_client_and_discover` 拼装。

use std::sync::Arc;
use std::time::Duration;

use tonic::transport::{Channel, Endpoint};

use crate::ServiceClient;
use crate::config::FlareAppConfig;
use crate::gateway::{GatewayRouter, GatewayRouterConfig};

use super::init::create_discover_from_config;

/// 为指定逻辑服务名建立一条 gRPC [`Channel`]（有注册中心则发现；否则连接 `static_fallback_uri`）。
pub async fn connect_grpc_channel_from_app_config(
    app_config: &FlareAppConfig,
    service_type: &str,
    static_fallback_uri: &str,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    match create_discover_from_config(app_config, service_type).await? {
        Some(discover) => {
            let mut client = ServiceClient::new(discover);
            let channel = client.get_channel().await?;
            Ok(channel)
        }
        None => {
            let endpoint = Endpoint::from_shared(static_fallback_uri.to_string())?;
            let timeout = Duration::from_secs(10);
            let channel = tokio::time::timeout(timeout, endpoint.connect())
                .await
                .map_err(|_| {
                    format!(
                        "timeout connecting to static gRPC endpoint {}",
                        static_fallback_uri
                    )
                })?
                .map_err(|e| {
                    format!(
                        "failed to connect to static gRPC endpoint {}: {}",
                        static_fallback_uri, e
                    )
                })?;
            Ok(channel)
        }
    }
}

/// 构建 [`GatewayRouter`]：有注册中心时注入双份 `ServiceDiscover` + [`ServiceClient`]；无则静态回退。
pub async fn build_gateway_router_from_app_config(
    app_config: &FlareAppConfig,
    access_gateway_service_name: &str,
    static_fallback: Option<String>,
) -> Result<Arc<GatewayRouter>, Box<dyn std::error::Error + Send + Sync>> {
    match create_discover_from_config(app_config, access_gateway_service_name).await? {
        Some(discover_lb) => {
            let discover_filter =
                create_discover_from_config(app_config, access_gateway_service_name)
                    .await?
                    .ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
                        "access gateway second discover missing (registry misconfigured)".into()
                    })?;
            let service_client = ServiceClient::new(discover_lb);
            Ok(GatewayRouter::with_service_client_and_discover(
                GatewayRouterConfig {
                    access_gateway_service: access_gateway_service_name.to_string(),
                    ..Default::default()
                },
                service_client,
                discover_filter,
            ))
        }
        None => {
            let ep = static_fallback.unwrap_or_else(|| "http://127.0.0.1:60051".to_string());
            tracing::info!(
                endpoint = %ep,
                "No registry configured; GatewayRouter using static Access Gateway endpoint"
            );
            Ok(GatewayRouter::new(GatewayRouterConfig {
                access_gateway_service: access_gateway_service_name.to_string(),
                static_fallback_endpoint: Some(ep),
                ..Default::default()
            }))
        }
    }
}
