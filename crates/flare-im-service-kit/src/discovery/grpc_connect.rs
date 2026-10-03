//! 基于 [`crate::config::FlareAppConfig`] 与 [`super::create_discover_from_config`] 的统一 gRPC 连接与 [`GatewayRouter`] 构建。
//!
//! ## 启动 vs 运行时
//! - [`connect_grpc_channel_lazy_from_app_config`]：**进程启动**用，不等待注册中心，避免微服务启动顺序耦合。
//! - [`connect_grpc_channel_from_app_config`]：**首包 RPC / 运行时**用，限时发现，失败回退静态 lazy。
//!
//! 禁止在 `wire::initialize` / `main` 中调用会阻塞等待对端实例上线的连接 API。

use std::sync::Arc;

use tonic::transport::Channel;

use crate::ServiceClient;
use crate::config::FlareAppConfig;
use crate::gateway::{GatewayRouter, GatewayRouterConfig};

use super::channel_resolve::{
    DISCOVERY_CHANNEL_TIMEOUT, STATIC_ACCESS_GATEWAY_GRPC_URI, StaticChannelPolicy,
    connect_static_channel_lazy_with_policy,
};
use super::init::create_discover_from_config;

/// 静态地址的懒连接通道，建连上限与 HTTP/2 探活见 [`StaticChannelPolicy`]。
fn connect_static_lazy(
    static_fallback_uri: &str,
    policy: StaticChannelPolicy,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    Ok(connect_static_channel_lazy_with_policy(
        static_fallback_uri,
        policy,
    )?)
}

/// 启动期连接：立即返回 lazy Channel，**不**等待 Consul / 对端进程。
pub fn connect_grpc_channel_lazy_from_app_config(
    _app_config: &FlareAppConfig,
    _service_type: &str,
    static_fallback_uri: &str,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    connect_static_lazy(static_fallback_uri, StaticChannelPolicy::DEFAULT)
}

/// 运行时连接：限时服务发现，失败则回退静态 lazy（用于首包 RPC 或需立即可用通道的场景）。
/// 静态回退按 [`StaticChannelPolicy::DEFAULT`] 建连。
pub async fn connect_grpc_channel_from_app_config(
    app_config: &FlareAppConfig,
    service_type: &str,
    static_fallback_uri: &str,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    connect_grpc_channel_from_app_config_with_policy(
        app_config,
        service_type,
        static_fallback_uri,
        StaticChannelPolicy::DEFAULT,
    )
    .await
}

/// 同 [`connect_grpc_channel_from_app_config`]，静态回退按给定策略建连（网关按配置的
/// `GRPC_CONNECT_TIMEOUT_SECS`）；注册中心发现到的通道不受该策略影响。
pub async fn connect_grpc_channel_from_app_config_with_policy(
    app_config: &FlareAppConfig,
    service_type: &str,
    static_fallback_uri: &str,
    policy: StaticChannelPolicy,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    connect_grpc_channel_from_app_config_inner(
        app_config,
        service_type,
        static_fallback_uri,
        policy,
    )
    .await
}

async fn connect_grpc_channel_from_app_config_inner(
    app_config: &FlareAppConfig,
    service_type: &str,
    static_fallback_uri: &str,
    policy: StaticChannelPolicy,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    match create_discover_from_config(app_config, service_type).await? {
        Some(discover) => {
            let mut client = ServiceClient::new(discover);
            match tokio::time::timeout(DISCOVERY_CHANNEL_TIMEOUT, client.get_channel()).await {
                Ok(Ok(channel)) => Ok(channel),
                Ok(Err(e)) => {
                    tracing::warn!(
                        service_type,
                        error = %e,
                        fallback = static_fallback_uri,
                        "service discovery failed, using static fallback"
                    );
                    connect_static_lazy(static_fallback_uri, policy)
                }
                Err(_) => {
                    tracing::warn!(
                        service_type,
                        timeout_secs = DISCOVERY_CHANNEL_TIMEOUT.as_secs(),
                        fallback = static_fallback_uri,
                        "service discovery timed out, using static fallback"
                    );
                    connect_static_lazy(static_fallback_uri, policy)
                }
            }
        }
        None => connect_static_lazy(static_fallback_uri, policy),
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
                    static_fallback_endpoint: static_fallback,
                    ..Default::default()
                },
                service_client,
                discover_filter,
            ))
        }
        None => {
            let ep = static_fallback.unwrap_or_else(|| STATIC_ACCESS_GATEWAY_GRPC_URI.to_string());
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
