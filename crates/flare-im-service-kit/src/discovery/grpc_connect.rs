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
use crate::config::{FlareAppConfig, ServiceRuntimeConfig};
use crate::gateway::{GatewayRouter, GatewayRouterConfig};
use crate::service_names::{
    CAPABILITY, CONVERSATION, MEDIA, MESSAGE_INGEST, ORCHESTRATOR, SIGNALING_ONLINE,
    SIGNALING_ROUTE, STORAGE_READER, SYNC_ORCHESTRATOR,
};
use flare_server_core::error::{ErrorBuilder, ErrorCode};

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

/// Resolve a same-Cell dependency from its canonical typed listener configuration.
/// Logical registry aliases must never select the service's runtime section.
/// Missing explicit configuration is an error, rather than another Cell's default port.
pub fn static_grpc_endpoint_from_app_config(
    app: &FlareAppConfig,
    dependency: &str,
) -> flare_server_core::error::Result<String> {
    let (runtime, prefix): (Option<&ServiceRuntimeConfig>, &str) = match dependency {
        CONVERSATION => (
            app.services.conversation.as_ref().map(|s| &s.runtime),
            "CONVERSATION",
        ),
        STORAGE_READER => (
            app.services.storage_reader.as_ref().map(|s| &s.runtime),
            "STORAGE_READER",
        ),
        SIGNALING_ONLINE => (
            app.services.signaling_online.as_ref().map(|s| &s.runtime),
            "SIGNALING_ONLINE",
        ),
        SIGNALING_ROUTE => (
            app.services.signaling_route.as_ref().map(|s| &s.runtime),
            "SIGNALING_ROUTE",
        ),
        SYNC_ORCHESTRATOR => (
            app.services.sync_orchestrator.as_ref().map(|s| &s.runtime),
            "SYNC_ORCHESTRATOR",
        ),
        MESSAGE_INGEST => (
            app.services.message_ingest.as_ref().map(|s| &s.runtime),
            "MESSAGE_INGEST",
        ),
        ORCHESTRATOR => (
            app.services
                .message_orchestrator
                .as_ref()
                .map(|s| &s.runtime),
            "MESSAGE_ORCHESTRATOR",
        ),
        MEDIA => (app.services.media.as_ref().map(|s| &s.runtime), "MEDIA"),
        CAPABILITY => (
            app.services.capability.as_ref().map(|s| &s.runtime),
            "CAPABILITY",
        ),
        _ => {
            return Err(ErrorBuilder::new(
                ErrorCode::ConfigurationError,
                format!("unsupported static gRPC dependency: {dependency}"),
            )
            .build_error());
        }
    };
    let runtime = runtime
        .filter(|runtime| runtime.server.as_ref().and_then(|s| s.port).is_some())
        .ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::ConfigurationError,
                format!("static {dependency} requires an explicit service listener port"),
            )
            .build_error()
        })?;
    let mut address =
        crate::build_service_runtime_plan(app, runtime, dependency, prefix, 0)?.address;
    if address.port() == 0 {
        return Err(ErrorBuilder::new(
            ErrorCode::ConfigurationError,
            format!("static {dependency} requires a nonzero listener port"),
        )
        .build_error());
    }
    if address.ip().is_unspecified() {
        address.set_ip(if address.is_ipv4() {
            std::net::Ipv4Addr::LOCALHOST.into()
        } else {
            std::net::Ipv6Addr::LOCALHOST.into()
        });
    }
    Ok(format!("http://{address}"))
}

/// 启动期连接：立即返回 lazy Channel，**不**等待 Consul / 对端进程。
pub fn connect_grpc_channel_lazy_from_app_config(
    app_config: &FlareAppConfig,
    dependency: &str,
    service_type: &str,
    static_fallback_uri: &str,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    connect_grpc_channel_lazy_in_mode(
        app_config,
        app_config.uses_static_registry(),
        dependency,
        service_type,
        static_fallback_uri,
    )
}

fn connect_grpc_channel_lazy_in_mode(
    app_config: &FlareAppConfig,
    static_registry_mode: bool,
    dependency: &str,
    _service_type: &str,
    static_fallback_uri: &str,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    if static_registry_mode {
        let endpoint = static_grpc_endpoint_from_app_config(app_config, dependency)?;
        return connect_static_lazy(&endpoint, StaticChannelPolicy::DEFAULT);
    }
    connect_static_lazy(static_fallback_uri, StaticChannelPolicy::DEFAULT)
}

/// 运行时连接：限时服务发现，失败则回退静态 lazy（用于首包 RPC 或需立即可用通道的场景）。
/// 静态回退按 [`StaticChannelPolicy::DEFAULT`] 建连。
pub async fn connect_grpc_channel_from_app_config(
    app_config: &FlareAppConfig,
    dependency: &str,
    service_type: &str,
    static_fallback_uri: &str,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    connect_grpc_channel_from_app_config_with_policy(
        app_config,
        dependency,
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
    dependency: &str,
    service_type: &str,
    static_fallback_uri: &str,
    policy: StaticChannelPolicy,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    connect_grpc_channel_from_app_config_inner(
        app_config,
        app_config.uses_static_registry(),
        dependency,
        service_type,
        static_fallback_uri,
        policy,
    )
    .await
}

async fn connect_grpc_channel_from_app_config_inner(
    app_config: &FlareAppConfig,
    static_registry_mode: bool,
    dependency: &str,
    service_type: &str,
    static_fallback_uri: &str,
    policy: StaticChannelPolicy,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    if static_registry_mode {
        let endpoint = static_grpc_endpoint_from_app_config(app_config, dependency)?;
        return connect_static_lazy(&endpoint, policy);
    }
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
    let router = build_gateway_router_without_lease(
        app_config,
        access_gateway_service_name,
        static_fallback,
    )
    .await?;
    // 网关租约与 Online 会话同在一个 Redis；找得到就挂探针，宕机网关上的设备按离线处理。
    // 找不到（没配会话 Redis）就不查租约，路由行为与以前一致。
    let Some(url) = crate::gateway::lease::session_redis_url(app_config) else {
        tracing::info!("Online session redis not configured; gateway lease checks disabled");
        return Ok(router);
    };
    match crate::gateway::lease::GatewayLeaseProbe::from_redis_url(&url) {
        Ok(probe) => router.attach_lease_probe(Arc::new(probe)),
        Err(error) => tracing::warn!(
            %error,
            "invalid Online session redis url; gateway lease checks disabled"
        ),
    }
    Ok(router)
}

async fn build_gateway_router_without_lease(
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

#[cfg(test)]
#[path = "grpc_connect_tests.rs"]
pub(crate) mod tests;
