//! Wire 风格的依赖注入模块
//!
//! 类似 Go 的 Wire 框架，提供简单的依赖构建方法

use std::sync::Arc;

use anyhow::{Context, Result};

use crate::config::GatewayConfig;
use crate::infrastructure::{GrpcHookClient, GrpcMediaClient, GrpcMessageClient, GrpcOnlineClient};
use crate::interface::grpc::handler::SimpleGatewayHandler;

/// 应用上下文 - 包含所有已初始化的服务
pub struct ApplicationContext {
    /// gRPC 透明代理（Media / Hook / Message / Online）
    pub grpc_handler: SimpleGatewayHandler,
}

/// 构建应用上下文
///
/// 类似 Go Wire 的 Initialize 函数，按照依赖顺序构建所有组件
///
/// # 参数
/// * `app_config` - 应用配置
///
/// # 返回
/// * `ApplicationContext` - 构建好的应用上下文
pub async fn initialize(
    app_config: &flare_im_core::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    // 1. 加载配置
    let _gateway_config =
        GatewayConfig::from_app_config(app_config).context("Failed to load gateway config")?;

    // 2. 创建服务发现（使用常量，支持环境变量覆盖）
    use flare_im_core::service_names::{ORCHESTRATOR, get_service_name};

    // 2.1 Media 服务发现
    let media_service = get_service_name("MEDIA");
    let media_discover = flare_im_core::discovery::create_discover(&media_service)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to create media service discover for {}: {}",
                media_service,
                e
            )
        })?;

    let media_service_client = if let Some(discover) = media_discover {
        Some(flare_server_core::discovery::ServiceClient::new(discover))
    } else {
        None
    };

    // 2.2 Hook 服务发现
    let hook_service = get_service_name("HOOK_ENGINE");
    let hook_discover = flare_im_core::discovery::create_discover(&hook_service)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to create hook service discover for {}: {}",
                hook_service,
                e
            )
        })?;

    let hook_service_client = if let Some(discover) = hook_discover {
        Some(flare_server_core::discovery::ServiceClient::new(discover))
    } else {
        None
    };

    // 2.3 Message 服务发现（Orchestrator）
    let message_service = get_service_name(ORCHESTRATOR);
    let message_discover = flare_im_core::discovery::create_discover(&message_service)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to create message service discover for {}: {}",
                message_service,
                e
            )
        })?;

    let message_service_client = if let Some(discover) = message_discover {
        Some(flare_server_core::discovery::ServiceClient::new(discover))
    } else {
        None
    };

    // 2.4 Online 服务发现
    let online_service = get_service_name("SIGNALING_ONLINE");
    let online_discover = flare_im_core::discovery::create_discover(&online_service)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to create online service discover for {}: {}",
                online_service,
                e
            )
        })?;

    let online_service_client = if let Some(discover) = online_discover {
        Some(flare_server_core::discovery::ServiceClient::new(discover))
    } else {
        None
    };

    // 3. 创建基础设施客户端
    let media_client = if let Some(service_client) = media_service_client {
        Arc::new(GrpcMediaClient::with_service_client(
            service_client,
            media_service.clone(),
        ))
    } else {
        Arc::new(GrpcMediaClient::new(media_service.clone()))
    };

    let hook_client = if let Some(service_client) = hook_service_client {
        Arc::new(GrpcHookClient::with_service_client(
            service_client,
            hook_service.clone(),
        ))
    } else {
        Arc::new(GrpcHookClient::new(hook_service.clone()))
    };

    let message_client = if let Some(service_client) = message_service_client {
        Arc::new(GrpcMessageClient::with_service_client(
            service_client,
            message_service.clone(),
        ))
    } else {
        Arc::new(GrpcMessageClient::new(message_service.clone()))
    };

    let online_client = if let Some(service_client) = online_service_client {
        Arc::new(GrpcOnlineClient::with_service_client(
            service_client,
            online_service.clone(),
        ))
    } else {
        Arc::new(GrpcOnlineClient::new(online_service.clone()))
    };

    let grpc_handler = SimpleGatewayHandler::new(
        media_client,
        hook_client,
        message_client,
        online_client,
    );

    Ok(ApplicationContext { grpc_handler })
}
