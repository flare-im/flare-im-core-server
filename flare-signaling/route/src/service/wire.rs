//! Wire 风格的依赖注入模块
//!
//! 类似 Go 的 Wire 框架，提供简单的依赖构建方法

use std::sync::Arc;

use anyhow::{Context as AnyhowContext, Result};

use crate::application::handlers::{
    AckRoutingHandler, DataRoutingHandler, EventRoutingHandler, MessageRoutingHandler,
};
use crate::config::RouteConfig;
use crate::domain::value_objects::DefaultFlowController;
use crate::infrastructure::{
    AckToPushProxyForwarder, GrpcConnectionPool, GrpcConnectionPoolConfig,
    forwarder::MessageForwarder,
};
use crate::interface::grpc::RouterUpstreamHandler;

/// 应用上下文 - 包含所有已初始化的服务
pub struct ApplicationContext {
    pub upstream_handler: RouterUpstreamHandler,
}

/// 构建应用上下文
pub async fn initialize(
    app_config: &flare_im_core::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    let route_config = Arc::new(
        RouteConfig::from_app_config(app_config)
            .with_context(|| "Failed to load route service configuration")?,
    );

    let default_tenant_id = route_config
        .default_tenant_id
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let message_forwarder = Arc::new(MessageForwarder::new(default_tenant_id));

    let _connection_pool = Arc::new(GrpcConnectionPool::new(GrpcConnectionPoolConfig::default()));

    let flow_controller = Arc::new(DefaultFlowController::new());
    let ack_to_push_proxy = AckToPushProxyForwarder::new();
    let message_routing_handler = Arc::new(MessageRoutingHandler::new(
        message_forwarder.clone(),
        Some(flow_controller.clone()),
    ));
    let event_routing_handler = Arc::new(EventRoutingHandler::new(
        message_forwarder.clone(),
        Some(flow_controller),
    ));
    let ack_routing_handler = Arc::new(AckRoutingHandler::new(ack_to_push_proxy));
    let data_routing_handler = Arc::new(DataRoutingHandler::new(message_forwarder));

    let upstream_handler = RouterUpstreamHandler::new(
        message_routing_handler,
        event_routing_handler,
        ack_routing_handler,
        data_routing_handler,
    );

    Ok(ApplicationContext { upstream_handler })
}
