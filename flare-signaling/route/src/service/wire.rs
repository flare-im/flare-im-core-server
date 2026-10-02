//! Wire 风格的依赖注入模块
//!
//! 类似 Go 的 Wire 框架，提供简单的依赖构建方法

use std::sync::Arc;

use flare_im_contracts::utils::normalize_tenant_id;
use flare_server_core::SignedContextVerifier;
use flare_server_core::error::{AnyhowContext, Result};

use crate::application::handlers::{
    AckRoutingHandler, DataRoutingHandler, EventRoutingHandler, MessageRoutingHandler,
};
use crate::config::RouteConfig;
use crate::domain::value_objects::DefaultFlowController;
use crate::infrastructure::{AckToPushProxyForwarder, forwarder::MessageForwarder};
use crate::interface::grpc::RouterUpstreamHandler;

/// 应用上下文 - 包含所有已初始化的服务
pub struct ApplicationContext {
    pub upstream_handler: RouterUpstreamHandler,
}

/// 构建应用上下文
pub async fn initialize(
    app_config: &flare_im_service_kit::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    let route_config = Arc::new(
        RouteConfig::from_app_config(app_config)
            .with_context(|| "Failed to load route service configuration")?,
    );

    let default_tenant_id = route_config
        .default_tenant_id
        .clone()
        .map(normalize_tenant_id)
        .unwrap_or_else(|| "0".to_string());
    let message_forwarder = Arc::new(MessageForwarder::new(default_tenant_id));

    let flow_controller = Arc::new(DefaultFlowController::new());
    let ack_to_push_proxy = AckToPushProxyForwarder::new(message_forwarder.clone());
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

    let message_verifier = match std::env::var("FLARE_INTERNAL_CTX_PUBLIC_KEY") {
        Ok(key) => Some(SignedContextVerifier::from_public_key(&key).map_err(|_| {
            flare_server_core::error::FlareError::system(
                "invalid FLARE_INTERNAL_CTX_PUBLIC_KEY for signaling route",
            )
        })?),
        Err(std::env::VarError::NotPresent) => {
            tracing::warn!("message routing disabled: internal context verifier missing");
            None
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(flare_server_core::error::FlareError::system(
                "invalid FLARE_INTERNAL_CTX_PUBLIC_KEY encoding",
            ));
        }
    };
    let upstream_handler = RouterUpstreamHandler::new(
        message_routing_handler,
        event_routing_handler,
        ack_routing_handler,
        data_routing_handler,
        message_verifier,
    );

    Ok(ApplicationContext { upstream_handler })
}
