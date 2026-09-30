//! 依赖装配：配置、JetStream 发布器、gRPC Handler

use std::sync::Arc;

use flare_server_core::error::Result;

use crate::application::{PushProxyCommandHandler, PushTaskStatusQuery};
use crate::config::PushProxyConfig;
use crate::infrastructure::{PushProxyMqPublisher, RedisDeviceTokenRegistry, RedisStateStore};
use crate::interface::grpc::PushServiceHandler;

pub struct ApplicationContext {
    pub handler: PushServiceHandler,
}

/// 从应用配置构建应用上下文
pub async fn initialize(
    app_config: &flare_im_service_kit::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    let config = Arc::new(PushProxyConfig::from_app_config(app_config));
    let delivery_refusal = delivery_refusal(&config);
    if let Some(reason) = &delivery_refusal {
        tracing::warn!(%reason, "Push enqueue refused until broker provenance is enforced");
    }
    let publisher = Arc::new(PushProxyMqPublisher::new(config.clone()).await?);
    let store = Arc::new(RedisStateStore::new(config.clone())?);
    let device_tokens = Arc::new(RedisDeviceTokenRegistry::new(config.clone())?);
    let command_handler = Arc::new(PushProxyCommandHandler::new(publisher));
    let status_query = Arc::new(PushTaskStatusQuery::new(store.clone()));
    let handler = PushServiceHandler::new(
        command_handler,
        status_query,
        store,
        device_tokens,
        delivery_refusal,
    );
    Ok(ApplicationContext { handler })
}

const PROVENANCE_ENV: &str = "PUSH_PROXY_EVENT_PROVENANCE_MODE";

/// `None` when the configured broker satisfies the Push provenance mode.
fn delivery_refusal(config: &PushProxyConfig) -> Option<String> {
    flare_im_service_kit::push_delivery::require_push_delivery(
        "Push Proxy",
        PROVENANCE_ENV,
        std::env::var(PROVENANCE_ENV).ok().as_deref(),
        flare_im_service_kit::push_delivery::BrokerEndpoint {
            backend: &config.mq_backend,
            nats_url: &config.jetstream_url,
        },
    )
    .err()
    .map(|error| error.to_string())
}
