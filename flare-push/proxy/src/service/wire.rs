//! 依赖装配：配置、Kafka 发布器、gRPC Handler

use std::sync::Arc;

use anyhow::Result;

use crate::application::{PushProxyCommandHandler, PushTaskStatusQuery};
use crate::config::PushProxyConfig;
use crate::infrastructure::{PushProxyMqPublisher, RedisStateStore};
use crate::interface::grpc::PushServiceHandler;

pub struct ApplicationContext {
    pub handler: PushServiceHandler,
}

/// 从应用配置构建应用上下文
pub async fn initialize(
    app_config: &flare_im_core::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    let config = Arc::new(PushProxyConfig::from_app_config(app_config));
    let publisher = Arc::new(PushProxyMqPublisher::new(config.clone())?);
    let store = Arc::new(RedisStateStore::new(config.clone())?);
    let command_handler = Arc::new(PushProxyCommandHandler::new(publisher));
    let status_query = Arc::new(PushTaskStatusQuery::new(store.clone()));
    let handler = PushServiceHandler::new(command_handler, status_query, store);
    Ok(ApplicationContext { handler })
}
