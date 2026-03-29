use std::sync::Arc;

use anyhow::Result;

use flare_im_core::discovery::{
    build_gateway_router_from_app_config, connect_grpc_channel_from_app_config,
};
use flare_im_core::service_names::{ACCESS_GATEWAY, SIGNALING_ONLINE, get_service_name};
use flare_server_core::event_bus::{EventHandler, MqEventHandler};
use flare_server_core::mq::consumer::ConsumerConfig;
use flare_server_core::mq::consumer::dispatcher::Dispatcher;
use flare_server_core::mq::consumer::{MessageHandler, TopicDispatcher};

use crate::application::GatewayPushExecutor;
use crate::config::PushWorkerConfig;
use crate::infrastructure::mq::dlq_publisher::DlqPublisher;
use crate::infrastructure::online_client::OnlineServiceClient;
use crate::interface::messaging::offline_consumer::OfflinePushTaskHandler;
use crate::interface::messaging::online_consumer::OnlinePushTaskHandler;

pub struct ApplicationContext {
    pub config: Arc<PushWorkerConfig>,
    pub consumer_config: ConsumerConfig,
    pub dispatcher: Arc<dyn Dispatcher>,
}

pub async fn initialize(
    app_config: &flare_im_core::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    let config = Arc::new(PushWorkerConfig::from_app_config(app_config));

    let dlq = Arc::new(DlqPublisher::new(config.clone())?);

    let online_service = get_service_name(SIGNALING_ONLINE);
    tracing::info!(
        service = %online_service,
        static_fallback = %config.online_service_endpoint,
        "Resolving Online gRPC (registry or static fallback)"
    );
    let online_channel = connect_grpc_channel_from_app_config(
        app_config,
        &online_service,
        config.online_service_endpoint.as_str(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("Online gRPC channel: {}", e))?;
    let online_client = Arc::new(OnlineServiceClient::from_channel(online_channel));

    let access_gateway_service = get_service_name(ACCESS_GATEWAY);
    let gateway_router = build_gateway_router_from_app_config(
        app_config,
        &access_gateway_service,
        config.access_gateway_static_endpoint.clone(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("GatewayRouter: {}", e))?;

    let gateway_push = Arc::new(GatewayPushExecutor::new(online_client, gateway_router));

    let online_handler = OnlinePushTaskHandler::new(gateway_push, dlq.clone());
    let offline_handler = OfflinePushTaskHandler::new(dlq);

    let consumer_cfg = ConsumerConfig::default().with_concurrency(128);

    let mut dispatcher = TopicDispatcher::new();
    let online_handler: Arc<dyn EventHandler> = Arc::new(online_handler);
    let online_adapter: Arc<dyn MessageHandler> = Arc::new(MqEventHandler::new(online_handler));
    Dispatcher::register(
        &mut dispatcher,
        config.push_online_topic.clone(),
        online_adapter,
    )?;

    let offline_handler: Arc<dyn EventHandler> = Arc::new(offline_handler);
    let offline_adapter: Arc<dyn MessageHandler> = Arc::new(MqEventHandler::new(offline_handler));
    Dispatcher::register(
        &mut dispatcher,
        config.push_offline_topic.clone(),
        offline_adapter,
    )?;

    Ok(ApplicationContext {
        config,
        consumer_config: consumer_cfg,
        dispatcher: Arc::new(dispatcher),
    })
}
