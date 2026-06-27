use std::sync::Arc;
use std::time::Duration;

use flare_server_core::error::Result;
use flare_server_core::mq::consumer::ConsumerConfig;
use flare_server_core::mq::consumer::MessageHandler;
use flare_server_core::mq::consumer::TopicDispatcher;
use flare_server_core::mq::consumer::dispatcher::Dispatcher;

use crate::application::PushRouterHandler;
use crate::config::PushServerConfig;
use crate::infrastructure::mq::publisher::PushServerMqPublisher;
use crate::infrastructure::online::online_status_service::OnlineStatusService;
use crate::interface::messaging::{PushEventHandler, PushHandler, PushMessageHandler};

pub struct ApplicationContext {
    pub config: Arc<PushServerConfig>,
    pub consumer_config: ConsumerConfig,
    pub dispatcher: Arc<dyn Dispatcher>,
}

pub async fn initialize(
    app_config: &flare_im_service_kit::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    let config = Arc::new(PushServerConfig::from_app_config(app_config));

    let publisher = Arc::new(PushServerMqPublisher::new(config.clone()).await?);
    let online_status = Arc::new(OnlineStatusService::new(config.clone()).await?);
    let route_handler = Arc::new(
        PushRouterHandler::new(online_status.clone(), online_status, publisher.clone())
            .with_conversation_ping_coalesce_window(Duration::from_millis(
                config.event_ping_coalesce_window_ms,
            )),
    );
    let message_handler = PushMessageHandler::new(route_handler.clone(), publisher.clone());
    let event_handler = PushEventHandler::new(route_handler.clone(), publisher.clone());
    let push_handler = PushHandler::new(route_handler);

    let consumer_cfg = ConsumerConfig::default()
        .with_concurrency(64)
        .with_ordered(true);

    let mut dispatcher = TopicDispatcher::new();

    register_handler(
        &mut dispatcher,
        config.push_message_topic.clone(),
        Arc::new(message_handler),
    )?;
    register_handler(
        &mut dispatcher,
        config.push_event_topic.clone(),
        Arc::new(event_handler),
    )?;
    register_handler(
        &mut dispatcher,
        config.push_envelope_topic.clone(),
        Arc::new(push_handler),
    )?;

    Ok(ApplicationContext {
        config,
        consumer_config: consumer_cfg,
        dispatcher: Arc::new(dispatcher),
    })
}

fn register_handler(
    dispatcher: &mut TopicDispatcher,
    topic: String,
    handler: Arc<dyn MessageHandler>,
) -> Result<()> {
    Dispatcher::register(dispatcher, topic.clone(), handler).map_err(|err| {
        flare_server_core::error::FlareError::system(format!(
            "register push server consumer {topic}: {err}"
        ))
    })
}
