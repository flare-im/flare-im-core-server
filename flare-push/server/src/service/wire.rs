use std::sync::Arc;

use anyhow::Result;
use flare_server_core::event_bus::{EventHandler, MqEventHandler};
use flare_server_core::mq::consumer::ConsumerConfig;
use flare_server_core::mq::consumer::MessageHandler;
use flare_server_core::mq::consumer::TopicDispatcher;
use flare_server_core::mq::consumer::dispatcher::Dispatcher;

use crate::application::PushRouterHandler;
use crate::config::PushServerConfig;
use crate::infrastructure::mq::publisher::PushServerMqPublisher;
use crate::infrastructure::online::online_status_service::OnlineStatusService;
use crate::interface::messaging::ack_consumer::PushAckRequestHandler;
use crate::interface::messaging::custom_consumer::PushCustomRequestHandler;
use crate::interface::messaging::event_consumer::PushEventRequestHandler;
use crate::interface::messaging::main_consumer::PushMainRequestHandler;
use crate::interface::messaging::message_consumer::PushMessageRequestHandler;
use crate::interface::messaging::notification_consumer::PushNotificationRequestHandler;

pub struct ApplicationContext {
    pub config: Arc<PushServerConfig>,
    pub consumer_config: ConsumerConfig,
    pub dispatcher: Arc<dyn Dispatcher>,
}

pub async fn initialize(
    app_config: &flare_im_core::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    let config = Arc::new(PushServerConfig::from_app_config(app_config));

    let publisher = Arc::new(PushServerMqPublisher::new(config.clone())?);
    let online_status = Arc::new(OnlineStatusService::new(config.clone()).await?);
    let route_handler = Arc::new(PushRouterHandler::new(online_status, publisher.clone()));
    let message_handler = PushMessageRequestHandler::new(route_handler.clone(), publisher.clone());
    let main_handler = PushMainRequestHandler::new(route_handler.clone(), publisher.clone());
    let event_handler = PushEventRequestHandler::new(route_handler.clone(), publisher.clone());
    let notification_handler =
        PushNotificationRequestHandler::new(route_handler.clone(), publisher.clone());
    let ack_handler = PushAckRequestHandler::new(route_handler.clone(), publisher.clone());
    let custom_handler = PushCustomRequestHandler::new(route_handler, publisher);

    let consumer_cfg = ConsumerConfig::default().with_concurrency(64);

    let mut dispatcher = TopicDispatcher::new();
    let message_handler: Arc<dyn EventHandler> = Arc::new(message_handler);
    let message_adapter: Arc<dyn MessageHandler> = Arc::new(MqEventHandler::new(message_handler));
    Dispatcher::register(
        &mut dispatcher,
        config.push_message_topic.clone(),
        message_adapter,
    )?;

    let main_handler: Arc<dyn EventHandler> = Arc::new(main_handler);
    let main_adapter: Arc<dyn MessageHandler> = Arc::new(MqEventHandler::new(main_handler));
    Dispatcher::register(
        &mut dispatcher,
        config.message_main_topic.clone(),
        main_adapter,
    )?;

    let event_handler: Arc<dyn EventHandler> = Arc::new(event_handler);
    let event_adapter: Arc<dyn MessageHandler> = Arc::new(MqEventHandler::new(event_handler));
    Dispatcher::register(
        &mut dispatcher,
        config.push_event_topic.clone(),
        event_adapter,
    )?;

    let notification_handler: Arc<dyn EventHandler> = Arc::new(notification_handler);
    let notification_adapter: Arc<dyn MessageHandler> =
        Arc::new(MqEventHandler::new(notification_handler));
    Dispatcher::register(
        &mut dispatcher,
        config.push_notification_topic.clone(),
        notification_adapter,
    )?;

    let ack_handler: Arc<dyn EventHandler> = Arc::new(ack_handler);
    let ack_adapter: Arc<dyn MessageHandler> = Arc::new(MqEventHandler::new(ack_handler));
    Dispatcher::register(&mut dispatcher, config.push_ack_topic.clone(), ack_adapter)?;

    let custom_handler: Arc<dyn EventHandler> = Arc::new(custom_handler);
    let custom_adapter: Arc<dyn MessageHandler> = Arc::new(MqEventHandler::new(custom_handler));
    Dispatcher::register(
        &mut dispatcher,
        config.push_custom_topic.clone(),
        custom_adapter,
    )?;

    Ok(ApplicationContext {
        config,
        consumer_config: consumer_cfg,
        dispatcher: Arc::new(dispatcher),
    })
}
