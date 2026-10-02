use std::sync::Arc;
use std::time::Duration;

use flare_im_service_kit::push_delivery::{
    AttestedRecordHandler, BrokerRecordIdentity, PushDeliveryMode,
};
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
    mode: PushDeliveryMode,
) -> Result<ApplicationContext> {
    let config = Arc::new(PushServerConfig::from_app_config(app_config));
    // Every consumed record is re-attested as this service for its tenant.
    let identity = BrokerRecordIdentity::from_env(mode, "flare-push-server")?;
    let attested = |handler: Arc<dyn MessageHandler>| -> Arc<dyn MessageHandler> {
        Arc::new(AttestedRecordHandler::new(identity.clone(), handler))
    };

    let publisher = Arc::new(PushServerMqPublisher::new(config.clone()).await?);
    let online_status = Arc::new(OnlineStatusService::new(config.clone()).await?);
    let policy = Arc::new(crate::infrastructure::ConversationNotifyPolicy::new());
    let route_handler = Arc::new(
        PushRouterHandler::new(online_status.clone(), online_status, publisher.clone())
            // 同一个实现同时提供通知偏好与全员枚举：读的是同一份参与者数据。
            .with_notify_policy(policy)
            .with_conversation_ping_coalesce_window(Duration::from_millis(
                config.event_ping_coalesce_window_ms,
            )),
    );
    let message_handler = PushMessageHandler::new(route_handler.clone(), publisher.clone());
    let event_handler = PushEventHandler::new(route_handler.clone(), publisher.clone());
    let push_handler = PushHandler::new(route_handler);

    let consumer_cfg = push_server_consumer_config();

    let mut dispatcher = TopicDispatcher::new();

    register_handler(
        &mut dispatcher,
        config.push_message_topic.clone(),
        attested(Arc::new(message_handler)),
    )?;
    register_handler(
        &mut dispatcher,
        config.push_event_topic.clone(),
        attested(Arc::new(event_handler)),
    )?;
    register_handler(
        &mut dispatcher,
        config.push_envelope_topic.clone(),
        attested(Arc::new(push_handler)),
    )?;

    Ok(ApplicationContext {
        config,
        consumer_config: consumer_cfg,
        dispatcher: Arc::new(dispatcher),
    })
}

/// push-server 消费者配置。
///
/// 路由失败（在线状态查不到、发布到 push-online/push-offline 失败）多是下游停机或重启引起的
/// 暂时性失败，handler 报可重试错误，交给 JetStream 按
/// [`flare_im_service_kit::consumer_retry`] 的退避延迟重投：12 次重试的等待合计 135 s，之后
/// 仍失败才进 `flare.im.dlq.push-server`。默认的 3 次立即重投会在下游停机的同一瞬间用完。
pub(crate) fn push_server_consumer_config() -> ConsumerConfig {
    flare_im_service_kit::consumer_retry::with_downstream_outage_retry(
        ConsumerConfig::default()
            .with_concurrency(64)
            .with_ordered(true),
    )
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

#[cfg(test)]
mod wire_tests {
    use super::*;
    use flare_server_core::mq::nats::NatsConsumerConfig;

    fn push_server_nats_config() -> PushServerConfig {
        PushServerConfig {
            mq_backend: "nats".into(),
            jetstream_url: "nats://127.0.0.1:1".into(),
            jetstream_timeout_ms: 1_000,
            jetstream_retries: 0,
            jetstream_retry_backoff_ms: 0,
            jetstream_stream_specs: flare_server_core::mq::nats::default_stream_specs(),
            kafka_brokers: Vec::new(),
            kafka_client_id: String::new(),
            kafka_options: Default::default(),
            consumer_group: "push-server".into(),
            push_message_topic: String::new(),
            push_event_topic: String::new(),
            push_envelope_topic: String::new(),
            push_online_topic: String::new(),
            push_offline_topic: String::new(),
            push_dlq_topic: String::new(),
            redis_url: String::new(),
            online_status_backend: "redis".into(),
            online_service_endpoint: String::new(),
            event_ping_coalesce_window_ms: 0,
            default_tenant_id: "0".into(),
        }
    }

    /// 门禁：push-server 的重试预算覆盖下游停机与重启窗口，且首投加全部重试落在它自己的
    /// JetStream 消费者的 `max_deliver` 之内。
    #[test]
    fn push_server_retries_span_a_restart_window_inside_max_deliver() {
        let consumer = push_server_consumer_config();
        let max_deliver = push_server_nats_config().max_deliver();
        let window = flare_im_service_kit::consumer_retry::check_downstream_outage_retry(
            &consumer,
            max_deliver,
        )
        .unwrap_or_else(|error| panic!("push-server retry budget: {error}"));
        println!(
            "push-server retries {} over {window:?}, max_deliver {max_deliver}",
            consumer.max_retries
        );
        assert!(consumer.ordered);
        assert!(consumer.enable_dlq);
    }
}
