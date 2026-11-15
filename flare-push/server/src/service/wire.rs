use std::sync::Arc;
use std::time::Duration;

use flare_im_contracts::constants::topics::TOPIC_PUSH_HANDOFF;
use flare_im_service_kit::metrics::PushServerMetrics;
use flare_im_service_kit::push_delivery::{
    AttestedRecordHandler, BrokerRecordIdentity, PushDeliveryMode,
};
use flare_im_service_kit::push_trace::PushTraceRecorder;
use flare_server_core::error::Result;
use flare_server_core::mq::consumer::ConsumerConfig;
use flare_server_core::mq::consumer::MessageHandler;
use flare_server_core::mq::consumer::TopicDispatcher;
use flare_server_core::mq::consumer::dispatcher::Dispatcher;

use crate::application::{NotifyGate, NotifyWaves, PushRouterHandler};
use crate::config::PushServerConfig;
use crate::infrastructure::mq::publisher::PushServerMqPublisher;
use crate::infrastructure::notify::GateKeys;
use crate::infrastructure::online::online_status_service::OnlineStatusService;
use crate::infrastructure::{
    CachedDeviceBindings, CachedUserNotifySettings, ConversationRecentUnread,
    ConversationUserNotifySettings, MemoryGateStore, RedisDeviceBindingRegistry,
    RedisDeviceSignals, RedisGateStore, StorageMessageLookup,
};
use crate::interface::messaging::{
    PushEventHandler, PushHandler, PushHandoffHandler, PushMessageHandler,
};
use crate::interface::timer::NotifyDueLoop;

pub struct ApplicationContext {
    pub config: Arc<PushServerConfig>,
    pub consumer_config: ConsumerConfig,
    pub dispatcher: Arc<dyn Dispatcher>,
    /// 防打扰闸门的到期循环；闸门关闭时为空。
    pub notify_due: Option<NotifyDueLoop>,
}

/// 防打扰闸门与群会话的波次：状态在同一个推送 Redis，不可用时退到同一个进程内存储（两者的到期
/// 条目都由闸门的到期循环领取）；设备信号读 Online 写在会话 Redis 里的标记；用户级设置经会话
/// 服务读，进程内缓存 60 秒。
async fn build_notify_gate(
    config: &PushServerConfig,
    metrics: Arc<PushServerMetrics>,
) -> Result<Option<(Arc<NotifyGate>, Arc<NotifyWaves>)>> {
    let settings = &config.notify;
    if !settings.enabled {
        tracing::info!(
            "notify gate disabled: offline tasks are emitted per message and group messages enumerate their recipients one by one"
        );
        return Ok(None);
    }
    let store = Arc::new(
        RedisGateStore::connect(
            &settings.redis_url,
            GateKeys::new(settings.key_prefix.clone(), settings.due_shards),
        )
        .await?,
    );
    let fallback = Arc::new(MemoryGateStore::fallback());
    let policy = Arc::new(settings.policy());
    let user_settings =
        CachedUserNotifySettings::new(Arc::new(ConversationUserNotifySettings::new()));
    let mut gate = NotifyGate::new(store.clone(), fallback.clone(), policy.clone())
        .with_settings(Arc::new(user_settings))
        .with_metrics(metrics.clone());
    // 设备信号只是提示：会话 Redis 连不上时闸门照常工作，只是不知道设备回过前台、角标没有基线。
    match RedisDeviceSignals::connect(&config.redis_url).await {
        Ok(signals) => gate = gate.with_signals(Arc::new(signals)),
        Err(error) => tracing::warn!(
            error = %error,
            "device signals unavailable; the gate runs without back-to-active markers and badge bases"
        ),
    }
    let waves = NotifyWaves::new(store, fallback, policy).with_metrics(metrics);
    Ok(Some((Arc::new(gate), Arc::new(waves))))
}

/// 推送决策链写在闸门所在的推送 Redis（`[redis.push]` 或 `PUSH_SERVER_GATE_REDIS_URL`）。只进有界
/// 队列、后台批量写，Redis 不可用或地址写错时只少了决策链，推送照常。
fn push_trace_recorder(config: &PushServerConfig) -> PushTraceRecorder {
    if !config.trace.enabled {
        return PushTraceRecorder::disabled();
    }
    match redis::Client::open(config.notify.redis_url.as_str()) {
        Ok(client) => PushTraceRecorder::spawn(client, config.trace.clone()),
        Err(error) => {
            tracing::warn!(error = %error, "push decision trace disabled: push Redis address is invalid");
            PushTraceRecorder::disabled()
        }
    }
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
    // 设备推送绑定：直读 Proxy 登记的绑定表，前面加一层进程内缓存。
    let device_bindings = Arc::new(CachedDeviceBindings::new(Arc::new(
        RedisDeviceBindingRegistry::connect(
            &config.device_token_redis_url,
            config.device_token_key_prefix.clone(),
            config.binding_inactive_days,
        )
        .await?,
    )));
    let metrics = Arc::new(PushServerMetrics::new());
    let notify = build_notify_gate(&config, metrics.clone()).await?;
    let mut route_handler = PushRouterHandler::new(
        online_status.clone(),
        online_status,
        device_bindings,
        publisher.clone(),
    )
    // 同一个实现同时提供通知偏好、成员与订阅者的枚举：读的是同一份参与者数据。
    .with_notify_policy(policy)
    .with_message_lookup(Arc::new(StorageMessageLookup::new()))
    // 网关失联的交接不带未确认条目，补提示范围向会话服务问。
    .with_recent_conversations(Arc::new(ConversationRecentUnread::new()))
    .with_metrics(metrics)
    .with_trace(push_trace_recorder(&config))
    .with_conversation_ping_coalesce_window(Duration::from_millis(
        config.event_ping_coalesce_window_ms,
    ));
    if let Some((gate, waves)) = &notify {
        route_handler = route_handler
            .with_notify_gate(gate.clone())
            .with_notify_waves(waves.clone());
    }
    let route_handler = Arc::new(route_handler);
    // 到期条目不是消息记录，没有上游上下文可以背书：按条目的租户现签一份本服务的身份。
    let notify_due = notify.map(|(gate, _)| {
        let identity = identity.clone();
        NotifyDueLoop::new(
            route_handler.clone(),
            gate,
            Arc::new(move |tenant_id: &str| identity.attest(tenant_id, "", "")),
            Duration::from_millis(config.notify.due_poll_interval_ms),
            config.notify.due_batch,
        )
    });
    let message_handler = PushMessageHandler::new(route_handler.clone(), publisher.clone());
    let event_handler = PushEventHandler::new(route_handler.clone(), publisher.clone());
    let handoff_handler = PushHandoffHandler::new(route_handler.clone(), publisher.clone());
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
    // 设备交接与其它推送入站 Topic 同在 FLARE_PUSH 流里，同样逐条重新背书。
    register_handler(
        &mut dispatcher,
        TOPIC_PUSH_HANDOFF.to_string(),
        attested(Arc::new(handoff_handler)),
    )?;

    Ok(ApplicationContext {
        config,
        consumer_config: consumer_cfg,
        dispatcher: Arc::new(dispatcher),
        notify_due,
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
            jetstream_stream_specs: flare_server_core::mq::nats::default_stream_specs(),
            ..PushServerConfig::for_tests()
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
