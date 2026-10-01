//! Wire 风格依赖注入：组装 Push Worker 相关组件

use std::sync::Arc;
use std::time::Duration;

use flare_server_core::error::Result;
use flare_server_core::mq::NatsConsumerConfig;
use flare_server_core::{ErrorCode, FlareError};

use flare_im_contracts::service_names::{ACCESS_GATEWAY, SIGNALING_ONLINE, get_service_name};
use flare_im_service_kit::discovery::{
    build_gateway_router_from_app_config, connect_grpc_channel_lazy_from_app_config,
};
use flare_im_service_kit::metrics::PushWorkerMetrics;
use flare_im_service_kit::push_delivery::{
    AttestedRecordHandler, BrokerRecordIdentity, PushDeliveryMode,
};
use flare_server_core::mq::consumer::ConsumerConfig;
use flare_server_core::mq::consumer::RetryBackoff;
use flare_server_core::mq::consumer::TopicDispatcher;
use flare_server_core::mq::consumer::dispatcher::Dispatcher;

use crate::application::GatewayPushExecutor;
use crate::config::{OfflineDeliveryBackend, PushWorkerConfig};
use crate::infrastructure::apns_push::{ApnsCredentials, ApnsOfflinePushExecutor};
use crate::infrastructure::device_tokens::RedisDeviceTokenRepository;
use crate::infrastructure::fcm_push::{FcmOfflinePushExecutor, FcmServiceAccount};
use crate::infrastructure::getui_push::{GetuiClient, GetuiConfig, GetuiOfflinePushExecutor};
use crate::infrastructure::mq::dlq_publisher::DlqPublisher;
use crate::infrastructure::offline_outbox::RedisOfflineOutbox;
use crate::infrastructure::rpc::OnlineServiceClient;
use crate::interface::messaging::offline_consumer::OfflinePushConsumerFactory;
use crate::interface::messaging::online_consumer::OnlinePushConsumerFactory;

pub struct ApplicationContext {
    pub config: Arc<PushWorkerConfig>,
    pub consumer_config: ConsumerConfig,
    pub dispatcher: Arc<dyn Dispatcher>,
}

pub async fn initialize(
    app_config: &flare_im_service_kit::config::FlareAppConfig,
    mode: PushDeliveryMode,
) -> Result<ApplicationContext> {
    let config = Arc::new(PushWorkerConfig::from_app_config(app_config)?);
    // Every consumed record is re-attested as this service for its tenant.
    let identity = BrokerRecordIdentity::from_env(mode, "flare-push-worker")?;

    // 1. 创建 DLQ 发布器
    let dlq = Arc::new(DlqPublisher::new(config.clone()).await?);
    let metrics = Arc::new(PushWorkerMetrics::new());

    // 2. 创建 Online 服务客户端
    let online_service = get_service_name(SIGNALING_ONLINE);
    tracing::info!(
        service = %online_service,
        static_fallback = %config.online_service_endpoint,
        "Resolving Online gRPC (registry or static fallback)"
    );
    let online_fallback = config.online_service_endpoint.as_str();
    let online_channel =
        connect_grpc_channel_lazy_from_app_config(app_config, &online_service, online_fallback)
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "Online gRPC lazy channel: {}",
                    e
                ))
            })?;
    let online_client = Arc::new(OnlineServiceClient::from_channel(online_channel));

    // 3. 创建 Gateway Router
    let access_gateway_service = get_service_name(ACCESS_GATEWAY);
    let gateway_router = build_gateway_router_from_app_config(
        app_config,
        &access_gateway_service,
        config.access_gateway_static_endpoint.clone(),
    )
    .await
    .map_err(|e| flare_server_core::error::FlareError::system(format!("GatewayRouter: {}", e)))?;

    // 4. 创建 GatewayPushExecutor（统一读扩散：事件按会话广播，无 per-user ping 去抖）
    let gateway_push = Arc::new(GatewayPushExecutor::new(online_client, gateway_router));

    // 5. 创建 MessageHandler（直接实现，无适配器）
    let online_handler = OnlinePushConsumerFactory::create_handler(gateway_push, dlq.clone());

    // 5.1 离线推送后端：开发期按生产目标显式选择，不做隐式降级。
    let offline_handler = match config.offline_delivery_backend {
        OfflineDeliveryBackend::Outbox => {
            let redis_url = config.offline_outbox_redis_url.as_deref().ok_or_else(|| {
                FlareError::localized(
                    ErrorCode::InvalidParameter,
                    "offline outbox backend requires PUSH_WORKER_OFFLINE_REDIS_URL",
                )
            })?;
            let outbox = RedisOfflineOutbox::connect(
                redis_url,
                config.offline_outbox_stream.clone(),
                config.offline_outbox_maxlen,
            )
            .await?;
            tracing::info!(
                stream = %config.offline_outbox_stream,
                maxlen = config.offline_outbox_maxlen,
                "offline push outbox enabled (Redis Stream)"
            );
            OfflinePushConsumerFactory::create_handler_with_delivery(
                dlq,
                Arc::new(outbox),
                config.offline_parking_capacity,
                metrics,
            )
        }
        OfflineDeliveryBackend::Apns => {
            let token_repo = RedisDeviceTokenRepository::connect(
                &config.device_token_redis_url,
                config.device_token_key_prefix.clone(),
            )
            .await?;
            let creds = ApnsCredentials {
                team_id: required_config(config.apns_team_id.clone(), "PUSH_WORKER_APNS_TEAM_ID")?,
                key_id: required_config(config.apns_key_id.clone(), "PUSH_WORKER_APNS_KEY_ID")?,
                private_key_pem: required_config(
                    config.apns_private_key.clone(),
                    "PUSH_WORKER_APNS_PRIVATE_KEY",
                )?,
                topic: required_config(config.apns_topic.clone(), "PUSH_WORKER_APNS_TOPIC")?,
                sandbox: config.apns_sandbox,
            };
            tracing::info!(
                topic = %creds.topic,
                sandbox = creds.sandbox,
                token_key_prefix = %config.device_token_key_prefix,
                "offline push apns backend enabled"
            );
            OfflinePushConsumerFactory::create_handler_with_delivery(
                dlq,
                Arc::new(ApnsOfflinePushExecutor::new(
                    creds,
                    reqwest::Client::new(),
                    Arc::new(token_repo),
                )),
                config.offline_parking_capacity,
                metrics,
            )
        }
        OfflineDeliveryBackend::Fcm => {
            let token_repo = RedisDeviceTokenRepository::connect(
                &config.device_token_redis_url,
                config.device_token_key_prefix.clone(),
            )
            .await?;
            let raw = required_config(
                config.fcm_service_account_json.clone(),
                "PUSH_WORKER_FCM_SERVICE_ACCOUNT_JSON",
            )?;
            let account: FcmServiceAccount = serde_json::from_str(&raw).map_err(|e| {
                flare_server_core::error::FlareError::localized(
                    flare_server_core::error::ErrorCode::InvalidParameter,
                    format!("PUSH_WORKER_FCM_SERVICE_ACCOUNT_JSON 不是合法的服务账号 JSON: {e}"),
                )
            })?;
            tracing::info!(
                project_id = %account.project_id,
                token_key_prefix = %config.device_token_key_prefix,
                "offline push fcm backend enabled"
            );
            OfflinePushConsumerFactory::create_handler_with_delivery(
                dlq,
                Arc::new(FcmOfflinePushExecutor::new(
                    account,
                    reqwest::Client::new(),
                    Arc::new(token_repo),
                )),
                config.offline_parking_capacity,
                metrics,
            )
        }
        OfflineDeliveryBackend::Getui => {
            let token_repo = RedisDeviceTokenRepository::connect(
                &config.device_token_redis_url,
                config.device_token_key_prefix.clone(),
            )
            .await?;
            let getui_config = GetuiConfig::new(
                required_config(config.getui_app_id.clone(), "PUSH_WORKER_GETUI_APP_ID")?,
                required_config(config.getui_app_key.clone(), "PUSH_WORKER_GETUI_APP_KEY")?,
                required_config(
                    config.getui_master_secret.clone(),
                    "PUSH_WORKER_GETUI_MASTER_SECRET",
                )?,
                config.getui_base_url.clone(),
                config.getui_default_ttl_ms,
                config.getui_request_timeout_ms,
            )?;
            let getui_client = Arc::new(GetuiClient::new(getui_config)?);
            tracing::info!(
                token_key_prefix = %config.device_token_key_prefix,
                ttl_ms = config.getui_default_ttl_ms,
                "offline push getui backend enabled"
            );
            OfflinePushConsumerFactory::create_handler_with_delivery(
                dlq,
                Arc::new(GetuiOfflinePushExecutor::new(
                    Arc::new(token_repo),
                    getui_client,
                    config.getui_default_ttl_ms,
                )),
                config.offline_parking_capacity,
                metrics,
            )
        }
        OfflineDeliveryBackend::Disabled => {
            tracing::warn!("offline push delivery disabled; tasks will go to DLQ/parking");
            OfflinePushConsumerFactory::create_handler(
                dlq,
                config.offline_parking_capacity,
                metrics,
            )
        }
    };

    // 6. 配置 ConsumerConfig
    let consumer_cfg = push_consumer_config(config.batch_size(), config.batch_timeout_ms());

    // 7. 注册到 Dispatcher
    let mut dispatcher = TopicDispatcher::new();

    Dispatcher::register(
        &mut dispatcher,
        config.push_online_topic.clone(),
        Arc::new(AttestedRecordHandler::new(identity.clone(), online_handler)),
    )
    .map_err(|err| {
        flare_server_core::error::FlareError::system(format!(
            "register push worker consumer {}: {err}",
            config.push_online_topic
        ))
    })?;

    Dispatcher::register(
        &mut dispatcher,
        config.push_offline_topic.clone(),
        Arc::new(AttestedRecordHandler::new(identity, offline_handler)),
    )
    .map_err(|err| {
        flare_server_core::error::FlareError::system(format!(
            "register push worker consumer {}: {err}",
            config.push_offline_topic
        ))
    })?;

    Ok(ApplicationContext {
        config,
        consumer_config: consumer_cfg,
        dispatcher: Arc::new(dispatcher),
    })
}

/// 推送消费者的重试次数上限（online 与 offline 两个 handler 共用这一个消费者配置）。
///
/// handler 遇到暂时性失败（Access Gateway 停机或重启、网关过载、离线推送后端暂时不可用）
/// 返回 Nack，交给 JetStream 按 [`push_delivery_retry_backoff`] 延迟重投：12 次重试的等待合计
/// 135 s，之后仍失败才进 `flare.im.dlq.push-delivery`。不设退避时 NAK 立即重投，连接被拒在
/// 毫秒内返回，默认的 3 次重试会在同一瞬间用完，停机或重启窗口里的推送照样进 DLQ（W6-S2 F4）。
///
/// 首投加全部重试不得超过 JetStream 的最大投递次数，否则 broker 先停止投递，消息既不送达
/// 也不进 DLQ；见 `wire_tests` 的门禁。
pub(crate) const PUSH_DELIVERY_MAX_RETRIES: u32 = 12;

/// 推送重投的退避：1 s 起翻倍，封顶 15 s。网关恢复后，处于退避中的推送最迟 15 s 内重投。
pub(crate) fn push_delivery_retry_backoff() -> RetryBackoff {
    RetryBackoff::new(Duration::from_secs(1), Duration::from_secs(15))
}

pub(crate) fn push_consumer_config(batch_size: usize, batch_timeout_ms: u64) -> ConsumerConfig {
    ConsumerConfig::default()
        .with_concurrency(128)
        .with_batch_size(batch_size)
        .with_batch_timeout_ms(batch_timeout_ms)
        .with_ordered(true)
        .with_max_retries(PUSH_DELIVERY_MAX_RETRIES)
        .with_retry_backoff(push_delivery_retry_backoff())
}

fn required_config(value: Option<String>, name: &'static str) -> Result<String> {
    value.ok_or_else(|| {
        FlareError::localized(
            ErrorCode::InvalidParameter,
            format!("missing required config {name}"),
        )
    })
}

#[cfg(test)]
mod wire_tests {
    use super::*;

    /// 只实现必需项的消费者配置：`build_nats_consumer_tasks_*` 按流解析时只保留这几项，
    /// `max_deliver` 等一律取 trait 默认值，所以这里读到的就是 JetStream 消费者实际的
    /// 最大投递次数。
    struct EffectiveNatsConsumer;

    impl NatsConsumerConfig for EffectiveNatsConsumer {
        fn nats_url(&self) -> &str {
            "nats://127.0.0.1:1"
        }
        fn consumer_group(&self) -> &str {
            "push-worker"
        }
        fn enable_manual_ack(&self) -> bool {
            true
        }
        fn batch_size(&self) -> usize {
            64
        }
        fn batch_timeout_ms(&self) -> u64 {
            50
        }
        fn enable_durable(&self) -> bool {
            true
        }
    }

    /// 门禁：推送的重试预算覆盖停机与重启窗口，且首投加全部重试落在 JetStream 的
    /// `max_deliver` 之内（否则 broker 先停投，消息既不送达也不进 DLQ）。
    #[test]
    fn push_retry_budget_spans_a_restart_window_inside_max_deliver() {
        let consumer = push_consumer_config(64, 50);
        let backoff = consumer
            .retry_backoff
            .expect("push consumer retries with backoff");
        let max_deliver = EffectiveNatsConsumer.max_deliver();
        println!(
            "push retries {} over {:?}, max_deliver {max_deliver}",
            consumer.max_retries,
            backoff.total_for(consumer.max_retries)
        );
        assert_eq!(consumer.max_retries, PUSH_DELIVERY_MAX_RETRIES);
        // 首投 + max_retries 次重投 ≤ max_deliver。
        assert!(i64::from(consumer.max_retries) < max_deliver);
        // W6-S2：core 从 stop 到重新 healthy 约 16 s（停机 5.3 s、间隔 5 s、启动 11 s）；
        // 重试窗口至少留出它的数倍。
        assert!(backoff.total_for(consumer.max_retries) >= Duration::from_secs(120));
        assert!(backoff.delay_for(0) > Duration::ZERO);
        assert!(backoff.delay_for(u32::MAX) <= Duration::from_secs(15));
        assert!(consumer.ordered);
        assert!(consumer.enable_dlq);
    }
}
