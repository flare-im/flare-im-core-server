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
use flare_server_core::mq::consumer::MessageHandler;
use flare_server_core::mq::consumer::RetryBackoff;
use flare_server_core::mq::consumer::TopicDispatcher;
use flare_server_core::mq::consumer::dispatcher::Dispatcher;

use super::consumer_lanes::{LaneKind, LaneSpec, lane_specs};
use crate::application::GatewayPushExecutor;
use crate::application::binding_delivery::DeliveryRuntime;
use crate::application::digest_coalescer::{CoalescerLimits, DigestCoalescer};
use crate::application::vendor_governance::VendorGovernance;
use crate::config::{OfflineDeliveryBackend, PushWorkerConfig};
use crate::infrastructure::device_tokens::RedisDeviceTokenRepository;
use crate::infrastructure::mq::dlq_publisher::DlqPublisher;
use crate::infrastructure::offline_outbox::RedisOfflineOutbox;
use crate::infrastructure::rpc::OnlineServiceClient;
use crate::infrastructure::vendor_trace::VendorTrace;
use crate::interface::messaging::offline_consumer::{
    OfflinePushConsumerFactory, UsageCountedExecutor,
};
use crate::interface::messaging::online_consumer::OnlinePushConsumerFactory;
use flare_im_service_kit::push_trace::{PushTraceConfig, PushTraceRecorder};

/// 一个消费者：它的形状、消费配置与只登记了它那个主题的分发器。
pub struct ConsumerLane {
    pub spec: LaneSpec,
    pub consumer_config: ConsumerConfig,
    pub dispatcher: Arc<dyn Dispatcher>,
}

pub struct ApplicationContext {
    pub config: Arc<PushWorkerConfig>,
    /// 在线投递与离线任务三条优先级通道，各一个消费者。
    pub consumers: Vec<ConsumerLane>,
    /// 用量预聚合（`push.attempts`）；启动层把它的 flush 循环挂进运行时。
    pub usage: flare_im_usage::UsageReporter,
    /// 厂商限流时攒下的提示（只在厂商后端时有）；启动层把它的交还循环挂进运行时。
    pub digest_coalescer: Option<Arc<DigestCoalescer>>,
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
    let online_channel = connect_grpc_channel_lazy_from_app_config(
        app_config,
        SIGNALING_ONLINE,
        &online_service,
        online_fallback,
    )
    .map_err(|e| {
        flare_server_core::error::FlareError::system(format!("Online gRPC lazy channel: {}", e))
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

    // 5.1 离线推送后端：开发期按生产目标显式选择，不做隐式降级。每个后端经用量计数包一层。
    let usage = flare_im_usage::UsageReporter::from_env("flare-push-worker").await?;
    let mut digest_coalescer = None;
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
                "{} (Redis Stream)",
                OfflineDeliveryBackend::Outbox.enabled_line()
            );
            OfflinePushConsumerFactory::create_handler_with_delivery(
                dlq,
                UsageCountedExecutor::wrap(Arc::new(outbox), "outbox", usage.recorder()),
                config.offline_parking_capacity,
                metrics,
                VendorTrace::disabled(),
            )
        }
        backend @ (OfflineDeliveryBackend::Apns
        | OfflineDeliveryBackend::Fcm
        | OfflineDeliveryBackend::Getui
        | OfflineDeliveryBackend::Mobile) => {
            let providers = if backend == OfflineDeliveryBackend::Mobile {
                config.offline_mobile_providers.clone()
            } else {
                vec![backend]
            };
            let provider_names = providers
                .iter()
                .map(|provider| provider.name())
                .collect::<Vec<_>>()
                .join(",");
            let tokens = Arc::new(
                RedisDeviceTokenRepository::connect(
                    &config.device_token_redis_url,
                    config.device_token_key_prefix.clone(),
                    config.binding_inactive_days,
                )
                .await?,
            );
            // 厂商治理、绑定选择与限流降级摘要活得和进程一样长：凭据租约轮换只重建厂商适配器。
            // 决策链写进推送 Redis（与设备绑定同一个实例），读 `[services.push_worker.trace]`。
            let trace = VendorTrace::new(push_trace_recorder(&config.device_token_redis_url));
            let governance = Arc::new(VendorGovernance::new(|vendor| config.vendor_limits(vendor)));
            let coalescer = Arc::new(DigestCoalescer::new(
                CoalescerLimits::default(),
                governance.clone(),
                trace.clone(),
            ));
            let runtime = Arc::new(DeliveryRuntime {
                binding_order: config.binding_order.clone(),
                governance,
                coalescer: coalescer.clone(),
                trace: trace.clone(),
                usage: usage.recorder(),
            });
            let binding_order = config
                .binding_order
                .iter()
                .map(|vendor| vendor.as_str())
                .collect::<Vec<_>>()
                .join(",");
            tracing::info!(binding_order = %binding_order, "offline push binding order");
            let executor: Arc<dyn crate::domain::offline_delivery::OfflinePushExecutor> = match (
                &config.mobile_applications_file,
                &config.mobile_snapshot_file,
            ) {
                (Some(path), None) => Arc::new(super::mobile_credentials::build(
                    std::path::Path::new(path),
                    &providers,
                    tokens,
                    runtime,
                    config.getui_default_ttl_ms,
                    config.getui_request_timeout_ms,
                )?),
                (None, Some(path)) => Arc::new(super::mobile_credentials::LeasedApplications::new(
                    path.into(),
                    providers,
                    tokens,
                    runtime,
                    config.getui_default_ttl_ms,
                    config.getui_request_timeout_ms,
                )),
                _ => {
                    return Err(FlareError::localized(
                        ErrorCode::InvalidParameter,
                        "exactly one private mobile credential source required",
                    ));
                }
            };
            // 攒下的提示交还给同一个投递后端（弱引用：后端本身持有攒提示的地方）。
            coalescer.set_target(Arc::downgrade(&executor));
            digest_coalescer = Some(coalescer);
            // 每次启动一行，说明在用哪个厂商后端（部署冒烟据此核对）。
            tracing::info!(providers = %provider_names, "{}", backend.enabled_line());
            OfflinePushConsumerFactory::create_handler_with_delivery(
                dlq,
                executor,
                config.offline_parking_capacity,
                metrics,
                trace,
            )
        }
        OfflineDeliveryBackend::Disabled => {
            // 启动时只说这一次：离线推送是关着的，不是坏了。生产环境走到这里必然是显式配置的，
            // 其他环境也可能只是没配置。
            tracing::warn!(
                "offline push is off: PUSH_WORKER_OFFLINE_DELIVERY_BACKEND is disabled or unset; nothing reaches a phone and offline tasks go to the DLQ"
            );
            OfflinePushConsumerFactory::create_handler(
                dlq,
                config.offline_parking_capacity,
                metrics,
                VendorTrace::disabled(),
            )
        }
    };

    // 6. 每个主题一个消费者：在线投递，与离线任务的单聊、群、批量三条通道。三条离线通道共用
    //    同一个处理器，只是各自拉取、各自限并发：批量通道积压不影响单聊。
    let online_handler: Arc<dyn MessageHandler> =
        Arc::new(AttestedRecordHandler::new(identity.clone(), online_handler));
    let offline_handler: Arc<dyn MessageHandler> =
        Arc::new(AttestedRecordHandler::new(identity, offline_handler));
    let mut consumers = Vec::new();
    for spec in lane_specs(&config) {
        let handler = match spec.kind {
            LaneKind::Online => online_handler.clone(),
            LaneKind::Offline => offline_handler.clone(),
        };
        let mut dispatcher = TopicDispatcher::new();
        Dispatcher::register(&mut dispatcher, spec.topic.clone(), handler).map_err(|err| {
            flare_server_core::error::FlareError::system(format!(
                "register push worker consumer {}: {err}",
                spec.topic
            ))
        })?;
        tracing::info!(
            consumer = spec.name,
            topic = %spec.topic,
            group = %spec.group,
            concurrency = spec.concurrency,
            "push worker consumer registered"
        );
        consumers.push(ConsumerLane {
            consumer_config: push_consumer_config(config.batch_size(), config.batch_timeout_ms())
                .with_concurrency(spec.concurrency),
            dispatcher: Arc::new(dispatcher),
            spec,
        });
    }

    Ok(ApplicationContext {
        config,
        consumers,
        usage,
        digest_coalescer,
    })
}

/// 决策链的写入端：推送 Redis 地址读不懂时关掉决策链（不拦启动，也不把地址写进日志）。
fn push_trace_recorder(redis_url: &str) -> PushTraceRecorder {
    let config = PushTraceConfig::for_service("push_worker");
    match redis::Client::open(redis_url) {
        Ok(client) => PushTraceRecorder::spawn(client, config),
        Err(_) => {
            tracing::warn!("push decision trace disabled: the push Redis address is not usable");
            PushTraceRecorder::disabled()
        }
    }
}

/// 推送消费者的重试次数上限（四个消费者共用，各自只改并发）。
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

/// 推送消费者的共同配置；每个消费者再按自己的并发设置覆盖 `concurrency`。
pub(crate) fn push_consumer_config(batch_size: usize, batch_timeout_ms: u64) -> ConsumerConfig {
    ConsumerConfig::default()
        .with_concurrency(128)
        .with_batch_size(batch_size)
        .with_batch_timeout_ms(batch_timeout_ms)
        .with_ordered(true)
        .with_max_retries(PUSH_DELIVERY_MAX_RETRIES)
        .with_retry_backoff(push_delivery_retry_backoff())
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
