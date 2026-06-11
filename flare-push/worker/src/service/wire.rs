//! Wire 风格依赖注入：组装 Push Worker 相关组件

use std::sync::Arc;

use flare_server_core::error::Result;
use flare_server_core::mq::NatsConsumerConfig;

use flare_im_contracts::service_names::{ACCESS_GATEWAY, SIGNALING_ONLINE, get_service_name};
use flare_im_service_kit::discovery::{
    build_gateway_router_from_app_config, connect_grpc_channel_lazy_from_app_config,
};
use flare_im_service_kit::metrics::PushWorkerMetrics;
use flare_server_core::mq::consumer::ConsumerConfig;
use flare_server_core::mq::consumer::TopicDispatcher;
use flare_server_core::mq::consumer::dispatcher::Dispatcher;

use crate::application::GatewayPushExecutor;
use crate::config::PushWorkerConfig;
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
) -> Result<ApplicationContext> {
    let config = Arc::new(PushWorkerConfig::from_app_config(app_config));

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

    // 4. 创建 GatewayPushExecutor
    let gateway_push = Arc::new(GatewayPushExecutor::new(online_client, gateway_router));

    // 5. 创建 MessageHandler（直接实现，无适配器）
    let online_handler = OnlinePushConsumerFactory::create_handler(gateway_push, dlq.clone());

    // 5.1 离线推送 outbox（厂商通道接入前的持久化暂存）；
    //     连接失败/显式禁用时回退 DLQ/parking 路径并告警。
    let offline_handler = match config.offline_outbox_redis_url.as_deref() {
        Some(redis_url) => {
            match RedisOfflineOutbox::connect(
                redis_url,
                config.offline_outbox_stream.clone(),
                config.offline_outbox_maxlen,
            )
            .await
            {
                Ok(outbox) => {
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
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        redis = %redis_url,
                        "offline push outbox unavailable; falling back to DLQ/parking path"
                    );
                    OfflinePushConsumerFactory::create_handler(
                        dlq,
                        config.offline_parking_capacity,
                        metrics,
                    )
                }
            }
        }
        None => {
            tracing::warn!("offline push outbox disabled by config; tasks will go to DLQ/parking");
            OfflinePushConsumerFactory::create_handler(
                dlq,
                config.offline_parking_capacity,
                metrics,
            )
        }
    };

    // 6. 配置 ConsumerConfig
    let consumer_cfg = ConsumerConfig::default()
        .with_concurrency(128)
        .with_batch_size(config.batch_size())
        .with_batch_timeout_ms(config.batch_timeout_ms())
        .with_ordered(true);

    // 7. 注册到 Dispatcher
    let mut dispatcher = TopicDispatcher::new();

    Dispatcher::register(
        &mut dispatcher,
        config.push_online_topic.clone(),
        online_handler,
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
        offline_handler,
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
