//! Wire 风格依赖注入：组装 Push Worker 相关组件

use std::sync::Arc;

use flare_server_core::error::Result;

use flare_im_core::discovery::{
    build_gateway_router_from_app_config, connect_grpc_channel_lazy_from_app_config,
};
use flare_im_core::service_names::{ACCESS_GATEWAY, SIGNALING_ONLINE, get_service_name};
use flare_server_core::mq::consumer::ConsumerConfig;
use flare_server_core::mq::consumer::TopicDispatcher;
use flare_server_core::mq::consumer::dispatcher::Dispatcher;

use crate::application::GatewayPushExecutor;
use crate::config::PushWorkerConfig;
use crate::infrastructure::mq::dlq_publisher::DlqPublisher;
use crate::infrastructure::rpc::OnlineServiceClient;
use crate::interface::messaging::offline_consumer::OfflinePushConsumerFactory;
use crate::interface::messaging::online_consumer::OnlinePushConsumerFactory;

pub struct ApplicationContext {
    pub config: Arc<PushWorkerConfig>,
    pub consumer_config: ConsumerConfig,
    pub dispatcher: Arc<dyn Dispatcher>,
}

pub async fn initialize(
    app_config: &flare_im_core::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    let config = Arc::new(PushWorkerConfig::from_app_config(app_config));

    // 1. 创建 DLQ 发布器
    let dlq = Arc::new(DlqPublisher::new(config.clone()).await?);

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
    let offline_handler = OfflinePushConsumerFactory::create_handler(dlq);

    // 6. 配置 ConsumerConfig
    let consumer_cfg = ConsumerConfig::default()
        .with_concurrency(128)
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
