//! Wire 风格依赖注入：组装 Push Worker 相关组件

use std::sync::Arc;

use flare_server_core::error::Result;
use flare_server_core::mq::NatsConsumerConfig;
use flare_server_core::{ErrorCode, FlareError};

use flare_im_contracts::service_names::{ACCESS_GATEWAY, SIGNALING_ONLINE, get_service_name};
use flare_im_service_kit::discovery::{
    build_gateway_router_from_app_config, connect_grpc_channel_lazy_from_app_config,
};
use flare_im_service_kit::metrics::PushWorkerMetrics;
use flare_server_core::mq::consumer::ConsumerConfig;
use flare_server_core::mq::consumer::TopicDispatcher;
use flare_server_core::mq::consumer::dispatcher::Dispatcher;

use crate::application::GatewayPushExecutor;
use crate::config::{OfflineDeliveryBackend, PushWorkerConfig};
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

fn required_config(value: Option<String>, name: &'static str) -> Result<String> {
    value.ok_or_else(|| {
        FlareError::localized(
            ErrorCode::InvalidParameter,
            format!("missing required config {name}"),
        )
    })
}
