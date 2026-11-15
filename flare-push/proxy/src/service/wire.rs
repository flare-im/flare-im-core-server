//! 依赖装配：配置、JetStream 发布器、gRPC Handler

use std::sync::Arc;

use flare_im_service_kit::metrics::PushProxyMetrics;
use flare_im_service_kit::push_trace::{PushTraceConfig, PushTraceRecorder};
use flare_im_service_kit::redis_connection::{RedisConnectionLimits, SharedRedisConnection};
use flare_server_core::SignedContextVerifier;
use flare_server_core::error::Result;

use crate::application::{PushProxyCommandHandler, PushTaskStatusQuery};
use crate::config::PushProxyConfig;
use crate::infrastructure::{
    PushProxyMqPublisher, PushTraceStore, RedisDeviceTokenRegistry, RedisStateStore,
};
use crate::interface::grpc::{PushAdminHandler, PushServiceHandler};

pub struct ApplicationContext {
    pub handler: PushServiceHandler,
    /// 管理员查推送决策链（`PushAdminService`）。
    pub admin_handler: PushAdminHandler,
}

/// 从应用配置构建应用上下文
pub async fn initialize(
    app_config: &flare_im_service_kit::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    let config = Arc::new(PushProxyConfig::from_app_config(app_config));
    let delivery_refusal = delivery_refusal(&config);
    if let Some(reason) = &delivery_refusal {
        tracing::warn!(%reason, "Push enqueue refused until broker provenance is enforced");
    }
    let publisher = Arc::new(PushProxyMqPublisher::new(config.clone()).await?);
    let store = Arc::new(RedisStateStore::new(config.clone())?);
    let device_tokens = Arc::new(RedisDeviceTokenRegistry::new(config.clone())?);
    let command_handler = Arc::new(PushProxyCommandHandler::new(publisher));
    let status_query = Arc::new(PushTaskStatusQuery::new(store.clone()));

    // 推送决策链与设备绑定在同一个推送 Redis：到达回执写进去，管理员从这里读。
    let push_redis = Arc::new(SharedRedisConnection::new(
        redis::Client::open(config.device_token_redis_url.as_str())?,
        RedisConnectionLimits::default(),
    ));
    let trace = PushTraceRecorder::spawn(
        push_redis.clone(),
        PushTraceConfig::for_service("push_proxy"),
    );
    let handler = PushServiceHandler::new(
        command_handler,
        status_query,
        store,
        device_tokens,
        delivery_refusal,
    )
    .with_receipts(trace, Arc::new(PushProxyMetrics::new()));
    let admin_handler = PushAdminHandler::new(
        Some(Arc::new(PushTraceStore::new(push_redis))),
        staff_verifier(),
    );
    Ok(ApplicationContext {
        handler,
        admin_handler,
    })
}

/// 管理员入口的验签器：Cell 公钥集，Staff 钉死 `kid=gateway`（与 Online 相同）。配不出来时只关掉
/// 管理员查询（一律回未认证），推送入口照常。
fn staff_verifier() -> Option<SignedContextVerifier> {
    match flare_im_service_kit::staff_context::admin_staff_verifier_from_env("push-proxy") {
        Ok(verifier) => Some(verifier),
        Err(error) => {
            tracing::error!(error = %error, "push trace admin queries disabled: staff context verifier unavailable");
            None
        }
    }
}

const PROVENANCE_ENV: &str = "PUSH_PROXY_EVENT_PROVENANCE_MODE";

/// `None` when the configured broker satisfies the Push provenance mode.
fn delivery_refusal(config: &PushProxyConfig) -> Option<String> {
    flare_im_service_kit::push_delivery::require_push_delivery(
        "Push Proxy",
        PROVENANCE_ENV,
        std::env::var(PROVENANCE_ENV).ok().as_deref(),
        flare_im_service_kit::push_delivery::BrokerEndpoint {
            backend: &config.mq_backend,
            nats_url: &config.jetstream_url,
        },
    )
    .err()
    .map(|error| error.to_string())
}
