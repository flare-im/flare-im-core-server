//! Wire 风格的依赖注入模块
//!
//! 类似 Go 的 Wire 框架，提供简单的依赖构建方法

use std::sync::Arc;

use redis::Client;

use crate::application::handlers::{
    OnlineCommandHandler, OnlinePresenceWatcherHandler, OnlineQueryHandler, OnlineUserHandler,
};
use crate::config::OnlineConfig;
use crate::domain::connection_event_publisher::NoopConnectionEventPublisher;
use crate::domain::service::{OnlineStatusService, SubscriptionService, UserService};
use crate::infrastructure::persistence::redis::{
    RedisConversationRepository, RedisPresencePublisher, RedisPresenceWatcher,
    RedisSubscriptionRepository,
};
use crate::interface::grpc::OnlineHandler;
use flare_im_service_kit::tenant_runtime::TenantRuntimeCache;
use flare_server_core::SignedContextVerifier;
use flare_server_core::error::{ErrorCode, Result, map_infra_error};

/// 单态化后的 gRPC Handler（Redis 实现 + 无事件发布）
pub type WiredOnlineHandler = OnlineHandler<
    RedisConversationRepository,
    RedisSubscriptionRepository,
    RedisPresencePublisher,
    RedisPresenceWatcher,
>;

/// 应用上下文 - 包含所有已初始化的服务
pub struct ApplicationContext {
    pub online_handler: WiredOnlineHandler,
}

/// 构建应用上下文
///
/// 类似 Go Wire 的 Initialize 函数，按照依赖顺序构建所有组件
///
/// # 参数
/// * `app_config` - 应用配置
///
/// # 返回
/// * `ApplicationContext` - 构建好的应用上下文
pub async fn initialize(
    app_config: &flare_im_service_kit::config::FlareAppConfig,
) -> Result<ApplicationContext> {
    // 1. 加载在线服务配置
    let online_config = Arc::new(OnlineConfig::from_app_config(app_config).map_err(|e| {
        map_infra_error(
            e,
            ErrorCode::InternalError,
            "Failed to load online service configuration",
        )
    })?);

    // 2. 创建 Redis 客户端
    let redis_client = Arc::new(Client::open(online_config.redis_url.as_str()).map_err(|e| {
        map_infra_error(e, ErrorCode::NetworkError, "Failed to create Redis client")
    })?);

    // 3. 构建仓储（具体类型，禁止 `Arc<dyn>` + 异步 trait）
    let conversation_repository: Arc<RedisConversationRepository> = Arc::new(
        RedisConversationRepository::new(redis_client.clone(), online_config.clone()),
    );

    let subscription_repository: Arc<RedisSubscriptionRepository> = Arc::new(
        RedisSubscriptionRepository::new(redis_client.clone(), online_config.clone()),
    );

    let presence_publisher: Arc<RedisPresencePublisher> =
        Arc::new(RedisPresencePublisher::new(redis_client.clone()));

    let presence_watcher: Arc<RedisPresenceWatcher> = Arc::new(RedisPresenceWatcher::new(
        redis_client.clone(),
        online_config.clone(),
    ));

    // 4. 构建领域服务
    let gateway_id = format!("gateway-{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let tenant_runtime = match online_config.tenant_runtime_postgres_url.as_deref() {
        Some(url) => TenantRuntimeCache::connect(url, 2).await.map_err(|error| {
            flare_server_core::error::FlareError::system(format!(
                "Online tenant runtime connection failed: {error}"
            ))
        })?,
        None => TenantRuntimeCache::disabled(),
    };
    let online_domain_service = Arc::new(
        OnlineStatusService::<RedisConversationRepository, NoopConnectionEventPublisher>::new(
            conversation_repository.clone(),
            gateway_id,
        )
        .with_tenant_runtime(
            tenant_runtime,
            online_config.tenant_policy,
            online_config.max_online_devices_per_user,
        ),
    );

    let subscription_domain_service = Arc::new(SubscriptionService::new(
        subscription_repository,
        presence_publisher,
    ));

    let user_domain_service = Arc::new(UserService::new(conversation_repository.clone()));

    let user_handler = Arc::new(OnlineUserHandler::new(user_domain_service.clone()));
    let presence_watcher_handler =
        Arc::new(OnlinePresenceWatcherHandler::new(presence_watcher.clone()));

    // 5. 构建应用层 handlers
    let command_handler = Arc::new(OnlineCommandHandler::new(
        online_domain_service.clone(),
        subscription_domain_service.clone(),
    ));

    // Query handler: 直接使用基础设施层（查询侧不经过领域层）
    let query_handler = Arc::new(OnlineQueryHandler::new(conversation_repository.clone()));

    // 6. 构建 OnlineService Handler（interface::grpc，仅编排 application handlers）
    let control_verifier = flare_im_service_kit::control_auth::ConsoleControlVerifier::from_env()
        .map_err(|_| {
        flare_server_core::error::FlareError::system("invalid FLARE_CONSOLE_PUBLIC_KEY")
    })?;
    if control_verifier.is_none() {
        tracing::warn!("Console control public key missing; Online kick RPCs fail closed");
    }
    let identity_verifier = match std::env::var("FLARE_INTERNAL_CTX_PUBLIC_KEY") {
        Ok(key) => Some(SignedContextVerifier::from_public_key(&key).map_err(|_| {
            flare_server_core::error::FlareError::system(
                "invalid FLARE_INTERNAL_CTX_PUBLIC_KEY for Online",
            )
        })?),
        Err(std::env::VarError::NotPresent) => {
            tracing::warn!(
                "Online session and presence RPCs disabled: internal context verifier missing"
            );
            None
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(flare_server_core::error::FlareError::system(
                "invalid FLARE_INTERNAL_CTX_PUBLIC_KEY encoding",
            ));
        }
    };
    let online_handler = OnlineHandler::new(
        command_handler,
        query_handler,
        user_handler,
        presence_watcher_handler,
    )
    .with_control_verifier(control_verifier)
    .with_identity_verifier(identity_verifier);

    Ok(ApplicationContext { online_handler })
}

use flare_grpc_proto::signaling::online::online_service_server::OnlineService;
use flare_grpc_proto::signaling::online::*;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl OnlineService for WiredOnlineHandler {
    async fn login(
        &self,
        request: Request<LoginRequest>,
    ) -> std::result::Result<Response<LoginResponse>, Status> {
        self.handle_login(request).await
    }

    async fn logout(
        &self,
        request: Request<LogoutRequest>,
    ) -> std::result::Result<Response<LogoutResponse>, Status> {
        self.handle_logout(request).await
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> std::result::Result<Response<HeartbeatResponse>, Status> {
        self.handle_heartbeat(request).await
    }

    async fn get_online_status(
        &self,
        request: Request<GetOnlineStatusRequest>,
    ) -> std::result::Result<Response<GetOnlineStatusResponse>, Status> {
        self.handle_get_online_status(request).await
    }

    type WatchPresenceStream = ReceiverStream<std::result::Result<PresenceEvent, Status>>;

    async fn watch_presence(
        &self,
        request: Request<WatchPresenceRequest>,
    ) -> std::result::Result<Response<Self::WatchPresenceStream>, Status> {
        self.handle_watch_presence(request).await
    }

    async fn get_user_presence(
        &self,
        request: Request<GetUserPresenceRequest>,
    ) -> std::result::Result<Response<GetUserPresenceResponse>, Status> {
        self.handle_get_user_presence(request).await
    }

    async fn batch_get_user_presence(
        &self,
        request: Request<BatchGetUserPresenceRequest>,
    ) -> std::result::Result<Response<BatchGetUserPresenceResponse>, Status> {
        self.handle_batch_get_user_presence(request).await
    }

    type SubscribeUserPresenceStream =
        ReceiverStream<std::result::Result<UserPresenceEvent, Status>>;

    async fn subscribe_user_presence(
        &self,
        request: Request<SubscribeUserPresenceRequest>,
    ) -> std::result::Result<Response<Self::SubscribeUserPresenceStream>, Status> {
        self.handle_subscribe_user_presence(request).await
    }

    async fn list_user_devices(
        &self,
        request: Request<ListUserDevicesRequest>,
    ) -> std::result::Result<Response<ListUserDevicesResponse>, Status> {
        self.handle_list_user_devices(request).await
    }

    async fn kick_device(
        &self,
        request: Request<KickDeviceRequest>,
    ) -> std::result::Result<Response<KickDeviceResponse>, Status> {
        self.handle_kick_device(request).await
    }

    async fn self_kick_device(
        &self,
        request: Request<KickDeviceRequest>,
    ) -> std::result::Result<Response<KickDeviceResponse>, Status> {
        self.handle_self_kick_device(request).await
    }

    async fn kick_tenant(
        &self,
        request: Request<KickTenantRequest>,
    ) -> std::result::Result<Response<KickTenantResponse>, Status> {
        self.handle_kick_tenant(request).await
    }

    async fn get_device(
        &self,
        request: Request<GetDeviceRequest>,
    ) -> std::result::Result<Response<GetDeviceResponse>, Status> {
        self.handle_get_device(request).await
    }
}
