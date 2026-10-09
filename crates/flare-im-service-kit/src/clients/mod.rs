mod capability_client;
mod conversation_client;
mod media_client;
mod message_client;
mod online_client;
mod resolver;
mod storage_client;

use flare_server_core::error::Result;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

use crate::config::FlareAppConfig;
use crate::discovery::StaticChannelPolicy;

pub use capability_client::{CapabilityAdminClientWrapper, CapabilityDownstreamConfig};
pub use conversation_client::{
    ConversationManageServiceClientWrapper, ConversationReadServiceClientWrapper,
};
pub use flare_server_core::context::Ctx;
pub use media_client::{MediaCaller, MediaServiceClientWrapper};
pub use message_client::{
    MessageActionServiceClientWrapper, MessageEventServiceClientWrapper,
    MessageSendServiceClientWrapper,
};
pub use online_client::OnlineServiceClientWrapper;
pub use resolver::{DownstreamGrpcResolver, DownstreamKind};
pub use storage_client::StorageReaderServiceClientWrapper;

/// 下游 IM gRPC 客户端路由配置。
///
/// 该类型属于 IM Core：它描述 Gateway 如何访问 IM 核心服务，而不是某个
/// HTTP Gateway 的运行时细节。Gateway Common 只负责从对应环境作用域加载它。
#[derive(Debug, Clone, Deserialize)]
pub struct DownstreamGrpcConfig {
    /// MediaService 路由（`discovery://flare-media` 或静态 `http://` 覆盖）
    pub media_service_url: String,
    /// MessageIngest 路由（send command path）
    pub message_ingest_service_url: String,
    /// MessageOrchestrator 路由（message action / event fanout control path）
    pub message_orchestrator_service_url: String,
    /// ConversationService 路由
    pub conversation_service_url: String,
    /// Signaling Online 路由
    pub online_service_url: String,
    /// StorageReaderService 路由
    pub storage_reader_service_url: String,
    /// 无注册中心或发现失败时的 Media 静态回退 URI（本地开发）
    #[serde(default)]
    pub media_static_fallback: String,
    #[serde(default)]
    pub message_ingest_static_fallback: String,
    #[serde(default)]
    pub message_orchestrator_static_fallback: String,
    #[serde(default)]
    pub conversation_static_fallback: String,
    #[serde(default)]
    pub online_static_fallback: String,
    #[serde(default)]
    pub storage_reader_static_fallback: String,
    /// 静态地址（`http(s)://` 覆盖与静态回退）一次建连的上限（秒），见
    /// [`Self::static_channel_policy`]；`discovery://` 通道的建连由发现客户端决定。
    pub connect_timeout_secs: u64,
    /// 请求超时(秒)
    pub request_timeout_secs: u64,
}

impl DownstreamGrpcConfig {
    /// 静态地址通道的建连策略：建连上限取 [`Self::connect_timeout_secs`]，探活等其余设置同
    /// [`StaticChannelPolicy::DEFAULT`]。
    pub fn static_channel_policy(&self) -> StaticChannelPolicy {
        StaticChannelPolicy {
            connect_timeout: Duration::from_secs(self.connect_timeout_secs),
            ..StaticChannelPolicy::DEFAULT
        }
    }
}

/// gRPC 客户端管理器。
///
/// 网关所有 HTTP handler 通过该聚合对象访问下游服务，避免 handler 自己维护
/// channel、metadata、重连和超时策略。
pub struct GrpcClients {
    pub media: Arc<tokio::sync::Mutex<MediaServiceClientWrapper>>,
    pub message_ingest_send: Arc<tokio::sync::Mutex<MessageSendServiceClientWrapper>>,
    pub message_event: Arc<tokio::sync::Mutex<MessageEventServiceClientWrapper>>,
    pub message_action: Arc<tokio::sync::Mutex<MessageActionServiceClientWrapper>>,
    pub conversation_read: Arc<tokio::sync::Mutex<ConversationReadServiceClientWrapper>>,
    pub conversation_manage: Arc<tokio::sync::Mutex<ConversationManageServiceClientWrapper>>,
    pub online: Arc<tokio::sync::Mutex<OnlineServiceClientWrapper>>,
    pub storage_reader: Arc<tokio::sync::Mutex<StorageReaderServiceClientWrapper>>,
    /// 一次下游调用的请求超时，由 [`Self::bounded`] 施加；`None` 不限时。
    ///
    /// API Gateway 取 `FLARE_API_GATEWAY_GRPC_REQUEST_TIMEOUT_SECS`（见
    /// [`Self::with_request_timeout`]）。Admin Gateway 不用这个聚合：它只连自己用到的下游，见
    /// [`AdminGrpcClients`]。
    pub request_timeout: Option<Duration>,
}

/// 下游调用超过请求超时时回的状态。`DEADLINE_EXCEEDED` 转成 `FlareError` 是
/// `OperationTimeout`，各网关按可重试的 503/504 回给调用方。
pub const DOWNSTREAM_REQUEST_TIMED_OUT: &str = "downstream request timed out";

/// 给一次下游调用加请求超时：`timeout` 为 `None` 时原样等待；超时回 `DEADLINE_EXCEEDED`
/// （[`DOWNSTREAM_REQUEST_TIMED_OUT`]），被丢弃的调用随之取消。
pub async fn bounded_request<T, E, F>(
    timeout: Option<Duration>,
    call: F,
) -> std::result::Result<T, E>
where
    F: std::future::Future<Output = std::result::Result<T, E>>,
    E: From<tonic::Status>,
{
    match timeout {
        None => call.await,
        Some(timeout) => tokio::time::timeout(timeout, call)
            .await
            .unwrap_or_else(|_| {
                Err(E::from(tonic::Status::deadline_exceeded(
                    DOWNSTREAM_REQUEST_TIMED_OUT,
                )))
            }),
    }
}

impl GrpcClients {
    /// 设置每次下游调用的请求超时（见 [`Self::bounded`]）。
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// 按 [`Self::request_timeout`] 限时执行一次下游调用。
    ///
    /// 客户端放在互斥锁后面、调用期间一直持锁，所以把取锁也放进 `call`：限的是从开始等锁到
    /// 拿到应答的整段时间，排在慢调用后面的请求同样按时返回，而不是逐个等满超时。
    pub async fn bounded<T, E, F>(&self, call: F) -> std::result::Result<T, E>
    where
        F: std::future::Future<Output = std::result::Result<T, E>>,
        E: From<tonic::Status>,
    {
        bounded_request(self.request_timeout, call).await
    }

    /// 通过服务发现（或静态回退）初始化所有下游 gRPC 客户端。
    pub async fn new(app_config: Arc<FlareAppConfig>, grpc: &DownstreamGrpcConfig) -> Result<Self> {
        let resolver = DownstreamGrpcResolver::new(Arc::clone(&app_config), grpc.clone());

        let media_channel = resolver.connect(DownstreamKind::Media).await?;
        let message_ingest_channel = resolver.connect(DownstreamKind::MessageIngest).await?;
        let message_orchestrator_channel = resolver
            .connect(DownstreamKind::MessageOrchestrator)
            .await?;
        let conversation_channel = resolver.connect(DownstreamKind::Conversation).await?;
        let online_channel = resolver.connect(DownstreamKind::SignalingOnline).await?;
        let storage_reader_channel = resolver.connect(DownstreamKind::StorageReader).await?;

        let media_fallback = optional_static_fallback(&grpc.media_static_fallback);

        Ok(Self {
            media: Arc::new(tokio::sync::Mutex::new(
                MediaServiceClientWrapper::from_channel(media_channel, media_fallback),
            )),
            message_ingest_send: Arc::new(tokio::sync::Mutex::new(
                MessageSendServiceClientWrapper::from_channel(message_ingest_channel),
            )),
            message_event: Arc::new(tokio::sync::Mutex::new(
                MessageEventServiceClientWrapper::from_channel(
                    message_orchestrator_channel.clone(),
                ),
            )),
            message_action: Arc::new(tokio::sync::Mutex::new(
                MessageActionServiceClientWrapper::from_channel(message_orchestrator_channel),
            )),
            conversation_read: Arc::new(tokio::sync::Mutex::new(
                ConversationReadServiceClientWrapper::from_channel(conversation_channel.clone()),
            )),
            conversation_manage: Arc::new(tokio::sync::Mutex::new(
                ConversationManageServiceClientWrapper::from_channel(conversation_channel),
            )),
            online: Arc::new(tokio::sync::Mutex::new(
                OnlineServiceClientWrapper::from_channel(online_channel),
            )),
            storage_reader: Arc::new(tokio::sync::Mutex::new(
                StorageReaderServiceClientWrapper::from_channel(storage_reader_channel),
            )),
            request_timeout: None,
        })
    }
}

/// Admin Gateway 的下游 gRPC 客户端：只连它用到的四个固定下游（Media、Conversation 读、
/// Online、Storage Reader；可选的 Capability 另见 [`CapabilityAdminClientWrapper`]），不连
/// Message Ingest 与 Orchestrator，也不要求它们的地址可达。
///
/// 管理员调用只带中间件验过的 Staff `x-flare-ctx` 原样转发（见
/// [`crate::staff_context::staff_context_request`]）；Media 另由网关按 `kid=admin-gateway` 重签。
pub struct AdminGrpcClients {
    pub media: Arc<tokio::sync::Mutex<MediaServiceClientWrapper>>,
    pub conversation_read: Arc<tokio::sync::Mutex<ConversationReadServiceClientWrapper>>,
    pub online: Arc<tokio::sync::Mutex<OnlineServiceClientWrapper>>,
    pub storage_reader: Arc<tokio::sync::Mutex<StorageReaderServiceClientWrapper>>,
    /// 一次下游调用的请求超时（`FLARE_ADMIN_GATEWAY_GRPC_REQUEST_TIMEOUT_SECS`，缺省 5 秒），由
    /// [`Self::bounded`] 施加；`None` 不限时（只在测试里）。
    pub request_timeout: Option<Duration>,
}

impl AdminGrpcClients {
    /// 通过服务发现（或静态回退）只连 Admin Gateway 用到的四个下游。
    pub async fn connect(
        app_config: Arc<FlareAppConfig>,
        grpc: &DownstreamGrpcConfig,
        request_timeout: Option<Duration>,
    ) -> Result<Self> {
        let resolver = DownstreamGrpcResolver::new(Arc::clone(&app_config), grpc.clone());
        let media_channel = resolver.connect(DownstreamKind::Media).await?;
        let conversation_channel = resolver.connect(DownstreamKind::Conversation).await?;
        let online_channel = resolver.connect(DownstreamKind::SignalingOnline).await?;
        let storage_reader_channel = resolver.connect(DownstreamKind::StorageReader).await?;
        let media_fallback = optional_static_fallback(&grpc.media_static_fallback);
        Ok(Self {
            media: Arc::new(tokio::sync::Mutex::new(
                MediaServiceClientWrapper::from_channel(media_channel, media_fallback),
            )),
            conversation_read: Arc::new(tokio::sync::Mutex::new(
                ConversationReadServiceClientWrapper::from_channel(conversation_channel),
            )),
            online: Arc::new(tokio::sync::Mutex::new(
                OnlineServiceClientWrapper::from_channel(online_channel),
            )),
            storage_reader: Arc::new(tokio::sync::Mutex::new(
                StorageReaderServiceClientWrapper::from_channel(storage_reader_channel),
            )),
            request_timeout,
        })
    }

    /// 按 [`Self::request_timeout`] 限时执行一次下游调用（含取锁，见 [`GrpcClients::bounded`]）。
    pub async fn bounded<T, E, F>(&self, call: F) -> std::result::Result<T, E>
    where
        F: std::future::Future<Output = std::result::Result<T, E>>,
        E: From<tonic::Status>,
    {
        bounded_request(self.request_timeout, call).await
    }
}

fn optional_static_fallback(uri: &str) -> Option<String> {
    if uri.trim().is_empty() {
        None
    } else {
        Some(uri.to_string())
    }
}

#[cfg(test)]
mod request_timeout_tests {
    use super::{DOWNSTREAM_REQUEST_TIMED_OUT, bounded_request};
    use flare_server_core::error::{ErrorCode, FlareError};
    use std::time::Duration;

    /// 超时回 `DEADLINE_EXCEEDED`；按 `FlareError` 取结果的调用方拿到 `OperationTimeout`
    /// （网关据此回可重试的 503/504），按 `Status` 取结果的调用方拿到原样的状态码。
    #[tokio::test]
    async fn a_call_past_the_request_timeout_ends_with_deadline_exceeded() {
        let as_flare: Result<(), FlareError> = bounded_request(
            Some(Duration::from_millis(20)),
            std::future::pending::<Result<(), FlareError>>(),
        )
        .await;
        assert_eq!(
            as_flare.unwrap_err().code(),
            Some(ErrorCode::OperationTimeout)
        );

        let as_status: Result<(), tonic::Status> = bounded_request(
            Some(Duration::from_millis(20)),
            std::future::pending::<Result<(), tonic::Status>>(),
        )
        .await;
        let status = as_status.unwrap_err();
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
        assert_eq!(status.message(), DOWNSTREAM_REQUEST_TIMED_OUT);
    }

    /// 超时之内的结果（成功或下游自己的错误）原样返回；不设超时时一直等。
    #[tokio::test]
    async fn results_within_the_timeout_and_unbounded_calls_pass_through() {
        let ok: Result<u32, tonic::Status> =
            bounded_request(Some(Duration::from_secs(10)), async { Ok(7) }).await;
        assert_eq!(ok.unwrap(), 7);

        let denied: Result<(), tonic::Status> =
            bounded_request(Some(Duration::from_secs(10)), async {
                Err(tonic::Status::permission_denied("no"))
            })
            .await;
        assert_eq!(denied.unwrap_err().code(), tonic::Code::PermissionDenied);

        let slow: Result<u32, tonic::Status> = bounded_request(None, async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(1)
        })
        .await;
        assert_eq!(slow.unwrap(), 1);
    }
}
