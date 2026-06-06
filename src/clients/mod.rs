mod conversation_client;
mod media_client;
mod message_client;
mod online_client;
mod resolver;
mod storage_client;

use flare_server_core::error::Result;
use serde::Deserialize;
use std::sync::Arc;

use crate::config::FlareAppConfig;

pub use conversation_client::{
    ConversationManageServiceClientWrapper, ConversationReadServiceClientWrapper,
};
pub use flare_server_core::context::Ctx;
pub use media_client::MediaServiceClientWrapper;
pub use message_client::{MessageActionServiceClientWrapper, MessageSendServiceClientWrapper};
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
    /// MessageOrchestrator 路由
    pub message_service_url: String,
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
    pub message_static_fallback: String,
    #[serde(default)]
    pub conversation_static_fallback: String,
    #[serde(default)]
    pub online_static_fallback: String,
    #[serde(default)]
    pub storage_reader_static_fallback: String,
    /// 连接超时(秒)
    pub connect_timeout_secs: u64,
    /// 请求超时(秒)
    pub request_timeout_secs: u64,
}

/// gRPC 客户端管理器。
///
/// 网关所有 HTTP handler 通过该聚合对象访问下游服务，避免 handler 自己维护
/// channel、metadata、重连和超时策略。
pub struct GrpcClients {
    pub media: Arc<tokio::sync::Mutex<MediaServiceClientWrapper>>,
    pub message_send: Arc<tokio::sync::Mutex<MessageSendServiceClientWrapper>>,
    pub message_action: Arc<tokio::sync::Mutex<MessageActionServiceClientWrapper>>,
    pub conversation_read: Arc<tokio::sync::Mutex<ConversationReadServiceClientWrapper>>,
    pub conversation_manage: Arc<tokio::sync::Mutex<ConversationManageServiceClientWrapper>>,
    pub online: Arc<tokio::sync::Mutex<OnlineServiceClientWrapper>>,
    pub storage_reader: Arc<tokio::sync::Mutex<StorageReaderServiceClientWrapper>>,
}

impl GrpcClients {
    /// 通过服务发现（或静态回退）初始化所有下游 gRPC 客户端。
    pub async fn new(app_config: Arc<FlareAppConfig>, grpc: &DownstreamGrpcConfig) -> Result<Self> {
        let resolver = DownstreamGrpcResolver::new(Arc::clone(&app_config), grpc.clone());

        let media_channel = resolver.connect(DownstreamKind::Media).await?;
        let message_channel = resolver
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
            message_send: Arc::new(tokio::sync::Mutex::new(
                MessageSendServiceClientWrapper::from_channel(message_channel.clone()),
            )),
            message_action: Arc::new(tokio::sync::Mutex::new(
                MessageActionServiceClientWrapper::from_channel(message_channel),
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
        })
    }
}

fn optional_static_fallback(uri: &str) -> Option<String> {
    if uri.trim().is_empty() {
        None
    } else {
        Some(uri.to_string())
    }
}
