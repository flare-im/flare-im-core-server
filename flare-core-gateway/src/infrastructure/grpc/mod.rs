mod conversation_client;
mod media_client;
mod message_client;
mod resolver;

use anyhow::Result;
use std::sync::Arc;

use flare_im_core::config::FlareAppConfig;

use crate::config::GrpcConfig;

pub use conversation_client::{
    ConversationManageServiceClientWrapper, ConversationReadServiceClientWrapper,
};
pub use media_client::MediaServiceClientWrapper;
pub use message_client::{MessageActionServiceClientWrapper, MessageSendServiceClientWrapper};
pub use resolver::{DownstreamGrpcResolver, DownstreamKind};

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
}

impl GrpcClients {
    /// 通过服务发现（或静态回退）初始化所有下游 gRPC 客户端。
    pub async fn new(app_config: Arc<FlareAppConfig>, grpc: &GrpcConfig) -> Result<Self> {
        let resolver = DownstreamGrpcResolver::new(Arc::clone(&app_config), grpc.clone());

        let media_channel = resolver.connect(DownstreamKind::Media).await?;
        let message_channel = resolver
            .connect(DownstreamKind::MessageOrchestrator)
            .await?;
        let conversation_channel = resolver.connect(DownstreamKind::Conversation).await?;

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
