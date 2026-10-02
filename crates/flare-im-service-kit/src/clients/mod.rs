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

use crate::config::FlareAppConfig;

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
    pub message_ingest_send: Arc<tokio::sync::Mutex<MessageSendServiceClientWrapper>>,
    pub message_event: Arc<tokio::sync::Mutex<MessageEventServiceClientWrapper>>,
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
        })
    }
}

/// 管理员调用：把 admin-gateway 已验签的 admin 令牌原样放进 `authorization`。
///
/// 网关不持有任何签名私钥，也不附带未签名的身份元数据；下游按 admin 受众重新验签，
/// 只认令牌里的员工主体与租户（见 `docs/SEC02-console-control-auth.md`）。
pub(crate) fn admin_bearer_request<T>(payload: T, admin_token: &str) -> Result<tonic::Request<T>> {
    let rejected = || {
        flare_server_core::error::ErrorBuilder::new(
            flare_server_core::error::ErrorCode::AuthenticationRequired,
            "admin token required for downstream admin call",
        )
        .build_error()
    };
    // JWT 只含可见 ASCII；HTTP 头允许的 obs-text（0x80–0xFF）在这里同样拒绝。
    if admin_token.is_empty() || !admin_token.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(rejected());
    }
    let value = tonic::metadata::MetadataValue::try_from(format!("Bearer {admin_token}"))
        .map_err(|_| rejected())?;
    let mut request = tonic::Request::new(payload);
    request.metadata_mut().insert("authorization", value);
    Ok(request)
}

/// 管理检索调用：admin 令牌原样转发（[`admin_bearer_request`]），外加请求号与追踪号。
///
/// 这两个号不是身份：Storage Reader 只用它们关联日志，并让同一请求号的导出落到同一个导出
/// 任务；租户与操作人只取自令牌。
pub(crate) fn admin_traced_request<T>(
    payload: T,
    admin_token: &str,
    ctx: &Ctx,
) -> Result<tonic::Request<T>> {
    let mut request = admin_bearer_request(payload, admin_token)?;
    for (key, value) in [
        (
            flare_server_core::context::keys::REQUEST_ID,
            ctx.request_id(),
        ),
        (flare_server_core::context::keys::TRACE_ID, ctx.trace_id()),
    ] {
        if value.is_empty() {
            continue;
        }
        if let Ok(value) = tonic::metadata::MetadataValue::try_from(value) {
            request.metadata_mut().insert(key, value);
        }
    }
    Ok(request)
}

fn optional_static_fallback(uri: &str) -> Option<String> {
    if uri.trim().is_empty() {
        None
    } else {
        Some(uri.to_string())
    }
}

#[cfg(test)]
mod admin_request_tests {
    use super::{admin_bearer_request, admin_traced_request};

    #[test]
    fn traced_admin_calls_add_only_the_request_and_trace_ids() {
        let ctx = std::sync::Arc::new(
            flare_server_core::context::Context::with_request_id("req-1")
                .with_trace_id("trace-1")
                .with_tenant_id("tenant-a")
                .with_user_id("employee-1"),
        );
        let request = admin_traced_request((), "a.b.c", &ctx).unwrap();
        let metadata = request.metadata();
        assert_eq!(metadata.get("authorization").unwrap(), "Bearer a.b.c");
        assert_eq!(metadata.get("x-request-id").unwrap(), "req-1");
        assert_eq!(metadata.get("x-trace-id").unwrap(), "trace-1");
        // 上下文里的租户与用户不随调用下发：下游只认令牌。
        for key in ["x-tenant-id", "x-user-id", "x-actor-id", "x-flare-ctx"] {
            assert!(metadata.get(key).is_none(), "{key}");
        }
        assert!(admin_traced_request((), "", &ctx).is_err());
    }

    #[test]
    fn forwards_the_admin_token_as_the_only_identity() {
        let request = admin_bearer_request((), "a.b.c").unwrap();
        let values: Vec<_> = request
            .metadata()
            .get_all("authorization")
            .iter()
            .map(|value| value.to_str().unwrap().to_string())
            .collect();
        assert_eq!(values, vec!["Bearer a.b.c".to_string()]);
        // 不附带任何未签名的身份元数据。
        for key in ["x-tenant-id", "x-user-id", "x-actor-id", "x-flare-ctx"] {
            assert!(request.metadata().get(key).is_none(), "{key}");
        }
    }

    #[test]
    fn rejects_empty_or_ambiguous_tokens() {
        for token in ["", "a.b c", "a.b.c\n", "a.b.\u{7f}c", "令牌"] {
            assert!(admin_bearer_request((), token).is_err(), "{token:?}");
        }
    }
}
