//! 下游 gRPC 通道解析：优先注册中心发现（Consul / Mesh），本地无注册中心时回退静态 URI。

use std::sync::Arc;

use crate::clients::DownstreamGrpcConfig;
use crate::config::FlareAppConfig;
use crate::discovery::{
    StaticChannelPolicy, connect_grpc_channel_from_app_config_with_policy,
    invalidate_discovered_service, is_discovery_route_authority, resolve_grpc_channel,
    resolve_grpc_channel_with_policy,
};
use crate::service_names::{
    CONVERSATION, MEDIA, MESSAGE_INGEST, ORCHESTRATOR, SIGNALING_ONLINE, STORAGE_READER,
};
use flare_server_core::error::{AnyhowContext, Result};
use tonic::transport::Channel;
use tracing::info;

/// 下游逻辑服务类型（与 [`flare_im_contracts::service_names`] 对齐）。
#[derive(Debug, Clone, Copy)]
pub enum DownstreamKind {
    Media,
    MessageIngest,
    MessageOrchestrator,
    Conversation,
    SignalingOnline,
    StorageReader,
}

impl DownstreamKind {
    fn service_name(self) -> &'static str {
        match self {
            Self::Media => MEDIA,
            Self::MessageIngest => MESSAGE_INGEST,
            Self::MessageOrchestrator => ORCHESTRATOR,
            Self::Conversation => CONVERSATION,
            Self::SignalingOnline => SIGNALING_ONLINE,
            Self::StorageReader => STORAGE_READER,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Media => "media",
            Self::MessageIngest => "message-ingest",
            Self::MessageOrchestrator => "message-orchestrator",
            Self::Conversation => "conversation",
            Self::SignalingOnline => "signaling-online",
            Self::StorageReader => "storage-reader",
        }
    }
}

/// 基于 [`FlareAppConfig`] + 路由配置的下游 gRPC 解析器。
#[derive(Clone)]
pub struct DownstreamGrpcResolver {
    app_config: Arc<FlareAppConfig>,
    grpc: DownstreamGrpcConfig,
}

impl DownstreamGrpcResolver {
    pub fn new(app_config: Arc<FlareAppConfig>, grpc: DownstreamGrpcConfig) -> Self {
        Self { app_config, grpc }
    }

    pub async fn connect(&self, kind: DownstreamKind) -> Result<Channel> {
        let (route, fallback) = self.target_for(kind);
        self.connect_route(kind.label(), kind.service_name(), route, fallback)
            .await
    }

    /// 解析一条不在 [`DownstreamGrpcConfig`] 里的下游路由（例如只有 admin-gateway
    /// 才连的 Capability）；发现与静态回退规则与 [`Self::connect`] 相同。
    ///
    /// 本解析器建的每条静态通道（`http(s)://` 覆盖、`discovery://` 发现失败后的静态回退、
    /// 裸服务名经注册中心解析失败或无注册中心时的静态回退）都按 [`Self::static_channel_policy`]
    /// 建连。网关的环境变量只接受 `discovery://` 与 `http(s)://` 路由，裸服务名只出现在直接调用
    /// 本方法的代码里，同样按配置的建连上限。
    pub async fn connect_route(
        &self,
        label: &str,
        service_name: &str,
        route: &str,
        static_fallback: &str,
    ) -> Result<Channel> {
        if is_discovery_route_authority(route) {
            info!(
                service = label,
                route, "resolving downstream gRPC via service discovery"
            );
            match resolve_grpc_channel(route).await {
                Ok(channel) => {
                    info!(
                        service = label,
                        route, "downstream gRPC channel ready (discovery)"
                    );
                    Ok(channel)
                }
                Err(discover_err) => {
                    Self::try_static_fallback(
                        label,
                        static_fallback,
                        discover_err,
                        self.static_channel_policy(),
                    )
                    .await
                }
            }
        } else if route.starts_with("http://") || route.starts_with("https://") {
            info!(
                service = label,
                route, "resolving downstream gRPC via static URI override"
            );
            resolve_grpc_channel_with_policy(route, self.static_channel_policy())
                .await
                .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))
                .with_context(|| format!("connect static override for {label} at {route}"))
        } else {
            info!(
                service = label,
                service_name, "resolving downstream gRPC via app registry (with static fallback)"
            );
            connect_grpc_channel_from_app_config_with_policy(
                &self.app_config,
                service_name,
                static_fallback,
                self.static_channel_policy(),
            )
            .await
            .inspect(|_| {
                info!(
                    service = label,
                    service_name, "downstream gRPC channel ready"
                );
            })
            .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))
            .with_context(|| format!("connect {label} ({service_name})"))
        }
    }

    fn target_for(&self, kind: DownstreamKind) -> (&str, &str) {
        match kind {
            DownstreamKind::Media => (
                self.grpc.media_service_url.as_str(),
                self.grpc.media_static_fallback.as_str(),
            ),
            DownstreamKind::MessageIngest => (
                self.grpc.message_ingest_service_url.as_str(),
                self.grpc.message_ingest_static_fallback.as_str(),
            ),
            DownstreamKind::MessageOrchestrator => (
                self.grpc.message_orchestrator_service_url.as_str(),
                self.grpc.message_orchestrator_static_fallback.as_str(),
            ),
            DownstreamKind::Conversation => (
                self.grpc.conversation_service_url.as_str(),
                self.grpc.conversation_static_fallback.as_str(),
            ),
            DownstreamKind::SignalingOnline => (
                self.grpc.online_service_url.as_str(),
                self.grpc.online_static_fallback.as_str(),
            ),
            DownstreamKind::StorageReader => (
                self.grpc.storage_reader_service_url.as_str(),
                self.grpc.storage_reader_static_fallback.as_str(),
            ),
        }
    }

    /// 静态地址（`http(s)://` 覆盖与静态回退）的建连策略，建连上限取配置的
    /// `GRPC_CONNECT_TIMEOUT_SECS`。
    pub fn static_channel_policy(&self) -> StaticChannelPolicy {
        self.grpc.static_channel_policy()
    }

    async fn try_static_fallback(
        label: &str,
        static_fallback: &str,
        discover_err: String,
        policy: StaticChannelPolicy,
    ) -> Result<Channel> {
        if static_fallback.trim().is_empty() {
            return Err(flare_server_core::error::FlareError::system(discover_err));
        }
        tracing::warn!(
            service = label,
            fallback = static_fallback,
            error = %discover_err,
            "service discovery failed, falling back to static gRPC URI"
        );
        resolve_grpc_channel_with_policy(static_fallback, policy)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "discovery failed ({discover_err}); static fallback also failed: {e}"
                ))
            })
    }

    /// 连接错误后清除发现缓存，便于下次 RPC 重新选实例（Pod 漂移 / 滚动发布）。
    pub fn invalidate(kind: DownstreamKind) {
        invalidate_discovered_service(kind.service_name());
    }
}

#[cfg(test)]
mod static_policy_tests {
    use super::*;
    use flare_grpc_proto::conversation::ListConversationsRequest;
    use flare_grpc_proto::conversation::conversation_read_service_client::ConversationReadServiceClient;
    use std::net::SocketAddr;
    use std::time::{Duration, Instant};
    use tokio::net::{TcpSocket, TcpStream};

    /// 只接受一条排队连接、从不 accept 的回环监听：占满队列后，新的建连既不成功也不被拒，
    /// 一直等到客户端自己的建连上限（对端的 SYN 被丢弃）。
    async fn stalled_listener() -> (tokio::net::TcpListener, Vec<TcpStream>, SocketAddr) {
        let socket = TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(1).unwrap();
        let address = listener.local_addr().unwrap();
        let mut attempts = tokio::task::JoinSet::new();
        for _ in 0..8 {
            attempts.spawn(tokio::time::timeout(
                Duration::from_millis(300),
                TcpStream::connect(address),
            ));
        }
        let mut held = Vec::new();
        while let Some(attempt) = attempts.join_next().await {
            if let Ok(Ok(Ok(stream))) = attempt {
                held.push(stream);
            }
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(300), TcpStream::connect(address))
                .await
                .is_err(),
            "the listener queue must be full so that new connects stall"
        );
        (listener, held, address)
    }

    fn app_config_without_registry() -> FlareAppConfig {
        toml::from_str(
            r#"
            [service]
            name = "resolver-test"
            version = "0"

            [server]
            address = "127.0.0.1"
            port = 1
            "#,
        )
        .expect("minimal app config")
    }

    fn downstream(connect_timeout_secs: u64) -> DownstreamGrpcConfig {
        let unused = "http://127.0.0.1:9".to_string();
        DownstreamGrpcConfig {
            media_service_url: unused.clone(),
            message_ingest_service_url: unused.clone(),
            message_orchestrator_service_url: unused.clone(),
            conversation_service_url: unused.clone(),
            online_service_url: unused.clone(),
            storage_reader_service_url: unused,
            media_static_fallback: String::new(),
            message_ingest_static_fallback: String::new(),
            message_orchestrator_static_fallback: String::new(),
            conversation_static_fallback: String::new(),
            online_static_fallback: String::new(),
            storage_reader_static_fallback: String::new(),
            connect_timeout_secs,
            request_timeout_secs: 10,
        }
    }

    /// 裸服务名路由在没有注册中心时走静态回退：该通道同样按配置的建连上限
    /// （`GRPC_CONNECT_TIMEOUT_SECS`）建连，而不是缺省的 2 秒。建连卡住时，配置 3 秒的通道
    /// 要等满约 3 秒才报错；仍按缺省策略的话 2 秒就报错。
    #[tokio::test]
    async fn bare_service_name_fallback_uses_the_configured_connect_bound() {
        let (_listener, _held, address) = stalled_listener().await;
        let resolver =
            DownstreamGrpcResolver::new(Arc::new(app_config_without_registry()), downstream(3));
        assert_eq!(
            resolver.static_channel_policy().connect_timeout,
            Duration::from_secs(3)
        );
        let channel = resolver
            .connect_route(
                "conversation",
                CONVERSATION,
                CONVERSATION,
                &format!("http://{address}"),
            )
            .await
            .expect("lazy static fallback channel");

        let started = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(15),
            ConversationReadServiceClient::new(channel)
                .list_conversations(ListConversationsRequest::default()),
        )
        .await
        .expect("a stalled connect ends at the connect bound");
        let elapsed = started.elapsed();
        assert!(result.is_err(), "nothing answers on the stalled listener");
        assert!(
            elapsed >= Duration::from_millis(2600),
            "gave up after {elapsed:?}: the default 2 s connect bound was used"
        );
        assert!(elapsed < Duration::from_secs(10), "{elapsed:?}");
    }
}
