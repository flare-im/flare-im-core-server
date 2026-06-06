//! 下游 gRPC 通道解析：优先注册中心发现（Consul / Mesh），本地无注册中心时回退静态 URI。

use std::sync::Arc;

use crate::clients::DownstreamGrpcConfig;
use crate::config::FlareAppConfig;
use crate::discovery::{
    connect_grpc_channel_from_app_config, invalidate_discovered_service,
    is_discovery_route_authority, resolve_grpc_channel,
};
use crate::service_names::{CONVERSATION, MEDIA, ORCHESTRATOR, SIGNALING_ONLINE, STORAGE_READER};
use flare_server_core::error::{AnyhowContext, Result};
use tonic::transport::Channel;
use tracing::info;

/// 下游逻辑服务类型（与 [`flare_im_core::service_names`] 对齐）。
#[derive(Debug, Clone, Copy)]
pub enum DownstreamKind {
    Media,
    MessageOrchestrator,
    Conversation,
    SignalingOnline,
    StorageReader,
}

impl DownstreamKind {
    fn service_name(self) -> &'static str {
        match self {
            Self::Media => MEDIA,
            Self::MessageOrchestrator => ORCHESTRATOR,
            Self::Conversation => CONVERSATION,
            Self::SignalingOnline => SIGNALING_ONLINE,
            Self::StorageReader => STORAGE_READER,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Media => "media",
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
        self.connect_with_route(kind, route, fallback).await
    }

    async fn connect_with_route(
        &self,
        kind: DownstreamKind,
        route: &str,
        static_fallback: &str,
    ) -> Result<Channel> {
        let label = kind.label();
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
                    Self::try_static_fallback(label, static_fallback, discover_err).await
                }
            }
        } else if route.starts_with("http://") || route.starts_with("https://") {
            info!(
                service = label,
                route, "resolving downstream gRPC via static URI override"
            );
            resolve_grpc_channel(route)
                .await
                .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))
                .with_context(|| format!("connect static override for {label} at {route}"))
        } else {
            info!(
                service = label,
                service_name = kind.service_name(),
                "resolving downstream gRPC via app registry (with static fallback)"
            );
            connect_grpc_channel_from_app_config(
                &self.app_config,
                kind.service_name(),
                static_fallback,
            )
            .await
            .inspect(|_| {
                info!(
                    service = label,
                    service_name = kind.service_name(),
                    "downstream gRPC channel ready"
                );
            })
            .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))
            .with_context(|| format!("connect {label} ({})", kind.service_name()))
        }
    }

    fn target_for(&self, kind: DownstreamKind) -> (&str, &str) {
        match kind {
            DownstreamKind::Media => (
                self.grpc.media_service_url.as_str(),
                self.grpc.media_static_fallback.as_str(),
            ),
            DownstreamKind::MessageOrchestrator => (
                self.grpc.message_service_url.as_str(),
                self.grpc.message_static_fallback.as_str(),
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

    async fn try_static_fallback(
        label: &str,
        static_fallback: &str,
        discover_err: String,
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
        resolve_grpc_channel(static_fallback).await.map_err(|e| {
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
