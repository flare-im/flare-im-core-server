//! 插件 gRPC 通道解析：委托 [`flare_im_core::discovery::resolve_grpc_channel`]。

use tonic::transport::Channel;

pub(crate) use flare_im_core::discovery::DISCOVERY_CHANNEL_TIMEOUT as PLUGIN_DISCOVERY_TIMEOUT;

/// 按 `RegisteredPluginInstance.grpc_authority` 解析 gRPC 通道（`http(s)://` 或 `discovery://`）。
pub async fn resolve_plugin_channel(grpc_authority: &str) -> Result<Channel, String> {
    flare_im_core::discovery::resolve_grpc_channel(grpc_authority).await
}
