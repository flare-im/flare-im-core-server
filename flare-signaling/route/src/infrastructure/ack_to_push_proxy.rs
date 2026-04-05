//! 客户端 ACK 处理占位实现。
//!
//! 当前 PushService 已移除 PushAck RPC，这里保留调用点但不再转发到 Push Proxy。

use std::sync::Arc;

use flare_proto::common::Ack;
use flare_server_core::context::Context;
use tracing::debug;

use crate::error::Result;

/// 客户端 ACK 转发器（当前为 no-op，占位）。
pub struct AckToPushProxyForwarder;

impl AckToPushProxyForwarder {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }

    /// PushAck RPC 已下线：ACK 不再进入 push-proxy 链路，暂时仅记录日志。
    pub async fn forward_client_ack(&self, ctx: &Context, _ack: Ack) -> Result<()> {
        debug!(
            request_id = %ctx.request_id(),
            user_id = ctx.user_id().unwrap_or_default(),
            "skip client ack forward: PushAck RPC removed"
        );
        Ok(())
    }
}
