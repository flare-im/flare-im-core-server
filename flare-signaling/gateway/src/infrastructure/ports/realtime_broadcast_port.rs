//! 把一条轻量信令广播给**其余**网关节点（服务发现里除自己以外的实例）。
//!
//! 走的是 `AccessGateway.RelayRealtimeControl`，与消息的统一读扩散同一套连接池与服务发现，
//! 但载荷刻意不同：那条路运的是有 seq、会推进水位的 messages/events，这条路运的是不落库、
//! 不占 seq 的瞬时帧。

use std::sync::Arc;

use async_trait::async_trait;
use flare_grpc_proto::access_gateway::RelayRealtimeControlRequest;
use flare_im_service_kit::gateway::GatewayRouter;

use crate::domain::service::IRealtimeBroadcastPort;

pub struct GatewayRealtimeBroadcast {
    router: Arc<GatewayRouter>,
    /// 本节点的实例 id：发起节点已经转给过自己的连接，广播时要跳过自己。
    local_gateway_id: String,
}

impl GatewayRealtimeBroadcast {
    pub fn new(router: Arc<GatewayRouter>, local_gateway_id: String) -> Self {
        Self {
            router,
            local_gateway_id,
        }
    }
}

#[async_trait]
impl IRealtimeBroadcastPort for GatewayRealtimeBroadcast {
    async fn broadcast(&self, conversation_id: &str, packet: Vec<u8>) {
        if conversation_id.is_empty() || packet.is_empty() {
            return;
        }
        self.router
            .broadcast_relay_realtime_control(
                RelayRealtimeControlRequest {
                    conversation_id: conversation_id.to_string(),
                    packet,
                },
                Some(&self.local_gateway_id),
            )
            .await;
    }
}
