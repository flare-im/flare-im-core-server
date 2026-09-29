//! 把一条轻量信令广播给**其余**网关节点（服务发现里除自己以外的实例）。
//!
//! 走的是 `AccessGateway.RelayRealtimeControl`，与消息的统一读扩散同一套连接池与服务发现，
//! 但载荷刻意不同：那条路运的是有 seq、会推进水位的 messages/events，这条路运的是不落库、
//! 不占 seq 的瞬时帧。

use std::sync::Arc;

use async_trait::async_trait;
use flare_grpc_proto::access_gateway::RelayRealtimeControlRequest;
use flare_im_contracts::Ctx;
use flare_im_service_kit::gateway::GatewayRouter;
use flare_server_core::{SignedActorType, SignedContextIdentity, SignedContextSigner};

use crate::domain::service::IRealtimeBroadcastPort;
use crate::infrastructure::connection_context::AuthenticatedConnectionOrigin;

pub struct GatewayRealtimeBroadcast {
    router: Arc<GatewayRouter>,
    /// 本节点的实例 id：发起节点已经转给过自己的连接，广播时要跳过自己。
    local_gateway_id: String,
    signer: Option<SignedContextSigner>,
}

impl GatewayRealtimeBroadcast {
    pub fn new(
        router: Arc<GatewayRouter>,
        local_gateway_id: String,
        signer: Option<SignedContextSigner>,
    ) -> Self {
        Self {
            router,
            local_gateway_id,
            signer,
        }
    }
}

#[async_trait]
impl IRealtimeBroadcastPort for GatewayRealtimeBroadcast {
    async fn broadcast(&self, tx: &Ctx, conversation_id: &str, packet: Vec<u8>) {
        if conversation_id.is_empty() || packet.is_empty() {
            return;
        }
        let Some(origin) = tx.get_data::<AuthenticatedConnectionOrigin>() else {
            tracing::warn!("realtime broadcast denied: authenticated connection origin missing");
            return;
        };
        if tx.tenant_id() != Some(origin.tenant_id()) || tx.user_id() != Some(origin.user_id()) {
            tracing::warn!("realtime broadcast denied: connection identity mismatch");
            return;
        }
        let Some(signer) = self.signer.as_ref() else {
            tracing::warn!("realtime broadcast disabled: internal context signer unavailable");
            return;
        };
        let identity = SignedContextIdentity::new(
            SignedActorType::User,
            origin.user_id(),
            origin.tenant_id(),
            crate::domain::service::realtime_relay::relay_body_request_id(conversation_id, &packet),
        )
        .with_credential_id("signaling-gateway");
        let Ok(token) = signer.sign(&identity) else {
            tracing::warn!("realtime broadcast denied: cannot sign connection identity");
            return;
        };
        self.router
            .broadcast_relay_realtime_control(
                RelayRealtimeControlRequest {
                    conversation_id: conversation_id.to_string(),
                    packet,
                },
                Some(&self.local_gateway_id),
                &token,
            )
            .await;
    }
}
