//! 轻量信令（typing / presence / 已读光标）的直转。
//!
//! 一份实现，两个入口：上行帧由本节点的 [`SendDataDomainService`] 聚合后调它，其余网关节点经
//! `AccessGateway.RelayRealtimeControl` 收到同一帧后也调它。两边的语义必须逐字相同——写成两份，
//! 迟早会变成两种行为。
//!
//! 语义（与消息路径刻意不同）：
//! - **不落库、不占 seq、不动水位、不 ACK**；丢失可接受、最新覆盖旧。
//! - 不调 `ensure_conversation_members_subscribed`：这是高频路径，依赖消息活动已经建立的订阅；
//!   未订阅者收不到属于有损语义，不值得为它付一次成员解析。
//! - 复杂度 O(本节点该会话在线连接数)，与群总人数无关。
//!
//! [`SendDataDomainService`]: crate::domain::service::SendDataDomainService

use std::sync::Arc;

use flare_im_contracts::Ctx;

use crate::domain::ports::IPushPort;
use crate::domain::service::ConversationSubscriptionRegistry;

/// 把一条已编码的 RealtimeControl 帧广播给**其余**网关节点。
///
/// 发起节点自己的连接已经由本地直转覆盖，所以实现要跳过自己；单实例部署没有对等节点，是空操作。
#[async_trait::async_trait]
pub trait IRealtimeBroadcastPort: Send + Sync {
    async fn broadcast(&self, conversation_id: &str, packet: Vec<u8>);
}

/// 不广播：单节点部署，或还没接上服务发现时的行为——本地直转已经覆盖了全部在线订阅者。
pub struct NoRealtimeBroadcast;

#[async_trait::async_trait]
impl IRealtimeBroadcastPort for NoRealtimeBroadcast {
    async fn broadcast(&self, _conversation_id: &str, _packet: Vec<u8>) {}
}

/// 会话在线订阅集合内的直转。
pub struct RealtimeControlRelay {
    conversation_subscriptions: Arc<ConversationSubscriptionRegistry>,
    push_port: Arc<dyn IPushPort>,
}

impl RealtimeControlRelay {
    pub fn new(
        conversation_subscriptions: Arc<ConversationSubscriptionRegistry>,
        push_port: Arc<dyn IPushPort>,
    ) -> Self {
        Self {
            conversation_subscriptions,
            push_port,
        }
    }

    /// 发给本节点订阅该会话的在线连接。`exclude_connection_id` 用于把发送方自己排除在外
    /// （presence/custom 原样直转时要排除；typing 聚合包**包含**发送方，客户端按自身 id 过滤显示）。
    ///
    /// 返回实际投递的连接数，便于调用方判断「本节点没有订阅者」与「发出去但失败了」。
    pub async fn relay_locally(
        &self,
        tx: &Ctx,
        conversation_id: &str,
        packet: Vec<u8>,
        exclude_connection_id: Option<&str>,
    ) -> usize {
        if conversation_id.is_empty() {
            return 0;
        }
        let targets: Vec<String> = self
            .conversation_subscriptions
            .local_subscribers(conversation_id)
            .into_iter()
            .filter(|connection_id| Some(connection_id.as_str()) != exclude_connection_id)
            .collect();
        if targets.is_empty() {
            return 0;
        }
        let payload_type = flare_core::common::protocol::payload_command::Type::Data as i32;
        if let Err(error) = self
            .push_port
            .push_payload_to_connections(tx, &targets, payload_type, packet)
            .await
        {
            // 有损 ephemeral：失败仅 trace，不回报发送方。
            tracing::trace!(%conversation_id, ?error, "realtime control relay push failed (ignored)");
            return 0;
        }
        targets.len()
    }
}
