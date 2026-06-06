//! 存储处理器（编排层）- 处理 MqEnvelope 消息
//!
//! ## 核心职责
//! 1. 处理来自 MQ 的 MqEnvelope 消息
//! 2. 根据消息类型路由到对应的领域服务
//! 3. 将持久消息/事件 fanout 到存储流与推送流
//!
//! ## 设计原则
//! - 编排层：不包含业务逻辑，只负责流程编排
//! - 依赖注入：通过构造函数注入所有依赖
//! - 简化处理：直接调用领域服务，不处理复杂的标志逻辑

use std::sync::Arc;

use flare_im_core::Ctx;
use flare_proto::common::{MqEnvelope, MqPayloadKind, mq_envelope};
use tracing::instrument;

use crate::domain::service::{EventDomainService, MessageDomainService};
use flare_server_core::error::Result;

/// 存储处理器（编排层）
pub struct StorageHandler {
    /// 消息领域服务
    message_domain_service: Arc<MessageDomainService>,
    /// 事件领域服务
    event_domain_service: Arc<EventDomainService>,
}

impl StorageHandler {
    pub fn new(
        message_domain_service: Arc<MessageDomainService>,
        event_domain_service: Arc<EventDomainService>,
    ) -> Self {
        Self {
            message_domain_service,
            event_domain_service,
        }
    }

    /// 处理 MqEnvelope 消息
    ///
    /// # 编排流程
    /// 1. 根据 payload_kind 判断消息类型
    /// 2. 提取对应的载荷（Message 或 Event）
    /// 3. 调用领域服务 fanout 到存储与推送专用 topic
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `envelope`: MqEnvelope 消息
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        conversation_id = %envelope.conversation_id,
        payload_kind = ?envelope.payload_kind,
    ))]
    pub async fn handle_envelope(&self, ctx: &Ctx, envelope: MqEnvelope) -> Result<()> {
        match envelope.payload_kind {
            x if x == MqPayloadKind::Message as i32 => {
                // 提取 Message 载荷
                let message = match &envelope.payload {
                    Some(mq_envelope::Payload::Message(msg)) => msg.clone(),
                    _ => {
                        return Err(flare_server_core::error::FlareError::system(
                            "Message payload is missing in MqEnvelope".to_string(),
                        ));
                    }
                };

                tracing::trace!(
                    envelope_id = %envelope.envelope_id,
                    conversation_id = %envelope.conversation_id,
                    message_id = %message.server_id,
                    seq = envelope.seq,
                    "Processing message from MqEnvelope"
                );

                // 主流持久消息拆分为存储事件与实时推送事件。
                self.message_domain_service
                    .persist_and_push_with_recipients(ctx, message, envelope.recipient_user_ids)
                    .await?;
            }
            x if x == MqPayloadKind::Event as i32 => {
                // 提取 Event 载荷
                let event = match &envelope.payload {
                    Some(mq_envelope::Payload::Event(evt)) => evt.clone(),
                    _ => {
                        return Err(flare_server_core::error::FlareError::system(
                            "Event payload is missing in MqEnvelope".to_string(),
                        ));
                    }
                };

                tracing::trace!(
                    envelope_id = %envelope.envelope_id,
                    conversation_id = %envelope.conversation_id,
                    event_id = %event.event_id,
                    seq = envelope.seq,
                    "Processing event from MqEnvelope"
                );

                // 主流持久事件拆分为存储事件与实时推送事件。
                self.event_domain_service
                    .persist_and_push_with_recipients(ctx, event, envelope.recipient_user_ids)
                    .await?;
            }
            _ => {
                tracing::warn!(
                    envelope_id = %envelope.envelope_id,
                    conversation_id = %envelope.conversation_id,
                    payload_kind = envelope.payload_kind,
                    "Unknown payload kind in MqEnvelope, skipping"
                );
            }
        }

        Ok(())
    }
}
