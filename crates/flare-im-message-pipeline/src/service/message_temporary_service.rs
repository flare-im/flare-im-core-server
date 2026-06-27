//! 临时消息处理服务
//!
//! 处理临时消息（SYSTEM、NOTIFICATION、CUSTOM）：
//! - 只推送，不持久化
//! - 不经过 WAL
//! - 不分配 seq
//! - 离线消息直接丢弃

use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_proto::common::{Message, MessageType};
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tracing::instrument;

use crate::{MqPushRepository, PushRepository};

/// 临时消息处理服务
pub struct MessageTemporaryService {
    /// 推送仓储（使用具体类型以支持 async fn in traits）
    push_repository: Arc<MqPushRepository>,
}

impl MessageTemporaryService {
    pub fn new(push_repository: Arc<MqPushRepository>) -> Self {
        Self { push_repository }
    }

    /// 处理临时消息（只推送，不持久化）
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = %message.server_id,
        message_type = message.message_type,
        conversation_id = %message.conversation_id
    ))]
    pub async fn handle_temporary_message(&self, ctx: &Ctx, message: &Message) -> Result<()> {
        let conversation_id = message.conversation_id.trim();
        if conversation_id.is_empty() {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "conversation_id is required",
            )
            .build_error());
        }

        let recipient_user_ids = self.extract_recipient_user_ids(message);
        if recipient_user_ids.is_empty() {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "temporary message recipient_user_ids is required",
            )
            .build_error());
        }
        tracing::trace!(
            message_id = %message.server_id,
            conversation_id = %message.conversation_id,
            message_type = message.message_type,
            recipient_count = recipient_user_ids.len(),
            "Pushing temporary message without persistence"
        );

        self.push_repository
            .push_only_message(
                ctx,
                message.clone(),
                recipient_user_ids,
                conversation_id.to_string(),
            )
            .await
    }

    /// 提取接收者用户 ID 列表
    fn extract_recipient_user_ids(&self, message: &Message) -> Vec<String> {
        let mut user_ids = Vec::new();

        if let Ok(flare_proto::common::ConversationType::Single) =
            flare_proto::common::ConversationType::try_from(message.conversation_type)
            && !message.channel_id.is_empty()
        {
            user_ids.push(message.channel_id.clone());
        }

        user_ids
    }
}

/// 临时消息类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemporaryMessageType {
    /// 系统事件
    SystemEvent,
    /// 自定义临时消息
    Custom,
}

impl TemporaryMessageType {
    /// 从消息类型判断是否为临时消息
    pub fn from_message_type(message_type: i32) -> Option<Self> {
        match MessageType::try_from(message_type).ok()? {
            MessageType::System | MessageType::Notification => Some(Self::SystemEvent),
            MessageType::Custom => Some(Self::Custom),
            _ => None,
        }
    }

    /// 是否需要持久化
    pub fn needs_persistence(&self) -> bool {
        false // 临时消息都不持久化
    }

    /// 是否需要分配 seq
    pub fn needs_seq(&self) -> bool {
        false // 临时消息都不分配 seq
    }

    /// 是否需要在线推送
    pub fn require_online(&self) -> bool {
        true // 临时消息只在线推送
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporary_message_type_is_derived_from_current_proto_message_types() {
        assert_eq!(
            TemporaryMessageType::from_message_type(MessageType::System as i32),
            Some(TemporaryMessageType::SystemEvent)
        );
        assert_eq!(
            TemporaryMessageType::from_message_type(MessageType::Notification as i32),
            Some(TemporaryMessageType::SystemEvent)
        );
        assert_eq!(
            TemporaryMessageType::from_message_type(MessageType::Custom as i32),
            Some(TemporaryMessageType::Custom)
        );
        assert_eq!(
            TemporaryMessageType::from_message_type(MessageType::Text as i32),
            None
        );
    }
}
