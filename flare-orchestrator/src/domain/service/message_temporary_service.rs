//! 临时消息处理服务 - 重构版
//!
//! 处理临时消息（TYPING、SYSTEM_EVENT）：
//! - 只推送，不持久化
//! - 不经过 WAL
//! - 不分配 seq
//! - 离线消息直接丢弃
//!
//! ## TODO
//! 当前实现标记为 TODO，等待后续完善推送逻辑。

use std::sync::Arc;

use flare_im_core::Ctx;
use flare_proto::common::Message;
use tracing::instrument;

use crate::infrastructure::messaging::push_repository::MqPushRepository;
use flare_server_core::error::Result;

/// 临时消息处理服务
pub struct MessageTemporaryService {
    /// 推送仓储（使用具体类型以支持 async fn in traits）
    #[allow(dead_code)]
    push_repository: Arc<MqPushRepository>,
}

impl MessageTemporaryService {
    pub fn new(push_repository: Arc<MqPushRepository>) -> Self {
        Self { push_repository }
    }

    /// 处理临时消息（只推送，不持久化）
    ///
    /// ## TODO
    /// 当前实现为占位符，等待后续完善：
    /// - 推送逻辑需要与 PushRepository 集成
    /// - 需要支持不同的临时消息类型
    /// - 需要添加消息校验
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = %message.server_id,
        message_type = message.message_type,
        conversation_id = %message.conversation_id
    ))]
    pub async fn handle_temporary_message(&self, ctx: &Ctx, message: &Message) -> Result<()> {
        // TODO: 实现临时消息推送逻辑
        // 当前为占位符实现，等待后续完善

        tracing::trace!(
            message_id = %message.server_id,
            conversation_id = %message.conversation_id,
            message_type = message.message_type,
            "Temporary message handling - TODO: implement push logic"
        );

        // 提取接收者用户 ID 列表
        let _recipient_user_ids = self.extract_recipient_user_ids(message);

        // TODO: 使用 PushRepository 推送消息
        // 当前 PushRepository 的 publish_message 方法需要完整的 Message
        // 临时消息可能需要特殊处理（如不分配 seq）

        // 暂时返回成功，等待后续实现
        Ok(())
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
    /// 正在输入
    Typing,
    /// 系统事件
    SystemEvent,
    /// 自定义临时消息
    Custom,
}

impl TemporaryMessageType {
    /// 从消息类型判断是否为临时消息
    pub fn from_message_type(message_type: i32) -> Option<Self> {
        let _ = message_type;
        None
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
