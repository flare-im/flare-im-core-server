use crate::error::Result;
use std::future::Future;
use std::pin::Pin;

/// Conversation 仓储接口 - 用于确保 conversation 存在（Rust 2024: 原生异步 trait）
pub trait ConversationRepository: Send + Sync {
    /// 确保 conversation 存在，如果不存在则创建
    fn ensure_conversation<'a>(
        &'a self,
        ctx: &'a flare_server_core::context::Context,
        conversation_id: &'a str,
        conversation_type: &'a str,
        business_type: &'a str,
        participants: Vec<String>,
        // 落库 conversations.channel_id：单聊须空；非单聊为消息 channel_id
        stored_channel_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

    /// 标记会话已读（更新未读数；read_seq 为 0 时由 Conversation 服务用 last_message_seq）
    fn mark_conversation_as_read<'a>(
        &'a self,
        ctx: &'a flare_server_core::context::Context,
        conversation_id: &'a str,
        read_seq: i64,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}
