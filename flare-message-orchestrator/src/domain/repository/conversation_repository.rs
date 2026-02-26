use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use crate::error::Result;

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
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}