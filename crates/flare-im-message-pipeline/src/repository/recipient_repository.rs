//! 接收者仓储接口
//!
//! 提供获取消息和事件接收者 ID 的能力。
//!
//! ## 核心职责
//! 1. 根据会话 ID 和会话类型获取消息接收者 ID
//! 2. 根据消息 ID 和会话 ID 获取事件接收者 ID
//!
//! ## 设计原则
//! - 读模型：只读操作，不修改数据
//! - 缓存优先：优先从缓存获取，减少数据库查询
//! - 降级策略：缓存失败时降级到数据库

use std::future::Future;
use std::pin::Pin;

use flare_im_contracts::Ctx;
use flare_server_core::error::Result;

use crate::model::ConversationType;

/// 接收者仓储接口
///
/// ## 职责
/// 1. 获取消息接收者 ID 列表
/// 2. 获取事件接收者 ID 列表
///
/// ## 实现
/// - Infrastructure 层提供具体实现（如 Redis、Database）
/// - 支持缓存和降级
///
/// ## Rust 2024 兼容性
/// 使用 `Pin<Box<dyn Future>>` 返回类型以支持 `dyn Trait`
pub trait RecipientRepository: Send + Sync {
    /// 根据会话 ID 和会话类型获取消息接收者 ID 列表
    fn get_message_recipients<'a>(
        &'a self,
        ctx: &'a Ctx,
        conversation_id: &'a str,
        conversation_type: ConversationType,
        channel_id: Option<&'a str>,
        sender_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>>;

    /// 根据消息 ID 和会话 ID 获取事件接收者 ID 列表
    fn get_event_recipients<'a>(
        &'a self,
        ctx: &'a Ctx,
        message_id: &'a str,
        conversation_id: &'a str,
        event_type: flare_proto::common::EventType,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>>;

    /// 根据会话 ID 获取会话成员列表
    fn get_conversation_members<'a>(
        &'a self,
        ctx: &'a Ctx,
        conversation_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>>;
}

/// 辅助函数：判断会话类型是否需要查询成员列表
pub fn needs_member_lookup(conversation_type: ConversationType) -> bool {
    matches!(
        conversation_type,
        ConversationType::Group
            | ConversationType::Ai
            | ConversationType::Customer
            | ConversationType::System
    )
}
