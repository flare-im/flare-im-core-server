//! RPC 客户端抽象层
//!
//! 本模块定义了与外部服务通信的 RPC 客户端抽象 trait，
//! 实现框架无关设计，支持未来切换不同的 RPC 框架（如从 tonic 切换到 volo）。

mod impl_;

pub use impl_::ConversationClient;

use crate::model::ConversationType;
use flare_server_core::context::Context;
use flare_server_core::error::Result;
use std::future::Future;
use std::pin::Pin;

/// 会话服务 RPC 客户端抽象
///
/// 框架无关的会话服务客户端接口，业务层通过此 trait 调用下游会话服务。
/// 当前实现使用 tonic，未来可切换到 volo 等其他 RPC 框架。
pub trait ConversationRpcClient: Send + Sync {
    /// 确保会话存在（创建或获取已有会话）
    ///
    /// # 参数
    /// - `ctx`: 请求上下文，包含 trace_id、user_id 等追踪信息
    /// - `conversation_id`: 会话 ID
    /// - `conversation_type`: 会话类型（单聊/群聊/频道）
    /// - `business_type`: 业务类型
    /// - `participants`: 参与者用户 ID 列表
    /// - `stored_channel_id`: 存储的频道 ID（单聊为空，非单聊为消息 channel_id）
    fn ensure_conversation<'a>(
        &'a self,
        ctx: &'a Context,
        conversation_id: &'a str,
        conversation_type: ConversationType,
        business_type: &'a str,
        participants: Vec<String>,
        stored_channel_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

    /// 获取会话成员列表
    ///
    /// # 参数
    /// - `ctx`: 请求上下文
    /// - `conversation_id`: 会话 ID
    ///
    /// # 返回
    /// 成员用户 ID 列表
    fn get_conversation_members<'a>(
        &'a self,
        ctx: &'a Context,
        conversation_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>>;

    /// 获取会话成员数量。
    fn get_conversation_member_count<'a>(
        &'a self,
        ctx: &'a Context,
        conversation_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<usize>> + Send + 'a>>;

    /// 标记会话已读
    ///
    /// # 参数
    /// - `ctx`: 请求上下文
    /// - `conversation_id`: 会话 ID
    /// - `read_seq`: 已读序列号
    fn mark_conversation_as_read<'a>(
        &'a self,
        ctx: &'a Context,
        conversation_id: &'a str,
        read_seq: i64,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}
