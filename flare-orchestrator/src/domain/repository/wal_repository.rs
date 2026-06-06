use flare_server_core::error::Result;
use std::future::Future;
use std::pin::Pin;

use crate::domain::model::MessageSubmission;

#[derive(Debug, Clone)]
pub struct WalPendingMessage {
    pub message_id: String,
    pub tenant_id: String,
    pub message: flare_proto::common::Message,
}

/// WAL 仓储接口（Rust 2024: 原生异步 trait）
pub trait WalRepository: Send + Sync {
    fn append<'a>(
        &'a self,
        submission: &'a MessageSubmission,
        tenant_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

    /// 根据消息ID从 WAL 中查询消息（用于权限验证时的 fallback）
    fn find_by_message_id<'a>(
        &'a self,
        message_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<flare_proto::common::Message>>> + Send + 'a>>;

    /// 列出仍在 WAL 中等待恢复的消息。
    fn list_pending<'a>(
        &'a self,
        limit: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<WalPendingMessage>>> + Send + 'a>>;

    /// broker 已确认接受后移除 WAL 记录。
    fn remove<'a>(
        &'a self,
        message_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}
