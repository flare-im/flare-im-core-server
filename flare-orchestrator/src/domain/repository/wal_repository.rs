use crate::error::Result;
use std::future::Future;
use std::pin::Pin;

use crate::domain::model::MessageSubmission;

/// WAL 仓储接口（Rust 2024: 原生异步 trait）
pub trait WalRepository: Send + Sync {
    fn append<'a>(
        &'a self,
        submission: &'a MessageSubmission,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

    /// 根据消息ID从 WAL 中查询消息（用于权限验证时的 fallback）
    fn find_by_message_id<'a>(
        &'a self,
        message_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<flare_proto::common::Message>>> + Send + 'a>>;
}
