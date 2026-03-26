use std::future::Future;
use std::pin::Pin;

use crate::error::Result;

use crate::domain::model::MessageSubmission;
use crate::domain::repository::WalRepository;

#[derive(Debug, Default)]
pub struct NoopWalRepository;

impl WalRepository for NoopWalRepository {
    fn append<'a>(
        &'a self,
        _submission: &'a MessageSubmission,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { Ok(()) })
    }

    fn find_by_message_id<'a>(
        &'a self,
        _message_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<flare_proto::common::Message>>> + Send + 'a>> {
        Box::pin(async { Ok(None) })
    }
}

// shared() 方法已移除，现在使用 WalRepositoryItem::Noop 代替
