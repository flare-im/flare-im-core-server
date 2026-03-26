use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use crate::error::Result;

/// WalRepository 的枚举封装，用于在 Rust 2024 下避免 `dyn` + async trait 带来的
/// `E0038: trait is not dyn compatible` 问题。
#[derive(Debug)]
pub enum WalRepositoryItem {
    Noop(Arc<crate::infrastructure::persistence::noop_wal::NoopWalRepository>),
    Redis(Arc<crate::infrastructure::persistence::redis_wal::RedisWalRepository>),
}

impl crate::domain::repository::wal_repository::WalRepository for WalRepositoryItem {
    fn append<'a>(
        &'a self,
        submission: &'a crate::domain::model::MessageSubmission,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        match self {
            WalRepositoryItem::Noop(repo) => Box::pin(repo.append(submission)),
            WalRepositoryItem::Redis(repo) => Box::pin(repo.append(submission)),
        }
    }

    fn find_by_message_id<'a>(
        &'a self,
        message_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<flare_proto::common::Message>>> + Send + 'a>> {
        match self {
            WalRepositoryItem::Noop(repo) => Box::pin(repo.find_by_message_id(message_id)),
            WalRepositoryItem::Redis(repo) => Box::pin(repo.find_by_message_id(message_id)),
        }
    }
}