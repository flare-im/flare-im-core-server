use crate::domain::model::MessageSubmission;
use crate::domain::repository::{WalPendingMessage, WalRepository};
use flare_server_core::error::Result;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// 无 WAL 时的占位实现。
#[derive(Debug, Default)]
pub struct NoopWalRepository;

impl WalRepository for NoopWalRepository {
    fn append<'a>(
        &'a self,
        _submission: &'a MessageSubmission,
        _tenant_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { Ok(()) })
    }

    fn find_by_message_id<'a>(
        &'a self,
        _message_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<flare_proto::common::Message>>> + Send + 'a>>
    {
        Box::pin(async { Ok(None) })
    }

    fn list_pending<'a>(
        &'a self,
        _limit: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<WalPendingMessage>>> + Send + 'a>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn remove<'a>(
        &'a self,
        _message_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

/// WalRepository 的枚举封装，用于在 Rust 2024 下避免 `dyn` + async trait 带来的
/// `E0038: trait is not dyn compatible` 问题。
#[derive(Debug)]
pub enum WalRepositoryItem {
    Noop(Arc<NoopWalRepository>),
    Redis(Arc<crate::infrastructure::persistence::redis_wal::RedisWalRepository>),
}

impl WalRepository for WalRepositoryItem {
    fn append<'a>(
        &'a self,
        submission: &'a crate::domain::model::MessageSubmission,
        tenant_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        match self {
            WalRepositoryItem::Noop(repo) => Box::pin(repo.append(submission, tenant_id)),
            WalRepositoryItem::Redis(repo) => Box::pin(repo.append(submission, tenant_id)),
        }
    }

    fn find_by_message_id<'a>(
        &'a self,
        message_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<flare_proto::common::Message>>> + Send + 'a>>
    {
        match self {
            WalRepositoryItem::Noop(repo) => Box::pin(repo.find_by_message_id(message_id)),
            WalRepositoryItem::Redis(repo) => Box::pin(repo.find_by_message_id(message_id)),
        }
    }

    fn list_pending<'a>(
        &'a self,
        limit: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<WalPendingMessage>>> + Send + 'a>> {
        match self {
            WalRepositoryItem::Noop(repo) => Box::pin(repo.list_pending(limit)),
            WalRepositoryItem::Redis(repo) => Box::pin(repo.list_pending(limit)),
        }
    }

    fn remove<'a>(
        &'a self,
        message_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        match self {
            WalRepositoryItem::Noop(repo) => Box::pin(repo.remove(message_id)),
            WalRepositoryItem::Redis(repo) => Box::pin(repo.remove(message_id)),
        }
    }
}
