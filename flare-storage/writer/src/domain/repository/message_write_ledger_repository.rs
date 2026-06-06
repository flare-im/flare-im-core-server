//! Message write ledger state repository.
//!
//! The ledger is an operational state machine for the storage path. It is
//! separate from message/query models and exists so retry, replay, and admin
//! diagnostics can see where a message stopped.

use std::future::Future;
use std::pin::Pin;

use flare_im_core::Ctx;
use flare_server_core::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageWriteStage {
    ArchivePersisted,
    StoragePersisted,
    WalCleaned,
    WalCleanupFailed,
    AckPublished,
    AckPublishFailed,
}

impl MessageWriteStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ArchivePersisted => "archive_persisted",
            Self::StoragePersisted => "storage_persisted",
            Self::WalCleaned => "wal_cleaned",
            Self::WalCleanupFailed => "wal_cleanup_failed",
            Self::AckPublished => "ack_published",
            Self::AckPublishFailed => "ack_publish_failed",
        }
    }

    pub fn timestamp_column(self) -> &'static str {
        match self {
            Self::ArchivePersisted => "archive_persisted_at",
            Self::StoragePersisted => "storage_persisted_at",
            Self::WalCleaned => "wal_cleaned_at",
            Self::WalCleanupFailed | Self::AckPublishFailed => "failed_at",
            Self::AckPublished => "ack_published_at",
        }
    }
}

pub trait MessageWriteLedgerRepository: Send + Sync {
    fn mark_stage<'a>(
        &'a self,
        ctx: &'a Ctx,
        tenant_id: &'a str,
        message_id: &'a str,
        stage: MessageWriteStage,
        error: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}
