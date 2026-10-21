use async_trait::async_trait;
use flare_server_core::error::Result;

/// Outcome of offering a read position to a reader's read watermark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadWatermarkAdvance {
    /// The offered position moved the watermark forward. `previous` is the value it
    /// replaced (0 when the reader had no watermark yet).
    Advanced { previous: u64 },
    /// The offered position is at or below the watermark; nothing changed.
    NotAdvanced { current: u64 },
}

/// Per-reader high-water mark of accepted read receipts, keyed by
/// (tenant, conversation, reader). It is the admission gate in front of seq
/// allocation: a read receipt that does not move its reader's watermark must not
/// become a new durable event.
///
/// The watermark only ever trails the participant read cursor kept by
/// flare-conversation (every admitted receipt raises that cursor to at least its
/// `read_seq`), so it never rejects a receipt that would still advance the cursor.
/// Implementations must make `advance` an atomic compare-and-raise so that
/// concurrent acks from several devices of the same reader admit at most one event
/// per new position.
#[async_trait]
pub trait ReadWatermarkRepository: Send + Sync {
    async fn advance(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        user_id: &str,
        read_seq: u64,
    ) -> Result<ReadWatermarkAdvance>;

    /// Undo an `advance` whose event never made it out (seq allocation or fanout
    /// failed), so the client's retry of the same position is admitted again.
    /// Only applies while the watermark still equals `advanced_to`; a newer
    /// position written meanwhile wins.
    async fn rollback(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        user_id: &str,
        advanced_to: u64,
        previous: u64,
    ) -> Result<()>;
}
