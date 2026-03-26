use thiserror::Error;

#[derive(Debug, Error)]
pub enum SyncDomainError {
    #[error("sync cursor regression: previous={previous}, attempted={attempted}")]
    CursorRegression { previous: i64, attempted: i64 },
}
