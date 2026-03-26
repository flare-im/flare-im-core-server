use crate::domain::SyncDomainError;

/// 用户在某会话的同步游标只允许单调前移（或可相等），禁止回退导致「已确认」消息再次被当作未读/未同步。
pub fn ensure_cursor_monotonic(
    previous_last_seq: Option<i64>,
    new_last_seq: i64,
) -> Result<(), SyncDomainError> {
    if let Some(p) = previous_last_seq {
        if new_last_seq < p {
            return Err(SyncDomainError::CursorRegression {
                previous: p,
                attempted: new_last_seq,
            });
        }
    }
    Ok(())
}
