use crate::domain::SyncDomainError;

/// 用户在某会话的同步游标只允许单调前移；多端并发时取较大值合并，避免误报回退。
pub fn merge_cursor_monotonic(previous_last_seq: Option<i64>, new_last_seq: i64) -> i64 {
    match previous_last_seq {
        Some(p) => new_last_seq.max(p),
        None => new_last_seq,
    }
}

/// 显式回退（小于已存水位）仍拒绝，供需要严格校验的路径使用。
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
