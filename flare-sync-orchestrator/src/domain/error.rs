use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError};

pub fn cursor_regression(previous: i64, attempted: i64) -> FlareError {
    ErrorBuilder::new(
        ErrorCode::SyncCursorRegression,
        "同步游标不可回退，请重新拉取快照后再上报",
    )
    .param("previous_seq", previous.to_string())
    .param("attempted_seq", attempted.to_string())
    .details("若多端并发，请以较大 last_seq 为准或触发全量同步".to_string())
    .build_error()
}
