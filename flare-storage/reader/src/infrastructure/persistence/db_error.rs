//! 数据库错误分类：连不上数据库是可重试的「暂不可用」，其余是数据库错误。
//!
//! 读侧拿不到连接（连接池等待超时或已关闭）、连接被拒或中途断开、TLS 失败、PostgreSQL 正在
//! 停机或暂不接受连接（SQLSTATE 08xxx、57P01–57P03、53300）时报 `ServiceUnavailable`，gRPC
//! 层据此回 UNAVAILABLE，调用方可以稍后重试；SQL、解码、约束等错误报 `DatabaseError`，仍是
//! INTERNAL。错误文本只进 `details`，不进 gRPC 状态。

use flare_server_core::error::{ErrorCode, FlareError, Result, map_infra_error};

/// 连接类错误的 SQLSTATE（不含 `08` 整类，整类另判）。
const UNAVAILABLE_SQLSTATES: [&str; 4] = ["57P01", "57P02", "57P03", "53300"];

pub(crate) fn db_error(err: sqlx::Error, context: &str) -> FlareError {
    let code = if is_connection_failure(&err) {
        ErrorCode::ServiceUnavailable
    } else {
        ErrorCode::DatabaseError
    };
    map_infra_error(err, code, context)
}

fn is_connection_failure(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => true,
        sqlx::Error::Database(database) => database.code().is_some_and(|code| {
            code.starts_with("08") || UNAVAILABLE_SQLSTATES.contains(&code.as_ref())
        }),
        _ => false,
    }
}

/// `.context(...)` 的数据库版：按上面的规则分类，而不是一律当成系统错误。
pub(crate) trait DbResultExt<T> {
    fn db_context(self, context: &str) -> Result<T>;
}

impl<T> DbResultExt<T> for std::result::Result<T, sqlx::Error> {
    fn db_context(self, context: &str) -> Result<T> {
        self.map_err(|err| db_error(err, context))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_failures_are_unavailable_and_the_rest_are_database_errors() {
        let refused =
            || sqlx::Error::Io(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));
        for err in [
            refused(),
            sqlx::Error::PoolTimedOut,
            sqlx::Error::PoolClosed,
            sqlx::Error::WorkerCrashed,
        ] {
            let mapped = db_error(err, "query");
            assert_eq!(
                mapped.code(),
                Some(ErrorCode::ServiceUnavailable),
                "{mapped}"
            );
        }
        for err in [
            sqlx::Error::RowNotFound,
            sqlx::Error::ColumnNotFound("seq".into()),
            sqlx::Error::Protocol("unexpected message".into()),
        ] {
            let mapped = db_error(err, "query");
            assert_eq!(mapped.code(), Some(ErrorCode::DatabaseError), "{mapped}");
        }
        let failed: std::result::Result<(), sqlx::Error> = Err(refused());
        assert_eq!(
            failed.db_context("query").unwrap_err().code(),
            Some(ErrorCode::ServiceUnavailable)
        );
    }
}
