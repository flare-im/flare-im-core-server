//! 发送幂等账本的保留期清理（Port）。
//!
//! 账本只为 Redis 去重窗口之外的重发兜底，保留 7 天；过期行由 Storage Writer 分批删除，每批是一条
//! 独立语句，不在一个长事务里删完。

use std::future::Future;

use flare_server_core::error::Result;

pub trait IdempotencyLedgerRetention: Send + Sync {
    /// 删除入账时间早于 `cutoff` 的行，至多 `limit` 行，返回删除的行数。
    fn delete_created_before(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
        limit: u32,
    ) -> impl Future<Output = Result<u64>> + Send;
}
