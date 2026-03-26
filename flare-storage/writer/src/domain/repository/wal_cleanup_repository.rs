//! WAL 清理仓储（Port）
//!
//! **功能**：消息持久化完成后，从写前 WAL（如 Redis List/Stream）中移除对应条目，避免堆积与重复消费。
//! - `remove(message_id)`：按消息 ID 删除 WAL 中的记录。
//!
//! 典型实现：Redis（如 RedisWalCleanupRepository）。

use anyhow::Result;
use flare_server_core::context::Ctx;

pub trait WalCleanupRepository: Send + Sync {
    async fn remove(&self, ctx: &Ctx, message_id: &str) -> Result<()>;
}
