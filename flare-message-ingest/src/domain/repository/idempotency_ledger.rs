//! 发送幂等账本（PG `message_idempotency_ledger`）的只读端口。
//!
//! Redis 去重（[`super::IngestIdempotencyStore`]）只覆盖结果 TTL 这么长的窗口。客户端离线超过这个窗口后
//! 用同一 `client_msg_id` 重发，或 Redis 丢了去重键，摄入会把它当新消息处理。账本由 Storage Writer 在落库
//! 的同一事务里写入，按 `(tenant_id, sender_id, client_msg_id)` 唯一，保留 7 天；摄入只在
//! [`crate::domain::model::LedgerLookbackPolicy`] 判定需要时读它，新消息的热路径不访问 PG。

use async_trait::async_trait;
use flare_proto::common::SendAckDurability;
use flare_server_core::error::Result;

/// 账本里一次发送的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerRecord {
    /// 入账消息所在会话：重发不会换会话，会话不同说明 `client_msg_id` 被复用，不能回放。
    pub conversation_id: String,
    pub server_id: String,
    pub conversation_seq: u64,
    pub durability: SendAckDurability,
}

#[async_trait]
pub trait IdempotencyLedger: Send + Sync {
    /// 按发送者与 `client_msg_id` 读入账结果；没有入账返回 `None`。
    async fn find(
        &self,
        tenant_id: &str,
        sender_id: &str,
        client_msg_id: &str,
    ) -> Result<Option<LedgerRecord>>;
}
