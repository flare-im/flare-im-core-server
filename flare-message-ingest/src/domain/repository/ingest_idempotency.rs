//! 摄入边界幂等：按 `client_msg_id` 去重重复发送。
//!
//! ## 为什么需要
//! `MessageSubmission::prepare` 每次发送都生成新的随机 `server_id`，`prepare_and_allocate_seq` 又**无条件**
//! 分配 conversation_seq。在"首发成功但 ACK 丢失 → SDK reliable-queue 重试"的场景下，重试会：
//! 1. 烧掉一个新的 seq（产生序列空洞）；
//! 2. 在 deliver-then-persist 下，重复 echo（新 server_id）在 storage 持久化去重之前已经投递给客户端，
//!    客户端按 server_id 查不到、本地消息已 Sent 非 pending → 渲染成**可见重复**。
//!
//! storage 的 SETNX 去重发生在投递之后，太晚。本仓储在**摄入入口**（分配 seq、投递之前）按 `client_msg_id`
//! 幂等：重试直接返回首发的 `(server_id, conversation_seq)`，不再分配 seq、不再投递。

use async_trait::async_trait;
use flare_proto::common::SendAckDurability;
use flare_server_core::error::Result;

/// 首发成功后记录的幂等结果，用于在重试时重建 ACK。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotentRecord {
    pub server_id: String,
    pub conversation_seq: u64,
    pub durability: SendAckDurability,
}

/// `begin` 的三态结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdempotencyBegin {
    /// 首次见到该 key：占位成功，调用方继续正常处理，结束后须 `commit` 或失败时 `rollback`。
    Fresh,
    /// 已有完成结果（真正的重试）：调用方直接用该结果重建 ACK，**不要**再处理。
    Replay(IdempotentRecord),
    /// 同 key 正在处理中（并发重试，少见——SDK 对同一消息是串行重试）：调用方返回可重试错误。
    InFlight,
}

/// 摄入幂等存储。实现需保证 `begin` 的占位是原子的（如 Redis `SET NX`）。
#[async_trait]
pub trait IngestIdempotencyStore: Send + Sync {
    /// 占位或读取既有结果。见 [`IdempotencyBegin`]。
    async fn begin(&self, key: &str) -> Result<IdempotencyBegin>;
    /// 首发成功后记录结果（覆盖占位，延长 TTL）。
    async fn commit(&self, key: &str, record: &IdempotentRecord) -> Result<()>;
    /// 首发失败后清除占位，使后续重试可干净地重新处理。
    async fn rollback(&self, key: &str) -> Result<()>;
}
