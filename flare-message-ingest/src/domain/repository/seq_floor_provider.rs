//! 会话序列号"下限(floor)"提供者。
//!
//! seq 分配以 Redis 计数器为高水位。一旦 Redis 被 flush / 实例重启 / key 丢失，缺失 key 的
//! `INCR` 会从 1 重新计数，而持久化历史（Postgres）仍保留旧的更高 seq —— 新消息因此拿到
//! **低于历史** 的 seq，客户端按 seq 升序渲染时会把新消息插到历史中间（见 seq 回退问题）。
//!
//! 本 trait 抽象"该会话在持久化存储中的权威 `max_seq`"，供摄入侧在进程内 **首次触达某会话**
//! 时作为 floor 喂给 [`flare_im_seq::SequenceAllocator::allocate_seq_with_floor`]，消除回退。

use flare_im_contracts::Ctx;
use flare_server_core::error::Result;
use std::future::Future;
use std::pin::Pin;

/// 会话持久化 `max_seq` 提供者（权威高水位下限）。
pub trait SeqFloorProvider: Send + Sync {
    /// 返回 `conversation_id` 在持久化存储中的权威 `max_seq`；无历史（新会话）时返回 0。
    ///
    /// 实现应查询**消息存储的权威 MAX(seq)**（而非会话服务的异步投影，后者可能滞后于已持久化
    /// 消息而导致 floor 偏低撞号）。查询失败时由调用方降级处理（绝不比现状更差）。
    fn persisted_max_seq<'a>(
        &'a self,
        ctx: &'a Ctx,
        tenant_id: &'a str,
        conversation_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<u64>> + Send + 'a>>;
}
