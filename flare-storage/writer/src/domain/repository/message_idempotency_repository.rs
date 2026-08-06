//! 消息幂等仓储（Port）
//!
//! **功能**：基于服务端/客户端消息 ID 判断消息是否已处理，用于写前去重与幂等。
//! - 写入前调用 `is_new` / `is_new_by_client_msg_id`，若已存在则跳过或返回 Duplicate ack。
//! - 典型实现：Redis 等 KV 存储，key 为 message_id，TTL 与业务保留策略一致。

use flare_im_contracts::Ctx;
use flare_im_contracts::utils::normalize_tenant_id;

/// 客户端消息幂等 key 的**唯一**作用域规则（tenant/sender/conversation 归一化 + 兜底 "0"）。
/// trait 默认实现与所有存储实现都必须经由此函数——任何一处 fork 都会静默分裂去重键空间，
/// 造成跨实现的重复检测假阴性（生产不可见）。
pub fn scoped_client_idempotency_key(
    ctx: &Ctx,
    client_msg_id: &str,
    sender_id: Option<&str>,
    conversation_id: Option<&str>,
) -> String {
    let tenant_id = ctx
        .tenant_id()
        .filter(|tenant_id| !tenant_id.trim().is_empty())
        .map(normalize_tenant_id)
        .unwrap_or_else(|| "0".to_string());
    let sender_id = sender_id
        .filter(|sender_id| !sender_id.trim().is_empty())
        .unwrap_or("0");
    let conversation_id = conversation_id
        .filter(|conversation_id| !conversation_id.trim().is_empty())
        .unwrap_or("0");
    format!("client:{tenant_id}:{sender_id}:{conversation_id}:{client_msg_id}")
}

pub trait MessageIdempotencyRepository: Send + Sync {
    /// 检查消息ID是否为新消息（基于服务端消息ID）
    async fn is_new(&self, ctx: &Ctx, message_id: &str) -> flare_server_core::error::Result<bool>;

    /// 释放一次尚未完成 durable write 的服务端消息 ID 预占坑。
    ///
    /// 默认体是**有意的最佳努力 no-op**：仅“检查不预占”的幂等实现（`is_new`
    /// 不占坑，例如内存/测试 mock）本就无坑可释放，返回 `Ok(())` 是正确语义，
    /// 且调用方 `release_idempotency_reservation` 已把释放失败当最佳努力吞掉并 `warn!`。
    ///
    /// ⚠️ 但凡 `is_new` 会**预占坑**的实现（如 Redis `SET NX`），都**必须** override
    /// 本方法，否则失败路径下坑永不释放 → 客户端在 TTL 到期前无法重发（坑泄漏）。
    /// 这与 `ArchiveStoreRepository` 的写方法不同：那里 no-op = 静默丢数据，故那里默认体直接报错。
    async fn release(&self, ctx: &Ctx, message_id: &str) -> flare_server_core::error::Result<()> {
        let _ = (ctx, message_id);
        Ok(())
    }

    /// 检查客户端消息ID是否为新消息（用于去重）；默认委托给 is_new。
    async fn is_new_by_client_msg_id(
        &self,
        ctx: &Ctx,
        client_msg_id: &str,
        sender_id: Option<&str>,
        conversation_id: Option<&str>,
    ) -> flare_server_core::error::Result<bool> {
        if client_msg_id.is_empty() {
            return Ok(true);
        }
        self.is_new(
            ctx,
            &scoped_client_idempotency_key(ctx, client_msg_id, sender_id, conversation_id),
        )
        .await
    }

    /// 释放一次尚未完成 durable write 的客户端消息 ID 预占坑。
    async fn release_by_client_msg_id(
        &self,
        ctx: &Ctx,
        client_msg_id: &str,
        sender_id: Option<&str>,
        conversation_id: Option<&str>,
    ) -> flare_server_core::error::Result<()> {
        if client_msg_id.is_empty() {
            return Ok(());
        }
        self.release(
            ctx,
            &scoped_client_idempotency_key(ctx, client_msg_id, sender_id, conversation_id),
        )
        .await
    }
}
