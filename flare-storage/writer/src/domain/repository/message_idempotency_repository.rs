//! 消息幂等仓储（Port）
//!
//! **功能**：基于服务端/客户端消息 ID 判断消息是否已处理，用于写前去重与幂等。
//! - 写入前调用 `is_new` / `is_new_by_client_msg_id`，若已存在则跳过或返回 Duplicate ack。
//! - 典型实现：Redis 等 KV 存储，key 为 message_id，TTL 与业务保留策略一致。

use flare_im_core::Ctx;

pub trait MessageIdempotencyRepository: Send + Sync {
    /// 检查消息ID是否为新消息（基于服务端消息ID）
    async fn is_new(&self, ctx: &Ctx, message_id: &str) -> flare_server_core::error::Result<bool>;

    /// 释放一次尚未完成 durable write 的服务端消息 ID 预占坑。
    async fn release(&self, ctx: &Ctx, message_id: &str) -> flare_server_core::error::Result<()> {
        let _ = (ctx, message_id);
        Ok(())
    }

    /// 检查客户端消息ID是否为新消息（用于去重）；默认委托给 is_new。
    async fn is_new_by_client_msg_id(
        &self,
        ctx: &Ctx,
        client_msg_id: &str,
        _sender_id: Option<&str>,
    ) -> flare_server_core::error::Result<bool> {
        if client_msg_id.is_empty() {
            return Ok(true);
        }
        self.is_new(ctx, client_msg_id).await
    }

    /// 释放一次尚未完成 durable write 的客户端消息 ID 预占坑。
    async fn release_by_client_msg_id(
        &self,
        ctx: &Ctx,
        client_msg_id: &str,
        _sender_id: Option<&str>,
    ) -> flare_server_core::error::Result<()> {
        if client_msg_id.is_empty() {
            return Ok(());
        }
        self.release(ctx, client_msg_id).await
    }
}
