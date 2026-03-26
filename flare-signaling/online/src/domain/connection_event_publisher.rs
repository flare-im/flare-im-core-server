//! 连接领域事件发布端口（与 flare_im_core Connection BC 对齐）
//!
//! 当 Gateway 在 Login/Logout 的 metadata 中传入 connection_id 时，Online 可选择性发布
//! core 的 ConnectionEvent，便于跨 BC 溯源与审计。默认不注入则为 no-op。


use flare_im_core::ConnectionEvent;

pub trait ConnectionEventPublisher: Send + Sync {
    /// 发布连接领域事件（ConnectionRegistered / ConnectionKicked / ConnectionDisconnected）
    async fn publish(&self, event: &ConnectionEvent) -> anyhow::Result<()>;
}

/// 默认无操作实现（Rust 2024：避免 `dyn ConnectionEventPublisher`）
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopConnectionEventPublisher;

impl ConnectionEventPublisher for NoopConnectionEventPublisher {
    async fn publish(&self, _event: &ConnectionEvent) -> anyhow::Result<()> {
        Ok(())
    }
}
