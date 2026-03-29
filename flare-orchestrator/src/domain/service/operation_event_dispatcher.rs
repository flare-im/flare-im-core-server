//! 操作事件派发器（事件驱动）
//!
//! 操作服务产生 [crate::domain::event::MessageOperationDomainEvent] 后，由此 trait 统一派发：
//! 1. 将 proto Event 写入 Kafka 操作流（Storage Writer 消费）
//! 2. 根据领域事件构建 Push 并写入推送流
//! Context 由 gRPC 请求透传，写入 Kafka 时注入租户/请求信息。

use flare_server_core::context::Ctx;

use crate::domain::event::MessageOperationDomainEvent;

/// 操作事件派发器：一次调用完成「Kafka Event + Push」或仅 Event（如 Mark/Unmark）
pub trait OperationEventDispatcher: Send + Sync {
    /// 派发：先发布 proto Event 到操作 topic，再根据领域事件构建并发布 Push
    async fn dispatch(
        &self,
        ctx: &Ctx,
        proto_event: flare_proto::common::Event,
        domain_event: MessageOperationDomainEvent,
    ) -> crate::error::Result<()>;

    /// 仅发布 proto Event 到操作 topic（无 Push，用于 Mark/Unmark 等）
    async fn dispatch_event_only(
        &self,
        ctx: &Ctx,
        proto_event: flare_proto::common::Event,
    ) -> crate::error::Result<()>;
}
