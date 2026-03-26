//! 消息事件发布抽象（Event Bus 端口）
//!
//! 定义与存储/操作/推送队列无关的发布接口，便于实现 Kafka、NATS、Pulsar 等不同后端，
//! 支持水平扩展与多租户分区。Kafka Topic 名见 [crate::constants::topics]；`event_type` 字符串见 [crate::abstractions::topics]。
//!
//! 设计原则（DDD+CQRS）：
//! - 端口使用领域类型 [StorageMessagePayload]，不直接传递 gRPC 请求类型；
//! - 首参为 [flare_server_core::context::Ctx]，租户/用户/请求 ID 由系统从 gRPC 提取并透传，
//!   写入 Kafka 时由实现方从 Ctx 注入到信封（tenant_id、request_id 等）。

use std::future::Future;
use std::pin::Pin;

use flare_proto::common::Event;
use flare_proto::push::PushMessageRequest;
use flare_server_core::context::Ctx;

use crate::abstractions::storage_payload::StorageMessagePayload;
use crate::error::Result;

/// 消息事件发布器（Event Bus 端口）
///
/// 实现方负责将 payload 发往对应 Topic（见 [crate::constants::topics]），
/// 当前默认实现为 Kafka。Ctx 由调用链从 gRPC 请求自动提取、透传，并在发往 Kafka 时写入信封。
pub trait MessageEventPublisher: Send + Sync {
    /// 发布消息到存储队列（如 `flare.im.message.created`）
    ///
    /// 使用领域类型 [StorageMessagePayload]；实现方从 `ctx` 注入 tenant_id、request_id 等并写入 Kafka。
    fn publish_storage<'a>(
        &'a self,
        ctx: &'a Ctx,
        payload: StorageMessagePayload,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

    /// 发布领域事件到统一事件流（与 `TOPIC_MESSAGE_EVENTS` 对齐），由 storage writer / conversation 等消费
    fn publish_event<'a>(
        &'a self,
        ctx: &'a Ctx,
        event: Event,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

    /// 发布推送任务到推送队列（如 `flare.im.push.tasks`）
    fn publish_push<'a>(
        &'a self,
        ctx: &'a Ctx,
        payload: PushMessageRequest,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

    /// 并行发布到存储队列和推送队列（仅普通消息场景）
    fn publish_both<'a>(
        &'a self,
        ctx: &'a Ctx,
        storage_payload: StorageMessagePayload,
        push_payload: PushMessageRequest,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}
