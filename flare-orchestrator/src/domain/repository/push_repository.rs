use flare_im_core::Ctx;
// 假设 Message 和 Event 类型由 flare_common_v1 proto 生成
use crate::error::Result;
use flare_proto::common::{Event, Message, PushEnvelope};

/// 推送仓储接口
///
/// 职责：
/// 1. 维护会话状态（写模型）
/// 2. 将消息或事件发布到 MQ（通过 MqEnvelope 封装）
///
/// 注意：使用 async fn in traits (Rust 2024 原生支持)
pub trait PushRepository: Send + Sync {
    /// 推送完整消息到 MQ（持久化 + 推送）
    ///
    /// Infrastructure 实现层需负责：
    /// 1. 构造 MqEnvelope (MQ_PAYLOAD_KIND_MESSAGE)
    /// 2. 从 Ctx 提取 trace_id/tenant_id 填充 headers
    /// 3. 生成 envelope_id 和 produced_at_ms
    /// 4. 发布到 JetStream（由 Storage Writer 消费并持久化）
    async fn publish_message(
        &self,
        ctx: &Ctx,
        message: Message,
        recipient_user_ids: Vec<String>,
        conversation_id: String,
    ) -> Result<()>;

    /// 推送领域事件到 MQ（持久化 + 推送）
    ///
    /// Infrastructure 实现层需负责：
    /// 1. 构造 MqEnvelope (MQ_PAYLOAD_KIND_EVENT)
    /// 2. 从 Ctx 提取 trace_id/tenant_id 填充 headers
    /// 3. 生成 envelope_id 和 produced_at_ms
    /// 4. 发布到 JetStream（由 Storage Writer 消费并持久化，然后推送给用户）
    async fn publish_event(
        &self,
        ctx: &Ctx,
        event: Event,
        recipient_user_ids: Vec<String>,
        conversation_id: String,
    ) -> Result<()>;

    /// 仅保存消息（持久化但不推送）
    ///
    /// 用于需要持久化但不需要实时推送的场景：
    /// - 消息已读回执（只需保存状态，不需要推送）
    /// - 消息撤回记录（只需保存记录，不需要推送）
    /// - 系统内部操作（只需持久化，不需要推送）
    ///
    /// Infrastructure 实现层需负责：
    /// 1. 构造 MqEnvelope (MQ_PAYLOAD_KIND_MESSAGE)
    /// 2. 设置 envelope.persistence_only = true（标记为仅持久化）
    /// 3. 从 Ctx 提取 trace_id/tenant_id 填充 headers
    /// 4. 生成 envelope_id 和 produced_at_ms
    /// 5. 发布到 JetStream（由 Storage Writer 消费并持久化，不推送给用户）
    async fn persistence_only_message(
        &self,
        ctx: &Ctx,
        message: Message,
        conversation_id: String,
    ) -> Result<()>;

    /// 仅保存事件（持久化但不推送）
    ///
    /// 用于需要持久化但不需要实时推送的场景：
    /// - 事件记录（只需保存记录，不需要推送）
    /// - 审计日志（只需持久化，不需要推送）
    /// - 系统内部事件（只需保存，不需要推送）
    ///
    /// Infrastructure 实现层需负责：
    /// 1. 构造 MqEnvelope (MQ_PAYLOAD_KIND_EVENT)
    /// 2. 设置 envelope.persistence_only = true（标记为仅持久化）
    /// 3. 从 Ctx 提取 trace_id/tenant_id 填充 headers
    /// 4. 生成 envelope_id 和 produced_at_ms
    /// 5. 发布到 JetStream（由 Storage Writer 消费并持久化，不推送给用户）
    async fn persistence_only_event(
        &self,
        ctx: &Ctx,
        event: Event,
        conversation_id: String,
    ) -> Result<()>;

    /// 仅推送消息（不持久化）
    ///
    /// 用于临时消息（TYPING、SYSTEM_EVENT）：
    /// - 只推送给在线用户
    /// - 不经过 WAL
    /// - 不持久化到数据库
    /// - 离线用户直接丢弃
    ///
    /// Infrastructure 实现层需负责：
    /// 1. 构造 MqEnvelope (MQ_PAYLOAD_KIND_MESSAGE)
    /// 2. 设置 envelope.push_only = true（标记为仅推送）
    /// 3. 从 Ctx 提取 trace_id/tenant_id 填充 headers
    /// 4. 生成 envelope_id 和 produced_at_ms
    /// 5. 发布到 JetStream（由 Push Service 直接消费并推送，不经过 Storage Writer）
    async fn push_only_message(
        &self,
        ctx: &Ctx,
        message: Message,
        recipient_user_ids: Vec<String>,
        conversation_id: String,
    ) -> Result<()>;

    /// 仅推送事件（不持久化）
    ///
    /// 用于临时事件（如正在输入、在线状态等）：
    /// - 只推送给在线用户
    /// - 不经过 WAL
    /// - 不持久化到数据库
    /// - 离线用户直接丢弃
    ///
    /// Infrastructure 实现层需负责：
    /// 1. 构造 MqEnvelope (MQ_PAYLOAD_KIND_EVENT)
    /// 2. 设置 envelope.push_only = true（标记为仅推送）
    /// 3. 从 Ctx 提取 trace_id/tenant_id 填充 headers
    /// 4. 生成 envelope_id 和 produced_at_ms
    /// 5. 发布到 JetStream（由 Push Service 直接消费并推送，不经过 Storage Writer）
    async fn push_only_event(
        &self,
        ctx: &Ctx,
        event: Event,
        recipient_user_ids: Vec<String>,
        conversation_id: String,
    ) -> Result<()>;

    /// 发布统一推送信封（ACK、通知、CustomData、系统消息）
    ///
    /// 用于不需要存储的推送场景：
    /// - ACK 推送（消息确认）
    /// - 通知推送（系统通知）
    /// - CustomData 推送（自定义数据）
    /// - 系统消息推送（系统公告）
    ///
    /// Infrastructure 实现层需负责：
    /// 1. 将 PushEnvelope 序列化
    /// 2. 从 Ctx 提取 trace_id/tenant_id 填充 headers（如果未设置）
    /// 3. 发布到 JetStream Push Topic（由 Push Server 消费并执行推送）
    async fn publish_push_envelope(&self, ctx: &Ctx, envelope: PushEnvelope) -> Result<()>;
}
