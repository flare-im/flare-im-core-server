//! 事件领域服务 - 重构版
//!
//! ## 核心职责
//! 1. 事件校验（使用策略模式）
//! 2. 序列号分配（使用公共 SequenceAllocator）
//! 3. 事件推送（使用 PushRepository）
//!
//! ## 支持的事件类型
//! - 消息撤回 (EVENT_MESSAGE_RECALL)
//! - 消息编辑 (EVENT_MESSAGE_EDIT)
//! - 消息删除 (EVENT_MESSAGE_DELETE)
//! - 已读回执 (EVENT_READ_RECEIPT)
//! - 表情反应 (EVENT_REACTION)
//! - 置顶/取消置顶 (EVENT_PIN / EVENT_UNPIN)
//! - 标记/取消标记 (EVENT_MARK / EVENT_UNMARK)
//! - 自定义事件 (EVENT_CUSTOM)
//! - 通话信令 (EVENT_CALL_SIGNAL)：WebRTC / 媒体后端编排，落库前经 `CallCapabilityBridge` enrich（见 `handlers/plugin`）

use std::sync::Arc;

use flare_im_core::Ctx;
use flare_proto::common::call_audience::Shape as CallAudienceShape;
use flare_proto::common::event::Payload as EventPayload;
use flare_proto::common::{Event, EventType};
use flare_server_core::{flare_err, flare_err_details};
use tracing::instrument;

use crate::domain::PersistenceMode;
use crate::domain::repository::{PushRepository, RecipientRepository};
use crate::domain::service::call_signal_notice_message_builder::build_call_signal_notice_message;
use crate::domain::service::sequence_allocator::SequenceAllocator;
use crate::domain::service::validation_strategy::{
    CompositeEventValidationStrategy, EventValidationStrategy, ValidationContext,
};
use crate::error::{ErrorCode, Result};
use crate::infrastructure::messaging::push_repository::MqPushRepository;

/// 事件领域服务
pub trait SequenceAllocatorPort: Send + Sync {
    async fn allocate_seq(&self, conversation_id: &str, tenant_id: &str) -> Result<u64>;
}

impl SequenceAllocatorPort for SequenceAllocator {
    async fn allocate_seq(&self, conversation_id: &str, tenant_id: &str) -> Result<u64> {
        SequenceAllocator::allocate_seq(self, conversation_id, tenant_id).await
    }
}

pub struct EventDomainService<PR = MqPushRepository, SA = SequenceAllocator>
where
    PR: PushRepository,
    SA: SequenceAllocatorPort,
{
    /// 推送仓储（使用具体类型以支持 async fn in traits）
    push_repository: Arc<PR>,
    /// 接收者仓储
    recipient_repository: Arc<dyn RecipientRepository>,
    /// 序列号分配器
    sequence_allocator: Arc<SA>,
    /// 事件校验策略
    validation_strategy: Arc<dyn EventValidationStrategy>,
}

impl<PR, SA> EventDomainService<PR, SA>
where
    PR: PushRepository,
    SA: SequenceAllocatorPort,
{
    pub fn new(
        push_repository: Arc<PR>,
        recipient_repository: Arc<dyn RecipientRepository>,
        sequence_allocator: Arc<SA>,
        validation_strategy: Option<Arc<dyn EventValidationStrategy>>,
    ) -> Self {
        Self {
            push_repository,
            recipient_repository,
            sequence_allocator,
            validation_strategy: validation_strategy
                .unwrap_or_else(|| Arc::new(CompositeEventValidationStrategy::default_composite())),
        }
    }

    /// 校验事件
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `tenant_id`: 租户 ID
    /// - `event`: 事件
    ///
    /// # 返回
    /// - `Ok(())`: 校验通过
    /// - `Err`: 校验失败
    #[instrument(skip(self), fields(
        event_id = %event.event_id,
        conversation_id = %event.conversation_id,
    ))]
    pub async fn validate_event(&self, ctx: &Ctx, tenant_id: &str, event: &Event) -> Result<()> {
        let validation_context = ValidationContext {
            ctx,
            tenant_id,
            conversation_id: &event.conversation_id,
        };

        let validation_result = self
            .validation_strategy
            .validate(&validation_context, event)
            .await
            .map_err(|e| {
                flare_err!(
                    ErrorCode::InvalidParameter,
                    &format!("Event validation failed: {}", e)
                )
            })?;

        if !validation_result.is_valid {
            return Err(flare_err_details!(
                ErrorCode::InvalidParameter,
                "Event validation failed",
                format!("{:?}", validation_result.errors)
            ));
        }

        Ok(())
    }

    /// 分配序列号
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `tenant_id`: 租户 ID
    /// - `event`: 事件
    ///
    /// # 返回
    /// - `Ok(event_with_seq)`: 分配序列号后的事件
    /// - `Err`: 错误
    #[instrument(skip(self), fields(
        event_id = %event.event_id,
        conversation_id = %event.conversation_id,
    ))]
    pub async fn allocate_seq(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        mut event: Event,
    ) -> Result<Event> {
        let session_seq = self
            .sequence_allocator
            .allocate_seq(&event.conversation_id, tenant_id)
            .await
            .map_err(|e| {
                flare_err!(
                    ErrorCode::InternalError,
                    &format!(
                        "allocate seq failed for conversation_id={}: {}",
                        event.conversation_id, e
                    )
                )
            })?;

        tracing::trace!(
            conversation_id = %event.conversation_id,
            seq = session_seq,
            "Allocated session sequence for event"
        );

        event.seq = session_seq;
        Ok(event)
    }

    /// 获取接收者用户 ID 列表
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `event`: 事件
    ///
    /// # 返回
    /// - `Ok(recipient_user_ids)`: 接收者用户 ID 列表
    /// - `Err`: 错误
    #[instrument(skip(self), fields(
        conversation_id = %event.conversation_id,
    ))]
    pub async fn get_recipient_user_ids(&self, ctx: &Ctx, event: &Event) -> Result<Vec<String>> {
        if let Some(recipients) = resolve_recipients_from_event_payload(event) {
            return Ok(recipients);
        }

        let event_type = EventType::try_from(event.r#type).unwrap_or(EventType::Unspecified);
        let mut members = self
            .recipient_repository
            .get_conversation_members(ctx, &event.conversation_id)
            .await
            .map_err(|e| {
                flare_err!(
                    ErrorCode::InternalError,
                    &format!("Failed to get conversation members for event: {}", e)
                )
            })?;

        match event_type {
            EventType::EventTyping => {
                if let Some(EventPayload::Typing(t)) = &event.payload {
                    members.retain(|uid| uid != &t.user_id);
                } else if let Some(uid) = ctx.user_id() {
                    members.retain(|uid_m| uid_m != uid);
                }
            }
            EventType::EventReadReceipt => {
                if let Some(EventPayload::Read(r)) = &event.payload {
                    members.retain(|uid| uid != &r.user_id);
                }
            }
            _ => {}
        }

        Ok(members)
    }

    /// 仅推送事件（不持久化），由服务内部自动解析接收者
    ///
    /// 用于临时实时事件（如输入态、presence、通话信令）等场景。
    #[instrument(skip(self), fields(
        event_id = %event.event_id,
        conversation_id = %event.conversation_id,
        event_type = ?EventType::try_from(event.r#type),
    ))]
    pub async fn push_only(&self, ctx: &Ctx, event: Event) -> Result<()> {
        let recipient_user_ids = self.get_recipient_user_ids(ctx, &event).await?;
        self.push_only_with_recipients(ctx, event, recipient_user_ids)
            .await
    }

    /// 仅推送事件（不持久化），接收者由调用方显式提供
    ///
    /// 适用于上游已完成路由决策的场景，避免重复成员查询。
    #[instrument(skip(self, recipient_user_ids), fields(
        event_id = %event.event_id,
        conversation_id = %event.conversation_id,
        event_type = ?EventType::try_from(event.r#type),
        recipient_count = recipient_user_ids.len(),
    ))]
    pub async fn push_only_with_recipients(
        &self,
        ctx: &Ctx,
        event: Event,
        recipient_user_ids: Vec<String>,
    ) -> Result<()> {
        tracing::trace!(
            event_id = %event.event_id,
            conversation_id = %event.conversation_id,
            event_type = ?EventType::try_from(event.r#type),
            seq = event.seq,
            recipient_count = recipient_user_ids.len(),
            "Pushing event only (no persistence)"
        );

        let conversation_id = event.conversation_id.clone();
        self.push_repository
            .push_only_event(ctx, event, recipient_user_ids, conversation_id)
            .await
            .map_err(|e| {
                flare_err!(
                    ErrorCode::InternalError,
                    &format!("Failed to publish push-only event to MQ: {}", e)
                )
            })
    }

    /// 判断是否为临时事件（仅推送，不持久化）
    ///
    /// 根据 event.proto 定义，以下事件类型为临时事件：
    /// - EVENT_TYPING：正在输入（高频，无需持久化）
    /// - EVENT_PRESENCE：在线状态（实时性，无需持久化）
    /// - EVENT_CALL_SIGNAL：默认按临时事件处理；仅“终态信令”在 `push_event` 中提升为持久化
    fn is_temporary_event(event_type: EventType) -> bool {
        match event_type {
            EventType::EventTyping => true,     // 正在输入：高频，仅推送
            EventType::EventPresence => true,   // 在线状态：实时性，仅推送
            EventType::EventCallSignal => true, // 默认临时；终态由策略提升
            _ => false,                         // 其他事件：需要持久化
        }
    }

    /// 终态通话信令是否需要沉淀到会话历史。
    ///
    /// 仅保留用户可感知结果：
    /// - reject / busy
    /// - hangup（含取消、结束时长、异常中断等）
    ///
    /// 协商过程（invite/accept/ringing/ice/renegotiate/...）仅实时分发，不落聊天记录。
    fn should_persist_call_signal_terminal(event: &Event) -> bool {
        use flare_proto::common::call_signal_event::Signal;
        let Some(flare_proto::common::event::Payload::CallSignal(call)) = event.payload.as_ref()
        else {
            return false;
        };
        match call.signal.as_ref() {
            Some(Signal::Reject(_)) | Some(Signal::Busy(_)) | Some(Signal::Hangup(_)) => true,
            _ => false,
        }
    }

    /// 仅保存事件（持久化但不推送）
    ///
    /// 用于需要持久化但不需要实时推送的场景：
    /// - 事件记录（只需保存记录，不需要推送）
    /// - 审计日志（只需持久化，不需要推送）
    /// - 系统内部事件（只需保存，不需要推送）
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `event`: 事件
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self), fields(
        event_id = %event.event_id,
        conversation_id = %event.conversation_id,
        event_type = ?EventType::try_from(event.r#type),
    ))]
    pub async fn persistence_only(&self, ctx: &Ctx, event: Event) -> Result<()> {
        tracing::trace!(
            event_id = %event.event_id,
            conversation_id = %event.conversation_id,
            event_type = ?EventType::try_from(event.r#type),
            seq = event.seq,
            "Persisting event only (no push)"
        );

        let conversation_id = event.conversation_id.clone();
        self.push_repository
            .persistence_only_event(ctx, event, conversation_id)
            .await
            .map_err(|e| {
                flare_err!(
                    ErrorCode::InternalError,
                    &format!("Failed to publish persistence-only event to MQ: {}", e)
                )
            })
    }

    /// 推送事件
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `event`: 事件
    /// - `persistence_mode`: 持久化模式
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self), fields(
        event_id = %event.event_id,
        conversation_id = %event.conversation_id,
        event_type = ?EventType::try_from(event.r#type),
    ))]
    pub async fn push_event(
        &self,
        ctx: &Ctx,
        event: Event,
        persistence_mode: PersistenceMode,
    ) -> Result<()> {
        let event_type = EventType::try_from(event.r#type).unwrap_or(EventType::Unspecified);
        let is_temporary = Self::is_temporary_event(event_type);
        let should_push_only = match persistence_mode {
            PersistenceMode::Auto if event_type == EventType::EventCallSignal => {
                !Self::should_persist_call_signal_terminal(&event)
            }
            _ => persistence_mode.should_push_only(is_temporary),
        };
        if should_push_only {
            return self.push_only(ctx, event).await;
        }

        let recipient_user_ids = self.get_recipient_user_ids(ctx, &event).await?;
        tracing::trace!(
            event_id = %event.event_id,
            conversation_id = %event.conversation_id,
            event_type = ?event.r#type(),
            seq = event.seq,
            persistence_mode = ?persistence_mode,
            "Publishing event (persistence + push)"
        );

        // 终态通话信令附加沉淀为通知消息，沿消息主链路做 persistence+push，
        // 保障会话历史与多端同步一致。
        if let Some(call_notice) = build_call_signal_notice_message(&event) {
            self.push_repository
                .publish_message(
                    ctx,
                    call_notice,
                    recipient_user_ids.clone(),
                    event.conversation_id.clone(),
                )
                .await
                .map_err(|e| {
                    flare_err!(
                        ErrorCode::InternalError,
                        &format!("Failed to publish call notice message to MQ: {}", e)
                    )
                })?;
        }

        self.push_repository
            .publish_event(
                ctx,
                event.clone(),
                recipient_user_ids,
                event.conversation_id.clone(),
            )
            .await
            .map_err(|e| {
                flare_err!(
                    ErrorCode::InternalError,
                    &format!("Failed to publish event to MQ: {}", e)
                )
            })
    }
}

/// 从事件载荷解析推送目标，避免不必要的成员列表查询。
fn resolve_recipients_from_event_payload(event: &Event) -> Option<Vec<String>> {
    match &event.payload {
        Some(EventPayload::CallSignal(cs)) => {
            cs.audience.as_ref().and_then(|aud| match &aud.shape {
                Some(CallAudienceShape::Direct(d)) if !d.peer_user_id.trim().is_empty() => {
                    Some(vec![d.peer_user_id.clone()])
                }
                Some(CallAudienceShape::Explicit(e)) => {
                    let ids: Vec<String> = e
                        .user_ids
                        .iter()
                        .filter(|id| !id.trim().is_empty())
                        .cloned()
                        .collect();
                    if ids.is_empty() { None } else { Some(ids) }
                }
                _ => None,
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{EventDomainService, SequenceAllocatorPort};
    use crate::domain::PersistenceMode;
    use crate::domain::model::ConversationType;
    use crate::domain::repository::{PushRepository, RecipientRepository};
    use crate::error::{ErrorCode, Result};
    use flare_im_core::Ctx;
    use flare_proto::common::call_signal_event::Signal;
    use flare_proto::common::{
        CallBusy, CallHangup, CallReject, CallSignalEvent, Event, EventType,
        event::Payload as EventPayload,
    };
    use flare_server_core::flare_err;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};

    struct MockPushRepository {
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl MockPushRepository {
        fn record(&self, name: &str) -> Result<()> {
            let mut guard = self.calls.lock().map_err(|_| {
                flare_err!(
                    ErrorCode::InternalError,
                    "failed to lock call recorder in MockPushRepository"
                )
            })?;
            guard.push(name.to_string());
            Ok(())
        }
    }

    impl PushRepository for MockPushRepository {
        async fn publish_message(
            &self,
            _ctx: &Ctx,
            _message: flare_proto::common::Message,
            _recipient_user_ids: Vec<String>,
            _conversation_id: String,
        ) -> Result<()> {
            self.record("publish_message")
        }

        async fn publish_event(
            &self,
            _ctx: &Ctx,
            _event: Event,
            _recipient_user_ids: Vec<String>,
            _conversation_id: String,
        ) -> Result<()> {
            self.record("publish_event")
        }

        async fn persistence_only_message(
            &self,
            _ctx: &Ctx,
            _message: flare_proto::common::Message,
            _conversation_id: String,
        ) -> Result<()> {
            self.record("persistence_only_message")
        }

        async fn persistence_only_event(
            &self,
            _ctx: &Ctx,
            _event: Event,
            _conversation_id: String,
        ) -> Result<()> {
            self.record("persistence_only_event")
        }

        async fn push_only_message(
            &self,
            _ctx: &Ctx,
            _message: flare_proto::common::Message,
            _recipient_user_ids: Vec<String>,
            _conversation_id: String,
        ) -> Result<()> {
            self.record("push_only_message")
        }

        async fn push_only_event(
            &self,
            _ctx: &Ctx,
            _event: Event,
            _recipient_user_ids: Vec<String>,
            _conversation_id: String,
        ) -> Result<()> {
            self.record("push_only_event")
        }

        async fn publish_push_envelope(
            &self,
            _ctx: &Ctx,
            _envelope: flare_proto::common::PushEnvelope,
        ) -> Result<()> {
            self.record("publish_push_envelope")
        }
    }

    struct MockRecipientRepository;

    impl RecipientRepository for MockRecipientRepository {
        fn get_message_recipients<'a>(
            &'a self,
            _ctx: &'a Ctx,
            _conversation_id: &'a str,
            _conversation_type: ConversationType,
            _channel_id: Option<&'a str>,
            _sender_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<String>>> + Send + 'a>> {
            Box::pin(async { Ok(vec!["u1".to_string(), "u2".to_string()]) })
        }

        fn get_event_recipients<'a>(
            &'a self,
            _ctx: &'a Ctx,
            _message_id: &'a str,
            _conversation_id: &'a str,
            _event_type: EventType,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<String>>> + Send + 'a>> {
            Box::pin(async { Ok(vec!["u1".to_string(), "u2".to_string()]) })
        }

        fn get_conversation_members<'a>(
            &'a self,
            _ctx: &'a Ctx,
            _conversation_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<String>>> + Send + 'a>> {
            Box::pin(async { Ok(vec!["u1".to_string(), "u2".to_string()]) })
        }
    }

    struct MockSequenceAllocator;

    impl SequenceAllocatorPort for MockSequenceAllocator {
        async fn allocate_seq(&self, _conversation_id: &str, _tenant_id: &str) -> Result<u64> {
            Ok(1)
        }
    }

    fn make_call_event(signal: Signal) -> Event {
        Event {
            conversation_id: "c1".to_string(),
            seq: 10,
            r#type: EventType::EventCallSignal as i32,
            payload: Some(EventPayload::CallSignal(CallSignalEvent {
                call_id: "call-1".to_string(),
                conversation_id: "c1".to_string(),
                from_user_id: "u1".to_string(),
                signal: Some(signal),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    fn should_persist(event: &Event) -> bool {
        EventDomainService::<MockPushRepository, MockSequenceAllocator>::should_persist_call_signal_terminal(event)
    }

    #[test]
    fn terminal_call_signal_should_persist() {
        assert!(should_persist(&make_call_event(Signal::Reject(
            CallReject::default()
        ))));
        assert!(should_persist(&make_call_event(Signal::Busy(
            CallBusy::default()
        ))));
        assert!(should_persist(&make_call_event(Signal::Hangup(
            CallHangup::default()
        ))));
    }

    #[test]
    fn negotiation_call_signal_should_not_persist() {
        assert!(!should_persist(&make_call_event(Signal::Invite(
            Default::default()
        ))));
        assert!(!should_persist(&make_call_event(Signal::Accept(
            Default::default()
        ))));
    }

    #[test]
    fn hangup_should_persist_for_all_terminal_reason_codes() {
        // 1..=6: user_hangup/rejected/cancelled/no_answer_timeout/busy/failed
        for reason_code in 1..=6 {
            let event = make_call_event(Signal::Hangup(CallHangup {
                reason_code: Some(reason_code),
                ..Default::default()
            }));
            assert!(
                should_persist(&event),
                "hangup reason_code={reason_code} should persist"
            );
        }
    }

    #[test]
    fn non_call_event_or_invalid_payload_should_not_persist_as_call_terminal() {
        let non_call_event = Event {
            conversation_id: "c1".to_string(),
            seq: 11,
            r#type: EventType::EventTyping as i32,
            payload: None,
            ..Default::default()
        };
        assert!(!should_persist(&non_call_event));

        let call_event_without_signal = Event {
            conversation_id: "c1".to_string(),
            seq: 12,
            r#type: EventType::EventCallSignal as i32,
            payload: Some(EventPayload::CallSignal(CallSignalEvent {
                call_id: "call-2".to_string(),
                conversation_id: "c1".to_string(),
                from_user_id: "u2".to_string(),
                signal: None,
                ..Default::default()
            })),
            ..Default::default()
        };
        assert!(!should_persist(&call_event_without_signal));
    }

    #[tokio::test]
    async fn push_event_should_route_call_signal_by_terminal_policy() {
        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        let push_repository = Arc::new(MockPushRepository {
            calls: calls.clone(),
        });
        let recipient_repository: Arc<dyn RecipientRepository> = Arc::new(MockRecipientRepository);
        let sequence_allocator = Arc::new(MockSequenceAllocator);
        let service = EventDomainService::<MockPushRepository, MockSequenceAllocator>::new(
            push_repository,
            recipient_repository,
            sequence_allocator,
            None,
        );
        let ctx = Ctx::default();

        let invite_event = make_call_event(Signal::Invite(Default::default()));
        let invite_result = service
            .push_event(&ctx, invite_event, PersistenceMode::Auto)
            .await;
        assert!(invite_result.is_ok());
        let invite_calls = calls
            .lock()
            .map(|x| x.clone())
            .unwrap_or_else(|_| Vec::new());
        assert_eq!(invite_calls, vec!["push_only_event".to_string()]);

        if let Ok(mut guard) = calls.lock() {
            guard.clear();
        }

        let hangup_event = make_call_event(Signal::Hangup(CallHangup {
            reason_code: Some(6),
            ..Default::default()
        }));
        let hangup_result = service
            .push_event(&ctx, hangup_event, PersistenceMode::Auto)
            .await;
        assert!(hangup_result.is_ok());
        let hangup_calls = calls
            .lock()
            .map(|x| x.clone())
            .unwrap_or_else(|_| Vec::new());
        assert_eq!(
            hangup_calls,
            vec!["publish_message".to_string(), "publish_event".to_string()]
        );
    }
}
