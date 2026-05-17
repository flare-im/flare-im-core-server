//! 校验策略模式
//!
//! 为消息和事件提供统一的校验框架，支持不同类型的校验策略。
//!
//! ## 设计
//! - 策略接口：定义校验契约
//! - 消息校验策略：校验消息内容、大小、权限等
//! - 事件校验策略：校验事件类型、操作权限等
//! - 组合策略：支持多个策略组合

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use flare_core::common::conversation::{is_single_chat_conversation, validate_conversation_id};
use flare_im_core::Ctx;
use flare_proto::common::call_audience::Shape as CallAudienceShape;
use flare_proto::common::call_signal_event::Signal;
use flare_proto::common::event::Payload;
use flare_proto::common::{CallSessionKind, CallSignalEvent, Event, EventType, Message};
use tracing::warn;

use crate::error::Result;

const LEGACY_EXT_KEYS: [&str; 4] = [
    "flareSdpType",
    "flareSdp",
    "flareCameraEnabled",
    "flareMicrophoneEnabled",
];

/// 校验上下文
pub struct ValidationContext<'a> {
    pub ctx: &'a Ctx,
    pub tenant_id: &'a str,
    pub conversation_id: &'a str,
}

/// 校验结果
#[derive(Debug)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl ValidationResult {
    pub fn valid() -> Self {
        Self {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    pub fn invalid(error: impl Into<String>) -> Self {
        Self {
            is_valid: false,
            errors: vec![error.into()],
            warnings: Vec::new(),
        }
    }

    pub fn with_warning(mut self, warning: impl Into<String>) -> Self {
        self.warnings.push(warning.into());
        self
    }

    pub fn merge(mut self, other: ValidationResult) -> Self {
        self.is_valid = self.is_valid && other.is_valid;
        self.errors.extend(other.errors);
        self.warnings.extend(other.warnings);
        self
    }
}

fn validate_call_signal_invite(conversation_id: &str, cs: &CallSignalEvent) -> ValidationResult {
    if validate_conversation_id(conversation_id.trim()).is_err() {
        return ValidationResult::invalid(
            "call_signal invite requires valid canonical conversation_id (CID v1)",
        );
    }
    let ms_kind_i32 = cs
        .media_session
        .as_ref()
        .map(|m| m.kind)
        .unwrap_or(CallSessionKind::Unspecified as i32);
    if ms_kind_i32 == CallSessionKind::Unspecified as i32 || ms_kind_i32 == 0 {
        return ValidationResult::invalid(
            "call_signal invite requires media_session.kind (DIRECT for single-chat, GROUP otherwise)",
        );
    }
    let shape = cs.audience.as_ref().and_then(|a| a.shape.as_ref());
    if is_single_chat_conversation(conversation_id.trim()) {
        if ms_kind_i32 != CallSessionKind::Direct as i32 {
            return ValidationResult::invalid(
                "single-chat call invite must set media_session.kind=DIRECT",
            );
        }
        match shape {
            Some(CallAudienceShape::Direct(d)) if !d.peer_user_id.trim().is_empty() => {}
            _ => {
                return ValidationResult::invalid(
                    "single-chat call invite must set audience.direct.peer_user_id",
                );
            }
        }
    } else {
        if ms_kind_i32 != CallSessionKind::Group as i32 {
            return ValidationResult::invalid(
                "non-single call invite must set media_session.kind=GROUP",
            );
        }
        match shape {
            Some(CallAudienceShape::Explicit(e)) => {
                let non_empty: Vec<&str> = e
                    .user_ids
                    .iter()
                    .map(|s| s.as_str().trim())
                    .filter(|s| !s.is_empty())
                    .collect();
                if non_empty.is_empty() {
                    return ValidationResult::invalid(
                        "call invite audience.explicit.user_ids must be non-empty (use audience.broadcast for ring-all)",
                    );
                }
            }
            Some(CallAudienceShape::Broadcast(_)) => {}
            _ => {
                return ValidationResult::invalid(
                    "non-single call invite must use audience.explicit or audience.broadcast",
                );
            }
        }
    }
    ValidationResult::valid()
}

/// 消息校验策略
///
/// ## Rust 2024 兼容性
/// 使用 `Pin<Box<dyn Future>>` 返回类型以支持 `dyn Trait`
pub trait MessageValidationStrategy: Send + Sync {
    /// 校验消息
    fn validate<'a>(
        &'a self,
        context: &'a ValidationContext<'a>,
        message: &'a Message,
    ) -> Pin<Box<dyn Future<Output = Result<ValidationResult>> + Send + 'a>>;
}

/// 事件校验策略
///
/// ## Rust 2024 兼容性
/// 使用 `Pin<Box<dyn Future>>` 返回类型以支持 `dyn Trait`
pub trait EventValidationStrategy: Send + Sync {
    /// 校验事件
    fn validate<'a>(
        &'a self,
        context: &'a ValidationContext<'a>,
        event: &'a Event,
    ) -> Pin<Box<dyn Future<Output = Result<ValidationResult>> + Send + 'a>>;
}

// =============================================================================
// 内置校验策略实现
// =============================================================================

/// 消息大小校验策略
pub struct MessageSizeValidationStrategy {
    max_message_size: usize,
}

impl MessageSizeValidationStrategy {
    pub fn new(max_message_size: usize) -> Self {
        Self { max_message_size }
    }
}

impl Default for MessageSizeValidationStrategy {
    fn default() -> Self {
        Self::new(10 * 1024 * 1024) // 10MB
    }
}

impl MessageValidationStrategy for MessageSizeValidationStrategy {
    fn validate<'a>(
        &'a self,
        _context: &'a ValidationContext<'a>,
        message: &'a Message,
    ) -> Pin<Box<dyn Future<Output = Result<ValidationResult>> + Send + 'a>> {
        Box::pin(async move {
            use prost::Message as _;

            let message_bytes = message.encode_to_vec();
            if message_bytes.len() > self.max_message_size {
                return Ok(ValidationResult::invalid(format!(
                    "Message size {} bytes exceeds maximum allowed size {} bytes",
                    message_bytes.len(),
                    self.max_message_size
                )));
            }

            Ok(ValidationResult::valid())
        })
    }
}

/// 消息必填字段校验策略
pub struct MessageRequiredFieldsValidationStrategy;

impl MessageValidationStrategy for MessageRequiredFieldsValidationStrategy {
    fn validate<'a>(
        &'a self,
        _context: &'a ValidationContext<'a>,
        message: &'a Message,
    ) -> Pin<Box<dyn Future<Output = Result<ValidationResult>> + Send + 'a>> {
        Box::pin(async move {
            let mut result = ValidationResult::valid();

            // 校验 conversation_id
            if message.conversation_id.is_empty() {
                result = result.merge(ValidationResult::invalid("conversation_id is required"));
            }

            // 校验 sender_id
            if message.sender_id.is_empty() {
                result = result.merge(ValidationResult::invalid("sender_id is required"));
            }

            // 校验单聊时的 channel_id
            if message.conversation_type == flare_proto::common::ConversationType::Single as i32
                && message.channel_id.is_empty()
            {
                result = result.merge(ValidationResult::invalid(
                    "channel_id (receiver_id) is required for single chat",
                ));
            }

            Ok(result)
        })
    }
}

/// 事件类型校验策略
pub struct EventTypeValidationStrategy {
    allowed_types: Vec<flare_proto::common::EventType>,
}

impl EventTypeValidationStrategy {
    pub fn new(allowed_types: Vec<flare_proto::common::EventType>) -> Self {
        Self { allowed_types }
    }

    pub fn all_supported() -> Self {
        use flare_proto::common::EventType;
        Self::new(vec![
            EventType::EventMessage,
            EventType::EventMessageRecall,
            EventType::EventMessageEdit,
            EventType::EventMessageDelete,
            EventType::EventReadReceipt,
            EventType::EventTyping,
            EventType::EventConversationUpdate,
            EventType::EventConversationDelete,
            EventType::EventPresence,
            EventType::EventCallSignal,
            EventType::EventReaction,
            EventType::EventPin,
            EventType::EventUnpin,
            EventType::EventMark,
            EventType::EventUnmark,
            EventType::EventCustom,
        ])
    }
}

impl EventValidationStrategy for EventTypeValidationStrategy {
    fn validate<'a>(
        &'a self,
        _context: &'a ValidationContext<'a>,
        event: &'a Event,
    ) -> Pin<Box<dyn Future<Output = Result<ValidationResult>> + Send + 'a>> {
        Box::pin(async move {
            let event_type = flare_proto::common::EventType::try_from(event.r#type);

            match event_type {
                Ok(et) if self.allowed_types.contains(&et) => Ok(ValidationResult::valid()),
                Ok(et) => Ok(ValidationResult::invalid(format!(
                    "Event type {:?} is not allowed",
                    et
                ))),
                Err(_) => Ok(ValidationResult::invalid(format!(
                    "Invalid event type: {}",
                    event.r#type
                ))),
            }
        })
    }
}

/// 事件必填字段校验策略
pub struct EventRequiredFieldsValidationStrategy;

impl EventValidationStrategy for EventRequiredFieldsValidationStrategy {
    fn validate<'a>(
        &'a self,
        _context: &'a ValidationContext<'a>,
        event: &'a Event,
    ) -> Pin<Box<dyn Future<Output = Result<ValidationResult>> + Send + 'a>> {
        Box::pin(async move {
            let mut result = ValidationResult::valid();

            // 校验 conversation_id
            if event.conversation_id.is_empty() {
                result = result.merge(ValidationResult::invalid(
                    "conversation_id is required for event",
                ));
            }

            // 校验 event_id
            if event.event_id.is_empty() {
                result = result.merge(ValidationResult::invalid("event_id is required for event"));
            }

            Ok(result)
        })
    }
}

/// `EVENT_CALL_SIGNAL` 与 **RTC / 媒体扩展编排** 对齐的基础校验（在调用 `flare-capability` 之前执行）。
///
/// - 必须带 `CallSignal` 载荷且 `from_user_id` 非空（用于 `Dispatch.user_id` 与审计）。
/// - `accept` / `reject` / `hangup` 必须已有 `call_id`（`invite` 可由服务端回填）。
pub struct EventCallSignalRtcValidationStrategy;

impl EventValidationStrategy for EventCallSignalRtcValidationStrategy {
    fn validate<'a>(
        &'a self,
        _context: &'a ValidationContext<'a>,
        event: &'a Event,
    ) -> Pin<Box<dyn Future<Output = Result<ValidationResult>> + Send + 'a>> {
        Box::pin(async move {
            if EventType::try_from(event.r#type).ok() != Some(EventType::EventCallSignal) {
                return Ok(ValidationResult::valid());
            }
            let Some(ref payload) = event.payload else {
                return Ok(ValidationResult::invalid(
                    "EVENT_CALL_SIGNAL requires payload",
                ));
            };
            let Payload::CallSignal(cs) = payload else {
                return Ok(ValidationResult::invalid(
                    "EVENT_CALL_SIGNAL payload must be CallSignal",
                ));
            };
            if cs.from_user_id.trim().is_empty() {
                return Ok(ValidationResult::invalid(
                    "call_signal.from_user_id is required for RTC / capability correlation",
                ));
            }
            if matches!(cs.signal.as_ref(), Some(Signal::Invite(_))) {
                let r = validate_call_signal_invite(&event.conversation_id, cs);
                if !r.is_valid {
                    return Ok(r);
                }
            }
            match cs.signal.as_ref() {
                Some(Signal::Accept(_) | Signal::Reject(_) | Signal::Hangup(_))
                    if cs.call_id.trim().is_empty() =>
                {
                    return Ok(ValidationResult::invalid(
                        "call_signal.call_id is required for accept/reject/hangup",
                    ));
                }
                Some(Signal::IceCandidate(ic))
                    if ic
                        .candidate_json
                        .as_ref()
                        .map(|s| s.trim().is_empty())
                        .unwrap_or(true) =>
                {
                    warn!(
                        call_id = %cs.call_id,
                        from_user_id = %cs.from_user_id,
                        "reject call_signal.ice_candidate: candidate_json is required"
                    );
                    return Ok(ValidationResult::invalid(
                        "call_signal.ice_candidate.candidate_json is required",
                    ));
                }
                _ => {}
            }
            for key in LEGACY_EXT_KEYS {
                if cs.ext.contains_key(key) {
                    warn!(
                        call_id = %cs.call_id,
                        from_user_id = %cs.from_user_id,
                        legacy_key = %key,
                        "reject call_signal.ext: unsupported legacy key"
                    );
                    return Ok(ValidationResult::invalid(format!(
                        "call_signal.ext contains unsupported legacy key: {key}"
                    )));
                }
            }
            Ok(ValidationResult::valid())
        })
    }
}

// =============================================================================
// 组合策略
// =============================================================================

/// 组合消息校验策略
pub struct CompositeMessageValidationStrategy {
    strategies: Vec<Arc<dyn MessageValidationStrategy>>,
}

impl CompositeMessageValidationStrategy {
    pub fn new(strategies: Vec<Arc<dyn MessageValidationStrategy>>) -> Self {
        Self { strategies }
    }

    /// 创建默认组合策略
    pub fn default_composite() -> Self {
        Self::new(vec![
            Arc::new(MessageSizeValidationStrategy::default()),
            Arc::new(MessageRequiredFieldsValidationStrategy),
        ])
    }
}

impl MessageValidationStrategy for CompositeMessageValidationStrategy {
    fn validate<'a>(
        &'a self,
        context: &'a ValidationContext<'a>,
        message: &'a Message,
    ) -> Pin<Box<dyn Future<Output = Result<ValidationResult>> + Send + 'a>> {
        Box::pin(async move {
            let mut result = ValidationResult::valid();

            for strategy in &self.strategies {
                let r = strategy.validate(context, message).await?;
                result = result.merge(r);

                // 短路：如果已经无效，可以提前返回
                if !result.is_valid {
                    break;
                }
            }

            Ok(result)
        })
    }
}

/// 组合事件校验策略
pub struct CompositeEventValidationStrategy {
    strategies: Vec<Arc<dyn EventValidationStrategy>>,
}

impl CompositeEventValidationStrategy {
    pub fn new(strategies: Vec<Arc<dyn EventValidationStrategy>>) -> Self {
        Self { strategies }
    }

    /// 创建默认组合策略
    pub fn default_composite() -> Self {
        Self::new(vec![
            Arc::new(EventTypeValidationStrategy::all_supported()),
            Arc::new(EventRequiredFieldsValidationStrategy),
            Arc::new(EventCallSignalRtcValidationStrategy),
        ])
    }
}

impl EventValidationStrategy for CompositeEventValidationStrategy {
    fn validate<'a>(
        &'a self,
        context: &'a ValidationContext<'a>,
        event: &'a Event,
    ) -> Pin<Box<dyn Future<Output = Result<ValidationResult>> + Send + 'a>> {
        Box::pin(async move {
            let mut result = ValidationResult::valid();

            for strategy in &self.strategies {
                let r = strategy.validate(context, event).await?;
                result = result.merge(r);

                // 短路：如果已经无效，可以提前返回
                if !result.is_valid {
                    break;
                }
            }

            Ok(result)
        })
    }
}

#[cfg(test)]
mod validate_call_signal_invite_tests {
    use flare_core::common::conversation::{
        generate_group_conversation_id, generate_single_chat_conversation_id,
    };
    use flare_proto::common::call_audience;
    use flare_proto::common::call_signal_event::Signal;
    use flare_proto::common::{
        CallAudience, CallAudienceBroadcast, CallAudienceDirect, CallAudienceExplicit, CallInvite,
        CallMediaSessionInfo, CallOfferedMedia, CallSessionKind, CallSignalEvent,
    };

    use super::validate_call_signal_invite;

    fn invite_shell() -> CallInvite {
        CallInvite {
            offered_media: Some(CallOfferedMedia {
                types: vec![1],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn single_chat_invite_direct_ok() {
        let cid = generate_single_chat_conversation_id("u1", "u2");
        let cs = CallSignalEvent {
            audience: Some(CallAudience {
                shape: Some(call_audience::Shape::Direct(CallAudienceDirect {
                    peer_user_id: "u2".into(),
                })),
            }),
            media_session: Some(CallMediaSessionInfo {
                kind: CallSessionKind::Direct as i32,
                ..Default::default()
            }),
            signal: Some(Signal::Invite(invite_shell())),
            ..Default::default()
        };
        let r = validate_call_signal_invite(&cid, &cs);
        assert!(r.is_valid, "{:?}", r.errors);
    }

    #[test]
    fn multi_party_invite_explicit_ok() {
        let cid = generate_group_conversation_id("g1");
        let cs = CallSignalEvent {
            audience: Some(CallAudience {
                shape: Some(call_audience::Shape::Explicit(CallAudienceExplicit {
                    user_ids: vec!["x".into(), "y".into()],
                })),
            }),
            media_session: Some(CallMediaSessionInfo {
                kind: CallSessionKind::Group as i32,
                ..Default::default()
            }),
            signal: Some(Signal::Invite(invite_shell())),
            ..Default::default()
        };
        let r = validate_call_signal_invite(&cid, &cs);
        assert!(r.is_valid, "{:?}", r.errors);
    }

    #[test]
    fn multi_party_invite_broadcast_ok() {
        let cid = generate_group_conversation_id("g2");
        let cs = CallSignalEvent {
            audience: Some(CallAudience {
                shape: Some(call_audience::Shape::Broadcast(CallAudienceBroadcast {})),
            }),
            media_session: Some(CallMediaSessionInfo {
                kind: CallSessionKind::Group as i32,
                ..Default::default()
            }),
            signal: Some(Signal::Invite(invite_shell())),
            ..Default::default()
        };
        let r = validate_call_signal_invite(&cid, &cs);
        assert!(r.is_valid, "{:?}", r.errors);
    }

    #[test]
    fn multi_party_invite_direct_rejected() {
        let cid = generate_group_conversation_id("g3");
        let cs = CallSignalEvent {
            audience: Some(CallAudience {
                shape: Some(call_audience::Shape::Direct(CallAudienceDirect {
                    peer_user_id: "x".into(),
                })),
            }),
            media_session: Some(CallMediaSessionInfo {
                kind: CallSessionKind::Group as i32,
                ..Default::default()
            }),
            signal: Some(Signal::Invite(invite_shell())),
            ..Default::default()
        };
        let r = validate_call_signal_invite(&cid, &cs);
        assert!(!r.is_valid);
    }

    #[test]
    fn invalid_cid_rejected() {
        let cs = CallSignalEvent {
            audience: Some(CallAudience {
                shape: Some(call_audience::Shape::Broadcast(CallAudienceBroadcast {})),
            }),
            media_session: Some(CallMediaSessionInfo {
                kind: CallSessionKind::Group as i32,
                ..Default::default()
            }),
            signal: Some(Signal::Invite(invite_shell())),
            ..Default::default()
        };
        let r = validate_call_signal_invite("not-a-cid", &cs);
        assert!(!r.is_valid);
    }
}
