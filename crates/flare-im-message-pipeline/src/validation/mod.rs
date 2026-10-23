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

use flare_im_contracts::Ctx;
use flare_proto::common::{Event, Message};

use flare_server_core::error::Result;

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
            EventType::EventConversationUpdate,
            EventType::EventConversationDelete,
            EventType::EventReaction,
            EventType::EventPin,
            EventType::EventUnpin,
            EventType::EventMark,
            EventType::EventUnmark,
            EventType::EventMessageRetentionScheduled,
            EventType::EventMessageRetentionExpired,
            EventType::EventMessageRetentionPurged,
            EventType::EventCustom,
            EventType::EventMessagePluginState,
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

/// 插件实体共享状态事件（`EVENT_MESSAGE_PLUGIN_STATE`）的载荷校验：类型与载荷一致，消息、插件、实体
/// 非空且有界，版本 `1..=i64::MAX`，`values_json` 不超过 8 KiB。内容规则（键、值形状）由发布方
/// （Capability）按插件协议校验，Writer 落库前再校验一次。
///
/// 只有 Capability 的签名发布身份能发这种事件（[`is_plugin_state_publisher`]：Capability 自己的
/// 密钥 `kid=capability` 签的 System 上下文，唯一角色 `plugin.state.publish`）：客户端上行与
/// Capability 发布走同一个 `ExecuteEvent`，不拦来源时，会话成员拿卡片上公开的消息、插件与实体 ID
/// 就能伪造计票，或用最大版本号把卡片永久冻结（之后插件的真实版本都更低，写入端单调拒绝）。
/// 未签名的 `x-actor-type: System` 元数据不算：能直连 Orchestrator 的任何内部进程都
/// 能发这个头。
///
/// [`is_plugin_state_publisher`]: flare_im_service_kit::plugin_platform::is_plugin_state_publisher
pub struct PluginStateEventValidationStrategy;

/// `PluginStateEvent.values_json` 的上限（与插件协议 `MAX_SHARED_STATE_BYTES` 相同）。
pub const MAX_PLUGIN_STATE_VALUES_BYTES: usize = 8 * 1024;

impl EventValidationStrategy for PluginStateEventValidationStrategy {
    fn validate<'a>(
        &'a self,
        context: &'a ValidationContext<'a>,
        event: &'a Event,
    ) -> Pin<Box<dyn Future<Output = Result<ValidationResult>> + Send + 'a>> {
        use flare_proto::common::{EventType, event::Payload};
        use flare_server_core::grpc::middleware::signed_context::VerifiedContext;
        Box::pin(async move {
            let is_plugin_state = event.r#type == EventType::EventMessagePluginState as i32;
            let from_system = context
                .ctx
                .get_data::<VerifiedContext>()
                .and_then(VerifiedContext::claims)
                .is_some_and(flare_im_service_kit::plugin_platform::is_plugin_state_publisher);
            let result = match (&event.payload, is_plugin_state) {
                (Some(Payload::PluginState(_)), true) if !from_system => {
                    ValidationResult::invalid("plugin state is published by the system only")
                }
                (Some(Payload::PluginState(state)), true) => {
                    let entity_ok = !state.entity.is_empty()
                        && state.entity.len() <= 128
                        && state.entity.bytes().all(|b| {
                            b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b':')
                        });
                    if state.server_msg_id.is_empty()
                        || state.plugin_id.is_empty()
                        || state.plugin_id.len() > 128
                        || !entity_ok
                        || state.version == 0
                        || state.version > i64::MAX as u64
                        || state.values_json.len() > MAX_PLUGIN_STATE_VALUES_BYTES
                    {
                        ValidationResult::invalid("invalid plugin state event")
                    } else {
                        ValidationResult::valid()
                    }
                }
                (Some(Payload::PluginState(_)), false) | (_, true) => {
                    ValidationResult::invalid("plugin state type and payload disagree")
                }
                _ => ValidationResult::valid(),
            };
            Ok(result)
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
            Arc::new(PluginStateEventValidationStrategy),
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
mod plugin_state_tests {
    use super::*;
    use flare_proto::common::{EventType, PluginStateEvent, event::Payload};

    fn state_event(state: PluginStateEvent) -> Event {
        Event {
            conversation_id: "2-group".into(),
            event_id: "e-1".into(),
            r#type: EventType::EventMessagePluginState as i32,
            payload: Some(Payload::PluginState(state)),
            ..Default::default()
        }
    }

    fn valid_state() -> PluginStateEvent {
        PluginStateEvent {
            server_msg_id: "m-1".into(),
            plugin_id: "example.poll".into(),
            entity: "poll-1".into(),
            version: 3,
            values_json: r#"{"counts":[1,2]}"#.into(),
        }
    }

    fn actor_ctx(actor_type: flare_server_core::context::ActorType) -> Ctx {
        std::sync::Arc::new(flare_server_core::Context::with_request_id("r").with_actor(
            flare_server_core::context::ActorContext::new("actor").with_type(actor_type),
        ))
    }

    /// A context verified from a token `signer` signed (the way the signed-first layer builds it).
    pub(crate) fn signed_ctx(
        seed: u8,
        kid: Option<&str>,
        identity: flare_server_core::SignedContextIdentity,
    ) -> Ctx {
        use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
        let key = ed25519_dalek::SigningKey::from_bytes(&[seed; 32]);
        let mut signer = flare_server_core::SignedContextSigner::from_private_key(
            &key.to_pkcs8_pem(Default::default()).unwrap(),
        )
        .unwrap();
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        let verifier = match kid {
            Some(kid) => {
                signer = signer.with_kid(kid).unwrap();
                let mut keys = flare_server_core::auth::KeySet::empty();
                keys.insert(kid, &public).unwrap();
                flare_server_core::SignedContextVerifier::from_key_set(keys)
            }
            None => flare_server_core::SignedContextVerifier::from_public_key(&public).unwrap(),
        };
        let token = signer.sign(&identity).unwrap();
        let mut carrier = tonic::Request::new(());
        carrier
            .metadata_mut()
            .insert("x-flare-ctx", token.parse().unwrap());
        flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata(
            carrier.metadata(),
            &verifier,
        )
        .unwrap()
    }

    /// Capability's signed shared state publisher.
    pub(crate) fn publisher_ctx() -> Ctx {
        use flare_im_service_kit::plugin_platform::{
            CARD_POST_KID, STATE_PUBLISH_ROLE, STATE_PUBLISHER_ID,
        };
        signed_ctx(
            41,
            Some(CARD_POST_KID),
            flare_server_core::SignedContextIdentity::new(
                flare_server_core::SignedActorType::System,
                STATE_PUBLISHER_ID,
                "app-a",
                "r",
            )
            .with_roles(vec![STATE_PUBLISH_ROLE.into()]),
        )
    }

    async fn check(event: &Event) -> bool {
        check_as(&publisher_ctx(), event).await
    }

    async fn check_as(ctx: &Ctx, event: &Event) -> bool {
        let context = ValidationContext {
            ctx,
            tenant_id: "app-a",
            conversation_id: "2-group",
        };
        CompositeEventValidationStrategy::default_composite()
            .validate(&context, event)
            .await
            .unwrap()
            .is_valid
    }

    #[tokio::test]
    async fn plugin_state_events_are_supported_and_bounded() {
        assert!(check(&state_event(valid_state())).await);
        let invalid = [
            PluginStateEvent {
                server_msg_id: String::new(),
                ..valid_state()
            },
            PluginStateEvent {
                plugin_id: String::new(),
                ..valid_state()
            },
            PluginStateEvent {
                entity: "has space".into(),
                ..valid_state()
            },
            PluginStateEvent {
                version: 0,
                ..valid_state()
            },
            PluginStateEvent {
                version: u64::MAX,
                ..valid_state()
            },
            PluginStateEvent {
                values_json: "x".repeat(8193),
                ..valid_state()
            },
        ];
        for state in invalid {
            assert!(!check(&state_event(state.clone())).await, "{state:?}");
        }
        // Type and payload must agree.
        let mut without_payload = state_event(valid_state());
        without_payload.payload = None;
        assert!(!check(&without_payload).await);
        let mut mislabeled = state_event(valid_state());
        mislabeled.r#type = EventType::EventReaction as i32;
        assert!(!check(&mislabeled).await);
    }

    /// A member's uplink carries a user actor (or none): it must not forge the shared state of a
    /// card it can see, nor freeze it with a maximal version.
    #[tokio::test]
    async fn plugin_state_from_anyone_but_the_system_is_rejected() {
        use flare_server_core::context::ActorType;
        let event = state_event(PluginStateEvent {
            version: i64::MAX as u64,
            ..valid_state()
        });
        assert!(check(&event).await);
        for actor_type in [
            ActorType::User,
            ActorType::Guest,
            ActorType::Service,
            ActorType::TenantAdmin,
            ActorType::Staff,
            ActorType::Unspecified,
        ] {
            assert!(
                !check_as(&actor_ctx(actor_type), &event).await,
                "{actor_type:?}"
            );
        }
        let anonymous: Ctx = std::sync::Arc::new(flare_server_core::Context::with_request_id("r"));
        assert!(!check_as(&anonymous, &event).await);
        // Unsigned System metadata is not the publisher; nor is a System actor signed by a
        // Cell key, nor a card post token signed by Capability's own key.
        assert!(
            !check_as(
                &actor_ctx(flare_server_core::context::ActorType::System),
                &event
            )
            .await
        );
        use flare_im_service_kit::plugin_platform::{
            CARD_POST_KID, CARD_POST_ROLE, STATE_PUBLISH_ROLE, STATE_PUBLISHER_ID,
        };
        let cell_system = signed_ctx(
            42,
            None,
            flare_server_core::SignedContextIdentity::new(
                flare_server_core::SignedActorType::System,
                STATE_PUBLISHER_ID,
                "app-a",
                "r",
            )
            .with_roles(vec![STATE_PUBLISH_ROLE.into()]),
        );
        assert!(!check_as(&cell_system, &event).await);
        let card_post = signed_ctx(
            43,
            Some(CARD_POST_KID),
            flare_server_core::SignedContextIdentity::new(
                flare_server_core::SignedActorType::User,
                "alice",
                "app-a",
                "r",
            )
            .with_roles(vec![CARD_POST_ROLE.into()]),
        );
        assert!(!check_as(&card_post, &event).await);
        // Other events are not gated by origin.
        let mut reaction = state_event(valid_state());
        reaction.r#type = EventType::EventReaction as i32;
        reaction.payload = None;
        assert!(check_as(&actor_ctx(ActorType::User), &reaction).await);
    }
}
