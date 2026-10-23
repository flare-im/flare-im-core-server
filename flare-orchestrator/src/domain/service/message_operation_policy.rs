//! Authorization of message operation events: edit and recall, and every other event that acts on
//! a stored message or a conversation.
//!
//! Every event reaches the Orchestrator through `EventHandler::handle_event`: the WebSocket uplink
//! (signaling gateway, route, `ExecuteEvent`), the HTTP and gRPC message actions
//! (`MessageActionService`, which builds the event and calls the same handler), custom events and
//! Capability's shared state. This policy runs there, after the event's shape is validated and
//! before it is given a conversation seq, so no entry can skip it.
//!
//! **Who operates.** A signed context (`x-flare-ctx`, verified by the service's signed-first
//! layer) decides: a `User` (or `TenantAdmin`) acts as that user; a `System` actor and the
//! services in [`TRUSTED_OPERATION_SERVICES`] act as trusted Core/Social services. A context signed
//! with Capability's own key (`kid=capability`) may only publish plugin shared state. An unsigned
//! (legacy metadata) context is at most a user: its `x-actor-type` is never trusted, since any
//! internal process can send it.
//!
//! **What each event may do.**
//! - Recall: the sender within [`RECALL_WINDOW_MS`] of sending, or a trusted service with a
//!   reason. The client's `time_limit_seconds` and `allow_admin_recall` grant nothing and are
//!   cleared.
//! - Edit: the sender within [`EDIT_WINDOW_MS`], or a trusted service; only a text, rich text or a
//!   reply whose current content is one of those is editable, and the new content must be one of
//!   those too. A plugin card (APP_CARD, or a Custom of the plugin card type, in any case or spacing)
//!   is never editable and no edit may produce one: a card's bytes are what the platform posted.
//! - Delete: a user-private soft delete only for the operator's own view; a conversation-wide soft
//!   delete by the sender (as a recall, within its window) or a trusted service; a hard delete by a
//!   trusted service only.
//! - Reaction, pin, unpin, mark, unmark, read receipt: the user the payload names must be the
//!   operator (an empty one is filled with the operator). A trusted service may act for a user.
//! - Message-targeted events (recall, edit, delete, reaction, pin, unpin, mark, unmark) are checked
//!   against the stored message: it must belong to the event's conversation.
//! - Conversation update and delete, retention transitions and message events through this entry
//!   are trusted services only. Custom events: attributes naming a user must name the operator.
//!
//! **Group roles.** Core's participant rows carry a `roles` column, but nothing keeps it in step
//! with the group roles Social owns (Social never sends role updates), so Core does not read it to
//! let an owner or administrator recall another member's message. That decision stays with Social:
//! it calls Core as its signed service identity, which this policy trusts. `RecallHook` is not
//! called on the event path for the same reason: it would make every recall wait on an external
//! hook to answer a question Core can answer itself; Social's group-admin recall comes in as a
//! trusted service instead.
//!
//! Windows are platform constants until a tenant setting exists: Core has no per-tenant recall or
//! edit window today.
use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_service_kit::plugin_platform;
use flare_proto::common::event::Payload;
use flare_proto::common::message_content::Content;
use flare_proto::common::{Event, EventType, Message, MessageContent, MessageStatus};
use flare_server_core::VerifiedContext;
use flare_server_core::auth::SignedActorType;
use flare_server_core::context::ActorType;
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};

use crate::domain::repository::MessageTargetRepository;

/// How long after sending the sender may recall a message: 24 hours.
pub const RECALL_WINDOW_MS: i64 = 24 * 3600 * 1000;
/// How long after sending the sender may edit a message: 24 hours.
pub const EDIT_WINDOW_MS: i64 = 24 * 3600 * 1000;
/// Service identities (signed `Service` actors) trusted to operate on any message of their tenant:
/// the Social IM bridge, through which Social's group administration and moderation act.
pub const TRUSTED_OPERATION_SERVICES: &[&str] = &["flare-social-bridge"];

/// A refusal reason (the `ErrorDetail.reason` the sender sees).
pub mod reason {
    pub const NOT_SENDER: &str = "MESSAGE_OPERATION_NOT_SENDER";
    pub const CONVERSATION_MISMATCH: &str = "MESSAGE_OPERATION_CONVERSATION_MISMATCH";
    pub const WINDOW_EXPIRED: &str = "MESSAGE_OPERATION_WINDOW_EXPIRED";
    pub const NOT_EDITABLE: &str = "MESSAGE_NOT_EDITABLE";
    pub const PLUGIN_CARD_NOT_EDITABLE: &str = "PLUGIN_CARD_NOT_EDITABLE";
    pub const IDENTITY_MISMATCH: &str = "MESSAGE_OPERATION_IDENTITY_MISMATCH";
    pub const FORBIDDEN: &str = "MESSAGE_OPERATION_FORBIDDEN";
    pub const REASON_REQUIRED: &str = "MESSAGE_OPERATION_REASON_REQUIRED";
}

/// Who asks for an operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operator {
    /// A user, acting for themselves.
    User(String),
    /// A trusted Core or Social service (signed `System` actor or a [`TRUSTED_OPERATION_SERVICES`]
    /// service).
    Trusted(String),
    /// Capability's plugin shared state publisher: nothing but plugin state.
    StatePublisher,
    /// No operator the policy accepts for anything but plugin state (which its own check gates).
    None,
}

impl Operator {
    /// The operator of `ctx` (see the module docs).
    pub fn of(ctx: &Ctx) -> Self {
        if let Some(claims) = ctx
            .get_data::<VerifiedContext>()
            .and_then(VerifiedContext::claims)
        {
            if plugin_platform::is_card_post_context(claims) {
                return if plugin_platform::is_plugin_state_publisher(claims) {
                    Self::StatePublisher
                } else {
                    Self::None
                };
            }
            return match claims.actor_type {
                SignedActorType::User | SignedActorType::TenantAdmin
                    if !claims.actor_id.is_empty() =>
                {
                    Self::User(claims.actor_id.clone())
                }
                SignedActorType::System if !claims.actor_id.is_empty() => {
                    Self::Trusted(claims.actor_id.clone())
                }
                SignedActorType::Service
                    if TRUSTED_OPERATION_SERVICES.contains(&claims.actor_id.as_str()) =>
                {
                    Self::Trusted(claims.actor_id.clone())
                }
                _ => Self::None,
            };
        }
        // Legacy metadata: a user at most, whatever actor type it claims.
        let user = ctx
            .user_id()
            .filter(|user| !user.trim().is_empty())
            .map(str::to_owned)
            .or_else(|| {
                ctx.actor()
                    .filter(|actor| matches!(actor.actor_type, ActorType::User))
                    .map(|actor| actor.actor_id().to_owned())
                    .filter(|user| !user.trim().is_empty())
            });
        user.map_or(Self::None, Self::User)
    }
}

fn denied(code: &str, details: &str) -> FlareError {
    ErrorBuilder::new(ErrorCode::PermissionDenied, code)
        .details(details)
        .build_error()
}

fn invalid(details: &str) -> FlareError {
    ErrorBuilder::new(ErrorCode::InvalidParameter, "MESSAGE_OPERATION_INVALID")
        .details(details)
        .build_error()
}

/// Whether `content` may be edited, or be the content of an edit: text, rich text, or a reply
/// whose current content is one of those.
fn editable(content: Option<&MessageContent>) -> bool {
    match content.and_then(|content| content.content.as_ref()) {
        Some(Content::Text(_) | Content::RichText(_)) => true,
        Some(Content::Quote(quote)) => matches!(
            quote
                .current_content
                .as_ref()
                .and_then(|c| c.content.as_ref()),
            Some(Content::Text(_) | Content::RichText(_))
        ),
        _ => false,
    }
}

/// The user field a payload names must be the operator; an empty one becomes the operator.
fn bind_user(field: &mut String, operator: &Operator) -> Result<()> {
    match operator {
        Operator::User(user) if field.is_empty() => {
            *field = user.clone();
            Ok(())
        }
        Operator::User(user) if field == user => Ok(()),
        Operator::User(_) => Err(denied(
            reason::IDENTITY_MISMATCH,
            "the event names another user than its operator",
        )),
        Operator::Trusted(_) if !field.is_empty() => Ok(()),
        Operator::Trusted(_) => Err(invalid("a trusted operation must name its user")),
        Operator::StatePublisher | Operator::None => Err(denied(
            reason::FORBIDDEN,
            "this identity may not operate on messages",
        )),
    }
}

pub struct MessageOperationPolicy {
    targets: Option<Arc<dyn MessageTargetRepository>>,
    now_ms: fn() -> i64,
}

impl Default for MessageOperationPolicy {
    fn default() -> Self {
        Self::new(None)
    }
}

impl MessageOperationPolicy {
    /// Without `targets`, events that must be checked against their message are refused
    /// (retryable), never let through unchecked.
    pub fn new(targets: Option<Arc<dyn MessageTargetRepository>>) -> Self {
        Self {
            targets,
            now_ms: || chrono::Utc::now().timestamp_millis(),
        }
    }

    /// The same policy with a fixed clock (tests).
    pub fn with_clock(mut self, now_ms: fn() -> i64) -> Self {
        self.now_ms = now_ms;
        self
    }

    /// The stored message `server_msg_id` names, which must belong to the event's conversation.
    async fn target(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        conversation_id: &str,
        server_msg_id: &str,
    ) -> Result<Message> {
        if server_msg_id.trim().is_empty() {
            return Err(invalid("the event names no message"));
        }
        let targets = self.targets.as_ref().ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::ServiceUnavailable,
                "message operation authorization unavailable",
            )
            .build_error()
        })?;
        let message = targets
            .message(ctx, tenant_id, server_msg_id)
            .await?
            // Delivered before it is stored: an operation right after sending is retried.
            .ok_or_else(|| {
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "message not stored yet")
                    .details("retry the operation shortly")
                    .build_error()
            })?;
        if message.server_id != server_msg_id {
            return Err(denied(
                reason::CONVERSATION_MISMATCH,
                "the event names no message of this conversation",
            ));
        }
        if message.conversation_id != conversation_id {
            return Err(denied(
                reason::CONVERSATION_MISMATCH,
                "the message belongs to another conversation",
            ));
        }
        Ok(message)
    }

    /// The sender of `message` acting within `window_ms` of sending it, or a trusted service.
    fn sender_within(&self, operator: &Operator, message: &Message, window_ms: i64) -> Result<()> {
        match operator {
            Operator::Trusted(_) => Ok(()),
            Operator::User(user) if *user == message.sender_id => {
                let age = (self.now_ms)().saturating_sub(message.created_at);
                if message.created_at <= 0 || age > window_ms {
                    return Err(denied(
                        reason::WINDOW_EXPIRED,
                        "the operation window of this message has passed",
                    ));
                }
                Ok(())
            }
            Operator::User(_) => Err(denied(
                reason::NOT_SENDER,
                "only the sender may do this to a message",
            )),
            Operator::StatePublisher | Operator::None => Err(denied(
                reason::FORBIDDEN,
                "this identity may not operate on messages",
            )),
        }
    }

    /// Authorize `event` from `ctx` (see the module docs) and return it normalized: user fields
    /// bound to the operator, client-claimed recall permissions cleared.
    pub async fn authorize(&self, ctx: &Ctx, tenant_id: &str, mut event: Event) -> Result<Event> {
        let operator = Operator::of(ctx);
        let event_type = EventType::try_from(event.r#type).unwrap_or(EventType::Unspecified);
        // Shared state has its own gate (only the signed publisher); nothing else may use the
        // publisher's context, and the publisher may do nothing else.
        if event_type == EventType::EventMessagePluginState {
            return Ok(event);
        }
        if matches!(operator, Operator::StatePublisher | Operator::None) {
            return Err(denied(
                reason::FORBIDDEN,
                "this identity may not send message events",
            ));
        }
        let trusted = matches!(operator, Operator::Trusted(_));
        let conversation_id = event.conversation_id.clone();
        match event.payload.as_mut() {
            Some(Payload::Recall(recall)) => {
                let message = self
                    .target(ctx, tenant_id, &conversation_id, &recall.server_msg_id)
                    .await?;
                self.sender_within(&operator, &message, RECALL_WINDOW_MS)?;
                if trusted && recall.reason.trim().is_empty() {
                    return Err(denied(
                        reason::REASON_REQUIRED,
                        "a service recalling a message must say why",
                    ));
                }
                recall.time_limit_seconds = None;
                recall.allow_admin_recall = None;
            }
            Some(Payload::Edit(edit)) => {
                if plugin_platform::claims_plugin_card(edit.new_content.as_ref()) {
                    return Err(denied(
                        reason::PLUGIN_CARD_NOT_EDITABLE,
                        "an edit may not produce a plugin card",
                    ));
                }
                if !editable(edit.new_content.as_ref()) {
                    return Err(denied(
                        reason::NOT_EDITABLE,
                        "only text and rich text can be the content of an edit",
                    ));
                }
                let message = self
                    .target(ctx, tenant_id, &conversation_id, &edit.server_msg_id)
                    .await?;
                if plugin_platform::message_claims_plugin_card(&message) {
                    return Err(denied(
                        reason::PLUGIN_CARD_NOT_EDITABLE,
                        "a plugin card is never edited",
                    ));
                }
                if !editable(message.content.as_ref())
                    || message.status == MessageStatus::Recalled as i32
                    || message.status == MessageStatus::Deleted as i32
                {
                    return Err(denied(
                        reason::NOT_EDITABLE,
                        "only text and rich text messages can be edited",
                    ));
                }
                self.sender_within(&operator, &message, EDIT_WINDOW_MS)?;
            }
            Some(Payload::Delete(delete)) => {
                let hard = delete.delete_type == Some(flare_proto::common::DeleteType::Hard as i32);
                let global = match delete.scope {
                    Some(scope) => {
                        scope == flare_proto::common::DeleteScope::ConversationGlobal as i32
                    }
                    None => hard,
                };
                if hard && !trusted {
                    return Err(denied(
                        reason::FORBIDDEN,
                        "a hard delete is a trusted service operation",
                    ));
                }
                let message = self
                    .target(ctx, tenant_id, &conversation_id, &delete.server_msg_id)
                    .await?;
                if global {
                    self.sender_within(&operator, &message, RECALL_WINDOW_MS)?;
                } else {
                    let mut target = delete.target_user_id.clone().unwrap_or_default();
                    bind_user(&mut target, &operator)?;
                    delete.target_user_id = Some(target);
                }
            }
            Some(Payload::Reaction(reaction)) => {
                bind_user(&mut reaction.user_id, &operator)?;
                self.target(ctx, tenant_id, &conversation_id, &reaction.server_msg_id)
                    .await?;
            }
            Some(Payload::Pin(pin)) => {
                bind_user(&mut pin.pinned_by, &operator)?;
                self.target(ctx, tenant_id, &conversation_id, &pin.server_msg_id)
                    .await?;
            }
            Some(Payload::Unpin(unpin)) => {
                bind_user(&mut unpin.unpinned_by, &operator)?;
                self.target(ctx, tenant_id, &conversation_id, &unpin.server_msg_id)
                    .await?;
            }
            Some(Payload::Mark(mark)) => {
                bind_user(&mut mark.user_id, &operator)?;
                self.target(ctx, tenant_id, &conversation_id, &mark.server_msg_id)
                    .await?;
            }
            Some(Payload::Unmark(unmark)) => {
                bind_user(&mut unmark.user_id, &operator)?;
                self.target(ctx, tenant_id, &conversation_id, &unmark.server_msg_id)
                    .await?;
            }
            Some(Payload::Read(read)) => {
                bind_user(&mut read.user_id, &operator)?;
                if !read.conversation_id.is_empty() && read.conversation_id != conversation_id {
                    return Err(denied(
                        reason::CONVERSATION_MISMATCH,
                        "the receipt names another conversation",
                    ));
                }
            }
            Some(Payload::Custom(custom)) => {
                if let Operator::User(user) = &operator {
                    for key in [
                        "user_id",
                        "userId",
                        "operator_id",
                        "operatorId",
                        "x-user-id",
                    ] {
                        if custom
                            .attributes
                            .get(key)
                            .is_some_and(|named| named != user)
                        {
                            return Err(denied(
                                reason::IDENTITY_MISMATCH,
                                "the event names another user than its operator",
                            ));
                        }
                    }
                }
            }
            Some(
                Payload::Message(_)
                | Payload::Conversation(_)
                | Payload::ConversationDelete(_)
                | Payload::RetentionScheduled(_)
                | Payload::RetentionExpired(_)
                | Payload::RetentionPurged(_),
            ) if !trusted => {
                return Err(denied(
                    reason::FORBIDDEN,
                    "only a trusted service sends this event",
                ));
            }
            Some(_) => {}
            None => return Err(invalid("the event carries no payload")),
        }
        Ok(event)
    }
}

#[cfg(test)]
#[path = "message_operation_policy_tests.rs"]
mod tests;
