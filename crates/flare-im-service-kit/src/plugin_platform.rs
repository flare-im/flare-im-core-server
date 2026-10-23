//! Plugin platform identities and error codes shared by Capability, Ingest and the API gateway
//! (plugin platform v2, `docs/design/plugin-platform-v2-trust-lifecycle-admin.md` §1 and §4).
//!
//! **Platform card path.** A plugin card is a top-level APP_CARD [`PLUGIN_CARD_TYPE`]. Only Core
//! Capability posts one, after an operation the ticket's user invoked returned it: Capability
//! signs the internal context with its own key ([`CARD_POST_KID`], which a legacy Cell key never
//! resolves) as that user (`actor_type = User`, `actor_id` = the ticket's user) with the single
//! role [`CARD_POST_ROLE`], and sets the request id to [`card_post_request_id`] of the exact post:
//! tenant, sender, conversation, client message id and card. Ingest recomputes that digest over
//! the message it received ([`verify_card_post`]), so the signed context authorizes this card in
//! this conversation as this sender and nothing else; no field of the message body is trusted.
//! Every other caller sending a top-level plugin card is refused with
//! [`PLUGIN_CARD_PLATFORM_ONLY`]. A forward carrying a card among its items is not a top-level
//! card and is not affected: clients render only a top-level card as a plugin card, and Storage
//! Writer binds only a top-level card to its entity, so a card inside a forward item, a thread's
//! `root_content` or a quote never becomes interactive.
//!
//! **What counts as a card on refusal.** The core SDK renders a wire `Content::Custom` whose type
//! is the plugin card type as a plugin card as well, so a refusal ([`claims_plugin_card`]) covers
//! both an APP_CARD and a Custom of that type, ignoring surrounding white space and ASCII case.
//! The platform's own post ([`verify_card_post`]) is held to the exact type.
use flare_proto::common::message_content::Content;
use flare_proto::common::{AppCardContent, Message, MessageContent, MessageSource, MessageType};
use flare_server_core::{SignedActorType, auth::SignedContextClaims};
use sha2::{Digest, Sha256};

/// APP_CARD `card_type` of a plugin card.
pub const PLUGIN_CARD_TYPE: &str = "flare.plugin.message.v1";
/// APP_CARD `payload_schema` of a plugin card; the payload is the plugin message envelope.
pub const PLUGIN_CARD_PAYLOAD_SCHEMA: &str = "flare.plugin.message/v1";
/// The only role of a card post context.
pub const CARD_POST_ROLE: &str = "plugin.card.post";
/// Key id of Capability's context key.
pub const CARD_POST_KID: &str = flare_server_core::auth::kid::CAPABILITY;
/// Prefix of the client message id of every platform card post. The namespace is reserved: Ingest
/// refuses it from any other sender ([`is_reserved_client_msg_id`]), so nobody can occupy the id
/// of a card before the platform posts it (Ingest and Storage Writer both deduplicate by
/// sender, conversation and client message id).
pub const CARD_CLIENT_MSG_ID_PREFIX: &str = "pcard-";
/// Prefix of [`card_post_request_id`] (`pcard2`: the digest covers the routing fields too).
const CARD_POST_REQUEST_PREFIX: &str = "pcard2-";

/// A user (or any caller but Capability's card post) sent a top-level plugin card.
pub const PLUGIN_CARD_PLATFORM_ONLY: &str = "PLUGIN_CARD_PLATFORM_ONLY";
/// The plugin is not installed for the tenant (or was uninstalled or revoked).
pub const PLUGIN_NOT_INSTALLED: &str = "PLUGIN_NOT_INSTALLED";
/// The plugin is installed but disabled, or its projection lapsed.
pub const PLUGIN_DISABLED: &str = "PLUGIN_DISABLED";
/// The caller may not invoke this operation (an admin or lifecycle operation, or one the
/// installation did not approve).
pub const PLUGIN_PERMISSION_DENIED: &str = "PLUGIN_PERMISSION_DENIED";
/// The plugin does not support the caller's client platform.
pub const PLUGIN_UNSUPPORTED_PLATFORM: &str = "PLUGIN_UNSUPPORTED_PLATFORM";
/// A card action or detail names a message that is not the card bound to its entity (a forged or
/// forwarded copy).
pub const PLUGIN_CARD_UNBOUND: &str = "PLUGIN_CARD_UNBOUND";
/// The plugin created its entity but the platform could not post the card; the plugin was told
/// (`entityLifecycle.unbound`, reason `post_failed`).
pub const PLUGIN_CARD_NOT_POSTED: &str = "PLUGIN_CARD_NOT_POSTED";
/// The call names the entity of a recalled card.
pub const PLUGIN_MESSAGE_RECALLED: &str = "PLUGIN_MESSAGE_RECALLED";
/// The outcome of a command is unknown (timeout, transport failure): retry with the same request
/// or command id.
pub const CAPABILITY_OUTCOME_UNKNOWN: &str = "CAPABILITY_OUTCOME_UNKNOWN";
/// A plugin rejection without a well-formed code of its own.
pub const PLUGIN_REJECTED: &str = "PLUGIN_REJECTED";

/// The platform reasons a plugin call is refused before or around the plugin, with the HTTP status
/// the API gateway answers. A plugin's own rejection code is not listed: it passes through as 422.
pub const PLATFORM_DENIAL_CODES: &[(&str, u16)] = &[
    (PLUGIN_NOT_INSTALLED, 404),
    (PLUGIN_DISABLED, 403),
    (PLUGIN_PERMISSION_DENIED, 403),
    (PLUGIN_UNSUPPORTED_PLATFORM, 403),
    (PLUGIN_CARD_UNBOUND, 409),
    (PLUGIN_MESSAGE_RECALLED, 409),
    (PLUGIN_CARD_NOT_POSTED, 502),
];

/// Whether `code` is a well-formed plugin error code: `[A-Z0-9_]{1,64}`.
pub fn is_error_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 64
        && code
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
}

/// The top-level plugin card of `message`: its content is an APP_CARD of [`PLUGIN_CARD_TYPE`],
/// whatever its app id or payload.
pub fn top_level_plugin_card(message: &Message) -> Option<&AppCardContent> {
    match message.content.as_ref()?.content.as_ref()? {
        Content::AppCard(card) if card.card_type == PLUGIN_CARD_TYPE => Some(card),
        _ => None,
    }
}

/// Whether `value` names the plugin card type once surrounding white space and ASCII case are
/// ignored. Used to refuse look-alikes, never to accept a card.
pub fn is_plugin_card_type(value: &str) -> bool {
    value.trim().eq_ignore_ascii_case(PLUGIN_CARD_TYPE)
}

/// Whether `content` presents itself as a plugin card at the top level: an APP_CARD whose
/// `card_type`, or a Custom whose `type`, is the plugin card type up to white space and ASCII
/// case (see the module docs). Only the platform may send or produce such content; Ingest refuses
/// it from anyone else, and a message edit may neither target nor produce it.
pub fn claims_plugin_card(content: Option<&MessageContent>) -> bool {
    match content.and_then(|content| content.content.as_ref()) {
        Some(Content::AppCard(card)) => is_plugin_card_type(&card.card_type),
        Some(Content::Custom(custom)) => is_plugin_card_type(&custom.r#type),
        _ => false,
    }
}

/// Whether `client_msg_id` lies in the platform card namespace ([`CARD_CLIENT_MSG_ID_PREFIX`],
/// ignoring surrounding white space and ASCII case).
pub fn is_reserved_client_msg_id(client_msg_id: &str) -> bool {
    client_msg_id
        .trim()
        .get(..CARD_CLIENT_MSG_ID_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(CARD_CLIENT_MSG_ID_PREFIX))
}

/// [`claims_plugin_card`] of a message's content.
pub fn message_claims_plugin_card(message: &Message) -> bool {
    claims_plugin_card(message.content.as_ref())
}

/// The only role of the context Capability publishes plugin shared state with
/// (`EVENT_MESSAGE_PLUGIN_STATE` through Orchestrator `ExecuteEvent`).
pub const STATE_PUBLISH_ROLE: &str = "plugin.state.publish";
/// Actor id of that context.
pub const STATE_PUBLISHER_ID: &str = "flare-capability";

/// Whether the context is Capability's plugin shared state publisher: signed with Capability's own
/// key ([`CARD_POST_KID`], never resolved by a legacy Cell key), a `System` actor named
/// [`STATE_PUBLISHER_ID`] with the single role [`STATE_PUBLISH_ROLE`]. No unsigned metadata (an
/// `x-actor-type: System` header any internal process can send) and no other component key can
/// claim it.
pub fn is_plugin_state_publisher(claims: &SignedContextClaims) -> bool {
    claims.kid == CARD_POST_KID
        && claims.actor_type == SignedActorType::System
        && claims.actor_id == STATE_PUBLISHER_ID
        && claims.roles == [STATE_PUBLISH_ROLE]
        && claims.credential_id.is_none()
}

/// Whether the context was signed with Capability's card post key. Such a context authorizes
/// nothing but [`verify_card_post`].
pub fn is_card_post_context(claims: &SignedContextClaims) -> bool {
    claims.kid == CARD_POST_KID
}

fn field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

/// The request id binding a card post context to one post: `pcard2-` and the hex SHA-256 over the
/// length-prefixed tenant, sender, conversation, client message id, conversation type, channel and
/// the card's app id, type, payload schema, title, summary and payload. `None` when `message` is
/// not a top-level plugin card. The conversation type and channel route the message (the channel
/// of a single chat is its peer), so they are signed with it.
pub fn card_post_request_id(tenant_id: &str, message: &Message) -> Option<String> {
    let card = top_level_plugin_card(message)?;
    let mut hasher = Sha256::new();
    let conversation_type = message.conversation_type.to_string();
    for value in [
        tenant_id,
        &message.sender_id,
        &message.conversation_id,
        &message.client_msg_id,
        &conversation_type,
        &message.channel_id,
        &card.app_id,
        &card.card_type,
        &card.payload_schema,
        &card.title,
        &card.summary,
    ] {
        field(&mut hasher, value.as_bytes());
    }
    field(&mut hasher, &card.payload);
    Some(format!(
        "{CARD_POST_REQUEST_PREFIX}{}",
        hex::encode(hasher.finalize())
    ))
}

/// Ingest's check of a card post context (see the module docs): the exact identity Capability
/// signs, the card it posts and the request id binding both. Every field of the message outside
/// the digest that could change how it is stored, shown, routed or pushed (thread, sender name and
/// avatar, retention, offline push text, extensions, attributes, preset ids, sequence and status)
/// must be empty: Capability sets none of them, so a holder of the token cannot add one.
pub fn verify_card_post(
    claims: &SignedContextClaims,
    message: &Message,
) -> std::result::Result<(), &'static str> {
    if claims.kid != CARD_POST_KID
        || claims.actor_type != SignedActorType::User
        || claims.roles != [CARD_POST_ROLE]
        || claims.credential_id.is_some()
    {
        return Err("not a platform card post identity");
    }
    let card = top_level_plugin_card(message).ok_or("a card post carries a plugin card only")?;
    if message.sender_id != claims.actor_id
        || message.source != MessageSource::User as i32
        || message.message_type != MessageType::AppCard as i32
        || !message.attributes.is_empty()
        || !message.client_msg_id.starts_with(CARD_CLIENT_MSG_ID_PREFIX)
        || message.conversation_id.is_empty()
        || card.app_id.is_empty()
        || card.payload_schema != PLUGIN_CARD_PAYLOAD_SCHEMA
        || !card.subtitle.is_empty()
        || !card.thumbnail_url.is_empty()
        || !card.actions.is_empty()
        || !card.attributes.is_empty()
    {
        return Err("card post does not match the platform card shape");
    }
    if !message.server_id.is_empty()
        || message.conversation_seq != 0
        || message.message_seq.is_some()
        || message.created_at != 0
        || message.status != 0
        || !message.sender_name.is_empty()
        || !message.sender_avatar.is_empty()
        || message.thread_id.is_some()
        || message.retention_policy.is_some()
        || message.retention_state.is_some()
        || message.offline_push_info.is_some()
        || !message.extensions.is_empty()
    {
        return Err("card post sets a field the platform never sets");
    }
    if card_post_request_id(&claims.tenant_id, message).as_deref() != Some(&claims.request_id) {
        return Err("card post is not the one the context was signed for");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::MessageContent;

    fn card() -> AppCardContent {
        AppCardContent {
            app_id: "example.poll".into(),
            card_type: PLUGIN_CARD_TYPE.into(),
            payload_schema: PLUGIN_CARD_PAYLOAD_SCHEMA.into(),
            payload: br#"{"pluginId":"example.poll"}"#.to_vec(),
            ..Default::default()
        }
    }

    fn message(card: AppCardContent) -> Message {
        Message {
            sender_id: "alice".into(),
            conversation_id: "2-group".into(),
            client_msg_id: "pcard-1".into(),
            source: MessageSource::User as i32,
            message_type: MessageType::AppCard as i32,
            content: Some(MessageContent {
                content: Some(Content::AppCard(card)),
            }),
            ..Default::default()
        }
    }

    fn claims(message: &Message) -> SignedContextClaims {
        SignedContextClaims {
            sub: "alice".into(),
            iss: "flare-internal".into(),
            aud: "flare-internal".into(),
            token_use: "internal_context".into(),
            iat: 1,
            exp: 2,
            jti: "j".into(),
            actor_type: SignedActorType::User,
            actor_id: "alice".into(),
            tenant_id: "app-a".into(),
            roles: vec![CARD_POST_ROLE.into()],
            credential_id: None,
            request_id: card_post_request_id("app-a", message).unwrap(),
            org_id: None,
            permission: None,
            perm_version: None,
            trace_id: None,
            kid: CARD_POST_KID.into(),
        }
    }

    #[test]
    fn the_signed_post_authorizes_exactly_its_card() {
        let posted = message(card());
        let signed = claims(&posted);
        assert_eq!(verify_card_post(&signed, &posted), Ok(()));
        assert!(signed.request_id.len() <= 128);

        // Another sender, conversation, client message id or card under the same context.
        let mut other = posted.clone();
        other.sender_id = "bob".into();
        assert!(verify_card_post(&signed, &other).is_err());
        let mut other = posted.clone();
        other.conversation_id = "2-other".into();
        assert!(verify_card_post(&signed, &other).is_err());
        let mut other = posted.clone();
        other.client_msg_id = "pcard-2".into();
        assert!(verify_card_post(&signed, &other).is_err());
        // Outside the reserved namespace even a correctly signed post is refused.
        let mut outside = posted.clone();
        outside.client_msg_id = "cm-1".into();
        let outside_claims = claims(&outside);
        assert!(verify_card_post(&outside_claims, &outside).is_err());
        let mut forged = card();
        forged.payload = br#"{"pluginId":"example.poll","x":1}"#.to_vec();
        assert!(verify_card_post(&signed, &message(forged)).is_err());
        let mut forged = card();
        forged.app_id = "example.other".into();
        assert!(verify_card_post(&signed, &message(forged)).is_err());
        // Body-supplied attributes are no authority either.
        let mut other = posted.clone();
        other.attributes.insert("x-user-id".into(), "alice".into());
        assert!(verify_card_post(&signed, &other).is_err());
    }

    #[test]
    fn only_capabilitys_user_identity_with_the_post_role_is_a_card_post() {
        let posted = message(card());
        let base = claims(&posted);
        for kid in ["legacy", "gateway", "hook", "api-gateway"] {
            let mut c = base.clone();
            c.kid = kid.into();
            assert!(!is_card_post_context(&c));
            assert!(verify_card_post(&c, &posted).is_err(), "{kid}");
        }
        let mut c = base.clone();
        c.actor_type = SignedActorType::Service;
        assert!(verify_card_post(&c, &posted).is_err());
        let mut c = base.clone();
        c.roles = vec![];
        assert!(verify_card_post(&c, &posted).is_err());
        let mut c = base.clone();
        c.roles.push("admin".into());
        assert!(verify_card_post(&c, &posted).is_err());
        let mut c = base;
        c.credential_id = Some("cred".into());
        assert!(verify_card_post(&c, &posted).is_err());
    }

    #[test]
    fn capability_may_post_nothing_but_a_plugin_card() {
        let posted = message(card());
        let signed = claims(&posted);
        let mut text = posted.clone();
        text.content = Some(MessageContent {
            content: Some(Content::Text(Default::default())),
        });
        assert!(verify_card_post(&signed, &text).is_err());
        let mut other_card = card();
        other_card.card_type = "vote".into();
        assert!(verify_card_post(&signed, &message(other_card)).is_err());
        let mut system = posted;
        system.source = MessageSource::System as i32;
        assert!(verify_card_post(&signed, &system).is_err());
    }

    #[test]
    fn a_forward_is_not_a_top_level_card() {
        use flare_proto::common::{ForwardContent, ForwardItem};
        let mut forward = message(card());
        forward.content = Some(MessageContent {
            content: Some(Content::Forward(ForwardContent {
                items: vec![ForwardItem {
                    content: Some(MessageContent {
                        content: Some(Content::AppCard(card())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            })),
        });
        assert!(top_level_plugin_card(&forward).is_none());
        assert!(top_level_plugin_card(&message(card())).is_some());
    }

    /// A wire Custom of the plugin card type renders as a card in the core SDK, so it is
    /// refused like an APP_CARD, and so are case and white-space variants of either. Other cards and
    /// cards nested in a forward, thread or quote are not top-level cards.
    #[test]
    fn custom_and_variant_plugin_cards_are_claimed_cards() {
        use flare_proto::common::{CustomContent, QuoteContent, ThreadContent};
        let custom = |kind: &str| MessageContent {
            content: Some(Content::Custom(CustomContent {
                r#type: kind.into(),
                payload: br#"{"pluginId":"example.poll"}"#.to_vec(),
                ..Default::default()
            })),
        };
        let app_card = |kind: &str| MessageContent {
            content: Some(Content::AppCard(AppCardContent {
                card_type: kind.into(),
                ..card()
            })),
        };
        for kind in [
            PLUGIN_CARD_TYPE,
            "FLARE.PLUGIN.MESSAGE.V1",
            " flare.plugin.message.v1",
            "flare.plugin.message.v1\n",
            "\tFlare.Plugin.Message.V1 ",
        ] {
            assert!(claims_plugin_card(Some(&custom(kind))), "custom {kind:?}");
            assert!(
                claims_plugin_card(Some(&app_card(kind))),
                "app card {kind:?}"
            );
        }
        for kind in [
            "red_packet",
            "flare.plugin.message.v2",
            "flare.plugin.message",
            "",
        ] {
            assert!(!claims_plugin_card(Some(&custom(kind))), "custom {kind:?}");
            assert!(
                !claims_plugin_card(Some(&app_card(kind))),
                "app card {kind:?}"
            );
        }
        assert!(!claims_plugin_card(None));
        let thread = MessageContent {
            content: Some(Content::Thread(Box::new(ThreadContent {
                root_content: Some(Box::new(custom(PLUGIN_CARD_TYPE))),
                ..Default::default()
            }))),
        };
        let quote = MessageContent {
            content: Some(Content::Quote(Box::new(QuoteContent {
                current_content: Some(Box::new(app_card(PLUGIN_CARD_TYPE))),
                ..Default::default()
            }))),
        };
        assert!(!claims_plugin_card(Some(&thread)));
        assert!(!claims_plugin_card(Some(&quote)));
        // The platform's own post stays exact: a variant is not a platform card.
        let mut variant = card();
        variant.card_type = "FLARE.PLUGIN.MESSAGE.V1".into();
        assert!(top_level_plugin_card(&message(variant)).is_none());
    }

    /// Routing fields are signed with the card; every other field Capability never sets
    /// must stay empty, so the holder of a token cannot reroute, restyle or time-bomb the card.
    #[test]
    fn the_post_signs_its_route_and_refuses_unsigned_extras() {
        let mut posted = message(card());
        posted.conversation_type = 2;
        posted.channel_id = "group-a".into();
        let signed = claims(&posted);
        assert_eq!(verify_card_post(&signed, &posted), Ok(()));
        let mut other = posted.clone();
        other.channel_id = "group-b".into();
        assert!(verify_card_post(&signed, &other).is_err());
        let mut other = posted.clone();
        other.conversation_type = 1;
        assert!(verify_card_post(&signed, &other).is_err());
        let extras: Vec<fn(&mut Message)> = vec![
            |m| m.server_id = "preset".into(),
            |m| m.conversation_seq = 7,
            |m| m.message_seq = Some(7),
            |m| m.created_at = 1,
            |m| m.status = 2,
            |m| m.sender_name = "Admin".into(),
            |m| m.sender_avatar = "https://example.invalid/a.png".into(),
            |m| m.thread_id = Some("root".into()),
            |m| m.retention_policy = Some(Default::default()),
            |m| m.retention_state = Some(Default::default()),
            |m| m.offline_push_info = Some(Default::default()),
            |m| {
                m.extensions.insert("biz.x".into(), b"1".to_vec());
            },
        ];
        for (index, extra) in extras.into_iter().enumerate() {
            let mut other = posted.clone();
            extra(&mut other);
            assert!(verify_card_post(&signed, &other).is_err(), "extra #{index}");
        }
        assert!(card_post_request_id("app-a", &posted).is_some());
        let mut text = posted;
        text.content = Some(MessageContent {
            content: Some(Content::Text(Default::default())),
        });
        assert!(card_post_request_id("app-a", &text).is_none());
    }

    #[test]
    fn only_capabilitys_signed_system_context_publishes_plugin_state() {
        let mut publisher = claims(&message(card()));
        publisher.actor_type = SignedActorType::System;
        publisher.actor_id = STATE_PUBLISHER_ID.into();
        publisher.roles = vec![STATE_PUBLISH_ROLE.into()];
        assert!(is_plugin_state_publisher(&publisher));
        // A card post token (a user under the same key) is not a publisher, nor is the publisher a
        // card post.
        assert!(!is_plugin_state_publisher(&claims(&message(card()))));
        assert!(verify_card_post(&publisher, &message(card())).is_err());
        for kid in ["legacy", "gateway", "hook", "bridge"] {
            let mut other = publisher.clone();
            other.kid = kid.into();
            assert!(!is_plugin_state_publisher(&other), "{kid}");
        }
        let mut other = publisher.clone();
        other.actor_type = SignedActorType::Service;
        assert!(!is_plugin_state_publisher(&other));
        let mut other = publisher.clone();
        other.actor_id = "flare-social-bridge".into();
        assert!(!is_plugin_state_publisher(&other));
        let mut other = publisher.clone();
        other.roles.push("admin".into());
        assert!(!is_plugin_state_publisher(&other));
    }

    #[test]
    fn the_card_client_id_namespace_is_reserved() {
        for reserved in ["pcard-0123", "PCARD-x", " pcard-", "Pcard-abc"] {
            assert!(is_reserved_client_msg_id(reserved), "{reserved:?}");
        }
        for free in ["", "pcard", "cm-pcard-1", "pcar-d", "p\u{e9}card-"] {
            assert!(!is_reserved_client_msg_id(free), "{free:?}");
        }
    }

    #[test]
    fn error_codes_are_well_formed() {
        for (code, status) in PLATFORM_DENIAL_CODES {
            assert!(is_error_code(code), "{code}");
            assert!((400..600).contains(status));
        }
        assert!(is_error_code(PLUGIN_CARD_PLATFORM_ONLY));
        assert!(!is_error_code("poll_closed"));
        assert!(!is_error_code(&"A".repeat(65)));
    }
}
