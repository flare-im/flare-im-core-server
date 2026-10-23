//! Platform card posting (plugin platform v2 §1): the card a plugin operation returns in
//! `data.message` is posted by Core, never by the client.
//!
//! [`PluginCardPoster::post_result`] takes the successful result of a user call and, when it
//! carries `data.message`:
//!
//! 1. checks the envelope: a valid [`PluginMessage`] of the invoked plugin whose contribution is a
//!    declared message-content of the same schema version, with a valid entity at its
//!    `entityPath` when it declares one, within the protocol's size limit;
//! 2. writes the plugin's name (`displayName`, at most 64 bytes) into the envelope;
//! 3. reads the route of the ticket's conversation as the ticket's user and builds the APP_CARD
//!    `flare.plugin.message.v1` with the user as sender and a client message id derived from
//!    (tenant, plugin, user, conversation, `commandId` or the request id), so a replayed call posts
//!    the same message and Ingest deduplicates it;
//! 4. sends it through Ingest with Capability's card post context, retrying availability failures
//!    a bounded number of times with the same client message id;
//! 5. binds the entity to the posted card right away (Storage Writer binds it again when it stores
//!    the card, idempotently), so actions on it and its shared state work at once;
//! 6. answers the data without `message` and with `posted: {conversationId, serverMsgId,
//!    clientMsgId}`.
//!
//! An entity that already has a card is never posted again and never rolled back: a replayed call
//! (the plugin answers the same `data.message` for the same command) gets the bound card as its
//! `posted`, and a recalled one `PLUGIN_MESSAGE_RECALLED`. Ingest keeps card posts in their own
//! idempotency namespace, keyed by the signed post (sender, conversation, client message id and
//! card digest), so a client message id a user sent first can never stand in for the card, and a
//! replayed post only ever returns the card it posted.
//!
//! When the card cannot be posted for good — Ingest or the conversation refused it, or posting is
//! not configured — the plugin is told through the lifecycle outbox (`entityLifecycle.unbound`,
//! reason `post_failed`) and the call answers `PLUGIN_CARD_NOT_POSTED`. A replay of a call whose
//! card was given up posts nothing. When Ingest's answer leaves it open whether the card was stored
//! (unavailable on every attempt, a first send still in flight), nothing is rolled back yet: the
//! call answers an open outcome (`CAPABILITY_OUTCOME_UNKNOWN`, retry with the same command, which
//! Ingest answers with the stored card if it landed), and an unconfirmed `post_failed` is queued
//! for after [`POST_VERIFY_DELAY`]; the worker delivers it only if the entity is still unbound by
//! then (Storage Writer binds a card when it stores it).
use std::sync::Arc;
use std::time::Duration;

use flare_im_capability_core::plugin::PluginManifest;
use flare_im_capability_core::plugin::manifest::{
    UNBOUND_REASON_POST_FAILED, UiContribution, UiContributionKind,
};
use flare_im_capability_core::plugin::ui::{MAX_PLUGIN_DISPLAY_NAME_BYTES, PluginMessage};
use flare_im_service_kit::plugin_platform::{
    PLUGIN_CARD_NOT_POSTED, PLUGIN_CARD_PAYLOAD_SCHEMA, PLUGIN_CARD_TYPE, PLUGIN_MESSAGE_RECALLED,
};
use flare_proto::common::message_content::Content;
use flare_proto::common::{AppCardContent, Message, MessageContent, MessageSource, MessageType};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::domain::capability::{CapabilityError, Result};
use crate::domain::repository::entity_lifecycle::EntityLifecycleOutbox;
use crate::domain::repository::plugin_card::{CardPostError, PluginCardDelivery};
use crate::domain::repository::plugin_state::PluginEntityBindingStore;

/// Sends of one card before it is given up.
pub const MAX_POST_ATTEMPTS: u32 = 3;
/// How long an unconfirmed `post_failed` waits before the worker checks whether the card was
/// stored after all: past Ingest's in-flight reservation (180 s) and the WAL replay of a send
/// Ingest accepted, so a card Ingest stored is bound by then.
pub const POST_VERIFY_DELAY: Duration = Duration::from_secs(600);
/// Pause before the second and third send.
const POST_BACKOFF: [Duration; 2] = [Duration::from_millis(200), Duration::from_millis(600)];

/// The call a result came from: the signed identity and the plugin it reached.
pub struct CardPostCall<'a> {
    pub tenant_id: &'a str,
    pub user_id: &'a str,
    /// The ticket's conversation (the dispatch's), where the card goes.
    pub conversation_id: Option<&'a str>,
    pub request_id: &'a str,
    /// The call input; its `commandId` keys the client message id when present.
    pub input: &'a Value,
    pub manifest: &'a PluginManifest,
}

pub struct PluginCardPoster {
    /// `None` when the deployment has no Ingest route or card post key: every card is given up.
    delivery: Option<Arc<dyn PluginCardDelivery>>,
    bindings: Option<Arc<dyn PluginEntityBindingStore>>,
    lifecycle: Option<Arc<dyn EntityLifecycleOutbox>>,
    backoff: [Duration; 2],
}

/// The client message id of the card a call posts: `pcard-` and 32 hex digits of SHA-256 over the
/// tenant, plugin, user, conversation and the call's command key (`command:<commandId>` when the
/// input names a valid one, otherwise `request:<requestId>`, the plugin's own idempotency key).
pub fn card_client_msg_id(
    tenant_id: &str,
    plugin_id: &str,
    user_id: &str,
    conversation_id: &str,
    request_id: &str,
    input: &Value,
) -> String {
    let command = input
        .get("commandId")
        .and_then(Value::as_str)
        .filter(|id| {
            !id.is_empty()
                && id.len() <= 128
                && id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b':'))
        })
        .map_or_else(
            || format!("request:{request_id}"),
            |id| format!("command:{id}"),
        );
    let mut hasher = Sha256::new();
    for part in [
        tenant_id,
        plugin_id,
        user_id,
        conversation_id,
        command.as_str(),
    ] {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part.as_bytes());
    }
    let digest = hex::encode(hasher.finalize());
    format!(
        "{}{}",
        flare_im_service_kit::plugin_platform::CARD_CLIENT_MSG_ID_PREFIX,
        &digest[..32]
    )
}

/// The plugin's name for the envelope: trimmed, without control characters, cut at a character
/// boundary to [`MAX_PLUGIN_DISPLAY_NAME_BYTES`]. `None` when nothing is left.
pub fn envelope_display_name(display_name: &str) -> Option<String> {
    let mut name = String::new();
    for c in display_name.trim().chars().filter(|c| !c.is_control()) {
        if name.len() + c.len_utf8() > MAX_PLUGIN_DISPLAY_NAME_BYTES {
            break;
        }
        name.push(c);
    }
    let name = name.trim_end().to_owned();
    (!name.is_empty()).then_some(name)
}

/// The message-content contribution an envelope renders through, when the manifest declares it.
fn card_contribution<'m>(
    manifest: &'m PluginManifest,
    envelope: &PluginMessage,
) -> Option<&'m UiContribution> {
    manifest.client_extensions.contributions.iter().find(|c| {
        c.kind == UiContributionKind::MessageContent
            && c.id == envelope.contribution_id
            && c.schema_version == envelope.schema_version
    })
}

/// The entity a raw `data.message` names, read as leniently as possible, for telling the plugin
/// its card was not posted even when the envelope itself is invalid.
fn raw_entity(manifest: &PluginManifest, raw: &Value) -> Option<(String, String)> {
    let contribution_id = raw.get("contributionId")?.as_str()?;
    let contribution = manifest
        .client_extensions
        .contributions
        .iter()
        .find(|c| c.kind == UiContributionKind::MessageContent && c.id == contribution_id)?;
    let entity = contribution.entity_of(raw.get("payload")?)?;
    Some((contribution.id.clone(), entity))
}

fn not_posted(reason: impl Into<String>) -> CapabilityError {
    CapabilityError::plugin(PLUGIN_CARD_NOT_POSTED, reason)
}

/// An open outcome: the API gateway answers it `CAPABILITY_OUTCOME_UNKNOWN` (503) and the client
/// retries with the same command id.
fn outcome_unknown(reason: impl Into<String>) -> CapabilityError {
    CapabilityError::Timeout(reason.into())
}

/// The `posted` answer of a card.
fn posted_json(conversation_id: &str, server_msg_id: &str, client_msg_id: &str) -> Value {
    json!({
        "conversationId": conversation_id,
        "serverMsgId": server_msg_id,
        "clientMsgId": client_msg_id,
    })
}

impl PluginCardPoster {
    pub fn new(
        delivery: Option<Arc<dyn PluginCardDelivery>>,
        bindings: Option<Arc<dyn PluginEntityBindingStore>>,
        lifecycle: Option<Arc<dyn EntityLifecycleOutbox>>,
    ) -> Self {
        Self {
            delivery,
            bindings,
            lifecycle,
            backoff: POST_BACKOFF,
        }
    }

    /// The same poster with no pause between sends (tests).
    pub fn without_backoff(mut self) -> Self {
        self.backoff = [Duration::ZERO; 2];
        self
    }

    /// Tell the plugin its card was not posted (at least once, through the outbox).
    async fn give_up(&self, call: &CardPostCall<'_>, entity: Option<(String, String)>) {
        self.queue_post_failed(call, entity, None).await;
    }

    /// Queue `post_failed`: confirmed (`verify_after` `None`) when the card was refused or never
    /// sent; unconfirmed, due after `verify_after`, when Ingest may have stored it.
    async fn queue_post_failed(
        &self,
        call: &CardPostCall<'_>,
        entity: Option<(String, String)>,
        verify_after: Option<Duration>,
    ) {
        let Some((contribution_id, entity)) = entity else {
            return;
        };
        let plugin_id = call.manifest.plugin_id.as_str();
        let Some(lifecycle) = &self.lifecycle else {
            tracing::error!(tenant_id = call.tenant_id, plugin_id, %entity, "plugin card not posted and no lifecycle outbox: the plugin is not told");
            return;
        };
        // An entity with a card is never rolled back (the worker checks again before delivery,
        // so a failed read here is not a reason to skip the row).
        if let Some(bindings) = &self.bindings
            && bindings
                .bindings(call.tenant_id, plugin_id, &entity)
                .await
                .is_ok_and(|rows| !rows.is_empty())
        {
            tracing::warn!(tenant_id = call.tenant_id, plugin_id, %entity, "entity already has a card; post_failed not queued");
            return;
        }
        let stored = match verify_after {
            None => {
                lifecycle
                    .enqueue(
                        call.tenant_id,
                        plugin_id,
                        &contribution_id,
                        &entity,
                        UNBOUND_REASON_POST_FAILED,
                        None,
                    )
                    .await
            }
            Some(delay) => {
                lifecycle
                    .enqueue_unconfirmed(
                        call.tenant_id,
                        plugin_id,
                        &contribution_id,
                        &entity,
                        delay,
                    )
                    .await
            }
        };
        if let Err(error) = stored {
            tracing::error!(%error, tenant_id = call.tenant_id, plugin_id, %entity, "plugin card not posted and the post_failed notification was not stored");
        }
    }

    /// The card an entity already has (a replayed create): a live binding answers the call with
    /// that card, a
    /// recalled one refuses it; either way nothing is posted and nothing is rolled back. `None`
    /// when the entity has no binding yet. A store failure is an open outcome.
    async fn bound_card(
        &self,
        call: &CardPostCall<'_>,
        entity: &str,
    ) -> Result<Option<std::result::Result<(String, String), CapabilityError>>> {
        let Some(bindings) = &self.bindings else {
            return Ok(None);
        };
        let rows = bindings
            .bindings(call.tenant_id, &call.manifest.plugin_id, entity)
            .await
            .map_err(|_| outcome_unknown("plugin entity bindings unavailable; retry"))?;
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(match rows.iter().find(|b| !b.recalled) {
            Some(live) => Ok((live.conversation_id.clone(), live.server_msg_id.clone())),
            None => Err(CapabilityError::plugin(
                PLUGIN_MESSAGE_RECALLED,
                "the card of this entity was recalled",
            )),
        }))
    }

    /// Steps 1-6 of the module docs. `data` without `message` comes back unchanged.
    pub async fn post_result(&self, call: CardPostCall<'_>, mut data: Value) -> Result<Value> {
        let Some(raw) = data.as_object_mut().and_then(|data| data.remove("message")) else {
            return Ok(data);
        };
        let plugin_id = call.manifest.plugin_id.as_str();
        let checked = serde_json::from_value::<PluginMessage>(raw.clone())
            .ok()
            .filter(|envelope| envelope.plugin_id == plugin_id && envelope.validate().is_ok())
            .and_then(|envelope| {
                let contribution = card_contribution(call.manifest, &envelope)?;
                let entity = match &contribution.entity_path {
                    Some(_) => Some((
                        contribution.id.clone(),
                        contribution.entity_of(&envelope.payload)?,
                    )),
                    None => None,
                };
                Some((envelope, entity))
            });
        let Some((mut envelope, entity)) = checked else {
            tracing::warn!(
                tenant_id = call.tenant_id,
                plugin_id,
                "plugin returned an invalid data.message; no card posted"
            );
            self.give_up(&call, raw_entity(call.manifest, &raw)).await;
            return Err(not_posted("the plugin returned an invalid card"));
        };
        if let Some((_, entity)) = &entity {
            match self.bound_card(&call, entity).await? {
                None => {}
                Some(Ok((bound_conversation, server_msg_id))) => {
                    // The client message id the bound card was posted with when the original call
                    // carried the same command.
                    let client_msg_id = card_client_msg_id(
                        call.tenant_id,
                        plugin_id,
                        call.user_id,
                        &bound_conversation,
                        call.request_id,
                        call.input,
                    );
                    if let Some(object) = data.as_object_mut() {
                        object.insert(
                            "posted".into(),
                            posted_json(&bound_conversation, &server_msg_id, &client_msg_id),
                        );
                    }
                    return Ok(data);
                }
                Some(Err(recalled)) => return Err(recalled),
            }
        }
        let Some(conversation_id) = call.conversation_id.filter(|id| !id.is_empty()) else {
            self.give_up(&call, entity).await;
            return Err(not_posted(
                "the call has no conversation to post the card to",
            ));
        };
        if let (Some(lifecycle), Some((_, entity))) = (&self.lifecycle, &entity) {
            match lifecycle
                .post_failed(call.tenant_id, plugin_id, entity)
                .await
            {
                Ok(false) => {}
                Ok(true) => {
                    return Err(not_posted(
                        "the card of this entity was given up; the plugin rolled it back",
                    ));
                }
                Err(error) => {
                    // Posting unchecked could put a card on an entity the plugin rolled back.
                    tracing::warn!(%error, tenant_id = call.tenant_id, plugin_id, %entity, "lifecycle outbox unavailable; no card posted");
                    return Err(outcome_unknown(
                        "plugin entity lifecycle outbox unavailable; retry",
                    ));
                }
            }
        }
        envelope.display_name = envelope_display_name(&call.manifest.display_name);
        let payload = serde_json::to_vec(&envelope)
            .map_err(|_| CapabilityError::System("plugin card encoding failed".into()))?;
        if envelope.validate().is_err() {
            self.give_up(&call, entity).await;
            return Err(not_posted("the card exceeds the plugin message limit"));
        }
        let Some(delivery) = &self.delivery else {
            tracing::error!(
                tenant_id = call.tenant_id,
                plugin_id,
                "plugin card posting is not configured (FLARE_PLUGIN_CARD_INGEST_URL, FLARE_CAPABILITY_CTX_PRIVATE_KEY)"
            );
            self.give_up(&call, entity).await;
            return Err(not_posted("plugin card posting is not configured"));
        };
        let client_msg_id = card_client_msg_id(
            call.tenant_id,
            plugin_id,
            call.user_id,
            conversation_id,
            call.request_id,
            call.input,
        );
        let mut last = CardPostError::Unavailable("not attempted".into());
        let mut posted = None;
        // Whether a send reached Ingest without a definite answer: only then may the card exist.
        let mut sent_unanswered = false;
        for attempt in 0..MAX_POST_ATTEMPTS {
            if attempt > 0 {
                let pause = self.backoff[(attempt as usize - 1).min(self.backoff.len() - 1)];
                if !pause.is_zero() {
                    tokio::time::sleep(pause).await;
                }
            }
            let route = match delivery
                .route(
                    call.tenant_id,
                    call.user_id,
                    conversation_id,
                    call.request_id,
                )
                .await
            {
                Ok(route) => route,
                Err(error) => {
                    last = error;
                    if matches!(last, CardPostError::Refused(_)) {
                        break;
                    }
                    continue;
                }
            };
            let message = Message {
                client_msg_id: client_msg_id.clone(),
                conversation_id: conversation_id.to_owned(),
                sender_id: call.user_id.to_owned(),
                source: MessageSource::User as i32,
                message_type: MessageType::AppCard as i32,
                conversation_type: route.conversation_type,
                channel_id: route.channel_id,
                content: Some(MessageContent {
                    content: Some(Content::AppCard(AppCardContent {
                        app_id: plugin_id.to_owned(),
                        card_type: PLUGIN_CARD_TYPE.into(),
                        payload_schema: PLUGIN_CARD_PAYLOAD_SCHEMA.into(),
                        payload: payload.clone(),
                        ..Default::default()
                    })),
                }),
                ..Default::default()
            };
            match delivery.post(call.tenant_id, message).await {
                Ok(card) => {
                    posted = Some(card);
                    break;
                }
                Err(error) => {
                    tracing::warn!(tenant_id = call.tenant_id, plugin_id, attempt, %error, "plugin card post failed");
                    last = error;
                    if matches!(last, CardPostError::Refused(_)) {
                        break;
                    }
                    sent_unanswered = true;
                }
            }
        }
        let Some(card) = posted else {
            if sent_unanswered {
                // A send reached Ingest without a definite answer, so the card may be stored (a
                // later refusal of the route does not undo it): roll nothing back now.
                tracing::warn!(tenant_id = call.tenant_id, plugin_id, conversation_id, error = %last, "plugin card post outcome unknown; post_failed queued unconfirmed");
                self.queue_post_failed(&call, entity, Some(POST_VERIFY_DELAY))
                    .await;
                return Err(outcome_unknown(format!(
                    "the card post outcome is unknown ({last}); retry with the same command"
                )));
            }
            tracing::warn!(tenant_id = call.tenant_id, plugin_id, conversation_id, error = %last, "plugin card not posted; the plugin is told post_failed");
            self.give_up(&call, entity).await;
            return Err(not_posted(match last {
                CardPostError::Refused(reason) => format!("the card was refused: {reason}"),
                CardPostError::Unavailable(reason) => {
                    format!("the card could not be delivered: {reason}")
                }
            }));
        };
        if let (Some(bindings), Some((contribution_id, entity))) = (&self.bindings, &entity)
            && let Err(error) = bindings
                .register(
                    call.tenant_id,
                    plugin_id,
                    contribution_id,
                    entity,
                    &card.server_msg_id,
                    conversation_id,
                )
                .await
        {
            tracing::warn!(%error, tenant_id = call.tenant_id, plugin_id, %entity, "posted plugin card not bound yet; Storage Writer binds it when it stores the card");
        }
        if let Some(object) = data.as_object_mut() {
            object.insert(
                "posted".into(),
                posted_json(conversation_id, &card.server_msg_id, &client_msg_id),
            );
        }
        Ok(data)
    }
}

#[cfg(test)]
#[path = "plugin_card_tests.rs"]
pub(crate) mod tests;
