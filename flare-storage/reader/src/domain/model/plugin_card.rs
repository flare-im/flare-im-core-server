//! Read-time mark of plugin card copies (plugin platform v2 §1, `unbound_copy`).
//!
//! Only the card bound to a plugin entity (`plugin_entity_bindings`) is the card the platform
//! posted; actions on any other card naming that entity are refused (`PLUGIN_CARD_UNBOUND`). Every
//! read that returns messages marks those other cards with [`PLUGIN_CARD_UNBOUND_KEY`]` = "true"` so
//! clients render them read-only; the bound card never carries the key. A card is a copy when:
//!
//! - it is a top-level Custom of the plugin card type (whatever the case or surrounding blanks):
//!   the platform only ever posts APP_CARD, so such a card is never bound;
//! - it is a top-level APP_CARD `flare.plugin.message.v1` with a valid envelope of its `app_id`,
//!   no binding names it, and its contribution declares an entity it carries (the same rule
//!   Storage Writer binds by: an enabled, unrevoked, leased grant of the tenant's projection, a
//!   message-content contribution with `entityPath`, and a valid entity at that path; see the
//!   writer's `plugin_entity_bindings`). A card naming no entity is not a copy of anything: its
//!   actions are not gated by a binding, and marking it would make it read-only for nothing.
//!
//! The key is server-owned: any stored value (a sender could have preset it before Ingest stripped
//! it) is removed first. Cost per page: nothing for a page without APP_CARD plugin cards; otherwise
//! one indexed lookup of the bindings naming those messages, and the tenant's parsed projection
//! (cached per version) only when one of them has no binding.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use flare_im_capability_core::plugin::manifest::{UiContribution, UiContributionKind};
use flare_im_capability_core::plugin::runtime::PluginRuntimeSnapshot;
use flare_im_capability_core::plugin::ui::PluginMessage;
use flare_proto::common::MessageContent;
use flare_proto::common::message_content::Content;
use flare_server_core::error::Result;

/// Message attribute (extra) of a plugin card that is not the card bound to its entity.
pub const PLUGIN_CARD_UNBOUND_KEY: &str = "pluginCardUnbound";
/// APP_CARD `card_type` of a plugin card.
pub const PLUGIN_CARD_TYPE: &str = "flare.plugin.message.v1";
/// Envelope bytes a client accepts (the protocol's message limit).
const MAX_ENVELOPE_BYTES: usize = 65536;

/// A message a read returns: the domain message (`extra`) or a wire message inside an event
/// (`attributes`).
pub trait PluginCardCarrier: Send {
    fn server_id(&self) -> &str;
    fn content(&self) -> Option<&MessageContent>;
    fn attributes(&mut self) -> &mut HashMap<String, String>;
}

impl PluginCardCarrier for flare_im_contracts::message::Message {
    fn server_id(&self) -> &str {
        &self.server_id
    }
    fn content(&self) -> Option<&MessageContent> {
        self.content.as_ref()
    }
    fn attributes(&mut self) -> &mut HashMap<String, String> {
        &mut self.extra
    }
}

impl PluginCardCarrier for flare_proto::common::Message {
    fn server_id(&self) -> &str {
        &self.server_id
    }
    fn content(&self) -> Option<&MessageContent> {
        self.content.as_ref()
    }
    fn attributes(&mut self) -> &mut HashMap<String, String> {
        &mut self.attributes
    }
}

/// What a message's top-level content is, for the mark.
#[derive(Debug, Clone, PartialEq)]
pub enum PluginCard {
    /// A Custom carrying the plugin card type: never posted by the platform.
    Custom,
    /// An APP_CARD plugin card with a valid envelope of its `app_id`.
    Posted(PluginMessage),
}

/// The plugin card a top-level `content` is, if any. An APP_CARD with an invalid envelope or one
/// of another plugin is no card for clients either and is left alone.
pub fn plugin_card(content: Option<&MessageContent>) -> Option<PluginCard> {
    match content?.content.as_ref()? {
        Content::AppCard(card) if card.card_type == PLUGIN_CARD_TYPE => {
            if card.payload.len() > MAX_ENVELOPE_BYTES {
                return None;
            }
            let envelope: PluginMessage = serde_json::from_slice(&card.payload).ok()?;
            (envelope.validate().is_ok() && envelope.plugin_id == card.app_id)
                .then_some(PluginCard::Posted(envelope))
        }
        Content::Custom(custom) if custom.r#type.trim().eq_ignore_ascii_case(PLUGIN_CARD_TYPE) => {
            Some(PluginCard::Custom)
        }
        _ => None,
    }
}

/// The message-content contributions declaring `entityPath` of the plugins one projection version
/// enables: Storage Writer's binding rule, read side.
#[derive(Debug, Default)]
pub struct EntityRules {
    pub version: u64,
    pub valid_until_ms: i64,
    contributions: HashMap<(String, String), UiContribution>,
}

impl EntityRules {
    pub fn from_snapshot(snapshot: &PluginRuntimeSnapshot, now_ms: i64) -> Self {
        let mut contributions = HashMap::new();
        for projected in &snapshot.plugins {
            let grant = &projected.grant;
            if !grant.enabled
                || grant.revoked
                || now_ms < grant.valid_from_ms
                || now_ms >= grant.valid_until_ms
            {
                continue;
            }
            let Ok(manifest) = projected.manifest() else {
                continue;
            };
            for contribution in manifest.client_extensions.contributions {
                if contribution.kind == UiContributionKind::MessageContent
                    && contribution.entity_path.is_some()
                {
                    contributions.insert(
                        (manifest.plugin_id.clone(), contribution.id.clone()),
                        contribution,
                    );
                }
            }
        }
        Self {
            version: snapshot.version,
            valid_until_ms: snapshot.valid_until_ms,
            contributions,
        }
    }

    /// The entity `envelope` carries under these rules; `None` when its contribution names none.
    pub fn entity_of(&self, envelope: &PluginMessage, now_ms: i64) -> Option<String> {
        if now_ms >= self.valid_until_ms {
            return None;
        }
        self.contributions
            .get(&(envelope.plugin_id.clone(), envelope.contribution_id.clone()))?
            .entity_of(&envelope.payload)
    }
}

/// Where the mark reads bindings and the tenant's projection rules.
#[async_trait::async_trait]
pub trait PluginCardBindingSource: Send + Sync {
    /// `(server_msg_id, plugin_id)` of the bindings naming any of `server_ids`.
    async fn bound_cards(
        &self,
        tenant_id: &str,
        server_ids: &[String],
    ) -> Result<HashSet<(String, String)>>;

    /// The rules of the tenant's current, unexpired projection; `None` without one.
    async fn entity_rules(&self, tenant_id: &str, now_ms: i64) -> Result<Option<Arc<EntityRules>>>;
}

/// Mark the copies among `messages` (see the module docs). Returns how many were marked. A lookup
/// failure leaves the APP_CARD cards unmarked (logged): the mark is a rendering hint, the action
/// guard is the authority, and a read must not fail over it.
pub async fn mark_plugin_card_copies<S: PluginCardBindingSource + ?Sized>(
    source: &S,
    tenant_id: &str,
    messages: &mut [&mut dyn PluginCardCarrier],
    now_ms: i64,
) -> usize {
    let mut marked = 0;
    let mut posted: Vec<(usize, PluginMessage)> = Vec::new();
    for (index, message) in messages.iter_mut().enumerate() {
        message.attributes().remove(PLUGIN_CARD_UNBOUND_KEY);
        match plugin_card(message.content()) {
            Some(PluginCard::Custom) => {
                mark(&mut **message);
                marked += 1;
            }
            Some(PluginCard::Posted(envelope)) if !message.server_id().is_empty() => {
                posted.push((index, envelope));
            }
            _ => {}
        }
    }
    if posted.is_empty() {
        return marked;
    }
    let ids: Vec<String> = posted
        .iter()
        .map(|(index, _)| messages[*index].server_id().to_owned())
        .collect();
    let bound = match source.bound_cards(tenant_id, &ids).await {
        Ok(bound) => bound,
        Err(error) => {
            tracing::warn!(%error, tenant_id, "plugin card bindings unreadable; copies stay unmarked");
            return marked;
        }
    };
    let unbound: Vec<(usize, PluginMessage)> = posted
        .into_iter()
        .filter(|(index, envelope)| {
            !bound.contains(&(
                messages[*index].server_id().to_owned(),
                envelope.plugin_id.clone(),
            ))
        })
        .collect();
    if unbound.is_empty() {
        return marked;
    }
    let rules = match source.entity_rules(tenant_id, now_ms).await {
        Ok(Some(rules)) => rules,
        Ok(None) => return marked,
        Err(error) => {
            tracing::warn!(%error, tenant_id, "plugin projection unreadable; copies stay unmarked");
            return marked;
        }
    };
    for (index, envelope) in unbound {
        if rules.entity_of(&envelope, now_ms).is_some() {
            mark(&mut *messages[index]);
            marked += 1;
        }
    }
    marked
}

fn mark(message: &mut dyn PluginCardCarrier) {
    message
        .attributes()
        .insert(PLUGIN_CARD_UNBOUND_KEY.to_owned(), "true".to_owned());
}

#[cfg(test)]
#[path = "plugin_card_tests.rs"]
pub(crate) mod tests;
