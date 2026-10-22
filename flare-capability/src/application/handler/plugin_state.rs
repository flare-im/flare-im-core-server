//! Plugin entity shared state publishing (`EVENT_MESSAGE_PLUGIN_STATE`).
//!
//! A plugin publishes the state of one of its entities (a poll, a solitaire) in two ways: in a
//! successful call result (`data.sharedState`) or through the host write capability
//! `im.plugin.entity_state.publish`. Both go through [`PluginStatePublisher::submit`]:
//!
//! 1. the state is well formed ([`SharedState::validate`]);
//! 2. the entity is bound to a live (not recalled) card of the plugin, and for a user call the card
//!    is in the conversation the ticket was issued for;
//! 3. it is compared with the stored and pending versions: only a newer one is published, the same
//!    one is a duplicate, an older one is stale;
//! 4. publishes of one entity within [`COALESCE_WINDOW`] are coalesced, only the highest version is
//!    sent, so one entity fans out at most twice a second;
//! 5. each bound card gets one `PluginStateEvent` through the internal event entry, as the system.
//!
//! The fan-out runs in the background: a failure is logged and counted, never returned to the
//! call or bridge request that submitted the state.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flare_im_capability_core::plugin::manifest::{PluginManifest, UiContributionKind};
use flare_im_capability_core::plugin::resource::EntityStatePublishOutcome;
use flare_im_capability_core::plugin::state::{SharedState, SharedStateOrder, valid_entity_id};
use flare_im_capability_core::plugin::ui::{UiActionTarget, UiDocument};
use flare_im_service_kit::metrics::REGISTRY;
use flare_proto::common::PluginStateEvent;
use once_cell::sync::Lazy;
use prometheus::{IntCounterVec, Opts};
use serde_json::Value;
use tokio::time::Instant;

use crate::domain::capability::{CapabilityError, Result};
use crate::domain::repository::plugin_state::{
    EntityBinding, PluginEntityBindingStore, PluginStateEventSink,
};

/// Publishes of one entity closer together than this are coalesced into one.
pub const COALESCE_WINDOW: Duration = Duration::from_millis(500);
/// Entities whose throttle state is kept; idle ones are dropped first.
const MAX_TRACKED_ENTITIES: usize = 4096;

/// Where a publish came from (metric label).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishSource {
    /// `data.sharedState` of a successful call.
    Invocation,
    /// `im.plugin.entity_state.publish` on the resource bridge.
    Bridge,
}
impl PublishSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Invocation => "invocation",
            Self::Bridge => "bridge",
        }
    }
}

/// What the credential binds the publish to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishScope<'a> {
    /// A user call: a bound card must be in this conversation.
    Conversation(&'a str),
    /// A staff call or the plugin runtime: tenant-wide, any bound card of the plugin.
    Tenant,
}

/// `capability_plugin_state_total{source, result}`: submits by outcome (`published`, `duplicate`,
/// `stale`, `invalid`, `unbound`, `unavailable`) and fan-out results (`fanned_out`, `superseded`,
/// `event_failed`, `claim_failed`).
pub struct PluginStateMetrics {
    pub total: IntCounterVec,
}
static METRICS: Lazy<Arc<PluginStateMetrics>> = Lazy::new(|| {
    let metrics = PluginStateMetrics::new();
    let _ = REGISTRY.register(Box::new(metrics.total.clone()));
    Arc::new(metrics)
});
impl PluginStateMetrics {
    pub fn new() -> Self {
        Self {
            total: IntCounterVec::new(
                Opts::new(
                    "capability_plugin_state_total",
                    "Plugin entity shared state submits and fan-outs by source and result",
                ),
                &["source", "result"],
            )
            .expect("valid metric"),
        }
    }
    pub fn registered() -> Arc<Self> {
        Arc::clone(&METRICS)
    }
    pub fn record(&self, source: PublishSource, result: &str) {
        self.total
            .with_label_values(&[source.as_str(), result])
            .inc();
    }
}
impl Default for PluginStateMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct EntityKey {
    pub tenant_id: String,
    pub plugin_id: String,
    pub entity: String,
}

#[derive(Debug, Default)]
struct Slot {
    last_flush: Option<Instant>,
    pending: Option<(SharedState, PublishSource)>,
    scheduled: bool,
}

/// Leading-edge throttle per entity: the first publish after a quiet window flushes at once; later
/// ones within the window wait for its end and only the highest version is flushed.
#[derive(Debug)]
pub(crate) struct Coalescer {
    window: Duration,
    slots: HashMap<EntityKey, Slot>,
}
impl Coalescer {
    pub(crate) fn new(window: Duration) -> Self {
        Self {
            window,
            slots: HashMap::new(),
        }
    }
    /// The highest version waiting for the entity's next flush.
    pub(crate) fn pending_version(&self, key: &EntityKey) -> Option<u64> {
        self.slots
            .get(key)
            .and_then(|slot| slot.pending.as_ref())
            .map(|(state, _)| state.version)
    }
    /// Queue `state`. `Some(delay)` when the caller must flush after `delay`; `None` when a flush
    /// is already scheduled and will carry the highest pending version.
    pub(crate) fn offer(
        &mut self,
        key: &EntityKey,
        state: SharedState,
        source: PublishSource,
        now: Instant,
    ) -> Option<Duration> {
        if self.slots.len() >= MAX_TRACKED_ENTITIES && !self.slots.contains_key(key) {
            let window = self.window;
            self.slots.retain(|_, slot| {
                slot.scheduled
                    || slot.pending.is_some()
                    || slot
                        .last_flush
                        .is_some_and(|at| now.saturating_duration_since(at) < window)
            });
        }
        let slot = self.slots.entry(key.clone()).or_default();
        if slot
            .pending
            .as_ref()
            .is_none_or(|(pending, _)| state.version > pending.version)
        {
            slot.pending = Some((state, source));
        }
        if slot.scheduled {
            return None;
        }
        slot.scheduled = true;
        let ready_at = slot.last_flush.map_or(now, |at| at + self.window);
        Some(ready_at.saturating_duration_since(now))
    }
    /// The state to flush now; the window restarts.
    pub(crate) fn take(
        &mut self,
        key: &EntityKey,
        now: Instant,
    ) -> Option<(SharedState, PublishSource)> {
        let slot = self.slots.get_mut(key)?;
        slot.scheduled = false;
        slot.last_flush = Some(now);
        slot.pending.take()
    }
}

pub struct PluginStatePublisher {
    bindings: Arc<dyn PluginEntityBindingStore>,
    sink: Arc<dyn PluginStateEventSink>,
    coalescer: Mutex<Coalescer>,
    metrics: Arc<PluginStateMetrics>,
}

fn rejected(reason: &str) -> CapabilityError {
    CapabilityError::PolicyDenied(format!("plugin shared state rejected: {reason}"))
}

impl PluginStatePublisher {
    pub fn new(
        bindings: Arc<dyn PluginEntityBindingStore>,
        sink: Arc<dyn PluginStateEventSink>,
    ) -> Self {
        Self::with_window(
            bindings,
            sink,
            COALESCE_WINDOW,
            PluginStateMetrics::registered(),
        )
    }

    pub fn with_window(
        bindings: Arc<dyn PluginEntityBindingStore>,
        sink: Arc<dyn PluginStateEventSink>,
        window: Duration,
        metrics: Arc<PluginStateMetrics>,
    ) -> Self {
        Self {
            bindings,
            sink,
            coalescer: Mutex::new(Coalescer::new(window)),
            metrics,
        }
    }

    pub fn metrics(&self) -> &PluginStateMetrics {
        &self.metrics
    }

    /// Steps 1-4 of the module docs; the fan-out (5) runs in the background. Every outcome is a
    /// success for the caller; a malformed state, an unbound entity or a card in another
    /// conversation is a policy rejection.
    pub async fn submit(
        self: &Arc<Self>,
        tenant_id: &str,
        plugin_id: &str,
        scope: PublishScope<'_>,
        state: SharedState,
        source: PublishSource,
    ) -> Result<EntityStatePublishOutcome> {
        if state.validate().is_err() {
            self.metrics.record(source, "invalid");
            return Err(rejected("malformed state"));
        }
        let bindings = match self
            .bindings
            .bindings(tenant_id, plugin_id, &state.entity)
            .await
        {
            Ok(bindings) => bindings,
            Err(error) => {
                self.metrics.record(source, "unavailable");
                tracing::warn!(%error, tenant_id, plugin_id, entity = %state.entity, "plugin entity bindings unavailable");
                return Err(CapabilityError::System(
                    "plugin entity bindings unavailable".into(),
                ));
            }
        };
        let live: Vec<&EntityBinding> = bindings.iter().filter(|b| !b.recalled).collect();
        let in_scope = match scope {
            PublishScope::Conversation(conversation) => {
                live.iter().any(|b| b.conversation_id == conversation)
            }
            PublishScope::Tenant => !live.is_empty(),
        };
        if !in_scope {
            self.metrics.record(source, "unbound");
            return Err(rejected(
                "entity is not bound to a card of this plugin in scope",
            ));
        }
        let key = EntityKey {
            tenant_id: tenant_id.to_owned(),
            plugin_id: plugin_id.to_owned(),
            entity: state.entity.clone(),
        };
        let stored = live.iter().map(|b| b.published_version).max().unwrap_or(0);
        let delay = {
            let mut coalescer = self
                .coalescer
                .lock()
                .map_err(|_| CapabilityError::System("plugin state coalescer poisoned".into()))?;
            let known = stored.max(coalescer.pending_version(&key).unwrap_or(0));
            match state.order_against((known > 0).then_some(known)) {
                SharedStateOrder::Older => {
                    self.metrics.record(source, "stale");
                    return Ok(EntityStatePublishOutcome::Stale);
                }
                SharedStateOrder::Same => {
                    self.metrics.record(source, "duplicate");
                    return Ok(EntityStatePublishOutcome::Duplicate);
                }
                SharedStateOrder::Newer => coalescer.offer(&key, state, source, Instant::now()),
            }
        };
        self.metrics.record(source, "published");
        if let Some(delay) = delay {
            let publisher = Arc::clone(self);
            tokio::spawn(async move {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                publisher.flush(key).await;
            });
        }
        Ok(EntityStatePublishOutcome::Published)
    }

    async fn flush(&self, key: EntityKey) {
        let taken = self
            .coalescer
            .lock()
            .ok()
            .and_then(|mut coalescer| coalescer.take(&key, Instant::now()));
        let Some((state, source)) = taken else {
            return;
        };
        let claimed = match self
            .bindings
            .claim_version(&key.tenant_id, &key.plugin_id, &key.entity, state.version)
            .await
        {
            Ok(claimed) => claimed,
            Err(error) => {
                self.metrics.record(source, "claim_failed");
                tracing::warn!(%error, tenant_id = %key.tenant_id, plugin_id = %key.plugin_id, entity = %key.entity, version = state.version, "plugin state version claim failed; not fanned out");
                return;
            }
        };
        if claimed.is_empty() {
            self.metrics.record(source, "superseded");
            tracing::debug!(tenant_id = %key.tenant_id, plugin_id = %key.plugin_id, entity = %key.entity, version = state.version, "plugin state already published at this or a newer version");
            return;
        }
        let values_json = state.values_json();
        for binding in claimed {
            let event = PluginStateEvent {
                server_msg_id: binding.server_msg_id.clone(),
                plugin_id: key.plugin_id.clone(),
                entity: key.entity.clone(),
                version: state.version,
                values_json: values_json.clone(),
            };
            match self
                .sink
                .publish(&key.tenant_id, &binding.conversation_id, event)
                .await
            {
                Ok(()) => self.metrics.record(source, "fanned_out"),
                Err(error) => {
                    self.metrics.record(source, "event_failed");
                    tracing::error!(%error, tenant_id = %key.tenant_id, conversation_id = %binding.conversation_id, message_id = %binding.server_msg_id, plugin_id = %key.plugin_id, entity = %key.entity, version = state.version, "plugin state event not sent; version handed back for a retry");
                    if let Err(error) = self
                        .bindings
                        .release_version(
                            &key.tenant_id,
                            &key.plugin_id,
                            &key.entity,
                            &binding,
                            state.version,
                        )
                        .await
                    {
                        tracing::warn!(%error, message_id = %binding.server_msg_id, "plugin state version release failed");
                    }
                }
            }
        }
    }

    /// Whether every card bound to the entity was recalled (`false` without a binding: an entity
    /// not shown by any card yet, such as one being created).
    pub async fn entity_recalled(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        entity: &str,
    ) -> Result<bool> {
        let bindings = self
            .bindings
            .bindings(tenant_id, plugin_id, entity)
            .await
            .map_err(|_| CapabilityError::System("plugin entity bindings unavailable".into()))?;
        Ok(!bindings.is_empty() && bindings.iter().all(|b| b.recalled))
    }
}

/// The entity an invocation of `operation` names: the value an action of a message-content
/// contribution binds from the card's `entityPath` (or its detail operation's input does).
/// `None` when no card action of this operation carries the entity.
pub fn invocation_entity(
    manifest: &PluginManifest,
    operation: &str,
    input: &Value,
) -> Option<String> {
    let entity_at = |name: &str| -> Option<String> {
        let id = match input.get(name)? {
            Value::String(id) => id.clone(),
            Value::Number(n) if n.is_u64() || n.is_i64() => n.to_string(),
            _ => return None,
        };
        valid_entity_id(&id).then_some(id)
    };
    let from_document = |document: &UiDocument, entity_path: &str| {
        document
            .actions
            .iter()
            .find_map(|action| match &action.target {
                UiActionTarget::Operation { operation: target } if target == operation => action
                    .input
                    .iter()
                    .find(|(_, path)| path.as_str() == entity_path)
                    .and_then(|(name, _)| entity_at(name)),
                _ => None,
            })
    };
    manifest
        .client_extensions
        .contributions
        .iter()
        .filter(|c| c.kind == UiContributionKind::MessageContent)
        .find_map(|contribution| {
            let entity_path = contribution.entity_path.as_deref()?;
            from_document(&contribution.schema, entity_path).or_else(|| {
                let detail = contribution.detail.as_ref()?;
                if detail.operation == operation
                    && let Some(found) = detail
                        .input
                        .iter()
                        .find(|(_, path)| path.as_str() == entity_path)
                        .and_then(|(name, _)| entity_at(name))
                {
                    return Some(found);
                }
                from_document(&detail.document, entity_path)
            })
        })
}

#[cfg(test)]
#[path = "plugin_state_tests.rs"]
pub(crate) mod tests;
