use super::*;
use crate::domain::repository::plugin_state::{ClaimedBinding, MessageBinding};
use serde_json::json;
use std::collections::BTreeMap;

pub(crate) const POLL_MANIFEST: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../flare-proto/flare-plugin-protocol/tests/fixtures/plugin-ui-v2-manifest.json"
));

/// Bindings with the store's compare-and-set semantics, in memory.
#[derive(Default)]
pub(crate) struct MemoryBindings {
    pub rows: Mutex<BTreeMap<(String, String, String), Vec<EntityBinding>>>,
    /// The contribution of a bound message, when not the example poll's card.
    pub contributions: Mutex<BTreeMap<String, String>>,
    pub fail: std::sync::atomic::AtomicBool,
}
impl MemoryBindings {
    /// Bind like [`Self::bind`] through the card of `contribution`.
    pub(crate) fn bind_through(
        &self,
        tenant: &str,
        plugin: &str,
        contribution: &str,
        entity: &str,
        conversation: &str,
        message: &str,
    ) {
        self.bind(tenant, plugin, entity, conversation, message);
        self.contributions
            .lock()
            .unwrap()
            .insert(message.into(), contribution.into());
    }
    pub(crate) fn bind(
        &self,
        tenant: &str,
        plugin: &str,
        entity: &str,
        conversation: &str,
        message: &str,
    ) {
        self.rows
            .lock()
            .unwrap()
            .entry((tenant.into(), plugin.into(), entity.into()))
            .or_default()
            .push(EntityBinding {
                conversation_id: conversation.into(),
                server_msg_id: message.into(),
                published_version: 0,
                recalled: false,
            });
    }
    pub(crate) fn recall(&self, message: &str) {
        for rows in self.rows.lock().unwrap().values_mut() {
            for row in rows.iter_mut().filter(|row| row.server_msg_id == message) {
                row.recalled = true;
            }
        }
    }
    fn key(tenant: &str, plugin: &str, entity: &str) -> (String, String, String) {
        (tenant.into(), plugin.into(), entity.into())
    }
}
#[async_trait::async_trait]
impl PluginEntityBindingStore for MemoryBindings {
    async fn bindings(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        entity: &str,
    ) -> flare_server_core::error::Result<Vec<EntityBinding>> {
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(flare_server_core::error::FlareError::system("injected"));
        }
        Ok(self
            .rows
            .lock()
            .unwrap()
            .get(&Self::key(tenant_id, plugin_id, entity))
            .cloned()
            .unwrap_or_default())
    }
    async fn binding_by_message(
        &self,
        tenant_id: &str,
        server_msg_id: &str,
    ) -> flare_server_core::error::Result<Option<MessageBinding>> {
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(flare_server_core::error::FlareError::system("injected"));
        }
        let rows = self.rows.lock().unwrap();
        let found = rows
            .iter()
            .filter(|((tenant, _, _), _)| tenant == tenant_id)
            .find_map(|((_, plugin, entity), rows)| {
                rows.iter()
                    .find(|row| row.server_msg_id == server_msg_id)
                    .map(|row| (plugin.clone(), entity.clone(), row.clone()))
            });
        Ok(found.map(|(plugin_id, entity, row)| MessageBinding {
            plugin_id,
            contribution_id: self
                .contributions
                .lock()
                .unwrap()
                .get(server_msg_id)
                .cloned()
                .unwrap_or_else(|| "example.poll.card".into()),
            entity,
            conversation_id: row.conversation_id,
            recalled: row.recalled,
        }))
    }
    async fn claim_version(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        entity: &str,
        version: u64,
    ) -> flare_server_core::error::Result<Vec<ClaimedBinding>> {
        let mut rows = self.rows.lock().unwrap();
        let Some(rows) = rows.get_mut(&Self::key(tenant_id, plugin_id, entity)) else {
            return Ok(vec![]);
        };
        Ok(rows
            .iter_mut()
            .filter(|row| !row.recalled && row.published_version < version)
            .map(|row| {
                let prior_version = std::mem::replace(&mut row.published_version, version);
                ClaimedBinding {
                    conversation_id: row.conversation_id.clone(),
                    server_msg_id: row.server_msg_id.clone(),
                    prior_version,
                }
            })
            .collect())
    }
    async fn register(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        contribution_id: &str,
        entity: &str,
        server_msg_id: &str,
        conversation_id: &str,
    ) -> flare_server_core::error::Result<()> {
        let mut rows = self.rows.lock().unwrap();
        let rows = rows
            .entry(Self::key(tenant_id, plugin_id, entity))
            .or_default();
        if rows.is_empty() {
            rows.push(EntityBinding {
                conversation_id: conversation_id.into(),
                server_msg_id: server_msg_id.into(),
                published_version: 0,
                recalled: false,
            });
            self.contributions
                .lock()
                .unwrap()
                .insert(server_msg_id.into(), contribution_id.into());
        }
        Ok(())
    }
    async fn release_version(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        entity: &str,
        binding: &ClaimedBinding,
        version: u64,
    ) -> flare_server_core::error::Result<()> {
        let mut rows = self.rows.lock().unwrap();
        if let Some(rows) = rows.get_mut(&Self::key(tenant_id, plugin_id, entity)) {
            for row in rows.iter_mut().filter(|row| {
                row.server_msg_id == binding.server_msg_id && row.published_version == version
            }) {
                row.published_version = binding.prior_version;
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct RecordingSink {
    pub events: Mutex<Vec<(String, String, PluginStateEvent)>>,
    pub fail: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl PluginStateEventSink for RecordingSink {
    async fn publish(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        event: PluginStateEvent,
    ) -> flare_server_core::error::Result<()> {
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(flare_server_core::error::FlareError::system("injected"));
        }
        self.events
            .lock()
            .unwrap()
            .push((tenant_id.into(), conversation_id.into(), event));
        Ok(())
    }
}
impl RecordingSink {
    pub(crate) fn versions(&self) -> Vec<u64> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, event)| event.version)
            .collect()
    }
}

fn state(version: u64, votes: u64) -> SharedState {
    serde_json::from_value(
        json!({"entity":"poll-1","version":version,"values":{"counts":[votes,1]}}),
    )
    .unwrap()
}

pub(crate) fn publisher(
    bindings: Arc<MemoryBindings>,
    sink: Arc<RecordingSink>,
) -> Arc<PluginStatePublisher> {
    Arc::new(PluginStatePublisher::with_window(
        bindings,
        sink,
        COALESCE_WINDOW,
        Arc::new(PluginStateMetrics::new()),
    ))
}

/// Let spawned flushes run (time is paused: sleeping advances it).
async fn settle(by: Duration) {
    tokio::time::sleep(by).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

#[test]
fn the_coalescer_flushes_at_once_then_throttles_to_the_window() {
    let key = EntityKey {
        tenant_id: "app-a".into(),
        plugin_id: "example.poll".into(),
        entity: "poll-1".into(),
    };
    let start = Instant::now();
    let mut coalescer = Coalescer::new(COALESCE_WINDOW);
    // First publish: flush now.
    assert_eq!(
        coalescer.offer(&key, state(1, 1), PublishSource::Invocation, start),
        Some(Duration::ZERO)
    );
    // Before the flush ran, a newer one merges into it.
    assert_eq!(
        coalescer.offer(&key, state(2, 2), PublishSource::Invocation, start),
        None
    );
    assert_eq!(coalescer.pending_version(&key), Some(2));
    assert_eq!(coalescer.take(&key, start).unwrap().0.version, 2);
    // Within the window: wait for its end; an older pending never replaces a newer one.
    let soon = start + Duration::from_millis(100);
    assert_eq!(
        coalescer.offer(&key, state(4, 4), PublishSource::Invocation, soon),
        Some(Duration::from_millis(400))
    );
    assert_eq!(
        coalescer.offer(&key, state(3, 3), PublishSource::Bridge, soon),
        None
    );
    assert_eq!(coalescer.pending_version(&key), Some(4));
    let at = start + COALESCE_WINDOW;
    assert_eq!(coalescer.take(&key, at).unwrap().0.version, 4);
    // After a quiet window: at once again.
    assert_eq!(
        coalescer.offer(
            &key,
            state(5, 5),
            PublishSource::Bridge,
            at + COALESCE_WINDOW
        ),
        Some(Duration::ZERO)
    );
    // Nothing pending: nothing to flush.
    assert!(coalescer.take(&key, at + COALESCE_WINDOW).is_some());
    assert!(coalescer.take(&key, at + COALESCE_WINDOW).is_none());
}

#[tokio::test(start_paused = true)]
async fn newer_versions_fan_out_coalesced_same_is_duplicate_older_is_stale() {
    let bindings = Arc::new(MemoryBindings::default());
    bindings.bind("app-a", "example.poll", "poll-1", "2-group", "m-1");
    let sink = Arc::new(RecordingSink::default());
    let publisher = publisher(bindings.clone(), sink.clone());
    let conversation = PublishScope::Conversation("2-group");
    let submit = |state| {
        let publisher = publisher.clone();
        async move {
            publisher
                .submit(
                    "app-a",
                    "example.poll",
                    conversation,
                    state,
                    PublishSource::Invocation,
                )
                .await
                .unwrap()
        }
    };

    assert_eq!(
        submit(state(1, 1)).await,
        EntityStatePublishOutcome::Published
    );
    settle(Duration::from_millis(1)).await;
    assert_eq!(sink.versions(), vec![1]);
    // Two users vote within the window: one fan-out, the highest version.
    assert_eq!(
        submit(state(2, 2)).await,
        EntityStatePublishOutcome::Published
    );
    assert_eq!(
        submit(state(3, 3)).await,
        EntityStatePublishOutcome::Published
    );
    // Pending 3: the same version is a duplicate, an older one is stale.
    assert_eq!(
        submit(state(3, 3)).await,
        EntityStatePublishOutcome::Duplicate
    );
    assert_eq!(submit(state(2, 2)).await, EntityStatePublishOutcome::Stale);
    settle(Duration::from_millis(250)).await;
    assert_eq!(sink.versions(), vec![1], "throttled to the window");
    settle(Duration::from_millis(300)).await;
    assert_eq!(sink.versions(), vec![1, 3]);
    // Stored 3: same and older are refused without a fan-out.
    assert_eq!(
        submit(state(3, 3)).await,
        EntityStatePublishOutcome::Duplicate
    );
    assert_eq!(submit(state(1, 1)).await, EntityStatePublishOutcome::Stale);
    settle(Duration::from_secs(2)).await;
    assert_eq!(sink.versions(), vec![1, 3]);
    let events = sink.events.lock().unwrap().clone();
    let (tenant, conversation, event) = &events[1];
    assert_eq!(
        (tenant.as_str(), conversation.as_str()),
        ("app-a", "2-group")
    );
    assert_eq!(event.server_msg_id, "m-1");
    assert_eq!(event.plugin_id, "example.poll");
    assert_eq!(event.entity, "poll-1");
    assert_eq!(event.values_json, r#"{"counts":[3,1]}"#);
}

#[tokio::test(start_paused = true)]
async fn unbound_other_conversation_recalled_and_malformed_states_are_refused() {
    let bindings = Arc::new(MemoryBindings::default());
    bindings.bind("app-a", "example.poll", "poll-1", "2-group", "m-1");
    let sink = Arc::new(RecordingSink::default());
    let publisher = publisher(bindings.clone(), sink.clone());
    let refused = |scope: PublishScope<'static>, plugin: &'static str, state| {
        let publisher = publisher.clone();
        async move {
            matches!(
                publisher
                    .submit("app-a", plugin, scope, state, PublishSource::Bridge)
                    .await,
                Err(CapabilityError::PolicyDenied(_))
            )
        }
    };
    // A card of another conversation, another plugin's entity, an unknown entity.
    assert!(
        refused(
            PublishScope::Conversation("1-single"),
            "example.poll",
            state(1, 1)
        )
        .await
    );
    assert!(refused(PublishScope::Tenant, "example.other", state(1, 1)).await);
    let mut unknown = state(1, 1);
    unknown.entity = "poll-2".into();
    assert!(refused(PublishScope::Tenant, "example.poll", unknown).await);
    // A malformed state (object value, version 0).
    let mut nested = state(1, 1);
    nested.values.insert("bad".into(), json!({"x": 1}));
    assert!(refused(PublishScope::Tenant, "example.poll", nested).await);
    assert!(refused(PublishScope::Tenant, "example.poll", state(0, 1)).await);
    // The tenant scope (staff, runtime) reaches any bound card.
    assert_eq!(
        publisher
            .submit(
                "app-a",
                "example.poll",
                PublishScope::Tenant,
                state(1, 1),
                PublishSource::Bridge
            )
            .await
            .unwrap(),
        EntityStatePublishOutcome::Published
    );
    settle(Duration::from_millis(1)).await;
    // A recalled card takes no more state.
    bindings.recall("m-1");
    assert!(refused(PublishScope::Tenant, "example.poll", state(2, 2)).await);
    settle(Duration::from_secs(1)).await;
    assert_eq!(sink.versions(), vec![1]);
    // The binding store being down is an error, not a silent drop.
    bindings
        .fail
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(matches!(
        publisher
            .submit(
                "app-a",
                "example.poll",
                PublishScope::Tenant,
                state(3, 3),
                PublishSource::Bridge
            )
            .await,
        Err(CapabilityError::System(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn a_failed_event_hands_its_version_back_for_a_retry() {
    let bindings = Arc::new(MemoryBindings::default());
    bindings.bind("app-a", "example.poll", "poll-1", "2-group", "m-1");
    let sink = Arc::new(RecordingSink::default());
    let publisher = publisher(bindings.clone(), sink.clone());
    sink.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    publisher
        .submit(
            "app-a",
            "example.poll",
            PublishScope::Tenant,
            state(1, 1),
            PublishSource::Bridge,
        )
        .await
        .unwrap();
    settle(Duration::from_millis(1)).await;
    assert!(sink.versions().is_empty());
    assert_eq!(
        bindings
            .bindings("app-a", "example.poll", "poll-1")
            .await
            .unwrap()[0]
            .published_version,
        0
    );
    sink.fail.store(false, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        publisher
            .submit(
                "app-a",
                "example.poll",
                PublishScope::Tenant,
                state(1, 1),
                PublishSource::Bridge
            )
            .await
            .unwrap(),
        EntityStatePublishOutcome::Published
    );
    settle(Duration::from_secs(1)).await;
    assert_eq!(sink.versions(), vec![1]);
}

#[test]
fn invocation_entity_follows_the_card_actions_that_bind_entity_path() {
    let manifest = PluginManifest::from_json(POLL_MANIFEST.as_bytes()).unwrap();
    let input = json!({"pollId": "poll-1", "selection": [0]});
    for operation in [
        "example.poll.vote",
        "example.poll.close",
        "example.poll.details",
    ] {
        assert_eq!(
            invocation_entity(&manifest, operation, &input).as_deref(),
            Some("poll-1"),
            "{operation}"
        );
    }
    // Creating a poll, or an admin-only operation, names no card entity.
    assert_eq!(
        invocation_entity(&manifest, "example.poll.create", &input),
        None
    );
    assert_eq!(
        invocation_entity(&manifest, "example.poll.remove", &input),
        None
    );
    // An absent or invalid id names nothing.
    assert_eq!(
        invocation_entity(&manifest, "example.poll.vote", &json!({})),
        None
    );
    assert_eq!(
        invocation_entity(&manifest, "example.poll.vote", &json!({"pollId": "a b"})),
        None
    );
    assert_eq!(
        invocation_entity(&manifest, "example.poll.vote", &json!({"pollId": 7})).as_deref(),
        Some("7")
    );
}

/// Where each card passes its entity — per contribution, through card actions, the
/// detail operation's input and detail actions.
#[test]
fn entity_inputs_list_every_card_that_passes_its_entity() {
    let mut raw: Value = serde_json::from_str(POLL_MANIFEST).unwrap();
    let mut second = raw["clientExtensions"]["contributions"][0].clone();
    second["id"] = json!("example.poll.card2");
    for action in second["schema"]["actions"].as_array_mut().unwrap() {
        if action.get("input").is_some() {
            action["input"] = json!({"id": "/pollId"});
        }
    }
    second["detail"]["input"] = json!({"poll": "/pollId"});
    raw["clientExtensions"]["contributions"]
        .as_array_mut()
        .unwrap()
        .push(second);
    let manifest =
        PluginManifest::from_json(serde_json::to_string(&raw).unwrap().as_bytes()).unwrap();
    let pairs = |operation: &str| {
        entity_inputs(&manifest, operation)
            .into_iter()
            .map(|(c, n)| format!("{c}={n}"))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        pairs("example.poll.vote"),
        ["example.poll.card=pollId", "example.poll.card2=id"]
    );
    assert_eq!(
        pairs("example.poll.details"),
        ["example.poll.card=pollId", "example.poll.card2=poll"]
    );
    assert!(pairs("example.poll.create").is_empty());
    // The admin page passes pollId to remove, but it is no card: not listed.
    assert!(pairs("example.poll.remove").is_empty());
    // invocation_entity takes the first input that carries a valid entity.
    assert_eq!(
        invocation_entity(&manifest, "example.poll.vote", &json!({"id": "poll-2"})).as_deref(),
        Some("poll-2")
    );
}

/// The protocol's example poll, fully approved for `app-a`, served at `authority`.
pub(crate) fn poll_snapshot(
    authority: String,
) -> flare_im_capability_core::plugin::runtime::PluginRuntimeSnapshot {
    use flare_im_capability_core::plugin::authorization::TenantPluginGrant;
    use flare_im_capability_core::plugin::runtime::*;
    let manifest = PluginManifest::from_json(POLL_MANIFEST.as_bytes()).unwrap();
    let raw = POLL_MANIFEST.to_string();
    let digest = digest_bytes(raw.as_bytes());
    let now = chrono::Utc::now().timestamp_millis();
    PluginRuntimeSnapshot {
        tenant_id: "app-a".into(),
        version: 3,
        issued_at_ms: now,
        valid_until_ms: now + 300_000,
        plugins: vec![ProjectedPlugin {
            grant: TenantPluginGrant {
                grant_id: "g-poll".into(),
                revision: 4,
                tenant_id: "app-a".into(),
                plugin_id: "example.poll".into(),
                plugin_version: manifest.version.clone(),
                manifest_digest: digest.clone(),
                approved_operations: manifest.operation_ids(),
                approved_permissions: manifest.permissions.iter().map(|p| p.id.clone()).collect(),
                approved_data_scopes: manifest
                    .permissions
                    .iter()
                    .flat_map(|p| p.data_scopes.iter().cloned())
                    .collect(),
                enabled: true,
                revoked: false,
                valid_from_ms: now,
                valid_until_ms: now + 300_000,
            },
            manifest_json: raw,
            deployment: PluginDeployment {
                deployment_id: "poll-1".into(),
                plugin_id: "example.poll".into(),
                plugin_version: manifest.version.clone(),
                manifest_digest: digest,
                artifact_digest: manifest.artifact.as_ref().unwrap().digest.clone(),
                grpc_authority: authority,
            },
            configuration_digest: configuration_digest(&json!({"maxOptions": 6})).unwrap(),
            client_configuration: Default::default(),
        }],
    }
}
