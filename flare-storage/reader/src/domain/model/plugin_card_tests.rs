use super::*;
use flare_im_capability_core::plugin::authorization::TenantPluginGrant;
use flare_im_capability_core::plugin::runtime::{
    PluginDeployment, ProjectedPlugin, configuration_digest, digest_bytes,
};
use flare_im_contracts::message::Message;
use flare_proto::common::{AppCardContent, CustomContent, TextContent};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The protocol's v2 example poll: `example.poll.card` binds `/pollId`.
const POLL_MANIFEST: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../flare-proto/flare-plugin-protocol/tests/fixtures/plugin-ui-v2-manifest.json"
));

pub(crate) fn snapshot(
    tenant: &str,
    now: i64,
    manifest: serde_json::Value,
) -> PluginRuntimeSnapshot {
    let raw = serde_json::to_string(&manifest).unwrap();
    let digest = digest_bytes(raw.as_bytes());
    PluginRuntimeSnapshot {
        tenant_id: tenant.into(),
        version: 7,
        issued_at_ms: now,
        valid_until_ms: now + 300_000,
        plugins: vec![ProjectedPlugin {
            grant: TenantPluginGrant {
                grant_id: "g-poll".into(),
                revision: 1,
                tenant_id: tenant.into(),
                plugin_id: "example.poll".into(),
                plugin_version: "0.2.0".into(),
                manifest_digest: digest.clone(),
                approved_operations: Default::default(),
                approved_permissions: Default::default(),
                approved_data_scopes: Default::default(),
                enabled: true,
                revoked: false,
                valid_from_ms: now,
                valid_until_ms: now + 300_000,
            },
            manifest_json: raw,
            deployment: PluginDeployment {
                deployment_id: "poll-1".into(),
                plugin_id: "example.poll".into(),
                plugin_version: "0.2.0".into(),
                manifest_digest: digest,
                artifact_digest: manifest["artifact"]["digest"].as_str().unwrap().into(),
                grpc_authority: "http://127.0.0.1:1".into(),
            },
            configuration_digest: configuration_digest(&serde_json::json!({})).unwrap(),
            client_configuration: Default::default(),
        }],
    }
}

pub(crate) fn poll_manifest() -> serde_json::Value {
    serde_json::from_str(POLL_MANIFEST).unwrap()
}

/// The same poll whose card names no entity (no `entityPath`).
fn entityless_manifest() -> serde_json::Value {
    let mut manifest = poll_manifest();
    for contribution in manifest["clientExtensions"]["contributions"]
        .as_array_mut()
        .unwrap()
    {
        if contribution["id"] == "example.poll.card" {
            contribution.as_object_mut().unwrap().remove("entityPath");
        }
    }
    manifest
}

fn envelope(poll_id: &str) -> serde_json::Value {
    serde_json::json!({
        "pluginId": "example.poll",
        "contributionId": "example.poll.card",
        "schemaVersion": 1,
        "summary": "[Poll]",
        "payload": {"pollId": poll_id, "question": "Lunch?"}
    })
}

fn with_content(server_id: &str, content: Content) -> Message {
    Message {
        server_id: server_id.into(),
        conversation_id: "2-group-a".into(),
        content: Some(MessageContent {
            content: Some(content),
        }),
        ..Default::default()
    }
}

pub(crate) fn card(server_id: &str, app_id: &str, poll_id: &str) -> Message {
    with_content(
        server_id,
        Content::AppCard(AppCardContent {
            app_id: app_id.into(),
            card_type: PLUGIN_CARD_TYPE.into(),
            payload: serde_json::to_vec(&envelope(poll_id)).unwrap(),
            ..Default::default()
        }),
    )
}

pub(crate) fn custom(server_id: &str, r#type: &str) -> Message {
    with_content(
        server_id,
        Content::Custom(CustomContent {
            r#type: r#type.into(),
            payload: serde_json::to_vec(&envelope("poll-1")).unwrap(),
            ..Default::default()
        }),
    )
}

pub(crate) fn text(server_id: &str) -> Message {
    with_content(
        server_id,
        Content::Text(TextContent {
            text: "hi".into(),
            mentions: vec![],
        }),
    )
}

#[derive(Default)]
struct Source {
    bound: HashSet<(String, String)>,
    rules: Option<Arc<EntityRules>>,
    failing: bool,
    lookups: AtomicUsize,
    rule_reads: AtomicUsize,
    asked: Mutex<Vec<Vec<String>>>,
}

impl Source {
    fn with_rules(manifest: serde_json::Value) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            rules: Some(Arc::new(EntityRules::from_snapshot(
                &snapshot("app-a", now, manifest),
                now,
            ))),
            ..Default::default()
        }
    }

    fn bind(mut self, server_id: &str) -> Self {
        self.bound.insert((server_id.into(), "example.poll".into()));
        self
    }
}

#[async_trait::async_trait]
impl PluginCardBindingSource for Source {
    async fn bound_cards(
        &self,
        _tenant_id: &str,
        server_ids: &[String],
    ) -> Result<HashSet<(String, String)>> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        self.asked.lock().unwrap().push(server_ids.to_vec());
        if self.failing {
            return Err(flare_server_core::error::FlareError::system("down"));
        }
        Ok(self.bound.clone())
    }

    async fn entity_rules(
        &self,
        _tenant_id: &str,
        _now_ms: i64,
    ) -> Result<Option<Arc<EntityRules>>> {
        self.rule_reads.fetch_add(1, Ordering::SeqCst);
        if self.failing {
            return Err(flare_server_core::error::FlareError::system("down"));
        }
        Ok(self.rules.clone())
    }
}

async fn run(source: &Source, page: &mut [Message]) -> usize {
    let mut refs: Vec<&mut dyn PluginCardCarrier> = page
        .iter_mut()
        .map(|m| m as &mut dyn PluginCardCarrier)
        .collect();
    mark_plugin_card_copies(
        source,
        "app-a",
        &mut refs,
        chrono::Utc::now().timestamp_millis(),
    )
    .await
}

fn unbound(message: &Message) -> bool {
    message
        .extra
        .get(PLUGIN_CARD_UNBOUND_KEY)
        .map(String::as_str)
        == Some("true")
}

#[tokio::test]
async fn only_the_copies_of_a_bound_entity_are_marked() {
    let source = Source::with_rules(poll_manifest()).bind("m-bound");
    let mut page = vec![
        card("m-bound", "example.poll", "poll-1"),
        card("m-copy", "example.poll", "poll-1"),
        card("m-unknown", "example.poll", "poll-9"),
        text("m-text"),
    ];
    // A preset key on the bound card and on a text message is not the server's word.
    page[0]
        .extra
        .insert(PLUGIN_CARD_UNBOUND_KEY.into(), "true".into());
    page[3]
        .extra
        .insert(PLUGIN_CARD_UNBOUND_KEY.into(), "true".into());
    assert_eq!(run(&source, &mut page).await, 2);
    assert!(!page[0].extra.contains_key(PLUGIN_CARD_UNBOUND_KEY));
    assert!(unbound(&page[1]));
    // An entity no card is bound to: its actions are refused too, so the card is read-only.
    assert!(unbound(&page[2]));
    assert!(!page[3].extra.contains_key(PLUGIN_CARD_UNBOUND_KEY));
    // One lookup for the page, naming the APP_CARD plugin cards only; the rules once.
    assert_eq!(source.lookups.load(Ordering::SeqCst), 1);
    assert_eq!(
        source.asked.lock().unwrap()[0],
        vec!["m-bound", "m-copy", "m-unknown"]
    );
    assert_eq!(source.rule_reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_binding_of_another_plugin_does_not_make_a_card_bound() {
    let mut source = Source::with_rules(poll_manifest());
    source
        .bound
        .insert(("m-copy".into(), "example.other".into()));
    let mut page = vec![card("m-copy", "example.poll", "poll-1")];
    assert_eq!(run(&source, &mut page).await, 1);
    assert!(unbound(&page[0]));
}

#[tokio::test]
async fn cards_naming_no_entity_stay_interactive() {
    let source = Source::with_rules(entityless_manifest());
    let mut page = vec![card("m-1", "example.poll", "poll-1")];
    assert_eq!(run(&source, &mut page).await, 0);
    assert!(!page[0].extra.contains_key(PLUGIN_CARD_UNBOUND_KEY));
    // Without a projection (plugin uninstalled) nothing says the card has an entity either.
    let source = Source::default();
    let mut page = vec![card("m-1", "example.poll", "poll-1")];
    assert_eq!(run(&source, &mut page).await, 0);
    assert!(!page[0].extra.contains_key(PLUGIN_CARD_UNBOUND_KEY));
}

#[tokio::test]
async fn custom_plugin_cards_are_always_copies_without_a_lookup() {
    let source = Source::with_rules(poll_manifest());
    let mut page = vec![
        custom("m-1", PLUGIN_CARD_TYPE),
        custom("m-2", " Flare.Plugin.Message.V1 "),
        custom("m-3", "red_packet"),
    ];
    assert_eq!(run(&source, &mut page).await, 2);
    assert!(unbound(&page[0]));
    assert!(unbound(&page[1]));
    assert!(!page[2].extra.contains_key(PLUGIN_CARD_UNBOUND_KEY));
    assert_eq!(source.lookups.load(Ordering::SeqCst), 0);
    assert_eq!(source.rule_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn pages_without_unbound_app_cards_read_nothing_more() {
    // No plugin card: no lookup at all.
    let source = Source::with_rules(poll_manifest());
    let mut page = vec![text("m-1"), text("m-2")];
    assert_eq!(run(&source, &mut page).await, 0);
    assert_eq!(source.lookups.load(Ordering::SeqCst), 0);
    // Every card bound: the projection is never read.
    let source = Source::with_rules(poll_manifest()).bind("m-1").bind("m-2");
    let mut page = vec![
        card("m-1", "example.poll", "poll-1"),
        card("m-2", "example.poll", "poll-2"),
    ];
    assert_eq!(run(&source, &mut page).await, 0);
    assert_eq!(source.lookups.load(Ordering::SeqCst), 1);
    assert_eq!(source.rule_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn invalid_cards_are_no_cards_and_failures_never_fail_the_read() {
    // app_id is not the envelope's plugin: clients do not render it as a card.
    let source = Source::with_rules(poll_manifest());
    let mut page = vec![card("m-1", "example.other", "poll-1")];
    assert_eq!(run(&source, &mut page).await, 0);
    assert_eq!(source.lookups.load(Ordering::SeqCst), 0);
    // A lookup failure leaves the APP_CARD cards unmarked; a Custom card is still marked.
    let source = Source {
        failing: true,
        ..Source::with_rules(poll_manifest())
    };
    let mut page = vec![
        card("m-1", "example.poll", "poll-1"),
        custom("m-2", PLUGIN_CARD_TYPE),
    ];
    assert_eq!(run(&source, &mut page).await, 1);
    assert!(!page[0].extra.contains_key(PLUGIN_CARD_UNBOUND_KEY));
    assert!(unbound(&page[1]));
}

#[tokio::test]
async fn wire_messages_inside_events_are_marked_the_same_way() {
    let source = Source::with_rules(poll_manifest()).bind("m-bound");
    let mut wire = [
        flare_im_contracts::message::message_to_proto(&card("m-bound", "example.poll", "poll-1")),
        flare_im_contracts::message::message_to_proto(&card("m-copy", "example.poll", "poll-1")),
    ];
    wire[0]
        .attributes
        .insert(PLUGIN_CARD_UNBOUND_KEY.into(), "true".into());
    let mut refs: Vec<&mut dyn PluginCardCarrier> = wire
        .iter_mut()
        .map(|m| m as &mut dyn PluginCardCarrier)
        .collect();
    let now = chrono::Utc::now().timestamp_millis();
    assert_eq!(
        mark_plugin_card_copies(&source, "app-a", &mut refs, now).await,
        1
    );
    assert!(!wire[0].attributes.contains_key(PLUGIN_CARD_UNBOUND_KEY));
    assert_eq!(
        wire[1]
            .attributes
            .get(PLUGIN_CARD_UNBOUND_KEY)
            .map(String::as_str),
        Some("true")
    );
}

#[test]
fn rules_follow_the_writer_binding_rule() {
    let now = chrono::Utc::now().timestamp_millis();
    let PluginCard::Posted(card_envelope) =
        plugin_card(card("m-1", "example.poll", "poll-1").content.as_ref()).unwrap()
    else {
        panic!("an APP_CARD plugin card");
    };
    let rules = EntityRules::from_snapshot(&snapshot("app-a", now, poll_manifest()), now);
    assert_eq!(
        rules.entity_of(&card_envelope, now).as_deref(),
        Some("poll-1")
    );
    assert_eq!(rules.entity_of(&card_envelope, now + 300_000), None);
    for mode in 0..3 {
        let mut snapshot = snapshot("app-a", now, poll_manifest());
        match mode {
            0 => snapshot.plugins[0].grant.enabled = false,
            1 => snapshot.plugins[0].grant.revoked = true,
            _ => snapshot.plugins[0].grant.valid_until_ms = now,
        }
        assert_eq!(
            EntityRules::from_snapshot(&snapshot, now).entity_of(&card_envelope, now),
            None,
            "mode {mode}"
        );
    }
}
