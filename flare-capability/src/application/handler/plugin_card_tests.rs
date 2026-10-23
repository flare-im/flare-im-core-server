use super::*;
use crate::application::handler::plugin_state::tests::{MemoryBindings, POLL_MANIFEST};
use crate::domain::repository::entity_lifecycle::{LifecycleOutcome, LifecycleTask};
use crate::domain::repository::plugin_card::{CardRoute, PostedCard};
use std::sync::Mutex;

/// One outbox row: (tenant, plugin, contribution, entity, reason, server message).
pub(crate) type OutboxRow = (String, String, String, String, String, Option<String>);

/// Outbox rows in memory: confirmed rows, and unconfirmed `post_failed` rows with their delay.
#[derive(Default)]
pub(crate) struct MemoryOutbox {
    pub rows: Mutex<Vec<OutboxRow>>,
    pub unconfirmed: Mutex<Vec<(OutboxRow, Duration)>>,
    pub fail_reads: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl EntityLifecycleOutbox for MemoryOutbox {
    async fn enqueue(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        contribution_id: &str,
        entity: &str,
        reason: &str,
        server_msg_id: Option<&str>,
    ) -> flare_server_core::error::Result<()> {
        let mut rows = self.rows.lock().unwrap();
        // A confirmed row replaces an unconfirmed one of the entity.
        self.unconfirmed.lock().unwrap().retain(|(r, _)| {
            !(r.0 == tenant_id && r.1 == plugin_id && r.3 == entity && r.4 == reason)
        });
        if !rows
            .iter()
            .any(|r| r.0 == tenant_id && r.1 == plugin_id && r.3 == entity && r.4 == reason)
        {
            rows.push((
                tenant_id.into(),
                plugin_id.into(),
                contribution_id.into(),
                entity.into(),
                reason.into(),
                server_msg_id.map(Into::into),
            ));
        }
        Ok(())
    }
    async fn enqueue_unconfirmed(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        contribution_id: &str,
        entity: &str,
        verify_after: Duration,
    ) -> flare_server_core::error::Result<()> {
        let same = |r: &OutboxRow| {
            r.0 == tenant_id && r.1 == plugin_id && r.3 == entity && r.4 == "post_failed"
        };
        let mut unconfirmed = self.unconfirmed.lock().unwrap();
        if !self.rows.lock().unwrap().iter().any(same) && !unconfirmed.iter().any(|(r, _)| same(r))
        {
            unconfirmed.push((
                (
                    tenant_id.into(),
                    plugin_id.into(),
                    contribution_id.into(),
                    entity.into(),
                    "post_failed".into(),
                    None,
                ),
                verify_after,
            ));
        }
        Ok(())
    }
    async fn post_failed(
        &self,
        tenant_id: &str,
        plugin_id: &str,
        entity: &str,
    ) -> flare_server_core::error::Result<bool> {
        if self.fail_reads.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(flare_server_core::error::FlareError::system("injected"));
        }
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.0 == tenant_id && r.1 == plugin_id && r.3 == entity && r.4 == "post_failed"))
    }
    async fn claim_due(
        &self,
        _limit: u32,
        _lease: Duration,
    ) -> flare_server_core::error::Result<Vec<LifecycleTask>> {
        Ok(vec![])
    }
    async fn complete(
        &self,
        _id: i64,
        _outcome: LifecycleOutcome,
        _note: Option<&str>,
    ) -> flare_server_core::error::Result<()> {
        Ok(())
    }
    async fn retry_after(
        &self,
        _id: i64,
        _delay: Duration,
        _error: &str,
    ) -> flare_server_core::error::Result<()> {
        Ok(())
    }
}

/// Ingest and Conversation in memory: answers from a script, records every post.
pub(crate) struct ScriptedDelivery {
    pub route: Mutex<std::result::Result<CardRoute, CardPostError>>,
    /// Answers of successive posts; the last one repeats.
    pub answers: Mutex<Vec<std::result::Result<PostedCard, CardPostError>>>,
    pub posts: Mutex<Vec<(String, Message)>>,
}
impl ScriptedDelivery {
    pub(crate) fn accepting(server_msg_id: &str) -> Self {
        Self {
            route: Mutex::new(Ok(CardRoute {
                conversation_type: flare_proto::common::ConversationType::Group as i32,
                channel_id: "group-a".into(),
            })),
            answers: Mutex::new(vec![Ok(PostedCard {
                server_msg_id: server_msg_id.into(),
            })]),
            posts: Mutex::default(),
        }
    }
    pub(crate) fn answering(answers: Vec<std::result::Result<PostedCard, CardPostError>>) -> Self {
        let delivery = Self::accepting("unused");
        *delivery.answers.lock().unwrap() = answers;
        delivery
    }
}
#[async_trait::async_trait]
impl PluginCardDelivery for ScriptedDelivery {
    async fn route(
        &self,
        _tenant_id: &str,
        _user_id: &str,
        _conversation_id: &str,
        _request_id: &str,
    ) -> std::result::Result<CardRoute, CardPostError> {
        self.route.lock().unwrap().clone()
    }
    async fn post(
        &self,
        tenant_id: &str,
        message: Message,
    ) -> std::result::Result<PostedCard, CardPostError> {
        self.posts.lock().unwrap().push((tenant_id.into(), message));
        let mut answers = self.answers.lock().unwrap();
        if answers.len() > 1 {
            answers.remove(0)
        } else {
            answers[0].clone()
        }
    }
}

fn manifest() -> PluginManifest {
    PluginManifest::from_json(POLL_MANIFEST.as_bytes()).unwrap()
}

fn envelope(poll_id: &str) -> Value {
    json!({
        "pluginId": "example.poll",
        "contributionId": "example.poll.card",
        "schemaVersion": 1,
        "summary": "[Poll] Lunch?",
        "payload": {"pollId": poll_id, "question": "Lunch?"}
    })
}

struct Fixture {
    delivery: Arc<ScriptedDelivery>,
    bindings: Arc<MemoryBindings>,
    outbox: Arc<MemoryOutbox>,
    poster: PluginCardPoster,
}

fn fixture(delivery: ScriptedDelivery) -> Fixture {
    let delivery = Arc::new(delivery);
    let bindings = Arc::new(MemoryBindings::default());
    let outbox = Arc::new(MemoryOutbox::default());
    let poster = PluginCardPoster::new(
        Some(delivery.clone()),
        Some(bindings.clone()),
        Some(outbox.clone()),
    )
    .without_backoff();
    Fixture {
        delivery,
        bindings,
        outbox,
        poster,
    }
}

async fn post(fixture: &Fixture, input: &Value, data: Value) -> Result<Value> {
    let manifest = manifest();
    fixture
        .poster
        .post_result(
            CardPostCall {
                tenant_id: "app-a",
                user_id: "u1",
                conversation_id: Some("2-group-a"),
                request_id: "r1",
                input,
                manifest: &manifest,
            },
            data,
        )
        .await
}

fn not_posted(result: &Result<Value>) -> bool {
    matches!(result, Err(error) if error.plugin_code() == Some(PLUGIN_CARD_NOT_POSTED))
}

#[tokio::test]
async fn the_platform_posts_the_card_as_the_ticket_user_and_answers_posted() {
    let f = fixture(ScriptedDelivery::accepting("m-1"));
    let data = json!({"pollId": "poll-1", "message": envelope("poll-1"), "sharedState": {"entity": "poll-1", "version": 1, "values": {}}});
    let input = json!({"question": "Lunch?", "commandId": "cmd-1"});
    let out = post(&f, &input, data).await.unwrap();
    assert!(
        out.get("message").is_none(),
        "the envelope never reaches the client"
    );
    assert_eq!(out["pollId"], "poll-1");
    assert_eq!(out["sharedState"]["version"], 1);
    let client_msg_id =
        card_client_msg_id("app-a", "example.poll", "u1", "2-group-a", "r1", &input);
    assert_eq!(
        out["posted"],
        json!({"conversationId": "2-group-a", "serverMsgId": "m-1", "clientMsgId": client_msg_id})
    );
    let posts = f.delivery.posts.lock().unwrap();
    let (tenant, message) = &posts[0];
    assert_eq!(tenant, "app-a");
    assert_eq!(
        (
            message.sender_id.as_str(),
            message.conversation_id.as_str(),
            message.channel_id.as_str(),
            message.client_msg_id.as_str(),
        ),
        ("u1", "2-group-a", "group-a", client_msg_id.as_str())
    );
    assert_eq!(message.source, MessageSource::User as i32);
    assert_eq!(message.message_type, MessageType::AppCard as i32);
    let Some(Content::AppCard(card)) = message.content.as_ref().unwrap().content.as_ref() else {
        panic!("an APP_CARD");
    };
    assert_eq!(
        (
            card.app_id.as_str(),
            card.card_type.as_str(),
            card.payload_schema.as_str()
        ),
        ("example.poll", PLUGIN_CARD_TYPE, PLUGIN_CARD_PAYLOAD_SCHEMA)
    );
    let posted: Value = serde_json::from_slice(&card.payload).unwrap();
    assert_eq!(
        posted["displayName"], "Example Poll",
        "the platform writes the plugin's name"
    );
    assert_eq!(posted["payload"]["pollId"], "poll-1");
    // The entity is bound to the posted card at once.
    let rows = f.bindings.rows.lock().unwrap();
    let bound = &rows[&(
        String::from("app-a"),
        String::from("example.poll"),
        String::from("poll-1"),
    )];
    assert_eq!(bound[0].server_msg_id, "m-1");
    assert_eq!(bound[0].conversation_id, "2-group-a");
    assert!(f.outbox.rows.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_result_without_a_message_is_untouched() {
    let f = fixture(ScriptedDelivery::accepting("m-1"));
    let data = json!({"voters": []});
    assert_eq!(post(&f, &json!({}), data.clone()).await.unwrap(), data);
    assert!(f.delivery.posts.lock().unwrap().is_empty());
}

#[test]
fn client_message_ids_follow_the_command_and_nothing_else() {
    let a = card_client_msg_id(
        "app-a",
        "example.poll",
        "u1",
        "2-g",
        "r1",
        &json!({"commandId": "c-1"}),
    );
    // A retry of the same command (another request id) posts the same message.
    assert_eq!(
        a,
        card_client_msg_id(
            "app-a",
            "example.poll",
            "u1",
            "2-g",
            "r2",
            &json!({"commandId": "c-1"})
        )
    );
    for other in [
        card_client_msg_id(
            "app-b",
            "example.poll",
            "u1",
            "2-g",
            "r1",
            &json!({"commandId": "c-1"}),
        ),
        card_client_msg_id(
            "app-a",
            "example.other",
            "u1",
            "2-g",
            "r1",
            &json!({"commandId": "c-1"}),
        ),
        card_client_msg_id(
            "app-a",
            "example.poll",
            "u2",
            "2-g",
            "r1",
            &json!({"commandId": "c-1"}),
        ),
        card_client_msg_id(
            "app-a",
            "example.poll",
            "u1",
            "2-h",
            "r1",
            &json!({"commandId": "c-1"}),
        ),
        card_client_msg_id(
            "app-a",
            "example.poll",
            "u1",
            "2-g",
            "r1",
            &json!({"commandId": "c-2"}),
        ),
    ] {
        assert_ne!(a, other);
    }
    // Without a valid command id the request id keys it.
    let by_request = card_client_msg_id("app-a", "example.poll", "u1", "2-g", "r1", &json!({}));
    assert_eq!(
        by_request,
        card_client_msg_id(
            "app-a",
            "example.poll",
            "u1",
            "2-g",
            "r1",
            &json!({"commandId": "bad id"})
        )
    );
    assert_ne!(
        by_request,
        card_client_msg_id("app-a", "example.poll", "u1", "2-g", "r2", &json!({}))
    );
    assert!(a.starts_with("pcard-") && a.len() == 38);
}

#[tokio::test]
async fn availability_failures_retry_with_the_same_client_message_id() {
    let f = fixture(ScriptedDelivery::answering(vec![
        Err(CardPostError::Unavailable("ingest down".into())),
        Ok(PostedCard {
            server_msg_id: "m-2".into(),
        }),
    ]));
    let out = post(&f, &json!({}), json!({"message": envelope("poll-1")}))
        .await
        .unwrap();
    assert_eq!(out["posted"]["serverMsgId"], "m-2");
    let posts = f.delivery.posts.lock().unwrap();
    assert_eq!(posts.len(), 2);
    assert_eq!(posts[0].1.client_msg_id, posts[1].1.client_msg_id);
    assert_eq!(posts[0].1.content, posts[1].1.content);
}

#[tokio::test]
async fn a_card_given_up_tells_the_plugin_post_failed_and_answers_not_posted() {
    // Refused once (a muted sender): no retry.
    let f = fixture(ScriptedDelivery::answering(vec![Err(
        CardPostError::Refused("SOCIAL_MUTED".into()),
    )]));
    let result = post(&f, &json!({}), json!({"message": envelope("poll-1")})).await;
    assert!(not_posted(&result));
    assert_eq!(f.delivery.posts.lock().unwrap().len(), 1);
    assert_eq!(
        f.outbox.rows.lock().unwrap().clone(),
        vec![(
            "app-a".to_string(),
            "example.poll".to_string(),
            "example.poll.card".to_string(),
            "poll-1".to_string(),
            "post_failed".to_string(),
            None
        )]
    );
    assert!(f.bindings.rows.lock().unwrap().is_empty());
    // A replay of the same result posts nothing: the plugin rolled the entity back.
    *f.delivery.answers.lock().unwrap() = vec![Ok(PostedCard {
        server_msg_id: "m-9".into(),
    })];
    let result = post(&f, &json!({}), json!({"message": envelope("poll-1")})).await;
    assert!(not_posted(&result));
    assert_eq!(f.delivery.posts.lock().unwrap().len(), 1);

    // The conversation route unavailable on every attempt: nothing reached Ingest, so nothing
    // can be stored; given up.
    let f = fixture(ScriptedDelivery::accepting("m-1"));
    *f.delivery.route.lock().unwrap() = Err(CardPostError::Unavailable("down".into()));
    let result = post(&f, &json!({}), json!({"message": envelope("poll-2")})).await;
    assert!(not_posted(&result));
    assert!(f.delivery.posts.lock().unwrap().is_empty());
    assert_eq!(f.outbox.rows.lock().unwrap()[0].3, "poll-2");

    // The route is refused (not a member): nothing is sent.
    let f = fixture(ScriptedDelivery::accepting("m-1"));
    *f.delivery.route.lock().unwrap() = Err(CardPostError::Refused("not a member".into()));
    let result = post(&f, &json!({}), json!({"message": envelope("poll-3")})).await;
    assert!(not_posted(&result));
    assert!(f.delivery.posts.lock().unwrap().is_empty());
    assert_eq!(f.outbox.rows.lock().unwrap()[0].3, "poll-3");
}

#[tokio::test]
async fn invalid_envelopes_are_never_posted() {
    let mut other_plugin = envelope("poll-1");
    other_plugin["pluginId"] = json!("example.other");
    other_plugin["contributionId"] = json!("example.other.card");
    let mut unknown_contribution = envelope("poll-1");
    unknown_contribution["contributionId"] = json!("example.poll.missing");
    let mut not_message_content = envelope("poll-1");
    not_message_content["contributionId"] = json!("example.poll.create");
    let mut wrong_schema = envelope("poll-1");
    wrong_schema["schemaVersion"] = json!(2);
    let mut no_entity = envelope("poll-1");
    no_entity["payload"] = json!({"question": "Lunch?"});
    let mut too_large = envelope("poll-1");
    too_large["payload"]["question"] = json!("x".repeat(70_000));
    for (case, raw) in [
        ("another plugin", other_plugin),
        ("unknown contribution", unknown_contribution),
        ("not message-content", not_message_content),
        ("schema version", wrong_schema),
        ("no entity at entityPath", no_entity),
        ("over the size limit", too_large),
        ("not an envelope", json!("text")),
    ] {
        let f = fixture(ScriptedDelivery::accepting("m-1"));
        let result = post(&f, &json!({}), json!({ "message": raw })).await;
        assert!(not_posted(&result), "{case}");
        assert!(f.delivery.posts.lock().unwrap().is_empty(), "{case}");
    }
    // An invalid envelope that still names a valid entity of a declared card tells the plugin.
    let f = fixture(ScriptedDelivery::accepting("m-1"));
    let mut invalid = envelope("poll-7");
    invalid["summary"] = json!("bad\u{0007}summary");
    assert!(not_posted(
        &post(&f, &json!({}), json!({ "message": invalid })).await
    ));
    assert_eq!(f.outbox.rows.lock().unwrap()[0].3, "poll-7");
}

#[tokio::test]
async fn without_posting_configured_every_card_is_given_up() {
    let outbox = Arc::new(MemoryOutbox::default());
    let poster = PluginCardPoster::new(None, None, Some(outbox.clone()));
    let manifest = manifest();
    let result = poster
        .post_result(
            CardPostCall {
                tenant_id: "app-a",
                user_id: "u1",
                conversation_id: Some("2-group-a"),
                request_id: "r1",
                input: &json!({}),
                manifest: &manifest,
            },
            json!({"message": envelope("poll-1")}),
        )
        .await;
    assert!(not_posted(&result));
    assert_eq!(outbox.rows.lock().unwrap()[0].4, "post_failed");
}

#[test]
fn display_names_are_cut_at_a_character_boundary() {
    assert_eq!(envelope_display_name("  Poll \n"), Some("Poll".into()));
    assert_eq!(envelope_display_name("   "), None);
    let long = "投".repeat(40);
    let name = envelope_display_name(&long).unwrap();
    assert!(name.len() <= MAX_PLUGIN_DISPLAY_NAME_BYTES);
    assert_eq!(name.chars().count(), MAX_PLUGIN_DISPLAY_NAME_BYTES / 3);
}

fn outcome_unknown(result: &Result<Value>) -> bool {
    matches!(result, Err(CapabilityError::Timeout(_)))
}

/// Ingest unavailable on every attempt leaves it open whether the card was stored (the
/// first send may still be in flight). Nothing is rolled back now: the call answers an open
/// outcome, an unconfirmed post_failed waits [`POST_VERIFY_DELAY`], and the client's retry with the
/// same command still posts (Ingest replays a stored card) and confirms nothing.
#[tokio::test]
async fn an_unknown_post_outcome_rolls_nothing_back_and_leaves_the_retry_open() {
    let f = fixture(ScriptedDelivery::answering(vec![Err(
        CardPostError::Unavailable("deadline exceeded".into()),
    )]));
    let input = json!({"commandId": "c-1"});
    let result = post(&f, &input, json!({"message": envelope("poll-1")})).await;
    assert!(outcome_unknown(&result), "{result:?}");
    assert_eq!(
        f.delivery.posts.lock().unwrap().len(),
        MAX_POST_ATTEMPTS as usize
    );
    assert!(
        f.outbox.rows.lock().unwrap().is_empty(),
        "nothing confirmed"
    );
    let unconfirmed = f.outbox.unconfirmed.lock().unwrap().clone();
    assert_eq!(unconfirmed.len(), 1);
    assert_eq!(unconfirmed[0].0.3, "poll-1");
    assert_eq!(unconfirmed[0].1, POST_VERIFY_DELAY);
    assert!(POST_VERIFY_DELAY > Duration::from_secs(180));

    // The retry: Ingest answers the stored card; it is bound and posted.
    *f.delivery.answers.lock().unwrap() = vec![Ok(PostedCard {
        server_msg_id: "m-1".into(),
    })];
    let out = post(&f, &input, json!({"message": envelope("poll-1")}))
        .await
        .unwrap();
    assert_eq!(out["posted"]["serverMsgId"], "m-1");
    let posts = f.delivery.posts.lock().unwrap().clone();
    assert_eq!(posts[0].1.client_msg_id, posts[3].1.client_msg_id);

    // An unknown outcome followed by a refusal is still unknown: the first send may be stored.
    let f = fixture(ScriptedDelivery::answering(vec![
        Err(CardPostError::Unavailable("in flight".into())),
        Err(CardPostError::Refused("SOCIAL_MUTED".into())),
    ]));
    let result = post(&f, &json!({}), json!({"message": envelope("poll-3")})).await;
    assert!(outcome_unknown(&result));
    assert!(f.outbox.rows.lock().unwrap().is_empty());
    assert_eq!(f.outbox.unconfirmed.lock().unwrap().len(), 1);
}

/// A replayed create of an entity that already
/// has a card posts nothing and rolls nothing back, whatever conversation the replay names; it
/// answers the bound card. A recalled card's entity is refused.
#[tokio::test]
async fn an_entity_with_a_card_is_never_posted_again_nor_rolled_back() {
    let f = fixture(ScriptedDelivery::answering(vec![Err(
        CardPostError::Refused("NOT_CONVERSATION_MEMBER".into()),
    )]));
    f.bindings
        .bind("app-a", "example.poll", "poll-1", "2-group-a", "m-1");
    let input = json!({"commandId": "c-1"});
    let manifest = manifest();
    for conversation in [Some("2-group-a"), Some("2-other"), None] {
        let out = f
            .poster
            .post_result(
                CardPostCall {
                    tenant_id: "app-a",
                    user_id: "u1",
                    conversation_id: conversation,
                    request_id: "r2",
                    input: &input,
                    manifest: &manifest,
                },
                json!({"pollId": "poll-1", "message": envelope("poll-1")}),
            )
            .await;
        let out = out.unwrap_or_else(|error| panic!("{conversation:?}: {error}"));
        assert_eq!(
            out["posted"],
            json!({
                "conversationId": "2-group-a",
                "serverMsgId": "m-1",
                "clientMsgId": card_client_msg_id(
                    "app-a", "example.poll", "u1", "2-group-a", "r2", &input
                ),
            }),
            "{conversation:?}"
        );
        assert!(out.get("message").is_none());
    }
    assert!(
        f.delivery.posts.lock().unwrap().is_empty(),
        "nothing posted"
    );
    assert!(
        f.outbox.rows.lock().unwrap().is_empty(),
        "nothing rolled back"
    );
    assert!(f.outbox.unconfirmed.lock().unwrap().is_empty());

    // Even a replay whose envelope is now invalid does not roll the bound entity back.
    let mut broken = envelope("poll-1");
    broken["summary"] = json!("bad\u{0007}summary");
    assert!(not_posted(
        &post(&f, &input, json!({ "message": broken })).await
    ));
    assert!(f.outbox.rows.lock().unwrap().is_empty());

    f.bindings.recall("m-1");
    let result = post(&f, &input, json!({"message": envelope("poll-1")})).await;
    assert!(
        matches!(&result, Err(error) if error.plugin_code() == Some(PLUGIN_MESSAGE_RECALLED)),
        "{result:?}"
    );
    assert!(f.delivery.posts.lock().unwrap().is_empty());
    assert!(f.outbox.rows.lock().unwrap().is_empty());

    // Bindings unreadable: an open outcome, nothing posted, nothing rolled back.
    f.bindings
        .fail
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let result = post(&f, &input, json!({"message": envelope("poll-2")})).await;
    assert!(outcome_unknown(&result));
    assert!(f.delivery.posts.lock().unwrap().is_empty());
    assert!(f.outbox.rows.lock().unwrap().is_empty());
}

/// The outbox unreadable means the entity may have been rolled back: no card is posted
/// unchecked; the call answers an open outcome.
#[tokio::test]
async fn an_unreadable_outbox_posts_no_card() {
    let f = fixture(ScriptedDelivery::accepting("m-1"));
    f.outbox
        .fail_reads
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let result = post(&f, &json!({}), json!({"message": envelope("poll-1")})).await;
    assert!(outcome_unknown(&result));
    assert!(f.delivery.posts.lock().unwrap().is_empty());
    assert!(f.bindings.rows.lock().unwrap().is_empty());
}
