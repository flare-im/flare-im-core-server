//! Plugin shared state end to end on PostgreSQL, every hop the production code except the two MQ
//! and gRPC transports: Storage Writer stores the card and binds its entity, Capability's publisher
//! checks and claims versions on the bindings, Orchestrator's `EventHandler` validates, sequences
//! and fans the event out, Storage Writer applies it (side table + `events`), Storage Reader serves
//! history (extra) and offline sync (events).
//!
//! Needs `FLARE_PLUGIN_STATE_TEST_DATABASE_URL`: a disposable database whose name starts with
//! `flare_c2_`, initialized with `deploy/init.sql`. Ignored by default (`cargo test -- --ignored`).
use super::plugin_state::tests::poll_snapshot;
use super::plugin_state::{PluginStateMetrics, PluginStatePublisher, PublishScope, PublishSource};
use crate::domain::repository::plugin_state::PluginStateEventSink;
use crate::infrastructure::adapters::plugin_state_events::{plugin_state_event, system_context};
use crate::infrastructure::persistence::postgres_plugin_entity_bindings::PostgresPluginEntityBindings;
use flare_im_capability_core::plugin::resource::EntityStatePublishOutcome;
use flare_im_capability_core::plugin::state::SharedState;
use flare_im_contracts::Ctx;
use flare_im_message_pipeline::{ConversationType, PushRepository, RecipientRepository};
use flare_orchestrator::application::handlers::EventHandler;
use flare_orchestrator::domain::service::EventDomainService;
use flare_orchestrator::domain::service::event_domain_service::SequenceAllocatorPort;
use flare_proto::common::{Event, EventType, Message, PluginStateEvent, PushEnvelope};
use flare_server_core::error::Result;
use flare_storage_reader::MessageStorage;
use flare_storage_reader::infrastructure::persistence::optimized_postgres_store::OptimizedPostgresMessageStorageImpl;
use flare_storage_reader::infrastructure::persistence::postgres_base::PostgresBaseStorage;
use flare_storage_writer::domain::model::Message as StoredMessage;
use flare_storage_writer::domain::repository::{
    ArchiveStoreRepository, PluginEntityBindingRepository,
};
use flare_storage_writer::domain::service::EventApplicationService;
use flare_storage_writer::infrastructure::persistence::repository::event_stream::PostgresEventStreamStore;
use flare_storage_writer::infrastructure::persistence::repository::postgres_store::PostgresMessageStore;
use serde_json::json;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const TENANT: &str = "app-a";
const GROUP: &str = "2C2PLUGINSTATETEST01";
const CARD: &str = "c2-poll-card-1";
const MEMBERS: [&str; 3] = ["alice", "bob", "carol"];

type Writer = EventApplicationService<PostgresMessageStore, PostgresEventStreamStore>;

/// Orchestrator's MQ boundary: records the fan-out and hands the event to Storage Writer, as the
/// storage and push topics would.
struct Topics {
    writer: Arc<Writer>,
    fanned_out: Mutex<Vec<(Event, Vec<String>)>>,
}
impl PushRepository for Topics {
    async fn publish_message(
        &self,
        _: &Ctx,
        _: Message,
        _: Vec<String>,
        _: String,
        _: bool,
    ) -> Result<()> {
        unreachable!()
    }
    async fn publish_event(
        &self,
        ctx: &Ctx,
        event: Event,
        recipients: Vec<String>,
        _conversation_id: String,
    ) -> Result<()> {
        self.fanned_out
            .lock()
            .unwrap()
            .push((event.clone(), recipients));
        let mut stored = flare_storage_writer::convert::event_from_proto(&event);
        flare_storage_writer::convert::enrich_operation_event_from_ctx(&mut stored, ctx)
            .expect("tenant header");
        self.writer.process_event(ctx, &stored).await
    }
    async fn persistence_only_message(&self, _: &Ctx, _: Message, _: String) -> Result<()> {
        unreachable!()
    }
    async fn persistence_only_event(&self, _: &Ctx, _: Event, _: String) -> Result<()> {
        unreachable!()
    }
    async fn push_only_message(
        &self,
        _: &Ctx,
        _: Message,
        _: Vec<String>,
        _: String,
    ) -> Result<()> {
        unreachable!()
    }
    async fn push_only_event(&self, _: &Ctx, _: Event, _: Vec<String>, _: String) -> Result<()> {
        unreachable!()
    }
    async fn publish_push_envelope(&self, _: &Ctx, _: PushEnvelope) -> Result<()> {
        unreachable!()
    }
}

/// The card is seq 1; events follow.
struct Seq(AtomicU64);
impl SequenceAllocatorPort for Seq {
    async fn allocate_seq(&self, _: &str, _: &str) -> Result<u64> {
        Ok(self.0.fetch_add(1, Ordering::SeqCst) + 1)
    }
}

struct Members;
impl RecipientRepository for Members {
    fn get_message_recipients<'a>(
        &'a self,
        _: &'a Ctx,
        _: &'a str,
        _: ConversationType,
        _: Option<&'a str>,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>> {
        unreachable!()
    }
    fn get_event_recipients<'a>(
        &'a self,
        _: &'a Ctx,
        _: &'a str,
        _: &'a str,
        _: EventType,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>> {
        unreachable!()
    }
    fn get_conversation_members<'a>(
        &'a self,
        _: &'a Ctx,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>> {
        Box::pin(async { Ok(MEMBERS.map(String::from).to_vec()) })
    }
    fn get_conversation_member_count<'a>(
        &'a self,
        _: &'a Ctx,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<usize>> + Send + 'a>> {
        Box::pin(async { Ok(3) })
    }
}

/// Capability's internal event entry, in process instead of over gRPC: same event, same context.
struct InProcessOrchestrator(EventHandler<Topics, Seq>);
#[async_trait::async_trait]
impl PluginStateEventSink for InProcessOrchestrator {
    async fn publish(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        event: PluginStateEvent,
    ) -> Result<()> {
        self.0
            .handle_event(
                &system_context(tenant_id),
                plugin_state_event(conversation_id, event),
            )
            .await
    }
}

fn ctx() -> Ctx {
    Arc::new(flare_server_core::Context::with_request_id("pg-test").with_tenant_id(TENANT))
}

fn vote(version: u64, counts: [u64; 2], voters: u64) -> SharedState {
    serde_json::from_value(json!({
        "entity": "poll-1",
        "version": version,
        "values": {"counts": counts, "voterCount": voters}
    }))
    .unwrap()
}

fn card() -> StoredMessage {
    let envelope = json!({
        "pluginId": "example.poll",
        "contributionId": "example.poll.card",
        "schemaVersion": 1,
        "summary": "[Poll] Lunch?",
        "payload": {"pollId": "poll-1", "question": "Lunch?", "options": ["Noodles", "Rice"], "counts": [0, 0]}
    });
    StoredMessage {
        server_id: CARD.into(),
        conversation_id: GROUP.into(),
        client_msg_id: format!("{CARD}-client"),
        sender_id: "alice".into(),
        conversation_seq: 1,
        conversation_type: 2,
        message_type: 14,
        content: Some(flare_proto::common::MessageContent {
            content: Some(flare_proto::common::message_content::Content::AppCard(
                flare_proto::common::AppCardContent {
                    app_id: "example.poll".into(),
                    card_type: "flare.plugin.message.v1".into(),
                    payload: serde_json::to_vec(&envelope).unwrap(),
                    ..Default::default()
                },
            )),
        }),
        status: 2,
        extra: [
            ("tenant_id".to_string(), TENANT.to_string()),
            // A sender's attempt to preset the shared layer never reaches readers.
            ("pluginSharedStateVersion".to_string(), "999".to_string()),
        ]
        .into_iter()
        .collect(),
        timestamp: Some(prost_types::Timestamp {
            seconds: chrono::Utc::now().timestamp(),
            nanos: 0,
        }),
        ..Default::default()
    }
}

async fn reset(pool: &sqlx::PgPool) {
    for table in [
        "DELETE FROM message_plugin_states WHERE tenant_id = $1",
        "DELETE FROM plugin_entity_bindings WHERE tenant_id = $1",
        "DELETE FROM events WHERE tenant_id = $1",
        "DELETE FROM message_operation_history WHERE tenant_id = $1",
        "DELETE FROM message_write_ledger WHERE tenant_id = $1",
        "DELETE FROM messages WHERE tenant_id = $1",
        "DELETE FROM plugin_authorization_snapshots WHERE app_id = $1",
    ] {
        sqlx::query(table).bind(TENANT).execute(pool).await.unwrap();
    }
}

async fn wait_for(mut done: impl FnMut() -> bool) {
    for _ in 0..200 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached");
}

#[tokio::test]
#[ignore = "needs FLARE_PLUGIN_STATE_TEST_DATABASE_URL (a disposable flare_c2_* database from deploy/init.sql)"]
async fn two_voters_publish_a_third_member_receives_and_offline_sync_converges() {
    let url = std::env::var("FLARE_PLUGIN_STATE_TEST_DATABASE_URL").expect("database url");
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(name.starts_with("flare_c2_"), "refusing database {name}");
    reset(&pool).await;

    // The tenant enabled the example poll (control plane projection).
    let snapshot = poll_snapshot("http://127.0.0.1:1".into());
    sqlx::query("INSERT INTO plugin_authorization_snapshots(app_id, version, snapshot, valid_until_ms) VALUES ($1, $2, $3, $4)")
        .bind(TENANT)
        .bind(snapshot.version as i64)
        .bind(serde_json::to_value(&snapshot).unwrap())
        .bind(snapshot.valid_until_ms)
        .execute(&pool)
        .await
        .unwrap();

    // Storage Writer stores Alice's card and binds its entity.
    let messages = Arc::new(PostgresMessageStore::from_pool(pool.clone()));
    messages.store_archive(&ctx(), &card()).await.unwrap();
    assert_eq!(
        messages
            .plugin_entity_bindings()
            .register(&ctx(), TENANT, &[card()])
            .await
            .unwrap(),
        1
    );
    let writer = Arc::new(Writer::new(
        Some(messages.clone()),
        Some(Arc::new(PostgresEventStreamStore::new(pool.clone()))),
    ));
    let topics = Arc::new(Topics {
        writer: writer.clone(),
        fanned_out: Mutex::new(Vec::new()),
    });
    let handler = EventHandler::new(Arc::new(EventDomainService::new(
        topics.clone(),
        Arc::new(Members),
        Arc::new(Seq(AtomicU64::new(1))),
        None,
    )));
    let publisher = Arc::new(PluginStatePublisher::with_window(
        Arc::new(PostgresPluginEntityBindings {
            pool: Arc::new(pool.clone()),
        }),
        Arc::new(InProcessOrchestrator(handler)),
        Duration::from_millis(200),
        Arc::new(PluginStateMetrics::new()),
    ));
    let in_group = PublishScope::Conversation(GROUP);
    let submit = |state| {
        let publisher = publisher.clone();
        async move {
            publisher
                .submit(
                    TENANT,
                    "example.poll",
                    in_group,
                    state,
                    PublishSource::Invocation,
                )
                .await
                .unwrap()
        }
    };
    let versions = || {
        topics
            .fanned_out
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(event, _)| match &event.payload {
                Some(flare_proto::common::event::Payload::PluginState(s)) => Some(s.version),
                _ => None,
            })
            .collect::<Vec<_>>()
    };

    // Alice votes, then Bob twice within the window: Alice's state goes out at once, Bob's two
    // coalesce into the highest.
    assert_eq!(
        submit(vote(1, [1, 0], 1)).await,
        EntityStatePublishOutcome::Published
    );
    wait_for(|| versions() == [1]).await;
    assert_eq!(
        submit(vote(2, [1, 1], 2)).await,
        EntityStatePublishOutcome::Published
    );
    assert_eq!(
        submit(vote(3, [1, 1], 2)).await,
        EntityStatePublishOutcome::Published
    );
    wait_for(|| versions() == [1, 3]).await;
    // Late and repeated publishes change nothing.
    assert_eq!(
        submit(vote(2, [1, 1], 2)).await,
        EntityStatePublishOutcome::Stale
    );
    assert_eq!(
        submit(vote(3, [1, 1], 2)).await,
        EntityStatePublishOutcome::Duplicate
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(versions(), [1, 3]);

    // A group event is a conversation broadcast: Carol, a member, receives it without a
    // per-member fan-out list; each event has its own seq after the card's.
    let fanned_out = topics.fanned_out.lock().unwrap().clone();
    for (event, recipients) in &fanned_out {
        assert_eq!(event.conversation_id, GROUP);
        assert!(recipients.is_empty(), "group events are broadcast");
    }
    assert_eq!(
        fanned_out
            .iter()
            .map(|(e, _)| e.conversation_seq)
            .collect::<Vec<_>>(),
        [2, 3]
    );

    // A delayed older event (another instance, a redelivery) reaches the writer after v3: it is
    // recorded at its seq, the card keeps v3.
    let mut late = fanned_out[0].0.clone();
    late.conversation_seq = 4;
    late.event_id = "late".into();
    topics
        .publish_event(&system_context(TENANT), late, vec![], GROUP.into())
        .await
        .unwrap();

    // Carol was offline. History: the card carries the latest shared layer; the sender's preset
    // version is gone.
    let reader = OptimizedPostgresMessageStorageImpl::new(
        PostgresBaseStorage::from_pool_and_cache(pool.clone(), None)
            .await
            .unwrap(),
        None,
        None,
    );
    let history = reader
        .query_messages_by_seq(&ctx(), GROUP, None, 0, None, 50, false)
        .await
        .unwrap();
    let stored = history
        .iter()
        .find(|m| m.server_id == CARD)
        .expect("card in history");
    assert_eq!(stored.extra["pluginSharedStateVersion"], "3");
    assert_eq!(stored.extra["pluginSharedEntity"], "poll-1");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&stored.extra["pluginSharedStateJson"]).unwrap(),
        json!({"counts": [1, 1], "voterCount": 2})
    );
    // Offline sync: the plugin state events (type 17) after the card, decoded.
    let events = reader
        .query_events(
            &ctx(),
            TENANT,
            GROUP,
            1,
            i64::MAX,
            50,
            vec![EventType::EventMessagePluginState as i32],
        )
        .await
        .unwrap();
    let synced: Vec<u64> = events
        .iter()
        .map(|event| {
            let event = flare_storage_reader::convert::event_to_proto(event).expect("event");
            assert_eq!(event.r#type, EventType::EventMessagePluginState as i32);
            let Some(flare_proto::common::event::Payload::PluginState(state)) = event.payload
            else {
                panic!("plugin state payload decodes");
            };
            assert_eq!(state.server_msg_id, CARD);
            state.version
        })
        .collect();
    assert_eq!(
        synced,
        [1, 3, 1],
        "every seq is in the stream; clients keep the highest"
    );
    let side: (i64, String) = sqlx::query_as(
        "SELECT version, values_json FROM message_plugin_states WHERE tenant_id = $1 AND server_msg_id = $2",
    )
    .bind(TENANT)
    .bind(CARD)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(side.0, 3);

    // The card is recalled: no more state, and the writer's binding flag says so.
    messages
        .update_message_fsm_state(&ctx(), TENANT, CARD, "RECALLED", None)
        .await
        .unwrap();
    assert!(
        publisher
            .submit(
                TENANT,
                "example.poll",
                in_group,
                vote(4, [2, 1], 3),
                PublishSource::Bridge
            )
            .await
            .is_err()
    );
    assert!(
        publisher
            .entity_recalled(TENANT, "example.poll", "poll-1")
            .await
            .unwrap()
    );
    reset(&pool).await;
}
