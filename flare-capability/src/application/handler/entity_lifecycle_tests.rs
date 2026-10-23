use super::*;
use crate::application::handler::plugin_state::tests::{
    MemoryBindings, POLL_MANIFEST, poll_snapshot,
};
use crate::application::handler::plugin_test_server::PluginServer;
use flare_im_capability_core::plugin::PluginManifest;
use flare_im_capability_core::plugin::invocation::PluginCallVerifier;
use flare_im_capability_core::plugin::runtime::{PluginRuntimeSnapshot, digest_bytes};
use serde_json::Value;
use std::sync::Mutex;

const SIGNING_SEED: [u8; 32] = [41; 32];

struct Projection(Mutex<Option<PluginRuntimeSnapshot>>);
#[async_trait::async_trait]
impl PluginAuthorizationReader for Projection {
    async fn plugin_snapshot(
        &self,
        tenant: &str,
    ) -> flare_server_core::error::Result<Option<PluginRuntimeSnapshot>> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .clone()
            .filter(|s| s.tenant_id == tenant))
    }
}

/// The example poll with an `unbound` operation declared on its card.
fn lifecycle_snapshot(authority: String) -> PluginRuntimeSnapshot {
    let mut raw: Value = serde_json::from_str(POLL_MANIFEST).unwrap();
    let mut unbound = raw["capabilities"][0]["operations"][0].clone();
    unbound["id"] = "unbound".into();
    raw["capabilities"][0]["operations"]
        .as_array_mut()
        .unwrap()
        .push(unbound);
    raw["clientExtensions"]["contributions"][0]["entityLifecycle"] =
        serde_json::json!({"unbound": "example.poll.unbound"});
    let raw = serde_json::to_string(&raw).unwrap();
    let manifest = PluginManifest::from_json(raw.as_bytes()).unwrap();
    let digest = digest_bytes(raw.as_bytes());
    let mut snapshot = poll_snapshot(authority);
    let projected = &mut snapshot.plugins[0];
    projected.manifest_json = raw;
    projected.grant.manifest_digest = digest.clone();
    projected.deployment.manifest_digest = digest;
    projected.grant.approved_operations = manifest.operation_ids();
    snapshot
}

fn task(id: i64, reason: &str) -> LifecycleTask {
    LifecycleTask {
        id,
        tenant_id: "app-a".into(),
        plugin_id: "example.poll".into(),
        contribution_id: "example.poll.card".into(),
        entity: "poll-1".into(),
        reason: reason.into(),
        server_msg_id: (reason == "recalled").then(|| "m-1".into()),
        attempts: 1,
        unconfirmed: false,
        created_at_ms: chrono::Utc::now().timestamp_millis(),
    }
}

struct Fixture {
    plugin: PluginServer,
    reader: Arc<Projection>,
    worker: EntityLifecycleWorker,
    outbox: Arc<Recorder>,
    bindings: Arc<MemoryBindings>,
}

/// Outbox rows handed to the worker and what it did with them.
#[derive(Default)]
struct Recorder {
    due: Mutex<Vec<LifecycleTask>>,
    completed: Mutex<Vec<(i64, LifecycleOutcome)>>,
    retried: Mutex<Vec<i64>>,
}
#[async_trait::async_trait]
impl EntityLifecycleOutbox for Recorder {
    async fn enqueue(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: Option<&str>,
    ) -> flare_server_core::error::Result<()> {
        Ok(())
    }
    async fn enqueue_unconfirmed(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: Duration,
    ) -> flare_server_core::error::Result<()> {
        Ok(())
    }
    async fn post_failed(
        &self,
        _: &str,
        _: &str,
        _: &str,
    ) -> flare_server_core::error::Result<bool> {
        Ok(false)
    }
    async fn claim_due(
        &self,
        _limit: u32,
        _lease: Duration,
    ) -> flare_server_core::error::Result<Vec<LifecycleTask>> {
        Ok(std::mem::take(&mut *self.due.lock().unwrap()))
    }
    async fn complete(
        &self,
        id: i64,
        outcome: LifecycleOutcome,
        _note: Option<&str>,
    ) -> flare_server_core::error::Result<()> {
        self.completed.lock().unwrap().push((id, outcome));
        Ok(())
    }
    async fn retry_after(
        &self,
        id: i64,
        _delay: Duration,
        _error: &str,
    ) -> flare_server_core::error::Result<()> {
        self.retried.lock().unwrap().push(id);
        Ok(())
    }
}

async fn fixture(
    respond: Arc<crate::application::handler::plugin_test_server::Respond>,
) -> Fixture {
    let plugin = PluginServer::start(respond).await;
    let reader = Arc::new(Projection(Mutex::new(Some(lifecycle_snapshot(
        plugin.authority.clone(),
    )))));
    let outbox = Arc::new(Recorder::default());
    let bindings = Arc::new(MemoryBindings::default());
    let worker = EntityLifecycleWorker::new(
        outbox.clone(),
        reader.clone(),
        Arc::new(PluginCallSigner::new("call".into(), SIGNING_SEED).unwrap()),
        Duration::from_secs(2),
    )
    .with_bindings(Some(bindings.clone()));
    Fixture {
        plugin,
        reader,
        worker,
        outbox,
        bindings,
    }
}

fn verifier() -> PluginCallVerifier {
    let signer = PluginCallSigner::new("call".into(), SIGNING_SEED).unwrap();
    let mut verifier = PluginCallVerifier::default();
    verifier.insert("call".into(), signer.public_key()).unwrap();
    verifier
}

#[tokio::test]
async fn a_recall_reaches_the_declared_unbound_operation_with_a_system_ticket() {
    let f = fixture(Arc::new(|_, _| Ok(serde_json::json!({})))).await;
    assert_eq!(
        f.worker.deliver(&task(7, "recalled")).await,
        Disposition::Done(LifecycleOutcome::Delivered, None)
    );
    let calls = f.plugin.calls();
    assert_eq!(calls.len(), 1);
    let call = &calls[0];
    assert_eq!(call.operation, "example.poll.unbound");
    assert_eq!(
        call.payload,
        serde_json::json!({"entity": "poll-1", "reason": "recalled", "serverMsgId": "m-1"})
    );
    assert!(
        !call.metadata.contains_key("user_id"),
        "no user on a system call"
    );
    let now = chrono::Utc::now().timestamp_millis();
    let claims = verifier().verify_system(&call.ticket, now).unwrap();
    assert_eq!(
        (
            claims.tenant_id.as_str(),
            claims.plugin_id.as_str(),
            claims.operation.as_str(),
            claims.entity.as_str(),
            claims.reason.as_str(),
            claims.request_id.as_str(),
            claims.deployment_id.as_str(),
            claims.grant_revision,
        ),
        (
            "app-a",
            "example.poll",
            "example.poll.unbound",
            "poll-1",
            "recalled",
            "lifecycle-7",
            "poll-1",
            4
        )
    );
    // The system ticket is no user or staff ticket.
    assert!(verifier().verify(&call.ticket, now).is_err());
    assert!(verifier().verify_staff(&call.ticket, now).is_err());

    // post_failed carries no message id.
    assert_eq!(
        f.worker.deliver(&task(8, "post_failed")).await,
        Disposition::Done(LifecycleOutcome::Delivered, None)
    );
    assert_eq!(
        f.plugin.calls()[1].payload,
        serde_json::json!({"entity": "poll-1", "reason": "post_failed"})
    );
}

#[tokio::test]
async fn a_plugin_answer_ends_the_row_and_only_open_outcomes_retry() {
    let f = fixture(Arc::new(|_, _| Err("POLL_NOT_FOUND".into()))).await;
    assert!(matches!(
        f.worker.deliver(&task(1, "recalled")).await,
        Disposition::Done(LifecycleOutcome::Delivered, Some(note)) if note.contains("POLL_NOT_FOUND")
    ));

    // A card without an unbound operation: nothing to deliver.
    *f.reader.0.lock().unwrap() = Some(poll_snapshot(f.plugin.authority.clone()));
    assert!(matches!(
        f.worker.deliver(&task(2, "recalled")).await,
        Disposition::Done(LifecycleOutcome::Skipped, _)
    ));

    // Disabled, or unreachable: retried later; past the retention window, abandoned.
    let mut disabled = lifecycle_snapshot(f.plugin.authority.clone());
    disabled.plugins[0].grant.enabled = false;
    *f.reader.0.lock().unwrap() = Some(disabled);
    assert!(matches!(
        f.worker.deliver(&task(3, "recalled")).await,
        Disposition::Retry(_, _)
    ));
    let mut old = task(4, "recalled");
    old.created_at_ms -= RETENTION.as_millis() as i64 + 1;
    assert!(matches!(
        f.worker.deliver(&old).await,
        Disposition::Done(LifecycleOutcome::Abandoned, _)
    ));
    *f.reader.0.lock().unwrap() = Some(lifecycle_snapshot("http://127.0.0.1:1".into()));
    assert!(matches!(
        f.worker.deliver(&task(5, "recalled")).await,
        Disposition::Retry(_, _)
    ));
}

#[tokio::test]
async fn a_round_ends_or_reschedules_every_claimed_row() {
    let f = fixture(Arc::new(|_, _| Ok(serde_json::json!({})))).await;
    let mut other_tenant = task(2, "recalled");
    other_tenant.tenant_id = "app-b".into();
    *f.outbox.due.lock().unwrap() = vec![task(1, "recalled"), other_tenant];
    assert_eq!(f.worker.deliver_due().await, 2);
    assert_eq!(
        f.outbox.completed.lock().unwrap().clone(),
        vec![(1, LifecycleOutcome::Delivered)]
    );
    assert_eq!(f.outbox.retried.lock().unwrap().clone(), vec![2]);
}

#[test]
fn backoff_doubles_up_to_an_hour() {
    assert_eq!(backoff(1), Duration::from_secs(5));
    assert_eq!(backoff(2), Duration::from_secs(10));
    assert_eq!(backoff(4), Duration::from_secs(40));
    assert_eq!(backoff(40), Duration::from_secs(3600));
}

/// A `post_failed` is never delivered for an entity whose card exists after all
/// (Storage Writer bound it when it stored it, or a retried call posted it), confirmed or not; a
/// recall is not affected. Without readable bindings the row waits.
#[tokio::test]
async fn a_post_failed_whose_card_was_stored_after_all_is_skipped() {
    let f = fixture(Arc::new(|_, _| Ok(serde_json::json!({})))).await;
    f.bindings
        .bind("app-a", "example.poll", "poll-1", "2-group", "m-1");
    for unconfirmed in [false, true] {
        let mut row = task(1, "post_failed");
        row.unconfirmed = unconfirmed;
        assert_eq!(
            f.worker.deliver(&row).await,
            Disposition::Done(
                LifecycleOutcome::Skipped,
                Some("the card was stored after all".into())
            ),
            "unconfirmed={unconfirmed}"
        );
    }
    assert!(f.plugin.calls().is_empty(), "nothing rolled back");
    // A recall of the bound card is delivered as before.
    assert_eq!(
        f.worker.deliver(&task(2, "recalled")).await,
        Disposition::Done(LifecycleOutcome::Delivered, None)
    );
    // An entity still without a card after the verification delay is rolled back.
    let mut unbound = task(3, "post_failed");
    unbound.entity = "poll-2".into();
    unbound.unconfirmed = true;
    assert_eq!(
        f.worker.deliver(&unbound).await,
        Disposition::Done(LifecycleOutcome::Delivered, None)
    );
    // Bindings unreadable: retried, not delivered blind.
    f.bindings
        .fail
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(matches!(
        f.worker.deliver(&unbound).await,
        Disposition::Retry(_, note) if note.contains("bindings")
    ));
    assert_eq!(f.plugin.calls().len(), 2);
    // A worker without bindings never delivers a post_failed.
    let blind = EntityLifecycleWorker::new(
        f.outbox.clone(),
        f.reader.clone(),
        Arc::new(PluginCallSigner::new("call".into(), SIGNING_SEED).unwrap()),
        Duration::from_secs(2),
    );
    assert!(matches!(
        blind.deliver(&unbound).await,
        Disposition::Retry(_, _)
    ));
    assert_eq!(f.plugin.calls().len(), 2);
}

/// The host's refusal codes: `Unavailable` (its projection is not current, the
/// callback did not run) is retried; `Unauthenticated` / `PermissionDenied` (the host refused the
/// ticket itself, a Core signing defect a retry cannot fix) ends the row `abandoned`, never
/// `delivered`; a plugin's own refusal is `delivered`.
#[tokio::test]
async fn host_ticket_refusals_are_abandoned_unavailable_is_retried_and_plugin_refusals_delivered() {
    for (answer, expected) in [
        ("grpc:unavailable", "retry"),
        ("grpc:permission_denied", "abandoned"),
        ("grpc:unauthenticated", "abandoned"),
        (
            "grpc:permission_denied:GRANT_REVISION_MISMATCH",
            "abandoned",
        ),
        ("grpc:failed_precondition:POLL_CLOSED", "delivered"),
        ("POLL_NOT_FOUND", "delivered"),
    ] {
        let f = fixture(Arc::new(move |_, _| Err(answer.to_owned()))).await;
        let disposition = f.worker.deliver(&task(1, "recalled")).await;
        let ok = match expected {
            "retry" => matches!(&disposition, Disposition::Retry(_, _)),
            "abandoned" => matches!(
                &disposition,
                Disposition::Done(LifecycleOutcome::Abandoned, Some(note)) if note.contains("system ticket")
            ),
            _ => matches!(
                &disposition,
                Disposition::Done(LifecycleOutcome::Delivered, Some(_))
            ),
        };
        assert!(ok, "{answer}: {disposition:?}");
        assert_eq!(f.plugin.calls().len(), 1, "{answer}");
    }
}
