use super::*;
use crate::application::handler::plugin_state::PluginStatePublisher;
use crate::application::handler::plugin_state::tests::{
    MemoryBindings, POLL_MANIFEST, RecordingSink, poll_snapshot, publisher,
};
use crate::application::handler::plugin_test_server::PluginServer;
use crate::domain::repository::PluginAuthorizationReader;
use ed25519_dalek::{
    SigningKey,
    pkcs8::{EncodePrivateKey, EncodePublicKey},
};
use flare_im_capability_core::plugin::PluginManifest;
use flare_im_capability_core::plugin::invocation::{PluginCallSigner, PluginCallVerifier};
use flare_im_capability_core::plugin::runtime::PluginRuntimeSnapshot;
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
};
use serde_json::json;
use std::sync::Mutex;

/// Evidence that is always written and not looked at.
struct AllowEvidence;
#[async_trait::async_trait]
impl AdminEvidence for AllowEvidence {
    async fn intent(
        &self,
        _: &str,
        _: &str,
        _: AdminCall,
        _: AdminIntent<'_>,
    ) -> std::result::Result<(), String> {
        Ok(())
    }
    async fn outcome(
        &self,
        _: &str,
        _: &str,
        _: AdminCall,
        _: AdminOutcome,
    ) -> std::result::Result<(), String> {
        Ok(())
    }
}
static ALLOW_EVIDENCE: AllowEvidence = AllowEvidence;

/// Evidence rows in order (`tenant`, `aggregate id`, action); the intents' reason and audited
/// target; `fail_intent` refuses the intent.
#[derive(Default)]
struct RecordingEvidence {
    rows: Mutex<Vec<(String, String, &'static str)>>,
    intents: Mutex<Vec<(Option<String>, Option<String>)>>,
    fail_intent: bool,
}
#[async_trait::async_trait]
impl AdminEvidence for RecordingEvidence {
    async fn intent(
        &self,
        tenant_id: &str,
        aggregate_id: &str,
        call: AdminCall,
        intent: AdminIntent<'_>,
    ) -> std::result::Result<(), String> {
        if self.fail_intent {
            return Err("audit store down".into());
        }
        self.rows.lock().unwrap().push((
            tenant_id.into(),
            aggregate_id.into(),
            admin_evidence_action(call, None),
        ));
        self.intents.lock().unwrap().push((
            intent.reason.map(str::to_owned),
            intent.audit_target.map(str::to_owned),
        ));
        Ok(())
    }
    async fn outcome(
        &self,
        tenant_id: &str,
        aggregate_id: &str,
        call: AdminCall,
        outcome: AdminOutcome,
    ) -> std::result::Result<(), String> {
        self.rows.lock().unwrap().push((
            tenant_id.into(),
            aggregate_id.into(),
            admin_evidence_action(call, Some(outcome)),
        ));
        Ok(())
    }
}

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

/// A gateway-signed Staff context for `app-a` with `permission` and the operator's reason,
/// verified as the admin RPCs do.
pub(crate) fn staff(permission: &str) -> VerifiedStaff {
    staff_stating(permission, Some("ticket-42 spam cleanup"))
}

/// [`staff`] with the stated reason, or none.
pub(crate) fn staff_stating(permission: &str, reason: Option<&str>) -> VerifiedStaff {
    staff_attempt("staff-7", permission, reason, "admin-req-1")
}

/// One attempt of staff member `staff_id`: a Staff context with its own request id.
fn staff_attempt(
    staff_id: &str,
    permission: &str,
    reason: Option<&str>,
    request_id: &str,
) -> VerifiedStaff {
    let key = SigningKey::from_bytes(&[61; 32]);
    let signer = SignedContextSigner::from_private_key(
        key.to_pkcs8_pem(Default::default()).unwrap().as_str(),
    )
    .unwrap()
    .with_kid("gateway")
    .unwrap();
    let verifier = SignedContextVerifier::from_public_key(
        &key.verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap(),
    )
    .unwrap()
    .require_staff_kid(["gateway"]);
    let mut identity =
        SignedContextIdentity::new(SignedActorType::Staff, staff_id, "app-a", request_id)
            .with_staff_grant(Some("org-1".into()), Some(permission.into()), 1);
    if let Some(reason) = reason {
        identity = identity.with_reason(reason);
    }
    let token = signer.sign(&identity).unwrap();
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert("x-flare-ctx", token.parse().unwrap());
    flare_im_service_kit::staff_context::verify_staff_metadata(&metadata, &verifier, permission)
        .unwrap()
}

struct Harness {
    routes: Arc<PluginRouteBook>,
    plugin: PluginServer,
    reader: Arc<Projection>,
    verifier: PluginCallVerifier,
    bindings: Arc<MemoryBindings>,
    sink: Arc<RecordingSink>,
}

async fn harness() -> Harness {
    let plugin = PluginServer::start(Arc::new(|operation, _payload| match operation {
        "example.poll.list" => Ok(json!({"items": [{"pollId": "poll-1", "question": "Lunch?"}]})),
        "example.poll.end" => Ok(json!({
            "closed": true,
            "sharedState": {"entity": "poll-1", "version": 9, "values": {"closed": true}}
        })),
        "example.poll.remove" => Err("POLL_NOT_FOUND".into()),
        _ => Ok(json!({})),
    }))
    .await;
    let signer = Arc::new(PluginCallSigner::new("call".into(), [33; 32]).unwrap());
    let mut verifier = PluginCallVerifier::default();
    verifier.insert("call".into(), signer.public_key()).unwrap();
    let reader = Arc::new(Projection(Mutex::new(Some(poll_snapshot(
        plugin.authority.clone(),
    )))));
    let bindings = Arc::new(MemoryBindings::default());
    let sink = Arc::new(RecordingSink::default());
    let publisher: Arc<PluginStatePublisher> = publisher(bindings.clone(), sink.clone());
    let routes = Arc::new(
        PluginRouteBook::new()
            .with_runtime_authorization(reader.clone(), Some(signer))
            .with_state_publisher(Some(publisher)),
    );
    Harness {
        routes,
        plugin,
        reader,
        verifier,
        bindings,
        sink,
    }
}

fn request(operation: &str, input: Value) -> AdminPluginRequest {
    AdminPluginRequest {
        plugin_id: "example.poll".into(),
        operation: operation.into(),
        input,
        idempotency_key: None,
    }
}

/// The fixture's staff permissions: `list` and `inspect` need `read`; `end` and `remove`
/// need `manage` and a stated reason, with the poll id as their audited target.
const READ: &str = "plugin:example.poll:read";
const MANAGE: &str = "plugin:example.poll:manage";

#[tokio::test]
async fn staff_reads_and_acts_through_a_staff_ticket_without_a_conversation() {
    let h = harness().await;
    let reader = staff(READ);
    let listed = dispatch_admin(
        &reader,
        &h.routes,
        AdminCall::Query,
        request("example.poll.list", json!({"status": "open", "cursor": ""})),
        Duration::from_secs(2),
        &ALLOW_EVIDENCE,
    )
    .await
    .unwrap();
    assert!(listed.success);
    assert_eq!(listed.data["items"][0]["pollId"], "poll-1");
    assert_eq!(listed.request_id, "admin-req-1");

    let call = &h.plugin.calls()[0];
    assert_eq!(call.operation, "example.poll.list");
    assert_eq!(call.payload, json!({"status": "open", "cursor": ""}));
    assert!(!call.metadata.contains_key("user_id"));
    let now = chrono::Utc::now().timestamp_millis();
    assert!(
        h.verifier.verify(&call.ticket, now).is_err(),
        "not a user ticket"
    );
    let claims = h.verifier.verify_staff(&call.ticket, now).unwrap();
    assert_eq!(
        (
            claims.tenant_id.as_str(),
            claims.staff_id.as_str(),
            claims.org_id.as_deref(),
            claims.permission.as_str(),
            claims.operation.as_str(),
            claims.request_id.as_str(),
            claims.deployment_id.as_str(),
            claims.grant_revision,
        ),
        (
            "app-a",
            "staff-7",
            Some("org-1"),
            READ,
            "example.poll.list",
            "admin-req-1",
            "poll-1",
            4
        )
    );
    assert_eq!(claims.reason.as_deref(), Some("ticket-42 spam cleanup"));

    // A plugin rejection is a result, not an error.
    let removed = dispatch_admin(
        &staff(MANAGE),
        &h.routes,
        AdminCall::Dispatch,
        request("example.poll.remove", json!({"pollId": "poll-1"})),
        Duration::from_secs(2),
        &ALLOW_EVIDENCE,
    )
    .await
    .unwrap();
    assert!(!removed.success);
    assert_eq!(removed.error_code.as_deref(), Some("POLL_NOT_FOUND"));
}

#[tokio::test]
async fn an_admin_action_result_publishes_its_shared_state_tenant_wide() {
    let h = harness().await;
    h.bindings
        .bind("app-a", "example.poll", "poll-1", "2-group", "m-1");
    let closed = dispatch_admin(
        &staff(MANAGE),
        &h.routes,
        AdminCall::Dispatch,
        request("example.poll.end", json!({"pollId": "poll-1"})),
        Duration::from_secs(2),
        &ALLOW_EVIDENCE,
    )
    .await
    .unwrap();
    assert!(closed.success);
    for _ in 0..50 {
        if !h.sink.versions().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(h.sink.versions(), vec![9]);
    let (_, conversation, event) = h.sink.events.lock().unwrap()[0].clone();
    assert_eq!(
        (conversation.as_str(), event.server_msg_id.as_str()),
        ("2-group", "m-1")
    );
    assert_eq!(event.values_json, r#"{"closed":true}"#);
}

#[tokio::test]
async fn permission_operation_kind_and_grant_are_enforced() {
    let h = harness().await;
    let call = |staff: VerifiedStaff, kind, request| {
        let routes = h.routes.clone();
        async move {
            dispatch_admin(
                &staff,
                &routes,
                kind,
                request,
                Duration::from_secs(2),
                &ALLOW_EVIDENCE,
            )
            .await
        }
    };
    // Another plugin's code, or a Core code.
    for permission in ["plugin:example.other:read", "core:plugins:write"] {
        let mut other = request("example.poll.list", json!({}));
        if permission.contains("other") {
            other.plugin_id = "example.other".into();
            assert!(matches!(
                call(
                    staff(permission),
                    AdminCall::Query,
                    request("example.poll.list", json!({}))
                )
                .await,
                Err(CapabilityError::PolicyDenied(_))
            ));
        }
        assert!(matches!(
            call(staff(permission), AdminCall::Query, other).await,
            Err(CapabilityError::PolicyDenied(_))
        ));
    }
    // Not an admin operation (a card action), or the wrong kind of call.
    for (kind, operation, permission) in [
        (AdminCall::Dispatch, "example.poll.vote", MANAGE),
        (AdminCall::Query, "example.poll.vote", READ),
        (AdminCall::Query, "example.poll.details", READ),
        (AdminCall::Dispatch, "example.poll.close", MANAGE),
        (AdminCall::Dispatch, "example.poll.list", MANAGE),
        (AdminCall::Query, "example.poll.end", MANAGE),
        (AdminCall::Query, "example.poll.missing", READ),
    ] {
        assert!(
            matches!(
                call(staff(permission), kind, request(operation, json!({}))).await,
                Err(CapabilityError::PolicyDenied(_))
            ),
            "{kind:?} {operation}"
        );
    }
    // Each operation needs exactly the permission it declares: `read` reaches no change and
    // `manage` no read.
    for (kind, operation, permission) in [
        (AdminCall::Dispatch, "example.poll.remove", READ),
        (AdminCall::Query, "example.poll.list", MANAGE),
        (
            AdminCall::Query,
            "example.poll.list",
            "plugin:example.poll:admin",
        ),
    ] {
        assert!(
            matches!(
                call(staff(permission), kind, request(operation, json!({}))).await,
                Err(CapabilityError::PolicyDenied(_))
            ),
            "{operation} {permission}"
        );
    }
    // The detail operation is a read.
    assert!(
        call(
            staff(READ),
            AdminCall::Query,
            request("example.poll.inspect", json!({"pollId": "poll-1"}))
        )
        .await
        .unwrap()
        .success
    );
    // A disabled grant.
    let mut snapshot = poll_snapshot(h.plugin.authority.clone());
    snapshot.plugins[0].grant.enabled = false;
    *h.reader.0.lock().unwrap() = Some(snapshot);
    assert!(matches!(
        call(
            staff(READ),
            AdminCall::Query,
            request("example.poll.list", json!({}))
        )
        .await,
        Err(CapabilityError::PolicyDenied(_))
    ));
    assert_eq!(
        h.plugin.calls().len(),
        1,
        "only the detail call reached the plugin"
    );
}

#[tokio::test]
async fn a_change_that_requires_a_reason_needs_the_stated_one() {
    let h = harness().await;
    let remove = || request("example.poll.remove", json!({"pollId": "poll-1"}));
    let evidence = RecordingEvidence::default();
    // No stated reason: refused before any evidence or call.
    assert!(matches!(
        dispatch_admin(
            &staff_stating(MANAGE, None),
            &h.routes,
            AdminCall::Dispatch,
            remove(),
            Duration::from_secs(2),
            &evidence
        )
        .await,
        Err(CapabilityError::PolicyDenied(_))
    ));
    assert!(h.plugin.calls().is_empty());
    assert!(evidence.rows.lock().unwrap().is_empty());
    // A read asks for none.
    assert!(
        dispatch_admin(
            &staff_stating(READ, None),
            &h.routes,
            AdminCall::Query,
            request("example.poll.list", json!({})),
            Duration::from_secs(2),
            &evidence
        )
        .await
        .unwrap()
        .success
    );
    // With the reason: the intent records it and the target, the ticket carries it.
    let removed = dispatch_admin(
        &staff_stating(MANAGE, Some("清理违规投票")),
        &h.routes,
        AdminCall::Dispatch,
        remove(),
        Duration::from_secs(2),
        &evidence,
    )
    .await
    .unwrap();
    assert_eq!(removed.audit_target.as_deref(), Some("poll-1"));
    assert!(
        serde_json::to_value(&removed)
            .unwrap()
            .get("auditTarget")
            .is_none(),
        "the audited target is for Core's audit only"
    );
    assert_eq!(
        evidence.intents.lock().unwrap().clone(),
        vec![
            (None, None),
            (Some("清理违规投票".into()), Some("poll-1".into()))
        ]
    );
    let now = chrono::Utc::now().timestamp_millis();
    let claims = h
        .verifier
        .verify_staff(&h.plugin.calls()[1].ticket, now)
        .unwrap();
    assert_eq!(
        (claims.permission.as_str(), claims.reason.as_deref()),
        (MANAGE, Some("清理违规投票")),
        "the ticket carries the operation's code and the reason"
    );
}

/// A plugin host whose projection has not caught up answers `Unavailable`: the call did not run,
/// so it is an open outcome to retry, never a plugin rejection.
#[tokio::test]
async fn a_host_that_is_not_current_yet_leaves_the_outcome_open() {
    let plugin = PluginServer::start(Arc::new(|_, _| Err("grpc:unavailable".into()))).await;
    let h = harness().await;
    *h.reader.0.lock().unwrap() = Some(poll_snapshot(plugin.authority.clone()));
    let evidence = RecordingEvidence::default();
    let outcome = dispatch_admin(
        &staff(MANAGE),
        &h.routes,
        AdminCall::Dispatch,
        request("example.poll.remove", json!({"pollId": "poll-1"})),
        Duration::from_secs(2),
        &evidence,
    )
    .await;
    assert!(
        matches!(outcome, Err(CapabilityError::Timeout(_))),
        "{outcome:?}"
    );
    assert_eq!(
        evidence.rows.lock().unwrap().last().unwrap().2,
        "dispatch_unknown"
    );
}

#[test]
fn admin_operations_are_the_admin_section_only() {
    let manifest = PluginManifest::from_json(POLL_MANIFEST.as_bytes()).unwrap();
    assert_eq!(
        manifest.admin_operations(),
        [
            "example.poll.end",
            "example.poll.inspect",
            "example.poll.list",
            "example.poll.remove"
        ]
        .map(String::from)
        .into()
    );
}

#[test]
fn admin_requests_are_bounded_objects_of_one_plugin() {
    let parsed = AdminPluginRequest::parse(
        br#"{"pluginId":"example.poll","operation":"example.poll.list","input":{"cursor":""}}"#,
    )
    .unwrap();
    assert!(parsed.is_plugin_permission(READ));
    assert!(parsed.is_plugin_permission(MANAGE));
    for other in [
        "plugin:example.other:read",
        "core:plugins:write",
        "plugin:example.poll",
    ] {
        assert!(!parsed.is_plugin_permission(other), "{other}");
    }
    assert_eq!(
        AdminPluginRequest::parse(
            br#"{"pluginId":"example.poll","operation":"example.poll.list"}"#
        )
        .unwrap()
        .input,
        json!({})
    );
    for bad in [
        br#"{"pluginId":"example.poll","operation":"example.poll.list","input":[1]}"#.as_slice(),
        br#"{"pluginId":"","operation":"example.poll.list"}"#,
        br#"{"pluginId":"example.poll","operation":""}"#,
        br#"{"pluginId":"example.poll","operation":"x","extra":1}"#,
    ] {
        assert!(AdminPluginRequest::parse(bad).is_err());
    }
    let long = format!(r#"{{"pluginId":"{}","operation":"x"}}"#, "p".repeat(120));
    assert!(
        AdminPluginRequest::parse(long.as_bytes()).is_err(),
        "permission code over 128 bytes"
    );
    let huge = format!(
        r#"{{"pluginId":"example.poll","operation":"x","input":{{"q":"{}"}}}}"#,
        "x".repeat(MAX_ADMIN_REQUEST_BYTES)
    );
    assert!(AdminPluginRequest::parse(huge.as_bytes()).is_err());
}

/// The intent is durable before the plugin runs (no evidence, no call), the outcome
/// after it, and a plugin refusal is recorded as a refusal, not an effect.
#[tokio::test]
async fn admin_calls_record_intent_before_the_plugin_and_their_outcome_after() {
    let h = harness().await;
    let manager = staff(MANAGE);
    let evidence = RecordingEvidence::default();
    let listed = dispatch_admin(
        &staff(READ),
        &h.routes,
        AdminCall::Query,
        request("example.poll.list", json!({"status": "open", "cursor": ""})),
        Duration::from_secs(2),
        &evidence,
    )
    .await
    .unwrap();
    assert!(listed.success);
    let removed = dispatch_admin(
        &manager,
        &h.routes,
        AdminCall::Dispatch,
        request("example.poll.remove", json!({"pollId": "poll-1"})),
        Duration::from_secs(2),
        &evidence,
    )
    .await
    .unwrap();
    assert!(!removed.success);
    let row = |id: &str, action: &'static str| ("app-a".to_string(), id.to_string(), action);
    assert_eq!(
        evidence.rows.lock().unwrap().clone(),
        vec![
            row("example.poll:example.poll.list", "query_intent"),
            row("example.poll:example.poll.list", "query_succeeded"),
            row("example.poll:example.poll.remove", "dispatch_intent"),
            row("example.poll:example.poll.remove", "dispatch_refused"),
        ]
    );
    assert_eq!(h.plugin.calls().len(), 2);

    // The intent cannot be written: the plugin is never called.
    let failing = RecordingEvidence {
        fail_intent: true,
        ..Default::default()
    };
    let refused = dispatch_admin(
        &manager,
        &h.routes,
        AdminCall::Dispatch,
        request("example.poll.remove", json!({"pollId": "poll-1"})),
        Duration::from_secs(2),
        &failing,
    )
    .await;
    assert!(matches!(refused, Err(CapabilityError::System(_))));
    assert_eq!(h.plugin.calls().len(), 2, "no call without its intent");
    assert!(failing.rows.lock().unwrap().is_empty());

    // An unauthorized call writes no intent at all.
    let read_only = dispatch_admin(
        &manager,
        &h.routes,
        AdminCall::Query,
        request("example.poll.remove", json!({"pollId": "poll-1"})),
        Duration::from_secs(2),
        &evidence,
    )
    .await;
    assert!(read_only.is_err());
    assert_eq!(evidence.rows.lock().unwrap().len(), 4);
}

/// A console action sent again under its `Idempotency-Key` after an unknown outcome is a new
/// request (a new request id) carrying the same command id, which the plugin keys staff commands
/// by: the command runs once. Another staff member's key, or a read, carries none of it.
#[tokio::test]
async fn retries_of_one_staff_intent_carry_one_command_id_and_run_once() {
    let executed = Arc::new(Mutex::new(std::collections::BTreeMap::<String, u32>::new()));
    let ledger = executed.clone();
    // The plugin runs a staff command once per command id and replays it afterwards.
    let plugin = PluginServer::start(Arc::new(move |operation, payload| {
        let command = payload["commandId"].as_str().unwrap_or_default().to_owned();
        let mut ledger = ledger.lock().unwrap();
        let runs = ledger.entry(format!("{operation}/{command}")).or_default();
        *runs += 1;
        Ok(json!({"closed": true, "replayed": *runs > 1}))
    }))
    .await;
    let signer = Arc::new(PluginCallSigner::new("call".into(), [33; 32]).unwrap());
    let reader = Arc::new(Projection(Mutex::new(Some(poll_snapshot(
        plugin.authority.clone(),
    )))));
    let routes = Arc::new(PluginRouteBook::new().with_runtime_authorization(reader, Some(signer)));
    let attempt = |staff_id: &'static str, request_id: &'static str, key: Option<&'static str>| {
        let routes = routes.clone();
        async move {
            let mut close = request("example.poll.end", json!({"pollId": "poll-1"}));
            close.idempotency_key = key.map(str::to_owned);
            dispatch_admin(
                &staff_attempt(staff_id, MANAGE, Some("spam"), request_id),
                &routes,
                AdminCall::Dispatch,
                close,
                Duration::from_secs(2),
                &ALLOW_EVIDENCE,
            )
            .await
            .unwrap()
        }
    };
    let first = attempt("staff-7", "attempt-1", Some("close-7")).await;
    let retry = attempt("staff-7", "attempt-2", Some("close-7")).await;
    assert_eq!(first.request_id, "attempt-1");
    assert_eq!(retry.request_id, "attempt-2");
    assert_eq!(retry.data["replayed"], true);
    let calls = plugin.calls();
    let command_id = staff_command_id("staff-7", "close-7");
    assert!(command_id.len() <= 128 && command_id.starts_with("intent-"));
    for call in &calls {
        assert_eq!(call.payload["commandId"], command_id.as_str());
        assert_eq!(call.payload["pollId"], "poll-1");
    }
    // A client-chosen command id does not override the key's.
    let mut forged = request(
        "example.poll.end",
        json!({"pollId": "poll-1", "commandId": "mine"}),
    );
    forged.idempotency_key = Some("close-7".into());
    dispatch_admin(
        &staff_attempt("staff-7", MANAGE, Some("spam"), "attempt-3"),
        &routes,
        AdminCall::Dispatch,
        forged,
        Duration::from_secs(2),
        &ALLOW_EVIDENCE,
    )
    .await
    .unwrap();
    assert_eq!(plugin.calls()[2].payload["commandId"], command_id.as_str());
    assert_eq!(
        executed.lock().unwrap()[&format!("example.poll.end/{command_id}")],
        3,
        "three attempts, one command"
    );
    // Another staff member's key, or another key, is another command.
    attempt("staff-8", "attempt-4", Some("close-7")).await;
    attempt("staff-7", "attempt-5", Some("close-8")).await;
    let ids: std::collections::BTreeSet<String> = plugin
        .calls()
        .iter()
        .map(|call| call.payload["commandId"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(ids.len(), 3);
    assert_ne!(
        staff_command_id("staff-7", "close-7"),
        staff_command_id("staff-8", "close-7")
    );
    // Without a key the input goes as it is.
    attempt("staff-7", "attempt-6", None).await;
    assert!(plugin.calls()[5].payload.get("commandId").is_none());
}

#[test]
fn an_idempotency_key_is_bounded_ascii_without_control_characters() {
    let body = |key: &str| {
        serde_json::to_vec(&json!({
            "pluginId": "example.poll", "operation": "example.poll.end",
            "input": {"pollId": "p"}, "idempotencyKey": key
        }))
        .unwrap()
    };
    assert_eq!(
        AdminPluginRequest::parse(&body("close-7"))
            .unwrap()
            .idempotency_key
            .as_deref(),
        Some("close-7")
    );
    for bad in ["", "a\tb", "清理", &"k".repeat(129)] {
        assert!(AdminPluginRequest::parse(&body(bad)).is_err(), "{bad:?}");
    }
}
