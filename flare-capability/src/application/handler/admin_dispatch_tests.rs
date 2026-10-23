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
    async fn intent(&self, _: &str, _: &str, _: AdminCall) -> std::result::Result<(), String> {
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

/// Evidence rows in order (`tenant`, `aggregate id`, action); `fail_intent` refuses the intent.
#[derive(Default)]
struct RecordingEvidence {
    rows: Mutex<Vec<(String, String, &'static str)>>,
    fail_intent: bool,
}
#[async_trait::async_trait]
impl AdminEvidence for RecordingEvidence {
    async fn intent(
        &self,
        tenant_id: &str,
        aggregate_id: &str,
        call: AdminCall,
    ) -> std::result::Result<(), String> {
        if self.fail_intent {
            return Err("audit store down".into());
        }
        self.rows.lock().unwrap().push((
            tenant_id.into(),
            aggregate_id.into(),
            admin_evidence_action(call, None),
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

/// A gateway-signed Staff context for `app-a` with `permission`, verified as the admin RPCs do.
pub(crate) fn staff(permission: &str) -> VerifiedStaff {
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
    let token = signer
        .sign(
            &SignedContextIdentity::new(SignedActorType::Staff, "staff-7", "app-a", "admin-req-1")
                .with_staff_grant(Some("org-1".into()), Some(permission.into()), 1),
        )
        .unwrap();
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
        "example.poll.close" => Ok(json!({
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
    }
}

const ADMIN: &str = "plugin:example.poll:admin";

#[tokio::test]
async fn staff_reads_and_acts_through_a_staff_ticket_without_a_conversation() {
    let h = harness().await;
    let staff = staff(ADMIN);
    let listed = dispatch_admin(
        &staff,
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
            ADMIN,
            "example.poll.list",
            "admin-req-1",
            "poll-1",
            4
        )
    );

    // A plugin rejection is a result, not an error.
    let removed = dispatch_admin(
        &staff,
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
        &staff(ADMIN),
        &h.routes,
        AdminCall::Dispatch,
        request("example.poll.close", json!({"pollId": "poll-1"})),
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
    for permission in ["plugin:example.other:admin", "core:plugins:write"] {
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
    // Not an admin-page operation (a card action), or the wrong kind of call.
    for (kind, operation) in [
        (AdminCall::Dispatch, "example.poll.vote"),
        (AdminCall::Query, "example.poll.vote"),
        (AdminCall::Dispatch, "example.poll.list"),
        (AdminCall::Query, "example.poll.close"),
        (AdminCall::Query, "example.poll.missing"),
    ] {
        assert!(
            matches!(
                call(staff(ADMIN), kind, request(operation, json!({}))).await,
                Err(CapabilityError::PolicyDenied(_))
            ),
            "{kind:?} {operation}"
        );
    }
    // The detail operation is a read.
    assert!(
        call(
            staff(ADMIN),
            AdminCall::Query,
            request("example.poll.details", json!({"pollId": "poll-1"}))
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
            staff(ADMIN),
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

/// The example poll with an `admin` section: `remove` is a command needing `manage`, with the
/// poll id as its audited target.
fn snapshot_with_admin_section(authority: String) -> PluginRuntimeSnapshot {
    use flare_im_capability_core::plugin::runtime::digest_bytes;
    let mut raw: Value = serde_json::from_str(POLL_MANIFEST).unwrap();
    raw["admin"] = json!({
        "contractVersion": 1,
        "permissions": [{"id": "manage", "label": {"en-US": "Manage polls"}, "risk": "high"}],
        "operations": [{"id": "example.poll.remove", "mode": "command", "permission": "manage",
                        "requiresReason": true, "auditTarget": "/pollId"}]
    });
    let raw = serde_json::to_string(&raw).unwrap();
    PluginManifest::from_json(raw.as_bytes()).expect("a valid admin section");
    let digest = digest_bytes(raw.as_bytes());
    let mut snapshot = poll_snapshot(authority);
    let projected = &mut snapshot.plugins[0];
    projected.manifest_json = raw;
    projected.grant.manifest_digest = digest.clone();
    projected.deployment.manifest_digest = digest;
    snapshot
}

#[tokio::test]
async fn an_admin_section_operation_needs_its_own_permission_and_mode() {
    let h = harness().await;
    *h.reader.0.lock().unwrap() = Some(snapshot_with_admin_section(h.plugin.authority.clone()));
    let remove = || request("example.poll.remove", json!({"pollId": "poll-1"}));
    // The plugin-wide admin code no longer covers an operation that declares its permission.
    assert!(matches!(
        dispatch_admin(
            &staff(ADMIN),
            &h.routes,
            AdminCall::Dispatch,
            remove(),
            Duration::from_secs(2),
            &ALLOW_EVIDENCE
        )
        .await,
        Err(CapabilityError::PolicyDenied(_))
    ));
    // A command is no read.
    let manage = "plugin:example.poll:manage";
    assert!(matches!(
        dispatch_admin(
            &staff(manage),
            &h.routes,
            AdminCall::Query,
            remove(),
            Duration::from_secs(2),
            &ALLOW_EVIDENCE
        )
        .await,
        Err(CapabilityError::PolicyDenied(_))
    ));
    assert!(h.plugin.calls().is_empty());
    let removed = dispatch_admin(
        &staff(manage),
        &h.routes,
        AdminCall::Dispatch,
        remove(),
        Duration::from_secs(2),
        &ALLOW_EVIDENCE,
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
    let now = chrono::Utc::now().timestamp_millis();
    let claims = h
        .verifier
        .verify_staff(&h.plugin.calls()[0].ticket, now)
        .unwrap();
    assert_eq!(
        claims.permission, manage,
        "the ticket carries the operation's code"
    );
    // Admin-page operations keep the plugin-wide code.
    assert!(
        dispatch_admin(
            &staff(ADMIN),
            &h.routes,
            AdminCall::Query,
            request("example.poll.list", json!({})),
            Duration::from_secs(2),
            &ALLOW_EVIDENCE
        )
        .await
        .unwrap()
        .success
    );
}

#[test]
fn admin_sets_split_reads_from_actions() {
    let manifest = PluginManifest::from_json(POLL_MANIFEST.as_bytes()).unwrap();
    assert_eq!(
        admin_sources(&manifest),
        ["example.poll.details", "example.poll.list"]
            .map(String::from)
            .into()
    );
    assert_eq!(
        admin_actions(&manifest),
        ["example.poll.close", "example.poll.remove"]
            .map(String::from)
            .into()
    );
    assert!(admin_sources(&manifest).is_subset(&manifest.admin_operations()));
    assert!(admin_actions(&manifest).is_subset(&manifest.admin_operations()));
}

#[test]
fn admin_requests_are_bounded_objects_of_one_plugin() {
    let parsed = AdminPluginRequest::parse(
        br#"{"pluginId":"example.poll","operation":"example.poll.list","input":{"cursor":""}}"#,
    )
    .unwrap();
    assert!(parsed.is_plugin_permission(ADMIN));
    assert!(parsed.is_plugin_permission("plugin:example.poll:manage"));
    for other in [
        "plugin:example.other:admin",
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
    let long = format!(r#"{{"pluginId":"{}","operation":"x"}}"#, "p".repeat(116));
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
    let staff = staff(ADMIN);
    let evidence = RecordingEvidence::default();
    let listed = dispatch_admin(
        &staff,
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
        &staff,
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
        &staff,
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
        &staff,
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
