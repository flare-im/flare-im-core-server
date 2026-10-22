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
        async move { dispatch_admin(&staff, &routes, kind, request, Duration::from_secs(2)).await }
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
    assert_eq!(parsed.permission(), ADMIN);
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
