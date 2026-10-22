//! The bridge write `im.plugin.entity_state.publish` under its three credentials.
use super::plugin_state::tests::{MemoryBindings, RecordingSink, poll_snapshot, publisher};
use super::resource_bridge::*;
use crate::domain::capability::{CapabilityError, Result};
use crate::domain::repository::{PluginAuthorizationReader, resource_access::*};
use crate::infrastructure::capability::InMemoryCapabilityGrants;
use flare_im_capability_core::plugin::invocation::{
    PluginCallClaims, PluginCallSigner, PluginCallVerifier, PluginRuntimeClaims,
    PluginRuntimeSigner, PluginRuntimeVerifier, PluginStaffCallClaims,
};
use flare_im_capability_core::plugin::resource::{HostCapability, ResourceRequest};
use flare_im_capability_core::plugin::runtime::{PluginRuntimeSnapshot, digest_bytes};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct Projection(Mutex<PluginRuntimeSnapshot>);
#[async_trait::async_trait]
impl PluginAuthorizationReader for Projection {
    async fn plugin_snapshot(
        &self,
        tenant: &str,
    ) -> flare_server_core::error::Result<Option<PluginRuntimeSnapshot>> {
        Ok(Some(self.0.lock().unwrap().clone()).filter(|s| s.tenant_id == tenant))
    }
}
struct Budget;
#[async_trait::async_trait]
impl ResourceCallBudget for Budget {
    async fn reserve(&self, _: &str, _: &str, _: &str, _: &str, _: i64) -> Result<bool> {
        Ok(true)
    }
}
struct NoReads;
#[async_trait::async_trait]
impl ResourceAccessBackend for NoReads {
    async fn read(&self, _: &ResourceActor, _: &ResourceRequest) -> Result<Value> {
        Err(CapabilityError::PolicyDenied("no reads here".into()))
    }
}

const OPERATION: &str = "im.plugin.entity_state.publish";

struct Fixture {
    bridge: ResourceBridge,
    projection: Arc<Projection>,
    call_signer: PluginCallSigner,
    runtime_signer: PluginRuntimeSigner,
    bindings: Arc<MemoryBindings>,
    sink: Arc<RecordingSink>,
}

fn fixture() -> Fixture {
    let projection = Arc::new(Projection(Mutex::new(poll_snapshot(
        "http://127.0.0.1:1".into(),
    ))));
    let call_signer = PluginCallSigner::new("call".into(), [71; 32]).unwrap();
    let mut verifier = PluginCallVerifier::default();
    verifier
        .insert(call_signer.key_id().into(), call_signer.public_key())
        .unwrap();
    let runtime_signer = PluginRuntimeSigner::new("poll-runtime".into(), [72; 32]).unwrap();
    let runtime_verifier = PluginRuntimeVerifier::from_public_keys_json(
        &json!([{
            "key_id": "poll-runtime",
            "deployment_id": "poll-1",
            "public_key_base64": base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                runtime_signer.public_key()
            ),
        }])
        .to_string(),
    )
    .unwrap();
    let policy = Arc::new(InMemoryCapabilityGrants::new());
    policy.grant_user_capability("app-a", "u1", "example.poll.vote", None, None, None);
    let bindings = Arc::new(MemoryBindings::default());
    bindings.bind("app-a", "example.poll", "poll-1", "2-group", "m-1");
    let sink = Arc::new(RecordingSink::default());
    let bridge = ResourceBridge::new(
        projection.clone(),
        policy,
        verifier,
        Arc::new(NoReads),
        Arc::new(Budget),
    )
    .with_writes(
        Some(publisher(bindings.clone(), sink.clone())),
        Some(runtime_verifier),
    );
    Fixture {
        bridge,
        projection,
        call_signer,
        runtime_signer,
        bindings,
        sink,
    }
}

fn now() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn body(version: u64, entity: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "capability": OPERATION,
        "entity": entity,
        "version": version,
        "values": {"closed": true}
    }))
    .unwrap()
}

fn grant_identity(projection: &Projection) -> (String, String, String, String, u64, String) {
    let snapshot = projection.0.lock().unwrap();
    let p = &snapshot.plugins[0];
    (
        p.grant.plugin_version.clone(),
        p.grant.manifest_digest.clone(),
        p.deployment.deployment_id.clone(),
        p.grant.grant_id.clone(),
        p.grant.revision,
        p.configuration_digest.clone(),
    )
}

fn user_claims(f: &Fixture, operation: &str, conversation: Option<&str>) -> PluginCallClaims {
    let (
        plugin_version,
        manifest_digest,
        deployment_id,
        grant_id,
        grant_revision,
        configuration_digest,
    ) = grant_identity(&f.projection);
    PluginCallClaims {
        tenant_id: "app-a".into(),
        plugin_id: "example.poll".into(),
        plugin_version,
        manifest_digest,
        deployment_id,
        grant_id,
        grant_revision,
        configuration_digest,
        actor_id: "u1".into(),
        operation: operation.into(),
        request_id: "req-1".into(),
        conversation_id: conversation.map(str::to_owned),
        issued_at_ms: now(),
        expires_at_ms: now() + 10_000,
    }
}

fn staff_claims(f: &Fixture, operation: &str) -> PluginStaffCallClaims {
    let (
        plugin_version,
        manifest_digest,
        deployment_id,
        grant_id,
        grant_revision,
        configuration_digest,
    ) = grant_identity(&f.projection);
    PluginStaffCallClaims {
        tenant_id: "app-a".into(),
        plugin_id: "example.poll".into(),
        plugin_version,
        manifest_digest,
        deployment_id,
        grant_id,
        grant_revision,
        configuration_digest,
        staff_id: "staff-7".into(),
        org_id: None,
        permission: "plugin:example.poll:admin".into(),
        operation: operation.into(),
        request_id: "req-1".into(),
        issued_at_ms: now(),
        expires_at_ms: now() + 10_000,
    }
}

fn runtime_token(f: &Fixture, bytes: &[u8], deployment: Option<&str>) -> String {
    let (plugin_version, manifest_digest, deployment_id, grant_id, grant_revision, _) =
        grant_identity(&f.projection);
    f.runtime_signer
        .sign(
            &PluginRuntimeClaims {
                tenant_id: "app-a".into(),
                plugin_id: "example.poll".into(),
                plugin_version,
                manifest_digest,
                deployment_id: deployment.map_or(deployment_id, str::to_owned),
                grant_id,
                grant_revision,
                capability: HostCapability::EntityStatePublish,
                request_id: "req-1".into(),
                request_digest: digest_bytes(bytes),
                issued_at_ms: now(),
                expires_at_ms: now() + 30_000,
            },
            now(),
        )
        .unwrap()
}

/// Time is paused: sleeping past the coalescing window lets every flush run.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(600)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_user_ticket_of_a_publishing_operation_writes_its_conversation_card() {
    let f = fixture();
    let token = f
        .call_signer
        .sign(
            &user_claims(&f, "example.poll.vote", Some("2-group")),
            now(),
        )
        .unwrap();
    let view = f
        .bridge
        .call(&token, OPERATION, "req-1", &body(3, "poll-1"))
        .await
        .unwrap();
    assert_eq!(
        view,
        json!({"entity": "poll-1", "version": 3, "outcome": "published"})
    );
    // Same again: a duplicate, still a success.
    let view = f
        .bridge
        .call(&token, OPERATION, "req-1", &body(3, "poll-1"))
        .await
        .unwrap();
    assert_eq!(view["outcome"], "duplicate");
    settle().await;
    assert_eq!(f.sink.versions(), vec![3]);

    for (claims, request_id, payload) in [
        // Another conversation's ticket, an operation without the capability, a forged request id.
        (
            user_claims(&f, "example.poll.vote", Some("2-other")),
            "req-1",
            body(4, "poll-1"),
        ),
        (
            user_claims(&f, "example.poll.details", Some("2-group")),
            "req-1",
            body(4, "poll-1"),
        ),
        (
            user_claims(&f, "example.poll.vote", Some("2-group")),
            "forged",
            body(4, "poll-1"),
        ),
        // An entity not bound to this plugin.
        (
            user_claims(&f, "example.poll.vote", Some("2-group")),
            "req-1",
            body(4, "poll-2"),
        ),
    ] {
        let token = f.call_signer.sign(&claims, now()).unwrap();
        assert!(
            f.bridge
                .call(&token, OPERATION, request_id, &payload)
                .await
                .is_err(),
            "{} {request_id}",
            claims.operation
        );
    }
}

#[tokio::test(start_paused = true)]
async fn staff_and_runtime_credentials_write_tenant_wide_for_their_plugin_only() {
    let f = fixture();
    let staff = f
        .call_signer
        .sign_staff(&staff_claims(&f, "example.poll.close"), now())
        .unwrap();
    let view = f
        .bridge
        .call(&staff, OPERATION, "req-1", &body(5, "poll-1"))
        .await
        .unwrap();
    assert_eq!(view["outcome"], "published");
    // Let the first flush run at once; the runtime write below falls in its window.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    // A staff ticket of an operation outside the admin pages, or without the capability.
    for operation in ["example.poll.vote", "example.poll.list"] {
        let token = f
            .call_signer
            .sign_staff(&staff_claims(&f, operation), now())
            .unwrap();
        assert!(
            f.bridge
                .call(&token, OPERATION, "req-1", &body(6, "poll-1"))
                .await
                .is_err(),
            "{operation}"
        );
    }

    let bytes = body(7, "poll-1");
    let runtime = runtime_token(&f, &bytes, None);
    let view = f
        .bridge
        .call_with(
            BridgeCredential::Runtime(&runtime),
            OPERATION,
            "req-1",
            &bytes,
        )
        .await
        .unwrap();
    assert_eq!(view["outcome"], "published");
    // Bound to the exact bytes, and to the key's own deployment.
    assert!(
        f.bridge
            .call_with(
                BridgeCredential::Runtime(&runtime),
                OPERATION,
                "req-1",
                &body(8, "poll-1")
            )
            .await
            .is_err()
    );
    let foreign = body(8, "poll-1");
    let other_deployment = runtime_token(&f, &foreign, Some("poll-2"));
    assert!(
        f.bridge
            .call_with(
                BridgeCredential::Runtime(&other_deployment),
                OPERATION,
                "req-1",
                &foreign
            )
            .await
            .is_err()
    );
    // A runtime credential reads nothing.
    let read = br#"{"capability":"im.conversation.read"}"#;
    assert!(
        f.bridge
            .call_with(
                BridgeCredential::Runtime(&runtime_token(&f, read, None)),
                "im.conversation.read",
                "req-1",
                read
            )
            .await
            .is_err()
    );
    // A user ticket never verifies as staff and the other way round.
    assert!(
        f.bridge
            .call(&runtime, OPERATION, "req-1", &bytes)
            .await
            .is_err()
    );
    // The grant disabled: refused.
    f.projection.0.lock().unwrap().plugins[0].grant.enabled = false;
    let bytes = body(9, "poll-1");
    let runtime = runtime_token(&f, &bytes, None);
    assert!(
        f.bridge
            .call_with(
                BridgeCredential::Runtime(&runtime),
                OPERATION,
                "req-1",
                &bytes
            )
            .await
            .is_err()
    );
    settle().await;
    assert_eq!(f.sink.versions(), vec![5, 7]);
}

#[tokio::test(start_paused = true)]
async fn the_request_limit_is_twelve_kib_and_recalled_cards_take_no_writes() {
    let f = fixture();
    let staff = f
        .call_signer
        .sign_staff(&staff_claims(&f, "example.poll.close"), now())
        .unwrap();
    let mut huge = serde_json::to_vec(&json!({
        "capability": OPERATION, "entity": "poll-1", "version": 1,
        "values": {"note": "x".repeat(1000)}
    }))
    .unwrap();
    assert!(huge.len() < 12 * 1024);
    assert!(
        f.bridge
            .call(&staff, OPERATION, "req-1", &huge)
            .await
            .is_ok()
    );
    huge.resize(12 * 1024 + 1, b' ');
    assert!(
        f.bridge
            .call(&staff, OPERATION, "req-1", &huge)
            .await
            .is_err()
    );
    f.bindings.recall("m-1");
    assert!(
        f.bridge
            .call(&staff, OPERATION, "req-1", &body(2, "poll-1"))
            .await
            .is_err()
    );
}
