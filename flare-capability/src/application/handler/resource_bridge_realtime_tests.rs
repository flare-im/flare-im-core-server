//! The bridge write `im.realtime.deliver`: who may deliver, into which conversation, to whom.
use super::plugin_state::tests::POLL_MANIFEST;
use super::resource_bridge::*;
use crate::domain::capability::{CapabilityError, Result};
use crate::domain::repository::realtime_delivery::{RealtimePacket, RealtimePacketDelivery};
use crate::domain::repository::{PluginAuthorizationReader, resource_access::*};
use crate::infrastructure::capability::InMemoryCapabilityGrants;
use flare_im_capability_core::plugin::invocation::{
    PluginCallClaims, PluginCallSigner, PluginCallVerifier, PluginRuntimeClaims,
    PluginRuntimeSigner, PluginRuntimeVerifier, PluginStaffCallClaims,
};
use flare_im_capability_core::plugin::resource::{
    HostCapability, MembersCheckView, ResourceRequest,
};
use flare_im_capability_core::plugin::runtime::{PluginRuntimeSnapshot, digest_bytes};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

const OPERATION: &str = "im.realtime.deliver";

/// The poll fixture with `vote` declaring `im.realtime.deliver` (and the publish permission
/// scoping it); `close` keeps only the entity state capability.
fn realtime_manifest() -> String {
    let mut raw: Value = serde_json::from_str(POLL_MANIFEST).unwrap();
    for permission in raw["permissions"].as_array_mut().unwrap() {
        if permission["id"] == "example.poll.publish" {
            permission["dataScopes"]
                .as_array_mut()
                .unwrap()
                .push(json!(OPERATION));
        }
    }
    for op in raw["capabilities"][0]["operations"].as_array_mut().unwrap() {
        if op["id"] == "vote" {
            op["hostCapabilities"]
                .as_array_mut()
                .unwrap()
                .push(json!(OPERATION));
        }
    }
    serde_json::to_string(&raw).unwrap()
}

fn snapshot() -> PluginRuntimeSnapshot {
    use flare_im_capability_core::plugin::authorization::TenantPluginGrant;
    use flare_im_capability_core::plugin::manifest::PluginManifest;
    use flare_im_capability_core::plugin::runtime::*;
    let raw = realtime_manifest();
    let manifest = PluginManifest::from_json(raw.as_bytes()).unwrap();
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
                grpc_authority: "http://127.0.0.1:1".into(),
            },
            configuration_digest: configuration_digest(&json!({"maxOptions": 6})).unwrap(),
            client_configuration: Default::default(),
        }],
    }
}

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

/// Conversation `2-group` of tenant `app-a` has members `u1`, `u2` and `u3`; every lookup is
/// recorded with the identity it ran as.
#[derive(Default)]
struct Members {
    /// `(user, conversation)` of the reads made as the ticket's user.
    user_reads: Mutex<Vec<(String, String)>>,
    /// `(tenant, conversation)` of the reads made as Capability for a tenant.
    tenant_reads: Mutex<Vec<(String, String)>>,
}
impl Members {
    fn of(tenant: &str, conversation: &str) -> BTreeSet<String> {
        if tenant == "app-a" && conversation == "2-group" {
            ["u1", "u2", "u3"].map(String::from).into()
        } else {
            BTreeSet::new()
        }
    }
}
#[async_trait::async_trait]
impl ResourceAccessBackend for Members {
    async fn read(&self, actor: &ResourceActor, request: &ResourceRequest) -> Result<Value> {
        let ResourceRequest::ConversationMembersCheck { user_ids } = request else {
            return Err(CapabilityError::PolicyDenied("members check only".into()));
        };
        self.user_reads
            .lock()
            .unwrap()
            .push((actor.user_id.clone(), actor.conversation_id.clone()));
        let members = Self::of(&actor.tenant_id, &actor.conversation_id);
        if !members.contains(&actor.user_id) {
            return Err(CapabilityError::PolicyDenied("not a member".into()));
        }
        Ok(
            serde_json::to_value(MembersCheckView::partition(user_ids, |id| {
                members.contains(id)
            }))
            .unwrap(),
        )
    }
    async fn tenant_members(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        _: &str,
        user_ids: &[String],
    ) -> Result<BTreeSet<String>> {
        self.tenant_reads
            .lock()
            .unwrap()
            .push((tenant_id.into(), conversation_id.into()));
        let members = Self::of(tenant_id, conversation_id);
        Ok(user_ids
            .iter()
            .filter(|id| members.contains(*id))
            .cloned()
            .collect())
    }
}

#[derive(Default)]
struct Deliveries(Mutex<Vec<RealtimePacket>>);
#[async_trait::async_trait]
impl RealtimePacketDelivery for Deliveries {
    async fn deliver(&self, packet: &RealtimePacket) -> Result<()> {
        self.0.lock().unwrap().push(packet.clone());
        Ok(())
    }
}
impl Deliveries {
    fn taken(&self) -> Vec<RealtimePacket> {
        std::mem::take(&mut self.0.lock().unwrap())
    }
}

struct Fixture {
    bridge: ResourceBridge,
    projection: Arc<Projection>,
    call_signer: PluginCallSigner,
    runtime_signer: PluginRuntimeSigner,
    members: Arc<Members>,
    deliveries: Arc<Deliveries>,
}

fn fixture_with(realtime: bool) -> Fixture {
    let projection = Arc::new(Projection(Mutex::new(snapshot())));
    let call_signer = PluginCallSigner::new("call".into(), [81; 32]).unwrap();
    let mut verifier = PluginCallVerifier::default();
    verifier
        .insert(call_signer.key_id().into(), call_signer.public_key())
        .unwrap();
    let runtime_signer = PluginRuntimeSigner::new("poll-runtime".into(), [82; 32]).unwrap();
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
    let members = Arc::new(Members::default());
    let deliveries = Arc::new(Deliveries::default());
    let bridge = ResourceBridge::new(
        projection.clone(),
        Arc::new(InMemoryCapabilityGrants::new()),
        verifier,
        members.clone(),
        Arc::new(Budget),
    )
    .with_writes(None, Some(runtime_verifier))
    .with_realtime(realtime.then(|| deliveries.clone() as Arc<dyn RealtimePacketDelivery>));
    Fixture {
        bridge,
        projection,
        call_signer,
        runtime_signer,
        members,
        deliveries,
    }
}

fn now() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn body(conversation: &str, users: &[&str]) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "capability": OPERATION,
        "conversationId": conversation,
        "userIds": users,
        "packetType": "call.snapshot",
        "payload": {"callId": "c-1", "version": 2},
        "ttlMs": 60000
    }))
    .unwrap()
}

fn identity(f: &Fixture) -> (String, String, String, String, u64, String) {
    let snapshot = f.projection.0.lock().unwrap();
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

fn user_ticket(f: &Fixture, operation: &str, conversation: &str) -> String {
    let (plugin_version, manifest_digest, deployment_id, grant_id, grant_revision, config) =
        identity(f);
    f.call_signer
        .sign(
            &PluginCallClaims {
                tenant_id: "app-a".into(),
                plugin_id: "example.poll".into(),
                plugin_version,
                manifest_digest,
                deployment_id,
                grant_id,
                grant_revision,
                configuration_digest: config,
                actor_id: "u1".into(),
                operation: operation.into(),
                request_id: "req-1".into(),
                conversation_id: Some(conversation.into()),
                issued_at_ms: now(),
                expires_at_ms: now() + 10_000,
            },
            now(),
        )
        .unwrap()
}

fn runtime_token(f: &Fixture, bytes: &[u8], capability: HostCapability) -> String {
    let (plugin_version, manifest_digest, deployment_id, grant_id, grant_revision, _) = identity(f);
    f.runtime_signer
        .sign(
            &PluginRuntimeClaims {
                tenant_id: "app-a".into(),
                plugin_id: "example.poll".into(),
                plugin_version,
                manifest_digest,
                deployment_id,
                grant_id,
                grant_revision,
                capability,
                request_id: "req-1".into(),
                request_digest: digest_bytes(bytes),
                issued_at_ms: now(),
                expires_at_ms: now() + 30_000,
            },
            now(),
        )
        .unwrap()
}

#[tokio::test]
async fn a_user_ticket_delivers_to_members_of_its_conversation_and_skips_the_rest() {
    let f = fixture_with(true);
    let token = user_ticket(&f, "example.poll.vote", "2-group");
    let before = now();
    let view = f
        .bridge
        .call(
            &token,
            OPERATION,
            "req-1",
            &body("2-group", &["u2", "u9", "u3", "u8"]),
        )
        .await
        .unwrap();
    assert_eq!(view["delivered"], 2);
    assert_eq!(view["skipped"], json!(["u9", "u8"]));
    let delivery_id = view["deliveryId"].as_str().unwrap().to_owned();
    assert!(!delivery_id.is_empty());

    let packets = f.deliveries.taken();
    assert_eq!(packets.len(), 1);
    let packet = &packets[0];
    assert_eq!(packet.tenant_id, "app-a");
    assert_eq!(packet.plugin_id, "example.poll");
    assert_eq!(packet.conversation_id, "2-group");
    assert_eq!(packet.user_ids, vec!["u2", "u3"]);
    assert_eq!(packet.packet_type, "call.snapshot");
    assert_eq!(
        serde_json::from_slice::<Value>(&packet.payload_json).unwrap(),
        json!({"callId": "c-1", "version": 2})
    );
    assert_eq!(packet.delivery_id, delivery_id);
    assert!((before + 60_000..=now() + 60_000).contains(&packet.expires_at_ms));
    // Read as the ticket's user, in the ticket's conversation.
    assert_eq!(
        *f.members.user_reads.lock().unwrap(),
        vec![("u1".to_string(), "2-group".to_string())]
    );
    assert!(f.members.tenant_reads.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_user_ticket_names_only_its_own_conversation_and_a_declaring_operation() {
    let f = fixture_with(true);
    // The ticket is bound to 2-group; the request names another conversation.
    let token = user_ticket(&f, "example.poll.vote", "2-group");
    assert!(matches!(
        f.bridge
            .call(&token, OPERATION, "req-1", &body("2-other", &["u2"]))
            .await,
        Err(CapabilityError::PolicyDenied(_))
    ));
    // `close` does not declare im.realtime.deliver.
    let token = user_ticket(&f, "example.poll.close", "2-group");
    assert!(
        f.bridge
            .call(&token, OPERATION, "req-1", &body("2-group", &["u2"]))
            .await
            .is_err()
    );
    // A forged request id.
    let token = user_ticket(&f, "example.poll.vote", "2-group");
    assert!(
        f.bridge
            .call(&token, OPERATION, "forged", &body("2-group", &["u2"]))
            .await
            .is_err()
    );
    // The caller is not a member of the ticket's conversation: the members check refuses.
    let token = user_ticket(&f, "example.poll.vote", "2-lonely");
    assert!(
        f.bridge
            .call(&token, OPERATION, "req-1", &body("2-lonely", &["u2"]))
            .await
            .is_err()
    );
    assert!(f.deliveries.taken().is_empty());
}

#[tokio::test]
async fn a_staff_ticket_cannot_deliver() {
    let f = fixture_with(true);
    let (plugin_version, manifest_digest, deployment_id, grant_id, grant_revision, config) =
        identity(&f);
    let staff = f
        .call_signer
        .sign_staff(
            &PluginStaffCallClaims {
                tenant_id: "app-a".into(),
                plugin_id: "example.poll".into(),
                plugin_version,
                manifest_digest,
                deployment_id,
                grant_id,
                grant_revision,
                configuration_digest: config,
                staff_id: "staff-7".into(),
                org_id: None,
                permission: "plugin:example.poll:manage".into(),
                operation: "example.poll.end".into(),
                request_id: "req-1".into(),
                reason: Some("ticket-42".into()),
                issued_at_ms: now(),
                expires_at_ms: now() + 10_000,
            },
            now(),
        )
        .unwrap();
    assert!(
        f.bridge
            .call(&staff, OPERATION, "req-1", &body("2-group", &["u2"]))
            .await
            .is_err()
    );
    assert!(f.deliveries.taken().is_empty());
}

#[tokio::test]
async fn the_runtime_credential_delivers_into_any_conversation_of_its_tenant() {
    let f = fixture_with(true);
    let bytes = body("2-group", &["u1", "u3", "u7"]);
    let runtime = runtime_token(&f, &bytes, HostCapability::RealtimeDeliver);
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
    assert_eq!(view["delivered"], 2);
    assert_eq!(view["skipped"], json!(["u7"]));
    assert_eq!(f.deliveries.taken()[0].user_ids, vec!["u1", "u3"]);
    // Read as Capability for the credential's tenant, not as a user.
    assert_eq!(
        *f.members.tenant_reads.lock().unwrap(),
        vec![("app-a".to_string(), "2-group".to_string())]
    );
    assert!(f.members.user_reads.lock().unwrap().is_empty());

    // A conversation with none of them as members: nothing is delivered, all are skipped.
    let bytes = body("2-elsewhere", &["u1", "u2"]);
    let runtime = runtime_token(&f, &bytes, HostCapability::RealtimeDeliver);
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
    assert_eq!(view["delivered"], 0);
    assert_eq!(view["skipped"], json!(["u1", "u2"]));
    assert!(f.deliveries.taken().is_empty());

    // Bound to the exact bytes and to the capability it was issued for.
    let other = body("2-group", &["u2"]);
    assert!(
        f.bridge
            .call_with(
                BridgeCredential::Runtime(&runtime),
                OPERATION,
                "req-1",
                &other
            )
            .await
            .is_err()
    );
    let entity_state = runtime_token(&f, &other, HostCapability::EntityStatePublish);
    assert!(
        f.bridge
            .call_with(
                BridgeCredential::Runtime(&entity_state),
                OPERATION,
                "req-1",
                &other
            )
            .await
            .is_err()
    );
    // The grant disabled: refused.
    f.projection.0.lock().unwrap().plugins[0].grant.enabled = false;
    let runtime = runtime_token(&f, &other, HostCapability::RealtimeDeliver);
    assert!(
        f.bridge
            .call_with(
                BridgeCredential::Runtime(&runtime),
                OPERATION,
                "req-1",
                &other
            )
            .await
            .is_err()
    );
    assert!(f.deliveries.taken().is_empty());
}

#[tokio::test]
async fn invalid_or_oversized_deliveries_are_denied() {
    let f = fixture_with(true);
    let token = user_ticket(&f, "example.poll.vote", "2-group");
    let valid: Value = serde_json::from_slice(&body("2-group", &["u2"])).unwrap();
    let with = |key: &str, value: Value| {
        let mut request = valid.clone();
        request[key] = value;
        serde_json::to_vec(&request).unwrap()
    };
    let many: Vec<String> = (0..33).map(|i| format!("u{i}")).collect();
    for (case, bytes) in [
        (
            "6 KiB payload",
            with("payload", json!({"pad": "x".repeat(6 * 1024)})),
        ),
        ("payload not an object", with("payload", json!([1, 2]))),
        ("no recipients", with("userIds", json!([]))),
        ("33 recipients", with("userIds", json!(many))),
        ("duplicate recipients", with("userIds", json!(["u2", "u2"]))),
        ("packet type", with("packetType", json!("Call.Snapshot"))),
        ("zero ttl", with("ttlMs", json!(0))),
        ("ttl over two minutes", with("ttlMs", json!(120_001))),
        ("unknown field", with("extra", json!(true))),
    ] {
        assert!(
            f.bridge
                .call(&token, OPERATION, "req-1", &bytes)
                .await
                .is_err(),
            "{case}"
        );
    }
    // Over the request bound of the listener.
    let mut huge = body("2-group", &["u2"]);
    huge.resize(12 * 1024 + 1, b' ');
    assert!(
        f.bridge
            .call(&token, OPERATION, "req-1", &huge)
            .await
            .is_err()
    );
    assert!(f.deliveries.taken().is_empty());
    assert!(f.members.user_reads.lock().unwrap().is_empty());
}

#[tokio::test]
async fn without_the_push_path_a_delivery_is_unavailable() {
    let f = fixture_with(false);
    let token = user_ticket(&f, "example.poll.vote", "2-group");
    assert!(matches!(
        f.bridge
            .call(&token, OPERATION, "req-1", &body("2-group", &["u2"]))
            .await,
        Err(CapabilityError::System(_))
    ));
}
