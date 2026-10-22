use super::resource_bridge::*;
use crate::domain::capability::{CapabilityError, Result};
use crate::{
    domain::repository::{PluginAuthorizationReader, resource_access::*},
    infrastructure::capability::InMemoryCapabilityGrants,
};
use flare_im_capability_core::plugin::{
    authorization::TenantPluginGrant,
    invocation::{PluginCallClaims, PluginCallSigner},
    runtime::*,
};
use flare_im_capability_core::plugin::{invocation::PluginCallVerifier, resource::ResourceRequest};
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

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
struct Budget(AtomicUsize);
#[async_trait::async_trait]
impl ResourceCallBudget for Budget {
    async fn reserve(&self, _: &str, _: &str, _: &str, _: &str, _: i64) -> Result<bool> {
        Ok(self
            .0
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                (n < 8).then_some(n + 1)
            })
            .is_ok())
    }
}
#[derive(Default)]
struct Backend {
    hits: AtomicUsize,
    denied: AtomicBool,
    pause: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl ResourceAccessBackend for Backend {
    async fn read(&self, actor: &ResourceActor, _: &ResourceRequest) -> Result<Value> {
        assert_eq!(actor.tenant_id, "201");
        assert_eq!(actor.user_id, "u1");
        assert_eq!(actor.conversation_id, "conversation-1");
        self.hits.fetch_add(1, Ordering::SeqCst);
        if self.pause.load(Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        if self.denied.load(Ordering::SeqCst) {
            return Err(CapabilityError::PolicyDenied("membership denied".into()));
        }
        Ok(json!({"conversationId":actor.conversation_id}))
    }
}
fn fixture() -> (
    Arc<ResourceBridge>,
    Arc<Projection>,
    Arc<Backend>,
    PluginCallSigner,
    PluginCallClaims,
    Arc<InMemoryCapabilityGrants>,
) {
    let raw=serde_json::to_string(&json!({"manifestVersion":1,"pluginId":"example.resource","version":"0.1.0","apiVersion":"1.0","displayName":"Resource fixture","publisher":{"publisherId":"fixture","displayName":"Fixture"},"runtime":{"kind":"grpc","service":"flare.capability.v1.ExtensionPlugin","protocol":"flare.capability.v1"},"capabilities":[{"id":"example.resource","operations":[{"id":"run","inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"permissionIds":["example.resource.read"],"hostCapabilities":["im.conversation.read"],"timeoutMs":1000,"idempotent":true}]}],"permissions":[{"id":"example.resource.read","description":"Read bound resource","riskLevel":"high","dataScopes":["im.conversation.read"]}],"artifact":{"digest":format!("sha256:{}","a".repeat(64)),"signatureRef":"fixture"}})).unwrap();
    let digest = digest_bytes(raw.as_bytes());
    let now = chrono::Utc::now().timestamp_millis();
    let config = configuration_digest(&json!({})).unwrap();
    let projection = Arc::new(Projection(Mutex::new(PluginRuntimeSnapshot {
        tenant_id: "201".into(),
        version: 1,
        issued_at_ms: now,
        valid_until_ms: now + 60000,
        plugins: vec![ProjectedPlugin {
            grant: TenantPluginGrant {
                grant_id: "grant-1".into(),
                revision: 2,
                tenant_id: "201".into(),
                plugin_id: "example.resource".into(),
                plugin_version: "0.1.0".into(),
                manifest_digest: digest.clone(),
                approved_operations: ["example.resource.run".into()].into(),
                approved_permissions: ["example.resource.read".into()].into(),
                approved_data_scopes: ["im.conversation.read".into()].into(),
                enabled: true,
                revoked: false,
                valid_from_ms: now,
                valid_until_ms: now + 60000,
            },
            manifest_json: raw,
            deployment: PluginDeployment {
                deployment_id: "deploy-1".into(),
                plugin_id: "example.resource".into(),
                plugin_version: "0.1.0".into(),
                manifest_digest: digest.clone(),
                artifact_digest: format!("sha256:{}", "a".repeat(64)),
                grpc_authority: "http://127.0.0.1:1".into(),
            },
            configuration_digest: config.clone(),
            client_configuration: Default::default(),
        }],
    })));
    let signer = PluginCallSigner::new("resource-test".into(), [43; 32]).unwrap();
    let mut verifier = PluginCallVerifier::default();
    verifier
        .insert(signer.key_id().into(), signer.public_key())
        .unwrap();
    let policy = Arc::new(InMemoryCapabilityGrants::new());
    policy.grant_user_capability("201", "u1", "example.resource.run", None, None, None);
    let backend = Arc::new(Backend::default());
    let bridge = Arc::new(ResourceBridge::new(
        projection.clone(),
        policy.clone(),
        verifier,
        backend.clone(),
        Arc::new(Budget(AtomicUsize::new(0))),
    ));
    let claims = PluginCallClaims {
        tenant_id: "201".into(),
        plugin_id: "example.resource".into(),
        plugin_version: "0.1.0".into(),
        manifest_digest: digest,
        deployment_id: "deploy-1".into(),
        grant_id: "grant-1".into(),
        grant_revision: 2,
        configuration_digest: config,
        actor_id: "u1".into(),
        operation: "example.resource.run".into(),
        request_id: "req-1".into(),
        conversation_id: Some("conversation-1".into()),
        issued_at_ms: now,
        expires_at_ms: now + 10000,
    };
    (bridge, projection, backend, signer, claims, policy)
}
const READ: &[u8] = br#"{"capability":"im.conversation.read"}"#;
async fn read(
    bridge: &ResourceBridge,
    signer: &PluginCallSigner,
    claims: &PluginCallClaims,
) -> Result<Value> {
    bridge
        .call(
            &signer
                .sign(claims, chrono::Utc::now().timestamp_millis())
                .unwrap(),
            "im.conversation.read",
            &claims.request_id,
            READ,
        )
        .await
}

#[tokio::test]
async fn resource_callback_revalidates_tenant_actor_revision_scope_and_membership() {
    let (bridge, projection, backend, signer, claims, _) = fixture();
    assert!(read(&bridge, &signer, &claims).await.is_ok());
    for mode in 0..5 {
        let mut bad = claims.clone();
        match mode {
            0 => bad.tenant_id = "202".into(),
            1 => bad.actor_id = "u2".into(),
            2 => bad.grant_revision = 1,
            3 => bad.conversation_id = None,
            _ => bad.configuration_digest = format!("sha256:{}", "b".repeat(64)),
        }
        assert!(read(&bridge, &signer, &bad).await.is_err());
    }
    assert_eq!(backend.hits.load(Ordering::SeqCst), 1);
    projection.0.lock().unwrap().plugins[0]
        .grant
        .approved_data_scopes
        .clear();
    assert!(read(&bridge, &signer, &claims).await.is_err());
    projection.0.lock().unwrap().plugins[0]
        .grant
        .approved_data_scopes
        .insert("im.conversation.read".into());
    backend.denied.store(true, Ordering::SeqCst);
    assert!(read(&bridge, &signer, &claims).await.is_err());
}

#[tokio::test]
async fn resource_callback_drops_result_if_authorization_changes_during_read() {
    let (bridge, projection, backend, signer, claims, _) = fixture();
    backend.pause.store(true, Ordering::SeqCst);
    let task = tokio::spawn(async move { read(&bridge, &signer, &claims).await });
    backend.entered.notified().await;
    projection.0.lock().unwrap().plugins[0].grant.revoked = true;
    backend.release.notify_one();
    assert!(task.await.unwrap().is_err());
}

#[tokio::test]
async fn callback_pages_and_quotas_cannot_be_forged() {
    let (bridge, _, backend, signer, claims, _) = fixture();
    let token = signer
        .sign(&claims, chrono::Utc::now().timestamp_millis())
        .unwrap();
    for payload in [
        br#"{"capability":"im.conversation.read","conversationId":"other"}"#.as_slice(),
        br#"{"capability":"im.conversation.read","tenantId":"202"}"#.as_slice(),
    ] {
        assert!(
            bridge
                .call(&token, "im.conversation.read", "req-1", payload)
                .await
                .is_err()
        );
    }
    assert!(
        bridge
            .call(&token, "im.conversation.read", "forged", READ)
            .await
            .is_err()
    );
    assert_eq!(backend.hits.load(Ordering::SeqCst), 0);
    for _ in 0..8 {
        assert!(read(&bridge, &signer, &claims).await.is_ok());
    }
    assert!(read(&bridge, &signer, &claims).await.is_err());
    assert_eq!(backend.hits.load(Ordering::SeqCst), 8);
}

#[tokio::test]
async fn dedicated_bridge_rpc_rejects_cell_identity_and_duplicate_tickets() {
    use crate::interface::grpc::resource_bridge::PluginHostBridgeGrpc;
    use flare_grpc_proto::capability::{
        GenericRequest, plugin_host_bridge_client::PluginHostBridgeClient,
        plugin_host_bridge_server::PluginHostBridgeServer,
    };
    let (bridge, _, _, signer, claims, _) = fixture();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let authority = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(PluginHostBridgeServer::new(PluginHostBridgeGrpc(bridge)))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = stopped.await;
                },
            ),
    );
    let mut client = PluginHostBridgeClient::connect(authority).await.unwrap();
    let body = GenericRequest {
        operation: "im.conversation.read".into(),
        request_id: "req-1".into(),
        payload: Some(prost_types::Any {
            type_url: "json".into(),
            value: READ.into(),
        }),
        ..Default::default()
    };
    let mut raw = tonic::Request::new(body.clone());
    raw.metadata_mut()
        .insert("x-flare-ctx", "forged-user".parse().unwrap());
    assert_eq!(
        client.call(raw).await.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    let token = signer
        .sign(&claims, chrono::Utc::now().timestamp_millis())
        .unwrap();
    let mut request = tonic::Request::new(body.clone());
    request.metadata_mut().insert(
        flare_im_capability_core::plugin::invocation::PLUGIN_CALL_HEADER,
        token.parse().unwrap(),
    );
    assert!(client.call(request).await.unwrap().into_inner().ok);
    let mut duplicate = tonic::Request::new(body);
    for _ in 0..2 {
        duplicate.metadata_mut().append(
            flare_im_capability_core::plugin::invocation::PLUGIN_CALL_HEADER,
            token.parse().unwrap(),
        );
    }
    assert_eq!(
        client.call(duplicate).await.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}
