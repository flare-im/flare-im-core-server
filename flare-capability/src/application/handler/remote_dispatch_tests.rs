use super::*;
use crate::domain::repository::PluginAuthorizationReader;
use crate::infrastructure::capability::InMemoryCapabilityGrants;
use ed25519_dalek::{
    SigningKey,
    pkcs8::{EncodePrivateKey, EncodePublicKey},
};
use flare_im_capability_core::plugin::{
    authorization::TenantPluginGrant,
    invocation::{PLUGIN_CALL_HEADER, PluginCallSigner, PluginCallVerifier},
    runtime::*,
};
use flare_server_core::{SignedContextIdentity, SignedContextSigner};
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
fn snapshot(authority: String) -> PluginRuntimeSnapshot {
    let raw=serde_json::to_string(&serde_json::json!({"manifestVersion":1,"pluginId":"example.echo","version":"1.0.0","apiVersion":"1.0","displayName":"Echo","publisher":{"publisherId":"test","displayName":"Test"},"runtime":{"kind":"grpc","service":"example.echo.v1.PluginService","protocol":"flare.capability.v1"},"capabilities":[{"id":"example.echo","operations":[{"id":"run","inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"permissionIds":["example.echo.run"],"idempotent":true,"timeoutMs":1000}]}],"permissions":[{"id":"example.echo.run","description":"Run","dataScopes":[],"riskLevel":"low"}],"artifact":{"digest":format!("sha256:{}","a".repeat(64)),"signatureRef":"test"}})).unwrap();
    let digest = digest_bytes(raw.as_bytes());
    let now = chrono::Utc::now().timestamp_millis();
    PluginRuntimeSnapshot {
        tenant_id: "app-a".into(),
        version: 10,
        issued_at_ms: now,
        valid_until_ms: now + 300000,
        plugins: vec![ProjectedPlugin {
            grant: TenantPluginGrant {
                grant_id: "g1".into(),
                revision: 2,
                tenant_id: "app-a".into(),
                plugin_id: "example.echo".into(),
                plugin_version: "1.0.0".into(),
                manifest_digest: digest.clone(),
                approved_operations: ["example.echo.run".into()].into(),
                approved_permissions: ["example.echo.run".into()].into(),
                approved_data_scopes: Default::default(),
                enabled: true,
                revoked: false,
                valid_from_ms: now,
                valid_until_ms: now + 300000,
            },
            manifest_json: raw,
            deployment: PluginDeployment {
                deployment_id: "echo-1".into(),
                plugin_id: "example.echo".into(),
                plugin_version: "1.0.0".into(),
                manifest_digest: digest,
                artifact_digest: format!("sha256:{}", "a".repeat(64)),
                grpc_authority: authority,
            },
            configuration_digest: configuration_digest(&serde_json::json!({})).unwrap(),
        }],
    }
}
fn command() -> CapabilityDispatchCommand {
    CapabilityDispatchCommand {
        capability_id: "example.echo.run".into(),
        tenant_id: Some("app-a".into()),
        user_id: Some("u1".into()),
        conversation_id: Some("conversation-1".into()),
        payload: None,
        request_id: Some("r1".into()),
    }
}
fn policy() -> Arc<InMemoryCapabilityGrants> {
    let policy = Arc::new(InMemoryCapabilityGrants::new());
    policy.grant_user_capability("app-a", "u1", "example.echo.run", None, None, None);
    policy
}

#[tokio::test]
async fn registered_endpoint_cannot_bypass_absent_expired_disabled_or_revoked_projection() {
    let (ctx, _, _) = identity(SignedActorType::User);
    let reader = Arc::new(Projection(Mutex::new(None)));
    let routes = Arc::new(PluginRouteBook::new().with_runtime_authorization(
        reader.clone(),
        Some(Arc::new(
            PluginCallSigner::new("call".into(), [29; 32]).unwrap(),
        )),
    ));
    routes.upsert(
        "app-a",
        RegisteredPluginInstance {
            plugin_id: "unapproved".into(),
            capability_id: "example.echo.run".into(),
            grpc_authority: "http://127.0.0.1:1".into(),
            ..Default::default()
        },
    );
    let backend: Arc<dyn CapabilityPolicyBackend> = policy();
    for mode in 0..6 {
        let mut value = snapshot("http://127.0.0.1:1".into());
        match mode {
            0 => value.plugins.clear(),
            1 => value.valid_until_ms = 1,
            2 => value.plugins[0].grant.enabled = false,
            3 => value.plugins[0].grant.revoked = true,
            4 => value.plugins[0].grant.approved_permissions.clear(),
            _ => value.tenant_id = "other".into(),
        };
        *reader.0.lock().unwrap() = Some(value);
        assert!(matches!(
            dispatch_remote(&ctx, &command(), &routes, &backend, Duration::from_secs(1)).await,
            Err(CapabilityError::PolicyDenied(_))
        ));
    }
}

#[tokio::test]
async fn projected_target_gets_only_scoped_ticket_and_user_switch_still_applies() {
    use flare_grpc_proto::capability::{
        GenericResponse,
        extension_plugin_server::{ExtensionPlugin, ExtensionPluginServer},
    };
    struct Endpoint {
        verifier: PluginCallVerifier,
    }
    #[tonic::async_trait]
    impl ExtensionPlugin for Endpoint {
        async fn call(
            &self,
            request: Request<GenericRequest>,
        ) -> std::result::Result<tonic::Response<GenericResponse>, Status> {
            assert!(request.metadata().get("x-flare-ctx").is_none());
            let token = request
                .metadata()
                .get(PLUGIN_CALL_HEADER)
                .unwrap()
                .to_str()
                .unwrap();
            let claims = self
                .verifier
                .verify(token, chrono::Utc::now().timestamp_millis())
                .unwrap();
            assert_eq!(claims.conversation_id.as_deref(), Some("conversation-1"));
            assert_eq!(
                (
                    claims.tenant_id.as_str(),
                    claims.actor_id.as_str(),
                    claims.operation.as_str(),
                    claims.request_id.as_str(),
                    claims.deployment_id.as_str(),
                    claims.grant_revision
                ),
                ("app-a", "u1", "example.echo.run", "r1", "echo-1", 2)
            );
            Ok(tonic::Response::new(GenericResponse {
                ok: true,
                request_id: claims.request_id,
                ..Default::default()
            }))
        }
    }
    let signer = Arc::new(PluginCallSigner::new("call".into(), [29; 32]).unwrap());
    let mut verifier = PluginCallVerifier::default();
    verifier.insert("call".into(), signer.public_key()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ExtensionPluginServer::new(Endpoint { verifier }))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    rx.await.ok();
                },
            ),
    );
    let reader = Arc::new(Projection(Mutex::new(Some(snapshot(format!(
        "http://{addr}"
    ))))));
    let routes = Arc::new(PluginRouteBook::new().with_runtime_authorization(reader, Some(signer)));
    let (ctx, _, _) = identity(SignedActorType::User);
    let policy = policy();
    let backend: Arc<dyn CapabilityPolicyBackend> = policy.clone();
    let result = dispatch_remote(&ctx, &command(), &routes, &backend, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(result.success);
    policy.set_tenant_capability("app-a", "example.echo.run", false);
    assert!(matches!(
        dispatch_remote(&ctx, &command(), &routes, &backend, Duration::from_secs(1)).await,
        Err(CapabilityError::PolicyDenied(_))
    ));
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

fn identity(actor: SignedActorType) -> (Ctx, String, SignedContextVerifier) {
    let key = SigningKey::from_bytes(&[54; 32]);
    let signer =
        SignedContextSigner::from_private_key(&key.to_pkcs8_pem(Default::default()).unwrap())
            .unwrap();
    let verifier = SignedContextVerifier::from_public_key(
        &key.verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap(),
    )
    .unwrap();
    let token = signer
        .sign(&SignedContextIdentity::new(actor, "u1", "app-a", "r1"))
        .unwrap();
    let mut metadata = MetadataMap::new();
    metadata.insert("x-flare-ctx", token.parse().unwrap());
    (
        VerifiedDispatchOrigin::verify(&metadata, &verifier)
            .unwrap()
            .into_context(),
        token,
        verifier,
    )
}
#[test]
fn dispatch_identity_requires_exact_user_tenant_and_request() {
    let (ctx, _, _) = identity(SignedActorType::User);
    let origin = ctx.get_data::<VerifiedDispatchOrigin>().unwrap();
    assert!(origin.require_user_binding("app-a", "u1", "r1").is_ok());
    for (tenant, user, request) in [
        ("other", "u1", "r1"),
        ("app-a", "other", "r1"),
        ("app-a", "u1", "other"),
    ] {
        assert!(origin.require_user_binding(tenant, user, request).is_err());
    }
    let (ctx, _, _) = identity(SignedActorType::Service);
    assert!(
        ctx.get_data::<VerifiedDispatchOrigin>()
            .unwrap()
            .require_user_binding("app-a", "u1", "r1")
            .is_err()
    );
}
#[test]
fn absent_forged_and_duplicate_user_credentials_are_rejected() {
    let (_, token, verifier) = identity(SignedActorType::User);
    let mut metadata = MetadataMap::new();
    assert!(VerifiedDispatchOrigin::verify(&metadata, &verifier).is_err());
    metadata.insert("x-flare-ctx", "forged".parse().unwrap());
    assert!(VerifiedDispatchOrigin::verify(&metadata, &verifier).is_err());
    metadata.insert("x-flare-ctx", token.parse().unwrap());
    metadata.append("x-flare-ctx", token.parse().unwrap());
    assert!(VerifiedDispatchOrigin::verify(&metadata, &verifier).is_err());
}

#[tokio::test]
async fn authorization_storage_wait_is_bounded_by_dispatch_deadline() {
    struct Pending;
    #[async_trait::async_trait]
    impl PluginAuthorizationReader for Pending {
        async fn plugin_snapshot(
            &self,
            _: &str,
        ) -> flare_server_core::error::Result<Option<PluginRuntimeSnapshot>> {
            std::future::pending().await
        }
    }
    let (ctx, _, _) = identity(SignedActorType::User);
    let routes =
        Arc::new(PluginRouteBook::new().with_runtime_authorization(Arc::new(Pending), None));
    let backend: Arc<dyn CapabilityPolicyBackend> = policy();
    assert!(matches!(
        dispatch_remote(
            &ctx,
            &command(),
            &routes,
            &backend,
            Duration::from_millis(1)
        )
        .await,
        Err(CapabilityError::Timeout(_))
    ));
}

#[tokio::test]
async fn ui_catalog_uses_local_projection_actor_policy_and_strips_runtime_details() {
    let (ctx, _, _) = identity(SignedActorType::User);
    let mut projected = snapshot("http://127.0.0.1:1".into());
    let p = &mut projected.plugins[0];
    let grant_deadline = chrono::Utc::now().timestamp_millis() + 5000;
    p.grant.valid_until_ms = grant_deadline;
    let mut raw: Value = serde_json::from_str(&p.manifest_json).unwrap();
    raw["clientExtensions"] = serde_json::json!({"platforms":["web"],"contributions":[{"kind":"message-content","id":"example.echo.card","schemaVersion":1,"requiredPermissions":["example.echo.run"],"schema":{"title":"Echo","fields":[],"actions":[{"id":"run","label":"Run","target":{"kind":"operation","operation":"example.echo.run"}}]}}],"nativeCapabilities":[]});
    let mut other = raw["capabilities"][0]["operations"][0].clone();
    other["id"] = serde_json::json!("other");
    raw["capabilities"][0]["operations"]
        .as_array_mut()
        .unwrap()
        .push(other);
    raw["clientExtensions"]["contributions"][0]["schema"]["actions"].as_array_mut().unwrap().push(serde_json::json!({"id":"other","label":"Other","target":{"kind":"operation","operation":"example.echo.other"}}));
    raw["clientExtensions"]["contributions"][0]["schema"]["translations"] = serde_json::json!({"en-US":{"title":"Echo","fields":[],"actions":{"run":"Run","other":"Other"}}});
    p.manifest_json = serde_json::to_string(&raw).unwrap();
    p.grant.manifest_digest = digest_bytes(p.manifest_json.as_bytes());
    p.deployment.manifest_digest = p.grant.manifest_digest.clone();
    let reader = Arc::new(Projection(Mutex::new(Some(projected.clone()))));
    let routes = Arc::new(PluginRouteBook::new().with_runtime_authorization(reader.clone(), None));
    let mut command = command();
    command.capability_id = flare_im_capability_core::plugin::ui::UI_CATALOG_OPERATION.into();
    let backend: Arc<dyn CapabilityPolicyBackend> = policy();
    let result = dispatch_remote(&ctx, &command, &routes, &backend, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(result.data["plugins"][0]["pluginId"], "example.echo");
    assert!(result.data.to_string().contains("example.echo.run"));
    assert!(!result.data.to_string().contains("grpcAuthority"));
    assert!(!result.data.to_string().contains("configurationDigest"));
    assert!(!result.data.to_string().contains("example.echo.other"));
    let catalog: flare_im_capability_core::plugin::ui::UiCatalog =
        serde_json::from_value(result.data).unwrap();
    let schema = &catalog.plugins[0].extensions.contributions[0].schema;
    assert!(catalog.valid_until_ms <= grant_deadline);
    schema.validate().unwrap();
    assert!(!schema.translations["en-US"].actions.contains_key("other"));
    let denied: Arc<dyn CapabilityPolicyBackend> = Arc::new(InMemoryCapabilityGrants::new());
    let result = dispatch_remote(&ctx, &command, &routes, &denied, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(result.data["plugins"].as_array().unwrap().is_empty());
    projected.plugins[0].grant.enabled = false;
    *reader.0.lock().unwrap() = Some(projected);
    let result = dispatch_remote(&ctx, &command, &routes, &backend, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(result.data["plugins"].as_array().unwrap().is_empty());
}
