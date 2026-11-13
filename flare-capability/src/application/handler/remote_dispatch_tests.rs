use super::*;
use crate::application::handler::plugin_state::tests::{with_features, with_manifest};
use crate::domain::repository::PluginAuthorizationReader;
use crate::infrastructure::capability::InMemoryCapabilityGrants;
use ed25519_dalek::{
    SigningKey,
    pkcs8::{EncodePrivateKey, EncodePublicKey},
};
use flare_im_capability_core::plugin::{
    PluginManifest,
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
    ) -> flare_server_core::error::Result<Option<Arc<PluginRuntimeSnapshot>>> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .clone()
            .filter(|s| s.tenant_id == tenant)
            .map(Arc::new))
    }
}
fn echo_manifest() -> String {
    serde_json::to_string(&serde_json::json!({"manifestVersion":1,"pluginId":"example.echo","version":"1.0.0","apiVersion":"1.0","displayName":"Echo","publisher":{"publisherId":"test","displayName":"Test"},"runtime":{"kind":"grpc","service":"example.echo.v1.PluginService","protocol":"flare.capability.v1"},"capabilities":[{"id":"example.echo","operations":[{"id":"run","inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"permissionIds":["example.echo.run"],"idempotent":true,"timeoutMs":1000}]}],"permissions":[{"id":"example.echo.run","description":"Run","dataScopes":[],"riskLevel":"low"}],"features":[{"id":"run","label":{"en-US":"Run"},"gates":{"operations":["example.echo.run"]}}],"artifact":{"digest":format!("sha256:{}","a".repeat(64)),"signatureRef":"test"}})).unwrap()
}
fn snapshot(authority: String) -> PluginRuntimeSnapshot {
    let raw = echo_manifest();
    let digest = digest_bytes(raw.as_bytes());
    let now = chrono::Utc::now().timestamp_millis();
    PluginRuntimeSnapshot {
        organization_id: Some("org-1".into()),
        tenant_id: "app-a".into(),
        version: 10,
        issued_at_ms: now,
        valid_until_ms: now + 300000,
        plugins: vec![
            ResolvedPlugin::new(
                TenantPluginGrant {
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
                    suspended: false,
                    valid_from_ms: now,
                    valid_until_ms: now + 300000,
                },
                Arc::new(PluginManifest::from_json(raw.as_bytes()).unwrap()),
                PluginDeployment {
                    deployment_id: "echo-1".into(),
                    plugin_id: "example.echo".into(),
                    plugin_version: "1.0.0".into(),
                    manifest_digest: digest,
                    artifact_digest: format!("sha256:{}", "a".repeat(64)),
                    grpc_authority: authority,
                },
                configuration_digest(&serde_json::json!({})).unwrap(),
                Default::default(),
                [("run".to_owned(), true)].into(),
            )
            .unwrap(),
        ],
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
        message_id: None,
    }
}
/// What a web client of build 1 renders: the engine vocabulary, a bundle runtime when `bundle`.
fn catalog_descriptor(bundle: bool) -> flare_im_capability_core::plugin::ui::ClientUiDescriptor {
    use flare_im_capability_core::plugin::manifest::ClientPlatform;
    use flare_im_capability_core::plugin::ui::{
        BundleRuntime, ClientUiDescriptor, engine_vocabulary,
    };
    ClientUiDescriptor::new(
        &engine_vocabulary(),
        ClientPlatform::Web,
        1,
        "web",
        bundle.then(|| BundleRuntime {
            bridge: 1,
            engine: "browser".into(),
            engine_major: 120,
        }),
    )
}
/// The catalog request payload of [`catalog_descriptor`].
fn catalog_payload(bundle: bool) -> Value {
    serde_json::json!({"descriptor": catalog_descriptor(bundle)})
}
/// A policy backend with no user grant at all: plugin operations never need one.
fn policy() -> Arc<InMemoryCapabilityGrants> {
    Arc::new(InMemoryCapabilityGrants::new())
}

fn plugin_code(result: &Result<CapabilityDispatchResult>) -> Option<&str> {
    match result {
        Err(error) => error.plugin_code(),
        Ok(_) => None,
    }
}

#[tokio::test]
async fn dispatch_refuses_an_absent_expired_disabled_or_revoked_projection() {
    let (ctx, _, _) = identity(SignedActorType::User);
    let reader = Arc::new(Projection(Mutex::new(None)));
    let routes = Arc::new(PluginRouteBook::new().with_runtime_authorization(
        reader.clone(),
        Some(Arc::new(
            PluginCallSigner::new("call".into(), [29; 32]).unwrap(),
        )),
    ));
    let backend: Arc<dyn CapabilityPolicyBackend> = policy();
    use flare_im_service_kit::plugin_platform::{
        PLUGIN_DISABLED, PLUGIN_FEATURE_DISABLED, PLUGIN_NOT_INSTALLED, PLUGIN_PERMISSION_DENIED,
    };
    for (mode, code) in [
        (0, PLUGIN_NOT_INSTALLED),
        (1, PLUGIN_DISABLED),
        (2, PLUGIN_DISABLED),
        (3, PLUGIN_NOT_INSTALLED),
        (4, PLUGIN_PERMISSION_DENIED),
        (5, PLUGIN_NOT_INSTALLED),
        (6, PLUGIN_DISABLED),
        (7, PLUGIN_FEATURE_DISABLED),
        (8, PLUGIN_FEATURE_DISABLED),
    ] {
        let mut value = snapshot("http://127.0.0.1:1".into());
        match mode {
            0 => value.plugins.clear(),
            1 => value.valid_until_ms = 1,
            2 => value.plugins[0].grant.enabled = false,
            3 => value.plugins[0].grant.revoked = true,
            4 => value.plugins[0].grant.approved_permissions.clear(),
            5 => value.tenant_id = "other".into(),
            6 => value.plugins[0].grant.suspended = true,
            7 => {
                value.plugins[0] =
                    with_features(&value.plugins[0], [("run".to_owned(), false)].into())
            }
            _ => value.plugins[0] = with_features(&value.plugins[0], Default::default()),
        };
        *reader.0.lock().unwrap() = Some(value);
        let result =
            dispatch_remote(&ctx, &command(), &routes, &backend, Duration::from_secs(1)).await;
        assert_eq!(plugin_code(&result), Some(code), "mode {mode}");
    }
}

#[tokio::test]
async fn an_installed_plugin_needs_no_user_grant_and_the_emergency_switch_still_stops_it() {
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
    // An absent tenant switch is no refusal; a switch turned off is the emergency stop.
    policy.set_tenant_capability("app-a", "example.echo.run", false);
    let result = dispatch_remote(&ctx, &command(), &routes, &backend, Duration::from_secs(1)).await;
    assert_eq!(
        plugin_code(&result),
        Some(flare_im_service_kit::plugin_platform::PLUGIN_DISABLED)
    );
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
        ) -> flare_server_core::error::Result<Option<Arc<PluginRuntimeSnapshot>>> {
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
async fn ui_catalog_uses_local_projection_and_strips_runtime_details() {
    let (ctx, _, _) = identity(SignedActorType::User);
    let mut projected = snapshot("http://127.0.0.1:1".into());
    let grant_deadline = chrono::Utc::now().timestamp_millis() + 5000;
    projected.plugins[0].grant.valid_until_ms = grant_deadline;
    let mut raw: Value = serde_json::from_str(&echo_manifest()).unwrap();
    raw["clientExtensions"] = serde_json::json!({"platforms":["web"],"contributions":[{"kind":"message-content","id":"example.echo.card","schemaVersion":1,"requiredPermissions":["example.echo.run"],"schema":{"title":"Echo","fields":[],"actions":[{"id":"run","label":"Run","target":{"kind":"operation","operation":"example.echo.run"}}]}}],"nativeCapabilities":[]});
    let mut other = raw["capabilities"][0]["operations"][0].clone();
    other["id"] = serde_json::json!("other");
    raw["capabilities"][0]["operations"]
        .as_array_mut()
        .unwrap()
        .push(other);
    raw["clientExtensions"]["contributions"][0]["schema"]["actions"].as_array_mut().unwrap().push(serde_json::json!({"id":"other","label":"Other","target":{"kind":"operation","operation":"example.echo.other"}}));
    raw["clientExtensions"]["contributions"][0]["schema"]["translations"] = serde_json::json!({"en-US":{"title":"Echo","fields":[],"actions":{"run":"Run","other":"Other"}}});
    projected.plugins[0] =
        with_manifest(&projected.plugins[0], &serde_json::to_string(&raw).unwrap());
    let reader = Arc::new(Projection(Mutex::new(Some(projected.clone()))));
    let routes = Arc::new(PluginRouteBook::new().with_runtime_authorization(reader.clone(), None));
    let mut command = command();
    command.capability_id = flare_im_capability_core::plugin::ui::UI_CATALOG_OPERATION.into();
    command.payload = Some(catalog_payload(false));
    let backend: Arc<dyn CapabilityPolicyBackend> = policy();
    let result = dispatch_remote(&ctx, &command, &routes, &backend, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(result.data["plugins"][0]["pluginId"], "example.echo");
    assert!(result.data.to_string().contains("example.echo.run"));
    assert!(!result.data.to_string().contains("grpcAuthority"));
    assert!(!result.data.to_string().contains("configurationDigest"));
    assert!(!result.data.to_string().contains("example.echo.other"));
    assert!(
        result.data["plugins"][0].get("queryOperations").is_none(),
        "no query operation is offered"
    );
    let catalog: flare_im_capability_core::plugin::ui::UiCatalog =
        serde_json::from_value(result.data).unwrap();
    let schema = &catalog.plugins[0].extensions.contributions[0].schema;
    assert!(catalog.valid_until_ms <= grant_deadline);
    schema.validate().unwrap();
    assert!(!schema.translations["en-US"].actions.contains_key("other"));
    assert!(catalog.unavailable.is_empty());
    // Stopping the plugin's only offered operation leaves nothing to show: the plugin is listed
    // as disabled, so its cards say why they are inert.
    let stopped = policy();
    stopped.set_tenant_capability("app-a", "example.echo.run", false);
    let stopped: Arc<dyn CapabilityPolicyBackend> = stopped;
    let result = dispatch_remote(&ctx, &command, &routes, &stopped, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(result.data["plugins"].as_array().unwrap().is_empty());
    assert_eq!(
        result.data["unavailable"],
        serde_json::json!([{"pluginId": "example.echo", "reason": "disabled"}])
    );
    // Disabled or revoked installations are listed with their reason.
    for (disable, reason) in [(true, "disabled"), (false, "not_installed")] {
        let mut value = projected.clone();
        if disable {
            value.plugins[0].grant.enabled = false;
        } else {
            value.plugins[0].grant.revoked = true;
        }
        *reader.0.lock().unwrap() = Some(value);
        let result = dispatch_remote(&ctx, &command, &routes, &backend, Duration::from_secs(1))
            .await
            .unwrap();
        assert!(result.data["plugins"].as_array().unwrap().is_empty());
        assert_eq!(
            result.data["unavailable"],
            serde_json::json!([{"pluginId": "example.echo", "reason": reason}])
        );
    }
}

/// There is no catalog without a client UI descriptor: a missing, unreadable or out-of-bounds
/// one is `INVALID_ARGUMENT`, before the projection is read.
#[tokio::test]
async fn ui_catalog_requires_a_valid_client_ui_descriptor() {
    let (ctx, _, _) = identity(SignedActorType::User);
    let reader = Arc::new(Projection(Mutex::new(Some(snapshot(
        "http://127.0.0.1:1".into(),
    )))));
    let routes = Arc::new(PluginRouteBook::new().with_runtime_authorization(reader, None));
    let backend: Arc<dyn CapabilityPolicyBackend> = policy();
    let mut bad = serde_json::to_value(catalog_descriptor(false)).unwrap();
    bad["appBuild"] = serde_json::json!(-1);
    // Unknown fields are ignored but still count toward the size bound.
    let mut padded = serde_json::to_value(catalog_descriptor(false)).unwrap();
    padded["padding"] = serde_json::json!("x".repeat(5000));
    for payload in [
        Some(serde_json::json!({"descriptor": padded})),
        None,
        Some(Value::Null),
        Some(serde_json::json!({})),
        Some(serde_json::json!({"descriptor": {"platform": "web"}})),
        Some(serde_json::json!({"descriptor": bad})),
    ] {
        let mut request = command();
        request.capability_id = flare_im_capability_core::plugin::ui::UI_CATALOG_OPERATION.into();
        request.payload = payload.clone();
        let error = dispatch_remote(&ctx, &request, &routes, &backend, Duration::from_secs(1))
            .await
            .unwrap_err();
        let status = crate::interface::grpc::shared::helpers::capability_error_to_status(error);
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{payload:?}");
    }
    assert_eq!(routes.catalog_selection.cached(), 0);
}

/// The task board card of the plan in miniature: a web bundle variant, then the declarative
/// card. One projection, two descriptors, two variants; a `minClient` floor above the client's
/// build gives the upgrade placeholder; with web bundles switched off for the deployment a
/// client with a bundle runtime gets the declarative variant. Selections are cached per
/// projection version and descriptor.
#[tokio::test]
async fn ui_catalog_selects_contribution_variants_per_client_descriptor() {
    use flare_im_capability_core::plugin::ui::{UiBundleRef, UiCatalog, UiUpgrade};
    let (ctx, _, _) = identity(SignedActorType::User);
    let manifest = |min_client: Option<Value>| {
        let mut raw: Value = serde_json::from_str(&echo_manifest()).unwrap();
        raw["clientExtensions"] = serde_json::json!({"platforms":["web"],"nativeCapabilities":[],
            "webBundles":[{"id":"board","sha256":"ab".repeat(32),"size":4096,"bridge":1}],
            // A page: message cards stay declarative and never take a bundle variant.
            "contributions":[{"kind":"page","id":"example.echo.board","schemaVersion":1,
                "requiredPermissions":["example.echo.run"],
                "schema":{"title":"Echo","fields":[],"actions":[{"id":"run","label":"Run",
                    "target":{"kind":"operation","operation":"example.echo.run"}}],
                    "translations":{"en-US":{"title":"Echo board","fields":[],"actions":{"run":"Run"}}}},
                "variants":[
                    {"id":"board","requires":{"bundle":{"id":"board","bridge":1}},
                     "bundle":{"id":"board","route":"/"}},
                    {"id":"basic"}]}]});
        if let Some(min_client) = min_client {
            raw["clientExtensions"]["contributions"][0]["minClient"] = min_client;
        }
        serde_json::to_string(&raw).unwrap()
    };
    let mut projected = snapshot("http://127.0.0.1:1".into());
    projected.plugins[0] = with_manifest(&projected.plugins[0], &manifest(None))
        .with_web_bundles(vec![board_entry()])
        .unwrap();
    let reader = Arc::new(Projection(Mutex::new(Some(projected.clone()))));
    let routes = Arc::new(PluginRouteBook::new().with_runtime_authorization(reader.clone(), None));
    let backend: Arc<dyn CapabilityPolicyBackend> = policy();
    let catalog = |routes: Arc<PluginRouteBook>, bundle: bool| {
        let ctx = ctx.clone();
        let backend = backend.clone();
        async move {
            let mut request = command();
            request.capability_id =
                flare_im_capability_core::plugin::ui::UI_CATALOG_OPERATION.into();
            request.payload = Some(catalog_payload(bundle));
            let result = dispatch_remote(&ctx, &request, &routes, &backend, Duration::from_secs(1))
                .await
                .unwrap();
            let catalog: UiCatalog = serde_json::from_value(result.data).unwrap();
            assert!(catalog.unavailable.is_empty());
            catalog.plugins[0].extensions.contributions[0].clone()
        }
    };

    let board = catalog(routes.clone(), true).await;
    assert_eq!(board.variant.as_deref(), Some("board"));
    assert_eq!(
        board.bundle,
        Some(UiBundleRef {
            id: "board".into(),
            route: "/".into()
        })
    );
    assert!(board.variants.is_empty() && board.min_client.is_empty() && board.upgrade.is_none());
    let basic = catalog(routes.clone(), false).await;
    assert_eq!(basic.variant.as_deref(), Some("basic"));
    assert!(basic.bundle.is_none(), "a bundle is never inherited");
    assert_eq!(basic.id, board.id, "identity is the same in every variant");
    assert_eq!(basic.schema.actions.len(), 1);
    // One selection per descriptor of this projection version.
    assert_eq!(routes.catalog_selection.cached(), 2);
    let _ = catalog(routes.clone(), true).await;
    assert_eq!(
        routes.catalog_selection.cached(),
        2,
        "the same descriptor hits the cache"
    );

    // Web bundles switched off: only declarative variants, whatever the client offers.
    let declarative = Arc::new(
        PluginRouteBook::new()
            .with_runtime_authorization(reader.clone(), None)
            .with_web_bundles(false),
    );
    let chosen = catalog(declarative.clone(), true).await;
    assert_eq!(chosen.variant.as_deref(), Some("basic"));
    assert!(chosen.bundle.is_none());
    assert_eq!(declarative.catalog_selection.cached(), 1);
    let _ = catalog(declarative.clone(), false).await;
    assert_eq!(
        declarative.catalog_selection.cached(),
        1,
        "without bundles both clients share one selection"
    );

    // A floor above the client's build: the upgrade placeholder, title and translations only.
    // A new projection version is a new selection.
    let mut floored = projected.clone();
    floored.version += 1;
    floored.plugins[0] = with_manifest(
        &floored.plugins[0],
        &manifest(Some(serde_json::json!({"web": 5}))),
    )
    .with_web_bundles(vec![board_entry()])
    .unwrap();
    *reader.0.lock().unwrap() = Some(floored);
    let placeholder = catalog(routes.clone(), true).await;
    assert_eq!(
        placeholder.upgrade,
        Some(UiUpgrade {
            min_build: Some(5),
            unsupported: false
        })
    );
    assert_eq!(placeholder.id, "example.echo.board");
    assert_eq!(placeholder.schema.title, "Echo");
    assert_eq!(placeholder.schema.translations["en-US"].title, "Echo board");
    assert!(placeholder.schema.actions.is_empty() && placeholder.bundle.is_none());
    assert_eq!(routes.catalog_selection.cached(), 3);
}

/// The served `board` bundle of the board manifests below: the manifest's declaration with the
/// application's mirror addresses, unpacked files and approved bridge permissions.
fn board_entry() -> flare_im_capability_core::plugin::bundle::WebBundleDecl {
    use flare_im_capability_core::plugin::bridge::BridgePermission;
    flare_im_capability_core::plugin::bundle::WebBundleDecl {
        id: "board".into(),
        sha256: "ab".repeat(32),
        size: 4096,
        bridge: 1,
        digital_goods: false,
        urls: vec![
            "https://m1.example.com/b/board.fwb".into(),
            "https://m2.example.net/b/board.fwb".into(),
        ],
        files: Some(format!("https://m1.example.com/b/{}/", "ab".repeat(32))),
        permissions: [BridgePermission::PluginQuery].into(),
    }
}

/// A board page with a web bundle variant and a declarative one.
fn board_manifest(digital_goods: bool) -> String {
    let mut raw: Value = serde_json::from_str(&echo_manifest()).unwrap();
    raw["clientExtensions"] = serde_json::json!({"platforms":["web","android","ios"],"nativeCapabilities":[],
        "webBundles":[{"id":"board","sha256":"ab".repeat(32),"size":4096,"bridge":1,"digitalGoods":digital_goods}],
        "contributions":[{"kind":"page","id":"example.echo.board","schemaVersion":1,
            "requiredPermissions":["example.echo.run"],
            "schema":{"title":"Echo","fields":[],"actions":[{"id":"run","label":"Run",
                "target":{"kind":"operation","operation":"example.echo.run"}}]},
            "variants":[
                {"id":"board","requires":{"bundle":{"id":"board","bridge":1}},
                 "bundle":{"id":"board","route":"/"}},
                {"id":"basic"}]}]});
    serde_json::to_string(&raw).unwrap()
}

/// The catalog plugin a client of `platform` with a bundle runtime gets from `reader`.
async fn bundle_catalog(
    reader: Arc<Projection>,
    platform: flare_im_capability_core::plugin::manifest::ClientPlatform,
) -> flare_im_capability_core::plugin::ui::ClientPlugin {
    use flare_im_capability_core::plugin::ui::{
        BundleRuntime, ClientUiDescriptor, UiCatalog, engine_vocabulary,
    };
    let (ctx, _, _) = identity(SignedActorType::User);
    let routes = Arc::new(PluginRouteBook::new().with_runtime_authorization(reader, None));
    let backend: Arc<dyn CapabilityPolicyBackend> = policy();
    let descriptor = ClientUiDescriptor::new(
        &engine_vocabulary(),
        platform,
        1,
        "web",
        Some(BundleRuntime {
            bridge: 1,
            engine: "webview".into(),
            engine_major: 120,
        }),
    );
    let mut request = command();
    request.capability_id = flare_im_capability_core::plugin::ui::UI_CATALOG_OPERATION.into();
    request.payload = Some(serde_json::json!({ "descriptor": descriptor }));
    let result = dispatch_remote(&ctx, &request, &routes, &backend, Duration::from_secs(1))
        .await
        .unwrap();
    let catalog: UiCatalog = serde_json::from_value(result.data).unwrap();
    catalog.plugins[0].clone()
}

/// `webBundles` in the catalog: the served bundle a selected contribution opens, with the
/// application's addresses and approved permissions; `files` only for Web and Tauri; nothing
/// when the bundle is not served (not approved, withdrawn or not projected), and the declarative
/// variant is chosen instead.
#[tokio::test]
async fn ui_catalog_lists_the_served_web_bundles_a_selected_contribution_opens() {
    use flare_im_capability_core::plugin::manifest::ClientPlatform;
    let mut projected = snapshot("http://127.0.0.1:1".into());
    projected.plugins[0] = with_manifest(&projected.plugins[0], &board_manifest(false))
        .with_web_bundles(vec![board_entry()])
        .unwrap();
    let reader = Arc::new(Projection(Mutex::new(Some(projected.clone()))));

    let web = bundle_catalog(reader.clone(), ClientPlatform::Web).await;
    assert_eq!(
        web.extensions.contributions[0].variant.as_deref(),
        Some("board")
    );
    assert_eq!(web.extensions.web_bundles, vec![board_entry()]);

    let android = bundle_catalog(reader.clone(), ClientPlatform::Android).await;
    assert_eq!(
        android.extensions.contributions[0].variant.as_deref(),
        Some("board")
    );
    let mut entry = board_entry();
    entry.files = None;
    assert_eq!(
        android.extensions.web_bundles,
        vec![entry],
        "native clients read the .fwb file, not the unpacked copy"
    );

    // Withdrawn in the registry (or never approved): the resolution serves no bundle.
    let mut withdrawn = projected.clone();
    withdrawn.plugins[0] = with_manifest(&projected.plugins[0], &board_manifest(false));
    *reader.0.lock().unwrap() = Some(withdrawn);
    let fallback = bundle_catalog(reader.clone(), ClientPlatform::Web).await;
    assert_eq!(
        fallback.extensions.contributions[0].variant.as_deref(),
        Some("basic")
    );
    assert!(fallback.extensions.web_bundles.is_empty());
    assert!(fallback.extensions.contributions[0].bundle.is_none());
}

/// A bundle selling digital goods is never selected for iOS, and then not listed either.
#[tokio::test]
async fn ui_catalog_keeps_digital_goods_bundles_away_from_ios() {
    use flare_im_capability_core::plugin::manifest::ClientPlatform;
    let mut projected = snapshot("http://127.0.0.1:1".into());
    let mut entry = board_entry();
    entry.digital_goods = true;
    projected.plugins[0] = with_manifest(&projected.plugins[0], &board_manifest(true))
        .with_web_bundles(vec![entry.clone()])
        .unwrap();
    let reader = Arc::new(Projection(Mutex::new(Some(projected))));
    let ios = bundle_catalog(reader.clone(), ClientPlatform::Ios).await;
    assert_eq!(
        ios.extensions.contributions[0].variant.as_deref(),
        Some("basic")
    );
    assert!(ios.extensions.web_bundles.is_empty());
    let web = bundle_catalog(reader, ClientPlatform::Web).await;
    assert_eq!(web.extensions.web_bundles, vec![entry]);
}

/// A served bundle must be the manifest's declaration: another digest is refused.
#[test]
fn a_resolved_plugin_serves_only_declared_web_bundles() {
    let projected = snapshot("http://127.0.0.1:1".into());
    let plugin = with_manifest(&projected.plugins[0], &board_manifest(false));
    let mut other = board_entry();
    other.sha256 = "cd".repeat(32);
    assert!(plugin.clone().with_web_bundles(vec![other]).is_err());
    let mut unknown = board_entry();
    unknown.id = "other".into();
    assert!(plugin.clone().with_web_bundles(vec![unknown]).is_err());
    let mut no_urls = board_entry();
    no_urls.urls.clear();
    assert!(plugin.clone().with_web_bundles(vec![no_urls]).is_err());
    assert!(
        plugin
            .clone()
            .with_web_bundles(vec![board_entry(), board_entry()])
            .is_err()
    );
    assert!(plugin.with_web_bundles(vec![board_entry()]).is_ok());
}

/// Resolution serves a projected bundle only when the manifest declares it with that digest and
/// the registry holds it approved with that digest; an approval must match the manifest.
#[test]
fn served_web_bundles_need_the_manifest_the_registry_and_the_projection_to_agree() {
    use crate::domain::plugin::{WebBundleApproval, served_web_bundles};
    use flare_im_capability_core::plugin::bridge::BridgePermission;
    let manifest = PluginManifest::from_json(board_manifest(false).as_bytes()).unwrap();
    let projected = ProjectedWebBundle {
        id: "board".into(),
        sha256: "ab".repeat(32),
        urls: board_entry().urls,
        files: board_entry().files,
        permissions: [BridgePermission::PluginQuery].into(),
    };
    let approved: std::collections::BTreeMap<String, String> =
        [("board".to_owned(), "ab".repeat(32))].into();
    assert_eq!(
        served_web_bundles(&manifest, std::slice::from_ref(&projected), Some(&approved)),
        vec![board_entry()]
    );
    assert!(served_web_bundles(&manifest, std::slice::from_ref(&projected), None).is_empty());
    let stale: std::collections::BTreeMap<String, String> =
        [("board".to_owned(), "cd".repeat(32))].into();
    assert!(
        served_web_bundles(&manifest, std::slice::from_ref(&projected), Some(&stale)).is_empty(),
        "the registry approved other bytes"
    );
    let mut forged = projected.clone();
    forged.sha256 = "cd".repeat(32);
    assert!(
        served_web_bundles(&manifest, &[forged], Some(&stale)).is_empty(),
        "the manifest signs another digest"
    );
    let approval = |sha256: String, size: u64| WebBundleApproval {
        plugin_id: "example.echo".into(),
        plugin_version: "1.0.0".into(),
        bundle_id: "board".into(),
        sha256,
        size,
    };
    assert!(approval("ab".repeat(32), 4096).check(&manifest).is_ok());
    assert!(approval("cd".repeat(32), 4096).check(&manifest).is_err());
    assert!(approval("ab".repeat(32), 4097).check(&manifest).is_err());
    let mut other = approval("ab".repeat(32), 4096);
    other.bundle_id = "other".into();
    assert!(other.check(&manifest).is_err());
}

#[tokio::test]
async fn ui_catalog_query_operations_are_only_authorized_signed_queries() {
    let (ctx, _, _) = identity(SignedActorType::User);
    let mut raw: Value = serde_json::from_str(&echo_manifest()).unwrap();
    raw["clientExtensions"] = serde_json::json!({"platforms":["web"],"contributions":[{"kind":"message-content","id":"example.echo.card","schemaVersion":1,"requiredPermissions":[],"schema":{"title":"Echo","actions":[{"id":"run","label":"Run","target":{"kind":"operation","operation":"example.echo.run"}}]}}]});
    let original = raw["capabilities"][0]["operations"][0].clone();
    for id in ["allowed", "unapproved", "no-permission", "stopped"] {
        let mut query = original.clone();
        query["id"] = serde_json::json!(id);
        query["mode"] = serde_json::json!("query");
        if id == "no-permission" {
            query["permissionIds"] = serde_json::json!(["example.echo.private"]);
        }
        raw["capabilities"][0]["operations"]
            .as_array_mut()
            .unwrap()
            .push(query);
    }
    let mut permission = raw["permissions"][0].clone();
    permission["id"] = serde_json::json!("example.echo.private");
    raw["permissions"].as_array_mut().unwrap().push(permission);
    raw["clientExtensions"]["contributions"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
            "kind":"page","id":"example.echo.recovery","schemaVersion":1,"requiredPermissions":[],
            "schema":{"title":"Recovery","actions":[{"id":"observe","label":"Check original",
                "target":{"kind":"operation","operation":"example.echo.allowed"},
                "command":{"slot":"example.echo.workflow","mode":"observe"}}]}
        }));
    let mut projected = snapshot("http://127.0.0.1:1".into());
    projected.plugins[0] =
        with_manifest(&projected.plugins[0], &serde_json::to_string(&raw).unwrap());
    for id in ["allowed", "no-permission", "stopped"] {
        projected.plugins[0]
            .grant
            .approved_operations
            .insert(format!("example.echo.{id}"));
    }
    let reader = Arc::new(Projection(Mutex::new(Some(projected))));
    let routes = Arc::new(PluginRouteBook::new().with_runtime_authorization(reader, None));
    let backend_config = policy();
    backend_config.set_tenant_capability("app-a", "example.echo.stopped", false);
    let backend: Arc<dyn CapabilityPolicyBackend> = backend_config.clone();
    let mut request = command();
    request.capability_id = flare_im_capability_core::plugin::ui::UI_CATALOG_OPERATION.into();
    request.payload = Some(catalog_payload(false));
    let result = dispatch_remote(&ctx, &request, &routes, &backend, Duration::from_secs(1))
        .await
        .unwrap();
    let catalog: flare_im_capability_core::plugin::ui::UiCatalog =
        serde_json::from_value(result.data).unwrap();
    let plugin = &catalog.plugins[0];
    assert_eq!(
        plugin.query_operations,
        ["example.echo.allowed".into()].into()
    );
    assert!(plugin.query_operations.is_subset(&plugin.operations));
    assert!(plugin.operations.contains("example.echo.run"));
    backend_config.set_tenant_capability("app-a", "example.echo.allowed", false);
    let closed = dispatch_remote(&ctx, &request, &routes, &backend, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(
        closed.data["plugins"][0].get("queryOperations").is_none(),
        "a stopped query is not offered"
    );
}

#[tokio::test]
async fn ui_catalog_lists_the_authorized_transient_operations() {
    let (ctx, _, _) = identity(SignedActorType::User);
    let mut raw: Value = serde_json::from_str(&echo_manifest()).unwrap();
    raw["clientExtensions"] = serde_json::json!({"platforms":["web"],"contributions":[{"kind":"message-content","id":"example.echo.card","schemaVersion":1,"requiredPermissions":[],"schema":{"title":"Echo","actions":[{"id":"run","label":"Run","target":{"kind":"operation","operation":"example.echo.run"}}]}}]});
    let original = raw["capabilities"][0]["operations"][0].clone();
    for id in ["pay", "unapproved-pay"] {
        let mut transient = original.clone();
        transient["id"] = serde_json::json!(id);
        transient["mode"] = serde_json::json!("command");
        transient["transient"] = serde_json::json!(true);
        raw["capabilities"][0]["operations"]
            .as_array_mut()
            .unwrap()
            .push(transient);
    }
    let mut projected = snapshot("http://127.0.0.1:1".into());
    projected.plugins[0] =
        with_manifest(&projected.plugins[0], &serde_json::to_string(&raw).unwrap());
    projected.plugins[0]
        .grant
        .approved_operations
        .insert("example.echo.pay".into());
    let reader = Arc::new(Projection(Mutex::new(Some(projected))));
    let routes = Arc::new(PluginRouteBook::new().with_runtime_authorization(reader, None));
    let backend: Arc<dyn CapabilityPolicyBackend> = policy();
    let mut request = command();
    request.capability_id = flare_im_capability_core::plugin::ui::UI_CATALOG_OPERATION.into();
    request.payload = Some(catalog_payload(false));
    let result = dispatch_remote(&ctx, &request, &routes, &backend, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(
        result.data["plugins"][0]["transientOperations"],
        serde_json::json!(["example.echo.pay"])
    );
    let catalog: flare_im_capability_core::plugin::ui::UiCatalog =
        serde_json::from_value(result.data).unwrap();
    let plugin = &catalog.plugins[0];
    assert_eq!(
        plugin.transient_operations,
        ["example.echo.pay".into()].into()
    );
    assert!(plugin.transient_operations.is_subset(&plugin.operations));
    assert!(!plugin.transient_operations.contains("example.echo.run"));
    assert!(plugin.query_operations.is_empty());
}

/// Design 6.3: an action whose operation a closed feature gates leaves every surface, including a
/// page that stays open (the wallet's 「开户」 on its home page, V5c-W5); reopening brings it back.
#[tokio::test]
async fn a_closed_feature_drops_the_page_actions_targeting_its_operations() {
    let (ctx, _, _) = identity(SignedActorType::User);
    let mut raw: Value = serde_json::from_str(&echo_manifest()).unwrap();
    let mut other = raw["capabilities"][0]["operations"][0].clone();
    other["id"] = serde_json::json!("other");
    let mut home = other.clone();
    home["id"] = serde_json::json!("home");
    home["mode"] = serde_json::json!("query");
    raw["capabilities"][0]["operations"]
        .as_array_mut()
        .unwrap()
        .extend([other, home]);
    raw["clientExtensions"] = serde_json::json!({"platforms":["web"],"contributions":[{"kind":"page","id":"example.echo.page.home","schemaVersion":1,"requiredPermissions":["example.echo.run"],"source":{"operation":"example.echo.home"},"schema":{"title":"Home","fields":[],"actions":[{"id":"open","label":"Open","target":{"kind":"operation","operation":"example.echo.run"}},{"id":"other","label":"Other","target":{"kind":"operation","operation":"example.echo.other"}}],"translations":{"en-US":{"title":"Home","fields":[],"actions":{"open":"Open","other":"Other"}}}}}],"nativeCapabilities":[]});
    let mut projected = snapshot("http://127.0.0.1:1".into());
    projected.plugins[0] =
        with_manifest(&projected.plugins[0], &serde_json::to_string(&raw).unwrap());
    for operation in ["example.echo.other", "example.echo.home"] {
        projected.plugins[0]
            .grant
            .approved_operations
            .insert(operation.into());
    }
    let reader = Arc::new(Projection(Mutex::new(None)));
    let routes = Arc::new(PluginRouteBook::new().with_runtime_authorization(reader.clone(), None));
    let mut request = command();
    request.capability_id = flare_im_capability_core::plugin::ui::UI_CATALOG_OPERATION.into();
    request.payload = Some(catalog_payload(false));
    let backend: Arc<dyn CapabilityPolicyBackend> = policy();
    for (open, expected) in [(false, vec!["other"]), (true, vec!["open", "other"])] {
        let mut value = projected.clone();
        value.plugins[0] = with_features(&value.plugins[0], [("run".to_owned(), open)].into());
        *reader.0.lock().unwrap() = Some(value);
        let result = dispatch_remote(&ctx, &request, &routes, &backend, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            result.data["plugins"][0]["queryOperations"],
            serde_json::json!(["example.echo.home"]),
            "every authorized query is listed, Observe actions or not"
        );
        let catalog: flare_im_capability_core::plugin::ui::UiCatalog =
            serde_json::from_value(result.data).unwrap();
        let plugin = &catalog.plugins[0];
        assert_eq!(
            plugin.operations.contains("example.echo.run"),
            open,
            "open={open}"
        );
        let page = &plugin.extensions.contributions[0];
        assert_eq!(page.id, "example.echo.page.home");
        let actions: Vec<&str> = page.schema.actions.iter().map(|a| a.id.as_str()).collect();
        assert_eq!(actions, expected, "open={open}");
        let translated: Vec<&str> = page.schema.translations["en-US"]
            .actions
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(translated, expected, "open={open}");
        page.schema.validate().unwrap();
    }
}

/// The platform stamps a changed projection with its database clock. With Core's clock 2.85 s
/// behind (V4 full stack), every plugin of the application read `PLUGIN_DISABLED` until Core
/// caught up, `status` and `claim` included. A start within the tolerated skew is in effect; one
/// beyond it, an expiry or a disable still closes the plugin.
#[tokio::test]
async fn projection_issued_ahead_of_core_clock_keeps_plugins_in_effect() {
    use flare_im_capability_core::plugin::authorization::CLOCK_SKEW_MARGIN_MS;
    use flare_im_service_kit::plugin_platform::PLUGIN_DISABLED;
    let now = chrono::Utc::now().timestamp_millis();
    let mut value = snapshot("http://127.0.0.1:1".into());
    value.issued_at_ms = now + 2_850;
    value.valid_until_ms = now + 300_000;
    value.plugins[0].grant.valid_from_ms = value.issued_at_ms;
    value.plugins[0].grant.valid_until_ms = value.valid_until_ms;
    value.validate(now).unwrap();
    let backend: Arc<dyn CapabilityPolicyBackend> = policy();
    assert_eq!(
        authorize_user_operation(&value, "app-a", "u1", "example.echo.run", &backend, now)
            .await
            .unwrap(),
        0
    );

    let (ctx, _, _) = identity(SignedActorType::User);
    let mut request = command();
    request.capability_id = flare_im_capability_core::plugin::ui::UI_CATALOG_OPERATION.into();
    request.payload = Some(catalog_payload(false));
    let reader = Arc::new(Projection(Mutex::new(Some(value.clone()))));
    let routes = Arc::new(PluginRouteBook::new().with_runtime_authorization(reader, None));
    let catalog = dispatch_remote(&ctx, &request, &routes, &backend, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(
        catalog.data["unavailable"]
            .as_array()
            .is_none_or(|unavailable| unavailable.is_empty()),
        "{}",
        catalog.data
    );

    for close in ["start beyond skew", "expired", "disabled"] {
        let mut closed = value.clone();
        let grant = &mut closed.plugins[0].grant;
        match close {
            "start beyond skew" => grant.valid_from_ms = now + CLOCK_SKEW_MARGIN_MS + 1,
            "expired" => grant.valid_until_ms = now,
            _ => grant.enabled = false,
        }
        let result =
            authorize_user_operation(&closed, "app-a", "u1", "example.echo.run", &backend, now)
                .await;
        let Err(error) = result else {
            panic!("{close} must close the plugin");
        };
        assert_eq!(error.plugin_code(), Some(PLUGIN_DISABLED), "{close}");
    }
}

#[tokio::test]
async fn catalog_cross_plugin_pages_require_an_enabled_approved_destination() {
    fn projected(id: &str, actions: Value) -> ResolvedPlugin {
        let base = snapshot("http://127.0.0.1:1".into()).plugins.remove(0);
        let mut raw: Value =
            serde_json::from_str(&echo_manifest().replace("example.echo", id)).unwrap();
        raw["clientExtensions"] = serde_json::json!({"platforms":["web"], "nativeCapabilities":[],
            "contributions":[{"kind":"page","id":format!("{id}.home"),"schemaVersion":1,
                "requiredPermissions":[format!("{id}.run")],
                "schema":{"title":"Home","fields":[],"actions":actions}}]});
        let raw = serde_json::to_string(&raw).unwrap();
        let digest = digest_bytes(raw.as_bytes());
        let mut grant = base.grant.clone();
        grant.plugin_id = id.into();
        grant.grant_id = format!("grant-{id}");
        grant.approved_operations = [format!("{id}.run")].into();
        grant.approved_permissions = [format!("{id}.run")].into();
        grant.manifest_digest = digest.clone();
        let mut deployment = base.deployment.clone();
        deployment.plugin_id = id.into();
        deployment.deployment_id = format!("deployment-{id}");
        deployment.manifest_digest = digest;
        ResolvedPlugin::new(
            grant,
            Arc::new(PluginManifest::from_json(raw.as_bytes()).unwrap()),
            deployment,
            base.configuration_digest.clone(),
            Default::default(),
            base.features.clone(),
        )
        .unwrap()
    }
    let source = projected(
        "example.echo",
        serde_json::json!([
            {"id":"local","label":"Local","target":{"kind":"page","page":"example.echo.home"}},
            {"id":"peer","label":"Peer","target":{"kind":"page","pluginId":"example.peer","page":"example.peer.home"}},
            {"id":"absent","label":"Absent","target":{"kind":"page","pluginId":"example.peer","page":"example.peer.missing"}}
        ]),
    );
    let peer = projected("example.peer", serde_json::json!([]));
    let (ctx, _, _) = identity(SignedActorType::User);
    let mut request = command();
    request.capability_id = flare_im_capability_core::plugin::ui::UI_CATALOG_OPERATION.into();
    request.payload = Some(catalog_payload(false));
    for reversed in [false, true] {
        for blocked in [
            "none",
            "absent",
            "disabled",
            "revoked",
            "permission",
            "operation",
            "switch",
        ] {
            let mut peer = peer.clone();
            match blocked {
                "disabled" => peer.grant.enabled = false,
                "revoked" => peer.grant.revoked = true,
                "permission" => peer.grant.approved_permissions.clear(),
                "operation" => peer.grant.approved_operations.clear(),
                _ => {}
            }
            let mut projected = snapshot("http://127.0.0.1:1".into());
            projected.plugins = vec![source.clone()];
            if blocked != "absent" {
                projected.plugins.push(peer);
            }
            if reversed {
                projected.plugins.reverse();
            }
            for plugin in &mut projected.plugins {
                plugin.grant.valid_from_ms = projected.issued_at_ms;
                plugin.grant.valid_until_ms = projected.valid_until_ms;
            }
            projected
                .validate(chrono::Utc::now().timestamp_millis())
                .unwrap();
            let reader = Arc::new(Projection(Mutex::new(Some(projected))));
            let routes = Arc::new(PluginRouteBook::new().with_runtime_authorization(reader, None));
            let backend = policy();
            if blocked == "switch" {
                backend.set_tenant_capability("app-a", "example.peer.run", false);
            }
            let backend: Arc<dyn CapabilityPolicyBackend> = backend;
            let response =
                dispatch_remote(&ctx, &request, &routes, &backend, Duration::from_secs(1))
                    .await
                    .unwrap();
            let catalog: flare_im_capability_core::plugin::ui::UiCatalog =
                serde_json::from_value(response.data).unwrap();
            let actions = &catalog
                .plugins
                .iter()
                .find(|p| p.plugin_id == "example.echo")
                .unwrap()
                .extensions
                .contributions[0]
                .schema
                .actions;
            let ids: Vec<_> = actions.iter().map(|a| a.id.as_str()).collect();
            assert_eq!(
                ids,
                if blocked == "none" {
                    vec!["local", "peer"]
                } else {
                    vec!["local"]
                },
                "destination {blocked}, reversed={reversed}"
            );
        }
    }
}

mod plugin_state_and_catalog {
    use super::*;
    use crate::application::handler::plugin_card::PluginCardPoster;
    use crate::application::handler::plugin_card::tests::{MemoryOutbox, ScriptedDelivery};
    use crate::application::handler::plugin_state::tests::{
        MemoryBindings, RecordingSink, poll_snapshot, publisher,
    };
    use crate::application::handler::plugin_test_server::PluginServer;
    use flare_im_service_kit::plugin_platform::{
        PLUGIN_CARD_UNBOUND, PLUGIN_MESSAGE_RECALLED, PLUGIN_PERMISSION_DENIED,
    };
    use serde_json::json;

    /// No user grant anywhere: the installation projection authorizes.
    fn poll_policy() -> Arc<InMemoryCapabilityGrants> {
        Arc::new(InMemoryCapabilityGrants::new())
    }

    fn poll_command(operation: &str, input: Value) -> CapabilityDispatchCommand {
        CapabilityDispatchCommand {
            capability_id: operation.into(),
            tenant_id: Some("app-a".into()),
            user_id: Some("u1".into()),
            conversation_id: Some("2-group".into()),
            payload: Some(input),
            request_id: Some("r1".into()),
            message_id: None,
        }
    }

    fn from_card(operation: &str, input: Value, message_id: &str) -> CapabilityDispatchCommand {
        CapabilityDispatchCommand {
            message_id: Some(message_id.into()),
            ..poll_command(operation, input)
        }
    }

    struct Harness {
        routes: Arc<PluginRouteBook>,
        plugin: PluginServer,
        reader: Arc<Projection>,
        bindings: Arc<MemoryBindings>,
        sink: Arc<RecordingSink>,
        delivery: Arc<ScriptedDelivery>,
    }

    fn poll_card(poll_id: &str) -> Value {
        json!({
            "pluginId": "example.poll", "contributionId": "example.poll.card", "schemaVersion": 1,
            "summary": "[Poll]", "payload": {"pollId": poll_id, "question": "Lunch?"}
        })
    }

    async fn harness() -> Harness {
        harness_answering(Arc::new(|operation, _| {
            let state = json!({"entity": "poll-1", "version": 2, "values": {"counts": [1, 0]}});
            Ok(match operation {
                // vote declares im.plugin.entity_state.publish; create does not.
                "example.poll.vote" => json!({"messageState": {"version": 1, "values": {"mySelection": [0]}}, "sharedState": state}),
                "example.poll.create" => json!({"pollId": "poll-9", "message": poll_card("poll-9"), "sharedState": state}),
                _ => json!({}),
            })
        }))
        .await
    }

    async fn harness_answering(
        respond: Arc<crate::application::handler::plugin_test_server::Respond>,
    ) -> Harness {
        let plugin = PluginServer::start(respond).await;
        let reader = Arc::new(Projection(Mutex::new(Some(poll_snapshot(
            plugin.authority.clone(),
        )))));
        let bindings = Arc::new(MemoryBindings::default());
        let sink = Arc::new(RecordingSink::default());
        let delivery = Arc::new(ScriptedDelivery::accepting("m-posted"));
        let outbox = Arc::new(MemoryOutbox::default());
        let poster = PluginCardPoster::new(
            Some(delivery.clone()),
            Some(bindings.clone()),
            Some(outbox.clone()),
        )
        .without_backoff();
        let routes = Arc::new(
            PluginRouteBook::new()
                .with_runtime_authorization(
                    reader.clone(),
                    Some(Arc::new(
                        PluginCallSigner::new("call".into(), [29; 32]).unwrap(),
                    )),
                )
                .with_state_publisher(Some(publisher(bindings.clone(), sink.clone())))
                .with_entity_bindings(Some(bindings.clone()))
                .with_card_posting(Some(Arc::new(poster)), Some(outbox)),
        );
        Harness {
            routes,
            plugin,
            reader,
            bindings,
            sink,
            delivery,
        }
    }

    async fn until(mut done: impl FnMut() -> bool) {
        for _ in 0..100 {
            if done() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn dispatch(
        h: &Harness,
        command: &CapabilityDispatchCommand,
    ) -> Result<CapabilityDispatchResult> {
        let (ctx, _, _) = identity(SignedActorType::User);
        let backend: Arc<dyn CapabilityPolicyBackend> = poll_policy();
        dispatch_remote(&ctx, command, &h.routes, &backend, Duration::from_secs(2)).await
    }

    #[tokio::test]
    async fn a_call_result_shared_state_reaches_the_bound_card_of_its_conversation() {
        let h = harness().await;
        h.bindings
            .bind("app-a", "example.poll", "poll-1", "2-group", "m-1");
        let result = dispatch(
            &h,
            &from_card(
                "example.poll.vote",
                json!({"pollId": "poll-1", "selection": [0]}),
                "m-1",
            ),
        )
        .await
        .unwrap();
        // The result is returned unchanged; the publish runs behind it.
        assert!(result.success);
        assert_eq!(result.data["sharedState"]["version"], 2);
        until(|| !h.sink.versions().is_empty()).await;
        assert_eq!(h.sink.versions(), vec![2]);
        let (tenant, conversation, event) = h.sink.events.lock().unwrap()[0].clone();
        assert_eq!(
            (
                tenant.as_str(),
                conversation.as_str(),
                event.server_msg_id.as_str()
            ),
            ("app-a", "2-group", "m-1")
        );
        assert_eq!(event.values_json, r#"{"counts":[1,0]}"#);
    }

    #[tokio::test]
    async fn card_actions_are_accepted_only_from_the_card_bound_to_the_entity() {
        let h = harness().await;
        h.bindings
            .bind("app-a", "example.poll", "poll-1", "2-group", "m-1");
        let vote = json!({"pollId": "poll-1", "selection": [0]});
        // No message, a forged copy, the real card seen from another conversation.
        for command in [
            poll_command("example.poll.vote", vote.clone()),
            from_card("example.poll.vote", vote.clone(), "m-forged"),
            CapabilityDispatchCommand {
                conversation_id: Some("2-other".into()),
                ..from_card("example.poll.vote", vote.clone(), "m-1")
            },
        ] {
            let result = dispatch(&h, &command).await;
            assert_eq!(
                plugin_code(&result),
                Some(PLUGIN_CARD_UNBOUND),
                "{command:?}"
            );
        }
        // An entity no card is bound to.
        let result = dispatch(
            &h,
            &from_card("example.poll.vote", json!({"pollId": "poll-2"}), "m-1"),
        )
        .await;
        assert_eq!(plugin_code(&result), Some(PLUGIN_CARD_UNBOUND));
        assert!(h.plugin.calls().is_empty(), "refused before the plugin");
        // The bound card itself.
        assert!(
            dispatch(&h, &from_card("example.poll.vote", vote, "m-1"))
                .await
                .unwrap()
                .success
        );
        assert_eq!(h.plugin.calls().len(), 1);
    }

    #[tokio::test]
    async fn calls_naming_a_recalled_card_are_refused_before_the_plugin() {
        let h = harness().await;
        h.bindings
            .bind("app-a", "example.poll", "poll-1", "2-group", "m-1");
        h.bindings.recall("m-1");
        let result = dispatch(
            &h,
            &from_card(
                "example.poll.vote",
                json!({"pollId": "poll-1", "selection": [0]}),
                "m-1",
            ),
        )
        .await;
        assert_eq!(plugin_code(&result), Some(PLUGIN_MESSAGE_RECALLED));
        assert!(h.plugin.calls().is_empty());
        // Creating a new poll names no card and passes.
        assert!(
            dispatch(
                &h,
                &poll_command("example.poll.create", json!({"question": "Again?"}))
            )
            .await
            .unwrap()
            .success
        );
        assert_eq!(h.plugin.calls().len(), 1);
    }

    #[tokio::test]
    async fn a_created_card_is_posted_by_the_platform_and_its_actions_work_at_once() {
        let h = harness().await;
        let result = dispatch(
            &h,
            &poll_command(
                "example.poll.create",
                json!({"question": "Lunch?", "commandId": "c-1"}),
            ),
        )
        .await
        .unwrap();
        assert!(result.success);
        assert!(result.data.get("message").is_none());
        assert_eq!(result.data["posted"]["serverMsgId"], "m-posted");
        assert_eq!(result.data["posted"]["conversationId"], "2-group");
        let posts = h.delivery.posts.lock().unwrap().clone();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].1.sender_id, "u1");
        assert_eq!(posts[0].1.conversation_id, "2-group");
        // Create does not declare entity state publishing: its sharedState is not fanned out.
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(h.sink.versions().is_empty());
        // The card is bound already: a vote from it reaches the plugin.
        assert!(
            dispatch(
                &h,
                &from_card(
                    "example.poll.vote",
                    json!({"pollId": "poll-9", "selection": [0]}),
                    "m-posted"
                )
            )
            .await
            .unwrap()
            .success
        );
    }

    #[tokio::test]
    async fn user_tickets_never_reach_admin_operations() {
        let h = harness().await;
        for operation in ["example.poll.list", "example.poll.remove"] {
            let result = dispatch(&h, &poll_command(operation, json!({}))).await;
            assert_eq!(
                plugin_code(&result),
                Some(PLUGIN_PERMISSION_DENIED),
                "{operation}"
            );
        }
        assert!(h.plugin.calls().is_empty());
    }

    #[tokio::test]
    async fn the_client_catalog_keeps_console_pages_out_and_sends_visible_configuration_only() {
        let h = harness().await;
        let mut snapshot = poll_snapshot(h.plugin.authority.clone());
        snapshot.plugins[0].client_configuration = serde_json::from_value(
            json!({"maxOptions": 6, "allowAnonymous": true, "webhookSecret": "s3cret"}),
        )
        .unwrap();
        *h.reader.0.lock().unwrap() = Some(snapshot);
        let mut command = poll_command(
            flare_im_capability_core::plugin::ui::UI_CATALOG_OPERATION,
            catalog_payload(false),
        );
        command.conversation_id = None;
        let result = dispatch(&h, &command).await.unwrap();
        let text = result.data.to_string();
        assert!(!text.contains("\"admin\"") && !text.contains("example.poll.list"));
        assert!(!text.contains("s3cret") && !text.contains("webhookSecret"));
        let catalog: flare_im_capability_core::plugin::ui::UiCatalog =
            serde_json::from_value(result.data).unwrap();
        assert!(catalog.unavailable.is_empty());
        let plugin = &catalog.plugins[0];
        assert_eq!(
            Value::Object(plugin.configuration.clone()),
            json!({"maxOptions": 6, "allowAnonymous": true})
        );
        let kinds: Vec<_> = plugin
            .extensions
            .contributions
            .iter()
            .map(|c| c.id.as_str())
            .collect();
        assert_eq!(kinds, ["example.poll.card", "example.poll.create"]);
        // Admin operations are never offered to users; the card's own operations are.
        for admin in [
            "example.poll.list",
            "example.poll.inspect",
            "example.poll.end",
            "example.poll.remove",
        ] {
            assert!(!plugin.operations.contains(admin), "{admin}");
        }
        assert!(plugin.operations.contains("example.poll.details"));
        let card = &plugin.extensions.contributions[0];
        assert!(card.detail.is_some());
        assert_eq!(
            card.schema
                .actions
                .iter()
                .map(|a| a.id.as_str())
                .collect::<Vec<_>>(),
            ["vote", "close"]
        );
        card.schema.validate().unwrap();
    }

    /// The example poll with a second card (`example.poll.card2`) whose vote passes the poll as
    /// `id` instead of `pollId`.
    fn two_card_snapshot(
        authority: String,
    ) -> flare_im_capability_core::plugin::runtime::PluginRuntimeSnapshot {
        use crate::application::handler::plugin_state::tests::POLL_MANIFEST;
        let mut raw: Value = serde_json::from_str(POLL_MANIFEST).unwrap();
        let mut second = raw["clientExtensions"]["contributions"][0].clone();
        second["id"] = json!("example.poll.card2");
        for action in second["schema"]["actions"].as_array_mut().unwrap() {
            if action.get("input").is_some() {
                action["input"] = json!({"id": "/pollId"});
            }
        }
        second.as_object_mut().unwrap().remove("detail");
        raw["clientExtensions"]["contributions"]
            .as_array_mut()
            .unwrap()
            .push(second);
        let mut snapshot = poll_snapshot(authority);
        snapshot.plugins[0] =
            with_manifest(&snapshot.plugins[0], &serde_json::to_string(&raw).unwrap());
        snapshot
    }

    /// The call's message is looked up first; the entity must be the bound one, read at
    /// the input the bound card's own contribution uses, and no other entity input may name
    /// another entity. A card operation without the entity, with a malformed one, or from another
    /// plugin's card is refused before the plugin.
    #[tokio::test]
    async fn the_bound_card_decides_the_entity_not_the_input() {
        let h = harness().await;
        h.bindings
            .bind("app-a", "example.poll", "poll-1", "2-group", "m-1");
        h.bindings
            .bind("app-a", "example.other", "poll-2", "2-group", "m-other");
        for (case, input, message) in [
            ("entity missing", json!({"selection": [0]}), "m-1"),
            ("entity null", json!({"pollId": null}), "m-1"),
            ("entity malformed", json!({"pollId": "a b"}), "m-1"),
            (
                "entity an object",
                json!({"pollId": {"id": "poll-1"}}),
                "m-1",
            ),
            ("another entity", json!({"pollId": "poll-2"}), "m-1"),
            (
                "another plugin's card",
                json!({"pollId": "poll-2"}),
                "m-other",
            ),
        ] {
            let result = dispatch(&h, &from_card("example.poll.vote", input, message)).await;
            assert_eq!(plugin_code(&result), Some(PLUGIN_CARD_UNBOUND), "{case}");
        }
        assert!(h.plugin.calls().is_empty(), "refused before the plugin");
        // Bindings unavailable: refused, never let through.
        h.bindings
            .fail
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let result = dispatch(
            &h,
            &from_card("example.poll.vote", json!({"pollId": "poll-1"}), "m-1"),
        )
        .await;
        assert!(
            matches!(result, Err(CapabilityError::System(_))),
            "{result:?}"
        );
        assert!(h.plugin.calls().is_empty());
    }

    /// With two cards passing the entity under different input names, each card's call
    /// is checked at its own contribution's input, and a call filling both with different
    /// entities is refused.
    #[tokio::test]
    async fn each_card_names_its_entity_at_its_own_input() {
        let h = harness().await;
        *h.reader.0.lock().unwrap() = Some(two_card_snapshot(h.plugin.authority.clone()));
        h.bindings
            .bind("app-a", "example.poll", "poll-1", "2-group", "m-1");
        h.bindings.bind_through(
            "app-a",
            "example.poll",
            "example.poll.card2",
            "poll-2",
            "2-group",
            "m-2",
        );
        for (case, input, message) in [
            ("card2 read at pollId", json!({"pollId": "poll-2"}), "m-2"),
            (
                "a second entity input names another entity",
                json!({"pollId": "poll-1", "id": "poll-2"}),
                "m-1",
            ),
            ("card read at id", json!({"id": "poll-1"}), "m-1"),
        ] {
            let result = dispatch(&h, &from_card("example.poll.vote", input, message)).await;
            assert_eq!(plugin_code(&result), Some(PLUGIN_CARD_UNBOUND), "{case}");
        }
        assert!(h.plugin.calls().is_empty());
        for (input, message) in [
            (json!({"id": "poll-2", "selection": [0]}), "m-2"),
            (json!({"pollId": "poll-1", "id": "poll-1"}), "m-1"),
        ] {
            assert!(
                dispatch(&h, &from_card("example.poll.vote", input.clone(), message))
                    .await
                    .unwrap()
                    .success,
                "{input}"
            );
        }
        assert_eq!(h.plugin.calls().len(), 2);
    }

    /// A plugin cannot answer a platform code (`PLUGIN_*`, `CAPABILITY_*`) — clients act
    /// on those — whether it answers `ok: false` or a refusing gRPC status; its own codes pass.
    #[tokio::test]
    async fn plugins_cannot_answer_platform_codes() {
        for (answer, expected) in [
            ("PLUGIN_CARD_NOT_POSTED", "PLUGIN_REJECTED"),
            ("PLUGIN_DISABLED", "PLUGIN_REJECTED"),
            ("CAPABILITY_OUTCOME_UNKNOWN", "PLUGIN_REJECTED"),
            (
                "grpc:failed_precondition:PLUGIN_NOT_INSTALLED",
                "PLUGIN_REJECTED",
            ),
            (
                "grpc:permission_denied:CAPABILITY_RATE_LIMITED",
                "PLUGIN_REJECTED",
            ),
            ("POLL_CLOSED", "POLL_CLOSED"),
            ("grpc:failed_precondition:POLL_CLOSED", "POLL_CLOSED"),
            ("not a code", "PLUGIN_REJECTED"),
        ] {
            let h = harness_answering(Arc::new(move |_, _| Err(answer.to_owned()))).await;
            let result = dispatch(
                &h,
                &poll_command("example.poll.create", json!({"question": "Lunch?"})),
            )
            .await
            .unwrap();
            assert!(!result.success, "{answer}");
            assert_eq!(result.data["errorCode"], expected, "{answer}");
        }
    }
}
