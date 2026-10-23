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
            client_configuration: Default::default(),
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
        message_id: None,
    }
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
    use flare_im_service_kit::plugin_platform::{
        PLUGIN_DISABLED, PLUGIN_NOT_INSTALLED, PLUGIN_PERMISSION_DENIED,
    };
    for (mode, code) in [
        (0, PLUGIN_NOT_INSTALLED),
        (1, PLUGIN_DISABLED),
        (2, PLUGIN_DISABLED),
        (3, PLUGIN_NOT_INSTALLED),
        (4, PLUGIN_PERMISSION_DENIED),
        (5, PLUGIN_NOT_INSTALLED),
    ] {
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
    // The tenant has no capability_user_grants row: the enabled installation authorizes.
    assert!(policy.list_user_capabilities("app-a", "u1").is_empty());
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
async fn ui_catalog_uses_local_projection_and_strips_runtime_details() {
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
    async fn the_client_catalog_drops_admin_pages_and_sends_visible_configuration_only() {
        let h = harness().await;
        let mut snapshot = poll_snapshot(h.plugin.authority.clone());
        snapshot.plugins[0].client_configuration = serde_json::from_value(
            json!({"maxOptions": 6, "allowAnonymous": true, "webhookSecret": "s3cret"}),
        )
        .unwrap();
        *h.reader.0.lock().unwrap() = Some(snapshot);
        let mut command = poll_command(
            flare_im_capability_core::plugin::ui::UI_CATALOG_OPERATION,
            Value::Null,
        );
        command.conversation_id = None;
        let result = dispatch(&h, &command).await.unwrap();
        let text = result.data.to_string();
        assert!(!text.contains("admin-page") && !text.contains("example.poll.admin"));
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
        // The operations an admin page also uses (details, close) are admin operations: no user
        // is offered them, so the card keeps vote only and no detail.
        assert!(!plugin.operations.contains("example.poll.details"));
        assert!(!plugin.operations.contains("example.poll.list"));
        let card = &plugin.extensions.contributions[0];
        assert!(card.detail.is_none());
        assert_eq!(
            card.schema
                .actions
                .iter()
                .map(|a| a.id.as_str())
                .collect::<Vec<_>>(),
            ["vote"]
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
        let raw = serde_json::to_string(&raw).unwrap();
        let digest = digest_bytes(raw.as_bytes());
        let mut snapshot = poll_snapshot(authority);
        let projected = &mut snapshot.plugins[0];
        projected.manifest_json = raw;
        projected.grant.manifest_digest = digest.clone();
        projected.deployment.manifest_digest = digest;
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
