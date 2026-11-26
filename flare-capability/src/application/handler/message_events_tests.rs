use super::*;
use crate::infrastructure::capability::InMemoryCapabilityGrants;
use flare_im_core_capability_core::plugin::{
    PluginManifest,
    authorization::TenantPluginGrant,
    invocation::PluginCallVerifier,
    runtime::{PluginDeployment, PluginRuntimeSnapshot, ResolvedPlugin, configuration_digest},
};
use flare_proto::common::{Message as RawMessage, MessageContent, TextContent};
use flare_server_core::{
    Context,
    mq::consumer::{ContentType, MessageContext},
};
use prost::Message as _;
use serde_json::json;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

fn message(source: MessageSource) -> Message {
    let ctx = Arc::new(
        Context::with_request_id("send-1")
            .with_tenant_id("app")
            .with_user_id("member"),
    );
    let headers = flare_im_core_contracts::context_to_mq_metadata(&ctx);
    let envelope = MqEnvelope {
        envelope_id: "envelope-1".into(),
        conversation_id: "group".into(),
        payload_kind: MqPayloadKind::Message as i32,
        produced_at: 1000,
        headers: headers.clone(),
        payload: Some(mq_envelope::Payload::Message(RawMessage {
            server_id: "msg-1".into(),
            conversation_id: "group".into(),
            sender_id: "member".into(),
            source: source as i32,
            message_type: MessageType::Text as i32,
            conversation_type: ConversationType::Group as i32,
            created_at: 1000,
            content: Some(MessageContent {
                content: Some(message_content::Content::Text(TextContent {
                    text: "签到".into(),
                    mentions: vec![],
                })),
            }),
            ..Default::default()
        })),
        ..Default::default()
    };
    let mut context = MessageContext::new(ctx, "flare.im.message.main".into());
    context.headers = headers;
    Message::new(envelope.encode_to_vec(), ContentType::Protobuf, context)
}
#[test]
fn original_member_text_has_stable_id_and_untrusted_or_bot_sources_do_not_run() {
    let first = decode_message_event(&message(MessageSource::User))
        .unwrap()
        .unwrap();
    let again = decode_message_event(&message(MessageSource::User))
        .unwrap()
        .unwrap();
    assert_eq!(first, again);
    assert_eq!(first.data.text.as_deref(), Some("签到"));
    assert_eq!(first.data.actor_id, "member");
    for source in [
        MessageSource::Bot,
        MessageSource::System,
        MessageSource::Admin,
    ] {
        assert!(decode_message_event(&message(source)).unwrap().is_none());
    }
    let mut raw = message(MessageSource::User);
    let mut envelope = raw.decode_protobuf::<MqEnvelope>().unwrap();
    envelope.headers.insert("x-user-id".into(), "forged".into());
    raw.payload = envelope.encode_to_vec();
    assert!(decode_message_event(&raw).is_err());
}
struct Projection(Mutex<PluginRuntimeSnapshot>, std::sync::atomic::AtomicU8);
#[async_trait::async_trait]
impl PluginAuthorizationReader for Projection {
    async fn event_directory(
        &self,
        tenant: &str,
    ) -> flare_server_core::error::Result<crate::domain::repository::PluginEventDirectory> {
        if self.1.load(Ordering::SeqCst) == 3 {
            return Ok(Default::default());
        }
        let mut subscriptions = self
            .0
            .lock()
            .unwrap()
            .plugins
            .iter()
            .flat_map(|plugin| {
                plugin
                    .manifest()
                    .event_subscriptions
                    .iter()
                    .filter(|s| {
                        plugin
                            .authorize_event(
                                &s.event_type,
                                &s.operation,
                                "member",
                                chrono::Utc::now().timestamp_millis(),
                            )
                            .is_ok()
                    })
                    .map(|s| (plugin.grant.grant_id.clone(), s.operation.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        let mode = self.1.load(Ordering::SeqCst);
        if mode == 5 {
            subscriptions = Default::default();
        }
        Ok(crate::domain::repository::PluginEventDirectory {
            subscriptions,
            unavailable_grants: if matches!(mode, 4 | 5) {
                ["pending-grant".into()].into()
            } else {
                Default::default()
            },
            routes: self.plugin_snapshot(tenant).await?,
        })
    }
    async fn event_active_since_ms(
        &self,
        _: &str,
        _: &str,
        _: u64,
        _: &str,
    ) -> flare_server_core::error::Result<Option<i64>> {
        Ok(Some(1000))
    }
    async fn plugin_snapshot(
        &self,
        tenant: &str,
    ) -> flare_server_core::error::Result<Option<Arc<PluginRuntimeSnapshot>>> {
        if self.1.load(Ordering::SeqCst) == 1 {
            return Ok(None);
        }
        Ok((tenant == "app").then(|| {
            let mut snapshot = self.0.lock().unwrap().clone();
            if self.1.load(Ordering::SeqCst) == 2 {
                snapshot.plugins.clear();
            }
            Arc::new(snapshot)
        }))
    }
}
struct Receiver {
    verifier: PluginCallVerifier,
    ids: Arc<Mutex<Vec<String>>>,
    fail: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
}
#[tonic::async_trait]
impl flare_grpc_proto::capability::extension_plugin_server::ExtensionPlugin for Receiver {
    async fn call(
        &self,
        request: tonic::Request<GenericRequest>,
    ) -> Result<tonic::Response<flare_grpc_proto::capability::GenericResponse>, tonic::Status> {
        let token = request
            .metadata()
            .get(PLUGIN_CALL_HEADER)
            .unwrap()
            .to_str()
            .unwrap();
        let body = request.get_ref();
        let claims = self
            .verifier
            .verify_event(
                token,
                &body.payload.as_ref().unwrap().value,
                chrono::Utc::now().timestamp_millis(),
            )
            .unwrap();
        assert_eq!(claims.tenant_id, "app");
        assert_eq!(claims.deployment_id, "deployment");
        assert_eq!(claims.event_id, body.request_id);
        self.ids.lock().unwrap().push(claims.event_id);
        let failed = self.fail.load(Ordering::SeqCst);
        self.notify.notify_one();
        if failed {
            return Err(tonic::Status::unavailable("retry"));
        }
        Ok(tonic::Response::new(
            flare_grpc_proto::capability::GenericResponse {
                ok: true,
                request_id: body.request_id.clone(),
                ..Default::default()
            },
        ))
    }
}
#[tokio::test]
async fn signed_event_delivery_retries_same_id_and_disabled_body_scope_stops_delivery() {
    delivery_test(false, false, false).await;
}
#[tokio::test]
#[ignore = "starts an explicitly owned local nats-server"]
async fn durable_jetstream_main_stream_delivers_and_retries_signed_plugin_events() {
    delivery_test(true, false, false).await;
}
#[tokio::test]
async fn unavailable_binding_retains_retry_without_blocking_healthy_event_subscribers() {
    delivery_test(false, false, true).await;
}
async fn delivery_test(jetstream: bool, system_policy: bool, unavailable_binding: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let authority = format!("http://{}", listener.local_addr().unwrap());
    let now = chrono::Utc::now().timestamp_millis();
    let raw=serde_json::to_vec(&json!({"manifestVersion":1,"pluginId":"observer","version":"1.0.0","apiVersion":"1.0",
      "displayName":"Observer","publisher":{"publisherId":"test","displayName":"Test"},
      "runtime":{"kind":"grpc","service":"test.Plugin","protocol":"flare.capability.v1"},
      "capabilities":[{"id":"observer","operations":[{"id":"receive","inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"permissionIds":["observe"],"idempotent":true,"timeoutMs":1000}]}],
      "permissions":[{"id":"observe","description":"Messages","dataScopes":[MESSAGE_EVENTS_SCOPE,MESSAGE_BODY_SCOPE],"riskLevel":"high"}],
      "eventSubscriptions":[{"eventType":MESSAGE_CREATED,"schemaVersion":1,"operation":"observer.receive","permissionIds":["observe"],"includeBody":true}],
      "features":[{"id":"observe","label":{"en-US":"Observe"},"gates":{"operations":["observer.receive"]}}],
      "artifact":{"digest":format!("sha256:{}","a".repeat(64)),"signatureRef":"test"}})).unwrap();
    let digest = digest_bytes(&raw);
    let plugin = ResolvedPlugin::new(
        TenantPluginGrant {
            grant_id: "grant".into(),
            revision: 1,
            tenant_id: "app".into(),
            plugin_id: "observer".into(),
            plugin_version: "1.0.0".into(),
            manifest_digest: digest.clone(),
            approved_operations: ["observer.receive".into()].into(),
            approved_permissions: ["observe".into()].into(),
            approved_data_scopes: [MESSAGE_EVENTS_SCOPE.into(), MESSAGE_BODY_SCOPE.into()].into(),
            enabled: true,
            revoked: false,
            suspended: false,
            valid_from_ms: now,
            valid_until_ms: now + 60000,
        },
        Arc::new(PluginManifest::from_json(&raw).unwrap()),
        PluginDeployment {
            deployment_id: "deployment".into(),
            plugin_id: "observer".into(),
            plugin_version: "1.0.0".into(),
            manifest_digest: digest,
            artifact_digest: format!("sha256:{}", "a".repeat(64)),
            grpc_authority: authority,
        },
        configuration_digest(&json!({})).unwrap(),
        Default::default(),
        [("observe".into(), true)].into(),
    )
    .unwrap();
    let projection = Arc::new(Projection(
        Mutex::new(PluginRuntimeSnapshot {
            organization_id: None,
            tenant_id: "app".into(),
            version: 1,
            issued_at_ms: now,
            valid_until_ms: now + 60000,
            plugins: vec![plugin],
        }),
        std::sync::atomic::AtomicU8::new(0),
    ));
    let signer = Arc::new(PluginCallSigner::new("events".into(), [91; 32]).unwrap());
    let mut verifier = PluginCallVerifier::default();
    verifier
        .insert("events".into(), signer.public_key())
        .unwrap();
    let ids = Arc::new(Mutex::new(Vec::new()));
    let fail = Arc::new(AtomicBool::new(true));
    let notify = Arc::new(tokio::sync::Notify::new());
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(
                flare_grpc_proto::capability::extension_plugin_server::ExtensionPluginServer::new(
                    Receiver {
                        verifier,
                        ids: ids.clone(),
                        fail: fail.clone(),
                        notify: notify.clone(),
                    },
                ),
            )
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = stopped.await;
                },
            ),
    );
    let handler = Arc::new(MessageEventHandler::new(
        projection.clone(),
        if system_policy {
            Arc::new(AlwaysSystem)
        } else {
            Arc::new(InMemoryCapabilityGrants::new())
        },
        signer,
    ));
    if system_policy {
        assert!(
            matches!(
                handler.handle(message(MessageSource::User)).await,
                Err(ConsumerError::Handler(_))
            ),
            "policy storage failure must retain the event"
        );
        stop.send(()).unwrap();
        server.await.unwrap().unwrap();
        return;
    }
    if unavailable_binding {
        fail.store(false, Ordering::SeqCst);
        projection.1.store(4, Ordering::SeqCst);
        assert!(matches!(
            handler.handle(message(MessageSource::User)).await,
            Err(ConsumerError::Handler(_))
        ));
        assert_eq!(
            ids.lock().unwrap().len(),
            1,
            "healthy subscriber must run despite the unavailable binding"
        );
        projection.1.store(5, Ordering::SeqCst);
        assert!(
            matches!(
                handler.handle(message(MessageSource::User)).await,
                Err(ConsumerError::Handler(_))
            ),
            "an unavailable binding alone must never be acknowledged"
        );
        assert_eq!(ids.lock().unwrap().len(), 1);
        // The desired directory removes the unavailable binding after an explicit stop.
        projection.1.store(0, Ordering::SeqCst);
        assert!(matches!(
            handler.handle(message(MessageSource::User)).await,
            Ok(MessageResult::Ack)
        ));
        let observed = ids.lock().unwrap().clone();
        assert_eq!(observed.len(), 2);
        assert_eq!(
            observed[0], observed[1],
            "retry keeps the stable event identity"
        );
        stop.send(()).unwrap();
        server.await.unwrap().unwrap();
        return;
    }
    if jetstream {
        run_jetstream(handler, ids.clone(), fail, notify).await;
        stop.send(()).unwrap();
        server.await.unwrap().unwrap();
        return;
    }
    assert!(handler.handle(message(MessageSource::User)).await.is_err());
    projection.1.store(1, Ordering::SeqCst);
    assert!(
        handler.handle(message(MessageSource::User)).await.is_err(),
        "an expired projection must retain the pending event"
    );
    projection.1.store(2, Ordering::SeqCst);
    assert!(
        handler.handle(message(MessageSource::User)).await.is_err(),
        "expired health must retain the desired subscription"
    );
    projection.1.store(0, Ordering::SeqCst);
    projection.1.store(3, Ordering::SeqCst);
    handler.handle(message(MessageSource::User)).await.unwrap();
    projection.1.store(0, Ordering::SeqCst);
    fail.store(false, Ordering::SeqCst);
    handler.handle(message(MessageSource::User)).await.unwrap();
    assert_eq!(ids.lock().unwrap().len(), 2);
    let mut old = message(MessageSource::User);
    let mut envelope = old.decode_protobuf::<MqEnvelope>().unwrap();
    if let Some(mq_envelope::Payload::Message(raw)) = &mut envelope.payload {
        raw.created_at = 999;
    }
    old.payload = envelope.encode_to_vec();
    handler.handle(old).await.unwrap();
    assert_eq!(
        ids.lock().unwrap().len(),
        2,
        "messages before the approved source activation are not disclosed"
    );
    {
        let observed = ids.lock().unwrap();
        assert_eq!(observed[0], observed[1]);
    }
    projection.0.lock().unwrap().plugins[0]
        .grant
        .approved_data_scopes
        .remove(MESSAGE_BODY_SCOPE);
    assert!(handler.handle(message(MessageSource::User)).await.is_ok());
    assert_eq!(ids.lock().unwrap().len(), 2);
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

async fn run_jetstream(
    handler: Arc<MessageEventHandler>,
    ids: Arc<Mutex<Vec<String>>>,
    fail: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
) {
    use crate::composition::wiring::message_events::{EventMqConfig, build_tasks};
    use flare_core_runtime::task::Task;
    use flare_server_core::mq::{
        consumer::{MessageFetcher, failure::HEADER_ORIGINAL_TOPIC},
        nats::{NatsMessageFetcher, NatsProducer, default_stream_specs},
        producer::Producer,
    };
    struct Broker {
        child: std::process::Child,
        dir: std::path::PathBuf,
    }
    impl Drop for Broker {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);
    let dir =
        std::env::temp_dir().join(format!("flare-plugin-event-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let child = std::process::Command::new("nats-server")
        .args([
            "-js",
            "-a",
            "127.0.0.1",
            "-p",
            &port.to_string(),
            "-sd",
            dir.to_str().unwrap(),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let _broker = Broker { child, dir };
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let config = EventMqConfig {
        url: format!("nats://127.0.0.1:{port}"),
        streams: default_stream_specs()
            .into_iter()
            .map(|s| s.with_max_bytes(16 * 1024 * 1024))
            .collect(),
    };
    let tasks = build_tasks(&config, handler).await.unwrap();
    let mut running = Vec::new();
    for task in tasks {
        let (stop, shutdown) = tokio::sync::oneshot::channel();
        running.push((stop, tokio::spawn(Box::new(task).run(shutdown))));
    }
    let producer = NatsProducer::new(&config).await.unwrap();
    let raw = message(MessageSource::User);
    let ctx = Arc::new(
        Context::with_request_id("stream-message")
            .with_tenant_id("app")
            .with_user_id("member"),
    );
    producer
        .send(
            &ctx,
            "flare.im.message.main",
            Some("group"),
            raw.payload,
            Some(raw.context.headers),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), notify.notified())
        .await
        .unwrap();
    fail.store(false, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(10), notify.notified())
        .await
        .unwrap();
    {
        let ids = ids.lock().unwrap();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], ids[1]);
    }
    // A poison source cannot create an authenticated actor. It reaches the durable
    // dead-letter subject with the original record, rather than being acknowledged away.
    let mut dlq = NatsMessageFetcher::new(&config, vec!["flare.im.dlq.plugin-events".into()])
        .await
        .unwrap();
    let mut poison = message(MessageSource::User)
        .decode_protobuf::<MqEnvelope>()
        .unwrap();
    poison.envelope_id = "poison".into();
    poison.headers.insert("x-user-id".into(), "forged".into());
    producer
        .send(
            &ctx,
            "flare.im.message.main",
            Some("group"),
            poison.encode_to_vec(),
            None,
        )
        .await
        .unwrap();
    let dead = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(dead) = dlq.fetch().await.unwrap() {
                break dead;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        dead.context
            .headers
            .get(HEADER_ORIGINAL_TOPIC)
            .map(String::as_str),
        Some("flare.im.message.main")
    );
    assert_eq!(
        dead.decode_protobuf::<MqEnvelope>().unwrap().envelope_id,
        "poison"
    );
    assert_eq!(ids.lock().unwrap().len(), 2);
    for (stop, task) in running {
        let _ = stop.send(());
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

struct AlwaysSystem;
#[async_trait::async_trait]
impl CapabilityPolicyBackend for AlwaysSystem {
    async fn ensure_not_suspended(
        &self,
        _: &str,
        _: &str,
    ) -> crate::domain::capability::Result<()> {
        Err(crate::domain::capability::CapabilityError::System(
            "policy store down".into(),
        ))
    }
    async fn set_tenant_capability(
        &self,
        _: &str,
        _: &str,
        _: bool,
    ) -> crate::domain::capability::Result<()> {
        Ok(())
    }
}
#[tokio::test]
async fn policy_storage_failure_retains_the_pending_message_event() {
    delivery_test(false, true, false).await;
}
