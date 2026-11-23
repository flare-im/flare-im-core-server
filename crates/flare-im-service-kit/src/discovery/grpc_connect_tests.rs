use super::*;
use flare_grpc_proto::sync::{
    sync_service_client::SyncServiceClient,
    sync_service_server::{SyncService, SyncServiceServer},
};
use flare_proto::common::{Sync, SyncRes};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tonic::{Request, Response, Status};

pub(crate) fn configured(address: &str, port: u16) -> FlareAppConfig {
    configured_ports(address, [port; 10])
}

fn configured_ports(address: &str, ports: [u16; 10]) -> FlareAppConfig {
    let mut config = String::from(
        "[service]\nname = \"static-grpc-test\"\nversion = \"0\"\n[server]\naddress = \"127.0.0.1\"\nport = 1\n",
    );
    for (service, port) in [
        "conversation",
        "storage_reader",
        "signaling_online",
        "signaling_route",
        "sync_orchestrator",
        "message_ingest",
        "message_orchestrator",
        "media",
        "capability",
        "push_proxy",
    ]
    .into_iter()
    .zip(ports)
    {
        config.push_str(&format!(
            "[services.{service}.server]\naddress = {address:?}\nport = {port}\n"
        ));
    }
    toml::from_str(&config).expect("isolated typed config without a registry")
}

#[test]
fn shared_static_maps_canonical_dependencies_and_refuses_unknown_aliases() {
    let ports = [
        41231, 41232, 41233, 41234, 41235, 41236, 41237, 41238, 41239, 41240,
    ];
    let app = configured_ports("127.0.0.1", ports);
    for (dependency, port) in [
        CONVERSATION,
        STORAGE_READER,
        SIGNALING_ONLINE,
        SIGNALING_ROUTE,
        SYNC_ORCHESTRATOR,
        MESSAGE_INGEST,
        ORCHESTRATOR,
        MEDIA,
        CAPABILITY,
        PUSH_PROXY,
    ]
    .into_iter()
    .zip(ports)
    {
        assert_eq!(
            static_grpc_endpoint_from_app_config(&app, dependency).unwrap(),
            format!("http://127.0.0.1:{port}")
        );
    }
    assert!(static_grpc_endpoint_from_app_config(&app, "alias-conversation").is_err());
}

#[test]
fn shared_static_rejects_missing_listener_zero_port_and_invalid_address() {
    let mut app = configured("127.0.0.1", 41232);
    app.services.conversation = None;
    assert!(static_grpc_endpoint_from_app_config(&app, CONVERSATION).is_err());
    let mut app = configured("127.0.0.1", 41232);
    app.services.conversation.as_mut().unwrap().runtime.server = None;
    assert!(static_grpc_endpoint_from_app_config(&app, CONVERSATION).is_err());
    let mut app = configured("127.0.0.1", 41232);
    app.services
        .conversation
        .as_mut()
        .unwrap()
        .runtime
        .server
        .as_mut()
        .unwrap()
        .port = None;
    assert!(static_grpc_endpoint_from_app_config(&app, CONVERSATION).is_err());
    for app in [
        configured("127.0.0.1", 0),
        configured("invalid-host", 41232),
    ] {
        assert!(static_grpc_endpoint_from_app_config(&app, CONVERSATION).is_err());
    }
}

#[test]
fn shared_static_preserves_hosts_and_normalizes_both_wildcard_families() {
    for (host, expected) in [
        ("0.0.0.0", "127.0.0.1:41233"),
        ("[::]", "[::1]:41233"),
        ("127.0.0.2", "127.0.0.2:41233"),
    ] {
        assert_eq!(
            static_grpc_endpoint_from_app_config(&configured(host, 41233), CONVERSATION).unwrap(),
            format!("http://{expected}")
        );
    }
}

#[derive(Clone, Default)]
struct LoopbackSync(Arc<AtomicUsize>);

#[tonic::async_trait]
impl SyncService for LoopbackSync {
    async fn execute_sync(
        &self,
        _: Request<Sync>,
    ) -> std::result::Result<Response<SyncRes>, Status> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(SyncRes::default()))
    }
}

pub(crate) struct ProbeServer {
    pub(crate) port: u16,
    pub(crate) calls: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ProbeServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(crate) async fn probe_server() -> ProbeServer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let service = LoopbackSync::default();
    let calls = service.0.clone();
    let incoming = Box::pin(futures::stream::unfold(listener, |listener| async move {
        let stream = listener.accept().await.map(|(stream, _)| stream);
        Some((stream, listener))
    }));
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(SyncServiceServer::new(service))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    ProbeServer { port, calls, task }
}

pub(crate) async fn probe(channel: Channel) {
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        SyncServiceClient::new(channel).execute_sync(Sync::default()),
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_ok());
}

#[tokio::test]
async fn shared_static_lazy_aliases_reach_conversation_and_storage_typed_listeners() {
    for (dependency, alias) in [
        (CONVERSATION, "alias-conversation"),
        (STORAGE_READER, "alias-storage"),
    ] {
        let server = probe_server().await;
        let app = configured("127.0.0.1", server.port);
        let channel =
            connect_grpc_channel_lazy_in_mode(&app, true, dependency, alias, "http://127.0.0.1:1")
                .unwrap();
        probe(channel).await;
        assert_eq!(server.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn shared_static_configured_lazy_retains_fallback_without_typed_dependency() {
    let server = probe_server().await;
    let mut app = configured("127.0.0.1", 1);
    app.services = Default::default();
    let fallback = format!("http://127.0.0.1:{}", server.port);
    // The public entry point sees ordinary configured mode, not an inferred static mode.
    let channel = connect_grpc_channel_lazy_from_app_config(
        &app,
        "unknown-canonical",
        "configured-alias",
        &fallback,
    )
    .unwrap();
    probe(channel).await;
    assert_eq!(server.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shared_static_async_alias_uses_typed_dependency_before_discovery() {
    let server = probe_server().await;
    let app = configured("127.0.0.1", server.port);
    let channel = connect_grpc_channel_from_app_config_inner(
        &app,
        true,
        CONVERSATION,
        "alias-conversation",
        "http://127.0.0.1:1",
        StaticChannelPolicy::DEFAULT,
    )
    .await
    .unwrap();
    probe(channel).await;
    assert_eq!(server.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shared_static_configured_async_retains_no_registry_fallback() {
    let server = probe_server().await;
    let mut app = configured("127.0.0.1", 1);
    app.services = Default::default();
    let fallback = format!("http://127.0.0.1:{}", server.port);
    let channel = connect_grpc_channel_from_app_config_with_policy(
        &app,
        "unknown-canonical",
        "configured-alias",
        &fallback,
        StaticChannelPolicy::DEFAULT,
    )
    .await
    .unwrap();
    probe(channel).await;
    assert_eq!(server.calls.load(Ordering::SeqCst), 1);
}
