use super::*;
use crate::infrastructure::ports::{
    ConversationReadGrpcPool, SignalingRouteGrpcPool, StorageSyncGrpcPool,
};
use flare_grpc_proto::sync::sync_service_server::{SyncService, SyncServiceServer};
use flare_proto::common::{Sync, SyncRes};
use futures::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

fn configured(address: &str, ports: [u16; 4]) -> FlareAppConfig {
    let mut base: toml::Value = toml::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../config/base.toml"
    )))
    .unwrap();
    let mut services = toml::map::Map::new();
    for (name, port) in [
        "conversation",
        "signaling_route",
        "storage_reader",
        "sync_orchestrator",
    ]
    .into_iter()
    .zip(ports)
    {
        let section: toml::Value =
            toml::from_str(&format!("[server]\naddress = {address:?}\nport = {port}\n")).unwrap();
        services.insert(name.to_string(), section);
    }
    base.as_table_mut()
        .unwrap()
        .insert("services".into(), toml::Value::Table(services));
    base.try_into().unwrap()
}

#[test]
fn explicit_static_configuration_resolves_all_four_service_ports() {
    let endpoints =
        StaticReadEndpoints::resolve(&configured("127.0.0.1", [41001, 41002, 41003, 41004]), true)
            .unwrap();
    assert_eq!(
        endpoints.conversation.as_deref(),
        Some("http://127.0.0.1:41001")
    );
    assert_eq!(endpoints.route.as_deref(), Some("http://127.0.0.1:41002"));
    assert_eq!(
        endpoints.storage_reader.as_deref(),
        Some("http://127.0.0.1:41003")
    );
    assert_eq!(endpoints.sync.as_deref(), Some("http://127.0.0.1:41004"));
}

#[test]
fn configured_registry_does_not_select_or_validate_static_targets() {
    let endpoints =
        StaticReadEndpoints::resolve(&configured("invalid-address", [0; 4]), false).unwrap();
    assert!(endpoints.conversation.is_none());
    assert!(endpoints.route.is_none());
    assert!(endpoints.storage_reader.is_none());
    assert!(endpoints.sync.is_none());
}

#[test]
fn explicit_static_configuration_rejects_missing_zero_and_invalid_targets() {
    let mut missing = configured("127.0.0.1", [41001; 4]);
    missing.services.sync_orchestrator = None;
    assert!(StaticReadEndpoints::resolve(&missing, true).is_err());
    for index in 0..4 {
        let mut ports = [41001; 4];
        ports[index] = 0;
        assert!(StaticReadEndpoints::resolve(&configured("127.0.0.1", ports), true).is_err());
    }
    assert!(
        StaticReadEndpoints::resolve(&configured("invalid-address", [41001; 4]), true).is_err()
    );
    let mut no_listener = configured("127.0.0.1", [41001; 4]);
    no_listener
        .services
        .storage_reader
        .as_mut()
        .unwrap()
        .runtime
        .server = None;
    assert!(StaticReadEndpoints::resolve(&no_listener, true).is_err());
}

#[test]
fn wildcard_static_listeners_use_the_matching_loopback_family() {
    let v4 = StaticReadEndpoints::resolve(&configured("0.0.0.0", [41001; 4]), true).unwrap();
    let v6 = StaticReadEndpoints::resolve(&configured("[::]", [41001; 4]), true).unwrap();
    assert_eq!(v4.sync.as_deref(), Some("http://127.0.0.1:41001"));
    assert_eq!(v6.sync.as_deref(), Some("http://[::1]:41001"));
}

#[derive(Clone, Default)]
struct LoopbackSync {
    calls: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl SyncService for LoopbackSync {
    async fn execute_sync(
        &self,
        _: Request<Sync>,
    ) -> std::result::Result<Response<SyncRes>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(SyncRes::default()))
    }
}

struct Listener {
    port: u16,
    accepted: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn listener() -> Listener {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = socket.local_addr().unwrap().port();
    let accepted = Arc::new(AtomicUsize::new(0));
    let accepting = accepted.clone();
    let incoming = TcpListenerStream::new(socket).map(move |socket| {
        if socket.is_ok() {
            accepting.fetch_add(1, Ordering::SeqCst);
        }
        socket
    });
    let service = LoopbackSync::default();
    let calls = service.calls.clone();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(SyncServiceServer::new(service))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    Listener {
        port,
        accepted,
        calls,
        task,
    }
}

#[tokio::test]
async fn static_pools_reach_typed_listeners_despite_discovery_aliases() {
    let listeners = [
        listener().await,
        listener().await,
        listener().await,
        listener().await,
    ];
    let endpoints = StaticReadEndpoints::resolve(
        &configured("127.0.0.1", std::array::from_fn(|i| listeners[i].port)),
        true,
    )
    .unwrap();
    let conversation = ConversationReadGrpcPool::new_with_service_name("alias-conversation".into())
        .with_static_endpoint(endpoints.conversation);
    let route = SignalingRouteGrpcPool::new_with_service_name("alias-route".into())
        .with_static_endpoint(endpoints.route);
    let storage_sync =
        StorageSyncGrpcPool::new_with_service_names("alias-storage".into(), "alias-sync".into())
            .with_static_endpoints(endpoints.storage_reader, endpoints.sync);
    tokio::time::timeout(Duration::from_secs(3), async {
        conversation
            .ensure_client()
            .await
            .expect("typed Conversation target");
        route.ensure_client().await.expect("typed Route target");
        storage_sync
            .ensure_storage_reader_client()
            .await
            .expect("typed Storage target");
        let mut sync = storage_sync
            .ensure_sync_client()
            .await
            .expect("typed Sync target");
        sync.execute_sync(Sync::default())
            .await
            .expect("real Sync RPC");
        storage_sync
            .ensure_sync_client()
            .await
            .expect("cached typed Sync client");
        for listener in &listeners {
            while listener.accepted.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            assert_eq!(listener.accepted.load(Ordering::SeqCst), 1);
        }
    })
    .await
    .expect("all configured listeners must receive their connections");
    assert_eq!(listeners[3].calls.load(Ordering::SeqCst), 1);
}
