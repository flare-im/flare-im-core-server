use super::*;
use flare_grpc_proto::sync::{
    sync_service_client::SyncServiceClient,
    sync_service_server::{SyncService, SyncServiceServer},
};
use flare_proto::common::{Sync, SyncRes};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tonic::{Request, Response, Status};

fn incoming(
    listener: tokio::net::TcpListener,
) -> impl futures::Stream<Item = std::io::Result<tokio::net::TcpStream>> + Send + Unpin {
    Box::pin(futures::stream::unfold(listener, |listener| async move {
        let stream = listener.accept().await.map(|(stream, _)| stream);
        Some((stream, listener))
    }))
}

fn configured(address: &str, conversation_port: u16, storage_port: u16) -> FlareAppConfig {
    let mut app = flare_im_core_service_kit::config::load_config(Some(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../config/base.toml"
    )))
    .clone();
    app.services = Default::default();
    app.services.conversation = Some(Default::default());
    app.services.storage_reader = Some(Default::default());
    let runtime = |port| ServiceRuntimeConfig {
        server: Some(flare_im_core_service_kit::config::ServiceEndpointConfig {
            address: Some(address.to_string()),
            port: Some(port),
        }),
        ..Default::default()
    };
    app.services.conversation.as_mut().unwrap().runtime = runtime(conversation_port);
    app.services.storage_reader.as_mut().unwrap().runtime = runtime(storage_port);
    app
}

#[test]
fn static_sync_dependencies_use_their_typed_ports() {
    let app = configured("127.0.0.1", 41101, 41102);
    assert_eq!(
        static_sync_endpoint(&app, true, CONVERSATION)
            .unwrap()
            .as_deref(),
        Some("http://127.0.0.1:41101")
    );
    assert_eq!(
        static_sync_endpoint(&app, true, STORAGE_READER)
            .unwrap()
            .as_deref(),
        Some("http://127.0.0.1:41102")
    );
}

#[test]
fn configured_sync_retains_service_discovery_without_static_validation() {
    let app = configured("invalid-address", 0, 0);
    assert_eq!(
        static_sync_endpoint(&app, false, CONVERSATION).unwrap(),
        None
    );
    assert_eq!(
        static_sync_endpoint(&app, false, STORAGE_READER).unwrap(),
        None
    );
}

#[test]
fn static_sync_rejects_missing_invalid_zero_and_unknown_dependencies() {
    let mut missing = configured("127.0.0.1", 41101, 41102);
    missing.services.conversation = None;
    assert!(static_sync_endpoint(&missing, true, CONVERSATION).is_err());
    for service in [CONVERSATION, STORAGE_READER] {
        assert!(static_sync_endpoint(&configured("127.0.0.1", 0, 0), true, service).is_err());
        assert!(
            static_sync_endpoint(&configured("invalid-address", 41101, 41102), true, service)
                .is_err()
        );
    }
    assert!(static_sync_endpoint(&missing, true, "http://untrusted.invalid:41101").is_err());
}

#[test]
fn static_sync_wildcards_use_a_connectable_loopback_family() {
    assert_eq!(
        static_sync_endpoint(&configured("0.0.0.0", 41101, 41102), true, CONVERSATION)
            .unwrap()
            .as_deref(),
        Some("http://127.0.0.1:41101")
    );
    assert_eq!(
        static_sync_endpoint(&configured("[::]", 41101, 41102), true, STORAGE_READER)
            .unwrap()
            .as_deref(),
        Some("http://[::1]:41102")
    );
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

#[tokio::test]
async fn static_sync_channels_reach_typed_dependencies_despite_discovery_aliases() {
    let conversation = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let storage = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let app = configured(
        "127.0.0.1",
        conversation.local_addr().unwrap().port(),
        storage.local_addr().unwrap().port(),
    );
    let service = LoopbackSync::default();
    let calls = service.0.clone();
    let conversation_service = service.clone();
    let conversation_server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(SyncServiceServer::new(conversation_service))
            .serve_with_incoming(incoming(conversation))
            .await
            .unwrap();
    });
    let storage_server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(SyncServiceServer::new(service))
            .serve_with_incoming(incoming(storage))
            .await
            .unwrap();
    });
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        for (dependency, alias) in [
            (CONVERSATION, "alias-conversation"),
            (STORAGE_READER, "alias-storage"),
        ] {
            let channel =
                GrpcSyncAdapters::create_channel_with_config(&app, true, dependency, alias)
                    .await
                    .expect("configured dependency channel");
            SyncServiceClient::new(channel)
                .execute_sync(Sync::default())
                .await
                .expect("actual RPC at configured listener");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    })
    .await;
    conversation_server.abort();
    storage_server.abort();
    result.expect("both configured dependency listeners must answer");
}
