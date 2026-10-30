//! The plugin host SDK registers and withdraws a delegated plugin through Core's
//! `CapabilityService` over real gRPC, behind the same outer layer as the production listener.
use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_capability::capability_core::plugin::workload::*;
use flare_capability::infrastructure::capability::{
    CapabilityExtensionRegistry, InMemoryCapabilityGrants, PluginRouteBook,
};
use flare_capability::infrastructure::config::CapabilityRuntimeConfig;
use flare_capability::interface::grpc::{CapabilityGrpcServer, CapabilityInvocationMetrics};
use flare_grpc_proto::capability::RegisteredPluginInstance;
use flare_plugin_host::{
    PluginDeclaration, PluginHost, RegistrationTokenFuture, RegistrationTokenProvider,
};
use flare_server_core::auth::{KeySet, kid};
use flare_server_core::{SignedContextVerifier, TokenService, middleware::ContextLayer};

fn pem(seed: [u8; 32]) -> (String, String) {
    let key = SigningKey::from_bytes(&seed);
    (
        key.to_pkcs8_pem(Default::default()).unwrap().to_string(),
        key.verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap(),
    )
}

fn plugin(plugin_id: &str) -> RegisteredPluginInstance {
    RegisteredPluginInstance {
        plugin_id: plugin_id.to_string(),
        capability_id: "example.poll".to_string(),
        grpc_authority: "http://127.0.0.1:9".to_string(),
        plugin_version: "1.0.0".to_string(),
        api_version: "1.0".to_string(),
        manifest_sha256: "a".repeat(64),
        ..Default::default()
    }
}

fn server(routes: Arc<PluginRouteBook>) -> CapabilityGrpcServer {
    routes.upsert("tenant-a", plugin("plugin-a"));
    routes.upsert("tenant-b", plugin("plugin-b"));
    let mut keys = KeySet::empty();
    keys.insert(kid::GATEWAY, &pem([47; 32]).1).unwrap();
    keys.insert(kid::HOOK, &pem([48; 32]).1).unwrap();
    let keys = SignedContextVerifier::from_key_set(keys);
    CapabilityGrpcServer::new(
        CapabilityExtensionRegistry::new(),
        Arc::new(InMemoryCapabilityGrants::new()),
        Arc::new(CapabilityRuntimeConfig {
            max_payload_json_bytes: 1024,
            dispatch_timeout: Duration::from_secs(1),
            dispatch_max_per_minute: None,
            plugin_health_check_interval: Duration::from_secs(3600),
            plugin_call_timeout: Duration::from_millis(50),
            plugin_route_refresh_interval: Duration::from_secs(10),
            plugin_route_ttl: Duration::from_secs(86_400),
        }),
        None,
        None,
        Arc::new(CapabilityInvocationMetrics::default()),
        None,
        routes,
        Some(keys.clone()),
        Some(keys.require_staff_kid([kid::GATEWAY])),
    )
}

#[tokio::test]
async fn delegated_host_sdk_interoperates_with_core_over_real_grpc() {
    struct BrokerFixture(TokenService);
    impl RegistrationTokenProvider for BrokerFixture {
        fn token<'a>(&'a self, request: &'a RegistrationRequest) -> RegistrationTokenFuture<'a> {
            Box::pin(async move {
                self.0
                    .generate_with_ed25519(
                        &request.plugin_id,
                        Some(&request.tenant_id),
                        REGISTRATION_TOKEN_USE,
                        &request.audience().unwrap(),
                    )
                    .map_err(|_| flare_plugin_host::HostError::Sign("fixture issuer failed".into()))
            })
        }
    }
    struct OwnedServer(tokio::task::JoinHandle<()>);
    impl Drop for OwnedServer {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let (private, public) = pem([85; 32]);
    let routes = Arc::new(PluginRouteBook::new());
    let server = server(routes.clone()).with_registration_verifier(
        TokenService::from_ed25519_public_key(&public, REGISTRATION_ISSUER).unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    // The same outer layer as the production CapabilityService listener.
    let service = ContextLayer::new().allow_missing().layer(
        flare_grpc_proto::capability::capability_service_server::CapabilityServiceServer::new(
            server,
        ),
    );
    let _server = OwnedServer(tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    }));
    let mut host = PluginHost::connect_with_token_provider(
        endpoint,
        "example.notice",
        Arc::new(BrokerFixture(
            TokenService::from_ed25519_private_key(
                &private,
                REGISTRATION_ISSUER,
                REGISTRATION_TTL_SECS,
            )
            .unwrap(),
        )),
    )
    .await
    .unwrap();
    let declaration = PluginDeclaration {
        tenant_id: "tenant-a".into(),
        plugin_id: "example.notice".into(),
        capability_id: "example.notice.read".into(),
        grpc_authority: "http://127.0.0.1:9".into(),
        plugin_version: "1.0.0".into(),
        api_version: "1.0".into(),
        manifest_sha256: "a".repeat(64),
        declared_operations: vec!["example.notice.read".into()],
        labels: std::collections::HashMap::new(),
    };
    host.announce(&declaration).await.unwrap();
    assert!(
        routes
            .list_filtered("tenant-a", "")
            .iter()
            .any(|p| p.plugin_id == declaration.plugin_id)
    );
    host.withdraw("tenant-a", "example.notice").await.unwrap();
    assert!(
        !routes
            .list_filtered("tenant-a", "")
            .iter()
            .any(|p| p.plugin_id == declaration.plugin_id)
    );
}
