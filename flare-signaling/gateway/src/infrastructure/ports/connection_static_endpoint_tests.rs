use super::*;
use flare_core::server::connection::ConnectionManager;
use flare_grpc_proto::signaling::online::{
    online_service_server::{OnlineService, OnlineServiceServer},
    *,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status};

#[derive(Clone, Default)]
struct LoopbackOnline {
    calls: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl OnlineService for LoopbackOnline {
    async fn login(
        &self,
        _: Request<LoginRequest>,
    ) -> std::result::Result<Response<LoginResponse>, Status> {
        Err(Status::unimplemented("not used by endpoint regression"))
    }
    async fn logout(
        &self,
        _: Request<LogoutRequest>,
    ) -> std::result::Result<Response<LogoutResponse>, Status> {
        Err(Status::unimplemented("not used by endpoint regression"))
    }
    async fn report_device_state(
        &self,
        _: Request<ReportDeviceStateRequest>,
    ) -> std::result::Result<Response<ReportDeviceStateResponse>, Status> {
        Err(Status::unimplemented("not used by endpoint regression"))
    }
    async fn heartbeat(
        &self,
        _: Request<HeartbeatRequest>,
    ) -> std::result::Result<Response<HeartbeatResponse>, Status> {
        Err(Status::unimplemented("not used by endpoint regression"))
    }
    async fn get_user_presence(
        &self,
        _: Request<GetUserPresenceRequest>,
    ) -> std::result::Result<Response<GetUserPresenceResponse>, Status> {
        Err(Status::unimplemented("not used by endpoint regression"))
    }
    async fn batch_get_user_presence(
        &self,
        _: Request<BatchGetUserPresenceRequest>,
    ) -> std::result::Result<Response<BatchGetUserPresenceResponse>, Status> {
        Err(Status::unimplemented("not used by endpoint regression"))
    }
    async fn list_user_devices(
        &self,
        _: Request<ListUserDevicesRequest>,
    ) -> std::result::Result<Response<ListUserDevicesResponse>, Status> {
        Err(Status::unimplemented("not used by endpoint regression"))
    }
    async fn kick_device(
        &self,
        _: Request<KickDeviceRequest>,
    ) -> std::result::Result<Response<KickDeviceResponse>, Status> {
        Err(Status::unimplemented("not used by endpoint regression"))
    }
    async fn self_kick_device(
        &self,
        _: Request<KickDeviceRequest>,
    ) -> std::result::Result<Response<KickDeviceResponse>, Status> {
        Err(Status::unimplemented("not used by endpoint regression"))
    }
    async fn kick_tenant(
        &self,
        _: Request<KickTenantRequest>,
    ) -> std::result::Result<Response<KickTenantResponse>, Status> {
        Err(Status::unimplemented("not used by endpoint regression"))
    }
    async fn get_device(
        &self,
        _: Request<GetDeviceRequest>,
    ) -> std::result::Result<Response<GetDeviceResponse>, Status> {
        Err(Status::unimplemented("not used by endpoint regression"))
    }

    async fn get_online_status(
        &self,
        request: Request<GetOnlineStatusRequest>,
    ) -> std::result::Result<Response<GetOnlineStatusResponse>, Status> {
        assert_eq!(request.into_inner().user_ids, ["configured-endpoint-user"]);
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(GetOnlineStatusResponse::default()))
    }

    type WatchPresenceStream = ReceiverStream<std::result::Result<PresenceEvent, Status>>;
    async fn watch_presence(
        &self,
        _: Request<WatchPresenceRequest>,
    ) -> std::result::Result<Response<Self::WatchPresenceStream>, Status> {
        Err(Status::unimplemented("not used"))
    }
    type SubscribeUserPresenceStream =
        ReceiverStream<std::result::Result<UserPresenceEvent, Status>>;
    async fn subscribe_user_presence(
        &self,
        _: Request<SubscribeUserPresenceRequest>,
    ) -> std::result::Result<Response<Self::SubscribeUserPresenceStream>, Status> {
        Err(Status::unimplemented("not used"))
    }
}

fn configured(address: &str, port: u16) -> flare_im_service_kit::config::FlareAppConfig {
    let mut base: toml::Value =
        toml::from_str(include_str!("../../../../../config/base.toml")).unwrap();
    let service: toml::Value = toml::from_str(&format!(
        r#"
[services.signaling_online.server]
address = "{address}"
port = {port}
"#
    ))
    .expect("typed service configuration");
    base.as_table_mut()
        .unwrap()
        .extend(service.as_table().unwrap().clone());
    base.try_into().expect("complete application configuration")
}

#[tokio::test]
async fn configured_static_online_port_receives_actual_tonic_request() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = LoopbackOnline::default();
    let calls = service.calls.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(OnlineServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let app = configured("127.0.0.1", address.port());
    let endpoint = ConnectionRepository::resolve_online_static_endpoint(&app, true).unwrap();
    let repository = ConnectionRepository::new(Arc::new(ConnectionManager::new()), "0".into())
        .with_static_online_endpoint(endpoint);
    let mut client = repository
        .ensure_client()
        .await
        .expect("configured Online listener must be used");
    client
        .get_online_status(GetOnlineStatusRequest {
            user_ids: vec!["configured-endpoint-user".into()],
        })
        .await
        .expect("actual loopback RPC must succeed");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.abort();
}

#[test]
fn dynamic_registry_does_not_admit_the_static_configured_endpoint() {
    assert_eq!(
        ConnectionRepository::resolve_online_static_endpoint(
            &configured("127.0.0.1", 44321),
            false
        )
        .unwrap(),
        None
    );
}

#[test]
fn static_wildcard_listener_is_resolved_to_a_connectable_loopback_target() {
    assert_eq!(
        ConnectionRepository::resolve_online_static_endpoint(&configured("0.0.0.0", 44321), true)
            .unwrap(),
        Some("http://127.0.0.1:44321".into())
    );
    assert_eq!(
        ConnectionRepository::resolve_online_static_endpoint(&configured("[::]", 44321), true)
            .unwrap(),
        Some("http://[::1]:44321".into())
    );
}

#[test]
fn invalid_static_service_address_fails_instead_of_using_a_default() {
    assert!(
        ConnectionRepository::resolve_online_static_endpoint(
            &configured("invalid-host", 44321),
            true
        )
        .is_err()
    );
    assert_eq!(
        ConnectionRepository::resolve_online_static_endpoint(
            &configured("invalid-host", 44321),
            false
        )
        .unwrap(),
        None
    );
}
