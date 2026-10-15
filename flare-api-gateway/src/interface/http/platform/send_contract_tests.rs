// Actual HTTP router -> tonic/SignedContextLayer boundary; downstream ingestion
// is explicitly a stub. This is not a message storage/delivery/full-stack test.
#[derive(Clone)]
struct SystemIngest(
    std::sync::Arc<std::sync::Mutex<Vec<flare_grpc_proto::message::SendSystemMessageRequest>>>,
);
#[tonic::async_trait]
impl flare_grpc_proto::message::message_send_service_server::MessageSendService for SystemIngest {
    async fn send_message(
        &self,
        _: tonic::Request<flare_grpc_proto::message::SendMessageRequest>,
    ) -> Result<tonic::Response<flare_grpc_proto::message::SendMessageResponse>, tonic::Status>
    {
        Err(tonic::Status::unimplemented("not used"))
    }
    async fn batch_send_message(
        &self,
        _: tonic::Request<flare_grpc_proto::message::BatchSendMessageRequest>,
    ) -> Result<tonic::Response<flare_grpc_proto::message::BatchSendMessageResponse>, tonic::Status>
    {
        Err(tonic::Status::unimplemented("not used"))
    }
    async fn send_ack(
        &self,
        _: tonic::Request<flare_grpc_proto::message::SendAckRequest>,
    ) -> Result<tonic::Response<flare_grpc_proto::message::SendAckResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("not used"))
    }
    async fn send_custom_data(
        &self,
        _: tonic::Request<flare_grpc_proto::message::SendCustomDataRequest>,
    ) -> Result<tonic::Response<flare_grpc_proto::message::SendCustomDataResponse>, tonic::Status>
    {
        Err(tonic::Status::unimplemented("not used"))
    }
    async fn send_system_message(
        &self,
        request: tonic::Request<flare_grpc_proto::message::SendSystemMessageRequest>,
    ) -> Result<tonic::Response<flare_grpc_proto::message::SendSystemMessageResponse>, tonic::Status>
    {
        let ctx = flare_server_core::require_ctx_from_request(&request)?;
        let claims = ctx
            .get_data::<flare_server_core::VerifiedContext>()
            .and_then(|v| v.claims())
            .unwrap();
        assert_eq!(claims.tenant_id, "app-a");
        assert_eq!(claims.actor_id, "sa-native");
        assert!(flare_im_service_kit::platform_send::is_platform_system_sender(claims));
        let value = request.into_inner();
        self.0.lock().unwrap().push(value);
        Ok(tonic::Response::new(
            flare_grpc_proto::message::SendSystemMessageResponse {
                success: true,
                message_id: "accepted-1".into(),
            },
        ))
    }
}
#[tokio::test]
async fn system_write_uses_api_key_machine_context_and_preserves_retry_identity() {
    use ed25519_dalek::pkcs8::EncodePublicKey;
    let public = SigningKey::from_bytes(&[11; 32])
        .verifying_key()
        .to_public_key_pem(Default::default())
        .unwrap();
    let mut keys = flare_server_core::auth::KeySet::empty();
    keys.insert(
        flare_im_service_kit::platform_send::PLATFORM_API_KID,
        &public,
    )
    .unwrap();
    let verifier = flare_server_core::SignedContextVerifier::from_key_set(keys);
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let service = flare_server_core::SignedContextLayer::new(verifier).layer(
        flare_grpc_proto::message::message_send_service_server::MessageSendServiceServer::new(
            SystemIngest(seen.clone()),
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
    );
    let (mut sender, token, _) = state(
        flare_im_service_kit::platform_send::SYSTEM_MESSAGE_SCOPE,
        Some(TenantStatus::Active),
        false,
        false,
    );
    Arc::get_mut(&mut sender).unwrap().ingest = channel(&endpoint).unwrap();
    let path = "/platform/v1/apps/app-a/conversations/group:g1/system-messages";
    let id = uuid::Uuid::new_v4().to_string();
    let payload =
        serde_json::json!({"client_msg_id":id,"kind":"maintenance","body":"Maintenance notice"});
    let post = |token: &str, app_path: &str, payload: &serde_json::Value| {
        let mut r = request(app_path, Some(token));
        *r.method_mut() = axum::http::Method::POST;
        r.headers_mut()
            .insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        *r.body_mut() = Body::from(serde_json::to_vec(payload).unwrap());
        r
    };
    for _ in 0..2 {
        let response = router(sender.clone())
            .oneshot(post(&token, path, &payload))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(body["data"]["client_msg_id"], id);
        assert_eq!(body["data"]["accepted"], true);
        assert!(body["data"].get("delivered").is_none());
    }
    {
        let requests = seen.lock().unwrap();
        assert_eq!(requests.len(), 2);
        for r in requests.iter() {
            let m = r.message.as_ref().unwrap();
            assert_eq!(m.client_msg_id, id);
            assert_eq!(m.sender_id, "sa-native");
            assert_eq!(m.channel_id, "g1");
            assert_eq!(m.source, flare_proto::common::MessageSource::System as i32);
            assert_eq!(
                m.message_type,
                flare_proto::common::MessageType::System as i32
            );
            assert!(m.attributes.is_empty());
            assert_eq!(r.system_message_type, "platform.maintenance");
        }
    }
    let (denied, wrong, _) = state(SCOPE, Some(TenantStatus::Active), false, false);
    check(
        router(denied)
            .oneshot(post(&wrong, path, &payload))
            .await
            .unwrap(),
        StatusCode::FORBIDDEN,
        "API_KEY_FORBIDDEN",
    )
    .await;
    check(
        router(sender.clone())
            .oneshot(post(&token, &path.replace("app-a", "app-b"), &payload))
            .await
            .unwrap(),
        StatusCode::FORBIDDEN,
        "API_KEY_FORBIDDEN",
    )
    .await;
    let mut forged = payload.clone();
    forged["sender_id"] = "a-user".into();
    check(
        router(sender)
            .oneshot(post(&token, path, &forged))
            .await
            .unwrap(),
        StatusCode::BAD_REQUEST,
        "INVALID_REQUEST",
    )
    .await;
    assert_eq!(
        seen.lock().unwrap().len(),
        2,
        "denied requests must not dispatch"
    );
    server.abort();
    let _ = server.await;
}
