use super::*;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use flare_im_service_kit::clients::*;
use flare_im_service_kit::{
    gateway_auth::TenantRuntimeGuard,
    tenant_runtime::{TenantAccessPolicy, TenantRuntimeCache},
};
use flare_server_core::{
    AuthError, AuthenticatedPrincipal, TokenValidationRequest, TokenValidator,
};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tower::ServiceExt;

struct Validator;
#[async_trait::async_trait]
impl TokenValidator for Validator {
    async fn validate(
        &self,
        request: TokenValidationRequest,
    ) -> Result<AuthenticatedPrincipal, AuthError> {
        Ok(AuthenticatedPrincipal {
            user_id: "user".to_string(),
            tenant_id: if request.token == "unbound" {
                None
            } else {
                Some("tenant-a".to_string())
            },
            device_id: None,
            app_id: None,
            expires_at: None,
            scopes: vec![],
            metadata: Default::default(),
        })
    }
}

fn router() -> Router {
    // Lazy channels never connect for the rejected requests tested below.
    let c = Channel::from_static("http://127.0.0.1:9").connect_lazy();
    let clients = GrpcClients {
        media: Arc::new(Mutex::new(MediaServiceClientWrapper::from_channel(
            c.clone(),
            None,
        ))),
        message_ingest_send: Arc::new(Mutex::new(MessageSendServiceClientWrapper::from_channel(
            c.clone(),
        ))),
        message_event: Arc::new(Mutex::new(MessageEventServiceClientWrapper::from_channel(
            c.clone(),
        ))),
        message_action: Arc::new(Mutex::new(MessageActionServiceClientWrapper::from_channel(
            c.clone(),
        ))),
        conversation_read: Arc::new(Mutex::new(
            ConversationReadServiceClientWrapper::from_channel(c.clone()),
        )),
        conversation_manage: Arc::new(Mutex::new(
            ConversationManageServiceClientWrapper::from_channel(c.clone()),
        )),
        online: Arc::new(Mutex::new(OnlineServiceClientWrapper::from_channel(
            c.clone(),
        ))),
        storage_reader: Arc::new(Mutex::new(StorageReaderServiceClientWrapper::from_channel(
            c,
        ))),
    };
    create_public_router(Arc::new(clients))
        .layer(axum::Extension(
            Arc::new(Validator) as Arc<dyn TokenValidator>
        ))
        .layer(axum::Extension(TenantRuntimeGuard::new(
            TenantRuntimeCache::disabled(),
            TenantAccessPolicy::Lenient,
        )))
}

#[tokio::test]
async fn ordinary_user_cannot_reach_any_media_management_route() {
    let router = router();
    for (method, resource) in [
        ("POST", "cleanup-orphaned-assets"),
        ("POST", "object-acl"),
        ("GET", "objects"),
        ("GET", "bucket"),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(format!("/api/v1/medias/{resource}"))
                    .header("authorization", "Bearer ordinary-user")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{resource}");
    }
}

#[tokio::test]
async fn anonymous_download_and_unbound_token_with_spoofed_tenant_are_denied_before_rpc() {
    let router = router();
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/medias/files/file")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v1/medias/files/file")
                .header("authorization", "Bearer unbound")
                .header("x-tenant-id", "victim")
                .header("x-user-id", "victim-user")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[test]
fn public_openapi_has_no_media_management_operations() {
    let document = ApiDoc::openapi();
    for path in ["cleanup-orphaned-assets", "object-acl", "objects", "bucket"] {
        assert!(
            !document
                .paths
                .paths
                .contains_key(&format!("/api/v1/medias/{path}"))
        );
    }
}
