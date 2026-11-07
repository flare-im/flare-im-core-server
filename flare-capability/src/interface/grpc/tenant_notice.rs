//! `flare.capability.v1.TenantNoticeService`：业务服务发布自己命名空间里的租户配置通知。
//!
//! 只认 Cell 签名的服务身份（`x-flare-ctx`，与 message ingest 同一把公钥集）；租户只取签名里的
//! （`*` 为全部租户），调用方元数据不起作用。没有验签器或没有通知通道时 fail closed（UNAVAILABLE）。

use std::sync::Arc;

use flare_grpc_proto::capability::tenant_notice_service_server::TenantNoticeService;
use flare_grpc_proto::capability::{PublishTenantNoticeRequest, PublishTenantNoticeResponse};
use flare_im_contracts::domain::tenant::is_valid_tenant_id;
use flare_im_contracts::utils::normalize_tenant_id;
use flare_im_service_kit::tenant_notice::ALL_TENANTS;
use flare_server_core::auth::{SignedActorType, SignedContextVerifier};
use flare_server_core::grpc::middleware::signed_context::{
    VerifiedContext, verify_signed_context_metadata,
};
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::application::commands::publish_tenant_notice;
use crate::domain::model::{NoticeDenied, NoticePublishers};
use crate::domain::repository::tenant_notice::TenantNotices;

#[derive(Clone)]
pub struct TenantNoticeGrpcServer {
    notices: Option<Arc<dyn TenantNotices>>,
    publishers: Arc<NoticePublishers>,
    verifier: Option<SignedContextVerifier>,
}

impl TenantNoticeGrpcServer {
    pub fn new(
        notices: Option<Arc<dyn TenantNotices>>,
        publishers: NoticePublishers,
        verifier: Option<SignedContextVerifier>,
    ) -> Self {
        Self {
            notices,
            publishers: Arc::new(publishers),
            verifier,
        }
    }
}

/// 签名里的服务名与租户。
fn signed_service(
    metadata: &MetadataMap,
    verifier: Option<&SignedContextVerifier>,
) -> Result<(String, String), Status> {
    let verifier = verifier
        .ok_or_else(|| Status::unavailable("tenant notice identity verifier not configured"))?;
    let ctx = verify_signed_context_metadata(metadata, verifier)?;
    let claims = ctx
        .get_data::<VerifiedContext>()
        .and_then(|verified| verified.claims())
        .ok_or_else(|| Status::unauthenticated("signed service identity required"))?;
    if claims.actor_type != SignedActorType::Service {
        return Err(Status::permission_denied(
            "only services publish tenant notices",
        ));
    }
    let tenant = claims.tenant_id.trim();
    let canonical = tenant == ALL_TENANTS
        || (is_valid_tenant_id(tenant) && normalize_tenant_id(tenant) == tenant);
    if !canonical || tenant != claims.tenant_id {
        return Err(Status::permission_denied("non-canonical tenant identity"));
    }
    Ok((claims.actor_id.clone(), claims.tenant_id.clone()))
}

fn denied(reason: NoticeDenied) -> Status {
    match reason {
        NoticeDenied::InvalidTopic | NoticeDenied::InvalidVersion => {
            Status::invalid_argument(reason.to_string())
        }
        NoticeDenied::TopicNotPermitted | NoticeDenied::TenantWideNotPermitted => {
            Status::permission_denied(reason.to_string())
        }
    }
}

#[tonic::async_trait]
impl TenantNoticeService for TenantNoticeGrpcServer {
    async fn publish(
        &self,
        request: Request<PublishTenantNoticeRequest>,
    ) -> Result<Response<PublishTenantNoticeResponse>, Status> {
        let (service, tenant) = signed_service(request.metadata(), self.verifier.as_ref())?;
        let notices = self
            .notices
            .as_deref()
            .ok_or_else(|| Status::unavailable("tenant notice channel not configured"))?;
        let body = request.into_inner();
        let outcome = publish_tenant_notice(
            notices,
            &self.publishers,
            &service,
            &tenant,
            &body.topic,
            body.version,
        )
        .await
        .map_err(denied)?
        .map_err(|error| Status::unavailable(error.to_string()))?;
        Ok(Response::new(PublishTenantNoticeResponse {
            accepted: outcome.accepted,
            current_version: outcome.current_version,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::NoticeOutcome;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_server_core::{SignedContextIdentity, SignedContextSigner};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Recorded {
        published: Mutex<Vec<(String, String, u64)>>,
        last: Mutex<u64>,
    }

    #[async_trait::async_trait]
    impl TenantNotices for Recorded {
        async fn publish(
            &self,
            tenant_id: &str,
            topic: &str,
            version: u64,
        ) -> flare_server_core::error::Result<NoticeOutcome> {
            let mut last = self.last.lock().unwrap();
            if version <= *last {
                return Ok(NoticeOutcome {
                    accepted: false,
                    current_version: *last,
                });
            }
            *last = version;
            self.published
                .lock()
                .unwrap()
                .push((tenant_id.into(), topic.into(), version));
            Ok(NoticeOutcome {
                accepted: true,
                current_version: version,
            })
        }
    }

    fn keys(seed: u8) -> (SignedContextSigner, SignedContextVerifier) {
        let key = SigningKey::from_bytes(&[seed; 32]);
        let private = key.to_pkcs8_pem(Default::default()).unwrap();
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        (
            SignedContextSigner::from_private_key(&private).unwrap(),
            SignedContextVerifier::from_public_key(&public).unwrap(),
        )
    }

    fn request(
        signer: &SignedContextSigner,
        actor_type: SignedActorType,
        actor: &str,
        tenant: &str,
        topic: &str,
        version: u64,
    ) -> Request<PublishTenantNoticeRequest> {
        let token = signer
            .sign(&SignedContextIdentity::new(
                actor_type,
                actor,
                tenant,
                "req-notice",
            ))
            .unwrap();
        let mut request = Request::new(PublishTenantNoticeRequest {
            topic: topic.into(),
            version,
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", "forged".parse().unwrap());
        request
            .metadata_mut()
            .insert("x-flare-ctx", token.parse().unwrap());
        request
    }

    fn server(seed: u8) -> (Arc<Recorded>, TenantNoticeGrpcServer, SignedContextSigner) {
        let (signer, verifier) = keys(seed);
        let recorded = Arc::new(Recorded::default());
        let server = TenantNoticeGrpcServer::new(
            Some(recorded.clone() as Arc<dyn TenantNotices>),
            NoticePublishers::parse("flare-social-bridge=social.,*").unwrap(),
            Some(verifier),
        );
        (recorded, server, signer)
    }

    #[tokio::test]
    async fn a_registered_service_publishes_its_topic_once_per_version() {
        let (recorded, server, signer) = server(61);
        let bridge = |version| {
            request(
                &signer,
                SignedActorType::Service,
                "flare-social-bridge",
                "app-a",
                "social.features",
                version,
            )
        };
        assert!(
            server
                .publish(bridge(3))
                .await
                .unwrap()
                .into_inner()
                .accepted
        );
        let replay = server.publish(bridge(3)).await.unwrap().into_inner();
        assert!(!replay.accepted);
        assert_eq!(replay.current_version, 3);
        assert_eq!(
            *recorded.published.lock().unwrap(),
            vec![("app-a".to_string(), "social.features".to_string(), 3)]
        );
        let wide = request(
            &signer,
            SignedActorType::Service,
            "flare-social-bridge",
            "*",
            "social.features",
            4,
        );
        assert!(server.publish(wide).await.unwrap().into_inner().accepted);
    }

    #[tokio::test]
    async fn users_other_namespaces_and_core_topics_are_denied() {
        let (recorded, server, signer) = server(62);
        let cases = [
            (
                SignedActorType::User,
                "alice",
                "app-a",
                "social.features",
                1,
                tonic::Code::PermissionDenied,
            ),
            (
                SignedActorType::Service,
                "flare-api-gateway",
                "app-a",
                "social.features",
                1,
                tonic::Code::PermissionDenied,
            ),
            (
                SignedActorType::Service,
                "flare-social-bridge",
                "app-a",
                "billing.plan",
                1,
                tonic::Code::PermissionDenied,
            ),
            (
                SignedActorType::Service,
                "flare-social-bridge",
                "app-a",
                "flare.plugin.catalog",
                1,
                tonic::Code::InvalidArgument,
            ),
            (
                SignedActorType::Service,
                "flare-social-bridge",
                "app-a",
                "social.features",
                0,
                tonic::Code::InvalidArgument,
            ),
        ];
        for (actor_type, actor, tenant, topic, version, code) in cases {
            let status = server
                .publish(request(&signer, actor_type, actor, tenant, topic, version))
                .await
                .unwrap_err();
            assert_eq!(status.code(), code, "{actor} {tenant:?} {topic} {version}");
        }
        assert!(recorded.published.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unsigned_or_foreign_signatures_are_unauthenticated() {
        let (_, server, _) = server(63);
        let (other, _) = keys(64);
        let forged = request(
            &other,
            SignedActorType::Service,
            "flare-social-bridge",
            "app-a",
            "social.features",
            1,
        );
        assert_eq!(
            server.publish(forged).await.unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        let unsigned = Request::new(PublishTenantNoticeRequest {
            topic: "social.features".into(),
            version: 1,
        });
        assert_eq!(
            server.publish(unsigned).await.unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }
}
