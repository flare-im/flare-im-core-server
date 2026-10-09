//! `flare.control.v1.TenantProjection` gRPC 实现（控制面 → 核的租户投影接收端）。
//!
//! - `UpsertTenant`：校验 → 按 version 幂等写 `tenants`；旧版本返回 `accepted=false`。
//! - `GetVersions`：对账；核不消费功能开关投影，`features_version` 恒为 0。
//! - `UpsertFeatureFlags`：核只有能力开关（`CapabilityService.SetTenantCapabilitySwitch`），返回 `Unimplemented`。
//!
//! 所有投影 RPC 均验证平台公钥（`FLARE_PLATFORM_PUBLIC_KEY`）签的 worker 控制断言并绑定租户；
//! 内网监听不构成授权。员工不经这里：他们的 Staff 上下文只到 admin-gateway 与管理 RPC。

use std::collections::HashMap;
use std::sync::Arc;

use flare_grpc_proto::control::tenant_projection_server::TenantProjection;
use flare_grpc_proto::control::{
    CredentialsProjectionUpsert, FeatureFlagsProjectionUpsert, GetVersionsRequest, ProjectionAck,
    ProjectionVersions, RoleBundleProjectionUpsert, TenantProjectionUpsert, TenantVersions,
};
use flare_im_service_kit::control_auth::{
    PlatformActorKind, PlatformAssertionVerifier, require_platform_assertion,
};
use flare_server_core::auth::credential_snapshot::{
    CredentialKeyRecord, CredentialSnapshot, MAX_CREDENTIAL_KEYS,
};
use flare_server_core::error::grpc::IntoGrpc;
use tonic::{Request, Response, Status};

use crate::application::commands::apply_tenant_projection;
use crate::application::queries::tenant_projection_versions;
use crate::domain::model::TenantProjectionRecord;
use crate::domain::repository::TenantProjectionRepository;

fn credentials_snapshot(req: CredentialsProjectionUpsert) -> Result<CredentialSnapshot, Status> {
    let invalid = || Status::invalid_argument("invalid credential projection");
    if !flare_im_contracts::domain::tenant::is_valid_tenant_id(&req.app_id)
        || req.app_id != flare_im_contracts::utils::normalize_tenant_id(&req.app_id)
        || req.keys.len() > MAX_CREDENTIAL_KEYS
    {
        return Err(invalid());
    }
    let keys = req
        .keys
        .into_iter()
        .map(|key| {
            Ok(CredentialKeyRecord {
                key_id: key.key_id,
                service_account_id: key.service_account_id,
                mode: u8::try_from(key.mode).map_err(|_| invalid())?,
                hash: key.hash.try_into().map_err(|_| invalid())?,
                pepper_fingerprint: key.pepper_fingerprint.try_into().map_err(|_| invalid())?,
                scopes: key.scopes,
                expires_at_ms: key.expires_at_ms,
                ip_allow: key.ip_allow,
                revoked: key.revoked,
                service_account_enabled: key.service_account_enabled,
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    CredentialSnapshot::new(req.app_id, req.version, keys).map_err(|_| invalid())
}

pub struct TenantProjectionGrpcServer<R: TenantProjectionRepository + 'static> {
    repository: Arc<R>,
    control_verifier: Option<PlatformAssertionVerifier>,
}

impl<R: TenantProjectionRepository + 'static> TenantProjectionGrpcServer<R> {
    pub fn new(repository: Arc<R>, control_verifier: Option<PlatformAssertionVerifier>) -> Self {
        Self {
            repository,
            control_verifier,
        }
    }
}

impl<R: TenantProjectionRepository + 'static> Clone for TenantProjectionGrpcServer<R> {
    fn clone(&self) -> Self {
        Self {
            repository: Arc::clone(&self.repository),
            control_verifier: self.control_verifier.clone(),
        }
    }
}

#[tonic::async_trait]
impl<R: TenantProjectionRepository + 'static> TenantProjection for TenantProjectionGrpcServer<R> {
    async fn upsert_credentials(
        &self,
        request: Request<CredentialsProjectionUpsert>,
    ) -> Result<Response<ProjectionAck>, Status> {
        let actor = require_platform_assertion(&request, self.control_verifier.as_ref())?;
        actor.require_kind(PlatformActorKind::ProjectionWorker)?;
        actor.require_tenant(&request.get_ref().app_id)?;
        let snapshot = credentials_snapshot(request.into_inner())?;
        let outcome = self
            .repository
            .replace_credentials(&snapshot)
            .await
            .into_grpc()?;
        Ok(Response::new(ProjectionAck {
            accepted: outcome.accepted,
            current_version: outcome.current_version,
        }))
    }
    async fn upsert_tenant(
        &self,
        request: Request<TenantProjectionUpsert>,
    ) -> Result<Response<ProjectionAck>, Status> {
        let actor = require_platform_assertion(&request, self.control_verifier.as_ref())?;
        actor.require_kind(PlatformActorKind::ProjectionWorker)?;
        actor.require_tenant(&request.get_ref().im_tenant_id)?;
        let record = TenantProjectionRecord::from_proto(request.get_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let outcome = apply_tenant_projection(self.repository.as_ref(), &record)
            .await
            .into_grpc()?;
        Ok(Response::new(ProjectionAck {
            accepted: outcome.accepted,
            current_version: outcome.current_version,
        }))
    }

    async fn upsert_feature_flags(
        &self,
        request: Request<FeatureFlagsProjectionUpsert>,
    ) -> Result<Response<ProjectionAck>, Status> {
        let actor = require_platform_assertion(&request, self.control_verifier.as_ref())?;
        actor.require_kind(PlatformActorKind::ProjectionWorker)?;
        let tenant_id = &request.get_ref().im_tenant_id;
        if tenant_id == "*" {
            if actor.tenant_id != "*" {
                return Err(Status::permission_denied("control tenant mismatch"));
            }
        } else {
            actor.require_tenant(tenant_id)?;
        }
        Err(Status::unimplemented(
            "flare-im-core does not consume feature flag projections; core capability switches go through CapabilityService.SetTenantCapabilitySwitch",
        ))
    }

    /// 核不持有员工角色权限包:只有 Social 投影接收端实现它(供统一网关 admin 分区)。
    /// 先验签并要求通配 worker 范围,再回 UNIMPLEMENTED,与 `UpsertFeatureFlags` 同样的口径。
    async fn upsert_role_bundle(
        &self,
        request: Request<RoleBundleProjectionUpsert>,
    ) -> Result<Response<ProjectionAck>, Status> {
        let actor = require_platform_assertion(&request, self.control_verifier.as_ref())?;
        actor.require_kind(PlatformActorKind::ProjectionWorker)?;
        if actor.tenant_id != "*" {
            return Err(Status::permission_denied("control tenant mismatch"));
        }
        Err(Status::unimplemented(
            "flare-im-core does not consume role bundle projections; the Social projection receiver serves the gateway admin partition",
        ))
    }

    async fn get_versions(
        &self,
        request: Request<GetVersionsRequest>,
    ) -> Result<Response<ProjectionVersions>, Status> {
        let actor = require_platform_assertion(&request, self.control_verifier.as_ref())?;
        actor.require_kind(PlatformActorKind::ProjectionWorker)?;
        let ids = request.into_inner().im_tenant_ids;
        if ids.len() > 512 {
            return Err(Status::invalid_argument("too many projection tenants"));
        }
        if ids.len() == 1 {
            actor.require_tenant(&ids[0])?;
        } else if actor.tenant_id != "*" {
            return Err(Status::permission_denied(
                "multi-tenant projection read requires platform token",
            ));
        }
        if ids.iter().any(|id| {
            !flare_im_contracts::domain::tenant::is_valid_tenant_id(id)
                || id != &flare_im_contracts::utils::normalize_tenant_id(id)
        }) {
            return Err(Status::invalid_argument("im_tenant_ids must be canonical"));
        }
        let versions = tenant_projection_versions(self.repository.as_ref(), &ids)
            .await
            .into_grpc()?;
        let mut versions: HashMap<String, TenantVersions> = versions
            .into_iter()
            .map(|(tenant_id, tenant_version)| {
                (
                    tenant_id,
                    TenantVersions {
                        tenant_version,
                        features_version: 0,
                        credentials_version: 0,
                        role_bundle_version: 0,
                    },
                )
            })
            .collect();
        for (app_id, version) in self
            .repository
            .credential_versions(&ids)
            .await
            .into_grpc()?
        {
            versions.entry(app_id).or_default().credentials_version = version;
        }
        Ok(Response::new(ProjectionVersions { versions }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::TenantProjectionOutcome;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_grpc_proto::control::TenantStatus as ProtoStatus;
    use flare_server_core::error::Result as FlareResult;
    use std::sync::Mutex;

    /// 内存仓储：复刻「只接受更高版本」的写规则，用来验证接口层的幂等语义。
    #[derive(Default)]
    struct MemoryRepo {
        rows: Mutex<HashMap<String, (TenantProjectionRecord, u64)>>,
        credentials: Mutex<HashMap<String, CredentialSnapshot>>,
    }

    #[async_trait::async_trait]
    impl TenantProjectionRepository for MemoryRepo {
        async fn replace_credentials(
            &self,
            p: &CredentialSnapshot,
        ) -> FlareResult<TenantProjectionOutcome> {
            let mut rows = self.credentials.lock().unwrap();
            let current = rows
                .get(p.app_id())
                .map(|p| p.version() as u64)
                .unwrap_or(0);
            let accepted = p.version() as u64 > current;
            if accepted {
                rows.insert(p.app_id().into(), p.clone());
            }
            Ok(TenantProjectionOutcome {
                accepted,
                current_version: current.max(p.version() as u64),
            })
        }
        async fn credential_versions(&self, ids: &[String]) -> FlareResult<Vec<(String, u64)>> {
            Ok(self
                .credentials
                .lock()
                .unwrap()
                .iter()
                .filter(|(id, _)| ids.is_empty() || ids.contains(id))
                .map(|(id, p)| (id.clone(), p.version() as u64))
                .collect())
        }
        async fn upsert_if_newer(
            &self,
            record: &TenantProjectionRecord,
        ) -> FlareResult<TenantProjectionOutcome> {
            let mut rows = self.rows.lock().unwrap();
            let current = rows.get(&record.tenant_id).map(|(_, v)| *v).unwrap_or(0);
            if record.version > current {
                rows.insert(record.tenant_id.clone(), (record.clone(), record.version));
                Ok(TenantProjectionOutcome {
                    accepted: true,
                    current_version: record.version,
                })
            } else {
                Ok(TenantProjectionOutcome {
                    accepted: false,
                    current_version: current,
                })
            }
        }

        async fn versions(&self, tenant_ids: &[String]) -> FlareResult<Vec<(String, u64)>> {
            let rows = self.rows.lock().unwrap();
            Ok(rows
                .iter()
                .filter(|(id, _)| tenant_ids.is_empty() || tenant_ids.contains(id))
                .map(|(id, (_, v))| (id.clone(), *v))
                .collect())
        }
    }

    fn upsert(tenant: &str, status: ProtoStatus, version: u64) -> TenantProjectionUpsert {
        TenantProjectionUpsert {
            organization_id: Default::default(),
            im_tenant_id: tenant.to_string(),
            name: tenant.to_string(),
            status: status as i32,
            quota: None,
            settings_json: Vec::new(),
            version,
        }
    }

    fn server() -> (Arc<MemoryRepo>, TenantProjectionGrpcServer<MemoryRepo>) {
        let repo = Arc::new(MemoryRepo::default());
        let key = SigningKey::from_bytes(&[71; 32]);
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        let verifier = PlatformAssertionVerifier::from_public_key(&public).unwrap();
        (
            repo.clone(),
            TenantProjectionGrpcServer::new(repo, Some(verifier)),
        )
    }

    fn authed<T>(payload: T, tenant_id: &str) -> Request<T> {
        signed_request(
            payload,
            tenant_id,
            flare_im_service_kit::control_auth::PROJECTION_WORKER_SUBJECT,
            flare_im_service_kit::control_auth::CORE_PROJECTION_AUDIENCE,
        )
    }

    #[tokio::test]
    async fn credentials_rpc_requires_worker_scope_and_preserves_empty_watermarks() {
        let (repo, server) = server();
        let request = || CredentialsProjectionUpsert {
            app_id: "app-a".into(),
            version: 9,
            keys: vec![],
        };
        assert_eq!(
            server
                .upsert_credentials(Request::new(request()))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            server
                .upsert_credentials(authed(request(), "app-b"))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        // An employee subject is not a platform service: refused as unauthenticated.
        let employee = signed_request(
            request(),
            "app-a",
            "employee-7",
            flare_im_service_kit::control_auth::CORE_PROJECTION_AUDIENCE,
        );
        assert_eq!(
            server
                .upsert_credentials(employee)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        assert!(repo.credentials.lock().unwrap().is_empty());
        let ack = server
            .upsert_credentials(authed(request(), "app-a"))
            .await
            .unwrap()
            .into_inner();
        assert!(ack.accepted);
        assert_eq!(ack.current_version, 9);
        let mut old = request();
        old.version = 8;
        let ack = server
            .upsert_credentials(authed(old, "app-a"))
            .await
            .unwrap()
            .into_inner();
        assert!(!ack.accepted);
        assert_eq!(ack.current_version, 9);
        let versions = server
            .get_versions(authed(
                GetVersionsRequest {
                    im_tenant_ids: vec!["app-a".into()],
                },
                "app-a",
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(versions.versions["app-a"].credentials_version, 9);
        let mut invalid = request();
        invalid.version = 0;
        assert_eq!(
            server
                .upsert_credentials(authed(invalid, "app-a"))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        let mut invalid = request();
        invalid
            .keys
            .push(flare_grpc_proto::control::CredentialKey::default());
        assert_eq!(
            server
                .upsert_credentials(authed(invalid, "app-a"))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(repo.credentials.lock().unwrap()["app-a"].version(), 9);
    }

    fn signed_request<T>(payload: T, tenant_id: &str, subject: &str, audience: &str) -> Request<T> {
        let key = SigningKey::from_bytes(&[71; 32]);
        let private = key.to_pkcs8_pem(Default::default()).unwrap();
        let signer = flare_server_core::TokenService::from_ed25519_private_key(
            &private,
            flare_im_service_kit::control_auth::PLATFORM_ISSUER,
            60,
        )
        .unwrap();
        let token = signer
            .generate_with_ed25519(subject, Some(tenant_id), "control", audience)
            .unwrap();
        let mut request = Request::new(payload);
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        request
    }

    #[tokio::test]
    async fn upsert_is_idempotent_by_version() {
        let (repo, server) = server();

        let ack = server
            .upsert_tenant(authed(upsert("acme", ProtoStatus::Active, 3), "acme"))
            .await
            .unwrap()
            .into_inner();
        assert!(ack.accepted);
        assert_eq!(ack.current_version, 3);

        // 同版本重放：丢弃，报告本地版本。
        let ack = server
            .upsert_tenant(authed(upsert("acme", ProtoStatus::Suspended, 3), "acme"))
            .await
            .unwrap()
            .into_inner();
        assert!(!ack.accepted);
        assert_eq!(ack.current_version, 3);

        // 旧版本乱序到达：丢弃。
        let ack = server
            .upsert_tenant(authed(upsert("acme", ProtoStatus::Suspended, 2), "acme"))
            .await
            .unwrap()
            .into_inner();
        assert!(!ack.accepted);
        assert_eq!(ack.current_version, 3);
        assert_eq!(
            repo.rows.lock().unwrap()["acme"].0.status,
            flare_im_contracts::domain::tenant::TenantStatus::Active
        );

        // 更高版本：生效。
        let ack = server
            .upsert_tenant(authed(upsert("acme", ProtoStatus::Suspended, 4), "acme"))
            .await
            .unwrap()
            .into_inner();
        assert!(ack.accepted);
        assert_eq!(ack.current_version, 4);
        assert_eq!(
            repo.rows.lock().unwrap()["acme"].0.status,
            flare_im_contracts::domain::tenant::TenantStatus::Suspended
        );
    }

    #[tokio::test]
    async fn invalid_projection_is_invalid_argument() {
        let (_, server) = server();
        let status = server
            .upsert_tenant(authed(upsert("acme", ProtoStatus::Active, 0), "acme"))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let status = server
            .upsert_tenant(authed(upsert("*", ProtoStatus::Active, 1), "*"))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn get_versions_reports_tenant_version_and_zero_features_version() {
        let (_, server) = server();
        server
            .upsert_tenant(authed(upsert("a", ProtoStatus::Active, 5), "a"))
            .await
            .unwrap();
        server
            .upsert_tenant(authed(upsert("b", ProtoStatus::Active, 8), "b"))
            .await
            .unwrap();

        let all = server
            .get_versions(authed(
                GetVersionsRequest {
                    im_tenant_ids: vec![],
                },
                "*",
            ))
            .await
            .unwrap()
            .into_inner()
            .versions;
        assert_eq!(all.len(), 2);
        assert_eq!(all["a"].tenant_version, 5);
        assert_eq!(all["a"].features_version, 0);

        let some = server
            .get_versions(authed(
                GetVersionsRequest {
                    im_tenant_ids: vec!["b".to_string(), "missing".to_string()],
                },
                "*",
            ))
            .await
            .unwrap()
            .into_inner()
            .versions;
        assert_eq!(some.len(), 1);
        assert_eq!(some["b"].tenant_version, 8);
    }

    #[tokio::test]
    async fn feature_flags_are_unimplemented() {
        let (_, server) = server();
        let status = server
            .upsert_feature_flags(authed(
                FeatureFlagsProjectionUpsert {
                    im_tenant_id: "acme".into(),
                    ..Default::default()
                },
                "acme",
            ))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn projection_rejects_untrusted_actor_audience_and_cross_tenant_body() {
        let (_, server) = server();
        let body = upsert("tenant-a", ProtoStatus::Active, 1);
        assert_eq!(
            server
                .upsert_tenant(Request::new(body.clone()))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            server
                .upsert_tenant(authed(body.clone(), "tenant-b"))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        // An employee subject is not a platform service at all (no admin assertion exists).
        assert_eq!(
            server
                .upsert_tenant(signed_request(
                    body.clone(),
                    "tenant-a",
                    "employee-1",
                    flare_im_service_kit::control_auth::CORE_PROJECTION_AUDIENCE
                ))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            server
                .upsert_tenant(signed_request(
                    body,
                    "tenant-a",
                    flare_im_service_kit::control_auth::PROJECTION_WORKER_SUBJECT,
                    "flare-core-admin"
                ))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            server
                .get_versions(authed(
                    GetVersionsRequest {
                        im_tenant_ids: vec!["tenant-a".into(), "tenant-b".into()]
                    },
                    "tenant-a"
                ))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }
}
