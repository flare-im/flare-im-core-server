//! `flare.capability.v1.CapabilityAdminService`：管理员只读（admin-gateway 专用下游）。
//!
//! 只认 admin-gateway 原样转发的 Staff `x-flare-ctx`（网关 admin 分区按 `kid=gateway` 签）：
//! 租户运行时要求 `core:tenant:read`，能力开关要求 `core:capabilities:read`；租户只取自签名，
//! 请求不带 tenant_id。数据只来自本进程持有的开关表与租户投影，网关不需要任何数据库角色。

use std::sync::Arc;

use flare_grpc_proto::capability::capability_admin_service_server::CapabilityAdminService;
use flare_grpc_proto::capability::{
    GetTenantRuntimeRequest, GetTenantRuntimeResponse, ListTenantCapabilitySwitchesRequest,
    ListTenantCapabilitySwitchesResponse, TenantCapabilitySwitch as ProtoSwitch,
};
use flare_grpc_proto::control::{
    CoreQuota as ProtoCoreQuota, TenantQuota as ProtoQuota, TenantStatus as ProtoStatus,
};
use flare_im_contracts::domain::tenant::TenantStatus;
use flare_im_service_kit::staff_context::{VerifiedStaff, require_staff};
use flare_server_core::SignedContextVerifier;
use flare_server_core::auth::core_admin::permission::{CAPABILITIES_READ, TENANT_READ};
use flare_server_core::error::grpc::IntoGrpc;
use prost_types::Timestamp;
use tonic::{Request, Response, Status};

use crate::application::queries::{tenant_capability_switches, tenant_runtime};
use crate::domain::capability::CapabilityPolicyBackend;
use crate::domain::model::TenantRuntimeView;
use crate::domain::repository::TenantRuntimeReader;
use crate::interface::grpc::shared::helpers::capability_error_to_status;

pub struct CapabilityAdminGrpcServer<R: TenantRuntimeReader + 'static> {
    policy: Arc<dyn CapabilityPolicyBackend>,
    /// 无数据库时没有 `tenants` 表可读：`GetTenantRuntime` 返回 FAILED_PRECONDITION。
    runtime: Option<Arc<R>>,
    /// Cell 公钥集，Staff 钉死 `kid=gateway`；没有配置时两个 RPC 一律 UNAUTHENTICATED。
    staff_verifier: Option<SignedContextVerifier>,
}

impl<R: TenantRuntimeReader + 'static> CapabilityAdminGrpcServer<R> {
    pub fn new(
        policy: Arc<dyn CapabilityPolicyBackend>,
        runtime: Option<Arc<R>>,
        staff_verifier: Option<SignedContextVerifier>,
    ) -> Self {
        Self {
            policy,
            runtime,
            staff_verifier,
        }
    }

    fn staff<T>(&self, request: &Request<T>, permission: &str) -> Result<VerifiedStaff, Status> {
        let verifier = self
            .staff_verifier
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("staff context verifier not configured"))?;
        require_staff(request, verifier, permission)
    }
}

impl<R: TenantRuntimeReader + 'static> Clone for CapabilityAdminGrpcServer<R> {
    fn clone(&self) -> Self {
        Self {
            policy: Arc::clone(&self.policy),
            runtime: self.runtime.clone(),
            staff_verifier: self.staff_verifier.clone(),
        }
    }
}

fn timestamp(at: chrono::DateTime<chrono::Utc>) -> Timestamp {
    Timestamp {
        seconds: at.timestamp(),
        nanos: at.timestamp_subsec_nanos() as i32,
    }
}

fn runtime_response(
    tenant_id: String,
    view: Option<TenantRuntimeView>,
) -> GetTenantRuntimeResponse {
    let Some(view) = view else {
        return GetTenantRuntimeResponse {
            tenant_id,
            projected: false,
            ..Default::default()
        };
    };
    let status = match view.status {
        TenantStatus::Active => ProtoStatus::Active,
        TenantStatus::Suspended => ProtoStatus::Suspended,
        TenantStatus::Deleting => ProtoStatus::Deleting,
    };
    GetTenantRuntimeResponse {
        tenant_id,
        projected: true,
        name: view.name,
        status: status as i32,
        quota: Some(ProtoQuota {
            max_users: view.quota.max_users,
            max_groups: view.quota.max_groups,
            max_group_members: view.quota.max_group_members,
            core: Some(ProtoCoreQuota {
                send_qps: view.quota.core.send_qps,
                sync_pull_qps: view.quota.core.sync_pull_qps,
                max_online_devices_per_user: view.quota.core.max_online_devices_per_user,
            }),
        }),
        settings_json: view.settings_json,
        version: view.version,
        projected_at: view.projected_at.map(timestamp),
        updated_at: view.updated_at.map(timestamp),
    }
}

#[tonic::async_trait]
impl<R: TenantRuntimeReader + 'static> CapabilityAdminService for CapabilityAdminGrpcServer<R> {
    async fn list_tenant_capability_switches(
        &self,
        request: Request<ListTenantCapabilitySwitchesRequest>,
    ) -> Result<Response<ListTenantCapabilitySwitchesResponse>, Status> {
        let staff = self.staff(&request, CAPABILITIES_READ)?;
        tracing::info!(actor_id = %staff.actor_id, tenant_id = %staff.tenant_id, request_id = %staff.request_id, "admin list_tenant_capability_switches");
        let switches = tenant_capability_switches(self.policy.as_ref(), &staff.tenant_id)
            .await
            .map_err(capability_error_to_status)?
            .into_iter()
            .map(|switch| ProtoSwitch {
                capability_id: switch.capability_id,
                enabled: switch.enabled,
                updated_at: switch.updated_at.map(timestamp),
            })
            .collect();
        Ok(Response::new(ListTenantCapabilitySwitchesResponse {
            tenant_id: staff.tenant_id,
            switches,
        }))
    }

    async fn get_tenant_runtime(
        &self,
        request: Request<GetTenantRuntimeRequest>,
    ) -> Result<Response<GetTenantRuntimeResponse>, Status> {
        let staff = self.staff(&request, TENANT_READ)?;
        let reader = self.runtime.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "tenant projection store not configured: configure database_url for flare-capability",
            )
        })?;
        tracing::info!(actor_id = %staff.actor_id, tenant_id = %staff.tenant_id, request_id = %staff.request_id, "admin get_tenant_runtime");
        let view = tenant_runtime(reader.as_ref(), &staff.tenant_id)
            .await
            .into_grpc()?;
        Ok(Response::new(runtime_response(staff.tenant_id, view)))
    }
}

#[cfg(test)]
mod tests {
    //! 缺上下文、错误 kid、平台断言、权限码不符、跨租户的负向用例，外加租户只取自签名的正向对照。

    use std::collections::HashMap;

    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_im_contracts::domain::tenant::{TenantCoreQuota, TenantQuota};
    use flare_im_service_kit::control_auth::{CORE_PROJECTION_AUDIENCE, PLATFORM_ISSUER};
    use flare_server_core::auth::{KeySet, kid};
    use flare_server_core::{SignedActorType, SignedContextIdentity, SignedContextSigner};
    use tonic::Code;

    use super::*;
    use crate::infrastructure::capability::InMemoryCapabilityGrants;

    #[derive(Default)]
    struct MemoryRuntime(HashMap<String, TenantRuntimeView>);

    #[async_trait::async_trait]
    impl TenantRuntimeReader for MemoryRuntime {
        async fn tenant_runtime(
            &self,
            tenant_id: &str,
        ) -> flare_server_core::error::Result<Option<TenantRuntimeView>> {
            Ok(self.0.get(tenant_id).cloned())
        }
    }

    const GATEWAY_KEY: [u8; 32] = [53; 32];
    const HOOK_KEY: [u8; 32] = [54; 32];

    fn pem(seed: [u8; 32]) -> (String, String) {
        let key = SigningKey::from_bytes(&seed);
        (
            key.to_pkcs8_pem(Default::default()).unwrap().to_string(),
            key.verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap(),
        )
    }

    fn view(tenant: &str, status: TenantStatus, version: u64) -> TenantRuntimeView {
        TenantRuntimeView {
            tenant_id: tenant.to_string(),
            name: format!("name-{tenant}"),
            status,
            quota: TenantQuota {
                max_users: 10,
                max_groups: 0,
                max_group_members: 0,
                core: TenantCoreQuota {
                    send_qps: 5,
                    sync_pull_qps: 0,
                    max_online_devices_per_user: 2,
                },
            },
            settings_json: r#"{"locale":"zh-CN"}"#.to_string(),
            version,
            projected_at: Some(chrono::Utc::now()),
            updated_at: Some(chrono::Utc::now()),
        }
    }

    fn server(with_key: bool, with_db: bool) -> CapabilityAdminGrpcServer<MemoryRuntime> {
        let policy = Arc::new(InMemoryCapabilityGrants::new());
        policy.set_tenant_capability("tenant-a", "rtc.call", true);
        policy.set_tenant_capability("tenant-a", "ai.summary", false);
        policy.set_tenant_capability("tenant-b", "social.moments", true);
        let mut runtime = MemoryRuntime::default();
        runtime.0.insert(
            "tenant-a".to_string(),
            view("tenant-a", TenantStatus::Active, 7),
        );
        runtime.0.insert(
            "tenant-b".to_string(),
            view("tenant-b", TenantStatus::Suspended, 9),
        );
        let verifier = with_key.then(|| {
            let mut keys = KeySet::empty();
            keys.insert(kid::GATEWAY, &pem(GATEWAY_KEY).1).unwrap();
            keys.insert(kid::HOOK, &pem(HOOK_KEY).1).unwrap();
            SignedContextVerifier::from_key_set(keys).require_staff_kid([kid::GATEWAY])
        });
        CapabilityAdminGrpcServer::new(policy, with_db.then(|| Arc::new(runtime)), verifier)
    }

    fn staff_token(seed: [u8; 32], key_id: &str, tenant: &str, permission: &str) -> String {
        SignedContextSigner::from_private_key(&pem(seed).0)
            .unwrap()
            .with_kid(key_id)
            .unwrap()
            .sign(
                &SignedContextIdentity::new(SignedActorType::Staff, "employee-1", tenant, "req-1")
                    .with_staff_grant(Some("org-1".into()), Some(permission.into()), 2),
            )
            .unwrap()
    }

    fn with_ctx<T>(payload: T, token: &str) -> Request<T> {
        let mut request = Request::new(payload);
        request
            .metadata_mut()
            .insert("x-flare-ctx", token.parse().unwrap());
        request
    }

    fn switches(tenant: &str) -> Request<ListTenantCapabilitySwitchesRequest> {
        with_ctx(
            ListTenantCapabilitySwitchesRequest {},
            &staff_token(GATEWAY_KEY, kid::GATEWAY, tenant, CAPABILITIES_READ),
        )
    }

    fn runtime(tenant: &str) -> Request<GetTenantRuntimeRequest> {
        with_ctx(
            GetTenantRuntimeRequest {},
            &staff_token(GATEWAY_KEY, kid::GATEWAY, tenant, TENANT_READ),
        )
    }

    fn code<T>(result: Result<T, Status>) -> Code {
        result.map(|_| ()).unwrap_err().code()
    }

    #[tokio::test]
    async fn admin_reads_require_the_forwarded_staff_context() {
        let capability = server(true, true);
        assert_eq!(
            code(
                capability
                    .list_tenant_capability_switches(Request::new(
                        ListTenantCapabilitySwitchesRequest {}
                    ))
                    .await
            ),
            Code::Unauthenticated
        );
        assert_eq!(
            code(
                capability
                    .get_tenant_runtime(Request::new(GetTenantRuntimeRequest {}))
                    .await
            ),
            Code::Unauthenticated
        );
        // 平台断言放在 authorization 里不是身份。
        let platform = flare_server_core::TokenService::from_ed25519_private_key(
            &pem([55; 32]).0,
            PLATFORM_ISSUER,
            60,
        )
        .unwrap();
        for (token_use, aud) in [
            ("admin", "flare-core-admin"),
            ("control", CORE_PROJECTION_AUDIENCE),
        ] {
            let token = platform
                .generate_with_ed25519("employee-1", Some("tenant-a"), token_use, aud)
                .unwrap();
            let mut request = Request::new(GetTenantRuntimeRequest {});
            request
                .metadata_mut()
                .insert("authorization", format!("Bearer {token}").parse().unwrap());
            assert_eq!(
                code(capability.get_tenant_runtime(request).await),
                Code::Unauthenticated,
                "{token_use}/{aud}"
            );
        }
        let unkeyed = server(false, true);
        assert_eq!(
            code(unkeyed.get_tenant_runtime(runtime("tenant-a")).await),
            Code::Unauthenticated
        );
    }

    #[tokio::test]
    async fn admin_reads_reject_wrong_kid_and_wrong_permission() {
        let capability = server(true, true);
        let forged = staff_token(HOOK_KEY, kid::HOOK, "tenant-a", CAPABILITIES_READ);
        assert_eq!(
            code(
                capability
                    .list_tenant_capability_switches(with_ctx(
                        ListTenantCapabilitySwitchesRequest {},
                        &forged
                    ))
                    .await
            ),
            Code::Unauthenticated
        );
        // 开关的码读不了运行时，运行时的码读不了开关。
        assert_eq!(
            code(
                capability
                    .get_tenant_runtime(with_ctx(
                        GetTenantRuntimeRequest {},
                        &staff_token(GATEWAY_KEY, kid::GATEWAY, "tenant-a", CAPABILITIES_READ)
                    ))
                    .await
            ),
            Code::PermissionDenied
        );
        assert_eq!(
            code(
                capability
                    .list_tenant_capability_switches(with_ctx(
                        ListTenantCapabilitySwitchesRequest {},
                        &staff_token(GATEWAY_KEY, kid::GATEWAY, "tenant-a", TENANT_READ)
                    ))
                    .await
            ),
            Code::PermissionDenied
        );
        for tenant in ["", "*"] {
            assert_eq!(
                code(capability.get_tenant_runtime(runtime(tenant)).await),
                Code::PermissionDenied,
                "{tenant:?}"
            );
        }
    }

    #[tokio::test]
    async fn admin_reads_are_scoped_to_the_signed_tenant() {
        let capability = server(true, true);
        let a = capability
            .list_tenant_capability_switches(switches("tenant-a"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(a.tenant_id, "tenant-a");
        let a: Vec<_> = a
            .switches
            .iter()
            .map(|s| (s.capability_id.as_str(), s.enabled))
            .collect();
        assert_eq!(a, vec![("ai.summary", false), ("rtc.call", true)]);

        let b = capability
            .list_tenant_capability_switches(switches("tenant-b"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(b.switches.len(), 1);
        assert_eq!(b.switches[0].capability_id, "social.moments");

        let projected = capability
            .get_tenant_runtime(runtime("tenant-b"))
            .await
            .unwrap()
            .into_inner();
        assert!(projected.projected);
        assert_eq!(projected.tenant_id, "tenant-b");
        assert_eq!(projected.status, ProtoStatus::Suspended as i32);
        assert_eq!(projected.version, 9);
        assert_eq!(
            projected
                .quota
                .unwrap()
                .core
                .unwrap()
                .max_online_devices_per_user,
            2
        );

        // 跨租户：没有投影行的租户只拿到自己的空视图，拿不到别人的。
        let unknown = capability
            .get_tenant_runtime(runtime("tenant-c"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(unknown.tenant_id, "tenant-c");
        assert!(!unknown.projected);
        assert_eq!(unknown.version, 0);
        assert!(
            capability
                .list_tenant_capability_switches(switches("tenant-c"))
                .await
                .unwrap()
                .into_inner()
                .switches
                .is_empty()
        );
    }

    #[tokio::test]
    async fn runtime_read_without_a_database_fails_after_authentication() {
        let capability = server(true, false);
        assert_eq!(
            code(
                capability
                    .get_tenant_runtime(Request::new(GetTenantRuntimeRequest {}))
                    .await
            ),
            Code::Unauthenticated
        );
        assert_eq!(
            code(capability.get_tenant_runtime(runtime("tenant-a")).await),
            Code::FailedPrecondition
        );
    }
}
