//! `flare.capability.v1.CapabilityAdminService`：管理员只读（admin-gateway 专用下游）。
//!
//! 只认 admin-gateway 原样转发的 admin 令牌（受众 `flare-core-admin`、员工、具体租户）；
//! 租户只取自令牌，请求不带 tenant_id。数据只来自本进程持有的开关表与租户投影，
//! 网关不需要任何数据库角色。

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
use flare_im_service_kit::control_auth::{ConsoleControlVerifier, require_grpc_admin};
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
    control_verifier: Option<ConsoleControlVerifier>,
}

impl<R: TenantRuntimeReader + 'static> CapabilityAdminGrpcServer<R> {
    pub fn new(
        policy: Arc<dyn CapabilityPolicyBackend>,
        runtime: Option<Arc<R>>,
        control_verifier: Option<ConsoleControlVerifier>,
    ) -> Self {
        Self {
            policy,
            runtime,
            control_verifier,
        }
    }
}

impl<R: TenantRuntimeReader + 'static> Clone for CapabilityAdminGrpcServer<R> {
    fn clone(&self) -> Self {
        Self {
            policy: Arc::clone(&self.policy),
            runtime: self.runtime.clone(),
            control_verifier: self.control_verifier.clone(),
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
        let actor = require_grpc_admin(&request, self.control_verifier.as_ref())?;
        tracing::info!(actor_id = %actor.subject, tenant_id = %actor.tenant_id, "admin list_tenant_capability_switches");
        let switches = tenant_capability_switches(self.policy.as_ref(), &actor.tenant_id)
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
            tenant_id: actor.tenant_id,
            switches,
        }))
    }

    async fn get_tenant_runtime(
        &self,
        request: Request<GetTenantRuntimeRequest>,
    ) -> Result<Response<GetTenantRuntimeResponse>, Status> {
        let actor = require_grpc_admin(&request, self.control_verifier.as_ref())?;
        let reader = self.runtime.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "tenant projection store not configured: configure database_url for flare-capability",
            )
        })?;
        tracing::info!(actor_id = %actor.subject, tenant_id = %actor.tenant_id, "admin get_tenant_runtime");
        let view = tenant_runtime(reader.as_ref(), &actor.tenant_id)
            .await
            .into_grpc()?;
        Ok(Response::new(runtime_response(actor.tenant_id, view)))
    }
}

#[cfg(test)]
mod tests {
    //! 缺令牌、错误受众、跨租户三类负向用例，外加租户只取自令牌的正向对照。

    use std::collections::HashMap;

    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_im_contracts::domain::tenant::{TenantCoreQuota, TenantQuota};
    use flare_im_service_kit::control_auth::{
        CONSOLE_ISSUER, CORE_ADMIN_AUDIENCE, CORE_CONTROL_AUDIENCE, PROJECTION_WORKER_SUBJECT,
        SCHEDULER_SUBJECT,
    };
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

    const KEY: [u8; 32] = [53; 32];

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
            let public = SigningKey::from_bytes(&KEY)
                .verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap();
            ConsoleControlVerifier::from_public_key(&public).unwrap()
        });
        CapabilityAdminGrpcServer::new(policy, with_db.then(|| Arc::new(runtime)), verifier)
    }

    fn signed<T>(
        payload: T,
        subject: &str,
        tenant: &str,
        token_use: &str,
        aud: &str,
    ) -> Request<T> {
        let private = SigningKey::from_bytes(&KEY)
            .to_pkcs8_pem(Default::default())
            .unwrap();
        let token =
            flare_server_core::TokenService::from_ed25519_private_key(&private, CONSOLE_ISSUER, 60)
                .unwrap()
                .generate_with_ed25519(subject, Some(tenant), token_use, aud)
                .unwrap();
        let mut request = Request::new(payload);
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        request
    }

    fn admin<T>(payload: T, tenant: &str) -> Request<T> {
        signed(payload, "employee-1", tenant, "admin", CORE_ADMIN_AUDIENCE)
    }

    fn code<T>(result: Result<T, Status>) -> Code {
        result.map(|_| ()).unwrap_err().code()
    }

    #[tokio::test]
    async fn admin_reads_require_the_forwarded_admin_token() {
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
        let unkeyed = server(false, true);
        assert_eq!(
            code(
                unkeyed
                    .get_tenant_runtime(admin(GetTenantRuntimeRequest {}, "tenant-a"))
                    .await
            ),
            Code::Unauthenticated
        );
    }

    #[tokio::test]
    async fn admin_reads_reject_wrong_audience_and_non_employee_tokens() {
        let capability = server(true, true);
        for (token_use, aud) in [
            ("control", CORE_CONTROL_AUDIENCE),
            ("admin", CORE_CONTROL_AUDIENCE),
            ("control", CORE_ADMIN_AUDIENCE),
        ] {
            assert_eq!(
                code(
                    capability
                        .list_tenant_capability_switches(signed(
                            ListTenantCapabilitySwitchesRequest {},
                            "employee-1",
                            "tenant-a",
                            token_use,
                            aud,
                        ))
                        .await
                ),
                Code::Unauthenticated,
                "{token_use}/{aud}"
            );
        }
        for (subject, tenant) in [
            (PROJECTION_WORKER_SUBJECT, "tenant-a"),
            (SCHEDULER_SUBJECT, "tenant-a"),
            ("employee-1", "*"),
        ] {
            assert_eq!(
                code(
                    capability
                        .get_tenant_runtime(signed(
                            GetTenantRuntimeRequest {},
                            subject,
                            tenant,
                            "admin",
                            CORE_ADMIN_AUDIENCE,
                        ))
                        .await
                ),
                Code::PermissionDenied,
                "{subject}/{tenant}"
            );
        }
    }

    #[tokio::test]
    async fn admin_reads_are_scoped_to_the_token_tenant() {
        let capability = server(true, true);
        let a = capability
            .list_tenant_capability_switches(admin(
                ListTenantCapabilitySwitchesRequest {},
                "tenant-a",
            ))
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
            .list_tenant_capability_switches(admin(
                ListTenantCapabilitySwitchesRequest {},
                "tenant-b",
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(b.switches.len(), 1);
        assert_eq!(b.switches[0].capability_id, "social.moments");

        let runtime = capability
            .get_tenant_runtime(admin(GetTenantRuntimeRequest {}, "tenant-b"))
            .await
            .unwrap()
            .into_inner();
        assert!(runtime.projected);
        assert_eq!(runtime.tenant_id, "tenant-b");
        assert_eq!(runtime.status, ProtoStatus::Suspended as i32);
        assert_eq!(runtime.version, 9);
        assert_eq!(
            runtime
                .quota
                .unwrap()
                .core
                .unwrap()
                .max_online_devices_per_user,
            2
        );

        // 跨租户：没有投影行的租户只拿到自己的空视图，拿不到别人的。
        let unknown = capability
            .get_tenant_runtime(admin(GetTenantRuntimeRequest {}, "tenant-c"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(unknown.tenant_id, "tenant-c");
        assert!(!unknown.projected);
        assert_eq!(unknown.version, 0);
        assert!(
            capability
                .list_tenant_capability_switches(admin(
                    ListTenantCapabilitySwitchesRequest {},
                    "tenant-c"
                ))
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
            code(
                capability
                    .get_tenant_runtime(admin(GetTenantRuntimeRequest {}, "tenant-a"))
                    .await
            ),
            Code::FailedPrecondition
        );
    }
}
