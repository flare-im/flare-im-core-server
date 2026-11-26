//! `CapabilityService.Administer` — Hook 治理命令在控制面的统一入口（与 ExtensionPlugin 执行面分离）。
//!
//! 调用方是 admin-gateway 转发的 Staff `x-flare-ctx`：`service.rs` 先按 operation 复核权限码
//! （读 `core:hooks:read`；写 `core:hooks:write` 或 `core:hooks:platform:write`），再把已验证的员工
//! 交到这里；范围只取自签名，按 hook_id 的操作先核对该配置的范围。进程内的治理服务收到的上下文只由
//! 验过的声明重建，不看 `GenericRequest.metadata` 或载荷。
//!
//! 两个范围：应用（`tenant_id` 为应用，签名里是同一个应用）和平台（`tenant_id` 空，签名里是
//! [`PLATFORM_SCOPE_TENANT`] 且是平台员工）。写平台必须是 `core:hooks:platform:write`，写应用必须是
//! `core:hooks:write`，一个码开不了另一个范围。

use flare_grpc_proto::capability::{
    CreateHookConfigRequest, DeleteHookConfigRequest, GenericRequest, GenericResponse,
    GetHookConfigRequest, GetHookStatisticsRequest, ListHookConfigsRequest,
    QueryHookExecutionsRequest, SetHookStatusRequest, TestHookConfigRequest,
    UpdateHookConfigRequest,
};
use flare_im_core_service_kit::audit_effects::durable::StaffAudit;
use flare_im_core_service_kit::staff_context::VerifiedStaff;
use flare_server_core::auth::core_admin::PLATFORM_SCOPE_TENANT;
use flare_server_core::auth::core_admin::permission::{HOOKS_PLATFORM_WRITE, HOOKS_WRITE};
use prost::Message;
use tonic::{Request, Status};

use crate::interface::grpc::hooks::HookServiceServer;

const HOOK_POINT_LIST: &str = "flare.extension.v1.hook_point.list";
const HOOK_PROFILE_LIST: &str = "flare.extension.v1.hook_profile.list";
const HOOK_CONFIG_TEST: &str = "flare.extension.v1.hook_config.test";

/// `Administer` 的 operation 名，按读写分组；`service.rs` 据此选权限码。
pub const READ_OPERATIONS: [&str; 6] = [
    "flare.extension.v1.hook_config.get",
    "flare.extension.v1.hook_config.list",
    "flare.extension.v1.hook_config.get_statistics",
    "flare.extension.v1.hook_config.query_executions",
    HOOK_POINT_LIST,
    HOOK_PROFILE_LIST,
];
pub const WRITE_OPERATIONS: [&str; 4] = [
    "flare.extension.v1.hook_config.create",
    "flare.extension.v1.hook_config.update",
    "flare.extension.v1.hook_config.delete",
    "flare.extension.v1.hook_config.set_status",
];
/// 只有平台写码能调：测试调用会向外发请求。
pub const PLATFORM_WRITE_OPERATIONS: [&str; 1] = [HOOK_CONFIG_TEST];

/// 这次操作动的是配置还是只读。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Access {
    Read,
    Write,
}

/// Retains the original verified context, including permission and signed request identity.
#[derive(Debug, Clone)]
pub struct HookAdministrator {
    staff: VerifiedStaff,
}

impl HookAdministrator {
    pub fn from_staff(staff: &VerifiedStaff) -> Self {
        Self {
            staff: staff.clone(),
        }
    }

    /// 租户范围的操作必须指名签名里的租户。
    pub fn require_tenant(&self, tenant_id: &str) -> Result<(), Status> {
        self.staff.require_tenant(tenant_id)
    }

    fn platform_scope(&self) -> bool {
        self.staff.tenant_id == PLATFORM_SCOPE_TENANT && self.staff.is_platform_staff()
    }

    fn signed_write_code(&self, code: &str) -> Result<(), Status> {
        if self.staff.permission.as_deref() == Some(code) {
            return Ok(());
        }
        Err(Status::permission_denied(
            "this hook scope needs another write permission",
        ))
    }

    fn require_platform(&self, access: Access) -> Result<(), Status> {
        if !self.platform_scope() {
            return Err(Status::permission_denied(
                "platform hooks are for platform staff only",
            ));
        }
        match access {
            Access::Read => Ok(()),
            Access::Write => self.signed_write_code(HOOKS_PLATFORM_WRITE),
        }
    }

    /// 范围是 `tenant_id`：空是平台，否则是这个应用。
    fn require_scope(&self, tenant_id: &str, access: Access) -> Result<(), Status> {
        if tenant_id.is_empty() {
            return self.require_platform(access);
        }
        self.require_tenant(tenant_id)?;
        match access {
            Access::Read => Ok(()),
            Access::Write => self.signed_write_code(HOOKS_WRITE),
        }
    }
}

fn pack_ok(
    request_id: String,
    response_type_url: &str,
    msg: &impl prost::Message,
) -> Result<tonic::Response<GenericResponse>, Status> {
    let any = prost_types::Any {
        type_url: response_type_url.to_string(),
        value: msg.encode_to_vec(),
    };
    Ok(tonic::Response::new(GenericResponse {
        ok: true,
        request_id,
        payload: Some(any),
        error_code: String::new(),
        error_message: String::new(),
    }))
}

/// 将 `flare.extension.v1.hook_config.*` 治理操作映射到 `HookServiceServer` 应用服务。
pub async fn dispatch_hook_administer(
    governance: &HookServiceServer,
    outer: GenericRequest,
    actor: &HookAdministrator,
) -> Result<tonic::Response<GenericResponse>, Status> {
    dispatch_verified(governance, outer, actor)
        .staff_audit(&actor.staff.ctx, "flare-capability")
        .await
}

async fn dispatch_verified(
    governance: &HookServiceServer,
    outer: GenericRequest,
    actor: &HookAdministrator,
) -> Result<tonic::Response<GenericResponse>, Status> {
    let operation = outer.operation.clone();
    let request_id = actor.staff.ctx.request_id().to_owned();
    let payload = outer
        .payload
        .ok_or_else(|| Status::invalid_argument("payload required"))?;

    if operation == "flare.extension.v1.hook_config.create" {
        let inner = CreateHookConfigRequest::decode(payload.value.as_slice())
            .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
        actor.require_scope(&inner.tenant_id, Access::Write)?;
        let rsp = governance
            .grpc_create_hook_config(trusted_request(inner, actor, &request_id))
            .await?
            .into_inner();
        return pack_ok(
            request_id,
            "type.googleapis.com/flare.capability.v1.CreateHookConfigResponse",
            &rsp,
        );
    }
    if operation == "flare.extension.v1.hook_config.get" {
        let inner = GetHookConfigRequest::decode(payload.value.as_slice())
            .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
        require_hook_scope(governance, &inner.hook_id, actor, &request_id, Access::Read).await?;
        let rsp = governance
            .grpc_get_hook_config(trusted_request(inner, actor, &request_id))
            .await?
            .into_inner();
        return pack_ok(
            request_id,
            "type.googleapis.com/flare.capability.v1.GetHookConfigResponse",
            &rsp,
        );
    }
    if operation == "flare.extension.v1.hook_config.update" {
        let inner = UpdateHookConfigRequest::decode(payload.value.as_slice())
            .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
        require_hook_scope(
            governance,
            &inner.hook_id,
            actor,
            &request_id,
            Access::Write,
        )
        .await?;
        let rsp = governance
            .grpc_update_hook_config(trusted_request(inner, actor, &request_id))
            .await?
            .into_inner();
        return pack_ok(
            request_id,
            "type.googleapis.com/flare.capability.v1.UpdateHookConfigResponse",
            &rsp,
        );
    }
    if operation == "flare.extension.v1.hook_config.list" {
        let inner = ListHookConfigsRequest::decode(payload.value.as_slice())
            .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
        actor.require_scope(&inner.tenant_id, Access::Read)?;
        let rsp = governance
            .grpc_list_hook_configs(trusted_request(inner, actor, &request_id))
            .await?
            .into_inner();
        return pack_ok(
            request_id,
            "type.googleapis.com/flare.capability.v1.ListHookConfigsResponse",
            &rsp,
        );
    }
    if operation == "flare.extension.v1.hook_config.delete" {
        let inner = DeleteHookConfigRequest::decode(payload.value.as_slice())
            .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
        require_hook_scope(
            governance,
            &inner.hook_id,
            actor,
            &request_id,
            Access::Write,
        )
        .await?;
        let rsp = governance
            .grpc_delete_hook_config(trusted_request(inner, actor, &request_id))
            .await?
            .into_inner();
        return pack_ok(
            request_id,
            "type.googleapis.com/flare.capability.v1.DeleteHookConfigResponse",
            &rsp,
        );
    }
    if operation == "flare.extension.v1.hook_config.set_status" {
        let inner = SetHookStatusRequest::decode(payload.value.as_slice())
            .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
        require_hook_scope(
            governance,
            &inner.hook_id,
            actor,
            &request_id,
            Access::Write,
        )
        .await?;
        let rsp = governance
            .grpc_set_hook_status(trusted_request(inner, actor, &request_id))
            .await?
            .into_inner();
        return pack_ok(
            request_id,
            "type.googleapis.com/flare.capability.v1.SetHookStatusResponse",
            &rsp,
        );
    }
    if operation == "flare.extension.v1.hook_config.get_statistics" {
        let inner = GetHookStatisticsRequest::decode(payload.value.as_slice())
            .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
        require_evidence_scope(governance, &inner.hook_id, actor, &request_id).await?;
        let rsp = governance
            .grpc_get_hook_statistics(trusted_request(inner, actor, &request_id))
            .await?
            .into_inner();
        return pack_ok(
            request_id,
            "type.googleapis.com/flare.capability.v1.GetHookStatisticsResponse",
            &rsp,
        );
    }
    if operation == "flare.extension.v1.hook_config.query_executions" {
        let inner = QueryHookExecutionsRequest::decode(payload.value.as_slice())
            .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
        require_evidence_scope(governance, &inner.hook_id, actor, &request_id).await?;
        let rsp = governance
            .grpc_query_hook_executions(trusted_request(inner, actor, &request_id))
            .await?
            .into_inner();
        return pack_ok(
            request_id,
            "type.googleapis.com/flare.capability.v1.QueryHookExecutionsResponse",
            &rsp,
        );
    }

    if operation == HOOK_POINT_LIST {
        return pack_ok(
            request_id,
            "type.googleapis.com/flare.capability.v1.ListHookPointsResponse",
            &governance.hook_points(),
        );
    }
    if operation == HOOK_PROFILE_LIST {
        // 部署 Hook 的目标是内网地址，不给应用看。
        actor.require_platform(Access::Read)?;
        let rsp = governance.hook_profiles().await?;
        return pack_ok(
            request_id,
            "type.googleapis.com/flare.capability.v1.ListHookProfilesResponse",
            &rsp,
        );
    }
    if operation == HOOK_CONFIG_TEST {
        let inner = TestHookConfigRequest::decode(payload.value.as_slice())
            .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
        actor.require_platform(Access::Write)?;
        if !inner.hook_id.is_empty() {
            // 平台的写码只能测平台 Hook。
            require_hook_scope(
                governance,
                &inner.hook_id,
                actor,
                &request_id,
                Access::Write,
            )
            .await?;
        }
        let rsp = governance.test_hook_config(inner, &request_id).await?;
        return pack_ok(
            request_id,
            "type.googleapis.com/flare.capability.v1.TestHookConfigResponse",
            &rsp,
        );
    }

    Err(Status::unimplemented(format!(
        "unknown administer operation: {operation}"
    )))
}

fn trusted_request<T>(payload: T, actor: &HookAdministrator, _request_id: &str) -> Request<T> {
    // Preserve the verified token/claims; never rebuild an unsigned Staff from payload IDs.
    let mut request = Request::new(payload);
    request.extensions_mut().insert(actor.staff.ctx.clone());
    request
}

/// 统计与执行记录：平台范围的调用方也能查部署 Hook，它们没有配置行，由治理服务按名字核对。
async fn require_evidence_scope(
    governance: &HookServiceServer,
    hook_id: &str,
    actor: &HookAdministrator,
    request_id: &str,
) -> Result<(), Status> {
    if actor.platform_scope() {
        return Ok(());
    }
    require_hook_scope(governance, hook_id, actor, request_id, Access::Read).await
}

async fn require_hook_scope(
    governance: &HookServiceServer,
    hook_id: &str,
    actor: &HookAdministrator,
    request_id: &str,
    access: Access,
) -> Result<(), Status> {
    if hook_id.trim().is_empty() {
        return Err(Status::invalid_argument(
            "hook_id required for tenant-scoped operation",
        ));
    }
    let config = governance
        .grpc_get_hook_config(trusted_request(
            GetHookConfigRequest {
                hook_id: hook_id.to_owned(),
            },
            actor,
            request_id,
        ))
        .await?
        .into_inner()
        .config
        .ok_or_else(|| Status::not_found("hook config not found"))?;
    actor.require_scope(&config.tenant_id, access)
}

#[cfg(test)]
pub(super) fn test_employee(actor: &str, tenant: &str, request_id: &str) -> HookAdministrator {
    test_staff(actor, tenant, request_id, HOOKS_WRITE, None)
}

/// 平台员工，签平台范围和平台写码。
#[cfg(test)]
pub(super) fn test_platform_employee(actor: &str, request_id: &str) -> HookAdministrator {
    test_staff(
        actor,
        PLATFORM_SCOPE_TENANT,
        request_id,
        HOOKS_PLATFORM_WRITE,
        None,
    )
}

#[cfg(test)]
fn test_staff(
    actor: &str,
    tenant: &str,
    request_id: &str,
    permission: &str,
    org: Option<&str>,
) -> HookAdministrator {
    use ed25519_dalek::{
        SigningKey,
        pkcs8::{EncodePrivateKey, EncodePublicKey},
    };
    use flare_server_core::{
        SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
    };
    let key = SigningKey::from_bytes(&[91; 32]);
    let private = key.to_pkcs8_pem(Default::default()).unwrap();
    let public = key
        .verifying_key()
        .to_public_key_pem(Default::default())
        .unwrap();
    let signer = SignedContextSigner::from_private_key(private.as_str())
        .unwrap()
        .with_kid("gateway")
        .unwrap();
    let verifier = SignedContextVerifier::from_public_key(&public)
        .unwrap()
        .require_staff_kid(["gateway"]);
    let token = signer
        .sign(
            &SignedContextIdentity::new(SignedActorType::Staff, actor, tenant, request_id)
                .with_staff_grant(org.map(str::to_owned), Some(permission.into()), 1),
        )
        .unwrap();
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert("x-flare-ctx", token.parse().unwrap());
    HookAdministrator::from_staff(
        &flare_im_core_service_kit::staff_context::verify_staff_metadata(
            &metadata, &verifier, permission,
        )
        .unwrap(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_server_core::utils::require_ctx_from_request;

    fn staff_with(tenant: &str, permission: &str, org: Option<&str>) -> HookAdministrator {
        test_staff("employee-7", tenant, "request-1", permission, org)
    }

    #[test]
    fn inner_hook_request_uses_verified_staff_identity_only() {
        let actor = test_employee("employee-7", "tenant-a", "request-1");
        let request = trusted_request((), &actor, "forged-payload-request");
        let ctx = require_ctx_from_request(&request).unwrap();
        assert_eq!(ctx.user_id(), Some("employee-7"));
        assert_eq!(ctx.tenant_id(), Some("tenant-a"));
        assert_eq!(ctx.request_id(), "request-1");
        assert_eq!(ctx.actor().unwrap().actor_id(), "employee-7");
        assert_eq!(
            ctx.actor().unwrap().actor_type,
            flare_server_core::context::ActorType::Staff
        );
        assert!(
            ctx.get_data::<flare_server_core::VerifiedContext>()
                .is_some()
        );
        assert!(actor.require_tenant("tenant-a").is_ok());
        assert_eq!(
            actor.require_tenant("tenant-b").unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn every_operation_is_either_a_read_or_a_write() {
        for read in READ_OPERATIONS {
            assert!(!WRITE_OPERATIONS.contains(&read), "{read}");
        }
        for write in PLATFORM_WRITE_OPERATIONS {
            assert!(!READ_OPERATIONS.contains(&write), "{write}");
            assert!(!WRITE_OPERATIONS.contains(&write), "{write}");
        }
        assert_eq!(
            READ_OPERATIONS.len() + WRITE_OPERATIONS.len() + PLATFORM_WRITE_OPERATIONS.len(),
            11
        );
    }

    /// 平台范围要平台员工、平台租户；写还要平台写码。应用的写码开不了平台，平台的写码开不了应用。
    #[test]
    fn each_write_code_opens_only_its_own_scope() {
        let platform = staff_with(PLATFORM_SCOPE_TENANT, HOOKS_PLATFORM_WRITE, None);
        assert!(platform.require_scope("", Access::Write).is_ok());
        assert!(platform.require_scope("", Access::Read).is_ok());
        assert_eq!(
            platform
                .require_scope("acme", Access::Write)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );

        let application = staff_with("acme", HOOKS_WRITE, None);
        assert!(application.require_scope("acme", Access::Write).is_ok());
        for access in [Access::Read, Access::Write] {
            assert_eq!(
                application.require_scope("", access).unwrap_err().code(),
                tonic::Code::PermissionDenied
            );
        }

        // 平台范围的写码配上应用租户，或组织员工，都不算平台。
        let misplaced = staff_with("acme", HOOKS_PLATFORM_WRITE, None);
        assert!(misplaced.require_scope("", Access::Write).is_err());
        assert!(misplaced.require_scope("acme", Access::Write).is_err());
        let organization = staff_with(PLATFORM_SCOPE_TENANT, HOOKS_PLATFORM_WRITE, Some("org-1"));
        assert!(organization.require_scope("", Access::Read).is_err());

        // 叫 platform 的应用用应用写码，写不了平台行。
        let named_platform = staff_with(PLATFORM_SCOPE_TENANT, HOOKS_WRITE, None);
        assert!(named_platform.require_scope("", Access::Write).is_err());
        assert!(
            named_platform
                .require_scope(PLATFORM_SCOPE_TENANT, Access::Write)
                .is_ok()
        );
    }
}
