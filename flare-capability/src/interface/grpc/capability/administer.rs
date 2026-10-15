//! `CapabilityService.Administer` — Hook 治理命令在控制面的统一入口（与 ExtensionPlugin 执行面分离）。
//!
//! 调用方是 admin-gateway 转发的 Staff `x-flare-ctx`：`service.rs` 先按 operation 复核权限码
//! （读 `core:hooks:read`、写 `core:hooks:write`），再把已验证的员工交到这里；租户只取自签名，
//! 按 hook_id 的操作先核对该配置属于签名里的租户。进程内的治理服务收到的上下文只由验过的
//! 声明重建，不看 `GenericRequest.metadata` 或载荷。

use flare_grpc_proto::capability::{
    CreateHookConfigRequest, DeleteHookConfigRequest, GenericRequest, GenericResponse,
    GetHookConfigRequest, GetHookStatisticsRequest, ListHookConfigsRequest,
    QueryHookExecutionsRequest, SetHookStatusRequest, UpdateHookConfigRequest,
};
use flare_im_service_kit::audit_effects::durable::StaffAudit;
use flare_im_service_kit::staff_context::VerifiedStaff;
use prost::Message;
use tonic::{Request, Status};

use crate::interface::grpc::hooks::HookServiceServer;

/// `Administer` 的 operation 名，按读写分组；`service.rs` 据此选权限码。
pub const READ_OPERATIONS: [&str; 4] = [
    "flare.extension.v1.hook_config.get",
    "flare.extension.v1.hook_config.list",
    "flare.extension.v1.hook_config.get_statistics",
    "flare.extension.v1.hook_config.query_executions",
];
pub const WRITE_OPERATIONS: [&str; 4] = [
    "flare.extension.v1.hook_config.create",
    "flare.extension.v1.hook_config.update",
    "flare.extension.v1.hook_config.delete",
    "flare.extension.v1.hook_config.set_status",
];

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
        actor.require_tenant(&inner.tenant_id)?;
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
        require_hook_tenant(governance, &inner.hook_id, actor, &request_id).await?;
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
        require_hook_tenant(governance, &inner.hook_id, actor, &request_id).await?;
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
        actor.require_tenant(&inner.tenant_id)?;
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
        require_hook_tenant(governance, &inner.hook_id, actor, &request_id).await?;
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
        require_hook_tenant(governance, &inner.hook_id, actor, &request_id).await?;
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
        require_hook_tenant(governance, &inner.hook_id, actor, &request_id).await?;
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
        require_hook_tenant(governance, &inner.hook_id, actor, &request_id).await?;
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

async fn require_hook_tenant(
    governance: &HookServiceServer,
    hook_id: &str,
    actor: &HookAdministrator,
    request_id: &str,
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
    actor.require_tenant(&config.tenant_id)
}

#[cfg(test)]
pub(super) fn test_employee(actor: &str, tenant: &str, request_id: &str) -> HookAdministrator {
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
                .with_staff_grant(None, Some("core:hooks:write".into()), 1),
        )
        .unwrap();
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert("x-flare-ctx", token.parse().unwrap());
    HookAdministrator::from_staff(
        &flare_im_service_kit::staff_context::verify_staff_metadata(
            &metadata,
            &verifier,
            "core:hooks:write",
        )
        .unwrap(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_server_core::utils::require_ctx_from_request;

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
        assert_eq!(READ_OPERATIONS.len() + WRITE_OPERATIONS.len(), 8);
    }
}
