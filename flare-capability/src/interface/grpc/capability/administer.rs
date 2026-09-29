//! `CapabilityService.Administer` — Hook 治理命令在控制面的统一入口（与 ExtensionPlugin 执行面分离）。

use flare_grpc_proto::capability::{
    CreateHookConfigRequest, DeleteHookConfigRequest, GenericRequest, GenericResponse,
    GetHookConfigRequest, GetHookStatisticsRequest, ListHookConfigsRequest,
    QueryHookExecutionsRequest, SetHookStatusRequest, UpdateHookConfigRequest,
};
use flare_im_service_kit::control_auth::VerifiedControlActor;
use flare_server_core::context::{ActorContext, ActorType, Context, Ctx};
use prost::Message;
use std::sync::Arc;
use tonic::{Request, Status};

use crate::interface::grpc::hooks::HookServiceServer;

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
    actor: &VerifiedControlActor,
) -> Result<tonic::Response<GenericResponse>, Status> {
    let operation = outer.operation.clone();
    let request_id = outer.request_id.clone();
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

fn trusted_request<T>(payload: T, actor: &VerifiedControlActor, request_id: &str) -> Request<T> {
    // The inner governance service is called in-process, not through the gRPC
    // transport. Reconstruct its context only from the already verified
    // Console assertion, never from GenericRequest.metadata or its payload.
    let request_id = if request_id.len() <= 128
        && request_id.is_ascii()
        && !request_id.chars().any(char::is_control)
        && request_id.trim() == request_id
    {
        request_id
    } else {
        ""
    };
    let ctx: Ctx = Arc::new(
        Context::with_request_id(request_id)
            .with_tenant_id(&actor.tenant_id)
            .with_user_id(&actor.subject)
            .with_actor(ActorContext::new(&actor.subject).with_type(ActorType::TenantAdmin)),
    );
    let mut request = Request::new(payload);
    request.extensions_mut().insert(ctx);
    request
}

async fn require_hook_tenant(
    governance: &HookServiceServer,
    hook_id: &str,
    actor: &VerifiedControlActor,
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
mod tests {
    use super::*;
    use flare_im_service_kit::control_auth::ControlActorKind;
    use flare_server_core::utils::require_ctx_from_request;

    #[test]
    fn inner_hook_request_uses_verified_employee_identity_only() {
        let actor = VerifiedControlActor {
            subject: "employee-7".into(),
            tenant_id: "tenant-a".into(),
            kind: ControlActorKind::Employee,
        };
        let request = trusted_request((), &actor, "request-1");
        let ctx = require_ctx_from_request(&request).unwrap();
        assert_eq!(ctx.user_id(), Some("employee-7"));
        assert_eq!(ctx.tenant_id(), Some("tenant-a"));
        assert_eq!(ctx.request_id(), "request-1");
        assert_eq!(ctx.actor().unwrap().actor_id(), "employee-7");
        assert_eq!(ctx.actor().unwrap().actor_type, ActorType::TenantAdmin);
    }
}
