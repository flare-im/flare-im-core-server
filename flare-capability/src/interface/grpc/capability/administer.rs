//! `CapabilityService.Administer` — Hook 治理命令在控制面的统一入口（与 ExtensionPlugin 执行面分离）。

use flare_grpc_proto::capability::{
    CreateHookConfigRequest, DeleteHookConfigRequest, GenericRequest, GenericResponse,
    GetHookConfigRequest, GetHookStatisticsRequest, ListHookConfigsRequest,
    QueryHookExecutionsRequest, SetHookStatusRequest, UpdateHookConfigRequest,
};
use prost::Message;
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
) -> Result<tonic::Response<GenericResponse>, Status> {
    let operation = outer.operation.clone();
    let request_id = outer.request_id.clone();
    let payload = outer
        .payload
        .ok_or_else(|| Status::invalid_argument("payload required"))?;

    if operation == "flare.extension.v1.hook_config.create" {
        let inner = CreateHookConfigRequest::decode(payload.value.as_slice())
            .map_err(|e| Status::invalid_argument(format!("decode request: {e}")))?;
        let rsp = governance
            .grpc_create_hook_config(Request::new(inner))
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
        let rsp = governance
            .grpc_get_hook_config(Request::new(inner))
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
        let rsp = governance
            .grpc_update_hook_config(Request::new(inner))
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
        let rsp = governance
            .grpc_list_hook_configs(Request::new(inner))
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
        let rsp = governance
            .grpc_delete_hook_config(Request::new(inner))
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
        let rsp = governance
            .grpc_set_hook_status(Request::new(inner))
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
        let rsp = governance
            .grpc_get_hook_statistics(Request::new(inner))
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
        let rsp = governance
            .grpc_query_hook_executions(Request::new(inner))
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
