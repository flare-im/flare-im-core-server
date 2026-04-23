//! `ExtensionPlugin` — SFU 扩展面：`flare-strom-sfu` 已连接时转发 `SfuControl`；否则仅暴露路由查询。
//!
//! 约定 `operation`：
//! - `flare.sfu.v1.list_plugin_routes` — 查询 [`PluginRouteBook`]（payload 或 metadata 带 `tenant_id`）
//! - `flare.sfu.v1.health_check` — `SfuControl.HealthCheck`（需已连接 strom）
//! - `flare.sfu.v1.get_room_summary` — payload 为 `GetRoomSummaryRequest`（需已连接 strom）

use flare_grpc_proto::capability::extension_plugin_server::ExtensionPlugin;
use flare_grpc_proto::capability::{
    GenericRequest, GenericResponse, ListRegisteredPluginsRequest, ListRegisteredPluginsResponse,
};
use flare_grpc_proto::sfu_control::sfu_control_client::SfuControlClient;
use flare_grpc_proto::sfu_control::{GetRoomSummaryRequest, HealthCheckRequest};
use flare_server_core::client::set_context_metadata;
use flare_server_core::context::Context;
use prost::Message;
use std::sync::Arc;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::infrastructure::capability::{
    PluginRouteBook, StromSfuGrpcRtcCapability, STROM_CAPABILITY_CONTROL, STROM_PLUGIN_ID,
};

fn context_from_generic(outer: &GenericRequest) -> Context {
    let mut ctx = Context::with_request_id(outer.request_id.as_str());
    if let Some(tenant) = outer.metadata.get("tenant_id").filter(|s| !s.is_empty()) {
        ctx = ctx.with_tenant_id(tenant.as_str());
    }
    if let Some(uid) = outer.metadata.get("user_id").filter(|s| !s.is_empty()) {
        ctx = ctx.with_user_id(uid.as_str());
    }
    if let Some(trace) = outer.metadata.get("trace_id").filter(|s| !s.is_empty()) {
        ctx = ctx.with_trace_id(trace.as_str());
    }
    ctx
}

fn pack_any<M: prost::Message>(
    request_id: String,
    ok: bool,
    type_url: &str,
    msg: &M,
    error_code: &str,
    error_message: &str,
) -> Result<Response<GenericResponse>, Status> {
    let any = prost_types::Any {
        type_url: type_url.to_string(),
        value: msg.encode_to_vec(),
    };
    Ok(Response::new(GenericResponse {
        ok,
        request_id,
        payload: Some(any),
        error_code: error_code.to_string(),
        error_message: error_message.to_string(),
    }))
}

/// SFU / 媒体 `ExtensionPlugin`：strom 连接可选，路由簿始终可用。
#[derive(Clone)]
pub struct StromSfuExtensionPluginServer {
    client: Option<SfuControlClient<Channel>>,
    routes: Arc<PluginRouteBook>,
}

impl StromSfuExtensionPluginServer {
    pub fn new(
        strom: Option<Arc<StromSfuGrpcRtcCapability>>,
        routes: Arc<PluginRouteBook>,
    ) -> Self {
        Self {
            client: strom.map(|s| s.control_client()),
            routes,
        }
    }

    pub fn plugin_id() -> &'static str {
        STROM_PLUGIN_ID
    }

    pub fn capability_id() -> &'static str {
        STROM_CAPABILITY_CONTROL
    }

    fn require_sfu_client(&self) -> Result<SfuControlClient<Channel>, Status> {
        self.client.clone().ok_or_else(|| {
            Status::failed_precondition(
                "flare-strom-sfu gRPC client not configured; set FLARE_CAPABILITY_RTC_BACKEND=strom and FLARE_STROM_SFU_GRPC_ENDPOINT",
            )
        })
    }
}

#[tonic::async_trait]
impl ExtensionPlugin for StromSfuExtensionPluginServer {
    async fn call(
        &self,
        request: Request<GenericRequest>,
    ) -> Result<Response<GenericResponse>, Status> {
        let outer = request.into_inner();
        let operation = outer.operation.as_str();
        let request_id = outer.request_id.clone();
        let ctx = context_from_generic(&outer);

        match operation {
            "flare.sfu.v1.list_plugin_routes" => {
                let mut inner = outer
                    .payload
                    .as_ref()
                    .map(|p| {
                        ListRegisteredPluginsRequest::decode(p.value.as_slice()).map_err(|e| {
                            Status::invalid_argument(format!("decode ListRegisteredPluginsRequest: {e}"))
                        })
                    })
                    .transpose()?
                    .unwrap_or_else(|| ListRegisteredPluginsRequest {
                        tenant_id: String::new(),
                        capability_id: String::new(),
                    });

                if inner.tenant_id.is_empty() {
                    inner.tenant_id = outer
                        .metadata
                        .get("tenant_id")
                        .cloned()
                        .unwrap_or_default();
                }
                if inner.tenant_id.is_empty() {
                    return Err(Status::invalid_argument(
                        "tenant_id required in payload or metadata",
                    ));
                }
                let instances = self.routes.list_filtered(
                    inner.tenant_id.as_str(),
                    inner.capability_id.as_str(),
                );
                let rsp = ListRegisteredPluginsResponse { instances };
                return pack_any(
                    request_id,
                    true,
                    "type.googleapis.com/flare.capability.v1.ListRegisteredPluginsResponse",
                    &rsp,
                    "",
                    "",
                );
            }
            "flare.sfu.v1.health_check" => {
                let mut client = self.require_sfu_client()?;
                let mut grpc_req = Request::new(HealthCheckRequest {});
                set_context_metadata(&mut grpc_req, &ctx);
                let rsp = client
                    .health_check(grpc_req)
                    .await
                    .map_err(Status::from)?
                    .into_inner();
                return pack_any(
                    request_id,
                    true,
                    "type.googleapis.com/flare.sfu.control.v1.HealthCheckResponse",
                    &rsp,
                    "",
                    "",
                );
            }
            "flare.sfu.v1.get_room_summary" => {
                let mut client = self.require_sfu_client()?;
                let payload = outer
                    .payload
                    .ok_or_else(|| Status::invalid_argument("payload required"))?;
                let inner = GetRoomSummaryRequest::decode(payload.value.as_slice())
                    .map_err(|e| Status::invalid_argument(format!("decode GetRoomSummaryRequest: {e}")))?;
                let mut grpc_req = Request::new(inner);
                set_context_metadata(&mut grpc_req, &ctx);
                let rsp = client
                    .get_room_summary(grpc_req)
                    .await
                    .map_err(Status::from)?
                    .into_inner();
                return pack_any(
                    request_id,
                    true,
                    "type.googleapis.com/flare.sfu.control.v1.GetRoomSummaryResponse",
                    &rsp,
                    "",
                    "",
                );
            }
            _ => {}
        }

        Err(Status::unimplemented(format!(
            "unknown ExtensionPlugin operation: {operation}; supported: flare.sfu.v1.health_check | get_room_summary | list_plugin_routes (plugin={})",
            Self::plugin_id()
        )))
    }
}
