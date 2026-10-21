use crate::application::handler::resource_bridge::ResourceBridge;
use crate::domain::capability::CapabilityError;
use flare_grpc_proto::capability::{
    GenericRequest, GenericResponse, plugin_host_bridge_server::PluginHostBridge,
};
use flare_im_capability_core::plugin::invocation::PLUGIN_CALL_HEADER;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct PluginHostBridgeGrpc(pub Arc<ResourceBridge>);
#[tonic::async_trait]
impl PluginHostBridge for PluginHostBridgeGrpc {
    async fn call(
        &self,
        request: Request<GenericRequest>,
    ) -> Result<Response<GenericResponse>, Status> {
        let mut values = request.metadata().get_all(PLUGIN_CALL_HEADER).iter();
        let token = values
            .next()
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("plugin invocation ticket required"))?;
        if values.next().is_some() {
            return Err(Status::unauthenticated(
                "one plugin invocation ticket required",
            ));
        }
        let body = request.get_ref();
        // Caller-supplied legacy identity metadata has no role on this listener.
        if !body.metadata.is_empty() {
            return Err(Status::invalid_argument(
                "resource metadata is not accepted",
            ));
        }
        let bytes = body
            .payload
            .as_ref()
            .map(|v| v.value.as_slice())
            .unwrap_or(b"{}");
        let value = self
            .0
            .call(token, &body.operation, &body.request_id, bytes)
            .await
            .map_err(|error| match error {
                CapabilityError::Timeout(_) => {
                    Status::deadline_exceeded("resource deadline exceeded")
                }
                CapabilityError::System(_) => Status::unavailable("resource bridge unavailable"),
                _ => Status::permission_denied("plugin resource access denied"),
            })?;
        Ok(Response::new(GenericResponse {
            ok: true,
            request_id: body.request_id.clone(),
            payload: Some(prost_types::Any {
                type_url: "type.googleapis.com/flare.capability.v1.PayloadJson".into(),
                value: serde_json::to_vec(&value)
                    .map_err(|_| Status::internal("resource response encoding"))?,
            }),
            ..Default::default()
        }))
    }
}
