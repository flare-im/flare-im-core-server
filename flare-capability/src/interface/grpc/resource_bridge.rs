//! The plugin host bridge listener: plugins read resources and publish entity state here.
use crate::application::handler::resource_bridge::{BridgeCredential, ResourceBridge};
use crate::domain::capability::CapabilityError;
use flare_grpc_proto::capability::{
    GenericRequest, GenericResponse, plugin_host_bridge_server::PluginHostBridge,
};
use flare_im_capability_core::plugin::invocation::{PLUGIN_CALL_HEADER, PLUGIN_RUNTIME_HEADER};
use flare_im_service_kit::plugin_platform::{PLUGIN_CARD_UNBOUND, PLUGIN_MESSAGE_RECALLED};
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// A bridge failure as the plugin sees it. Refusals say nothing beyond "denied", except that an
/// entity state publish names an ended entity: its card was recalled (`PLUGIN_MESSAGE_RECALLED`)
/// or it has no card (`PLUGIN_CARD_UNBOUND`). Those answer `FAILED_PRECONDITION` with the code as
/// the message and the `ErrorDetail` reason, so the plugin stops publishing that entity instead of
/// retrying it.
pub(crate) fn bridge_status(error: CapabilityError) -> Status {
    match error {
        CapabilityError::Plugin { ref code, .. }
            if code == PLUGIN_MESSAGE_RECALLED || code == PLUGIN_CARD_UNBOUND =>
        {
            let code = code.clone();
            flare_server_core::error::grpc::build_status(
                tonic::Code::FailedPrecondition,
                flare_proto::common::ErrorDetail {
                    code: flare_server_core::error::ErrorCode::PermissionDenied.as_u32() as i32,
                    reason: code,
                    message: String::new(),
                    track: String::new(),
                },
            )
        }
        CapabilityError::Timeout(_) => Status::deadline_exceeded("resource deadline exceeded"),
        CapabilityError::System(_) => Status::unavailable("resource bridge unavailable"),
        _ => Status::permission_denied("plugin resource access denied"),
    }
}

pub struct PluginHostBridgeGrpc(pub Arc<ResourceBridge>);
#[tonic::async_trait]
impl PluginHostBridge for PluginHostBridgeGrpc {
    async fn call(
        &self,
        request: Request<GenericRequest>,
    ) -> Result<Response<GenericResponse>, Status> {
        // Exactly one credential: a call ticket (user or staff) or the plugin runtime credential.
        let one = |name: &str| -> Result<Option<&str>, Status> {
            let mut values = request.metadata().get_all(name).iter();
            let Some(first) = values.next() else {
                return Ok(None);
            };
            if values.next().is_some() {
                return Err(Status::unauthenticated(
                    "one plugin invocation ticket required",
                ));
            }
            first
                .to_str()
                .map(Some)
                .map_err(|_| Status::unauthenticated("invalid plugin invocation ticket"))
        };
        let credential = match (one(PLUGIN_CALL_HEADER)?, one(PLUGIN_RUNTIME_HEADER)?) {
            (Some(ticket), None) => BridgeCredential::Call(ticket),
            (None, Some(runtime)) => BridgeCredential::Runtime(runtime),
            (None, None) => {
                return Err(Status::unauthenticated("plugin invocation ticket required"));
            }
            (Some(_), Some(_)) => {
                return Err(Status::unauthenticated(
                    "one plugin invocation ticket required",
                ));
            }
        };
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
            .call_with(credential, &body.operation, &body.request_id, bytes)
            .await
            .map_err(bridge_status)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    /// An ended entity is the one refusal a plugin hears as such; every other denial stays
    /// "denied", whatever its reason.
    #[test]
    fn only_an_ended_entity_is_told_apart_from_a_denial() {
        for code in [PLUGIN_MESSAGE_RECALLED, PLUGIN_CARD_UNBOUND] {
            let status = bridge_status(CapabilityError::plugin(code, "why"));
            assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{code}");
            assert_eq!(status.message(), code);
            let detail = flare_server_core::error::grpc::decode_error_detail(&status).unwrap();
            assert_eq!(detail.reason, code);
        }
        for denied in [
            CapabilityError::plugin("PLUGIN_DISABLED", "off"),
            CapabilityError::PolicyDenied(
                "plugin shared state rejected: card in another conversation".into(),
            ),
        ] {
            let status = bridge_status(denied);
            assert_eq!(status.code(), tonic::Code::PermissionDenied);
            assert_eq!(status.message(), "plugin resource access denied");
        }
        assert_eq!(
            bridge_status(CapabilityError::Timeout("slow".into())).code(),
            tonic::Code::DeadlineExceeded
        );
        assert_eq!(
            bridge_status(CapabilityError::System("down".into())).code(),
            tonic::Code::Unavailable
        );
    }
}
