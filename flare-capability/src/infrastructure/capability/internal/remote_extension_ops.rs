//! `ExtensionOperationHandler` 实现：认领 `flare.media.v1.*` operation。

use std::sync::Arc;

use async_trait::async_trait;
use flare_core_base::context::Ctx;
use flare_grpc_proto::capability::{ListRegisteredPluginsRequest, ListRegisteredPluginsResponse};
use flare_grpc_proto::sfu_control::sfu_control_client::SfuControlClient;
use flare_grpc_proto::sfu_control::{GetRoomSummaryRequest, HealthCheckRequest};
use flare_server_core::client::set_context_metadata;
use prost::Message;
use prost_types::Any;
use tonic::transport::Channel;
use tonic::{Request, Status};

use crate::domain::capability::ExtensionOperationHandler;
use crate::infrastructure::capability::PluginRouteBook;

use super::remote_rtc_adapter::MediaControlGrpcRtcCapability;

const OP_PREFIX: &str = "flare.media.v1.";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MediaControlOperation {
    ListPluginRoutes,
    HealthCheck,
    GetRoomSummary,
}

impl MediaControlOperation {
    fn from_operation(operation: &str) -> Option<Self> {
        let suffix = operation.strip_prefix(OP_PREFIX)?;
        match suffix {
            "list_plugin_routes" => Some(Self::ListPluginRoutes),
            "health_check" => Some(Self::HealthCheck),
            "get_room_summary" => Some(Self::GetRoomSummary),
            _ => None,
        }
    }

    fn full_name(self) -> &'static str {
        match self {
            Self::ListPluginRoutes => "flare.media.v1.list_plugin_routes",
            Self::HealthCheck => "flare.media.v1.health_check",
            Self::GetRoomSummary => "flare.media.v1.get_room_summary",
        }
    }

    fn supported_full_names() -> Vec<&'static str> {
        vec![
            Self::ListPluginRoutes.full_name(),
            Self::HealthCheck.full_name(),
            Self::GetRoomSummary.full_name(),
        ]
    }
}

/// 认领 `flare.media.v1.*` operation 的扩展处理器。媒体后端连接可选；未连接时仅路由查询可用。
pub struct MediaControlExtensionOperations {
    media: Option<Arc<MediaControlGrpcRtcCapability>>,
    routes: Arc<PluginRouteBook>,
}

impl MediaControlExtensionOperations {
    pub fn new(
        media: Option<Arc<MediaControlGrpcRtcCapability>>,
        routes: Arc<PluginRouteBook>,
    ) -> Self {
        Self { media, routes }
    }

    async fn require_client(&self) -> Result<SfuControlClient<Channel>, Status> {
        let media = self.media.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "media control gRPC client not configured; wire(..) with a valid endpoint first",
            )
        })?;
        media.control_client().await.map_err(|e| {
            Status::unavailable(format!("media control gRPC unavailable: {e}"))
        })
    }

    fn tenant_id_from_ctx(ctx: &Ctx) -> String {
        ctx.tenant_id().map(|s| s.to_string()).unwrap_or_default()
    }
}

fn pack_any<M: Message>(type_url: &str, msg: &M) -> Any {
    Any {
        type_url: type_url.to_string(),
        value: msg.encode_to_vec(),
    }
}

#[async_trait]
impl ExtensionOperationHandler for MediaControlExtensionOperations {
    fn id(&self) -> &str {
        "media-control"
    }

    fn operation_prefixes(&self) -> &[&'static str] {
        &[OP_PREFIX]
    }

    async fn call(&self, ctx: &Ctx, operation: &str, payload: Option<Any>) -> Result<Any, Status> {
        let Some(op) = MediaControlOperation::from_operation(operation) else {
            let supported = MediaControlOperation::supported_full_names().join(" | ");
            return Err(Status::unimplemented(format!(
                "unknown flare.media.v1.* operation: {operation}; supported: {supported}"
            )));
        };

        match op {
            MediaControlOperation::ListPluginRoutes => {
                let mut inner = payload
                    .as_ref()
                    .map(|p| {
                        ListRegisteredPluginsRequest::decode(p.value.as_slice()).map_err(|e| {
                            Status::invalid_argument(format!(
                                "decode ListRegisteredPluginsRequest: {e}"
                            ))
                        })
                    })
                    .transpose()?
                    .unwrap_or_else(|| ListRegisteredPluginsRequest {
                        tenant_id: String::new(),
                        capability_id: String::new(),
                    });

                if inner.tenant_id.is_empty() {
                    inner.tenant_id = Self::tenant_id_from_ctx(ctx);
                }
                if inner.tenant_id.is_empty() {
                    return Err(Status::invalid_argument(
                        "tenant_id required in payload or metadata",
                    ));
                }
                let instances = self
                    .routes
                    .list_filtered(inner.tenant_id.as_str(), inner.capability_id.as_str());
                let rsp = ListRegisteredPluginsResponse { instances };
                Ok(pack_any(
                    "type.googleapis.com/flare.capability.v1.ListRegisteredPluginsResponse",
                    &rsp,
                ))
            }
            MediaControlOperation::HealthCheck => {
                let mut client = self.require_client().await?;
                let mut grpc_req = Request::new(HealthCheckRequest {});
                set_context_metadata(&mut grpc_req, ctx);
                let rsp = client.health_check(grpc_req).await?.into_inner();
                Ok(pack_any(
                    "type.googleapis.com/flare.sfu.control.v1.HealthCheckResponse",
                    &rsp,
                ))
            }
            MediaControlOperation::GetRoomSummary => {
                let mut client = self.require_client().await?;
                let payload =
                    payload.ok_or_else(|| Status::invalid_argument("payload required"))?;
                let inner =
                    GetRoomSummaryRequest::decode(payload.value.as_slice()).map_err(|e| {
                        Status::invalid_argument(format!("decode GetRoomSummaryRequest: {e}"))
                    })?;
                let mut grpc_req = Request::new(inner);
                set_context_metadata(&mut grpc_req, ctx);
                let rsp = client.get_room_summary(grpc_req).await?.into_inner();
                Ok(pack_any(
                    "type.googleapis.com/flare.sfu.control.v1.GetRoomSummaryResponse",
                    &rsp,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MediaControlOperation;

    #[test]
    fn parses_known_operation_suffixes() {
        assert_eq!(
            MediaControlOperation::from_operation("flare.media.v1.list_plugin_routes"),
            Some(MediaControlOperation::ListPluginRoutes)
        );
        assert_eq!(
            MediaControlOperation::from_operation("flare.media.v1.health_check"),
            Some(MediaControlOperation::HealthCheck)
        );
        assert_eq!(
            MediaControlOperation::from_operation("flare.media.v1.get_room_summary"),
            Some(MediaControlOperation::GetRoomSummary)
        );
        assert_eq!(
            MediaControlOperation::from_operation("flare.media.v1.unknown"),
            None
        );
    }

    #[test]
    fn supported_full_names_are_stable() {
        let ops = MediaControlOperation::supported_full_names();
        assert!(ops.contains(&"flare.media.v1.list_plugin_routes"));
        assert!(ops.contains(&"flare.media.v1.health_check"));
        assert!(ops.contains(&"flare.media.v1.get_room_summary"));
        assert_eq!(ops.len(), 3);
    }
}
