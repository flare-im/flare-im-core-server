//! `ExtensionOperationHandler` 实现：认领 `flare.media.v1.*` operation。
//!
//! 上下文是路由器交来的已验签上下文（见 `ExtensionPluginRouter`）：租户只取自签名。
//! `list_plugin_routes` 只列签名租户的插件；请求体里的 `tenant_id` 必须为空或等于它。
//! 对 SFU 控制面（`HealthCheck` / `GetRoomSummary`）只按已验证的上下文写身份元数据，不转发
//! 签名令牌：SFU 控制面不验 `x-flare-ctx`，转过去只会多一份可在 60 秒内重放的用户令牌。

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
        media
            .control_client()
            .await
            .map_err(|e| Status::unavailable(format!("media control gRPC unavailable: {e}")))
    }
}

/// `list_plugin_routes` 的租户：签名上下文里的租户；请求体里的 `tenant_id` 为空或与之相同时才放行，
/// 指名别的租户（含 `*`）回 PERMISSION_DENIED。上下文没有具体租户时同样拒绝（路由器已保证有）。
fn scoped_route_tenant(ctx_tenant: Option<&str>, requested: &str) -> Result<String, Status> {
    let tenant = ctx_tenant
        .filter(|tenant| !tenant.is_empty() && *tenant != "*")
        .ok_or_else(|| Status::permission_denied("a concrete signed tenant is required"))?;
    if !requested.is_empty() && requested != tenant {
        return Err(Status::permission_denied(
            "plugin routes can only be listed for the signed tenant",
        ));
    }
    Ok(tenant.to_string())
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
                let inner = payload
                    .as_ref()
                    .map(|p| {
                        ListRegisteredPluginsRequest::decode(p.value.as_slice()).map_err(|e| {
                            Status::invalid_argument(format!(
                                "decode ListRegisteredPluginsRequest: {e}"
                            ))
                        })
                    })
                    .transpose()?
                    .unwrap_or_default();
                let tenant_id = scoped_route_tenant(ctx.tenant_id(), &inner.tenant_id)?;
                let instances = self
                    .routes
                    .list_filtered(tenant_id.as_str(), inner.capability_id.as_str());
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
    use super::{MediaControlExtensionOperations, MediaControlOperation, scoped_route_tenant};
    use crate::domain::capability::ExtensionOperationHandler;
    use crate::infrastructure::capability::PluginRouteBook;
    use crate::interface::grpc::hooks::test_support::{cell_keys, signed_ctx};
    use flare_grpc_proto::capability::{
        ListRegisteredPluginsRequest, ListRegisteredPluginsResponse, RegisteredPluginInstance,
    };
    use flare_server_core::SignedActorType;
    use prost::Message;
    use std::sync::Arc;
    use tonic::Code;

    #[test]
    fn route_listing_is_scoped_to_the_signed_tenant() {
        assert_eq!(scoped_route_tenant(Some("t1"), "").unwrap(), "t1");
        assert_eq!(scoped_route_tenant(Some("t1"), "t1").unwrap(), "t1");
        for (ctx, requested) in [
            (Some("t1"), "t2"),
            (Some("t1"), "*"),
            (Some("*"), ""),
            (Some(""), ""),
            (None, "t1"),
        ] {
            assert_eq!(
                scoped_route_tenant(ctx, requested).unwrap_err().code(),
                Code::PermissionDenied,
                "{ctx:?} {requested}"
            );
        }
    }

    #[tokio::test]
    async fn list_plugin_routes_lists_only_the_signed_tenant() {
        let routes = Arc::new(PluginRouteBook::new());
        for (tenant, plugin) in [("t1", "p1"), ("t2", "p2")] {
            routes.upsert(
                tenant,
                RegisteredPluginInstance {
                    plugin_id: plugin.into(),
                    capability_id: "rtc.call".into(),
                    ..Default::default()
                },
            );
        }
        let ops = MediaControlExtensionOperations::new(None, Arc::clone(&routes));
        let keys = cell_keys(77);
        let ctx = signed_ctx(&keys, SignedActorType::Service, "plugin-host", "t1");
        let list = |tenant: &str| {
            Some(prost_types::Any {
                type_url: String::new(),
                value: ListRegisteredPluginsRequest {
                    tenant_id: tenant.into(),
                    capability_id: String::new(),
                }
                .encode_to_vec(),
            })
        };
        for payload in [None, list(""), list("t1")] {
            let any = ops
                .call(&ctx, "flare.media.v1.list_plugin_routes", payload)
                .await
                .unwrap();
            let listed = ListRegisteredPluginsResponse::decode(any.value.as_slice()).unwrap();
            let ids: Vec<_> = listed
                .instances
                .iter()
                .map(|i| i.plugin_id.as_str())
                .collect();
            assert_eq!(ids, ["p1"]);
        }
        let status = ops
            .call(&ctx, "flare.media.v1.list_plugin_routes", list("t2"))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::PermissionDenied);
    }

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
