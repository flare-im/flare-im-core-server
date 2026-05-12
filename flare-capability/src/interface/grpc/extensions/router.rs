//! **通用 `ExtensionPlugin` 路由器**：按 `operation` 前缀把请求分发给注册的 [`ExtensionOperationHandler`]。
//!
//! 核心只持此路由器（gRPC service 实例）；具体 operation 语义由插件在启动时通过
//! [`crate::infrastructure::capability::CapabilityExtensionRegistry::register_extension_operations`]
//! 注入。新增媒体后端（LiveKit / Janus / 自研实现 / 其他）不改 core 一行。
//!
//! 路由策略：**最长前缀匹配**；未命中返回 [`tonic::Status::unimplemented`]。

use std::sync::Arc;

use flare_core_base::context::{Ctx, keys};
use flare_core_base::utils::map_to_ctx;
use flare_grpc_proto::capability::extension_plugin_server::ExtensionPlugin;
use flare_grpc_proto::capability::{GenericRequest, GenericResponse};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::domain::capability::DynExtensionOperationHandler;

#[derive(Clone, Default)]
pub struct ExtensionPluginRouter {
    handlers: Arc<RwLock<Vec<DynExtensionOperationHandler>>>,
}

impl ExtensionPluginRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, handler: DynExtensionOperationHandler) {
        let mut guard = self.handlers.write().await;
        // 相同 id 视为替换（便于热重载 / 重启注册）。
        if let Some(pos) = guard.iter().position(|h| h.id() == handler.id()) {
            guard[pos] = handler;
        } else {
            guard.push(handler);
        }
    }

    pub async fn handler_ids(&self) -> Vec<String> {
        self.handlers
            .read()
            .await
            .iter()
            .map(|h| h.id().to_string())
            .collect()
    }

    async fn resolve(&self, operation: &str) -> Option<DynExtensionOperationHandler> {
        let guard = self.handlers.read().await;
        let mut best: Option<(usize, DynExtensionOperationHandler)> = None;
        for h in guard.iter() {
            for prefix in h.operation_prefixes() {
                if operation.starts_with(prefix) {
                    let len = prefix.len();
                    if best.as_ref().map(|(n, _)| len > *n).unwrap_or(true) {
                        best = Some((len, h.clone()));
                    }
                }
            }
        }
        best.map(|(_, h)| h)
    }

    fn context_from_generic(outer: &GenericRequest) -> Ctx {
        let mut map = outer.metadata.clone();
        if !outer.request_id.is_empty() {
            map.insert(keys::REQUEST_ID.to_string(), outer.request_id.clone());
        }
        map_to_ctx(&map)
    }
}

#[tonic::async_trait]
impl ExtensionPlugin for ExtensionPluginRouter {
    async fn call(
        &self,
        request: Request<GenericRequest>,
    ) -> Result<Response<GenericResponse>, Status> {
        let outer = request.into_inner();
        let operation = outer.operation.clone();
        let request_id = outer.request_id.clone();
        let ctx = Self::context_from_generic(&outer);

        if operation == "flare.capability.v1.health_check" {
            return Ok(Response::new(GenericResponse {
                ok: true,
                request_id,
                payload: None,
                error_code: String::new(),
                error_message: String::new(),
            }));
        }

        let handler = self.resolve(operation.as_str()).await.ok_or_else(|| {
            Status::unimplemented(format!(
                "no ExtensionOperationHandler registered for operation: {operation}"
            ))
        })?;

        let any = handler
            .call(&ctx, operation.as_str(), outer.payload)
            .await?;
        Ok(Response::new(GenericResponse {
            ok: true,
            request_id,
            payload: Some(any),
            error_code: String::new(),
            error_message: String::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::ExtensionPluginRouter;
    use crate::domain::capability::{DynExtensionOperationHandler, ExtensionOperationHandler};
    use async_trait::async_trait;
    use flare_core_base::context::Ctx;
    use flare_grpc_proto::capability::GenericRequest;
    use flare_grpc_proto::capability::extension_plugin_server::ExtensionPlugin;
    use prost_types::Any;
    use std::sync::Arc;
    use tonic::{Request, Status};

    struct MockHandler {
        id: &'static str,
        prefixes: &'static [&'static str],
    }

    #[async_trait]
    impl ExtensionOperationHandler for MockHandler {
        fn id(&self) -> &str {
            self.id
        }

        fn operation_prefixes(&self) -> &[&'static str] {
            self.prefixes
        }

        async fn call(
            &self,
            _ctx: &Ctx,
            _operation: &str,
            _payload: Option<Any>,
        ) -> Result<Any, Status> {
            Ok(Any {
                type_url: format!("type.googleapis.com/{}", self.id),
                value: vec![],
            })
        }
    }

    #[tokio::test]
    async fn longest_prefix_handler_wins() {
        let router = ExtensionPluginRouter::new();
        let generic: DynExtensionOperationHandler = Arc::new(MockHandler {
            id: "generic",
            prefixes: &["flare.media."],
        });
        let specific: DynExtensionOperationHandler = Arc::new(MockHandler {
            id: "specific",
            prefixes: &["flare.media.v1."],
        });
        router.register(generic).await;
        router.register(specific).await;

        let req = GenericRequest {
            operation: "flare.media.v1.health_check".to_string(),
            request_id: "req-1".to_string(),
            payload: None,
            metadata: Default::default(),
        };
        let resp = router
            .call(Request::new(req))
            .await
            .expect("router call should succeed")
            .into_inner();
        let type_url = resp.payload.expect("payload required").type_url;
        assert_eq!(type_url, "type.googleapis.com/specific");
    }

    #[tokio::test]
    async fn commercial_plugin_handler_can_be_injected() {
        let router = ExtensionPluginRouter::new();
        let commercial: DynExtensionOperationHandler = Arc::new(MockHandler {
            id: "vendor.commercial.plugin",
            prefixes: &["vendor.commercial.v1."],
        });
        router.register(commercial).await;

        let req = GenericRequest {
            operation: "vendor.commercial.v1.policy_sync".to_string(),
            request_id: "req-2".to_string(),
            payload: None,
            metadata: Default::default(),
        };
        let resp = router
            .call(Request::new(req))
            .await
            .expect("commercial operation should be routed")
            .into_inner();
        let type_url = resp.payload.expect("payload required").type_url;
        assert_eq!(type_url, "type.googleapis.com/vendor.commercial.plugin");
    }
}
