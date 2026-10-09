//! **通用 `ExtensionPlugin` 路由器**：按 `operation` 前缀把请求分发给注册的 [`ExtensionOperationHandler`]。
//!
//! 核心只持此路由器（gRPC service 实例）；具体 operation 语义由插件在启动时通过
//! [`crate::infrastructure::capability::CapabilityExtensionRegistry::register_extension_operations`]
//! 注入。新增媒体后端（LiveKit / Janus / 自研实现 / 其他）不改 core 一行。
//!
//! 路由策略：**最长前缀匹配**；未命中返回 [`tonic::Status::unimplemented`]。
//!
//! # 身份
//!
//! 进程把路由器挂在 `SignedContextLayer::allow_anonymous()` 后面（[`extension_plugin_services`]）：
//! 层验过的 `x-flare-ctx` 是唯一身份，`GenericRequest.metadata` 与 `x-tenant-id` 等元数据一概不读。
//! 匿名准入（没有 `x-flare-ctx`）只能调 [`HEALTH_CHECK_OPERATION`]；其余 operation 要求签名的
//! `User`、`Service` 或 `TenantAdmin`，且租户是一个具体租户（非空、非 `*`）。`Staff` 不在本面：
//! 员工只经 admin-gateway。没有配置 Cell 公钥集时不挂层，路由器只回答健康检查。

use std::sync::Arc;

use flare_core_base::context::Ctx;
use flare_grpc_proto::capability::extension_plugin_server::{
    ExtensionPlugin, ExtensionPluginServer,
};
use flare_grpc_proto::capability::{GenericRequest, GenericResponse};
use flare_server_core::grpc::middleware::signed_context::{
    SignedContextLayer, SignedContextService, VerifiedContext,
};
use flare_server_core::{SignedActorType, SignedContextVerifier};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::domain::capability::DynExtensionOperationHandler;

/// 唯一允许匿名调用的 operation：插件宿主与运维探活用，不带任何身份、不读任何数据。
pub const HEALTH_CHECK_OPERATION: &str = "flare.capability.v1.health_check";

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
}

/// 非健康检查的 operation 的准入：层验过的签名上下文，原样交给 handler（租户、用户、actor、
/// 请求号与追踪号都来自签名）。没有上下文、匿名准入、或上下文不是验签得来的：UNAUTHENTICATED；
/// actor 不是 User / Service / TenantAdmin，或租户为空、`*`、带首尾空白：PERMISSION_DENIED。
pub(crate) fn admitted_context(verified: Option<&Ctx>) -> Result<Ctx, Status> {
    let ctx = verified.ok_or_else(|| Status::unauthenticated("signed context required"))?;
    let claims = ctx
        .get_data::<VerifiedContext>()
        .and_then(VerifiedContext::claims)
        .ok_or_else(|| Status::unauthenticated("signed context required"))?;
    if !matches!(
        claims.actor_type,
        SignedActorType::User | SignedActorType::Service | SignedActorType::TenantAdmin
    ) {
        return Err(Status::permission_denied(
            "this actor type cannot call ExtensionPlugin",
        ));
    }
    let tenant = claims.tenant_id.as_str();
    if tenant.is_empty() || tenant == "*" || tenant.trim() != tenant {
        return Err(Status::permission_denied(
            "a concrete tenant is required in the signed context",
        ));
    }
    Ok(Arc::clone(ctx))
}

/// The two ways the process can serve the router, exactly one of them `Some`: behind
/// `SignedContextLayer` admitting anonymous calls when a Cell key set is configured, or bare
/// (no key set: only the health check is answered, every other call is UNAUTHENTICATED; caller
/// metadata is never an identity). Both carry the same service name, so a server adds both
/// with `add_optional_service`.
#[allow(clippy::type_complexity)]
pub fn extension_plugin_services(
    router: ExtensionPluginRouter,
    verifier: Option<SignedContextVerifier>,
) -> (
    Option<SignedContextService<ExtensionPluginServer<ExtensionPluginRouter>>>,
    Option<ExtensionPluginServer<ExtensionPluginRouter>>,
) {
    let server = ExtensionPluginServer::new(router);
    match verifier {
        Some(verifier) => (
            Some(
                SignedContextLayer::new(verifier)
                    .allow_anonymous()
                    .layer(server),
            ),
            None,
        ),
        None => (None, Some(server)),
    }
}

#[tonic::async_trait]
impl ExtensionPlugin for ExtensionPluginRouter {
    async fn call(
        &self,
        request: Request<GenericRequest>,
    ) -> Result<Response<GenericResponse>, Status> {
        let verified = request.extensions().get::<Ctx>().cloned();
        let outer = request.into_inner();
        let operation = outer.operation;
        // 请求体里的 request_id 只作关联：原样回显；handler 的上下文用签名里的请求号。
        let request_id = outer.request_id;

        if operation == HEALTH_CHECK_OPERATION {
            return Ok(Response::new(GenericResponse {
                ok: true,
                request_id,
                payload: None,
                error_code: String::new(),
                error_message: String::new(),
            }));
        }

        let ctx = admitted_context(verified.as_ref())?;
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
            request_id: if request_id.is_empty() {
                ctx.request_id().to_string()
            } else {
                request_id
            },
            payload: Some(any),
            error_code: String::new(),
            error_message: String::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::capability::ExtensionOperationHandler;
    use crate::interface::grpc::hooks::test_support::{CellKeys, cell_keys, signed_ctx};
    use async_trait::async_trait;
    use flare_grpc_proto::capability::extension_plugin_client::ExtensionPluginClient;
    use flare_server_core::{SignedContextIdentity, SignedContextSigner};
    use prost_types::Any;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tonic::Code;
    use tonic::transport::Channel;

    /// The context one call received: tenant, user, actor and request id.
    type SeenCall = (Option<String>, Option<String>, String, String);

    /// Records the context each call received.
    #[derive(Default)]
    struct Seen(Mutex<Vec<SeenCall>>);

    struct MockHandler {
        id: &'static str,
        prefixes: &'static [&'static str],
        seen: Arc<Seen>,
    }

    impl MockHandler {
        fn new(id: &'static str, prefixes: &'static [&'static str]) -> Self {
            Self {
                id,
                prefixes,
                seen: Arc::default(),
            }
        }
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
            ctx: &Ctx,
            _operation: &str,
            _payload: Option<Any>,
        ) -> Result<Any, Status> {
            self.seen.0.lock().unwrap().push((
                ctx.tenant_id().map(str::to_owned),
                ctx.user_id().map(str::to_owned),
                ctx.actor()
                    .map(|actor| actor.actor_id().to_owned())
                    .unwrap_or_default(),
                ctx.request_id().to_owned(),
            ));
            Ok(Any {
                type_url: format!("type.googleapis.com/{}", self.id),
                value: vec![],
            })
        }
    }

    fn generic(operation: &str) -> GenericRequest {
        GenericRequest {
            operation: operation.to_string(),
            request_id: "req-body".to_string(),
            payload: None,
            metadata: Default::default(),
        }
    }

    fn signed_request(keys: &CellKeys, operation: &str) -> Request<GenericRequest> {
        let mut request = Request::new(generic(operation));
        request
            .extensions_mut()
            .insert(signed_ctx(keys, SignedActorType::User, "u1", "tenant-a"));
        request
    }

    #[tokio::test]
    async fn longest_prefix_handler_wins() {
        let keys = cell_keys(71);
        let router = ExtensionPluginRouter::new();
        let generic: DynExtensionOperationHandler =
            Arc::new(MockHandler::new("generic", &["flare.media."]));
        let specific: DynExtensionOperationHandler =
            Arc::new(MockHandler::new("specific", &["flare.media.v1."]));
        router.register(generic).await;
        router.register(specific).await;

        let resp = router
            .call(signed_request(&keys, "flare.media.v1.health_check"))
            .await
            .expect("router call should succeed")
            .into_inner();
        let type_url = resp.payload.expect("payload required").type_url;
        assert_eq!(type_url, "type.googleapis.com/specific");
        assert_eq!(resp.request_id, "req-body");
    }

    #[tokio::test]
    async fn commercial_plugin_handler_can_be_injected() {
        let keys = cell_keys(72);
        let router = ExtensionPluginRouter::new();
        let commercial: DynExtensionOperationHandler = Arc::new(MockHandler::new(
            "vendor.commercial.plugin",
            &["vendor.commercial.v1."],
        ));
        router.register(commercial).await;

        let resp = router
            .call(signed_request(&keys, "vendor.commercial.v1.policy_sync"))
            .await
            .expect("commercial operation should be routed")
            .into_inner();
        let type_url = resp.payload.expect("payload required").type_url;
        assert_eq!(type_url, "type.googleapis.com/vendor.commercial.plugin");
    }

    /// The router served the way the process serves it, on a loopback port.
    async fn serve(
        router: ExtensionPluginRouter,
        verifier: Option<SignedContextVerifier>,
    ) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (signed, bare) = extension_plugin_services(router, verifier);
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_optional_service(signed)
                .add_optional_service(bare)
                .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
        );
        format!("http://{address}")
    }

    async fn client(endpoint: &str) -> ExtensionPluginClient<Channel> {
        for _ in 0..50 {
            if let Ok(client) = ExtensionPluginClient::connect(endpoint.to_string()).await {
                return client;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("router endpoint {endpoint} never came up");
    }

    fn token(signer: &SignedContextSigner, actor: SignedActorType, tenant: &str) -> String {
        let identity = SignedContextIdentity::new(actor, "actor-1", tenant, "req-signed");
        let identity = if actor == SignedActorType::Staff {
            identity.with_staff_grant(None, Some("core:plugins:read".into()), 1)
        } else {
            identity
        };
        signer.sign(&identity).unwrap()
    }

    /// Caller-supplied identity metadata a client might try: the old metadata map in the body and
    /// the legacy headers. Neither is an identity any more.
    fn forged(operation: &str, ctx: Option<&str>) -> Request<GenericRequest> {
        let mut body = generic(operation);
        body.metadata = HashMap::from([
            ("x-tenant-id".to_string(), "tenant-a".to_string()),
            ("x-user-id".to_string(), "victim".to_string()),
            ("tenant_id".to_string(), "tenant-a".to_string()),
        ]);
        let mut request = Request::new(body);
        request
            .metadata_mut()
            .insert("x-tenant-id", "tenant-a".parse().unwrap());
        request
            .metadata_mut()
            .insert("x-user-id", "victim".parse().unwrap());
        if let Some(ctx) = ctx {
            request
                .metadata_mut()
                .insert("x-flare-ctx", ctx.parse().unwrap());
        }
        request
    }

    #[tokio::test]
    async fn anonymous_callers_get_only_the_health_check() {
        let keys = cell_keys(73);
        let router = ExtensionPluginRouter::new();
        let handler = Arc::new(MockHandler::new("media", &["flare.media.v1."]));
        let seen = Arc::clone(&handler.seen);
        router.register(handler).await;
        let mut client = client(&serve(router, Some(keys.verifier.clone())).await).await;

        let health = client
            .call(Request::new(generic(HEALTH_CHECK_OPERATION)))
            .await
            .unwrap()
            .into_inner();
        assert!(health.ok);
        assert_eq!(health.request_id, "req-body");

        // Anonymous, with only metadata naming a tenant and a user: UNAUTHENTICATED.
        let status = client
            .call(forged("flare.media.v1.list_plugin_routes", None))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::Unauthenticated, "{status:?}");
        // An invalid token is refused by the layer, even for the health check.
        let status = client
            .call(forged(HEALTH_CHECK_OPERATION, Some("not-a-token")))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::Unauthenticated, "{status:?}");
        assert!(seen.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_handler_context_comes_from_the_signed_claims_only() {
        let keys = cell_keys(74);
        let router = ExtensionPluginRouter::new();
        let handler = Arc::new(MockHandler::new("media", &["flare.media.v1."]));
        let seen = Arc::clone(&handler.seen);
        router.register(handler).await;
        let mut client = client(&serve(router, Some(keys.verifier.clone())).await).await;

        for actor in [
            SignedActorType::User,
            SignedActorType::Service,
            SignedActorType::TenantAdmin,
        ] {
            let ctx = token(&keys.signer, actor, "tenant-b");
            client
                .call(forged("flare.media.v1.get_room_summary", Some(&ctx)))
                .await
                .unwrap_or_else(|status| panic!("{actor:?}: {status:?}"));
        }
        let seen = seen.0.lock().unwrap().clone();
        assert_eq!(seen.len(), 3);
        for (tenant, user, actor, request_id) in &seen {
            // The forged x-tenant-id / x-user-id and body metadata named tenant-a and "victim".
            assert_eq!(tenant.as_deref(), Some("tenant-b"));
            assert_ne!(user.as_deref(), Some("victim"));
            assert_eq!(actor, "actor-1");
            assert_eq!(request_id, "req-signed");
        }
        // A Service actor is not a user.
        assert_eq!(seen[1].1, None);
    }

    #[tokio::test]
    async fn staff_system_guest_and_non_concrete_tenants_are_refused() {
        let keys = cell_keys(75);
        let router = ExtensionPluginRouter::new();
        let handler = Arc::new(MockHandler::new("media", &["flare.media.v1."]));
        let seen = Arc::clone(&handler.seen);
        router.register(handler).await;
        let mut client = client(&serve(router, Some(keys.verifier.clone())).await).await;

        for (actor, tenant) in [
            (SignedActorType::Staff, "tenant-a"),
            (SignedActorType::System, "tenant-a"),
            (SignedActorType::Guest, "tenant-a"),
            (SignedActorType::Service, "*"),
            (SignedActorType::TenantAdmin, "*"),
        ] {
            let ctx = token(&keys.signer, actor, tenant);
            let status = client
                .call(forged("flare.media.v1.list_plugin_routes", Some(&ctx)))
                .await
                .unwrap_err();
            assert_eq!(status.code(), Code::PermissionDenied, "{actor:?} {tenant}");
        }
        assert!(seen.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn without_a_key_set_only_the_health_check_answers() {
        let keys = cell_keys(76);
        let router = ExtensionPluginRouter::new();
        let handler = Arc::new(MockHandler::new("media", &["flare.media.v1."]));
        let seen = Arc::clone(&handler.seen);
        router.register(handler).await;
        let mut client = client(&serve(router, None).await).await;

        assert!(
            client
                .call(Request::new(generic(HEALTH_CHECK_OPERATION)))
                .await
                .unwrap()
                .into_inner()
                .ok
        );
        // Even a validly signed context is not admitted: nothing verifies it.
        let ctx = token(&keys.signer, SignedActorType::User, "tenant-a");
        for request in [
            forged("flare.media.v1.list_plugin_routes", None),
            forged("flare.media.v1.list_plugin_routes", Some(&ctx)),
        ] {
            let status = client.call(request).await.unwrap_err();
            assert_eq!(status.code(), Code::Unauthenticated, "{status:?}");
        }
        assert!(seen.0.lock().unwrap().is_empty());
    }
}
