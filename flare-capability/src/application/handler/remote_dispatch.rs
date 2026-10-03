//! 远程能力动态分发：按 `capability_id` 选择已注册插件并执行 `ExtensionPlugin.Call`。

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use flare_core_base::context::Ctx;
use flare_grpc_proto::capability::GenericRequest;
use flare_grpc_proto::capability::RegisteredPluginInstance;
use flare_grpc_proto::capability::extension_plugin_client::ExtensionPluginClient;
use flare_server_core::context::{ActorContext, ActorType, Context};
use flare_server_core::{
    SignedActorType, SignedContextClaims, SignedContextVerifier, set_signed_context_token,
};
use serde_json::Value;
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};

use crate::domain::capability::{
    CapabilityDispatchCommand, CapabilityDispatchResult, CapabilityError, Result,
    require_dispatch_tenant,
};
use crate::infrastructure::capability::PluginRouteBook;
use crate::infrastructure::capability::plugin_channel::resolve_plugin_channel;

/// Only the verifier can construct this marker; an untrusted request body or
/// legacy Context metadata cannot create a dispatch identity.
pub(crate) struct VerifiedDispatchOrigin {
    claims: SignedContextClaims,
    token: String,
}

impl VerifiedDispatchOrigin {
    pub(crate) fn verify(
        metadata: &MetadataMap,
        verifier: &SignedContextVerifier,
    ) -> std::result::Result<Self, Status> {
        let mut headers = metadata.get_all("x-flare-ctx").iter();
        let token = headers
            .next()
            .ok_or_else(|| Status::unauthenticated("signed dispatch context required"))?
            .to_str()
            .map_err(|_| Status::unauthenticated("invalid signed dispatch context"))?;
        if headers.next().is_some() || token.len() > 8192 {
            return Err(Status::unauthenticated("invalid signed dispatch context"));
        }
        let claims = verifier
            .verify(token)
            .map_err(|_| Status::unauthenticated("invalid signed dispatch context"))?;
        Ok(Self {
            claims,
            token: token.to_owned(),
        })
    }

    pub(crate) fn require_user_binding(
        &self,
        tenant_id: &str,
        user_id: &str,
        request_id: &str,
    ) -> std::result::Result<(), Status> {
        if self.claims.actor_type != SignedActorType::User
            || tenant_id != self.claims.tenant_id
            || user_id != self.claims.actor_id
            || request_id != self.claims.request_id
        {
            return Err(Status::permission_denied(
                "dispatch identity does not match signed context",
            ));
        }
        Ok(())
    }

    pub(crate) fn into_context(self) -> Ctx {
        let mut actor = ActorContext::new(&self.claims.actor_id).with_type(ActorType::User);
        for role in &self.claims.roles {
            actor = actor.with_role(role);
        }
        Arc::new(
            Context::with_request_id(&self.claims.request_id)
                .with_tenant_id(&self.claims.tenant_id)
                .with_user_id(&self.claims.actor_id)
                .with_actor(actor)
                .insert_data(self),
        )
    }

    fn token(&self) -> &str {
        &self.token
    }
}

fn build_request_payload(
    req: &CapabilityDispatchCommand,
) -> flare_server_core::error::Result<prost_types::Any> {
    let payload_json = serde_json::to_string(req.payload.as_ref().unwrap_or(&Value::Null))
        .map_err(|e| {
            flare_server_core::error::FlareError::system(format!(
                "serialize request payload_json: {e}"
            ))
        })?;
    Ok(prost_types::Any {
        type_url: "type.googleapis.com/flare.capability.v1.PayloadJson".to_string(),
        value: payload_json.into_bytes(),
    })
}

fn decode_response_payload(payload: Option<prost_types::Any>) -> Value {
    let Some(any) = payload else {
        return Value::Null;
    };
    match String::from_utf8(any.value.clone()) {
        Ok(text) => serde_json::from_str::<Value>(&text).unwrap_or(Value::String(text)),
        Err(_) => Value::String(STANDARD.encode(any.value)),
    }
}

async fn invoke_once(
    ctx: &Ctx,
    req: &CapabilityDispatchCommand,
    endpoint: &RegisteredPluginInstance,
    timeout: Duration,
) -> Result<CapabilityDispatchResult> {
    let tenant_id = require_dispatch_tenant(req)?;
    let user_id = req.user_id.clone().unwrap_or_default();
    let request_id = req
        .request_id
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let origin = ctx
        .get_data::<VerifiedDispatchOrigin>()
        .ok_or_else(|| CapabilityError::PolicyDenied("verified dispatch origin required".into()))?;
    origin
        .require_user_binding(&tenant_id, &user_id, &request_id)
        .map_err(|_| CapabilityError::PolicyDenied("dispatch identity mismatch".into()))?;

    let payload = build_request_payload(req).map_err(|e| CapabilityError::System(e.to_string()))?;
    let channel = resolve_plugin_channel(endpoint.grpc_authority.as_str())
        .await
        .map_err(CapabilityError::System)?;
    let mut client = ExtensionPluginClient::new(channel);

    let mut metadata = std::collections::HashMap::new();
    metadata.insert("tenant_id".to_string(), tenant_id);
    if !user_id.is_empty() {
        metadata.insert("user_id".to_string(), user_id);
    }
    let trace_id = ctx.trace_id();
    if !trace_id.is_empty() {
        metadata.insert("trace_id".to_string(), trace_id.to_string());
    }
    let mut grpc_req = Request::new(GenericRequest {
        operation: req.capability_id.clone(),
        metadata,
        payload: Some(payload),
        request_id: request_id.clone(),
    });
    set_signed_context_token(&mut grpc_req, origin.token())
        .map_err(|_| CapabilityError::PolicyDenied("invalid dispatch credential".into()))?;

    let response = tokio::time::timeout(timeout, client.call(grpc_req))
        .await
        .map_err(|_| {
            CapabilityError::Timeout(format!("plugin call timeout: {}", endpoint.plugin_id))
        })?
        .map_err(|s| {
            CapabilityError::System(format!("plugin {} call failed: {s}", endpoint.plugin_id))
        })?
        .into_inner();

    if !response.ok {
        return Err(CapabilityError::System(format!(
            "plugin {} returned error: {} {}",
            endpoint.plugin_id, response.error_code, response.error_message
        )));
    }

    Ok(CapabilityDispatchResult::ok(
        request_id,
        endpoint.plugin_id.clone(),
        req.capability_id.clone(),
        decode_response_payload(response.payload),
    ))
}

/// 按 capability 动态调用远程插件，**候选实例由调用方传入**：
/// 优先健康实例，失败自动降级到下一个，全部失败时返回最后一次错误。
///
/// 不在这里自己查路由簿：`list_filtered` 是全表扫描 + 逐个 clone 实例
/// （含 declared_operations 向量与 labels 哈希），而上层为了判 `seat_model`
/// 已经查过一次。两处各查一次就是每次分发扫两遍全表 —— 插件多起来后是纯浪费，
/// 且这种回归**没有任何功能信号**：测试全绿、行为正确，只是慢。
///
/// 让调用方传候选，而不是把 seat_model 塞进 `CapabilityDispatchCommand`：
/// 命令是**请求的描述**，不该携带路由查询的中间结果。
pub async fn dispatch_remote_with_candidates(
    ctx: &Ctx,
    req: &CapabilityDispatchCommand,
    routes: &Arc<PluginRouteBook>,
    candidates: Vec<RegisteredPluginInstance>,
    plugin_timeout: Duration,
    health_stale: Duration,
) -> Result<CapabilityDispatchResult> {
    let tenant_id = require_dispatch_tenant(req)?;
    let mut candidates = candidates;
    if candidates.is_empty() {
        return Err(CapabilityError::NotRegistered(format!(
            "no plugin endpoint registered for capability {} (tenant={})",
            req.capability_id, tenant_id
        )));
    }

    // 声明即边界：插件声明过 declared_operations 的，声明之外的调用一律拒绝。
    //
    // unverified 插件（没带声明的旧插件、配置发现登记的端点）跳过这道检查——
    // 否则核心一升级，所有已部署插件立刻全部失效。代价是它们的边界无法强制，
    // 所以注册时已经打了 unverified 标并告警。
    //
    // 注意这里是**过滤**而不是直接报错：同一个 capability 可能有多个实例，
    // 其中一部分声明了、一部分没有；把不该接管的滤掉，剩下的照常走降级链路。
    let before = candidates.len();
    candidates.retain(|i| {
        i.unverified
            || i.declared_operations.is_empty()
            || i.declared_operations.contains(&req.capability_id)
    });
    if candidates.is_empty() {
        return Err(CapabilityError::PolicyDenied(format!(
            "capability {} is not in the declared operations of any registered plugin \
             (tenant={}, {} endpoint(s) rejected by declaration)",
            req.capability_id, tenant_id, before
        )));
    }

    candidates.sort_by_key(|i| {
        if routes.is_healthy(&tenant_id, &i.plugin_id, health_stale) {
            0u8
        } else {
            1u8
        }
    });

    let mut last_err: Option<CapabilityError> = None;
    for endpoint in candidates {
        match invoke_once(ctx, req, &endpoint, plugin_timeout).await {
            Ok(result) => {
                routes.mark_health(&tenant_id, &endpoint.plugin_id, true, None);
                return Ok(result);
            }
            Err(e) => {
                routes.mark_health(&tenant_id, &endpoint.plugin_id, false, Some(e.to_string()));
                last_err = Some(e);
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        CapabilityError::System("all registered plugin endpoints failed".to_string())
    }))
}

#[cfg(test)]
mod declaration_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_core_base::context::Ctx;
    use flare_grpc_proto::capability::RegisteredPluginInstance;
    use flare_server_core::{
        Context, SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
    };
    use tonic::metadata::MetadataMap;

    use super::{VerifiedDispatchOrigin, dispatch_remote_with_candidates, invoke_once};
    use crate::domain::capability::{CapabilityDispatchCommand, CapabilityError};
    use crate::infrastructure::capability::PluginRouteBook;

    fn verified_origin(actor_type: SignedActorType) -> (VerifiedDispatchOrigin, String) {
        let key = SigningKey::from_bytes(&[54; 32]);
        let private = key.to_pkcs8_pem(Default::default()).unwrap();
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        let signer = SignedContextSigner::from_private_key(&private).unwrap();
        let verifier = SignedContextVerifier::from_public_key(&public).unwrap();
        let token = signer
            .sign(&SignedContextIdentity::new(actor_type, "u1", "0", "r1"))
            .unwrap();
        let mut metadata = MetadataMap::new();
        metadata.insert("x-flare-ctx", token.parse().unwrap());
        (
            VerifiedDispatchOrigin::verify(&metadata, &verifier).unwrap(),
            token,
        )
    }

    #[test]
    fn dispatch_origin_requires_signed_user_and_exact_body_identity() {
        let (origin, token) = verified_origin(SignedActorType::User);
        origin.require_user_binding("0", "u1", "r1").unwrap();
        for (tenant, user, request) in [
            ("other", "u1", "r1"),
            ("0", "other", "r1"),
            ("0", "u1", "other"),
            ("", "u1", "r1"),
        ] {
            assert!(origin.require_user_binding(tenant, user, request).is_err());
        }
        let ctx = origin.into_context();
        assert_eq!(ctx.tenant_id(), Some("0"));
        assert_eq!(ctx.user_id(), Some("u1"));
        assert_eq!(
            ctx.get_data::<VerifiedDispatchOrigin>().unwrap().token(),
            token
        );

        let (service, _) = verified_origin(SignedActorType::Service);
        assert!(service.require_user_binding("0", "u1", "r1").is_err());
    }

    #[test]
    fn dispatch_origin_rejects_missing_invalid_and_duplicate_credentials() {
        let key = SigningKey::from_bytes(&[54; 32]);
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        let verifier = SignedContextVerifier::from_public_key(&public).unwrap();
        assert!(VerifiedDispatchOrigin::verify(&MetadataMap::new(), &verifier).is_err());
        let mut invalid = MetadataMap::new();
        invalid.insert("x-flare-ctx", "forged".parse().unwrap());
        assert!(VerifiedDispatchOrigin::verify(&invalid, &verifier).is_err());
        let (_, token) = verified_origin(SignedActorType::User);
        let mut duplicate = MetadataMap::new();
        duplicate.append("x-flare-ctx", token.parse().unwrap());
        duplicate.append("x-flare-ctx", token.parse().unwrap());
        assert!(VerifiedDispatchOrigin::verify(&duplicate, &verifier).is_err());
    }

    /// 构造一个已注册实例。`declared` 为空且 `unverified=true` 表示旧插件。
    fn instance(
        plugin_id: &str,
        capability_id: &str,
        declared: &[&str],
    ) -> RegisteredPluginInstance {
        RegisteredPluginInstance {
            plugin_id: plugin_id.into(),
            capability_id: capability_id.into(),
            // 指向一个不会有人监听的地址：这些用例只验「有没有被声明挡下」，
            // 挡下时返回 PolicyDenied，没挡下时才会去连而失败成别的错误。
            grpc_authority: "127.0.0.1:1".into(),
            labels: Default::default(),
            plugin_version: String::new(),
            api_version: String::new(),
            manifest_sha256: String::new(),
            declared_operations: declared.iter().map(|s| s.to_string()).collect(),
            unverified: declared.is_empty(),
            // 这些用例验的是声明边界，不涉及计费单位 —— 留空即旧语义。
            seat_model: String::new(),
        }
    }

    async fn dispatch(book: PluginRouteBook, capability_id: &str) -> Result<(), CapabilityError> {
        let ctx: Ctx = verified_origin(SignedActorType::User).0.into_context();
        let req = CapabilityDispatchCommand {
            capability_id: capability_id.into(),
            tenant_id: Some("0".into()),
            user_id: Some("u1".into()),
            conversation_id: None,
            payload: None,
            request_id: Some("r1".into()),
        };
        // 与生产路径一致：调用方先查候选再分发。
        let book = Arc::new(book);
        let candidates = book.list_filtered("0", capability_id);
        dispatch_remote_with_candidates(
            &ctx,
            &req,
            &book,
            candidates,
            Duration::from_millis(30),
            Duration::from_secs(30),
        )
        .await
        .map(|_| ())
    }

    #[tokio::test]
    async fn bare_context_cannot_dispatch_to_plugin() {
        let book = PluginRouteBook::new();
        book.upsert("0", instance("p1", "vendorx.do", &["vendorx.do"]));
        let book = Arc::new(book);
        let req = CapabilityDispatchCommand {
            capability_id: "vendorx.do".into(),
            tenant_id: Some("0".into()),
            user_id: Some("u1".into()),
            conversation_id: None,
            payload: None,
            request_id: Some("r1".into()),
        };
        let ctx: Ctx = Arc::new(Context::default());
        let err = dispatch_remote_with_candidates(
            &ctx,
            &req,
            &book,
            book.list_filtered("0", "vendorx.do"),
            Duration::from_millis(30),
            Duration::from_secs(30),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CapabilityError::PolicyDenied(_)));
    }

    /// 声明之外的调用被拒，且拒绝原因是「声明」而不是「没注册」。
    #[tokio::test]
    async fn undeclared_operation_is_denied_by_declaration() {
        let book = PluginRouteBook::new();
        book.upsert("0", instance("p1", "vendorx.do", &["vendorx.other"]));

        let err = dispatch(book, "vendorx.do").await.unwrap_err();
        assert!(
            matches!(err, CapabilityError::PolicyDenied(_)),
            "应当因声明被拒，实际: {err:?}"
        );
    }

    /// 声明内的调用放行（会因为连不上而失败，但**不是** PolicyDenied）。
    #[tokio::test]
    async fn declared_operation_passes_the_declaration_gate() {
        let book = PluginRouteBook::new();
        book.upsert("0", instance("p1", "vendorx.do", &["vendorx.do"]));

        let err = dispatch(book, "vendorx.do").await.unwrap_err();
        assert!(
            !matches!(err, CapabilityError::PolicyDenied(_)),
            "不该被声明挡下，实际: {err:?}"
        );
    }

    /// **兼容窗口**：没带声明的旧插件照常放行。
    ///
    /// 这条是整个 v2 契约能不能上线的前提——要求必填的话，核心升级的瞬间
    /// 所有已部署插件都会失效。
    #[tokio::test]
    async fn unverified_legacy_plugin_still_works() {
        let book = PluginRouteBook::new();
        book.upsert("0", instance("legacy", "vendorx.do", &[]));

        let err = dispatch(book, "vendorx.do").await.unwrap_err();
        assert!(
            !matches!(err, CapabilityError::PolicyDenied(_)),
            "旧插件不该被声明挡下，实际: {err:?}"
        );
    }

    /// 多实例混合：声明不匹配的被滤掉，匹配的仍然可用。
    #[tokio::test]
    async fn mixed_instances_keep_the_declaring_one() {
        let book = PluginRouteBook::new();
        book.upsert("0", instance("bad", "vendorx.do", &["vendorx.other"]));
        book.upsert("0", instance("good", "vendorx.do", &["vendorx.do"]));

        let err = dispatch(book, "vendorx.do").await.unwrap_err();
        assert!(
            !matches!(err, CapabilityError::PolicyDenied(_)),
            "还有合法实例时不该整体被拒，实际: {err:?}"
        );
    }

    /// 缺租户（含空白）的远程调度在挑实例、对签名身份、连插件之前就以 `PolicyDenied` 拒绝，
    /// 不再按租户 "0" 处理（W13-D）。签名身份恰好是租户 "0" 时，以前会一路连到租户 0 的插件，
    /// 连接失败还会把该实例记成不健康。
    #[tokio::test]
    async fn remote_dispatch_without_a_tenant_is_denied_not_sent_as_tenant_zero() {
        let book = PluginRouteBook::new();
        book.upsert("0", instance("p1", "vendorx.do", &["vendorx.do"]));
        let book = Arc::new(book);

        for tenant_id in [None, Some(""), Some("  ")] {
            let ctx: Ctx = verified_origin(SignedActorType::User).0.into_context();
            let req = CapabilityDispatchCommand {
                capability_id: "vendorx.do".into(),
                tenant_id: tenant_id.map(str::to_string),
                user_id: Some("u1".into()),
                conversation_id: None,
                payload: None,
                request_id: Some("r1".into()),
            };
            let err = dispatch_remote_with_candidates(
                &ctx,
                &req,
                &book,
                book.list_filtered("0", "vendorx.do"),
                Duration::from_millis(30),
                Duration::from_secs(30),
            )
            .await
            .unwrap_err();
            assert!(
                matches!(&err, CapabilityError::PolicyDenied(reason) if reason == "tenant_id required"),
                "{tenant_id:?}: {err:?}"
            );
        }
        assert_eq!(
            book.last_error("0", "p1"),
            None,
            "缺租户的调度不该以租户 0 去调插件"
        );
    }

    /// 单个实例的调用自己也要求租户（纵深防御）：缺租户时在对签名身份、连插件之前拒绝，
    /// 不再按租户 "0" 对上签名身份后去连插件（W13-D）。
    #[tokio::test]
    async fn invoke_once_without_a_tenant_is_denied_before_calling_the_plugin() {
        let endpoint = instance("p1", "vendorx.do", &["vendorx.do"]);
        for tenant_id in [None, Some(""), Some("  ")] {
            let ctx: Ctx = verified_origin(SignedActorType::User).0.into_context();
            let req = CapabilityDispatchCommand {
                capability_id: "vendorx.do".into(),
                tenant_id: tenant_id.map(str::to_string),
                user_id: Some("u1".into()),
                conversation_id: None,
                payload: None,
                request_id: Some("r1".into()),
            };
            let err = invoke_once(&ctx, &req, &endpoint, Duration::from_millis(30))
                .await
                .unwrap_err();
            assert!(
                matches!(&err, CapabilityError::PolicyDenied(reason) if reason == "tenant_id required"),
                "{tenant_id:?}: {err:?}"
            );
        }
    }
}
