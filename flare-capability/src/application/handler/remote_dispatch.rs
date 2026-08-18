//! 远程能力动态分发：按 `capability_id` 选择已注册插件并执行 `ExtensionPlugin.Call`。

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use flare_core_base::context::Ctx;
use flare_grpc_proto::capability::GenericRequest;
use flare_grpc_proto::capability::RegisteredPluginInstance;
use flare_grpc_proto::capability::extension_plugin_client::ExtensionPluginClient;
use serde_json::Value;
use tonic::Request;

use crate::domain::capability::{
    CapabilityDispatchCommand, CapabilityDispatchResult, CapabilityError, Result,
};
use crate::infrastructure::capability::PluginRouteBook;
use crate::infrastructure::capability::plugin_channel::resolve_plugin_channel;

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
    let payload = build_request_payload(req).map_err(|e| CapabilityError::System(e.to_string()))?;
    let channel = resolve_plugin_channel(endpoint.grpc_authority.as_str())
        .await
        .map_err(CapabilityError::System)?;
    let mut client = ExtensionPluginClient::new(channel);

    let tenant_id = req.tenant_id.clone().unwrap_or_else(|| "0".to_string());
    let user_id = req.user_id.clone().unwrap_or_default();
    let request_id = req
        .request_id
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("tenant_id".to_string(), tenant_id);
    if !user_id.is_empty() {
        metadata.insert("user_id".to_string(), user_id);
    }
    let trace_id = ctx.trace_id();
    if !trace_id.is_empty() {
        metadata.insert("trace_id".to_string(), trace_id.to_string());
    }

    let grpc_req = Request::new(GenericRequest {
        operation: req.capability_id.clone(),
        metadata,
        payload: Some(payload),
        request_id: request_id.clone(),
    });

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

/// 按 capability 动态调用远程插件：
/// - 优先健康实例；
/// - 失败自动降级到下一个实例；
/// - 全部失败时返回最后一次错误。
pub async fn dispatch_remote_by_capability_id(
    ctx: &Ctx,
    req: &CapabilityDispatchCommand,
    routes: &Arc<PluginRouteBook>,
    plugin_timeout: Duration,
    health_stale: Duration,
) -> Result<CapabilityDispatchResult> {
    let tenant_id = req.tenant_id.clone().unwrap_or_else(|| "0".to_string());
    let mut candidates = routes.list_filtered(&tenant_id, req.capability_id.as_str());
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

    use flare_core_base::context::Ctx;
    use flare_grpc_proto::capability::RegisteredPluginInstance;
    use flare_server_core::Context;

    use super::dispatch_remote_by_capability_id;
    use crate::domain::capability::{CapabilityDispatchCommand, CapabilityError};
    use crate::infrastructure::capability::PluginRouteBook;

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
        let ctx: Ctx = Arc::new(Context::default());
        let req = CapabilityDispatchCommand {
            capability_id: capability_id.into(),
            tenant_id: Some("0".into()),
            user_id: Some("u1".into()),
            conversation_id: None,
            payload: None,
            request_id: Some("r1".into()),
        };
        dispatch_remote_by_capability_id(
            &ctx,
            &req,
            &Arc::new(book),
            Duration::from_millis(30),
            Duration::from_secs(30),
        )
        .await
        .map(|_| ())
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
}
