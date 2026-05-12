//! 远程能力动态分发：按 `capability_id` 选择已注册插件并执行 `ExtensionPlugin.Call`。

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use dashmap::DashMap;
use flare_core_base::context::Ctx;
use flare_grpc_proto::capability::GenericRequest;
use flare_grpc_proto::capability::RegisteredPluginInstance;
use flare_grpc_proto::capability::extension_plugin_client::ExtensionPluginClient;
use once_cell::sync::Lazy;
use serde_json::Value;
use tonic::Request;
use tonic::transport::Channel;

use crate::domain::capability::{
    CapabilityDispatchCommand, CapabilityDispatchResult, CapabilityError, Result,
};
use crate::infrastructure::capability::PluginRouteBook;

static CHANNEL_CACHE: Lazy<DashMap<String, Channel>> = Lazy::new(DashMap::new);

fn build_request_payload(req: &CapabilityDispatchCommand) -> anyhow::Result<prost_types::Any> {
    let payload_json = serde_json::to_string(req.payload.as_ref().unwrap_or(&Value::Null))
        .map_err(|e| anyhow::anyhow!("serialize request payload_json: {e}"))?;
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
        Ok(text) => serde_json::from_str::<Value>(&text).unwrap_or_else(|_| Value::String(text)),
        Err(_) => Value::String(STANDARD.encode(any.value)),
    }
}

fn get_or_create_channel(grpc_authority: &str) -> Result<Channel> {
    if let Some(ch) = CHANNEL_CACHE.get(grpc_authority) {
        return Ok(ch.clone());
    }
    let endpoint =
        if grpc_authority.starts_with("http://") || grpc_authority.starts_with("https://") {
            grpc_authority.to_string()
        } else {
            format!("http://{grpc_authority}")
        };
    let channel = Channel::from_shared(endpoint.clone())
        .map_err(|e| CapabilityError::System(format!("invalid plugin endpoint {endpoint}: {e}")))?
        .connect_lazy();
    CHANNEL_CACHE.insert(grpc_authority.to_string(), channel.clone());
    Ok(channel)
}

async fn invoke_once(
    ctx: &Ctx,
    req: &CapabilityDispatchCommand,
    endpoint: &RegisteredPluginInstance,
    timeout: Duration,
) -> Result<CapabilityDispatchResult> {
    let payload = build_request_payload(req).map_err(|e| CapabilityError::System(e.to_string()))?;
    let channel = get_or_create_channel(endpoint.grpc_authority.as_str())?;
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
