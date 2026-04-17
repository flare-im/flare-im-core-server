//! 能力分发用例：`CapabilityDispatchCommand` → RTC 等扩展。

use std::sync::Arc;

use flare_core_base::context::Ctx;
use serde_json::{Map, Value};

use crate::domain::capability::{
    AcceptCallRequest, CapabilityDispatchCommand, CapabilityDispatchResult, CapabilityError,
    CapabilityPolicyBackend, CreateCallRequest, HangupCallRequest, Result, RtcCapability,
};
use crate::infrastructure::capability::CapabilityExtensionRegistry;

fn merge_object(mut base: Map<String, Value>, ext: Value) -> Value {
    if let Value::Object(m) = ext {
        for (k, v) in m {
            base.insert(k, v);
        }
    }
    Value::Object(base)
}

/// 执行一次能力分发（写路径入口）。
#[tracing::instrument(skip_all, fields(capability_id = %req.capability_id))]
pub async fn dispatch_capability_command(
    ctx: &Ctx,
    registry: &CapabilityExtensionRegistry,
    policy: &Arc<dyn CapabilityPolicyBackend>,
    req: &CapabilityDispatchCommand,
) -> Result<CapabilityDispatchResult> {
    let tenant = req.tenant_id.clone().unwrap_or_else(|| "0".into());
    let user = req
        .user_id
        .clone()
        .ok_or_else(|| CapabilityError::PolicyDenied("user_id required".into()))?;
    policy
        .ensure_dispatch_allowed(&tenant, &user, &req.capability_id)
        .await?;

    let rid = req
        .request_id
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let payload = req.payload.clone().unwrap_or(Value::Null);
    let conv = req.conversation_id.clone().unwrap_or_default();

    let rtc = registry.rtc_router().await;

    match req.capability_id.as_str() {
        "rtc.call.video" | "rtc.call.audio" => {
            let media = if req.capability_id.ends_with("audio") {
                "audio"
            } else {
                "video"
            };
            let create = CreateCallRequest {
                tenant_id: tenant.clone(),
                request_id: rid.clone(),
                conversation_id: conv,
                initiator_user_id: user,
                media: Some(media.into()),
                ext: payload,
            };
            let r = rtc.create_call(ctx, &create).await?;
            let mut m = Map::new();
            m.insert("call_id".into(), r.call_id.clone().into());
            m.insert("room_id".into(), r.room_id.clone().into());
            let data = merge_object(m, r.ext);
            Ok(CapabilityDispatchResult::ok(
                rid,
                "plugin.rtc",
                req.capability_id.clone(),
                data,
            ))
        }
        "rtc.call.accept" => {
            let call_id = extract_call_id(&payload)?;
            let ar = AcceptCallRequest {
                tenant_id: tenant.clone(),
                request_id: rid.clone(),
                call_id: call_id.clone(),
                user_id: user,
                ext: payload,
            };
            let r = rtc.accept_call(ctx, &ar).await?;
            let mut m = Map::new();
            m.insert("call_id".into(), r.call_id.into());
            let data = merge_object(m, r.ext);
            Ok(CapabilityDispatchResult::ok(
                rid,
                "plugin.rtc",
                req.capability_id.clone(),
                data,
            ))
        }
        "rtc.call.reject" | "rtc.call.end" => {
            let call_id = extract_call_id(&payload)?;
            let hr = HangupCallRequest {
                tenant_id: tenant.clone(),
                request_id: rid.clone(),
                call_id: call_id.clone(),
                user_id: user,
                ext: payload,
            };
            let r = rtc.hangup_call(ctx, &hr).await?;
            let mut m = Map::new();
            m.insert("call_id".into(), r.call_id.into());
            let data = merge_object(m, r.ext);
            Ok(CapabilityDispatchResult::ok(
                rid,
                "plugin.rtc",
                req.capability_id.clone(),
                data,
            ))
        }
        other => Err(CapabilityError::NotSupported(format!(
            "capability_id not dispatchable: {other}"
        ))),
    }
}

fn extract_call_id(payload: &Value) -> Result<String> {
    let id = payload
        .get("call_id")
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned)
        .ok_or_else(|| CapabilityError::System("payload.call_id required".into()))?;
    if id.is_empty() {
        return Err(CapabilityError::System("payload.call_id empty".into()));
    }
    Ok(id)
}
