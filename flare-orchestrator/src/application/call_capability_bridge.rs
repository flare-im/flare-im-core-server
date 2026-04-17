//! `EVENT_CALL_SIGNAL` → `CapabilityService.Dispatch`（`rtc.call.*`），并回填服务端 `call_id` / `sfu_room_id`。

use std::collections::HashMap;
use std::sync::Arc;

use flare_grpc_proto::capability::DispatchCapabilityRequest;
use flare_im_core::Ctx;
use flare_proto::common::call_signal_event::Kind;
use flare_proto::common::event::Payload;
use flare_proto::common::{CallMediaType, CallSignalEvent, Event, EventType};
use serde_json::{json, Value};
use tracing::{debug, instrument};

use crate::error::{ErrorBuilder, ErrorCode, Result};
use crate::infrastructure::rpc::CapabilityDispatchClient;
use flare_server_core::flare_err;

/// 将通话领域事件与独立 `flare-capability` 进程的 RTC 能力对齐（经 gRPC，不依赖 `flare-capability` crate）。
#[derive(Debug)]
pub struct CallCapabilityBridge {
    client: Arc<CapabilityDispatchClient>,
}

impl CallCapabilityBridge {
    pub fn new(client: Arc<CapabilityDispatchClient>) -> Self {
        Self { client }
    }

    /// 对 `EVENT_CALL_SIGNAL` 在入库/推送前调用能力服务：`invite` 建呼并写回 `call_id`、`sfu_room_id`；
    /// `accept` / `reject` / `hangup` 同步挂断状态机。
    #[instrument(skip(self, ctx, event), fields(conversation_id = %event.conversation_id))]
    pub async fn enrich_call_signal_event(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        event: &mut Event,
    ) -> Result<()> {
        if EventType::try_from(event.r#type).ok() != Some(EventType::EventCallSignal) {
            return Ok(());
        }

        let Some(Payload::CallSignal(cs)) = event.payload.as_mut() else {
            return Ok(());
        };

        let rid = event
            .request_id
            .clone()
            .filter(|s| !s.is_empty())
            .or_else(|| {
                let r = ctx.request_id();
                if r.is_empty() {
                    None
                } else {
                    Some(r.to_string())
                }
            })
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        match cs.kind.as_ref() {
            Some(Kind::Invite(inv)) => {
                let cap = invite_capability_id(inv);
                let payload = invite_payload_json(cs);
                let req = DispatchCapabilityRequest {
                    capability_id: cap.to_string(),
                    tenant_id: tenant_id.to_string(),
                    user_id: cs.from_user_id.clone(),
                    conversation_id: event.conversation_id.clone(),
                    payload_json: payload.to_string(),
                    request_id: rid,
                };
                let result = self.client.dispatch(ctx.as_ref(), req).await?;
                apply_create_result_to_call_signal(cs, &result.result_json)?;
            }
            Some(Kind::Accept(_)) => {
                let payload = call_id_payload(cs)?;
                let req = DispatchCapabilityRequest {
                    capability_id: "rtc.call.accept".into(),
                    tenant_id: tenant_id.to_string(),
                    user_id: cs.from_user_id.clone(),
                    conversation_id: event.conversation_id.clone(),
                    payload_json: payload.to_string(),
                    request_id: rid,
                };
                self.client.dispatch(ctx.as_ref(), req).await?;
            }
            Some(Kind::Reject(_) | Kind::Hangup(_)) => {
                let payload = call_id_payload(cs)?;
                let req = DispatchCapabilityRequest {
                    capability_id: "rtc.call.end".into(),
                    tenant_id: tenant_id.to_string(),
                    user_id: cs.from_user_id.clone(),
                    conversation_id: event.conversation_id.clone(),
                    payload_json: payload.to_string(),
                    request_id: rid,
                };
                self.client.dispatch(ctx.as_ref(), req).await?;
            }
            _ => {
                debug!("call capability bridge: skip non-rtc kind");
            }
        }

        Ok(())
    }
}

fn invite_capability_id(inv: &flare_proto::common::CallInvite) -> &'static str {
    let types = inv
        .offered_media
        .as_ref()
        .map(|m| m.types.as_slice())
        .unwrap_or_default();
    let has_video = types
        .iter()
        .any(|t| *t == CallMediaType::Video as i32);
    if has_video {
        "rtc.call.video"
    } else {
        "rtc.call.audio"
    }
}

fn string_map_to_json(m: &HashMap<String, String>) -> Value {
    let mut obj = serde_json::Map::new();
    for (k, v) in m {
        obj.insert(k.clone(), Value::String(v.clone()));
    }
    Value::Object(obj)
}

fn invite_payload_json(cs: &CallSignalEvent) -> Value {
    let mut base = serde_json::Map::new();
    if !cs.call_id.is_empty() {
        base.insert("client_call_id".into(), Value::String(cs.call_id.clone()));
    }
    if !cs.ext.is_empty() {
        base.insert("ext".into(), string_map_to_json(&cs.ext));
    }
    Value::Object(base)
}

fn call_id_payload(cs: &CallSignalEvent) -> Result<Value> {
    let id = cs.call_id.trim();
    if id.is_empty() {
        return Err(
            ErrorBuilder::new(ErrorCode::InvalidParameter, "call_id required for rtc accept/hangup")
                .build_error(),
        );
    }
    Ok(json!({ "call_id": id }))
}

fn apply_create_result_to_call_signal(cs: &mut CallSignalEvent, result_json: &str) -> Result<()> {
    let v: Value = serde_json::from_str(result_json).map_err(|e| {
        flare_err!(
            ErrorCode::InternalError,
            &format!("capability result_json invalid: {e}")
        )
    })?;
    if let Some(cid) = v.get("call_id").and_then(|x| x.as_str()) {
        cs.call_id = cid.to_string();
    }
    if let Some(rid) = v.get("room_id").and_then(|x| x.as_str()) {
        cs.sfu_room_id = rid.to_string();
    }
    Ok(())
}
