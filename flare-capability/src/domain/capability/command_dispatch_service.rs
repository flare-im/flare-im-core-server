//! 能力 **Dispatch** 领域服务：`CapabilityDispatchCommand` → RTC 端口（与传输、gRPC 无关）。
//!
//! 策略校验与 RTC 选路后的具体动作在此收敛；应用层 [`crate::application::handler`] 仅负责从注册表取出 `RtcCapability` 并调用本服务。

use flare_core_base::context::Ctx;
use serde_json::{Map, Value};

use super::{
    AcceptCallRequest, AddIceCandidateRequest, CapabilityDispatchCommand, CapabilityDispatchResult,
    CapabilityError, CapabilityPolicyBackend, CreateCallRequest, GetJoinTokenRequest,
    HandleSdpAnswerRequest, HandleSdpOfferRequest, HangupCallRequest, RejectCallRequest, Result,
    RtcCapability, SfuJoinRoomRequest, SfuLeaveRoomRequest,
};

fn payload_str(payload: &Value, key: &str) -> Result<String> {
    let s = payload
        .get(key)
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            CapabilityError::System(format!("payload.{key} required (non-empty string)"))
        })?;
    Ok(s)
}

fn payload_opt_str(payload: &Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
}

fn merge_object(mut base: Map<String, Value>, ext: Value) -> Value {
    if let Value::Object(m) = ext {
        for (k, v) in m {
            base.insert(k, v);
        }
    }
    Value::Object(base)
}

/// 在已通过策略校验的前提下，按 `capability_id` 路由到 RTC 动作（领域核心逻辑）。
#[tracing::instrument(skip_all, fields(capability_id = %req.capability_id))]
pub async fn dispatch_rtc_by_capability_id<R: RtcCapability + ?Sized>(
    ctx: &Ctx,
    rtc: &R,
    req: &CapabilityDispatchCommand,
) -> Result<CapabilityDispatchResult> {
    let tenant = req.tenant_id.clone().unwrap_or_else(|| "0".into());
    let user = req
        .user_id
        .clone()
        .ok_or_else(|| CapabilityError::PolicyDenied("user_id required".into()))?;

    let rid = req
        .request_id
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let payload = req.payload.clone().unwrap_or(Value::Null);
    let conv = req.conversation_id.clone().unwrap_or_default();

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
        "rtc.call.reject" => {
            let call_id = extract_call_id(&payload)?;
            let reason = payload
                .get("reason")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let rr = RejectCallRequest {
                tenant_id: tenant.clone(),
                request_id: rid.clone(),
                call_id: call_id.clone(),
                user_id: user,
                reason,
                ext: payload,
            };
            let r = rtc.reject_call(ctx, &rr).await?;
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
        "rtc.call.end" => {
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
        "rtc.call.join_token" => {
            let call_id = extract_call_id(&payload)?;
            let jt = GetJoinTokenRequest {
                tenant_id: tenant.clone(),
                request_id: rid.clone(),
                call_id: call_id.clone(),
                user_id: user,
                ext: payload,
            };
            let r = rtc.get_join_token(ctx, &jt).await?;
            let mut m = Map::new();
            m.insert("token".into(), Value::String(r.token));
            m.insert(
                "ttl_seconds".into(),
                Value::Number(serde_json::Number::from(r.ttl_seconds)),
            );
            let data = merge_object(m, r.ext);
            Ok(CapabilityDispatchResult::ok(
                rid,
                "plugin.rtc",
                req.capability_id.clone(),
                data,
            ))
        }
        "rtc.sfu.join_room" => {
            let room_id = payload_str(&payload, "room_id")?;
            let jr = SfuJoinRoomRequest {
                tenant_id: tenant.clone(),
                request_id: rid.clone(),
                room_id,
                call_id: payload_opt_str(&payload, "call_id").unwrap_or_default(),
                user_id: user,
                role: payload_opt_str(&payload, "role").unwrap_or_default(),
                peer_id: payload_opt_str(&payload, "peer_id"),
            };
            let r = rtc.sfu_join_room(ctx, &jr).await?;
            let mut m = Map::new();
            m.insert("room_id".into(), r.room_id.into());
            m.insert("peer_id".into(), r.peer_id.into());
            m.insert("session_id".into(), r.session_id.into());
            m.insert("call_id".into(), r.call_id.into());
            let data = merge_object(m, r.ext);
            Ok(CapabilityDispatchResult::ok(
                rid,
                "plugin.rtc",
                req.capability_id.clone(),
                data,
            ))
        }
        "rtc.sfu.leave_room" => {
            let room_id = payload_str(&payload, "room_id")?;
            let peer_id = payload_str(&payload, "peer_id")?;
            let lr = SfuLeaveRoomRequest {
                tenant_id: tenant.clone(),
                request_id: rid.clone(),
                room_id,
                peer_id,
                session_id: payload_opt_str(&payload, "session_id").unwrap_or_default(),
                user_id: user.clone(),
            };
            let r = rtc.sfu_leave_room(ctx, &lr).await?;
            let mut m = Map::new();
            m.insert("left".into(), Value::Bool(r.left));
            let data = merge_object(m, r.ext);
            Ok(CapabilityDispatchResult::ok(
                rid,
                "plugin.rtc",
                req.capability_id.clone(),
                data,
            ))
        }
        "rtc.sfu.handle_sdp_offer" => {
            let room_id = payload_str(&payload, "room_id")?;
            let peer_id = payload_str(&payload, "peer_id")?;
            let sdp_offer = payload_str(&payload, "sdp_offer")?;
            let ho = HandleSdpOfferRequest {
                tenant_id: tenant.clone(),
                request_id: rid.clone(),
                room_id,
                peer_id,
                sdp_offer,
            };
            let r = rtc.sfu_handle_sdp_offer(ctx, &ho).await?;
            let mut m = Map::new();
            m.insert("sdp_answer".into(), r.sdp_answer.into());
            let data = merge_object(m, r.ext);
            Ok(CapabilityDispatchResult::ok(
                rid,
                "plugin.rtc",
                req.capability_id.clone(),
                data,
            ))
        }
        "rtc.sfu.handle_sdp_answer" => {
            let room_id = payload_str(&payload, "room_id")?;
            let peer_id = payload_str(&payload, "peer_id")?;
            let sdp_answer = payload_str(&payload, "sdp_answer")?;
            let ha = HandleSdpAnswerRequest {
                tenant_id: tenant.clone(),
                request_id: rid.clone(),
                room_id,
                peer_id,
                sdp_answer,
            };
            let r = rtc.sfu_handle_sdp_answer(ctx, &ha).await?;
            let mut m = Map::new();
            m.insert("accepted".into(), Value::Bool(r.accepted));
            let data = merge_object(m, r.ext);
            Ok(CapabilityDispatchResult::ok(
                rid,
                "plugin.rtc",
                req.capability_id.clone(),
                data,
            ))
        }
        "rtc.sfu.add_ice_candidate" => {
            let room_id = payload_str(&payload, "room_id")?;
            let peer_id = payload_str(&payload, "peer_id")?;
            let candidate_json = payload_str(&payload, "candidate_json")?;
            let ice = AddIceCandidateRequest {
                tenant_id: tenant.clone(),
                request_id: rid.clone(),
                room_id,
                peer_id,
                candidate_json,
            };
            let r = rtc.sfu_add_ice_candidate(ctx, &ice).await?;
            let mut m = Map::new();
            m.insert("accepted".into(), Value::Bool(r.accepted));
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

/// 一次完整的 **Dispatch**：租户策略 → RTC 领域路由。
#[tracing::instrument(skip_all, fields(capability_id = %req.capability_id))]
pub async fn execute_capability_dispatch<R: RtcCapability + ?Sized>(
    ctx: &Ctx,
    rtc: &R,
    policy: &dyn CapabilityPolicyBackend,
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

    dispatch_rtc_by_capability_id(ctx, rtc, req).await
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
