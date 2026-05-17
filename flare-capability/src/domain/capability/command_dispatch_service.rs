//! 能力 **Dispatch** 领域服务：`CapabilityDispatchCommand` → RTC 端口（与传输、gRPC 无关）。
//!
//! 策略校验与 RTC 选路后的具体动作在此收敛；应用层 [`crate::application::handler`] 仅负责从注册表取出 `RtcCapability` 并调用本服务。

use flare_core_base::context::Ctx;
use serde_json::{Map, Value};

use super::{
    AcceptCallRequest, AddIceCandidateRequest, CapabilityDispatchCommand, CapabilityDispatchResult,
    CapabilityError, CapabilityPolicyBackend, CreateCallRequest, HandleSdpAnswerRequest,
    HandleSdpOfferRequest, HangupCallRequest, GetJoinTokenRequest, MediaGetNetworkQualityRequest,
    MediaGetRoomStateRequest, MediaJoinTransportRequest, MediaLeaveTransportRequest,
    MediaSetPublisherMuteRequest,
    MediaSetSimulcastLayerRequest, MediaSetSubscriptionRequest, RejectCallRequest, Result,
    RtcCapability,
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

fn payload_bool_required(payload: &Value, key: &str) -> Result<bool> {
    payload
        .get(key)
        .and_then(|v| v.as_bool())
        .ok_or_else(|| CapabilityError::System(format!("payload.{key} required (bool)")))
}

fn payload_u32(payload: &Value, key: &str, default: u32) -> u32 {
    payload
        .get(key)
        .and_then(|v| v.as_u64())
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(default)
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
            let media =
                if req.capability_id.ends_with("audio") || req.capability_id.ends_with(".audio") {
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
        "rtc.call.join_token" => {
            let call_id = extract_call_id(&payload)?;
            let jr = GetJoinTokenRequest {
                tenant_id: tenant.clone(),
                request_id: rid.clone(),
                call_id: call_id.clone(),
                user_id: user,
                ext: payload,
            };
            let r = rtc.get_join_token(ctx, &jr).await?;
            let mut m = Map::new();
            m.insert("call_id".into(), call_id.into());
            m.insert("sfu_join_token".into(), r.token.into());
            m.insert(
                "sfu_join_token_ttl_seconds".into(),
                Value::from(r.ttl_seconds),
            );
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
        "rtc.media.join" => {
            let room_id = payload_str(&payload, "room_id")?;
            let jr = MediaJoinTransportRequest {
                tenant_id: tenant.clone(),
                request_id: rid.clone(),
                room_id,
                call_id: payload_opt_str(&payload, "call_id").unwrap_or_default(),
                user_id: user,
                role: payload_opt_str(&payload, "role").unwrap_or_default(),
                peer_id: payload_opt_str(&payload, "peer_id"),
            };
            let r = rtc.media_join_transport(ctx, &jr).await?;
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
        "rtc.media.leave" => {
            let room_id = payload_str(&payload, "room_id")?;
            let peer_id = payload_str(&payload, "peer_id")?;
            let lr = MediaLeaveTransportRequest {
                tenant_id: tenant.clone(),
                request_id: rid.clone(),
                room_id,
                peer_id,
                session_id: payload_opt_str(&payload, "session_id").unwrap_or_default(),
                user_id: user.clone(),
            };
            let r = rtc.media_leave_transport(ctx, &lr).await?;
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
        "rtc.media.sdp.offer" => {
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
            let r = rtc.media_handle_sdp_offer(ctx, &ho).await?;
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
        "rtc.media.sdp.answer" => {
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
            let r = rtc.media_handle_sdp_answer(ctx, &ha).await?;
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
        "rtc.media.ice.candidate" => {
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
            let r = rtc.media_add_ice_candidate(ctx, &ice).await?;
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
        "rtc.media.publisher.mute" => {
            let room_id = payload_str(&payload, "room_id")?;
            let publisher_peer_id = payload_str(&payload, "publisher_peer_id")?;
            let mute_audio = payload_bool_required(&payload, "mute_audio")?;
            let mute_video = payload_bool_required(&payload, "mute_video")?;
            let mute = MediaSetPublisherMuteRequest {
                tenant_id: tenant.clone(),
                request_id: rid.clone(),
                room_id,
                publisher_peer_id,
                mute_audio,
                mute_video,
            };
            let r = rtc.media_set_publisher_mute(ctx, &mute).await?;
            let mut m = Map::new();
            m.insert("applied".into(), Value::Bool(r.applied));
            let data = merge_object(m, r.ext);
            Ok(CapabilityDispatchResult::ok(
                rid,
                "plugin.rtc",
                req.capability_id.clone(),
                data,
            ))
        }
        "rtc.media.subscription.set" => {
            let room_id = payload_str(&payload, "room_id")?;
            let subscriber_peer_id = payload_str(&payload, "subscriber_peer_id")?;
            let track_id = payload_str(&payload, "track_id")?;
            let sub = MediaSetSubscriptionRequest {
                tenant_id: tenant.clone(),
                request_id: rid.clone(),
                room_id,
                subscriber_peer_id,
                track_id,
                enable: payload_bool_required(&payload, "enable")?,
                media: payload_opt_str(&payload, "media"),
                preferred_layer: payload_opt_str(&payload, "preferred_layer"),
                priority: payload_u32(&payload, "priority", 0),
            };
            let r = rtc.media_set_subscription(ctx, &sub).await?;
            let mut m = Map::new();
            m.insert("applied".into(), Value::Bool(r.applied));
            let data = merge_object(m, r.ext);
            Ok(CapabilityDispatchResult::ok(
                rid,
                "plugin.rtc",
                req.capability_id.clone(),
                data,
            ))
        }
        "rtc.media.simulcast.layer.set" => {
            let room_id = payload_str(&payload, "room_id")?;
            let subscriber_peer_id = payload_str(&payload, "subscriber_peer_id")?;
            let track_id = payload_str(&payload, "track_id")?;
            let req_sim = MediaSetSimulcastLayerRequest {
                tenant_id: tenant.clone(),
                request_id: rid.clone(),
                room_id,
                subscriber_peer_id,
                track_id,
                layer: payload_str(&payload, "layer")?,
            };
            let r = rtc.media_set_simulcast_layer(ctx, &req_sim).await?;
            let mut m = Map::new();
            m.insert("applied".into(), Value::Bool(r.applied));
            let data = merge_object(m, r.ext);
            Ok(CapabilityDispatchResult::ok(
                rid,
                "plugin.rtc",
                req.capability_id.clone(),
                data,
            ))
        }
        "rtc.media.network.quality.get" => {
            let room_id = payload_str(&payload, "room_id")?;
            let peer_id = payload_str(&payload, "peer_id")?;
            let query = MediaGetNetworkQualityRequest {
                tenant_id: tenant.clone(),
                request_id: rid.clone(),
                room_id,
                peer_id,
            };
            let r = rtc.media_get_network_quality(ctx, &query).await?;
            let mut m = Map::new();
            m.insert("has_data".into(), Value::Bool(r.has_data));
            m.insert("upstream_score".into(), Value::from(r.upstream_score));
            m.insert("downstream_score".into(), Value::from(r.downstream_score));
            m.insert("rtt_ms".into(), Value::from(r.rtt_ms));
            m.insert("packet_loss_ratio".into(), Value::from(r.packet_loss_ratio));
            let data = merge_object(m, r.ext);
            Ok(CapabilityDispatchResult::ok(
                rid,
                "plugin.rtc",
                req.capability_id.clone(),
                data,
            ))
        }
        "rtc.media.room.state" => {
            let room_id = payload_str(&payload, "room_id")?;
            let query = MediaGetRoomStateRequest {
                tenant_id: tenant.clone(),
                request_id: rid.clone(),
                room_id,
            };
            let r = rtc.media_get_room_state(ctx, &query).await?;
            let mut m = Map::new();
            m.insert("room_id".into(), r.room_id.into());
            m.insert("exists".into(), Value::Bool(r.exists));
            m.insert("revision".into(), Value::from(r.revision));
            if !r.room_snapshot_json.trim().is_empty() {
                m.insert("room_snapshot_json".into(), r.room_snapshot_json.clone().into());
                if let Ok(v) = serde_json::from_str::<Value>(&r.room_snapshot_json) {
                    m.insert("room_snapshot".into(), v);
                }
            }
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
