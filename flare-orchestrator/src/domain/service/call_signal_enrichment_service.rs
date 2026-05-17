use std::collections::HashMap;
use std::sync::Arc;

use flare_im_core::Ctx;
use flare_proto::common::call_signal_event::Signal;
use flare_proto::common::event::Payload;
use flare_proto::common::{
    CallHangup, CallMediaType, CallReject, CallSignalEvent, Event, EventType,
};
use serde_json::{Map, Value, json};
use tracing::{debug, instrument, warn};

use crate::domain::repository::CapabilityDispatchGateway;
use crate::error::{ErrorBuilder, ErrorCode, Result};
use flare_server_core::flare_err;

const EXT_SDP_TYPE: &str = "flare_sdp_type";
const EXT_SDP: &str = "flare_sdp";
const EXT_CAMERA_ENABLED: &str = "flare_camera_enabled";
const EXT_MICROPHONE_ENABLED: &str = "flare_microphone_enabled";
const EXT_SUB_TRACK_ID: &str = "flare_subscription_track_id";
const EXT_SUB_ENABLE: &str = "flare_subscription_enable";
const EXT_SUB_MEDIA: &str = "flare_subscription_media";
const EXT_SUB_PREFERRED_LAYER: &str = "flare_subscription_preferred_layer";
const EXT_SUB_PRIORITY: &str = "flare_subscription_priority";
const EXT_SUB_SUBSCRIBER_PEER_ID: &str = "flare_subscription_subscriber_peer_id";
const EXT_SIMULCAST_TRACK_ID: &str = "flare_simulcast_track_id";
const EXT_SIMULCAST_LAYER: &str = "flare_simulcast_layer";
const EXT_SIMULCAST_SUBSCRIBER_PEER_ID: &str = "flare_simulcast_subscriber_peer_id";
const EXT_NQ_QUERY_PEER_ID: &str = "flare_network_quality_peer_id";
const EXT_NQ_HAS_DATA: &str = "flare_network_quality_has_data";
const EXT_NQ_UPSTREAM_SCORE: &str = "flare_network_quality_upstream_score";
const EXT_NQ_DOWNSTREAM_SCORE: &str = "flare_network_quality_downstream_score";
const EXT_NQ_RTT_MS: &str = "flare_network_quality_rtt_ms";
const EXT_NQ_PACKET_LOSS_RATIO: &str = "flare_network_quality_packet_loss_ratio";

const CAP_CALL_AUDIO_START: &str = "rtc.call.audio";
const CAP_CALL_VIDEO_START: &str = "rtc.call.video";
const CAP_CALL_ACCEPT: &str = "rtc.call.accept";
const CAP_CALL_JOIN_TOKEN: &str = "rtc.call.join_token";
const CAP_CALL_REJECT: &str = "rtc.call.reject";
const CAP_CALL_END: &str = "rtc.call.end";
const CAP_CALL_SIGNAL_SDP_OFFER: &str = "rtc.media.sdp.offer";
const CAP_CALL_SIGNAL_SDP_ANSWER: &str = "rtc.media.sdp.answer";
const CAP_CALL_SIGNAL_ICE: &str = "rtc.media.ice.candidate";
const CAP_CALL_MEDIA_PUBLISHER_STATE: &str = "rtc.media.publisher.mute";
const CAP_CALL_MEDIA_SUBSCRIPTION_SET: &str = "rtc.media.subscription.set";
const CAP_CALL_MEDIA_SIMULCAST_LAYER_SET: &str = "rtc.media.simulcast.layer.set";
const CAP_CALL_MEDIA_NETWORK_QUALITY_GET: &str = "rtc.media.network.quality.get";

/// `EVENT_CALL_SIGNAL` enrich 领域服务：
/// 负责把业务信令映射到 capability 调度（核心不暴露具体媒体后端实现命名）。
pub struct CallSignalEnrichmentService {
    gateway: Arc<dyn CapabilityDispatchGateway>,
}

impl CallSignalEnrichmentService {
    pub fn new(gateway: Arc<dyn CapabilityDispatchGateway>) -> Self {
        Self { gateway }
    }

    async fn dispatch_capability_json(
        &self,
        ctx: &Ctx,
        capability_id: &str,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        request_id: String,
        payload: Value,
    ) -> Result<Value> {
        self.gateway
            .dispatch_json(
                ctx,
                capability_id,
                tenant_id,
                user_id,
                conversation_id,
                request_id,
                payload,
            )
            .await
    }

    /// 对 `EVENT_CALL_SIGNAL` 在入库/推送前调用能力服务：
    /// - Invite：`rtc.call.video` / `rtc.call.audio`
    /// - Accept：`rtc.call.accept`
    /// - Reject：`rtc.call.reject`
    /// - Hangup：`rtc.call.end`
    /// - Renegotiate：`rtc.media.sdp.offer/answer`
    /// - IceCandidate：`rtc.media.ice.candidate`
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

        if cs.from_user_id.trim().is_empty() {
            warn!("call_signal_enrichment: skip RTC — empty call_signal.from_user_id");
            return Ok(());
        }

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

        let conversation_for_dispatch = event.conversation_id.clone();
        if conversation_for_dispatch.trim().is_empty() {
            warn!("call_signal_enrichment: skip RTC — empty event.conversation_id");
            return Ok(());
        }

        let signal = cs.signal.clone();
        match signal {
            Some(Signal::Invite(inv)) => {
                let cap = invite_capability_id(&inv);
                let payload = invite_payload_json(cs);
                let result = self
                    .dispatch_capability_json(
                        ctx,
                        cap,
                        tenant_id,
                        &cs.from_user_id,
                        &conversation_for_dispatch,
                        rid,
                        payload,
                    )
                    .await?;
                apply_create_result_to_call_signal(cs, &result)?;
            }
            Some(Signal::Accept(_)) => {
                let payload = call_id_payload(cs)?;
                let result = self
                    .dispatch_capability_json(
                        ctx,
                        CAP_CALL_ACCEPT,
                        tenant_id,
                        &cs.from_user_id,
                        &conversation_for_dispatch,
                        rid.clone(),
                        payload,
                    )
                    .await?;
                apply_accept_result_to_call_signal(cs, &result)?;
                match self
                    .dispatch_capability_json(
                        ctx,
                        CAP_CALL_JOIN_TOKEN,
                        tenant_id,
                        &cs.from_user_id,
                        &conversation_for_dispatch,
                        format!("{rid}-join-token"),
                        call_id_payload(cs)?,
                    )
                    .await
                {
                    Ok(join) => apply_join_token_result_to_call_signal(cs, &join)?,
                    Err(err) => {
                        // 接听信令不能因为 token 附加失败而丢失；前端仍可通过 capability join。
                        warn!(
                            error = %err,
                            call_id = %cs.call_id,
                            "call_signal_enrichment: accept succeeded but join_token enrichment failed"
                        );
                        cs.ext.insert("flare_rtc_enrich".into(), "degraded".into());
                        cs.ext.insert(
                            "flare_rtc_enrich_error".into(),
                            "join_token_unavailable".into(),
                        );
                    }
                }
            }
            Some(Signal::Reject(r)) => {
                let payload = reject_payload(cs, &r)?;
                let _ = self
                    .dispatch_capability_json(
                        ctx,
                        CAP_CALL_REJECT,
                        tenant_id,
                        &cs.from_user_id,
                        &conversation_for_dispatch,
                        rid,
                        payload,
                    )
                    .await?;
            }
            Some(Signal::Hangup(h)) => {
                let payload = hangup_payload(cs, &h)?;
                let _ = self
                    .dispatch_capability_json(
                        ctx,
                        CAP_CALL_END,
                        tenant_id,
                        &cs.from_user_id,
                        &conversation_for_dispatch,
                        rid,
                        payload,
                    )
                    .await?;
            }
            Some(Signal::Renegotiate(_)) => {
                self.handle_renegotiate(ctx, tenant_id, &conversation_for_dispatch, rid, cs)
                    .await?;
            }
            Some(Signal::IceCandidate(ic)) => {
                self.handle_ice_candidate(ctx, tenant_id, &conversation_for_dispatch, rid, cs, &ic)
                    .await?;
            }
            _ => {
                debug!("call_signal_enrichment: skip non-rtc kind");
            }
        }

        Ok(())
    }

    async fn handle_renegotiate(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        conversation_id: &str,
        rid: String,
        cs: &mut CallSignalEvent,
    ) -> Result<()> {
        let has_media_transport = cs
            .transport
            .as_ref()
            .map(|t| !t.room_id.trim().is_empty() && !t.peer_id.trim().is_empty())
            .unwrap_or(false);
        if !has_media_transport {
            debug!("call_signal_enrichment: renegotiate passthrough (no media transport context)");
            return Ok(());
        }

        let user_id = cs.from_user_id.as_str();
        if user_id.trim().is_empty() || cs.call_id.trim().is_empty() {
            warn!("call_signal_enrichment: renegotiate skip — missing from_user_id/call_id");
            return Ok(());
        }

        let room_id = cs
            .transport
            .as_ref()
            .map(|t| t.room_id.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_default();
        let peer_id = cs
            .transport
            .as_ref()
            .map(|t| t.peer_id.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_default();
        let media_session_id = cs
            .transport
            .as_ref()
            .and_then(|t| t.media_session_id.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        if room_id.is_empty() || peer_id.is_empty() {
            debug!(
                "call_signal_enrichment: renegotiate passthrough (missing room_id/peer_id in transport)"
            );
            return Ok(());
        }

        let sdp_type = cs
            .ext
            .get(EXT_SDP_TYPE)
            .map(|s| s.trim().to_ascii_lowercase())
            .unwrap_or_default();
        let sdp = cs
            .ext
            .get(EXT_SDP)
            .map(|s| s.trim().to_string())
            .unwrap_or_default();

        // 常用设备开关（摄像头/麦克风）统一经 `rtc.media.publisher.mute` 下发。
        // 该控制是“软开关”：关闭会触发轨道下线；重新开启由终端重发 publish。
        if let Some((mute_audio, mute_video)) = parse_media_toggle_from_ext(&cs.ext) {
            let _ = self
                .dispatch_capability_json(
                    ctx,
                    CAP_CALL_MEDIA_PUBLISHER_STATE,
                    tenant_id,
                    user_id,
                    conversation_id,
                    format!("{rid}-media-mute"),
                    json!({
                        "room_id": room_id.clone(),
                        "publisher_peer_id": peer_id.clone(),
                        "mute_audio": mute_audio,
                        "mute_video": mute_video
                    }),
                )
                .await?;
        }

        if let Some(payload) = parse_subscription_payload_from_ext(&cs.ext, &room_id, &peer_id) {
            let _ = self
                .dispatch_capability_json(
                    ctx,
                    CAP_CALL_MEDIA_SUBSCRIPTION_SET,
                    tenant_id,
                    user_id,
                    conversation_id,
                    format!("{rid}-media-subscription"),
                    payload,
                )
                .await?;
        }

        if let Some(payload) = parse_simulcast_payload_from_ext(&cs.ext, &room_id, &peer_id) {
            let _ = self
                .dispatch_capability_json(
                    ctx,
                    CAP_CALL_MEDIA_SIMULCAST_LAYER_SET,
                    tenant_id,
                    user_id,
                    conversation_id,
                    format!("{rid}-media-simulcast"),
                    payload,
                )
                .await?;
        }

        if let Some(query_peer_id) = non_empty_ext_str(&cs.ext, EXT_NQ_QUERY_PEER_ID) {
            let quality = self
                .dispatch_capability_json(
                    ctx,
                    CAP_CALL_MEDIA_NETWORK_QUALITY_GET,
                    tenant_id,
                    user_id,
                    conversation_id,
                    format!("{rid}-media-network-quality"),
                    json!({
                        "room_id": room_id.clone(),
                        "peer_id": query_peer_id
                    }),
                )
                .await?;
            write_network_quality_to_ext(&mut cs.ext, &quality);
        }

        if sdp.is_empty() {
            debug!("call_signal_enrichment: renegotiate skip SDP flow — no SDP in ext");
            return Ok(());
        }

        if sdp_type == "offer" {
            let offer_json = self
                .dispatch_capability_json(
                    ctx,
                    CAP_CALL_SIGNAL_SDP_OFFER,
                    tenant_id,
                    user_id,
                    conversation_id,
                    format!("{rid}-media-offer"),
                    json!({
                        "room_id": room_id,
                        "peer_id": peer_id,
                        "sdp_offer": sdp
                    }),
                )
                .await?;
            if let Some(answer) = offer_json.get("sdp_answer").and_then(|v| v.as_str()) {
                cs.ext.insert(EXT_SDP_TYPE.into(), "answer".into());
                cs.ext.insert(EXT_SDP.into(), answer.to_string());
            }
        } else if sdp_type == "answer" {
            let _ = self
                .dispatch_capability_json(
                    ctx,
                    CAP_CALL_SIGNAL_SDP_ANSWER,
                    tenant_id,
                    user_id,
                    conversation_id,
                    format!("{rid}-media-answer"),
                    json!({
                        "room_id": room_id,
                        "peer_id": peer_id,
                        "sdp_answer": sdp
                    }),
                )
                .await?;
        } else {
            warn!(
                sdp_type = %sdp_type,
                "call_signal_enrichment: renegotiate skip — unsupported sdp_type"
            );
            return Ok(());
        }

        let t = cs.transport.get_or_insert_with(Default::default);
        t.room_id = room_id;
        t.peer_id = peer_id;
        if t.media_session_id.is_none() {
            t.media_session_id = media_session_id;
        }
        Ok(())
    }

    async fn handle_ice_candidate(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        conversation_id: &str,
        rid: String,
        cs: &mut CallSignalEvent,
        ic: &flare_proto::common::CallIceCandidatePayload,
    ) -> Result<()> {
        // P2P 模式（无媒体传输上下文）下，ICE 必须在客户端间透传。
        // 若服务端在此劫持并转发到媒体后端，会与 P2P 协商冲突并导致远端黑屏/无声。
        let has_media_transport = cs
            .transport
            .as_ref()
            .map(|t| !t.room_id.trim().is_empty() && !t.peer_id.trim().is_empty())
            .unwrap_or(false);
        if !has_media_transport {
            debug!("call_signal_enrichment: ice passthrough (no media transport context)");
            return Ok(());
        }

        let user_id = cs.from_user_id.as_str();
        if user_id.trim().is_empty() || cs.call_id.trim().is_empty() {
            warn!("call_signal_enrichment: ice skip — missing from_user_id/call_id");
            return Ok(());
        }

        let room_id = cs
            .transport
            .as_ref()
            .map(|t| t.room_id.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_default();

        let peer_id = cs
            .transport
            .as_ref()
            .map(|t| t.peer_id.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_default();
        if room_id.is_empty() || peer_id.is_empty() {
            debug!(
                "call_signal_enrichment: ice passthrough (missing room_id/peer_id in transport)"
            );
            return Ok(());
        }

        let Some(candidate_json) = ic
            .candidate_json
            .as_ref()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
        else {
            warn!("call_signal_enrichment: ice skip — candidate_json required");
            return Ok(());
        };
        let _ = self
            .dispatch_capability_json(
                ctx,
                CAP_CALL_SIGNAL_ICE,
                tenant_id,
                user_id,
                conversation_id,
                format!("{rid}-media-ice"),
                json!({
                    "room_id": room_id.clone(),
                    "peer_id": peer_id.clone(),
                    "candidate_json": candidate_json
                }),
            )
            .await?;
        let t = cs.transport.get_or_insert_with(Default::default);
        t.room_id = room_id;
        t.peer_id = peer_id;
        Ok(())
    }
}

fn invite_capability_id(inv: &flare_proto::common::CallInvite) -> &'static str {
    let types = inv
        .offered_media
        .as_ref()
        .map(|m| m.types.as_slice())
        .unwrap_or_default();
    let has_video = types.iter().any(|t| *t == CallMediaType::Video as i32);
    if has_video {
        CAP_CALL_VIDEO_START
    } else {
        CAP_CALL_AUDIO_START
    }
}

fn string_map_to_json(m: &HashMap<String, String>) -> Value {
    let mut obj = Map::new();
    for (k, v) in m {
        obj.insert(k.clone(), Value::String(v.clone()));
    }
    Value::Object(obj)
}

fn invite_payload_json(cs: &CallSignalEvent) -> Value {
    let mut base = Map::new();
    if !cs.call_id.is_empty() {
        base.insert("call_id".into(), Value::String(cs.call_id.clone()));
    }
    if !cs.ext.is_empty() {
        base.insert("ext".into(), string_map_to_json(&cs.ext));
    }
    Value::Object(base)
}

fn call_id_payload(cs: &CallSignalEvent) -> Result<Value> {
    let id = cs.call_id.trim();
    if id.is_empty() {
        return Err(ErrorBuilder::new(
            ErrorCode::InvalidParameter,
            "call_id required for rtc accept/hangup",
        )
        .build_error());
    }
    Ok(json!({ "call_id": id }))
}

fn reject_payload(cs: &CallSignalEvent, r: &CallReject) -> Result<Value> {
    let mut p = call_id_payload(cs)?;
    if let Value::Object(ref mut obj) = p
        && !r.reason.is_empty()
    {
        obj.insert("reason".into(), Value::String(r.reason.clone()));
    }
    Ok(p)
}

fn hangup_payload(cs: &CallSignalEvent, h: &CallHangup) -> Result<Value> {
    let mut p = call_id_payload(cs)?;
    if let Value::Object(ref mut obj) = p
        && let Some(close) = h.close_room_if_vacant
    {
        obj.insert("close_room_if_vacant".into(), Value::Bool(close));
    }
    Ok(p)
}

fn ext_bool(ext: &HashMap<String, String>, key: &str) -> Option<bool> {
    ext.get(key)
        .and_then(|raw| match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
}

fn parse_media_toggle_from_ext(ext: &HashMap<String, String>) -> Option<(bool, bool)> {
    let camera_enabled = ext_bool(ext, EXT_CAMERA_ENABLED);
    let microphone_enabled = ext_bool(ext, EXT_MICROPHONE_ENABLED);
    if camera_enabled.is_none() && microphone_enabled.is_none() {
        return None;
    }
    let mute_audio = microphone_enabled.map(|v| !v).unwrap_or(false);
    let mute_video = camera_enabled.map(|v| !v).unwrap_or(false);
    Some((mute_audio, mute_video))
}

fn non_empty_ext_str(ext: &HashMap<String, String>, key: &str) -> Option<String> {
    ext.get(key)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn ext_u32(ext: &HashMap<String, String>, key: &str) -> Option<u32> {
    ext.get(key).and_then(|s| s.trim().parse::<u32>().ok())
}

fn parse_subscription_payload_from_ext(
    ext: &HashMap<String, String>,
    room_id: &str,
    default_subscriber_peer_id: &str,
) -> Option<Value> {
    let track_id = non_empty_ext_str(ext, EXT_SUB_TRACK_ID)?;
    let enable = ext_bool(ext, EXT_SUB_ENABLE).unwrap_or(true);
    let subscriber_peer_id = non_empty_ext_str(ext, EXT_SUB_SUBSCRIBER_PEER_ID)
        .unwrap_or_else(|| default_subscriber_peer_id.to_string());
    let media = non_empty_ext_str(ext, EXT_SUB_MEDIA);
    let preferred_layer = non_empty_ext_str(ext, EXT_SUB_PREFERRED_LAYER);
    let priority = ext_u32(ext, EXT_SUB_PRIORITY).unwrap_or(0);
    Some(json!({
        "room_id": room_id,
        "subscriber_peer_id": subscriber_peer_id,
        "track_id": track_id,
        "enable": enable,
        "media": media,
        "preferred_layer": preferred_layer,
        "priority": priority
    }))
}

fn parse_simulcast_payload_from_ext(
    ext: &HashMap<String, String>,
    room_id: &str,
    default_subscriber_peer_id: &str,
) -> Option<Value> {
    let track_id = non_empty_ext_str(ext, EXT_SIMULCAST_TRACK_ID)?;
    let layer = non_empty_ext_str(ext, EXT_SIMULCAST_LAYER)?;
    let subscriber_peer_id = non_empty_ext_str(ext, EXT_SIMULCAST_SUBSCRIBER_PEER_ID)
        .unwrap_or_else(|| default_subscriber_peer_id.to_string());
    Some(json!({
        "room_id": room_id,
        "subscriber_peer_id": subscriber_peer_id,
        "track_id": track_id,
        "layer": layer
    }))
}

fn write_network_quality_to_ext(ext: &mut HashMap<String, String>, quality: &Value) {
    if let Some(has_data) = quality.get("has_data").and_then(|v| v.as_bool()) {
        ext.insert(EXT_NQ_HAS_DATA.into(), has_data.to_string());
    }
    if let Some(upstream_score) = quality.get("upstream_score").and_then(|v| v.as_u64()) {
        ext.insert(EXT_NQ_UPSTREAM_SCORE.into(), upstream_score.to_string());
    }
    if let Some(downstream_score) = quality.get("downstream_score").and_then(|v| v.as_u64()) {
        ext.insert(EXT_NQ_DOWNSTREAM_SCORE.into(), downstream_score.to_string());
    }
    if let Some(rtt_ms) = quality.get("rtt_ms").and_then(|v| v.as_u64()) {
        ext.insert(EXT_NQ_RTT_MS.into(), rtt_ms.to_string());
    }
    if let Some(packet_loss_ratio) = quality.get("packet_loss_ratio").and_then(|v| v.as_f64()) {
        ext.insert(
            EXT_NQ_PACKET_LOSS_RATIO.into(),
            packet_loss_ratio.to_string(),
        );
    }
}

fn merge_transport_from_capability_json(cs: &mut CallSignalEvent, v: &Value) {
    let t = cs.transport.get_or_insert_with(Default::default);
    if let Some(rid) = v.get("room_id").and_then(|x| x.as_str())
        && !rid.is_empty()
    {
        t.room_id = rid.to_string();
    }
    if let Some(pid) = v.get("peer_id").and_then(|x| x.as_str())
        && !pid.is_empty()
    {
        t.peer_id = pid.to_string();
    }
    if let Some(ms) = v.get("media_session_id").and_then(|x| x.as_str())
        && !ms.is_empty()
    {
        t.media_session_id = Some(ms.to_string());
    }
    if let Some(ws) = v.get("signaling_ws_base").and_then(|x| x.as_str())
        && !ws.is_empty()
    {
        t.signaling_ws_base = Some(ws.to_string());
    }
    let instance_id = v
        .get("sfu_instance_id")
        .or_else(|| v.get("instance_id"))
        .and_then(|x| x.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if let Some(instance_id) = instance_id {
        t.instance_id = Some(instance_id.to_string());
    }
}

fn apply_create_result_to_call_signal(cs: &mut CallSignalEvent, v: &Value) -> Result<()> {
    if v.is_null() {
        return Err(flare_err!(
            ErrorCode::InternalError,
            "capability create result empty"
        ));
    }
    if let Some(cid) = v.get("call_id").and_then(|x| x.as_str()) {
        cs.call_id = cid.to_string();
    }
    merge_transport_from_capability_json(cs, v);
    Ok(())
}

fn apply_accept_result_to_call_signal(cs: &mut CallSignalEvent, v: &Value) -> Result<()> {
    if v.is_null() {
        return Err(flare_err!(
            ErrorCode::InternalError,
            "capability accept result empty"
        ));
    }
    if let Some(cid) = v.get("call_id").and_then(|x| x.as_str())
        && !cid.is_empty()
    {
        cs.call_id = cid.to_string();
    }
    merge_transport_from_capability_json(cs, v);
    Ok(())
}

fn apply_join_token_result_to_call_signal(cs: &mut CallSignalEvent, v: &Value) -> Result<()> {
    if v.is_null() {
        return Err(flare_err!(
            ErrorCode::InternalError,
            "capability join_token result empty"
        ));
    }
    merge_transport_from_capability_json(cs, v);
    if let Some(token) = v
        .get("sfu_join_token")
        .or_else(|| v.get("token"))
        .and_then(|x| x.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        cs.ext.insert("sfu_join_token".into(), token.to_string());
    }
    if let Some(ttl) = v
        .get("sfu_join_token_ttl_seconds")
        .or_else(|| v.get("ttl_seconds"))
        .and_then(|x| x.as_u64())
    {
        cs.ext
            .insert("sfu_join_token_ttl_seconds".into(), ttl.to_string());
    }
    Ok(())
}
