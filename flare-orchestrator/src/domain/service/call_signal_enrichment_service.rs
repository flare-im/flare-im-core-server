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
const EXT_SDP_TYPE_CAMEL: &str = "flareSdpType";
const EXT_SDP: &str = "flare_sdp";
const EXT_SDP_CAMEL: &str = "flareSdp";

/// `EVENT_CALL_SIGNAL` enrich 领域服务：
/// 负责把业务信令映射到 capability `rtc.*` 调度（含 `rtc.sfu.*`）。
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
    /// - Accept：`rtc.call.accept` + 可选 `rtc.call.join_token`
    /// - Reject：`rtc.call.reject`
    /// - Hangup：`rtc.call.end`
    /// - Renegotiate：`rtc.sfu.handle_sdp_offer/answer`
    /// - IceCandidate：`rtc.sfu.add_ice_candidate`
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
                        "rtc.call.accept",
                        tenant_id,
                        &cs.from_user_id,
                        &conversation_for_dispatch,
                        rid.clone(),
                        payload,
                    )
                    .await?;
                apply_accept_result_to_call_signal(cs, &result)?;

                let join_token_result = self
                    .dispatch_capability_json(
                        ctx,
                        "rtc.call.join_token",
                        tenant_id,
                        &cs.from_user_id,
                        &conversation_for_dispatch,
                        format!("{rid}-join-token"),
                        json!({ "call_id": cs.call_id }),
                    )
                    .await;
                match join_token_result {
                    Ok(v) => {
                        if let Err(e) = apply_join_token_to_call_signal(cs, &v) {
                            warn!(error = %e, "call_signal_enrichment: join_token result parse failed");
                        }
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            "call_signal_enrichment: join_token dispatch failed (accept still applied)"
                        );
                    }
                }
            }
            Some(Signal::Reject(r)) => {
                let payload = reject_payload(cs, &r)?;
                let _ = self
                    .dispatch_capability_json(
                        ctx,
                        "rtc.call.reject",
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
                        "rtc.call.end",
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
        // P2P 模式（无 SFU transport 上下文）下，renegotiate 应原样透传给对端，
        // 不能在服务端改写为 SFU answer，否则会破坏主/被叫协商状态机。
        let has_sfu_transport = cs
            .transport
            .as_ref()
            .map(|t| !t.room_id.trim().is_empty() && !t.peer_id.trim().is_empty())
            .unwrap_or(false);
        if !has_sfu_transport {
            debug!("call_signal_enrichment: renegotiate passthrough (no sfu transport context)");
            return Ok(());
        }

        let user_id = cs.from_user_id.as_str();
        if user_id.trim().is_empty() || cs.call_id.trim().is_empty() {
            warn!("call_signal_enrichment: renegotiate skip — missing from_user_id/call_id");
            return Ok(());
        }

        let mut room_id = cs
            .transport
            .as_ref()
            .map(|t| t.room_id.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_default();
        let mut peer_id = cs
            .transport
            .as_ref()
            .map(|t| t.peer_id.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_default();
        let mut media_session_id = cs
            .transport
            .as_ref()
            .and_then(|t| t.media_session_id.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        if room_id.is_empty() {
            let join_token_json = self
                .dispatch_capability_json(
                    ctx,
                    "rtc.call.join_token",
                    tenant_id,
                    user_id,
                    conversation_id,
                    format!("{rid}-sfu-join-token"),
                    json!({ "call_id": cs.call_id }),
                )
                .await?;
            if let Some(r) = join_token_json.get("room_id").and_then(|v| v.as_str()) {
                room_id = r.to_string();
            }
        }

        if room_id.is_empty() {
            warn!("call_signal_enrichment: renegotiate skip — room_id still empty");
            return Ok(());
        }

        if peer_id.is_empty() {
            let join_room_json = self
                .dispatch_capability_json(
                    ctx,
                    "rtc.sfu.join_room",
                    tenant_id,
                    user_id,
                    conversation_id,
                    format!("{rid}-sfu-join-room"),
                    json!({
                        "call_id": cs.call_id,
                        "room_id": room_id,
                        "role": "participant"
                    }),
                )
                .await?;
            if let Some(p) = join_room_json.get("peer_id").and_then(|v| v.as_str()) {
                peer_id = p.to_string();
            }
            if media_session_id.is_none() {
                media_session_id = join_room_json
                    .get("session_id")
                    .and_then(|v| v.as_str())
                    .map(ToOwned::to_owned);
            }
        }

        if peer_id.is_empty() {
            warn!("call_signal_enrichment: renegotiate skip — peer_id still empty");
            return Ok(());
        }

        let sdp_type = cs
            .ext
            .get(EXT_SDP_TYPE)
            .or_else(|| cs.ext.get(EXT_SDP_TYPE_CAMEL))
            .map(|s| s.trim().to_ascii_lowercase())
            .unwrap_or_default();
        let sdp = cs
            .ext
            .get(EXT_SDP)
            .or_else(|| cs.ext.get(EXT_SDP_CAMEL))
            .map(|s| s.trim().to_string())
            .unwrap_or_default();

        if sdp.is_empty() {
            warn!("call_signal_enrichment: renegotiate skip — SDP body missing in ext");
            return Ok(());
        }

        if sdp_type == "offer" {
            let offer_json = self
                .dispatch_capability_json(
                    ctx,
                    "rtc.sfu.handle_sdp_offer",
                    tenant_id,
                    user_id,
                    conversation_id,
                    format!("{rid}-sfu-offer"),
                    json!({
                        "room_id": room_id,
                        "peer_id": peer_id,
                        "sdp_offer": sdp
                    }),
                )
                .await?;
            if let Some(answer) = offer_json.get("sdp_answer").and_then(|v| v.as_str()) {
                cs.ext.insert(EXT_SDP_TYPE.into(), "answer".into());
                cs.ext.insert(EXT_SDP_TYPE_CAMEL.into(), "answer".into());
                cs.ext.insert(EXT_SDP.into(), answer.to_string());
                cs.ext.insert(EXT_SDP_CAMEL.into(), answer.to_string());
            }
        } else if sdp_type == "answer" {
            let _ = self
                .dispatch_capability_json(
                    ctx,
                    "rtc.sfu.handle_sdp_answer",
                    tenant_id,
                    user_id,
                    conversation_id,
                    format!("{rid}-sfu-answer"),
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
        // P2P 模式（无 SFU transport 上下文）下，ICE 必须在客户端间透传。
        // 若服务端在此劫持并转发到 SFU，会与 P2P 协商冲突并导致远端黑屏/无声。
        let has_sfu_transport = cs
            .transport
            .as_ref()
            .map(|t| !t.room_id.trim().is_empty() && !t.peer_id.trim().is_empty())
            .unwrap_or(false);
        if !has_sfu_transport {
            debug!("call_signal_enrichment: ice passthrough (no sfu transport context)");
            return Ok(());
        }

        let user_id = cs.from_user_id.as_str();
        if user_id.trim().is_empty() || cs.call_id.trim().is_empty() {
            warn!("call_signal_enrichment: ice skip — missing from_user_id/call_id");
            return Ok(());
        }

        let mut room_id = cs
            .transport
            .as_ref()
            .map(|t| t.room_id.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_default();
        if room_id.is_empty() {
            let join_token_json = self
                .dispatch_capability_json(
                    ctx,
                    "rtc.call.join_token",
                    tenant_id,
                    user_id,
                    conversation_id,
                    format!("{rid}-ice-join-token"),
                    json!({ "call_id": cs.call_id }),
                )
                .await?;
            if let Some(r) = join_token_json.get("room_id").and_then(|v| v.as_str()) {
                room_id = r.to_string();
            }
        }
        if room_id.is_empty() {
            warn!("call_signal_enrichment: ice skip — room_id still empty");
            return Ok(());
        }

        let mut peer_id = cs
            .transport
            .as_ref()
            .map(|t| t.peer_id.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_default();
        if peer_id.is_empty() {
            let join_room_json = self
                .dispatch_capability_json(
                    ctx,
                    "rtc.sfu.join_room",
                    tenant_id,
                    user_id,
                    conversation_id,
                    format!("{rid}-ice-join-room"),
                    json!({
                        "call_id": cs.call_id,
                        "room_id": room_id,
                        "role": "participant"
                    }),
                )
                .await?;
            if let Some(p) = join_room_json.get("peer_id").and_then(|v| v.as_str()) {
                peer_id = p.to_string();
            }
        }
        if peer_id.is_empty() {
            warn!("call_signal_enrichment: ice skip — peer_id still empty");
            return Ok(());
        }

        let candidate_json = ic
            .candidate_json
            .as_ref()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| {
                json!({
                    "candidate": ic.candidate,
                    "sdpMid": ic.sdp_mid,
                    "sdpMLineIndex": ic.sdp_mline_index
                })
                .to_string()
            });
        let _ = self
            .dispatch_capability_json(
                ctx,
                "rtc.sfu.add_ice_candidate",
                tenant_id,
                user_id,
                conversation_id,
                format!("{rid}-sfu-ice"),
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
        "rtc.call.video"
    } else {
        "rtc.call.audio"
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

fn json_u64(v: &Value, key: &str) -> Option<u64> {
    v.get(key).and_then(|x| {
        x.as_u64()
            .or_else(|| x.as_i64().filter(|&i| i >= 0).map(|i| i as u64))
    })
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

fn apply_join_token_to_call_signal(cs: &mut CallSignalEvent, v: &Value) -> Result<()> {
    if v.is_null() {
        return Err(flare_err!(
            ErrorCode::InternalError,
            "capability join_token result empty"
        ));
    }
    if let Some(tok) = v.get("token").and_then(|x| x.as_str())
        && !tok.is_empty()
    {
        cs.ext.insert("sfu_join_token".into(), tok.to_string());
    }
    if let Some(ttl) = json_u64(v, "ttl_seconds") {
        cs.ext
            .insert("sfu_join_token_ttl_seconds".into(), ttl.to_string());
    }
    merge_transport_from_capability_json(cs, v);
    Ok(())
}
