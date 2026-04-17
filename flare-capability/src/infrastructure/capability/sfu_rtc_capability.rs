//! 基于 flare-sfu 的 `RtcCapability` 实现（由原 plugin `SfuRemotePluginInvoker` 迁入）。

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use flare_sfu::domain::signaling::SignalingHandler;
use flare_sfu::interface::plugin::SfuPlugin;
use flare_sfu::{ParticipantId, RoomId, TrackId, UserId};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use flare_core_base::context::Ctx;

use crate::domain::capability::{
    AcceptCallRequest, AcceptCallResponse, CapabilityError, CreateCallRequest, CreateCallResponse,
    GetJoinTokenRequest, GetJoinTokenResponse, HangupCallRequest, HangupCallResponse,
    ListParticipantsRequest, ListParticipantsResponse, RejectCallRequest, RejectCallResponse,
    Result, RtcCapability,
};

#[derive(Debug, Clone)]
struct CallSession {
    room_id: RoomId,
    owner_participant_id: ParticipantId,
    media: String,
    #[allow(dead_code)]
    owner_track_id: Option<TrackId>,
}

#[derive(Debug, Deserialize)]
struct StartCallPayload {
    call_id: Option<String>,
    codec: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CallRefPayload {
    call_id: String,
}

pub struct SfuRtcCapability {
    sfu: Arc<SfuPlugin>,
    calls: DashMap<String, CallSession>,
}

impl SfuRtcCapability {
    pub fn new(sfu: Arc<SfuPlugin>) -> Self {
        Self {
            sfu,
            calls: DashMap::new(),
        }
    }

    fn media_type_from_request(
        media: Option<&str>,
        fallback_video: bool,
    ) -> flare_sfu::domain::MediaType {
        match media.map(|s| s.to_ascii_lowercase()).as_deref() {
            Some("audio") => flare_sfu::domain::MediaType::Audio,
            _ if !fallback_video => flare_sfu::domain::MediaType::Audio,
            _ => flare_sfu::domain::MediaType::Video,
        }
    }
}

#[async_trait]
impl RtcCapability for SfuRtcCapability {
    fn id(&self) -> &str {
        "sfu.rtc"
    }

    async fn create_call(&self, ctx: &Ctx, req: &CreateCallRequest) -> Result<CreateCallResponse> {
        let payload: StartCallPayload = if req.ext.is_null() {
            StartCallPayload {
                call_id: None,
                codec: None,
            }
        } else {
            serde_json::from_value(req.ext.clone()).map_err(|e| {
                CapabilityError::System(format!("invalid create_call ext payload: {e}"))
            })?
        };
        let signaling = self.sfu.signaling_handler();
        let user_id = req.initiator_user_id.clone();
        let call_id = payload
            .call_id
            .clone()
            .unwrap_or_else(|| format!("call-{}", Uuid::new_v4()));
        let video = !matches!(
            req.media.as_deref().map(str::to_ascii_lowercase).as_deref(),
            Some("audio")
        );
        let media_type = Self::media_type_from_request(req.media.as_deref(), video);
        let create = signaling
            .handle_create_room(
                ctx,
                flare_sfu::domain::signaling::CreateRoomReq {
                    config: flare_sfu::domain::RoomConfig::default(),
                    im_binding: None,
                },
            )
            .await
            .map_err(|e| CapabilityError::System(e.to_string()))?;
        let join = signaling
            .handle_join_room(
                ctx,
                flare_sfu::domain::signaling::JoinRoomReq {
                    room_id: create.room_id.clone(),
                    user_id: UserId::new(user_id).map_err(|e| CapabilityError::System(e.to_string()))?,
                    device_id: None,
                },
            )
            .await
            .map_err(|e| CapabilityError::System(e.to_string()))?;
        let publish = signaling
            .handle_publish(
                ctx,
                flare_sfu::domain::signaling::PublishReq {
                    room_id: create.room_id.clone(),
                    participant_id: join.participant_id.clone(),
                    media_type,
                    source: flare_sfu::domain::TrackSource::Camera,
                    codec: flare_sfu::domain::Codec::new(payload.codec.clone().unwrap_or_else(|| {
                        if media_type == flare_sfu::domain::MediaType::Audio {
                            "OPUS".into()
                        } else {
                            "VP8".into()
                        }
                    })),
                    stream_id: None,
                    sdp: None,
                },
            )
            .await
            .map_err(|e| CapabilityError::System(e.to_string()))?;

        self.calls.insert(
            call_id.clone(),
            CallSession {
                room_id: create.room_id.clone(),
                owner_participant_id: join.participant_id.clone(),
                media: if media_type == flare_sfu::domain::MediaType::Audio {
                    "audio".into()
                } else {
                    "video".into()
                },
                owner_track_id: Some(publish.track_id.clone()),
            },
        );

        Ok(CreateCallResponse {
            call_id,
            room_id: create.room_id.to_string(),
            ext: json!({
                "owner_participant_id": join.participant_id.to_string(),
                "owner_track_id": publish.track_id.to_string(),
                "media": if media_type == flare_sfu::domain::MediaType::Audio { "audio" } else { "video" },
            }),
        })
    }

    async fn accept_call(&self, ctx: &Ctx, req: &AcceptCallRequest) -> Result<AcceptCallResponse> {
        let call_id = serde_json::from_value::<CallRefPayload>(req.ext.clone())
            .ok()
            .filter(|p| !p.call_id.is_empty())
            .map(|p| p.call_id)
            .unwrap_or_else(|| req.call_id.clone());
        let call = self.calls.get(&call_id).ok_or_else(|| {
            CapabilityError::System(format!("call_id {call_id} not found"))
        })?;
        let signaling = self.sfu.signaling_handler();
        let join = signaling
            .handle_join_room(
                ctx,
                flare_sfu::domain::signaling::JoinRoomReq {
                    room_id: call.room_id.clone(),
                    user_id: UserId::new(req.user_id.clone())
                        .map_err(|e| CapabilityError::System(e.to_string()))?,
                    device_id: None,
                },
            )
            .await
            .map_err(|e| CapabilityError::System(e.to_string()))?;

        Ok(AcceptCallResponse {
            call_id,
            ext: json!({
                "room_id": call.room_id.to_string(),
                "participant_id": join.participant_id.to_string(),
                "media": call.media,
            }),
        })
    }

    async fn reject_call(&self, ctx: &Ctx, req: &RejectCallRequest) -> Result<RejectCallResponse> {
        let _ = self.hangup_call(ctx, &HangupCallRequest {
            tenant_id: req.tenant_id.clone(),
            request_id: req.request_id.clone(),
            call_id: req.call_id.clone(),
            user_id: req.user_id.clone(),
            ext: req.ext.clone(),
        }).await;
        Ok(RejectCallResponse {
            call_id: req.call_id.clone(),
            ext: json!({ "rejected": true }),
        })
    }

    async fn hangup_call(&self, ctx: &Ctx, req: &HangupCallRequest) -> Result<HangupCallResponse> {
        let call_id = serde_json::from_value::<CallRefPayload>(req.ext.clone())
            .ok()
            .filter(|p| !p.call_id.is_empty())
            .map(|p| p.call_id)
            .unwrap_or_else(|| req.call_id.clone());
        if let Some((_, call)) = self.calls.remove(&call_id) {
            let signaling = self.sfu.signaling_handler();
            let _ = signaling
                .handle_leave_room(
                    ctx,
                    flare_sfu::domain::signaling::LeaveRoomReq {
                        room_id: call.room_id.clone(),
                        participant_id: call.owner_participant_id.clone(),
                    },
                )
                .await;
        }
        Ok(HangupCallResponse {
            call_id,
            ext: json!({ "ended": true }),
        })
    }

    async fn get_join_token(
        &self,
        _ctx: &Ctx,
        req: &GetJoinTokenRequest,
    ) -> Result<GetJoinTokenResponse> {
        Ok(GetJoinTokenResponse {
            token: format!("stub-token-{}", req.call_id),
            ttl_seconds: 3600,
            ext: json!({}),
        })
    }

    async fn list_participants(
        &self,
        _ctx: &Ctx,
        req: &ListParticipantsRequest,
    ) -> Result<ListParticipantsResponse> {
        Ok(ListParticipantsResponse {
            participants: vec![],
            ext: json!({ "call_id": req.call_id }),
        })
    }
}
