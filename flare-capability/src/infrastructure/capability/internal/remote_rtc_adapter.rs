//! 通过 `flare.sfu.control.v1.SfuControl` 访问独立媒体控制面，实现 `RtcCapability`。

use std::collections::HashMap;

use async_trait::async_trait;
use flare_core_base::context::Ctx;
use flare_grpc_proto::sfu_control::sfu_control_client::SfuControlClient;
use flare_grpc_proto::sfu_control::{
    AcceptCallRequest as ProtoAcceptCallRequest, AddIceCandidateRequest as ProtoAddIce,
    CreateRoomRequest, GetJoinTokenRequest as ProtoJoin, GetPeerNetworkQualityRequest,
    GetRoomStateRequest, HandleSdpAnswerRequest as ProtoHandleAnswer,
    HandleSdpOfferRequest as ProtoHandleOffer, HangupCallRequest as ProtoHangup,
    JoinRoomRequest as ProtoJoinRoom, LeaveRoomRequest as ProtoLeaveRoom, MediaKind,
    SetPublisherMuteRequest as ProtoSetPublisherMute,
    SetSimulcastLayerRequest as ProtoSetSimulcastLayer,
    SetSubscriptionRequest as ProtoSetSubscription, SimulcastLayer,
};
use flare_server_core::client::set_context_metadata;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tonic::Request;
use tonic::transport::Channel;
use uuid::Uuid;

use crate::domain::capability::{
    AcceptCallRequest, AcceptCallResponse, AddIceCandidateRequest, AddIceCandidateResponse,
    CapabilityError, CreateCallRequest, CreateCallResponse, GetJoinTokenRequest,
    GetJoinTokenResponse, HandleSdpAnswerRequest, HandleSdpAnswerResponse, HandleSdpOfferRequest,
    HandleSdpOfferResponse, HangupCallRequest, HangupCallResponse, ListParticipantsRequest,
    ListParticipantsResponse, MediaGetNetworkQualityRequest, MediaGetNetworkQualityResponse,
    MediaGetRoomStateRequest, MediaGetRoomStateResponse, MediaJoinTransportRequest,
    MediaJoinTransportResponse, MediaLeaveTransportRequest, MediaLeaveTransportResponse,
    MediaSetPublisherMuteRequest, MediaSetPublisherMuteResponse, MediaSetSimulcastLayerRequest,
    MediaSetSimulcastLayerResponse, MediaSetSubscriptionRequest, MediaSetSubscriptionResponse,
    RejectCallRequest, RejectCallResponse, Result, RtcCapability,
};
use crate::infrastructure::capability::plugin_channel::{
    PLUGIN_DISCOVERY_TIMEOUT, resolve_plugin_channel,
};
use crate::infrastructure::config::capability_runtime::discovery_route_authority;

#[derive(Debug, Deserialize)]
struct CallRefPayload {
    call_id: String,
}

fn status_to_capability(s: tonic::Status) -> CapabilityError {
    CapabilityError::System(format!("media-control gRPC {}: {}", s.code(), s.message()))
}

enum MediaControlTransport {
    Static(Channel),
    /// `discovery://<service_name>`，与 [`resolve_plugin_channel`] / 健康检查共用缓存。
    Discovery {
        route_authority: String,
    },
}

/// 独立媒体控制面进程的 gRPC 后端（与进程内媒体后端二选一）。
pub struct MediaControlGrpcRtcCapability {
    transport: MediaControlTransport,
}

impl MediaControlGrpcRtcCapability {
    /// 静态 endpoint 延迟建连：启动阶段不拨号，首次 RTC 调用时再连接。
    pub fn from_static_lazy(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        let ep = endpoint.into();
        let channel = Channel::from_shared(ep.clone())
            .map_err(|e| anyhow::anyhow!("invalid media-control gRPC endpoint {ep}: {e}"))?
            .connect_lazy();
        Ok(Self {
            transport: MediaControlTransport::Static(channel),
        })
    }

    pub async fn from_service_name(service_name: impl Into<String>) -> anyhow::Result<Self> {
        let service_name = service_name.into();
        Ok(Self {
            transport: MediaControlTransport::Discovery {
                route_authority: discovery_route_authority(service_name.as_str()),
            },
        })
    }

    pub async fn control_client(&self) -> Result<SfuControlClient<Channel>> {
        match &self.transport {
            MediaControlTransport::Static(channel) => Ok(SfuControlClient::new(channel.clone())),
            MediaControlTransport::Discovery { route_authority } => {
                let channel = match tokio::time::timeout(
                    PLUGIN_DISCOVERY_TIMEOUT,
                    resolve_plugin_channel(route_authority),
                )
                .await
                {
                    Ok(Ok(channel)) => channel,
                    Ok(Err(e)) => {
                        return Err(CapabilityError::System(format!(
                            "discover media-control service {}: {e}",
                            route_authority
                                .strip_prefix("discovery://")
                                .unwrap_or(route_authority)
                        )));
                    }
                    Err(_) => {
                        return Err(CapabilityError::System(format!(
                            "discover media-control service {}: timeout (plugin unavailable)",
                            route_authority
                                .strip_prefix("discovery://")
                                .unwrap_or(route_authority)
                        )));
                    }
                };
                Ok(SfuControlClient::new(channel))
            }
        }
    }

    fn resolve_call_id(req_call_id: &str, ext: &Value) -> String {
        serde_json::from_value::<CallRefPayload>(ext.clone())
            .ok()
            .filter(|p| !p.call_id.is_empty())
            .map(|p| p.call_id)
            .unwrap_or_else(|| req_call_id.to_owned())
    }

    fn resolve_room_id(call_id: &str, conversation_id: &str) -> String {
        let call_id = call_id.trim();
        if let Ok(u) = Uuid::parse_str(call_id) {
            return u.to_string();
        }
        let conversation_id = conversation_id.trim();
        if let Ok(u) = Uuid::parse_str(conversation_id) {
            return u.to_string();
        }
        if !call_id.is_empty() || !conversation_id.is_empty() {
            // 不依赖 uuid v5 feature；在无可解析 UUID 时退化为随机 room_id。
            return Uuid::new_v4().to_string();
        }
        Uuid::new_v4().to_string()
    }

    fn parse_media_kind(media: Option<&str>) -> MediaKind {
        match media
            .map(str::trim)
            .map(|s| s.to_ascii_lowercase())
            .as_deref()
        {
            Some("audio") => MediaKind::Audio,
            Some("video") => MediaKind::Video,
            _ => MediaKind::Unspecified,
        }
    }

    fn parse_simulcast_layer(layer: Option<&str>) -> SimulcastLayer {
        match layer
            .map(str::trim)
            .map(|s| s.to_ascii_lowercase())
            .as_deref()
        {
            Some("high") => SimulcastLayer::High,
            Some("medium") => SimulcastLayer::Medium,
            Some("low") => SimulcastLayer::Low,
            _ => SimulcastLayer::Unspecified,
        }
    }
}

#[async_trait]
impl RtcCapability for MediaControlGrpcRtcCapability {
    fn id(&self) -> &str {
        "media-control.rtc"
    }

    async fn create_call(&self, ctx: &Ctx, req: &CreateCallRequest) -> Result<CreateCallResponse> {
        let signal_call_id = Self::resolve_call_id("", &req.ext);
        let room_id = Self::resolve_room_id(&signal_call_id, &req.conversation_id);

        let mut metadata = HashMap::new();
        metadata.insert("tenant_id".into(), req.tenant_id.clone());
        metadata.insert("initiator_user_id".into(), req.initiator_user_id.clone());
        metadata.insert("conversation_id".into(), req.conversation_id.clone());
        if let Some(m) = &req.media {
            metadata.insert("media".into(), m.clone());
        }

        let mut grpc_req = Request::new(CreateRoomRequest {
            request_id: req.request_id.clone(),
            room_id: room_id.clone(),
            metadata,
            media_policy_json: None,
        });
        set_context_metadata(&mut grpc_req, ctx);

        let resp = self
            .control_client()
            .await?
            .create_room(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        Ok(CreateCallResponse {
            call_id: inner.call_id,
            room_id: inner.room_id.clone(),
            ext: json!({
                "created": inner.created,
                "room_id": inner.room_id,
                "signaling_ws_base": inner.signaling_ws_base,
                "sfu_instance_id": inner.instance_id,
            }),
        })
    }

    async fn accept_call(&self, ctx: &Ctx, req: &AcceptCallRequest) -> Result<AcceptCallResponse> {
        let call_id = Self::resolve_call_id(&req.call_id, &req.ext);
        let mut grpc_req = Request::new(ProtoAcceptCallRequest {
            request_id: req.request_id.clone(),
            call_id: call_id.clone(),
            user_id: req.user_id.clone(),
            tenant_id: req.tenant_id.clone(),
        });
        set_context_metadata(&mut grpc_req, ctx);

        let resp = self
            .control_client()
            .await?
            .accept_call(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        Ok(AcceptCallResponse {
            call_id: inner.call_id,
            ext: json!({
                "room_id": inner.room_id,
                "signaling_ws_base": inner.signaling_ws_base,
                "sfu_instance_id": inner.instance_id,
            }),
        })
    }

    async fn reject_call(&self, ctx: &Ctx, req: &RejectCallRequest) -> Result<RejectCallResponse> {
        let _ = self
            .hangup_call(
                ctx,
                &HangupCallRequest {
                    tenant_id: req.tenant_id.clone(),
                    request_id: req.request_id.clone(),
                    call_id: req.call_id.clone(),
                    user_id: req.user_id.clone(),
                    ext: req.ext.clone(),
                },
            )
            .await;
        Ok(RejectCallResponse {
            call_id: req.call_id.clone(),
            ext: json!({ "rejected": true }),
        })
    }

    async fn hangup_call(&self, ctx: &Ctx, req: &HangupCallRequest) -> Result<HangupCallResponse> {
        let call_id = Self::resolve_call_id(&req.call_id, &req.ext);
        let close_room = req
            .ext
            .get("close_room_if_vacant")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let mut grpc_req = Request::new(ProtoHangup {
            request_id: req.request_id.clone(),
            call_id: call_id.clone(),
            user_id: req.user_id.clone(),
            tenant_id: req.tenant_id.clone(),
            close_room_if_vacant: close_room,
        });
        set_context_metadata(&mut grpc_req, ctx);

        let resp = self
            .control_client()
            .await?
            .hangup_call(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        Ok(HangupCallResponse {
            call_id: inner.call_id,
            ext: json!({ "ended": true }),
        })
    }

    async fn get_join_token(
        &self,
        ctx: &Ctx,
        req: &GetJoinTokenRequest,
    ) -> Result<GetJoinTokenResponse> {
        let mut grpc_req = Request::new(ProtoJoin {
            request_id: req.request_id.clone(),
            call_id: req.call_id.clone(),
            user_id: req.user_id.clone(),
            tenant_id: req.tenant_id.clone(),
        });
        set_context_metadata(&mut grpc_req, ctx);

        let resp = self
            .control_client()
            .await?
            .get_join_token(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        Ok(GetJoinTokenResponse {
            token: inner.token,
            ttl_seconds: inner.ttl_seconds,
            ext: json!({
                "room_id": inner.room_id,
                "signaling_ws_base": inner.signaling_ws_base,
                "sfu_instance_id": inner.instance_id,
            }),
        })
    }

    async fn list_participants(
        &self,
        _ctx: &Ctx,
        req: &ListParticipantsRequest,
    ) -> Result<ListParticipantsResponse> {
        Ok(ListParticipantsResponse {
            participants: vec![],
            ext: json!({
                "call_id": req.call_id,
                "note": "media-control gRPC: use GetRoomSummary / events for roster (placeholder)",
            }),
        })
    }

    async fn media_join_transport(
        &self,
        ctx: &Ctx,
        req: &MediaJoinTransportRequest,
    ) -> Result<MediaJoinTransportResponse> {
        let mut grpc_req = Request::new(ProtoJoinRoom {
            request_id: req.request_id.clone(),
            room_id: req.room_id.clone(),
            call_id: req.call_id.clone(),
            user_id: req.user_id.clone(),
            tenant_id: req.tenant_id.clone(),
            role: req.role.clone(),
            peer_id: req.peer_id.clone(),
        });
        set_context_metadata(&mut grpc_req, ctx);

        let resp = self
            .control_client()
            .await?
            .join_room(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        let mut ext = Map::new();
        if !inner.room_snapshot_json.trim().is_empty() {
            ext.insert(
                "room_snapshot_json".into(),
                inner.room_snapshot_json.clone().into(),
            );
            if let Ok(v) = serde_json::from_str::<Value>(&inner.room_snapshot_json) {
                ext.insert("room_snapshot".into(), v);
            }
        }
        Ok(MediaJoinTransportResponse {
            room_id: inner.room_id,
            peer_id: inner.peer_id,
            session_id: inner.session_id,
            call_id: inner.call_id,
            ext: Value::Object(ext),
        })
    }

    async fn media_get_room_state(
        &self,
        ctx: &Ctx,
        req: &MediaGetRoomStateRequest,
    ) -> Result<MediaGetRoomStateResponse> {
        let mut grpc_req = Request::new(GetRoomStateRequest {
            request_id: req.request_id.clone(),
            room_id: req.room_id.clone(),
        });
        set_context_metadata(&mut grpc_req, ctx);

        let resp = self
            .control_client()
            .await?
            .get_room_state(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        Ok(MediaGetRoomStateResponse {
            room_id: inner.room_id,
            exists: inner.exists,
            revision: inner.revision,
            room_snapshot_json: inner.room_snapshot_json,
            ext: Value::Null,
        })
    }

    async fn media_leave_transport(
        &self,
        ctx: &Ctx,
        req: &MediaLeaveTransportRequest,
    ) -> Result<MediaLeaveTransportResponse> {
        let mut grpc_req = Request::new(ProtoLeaveRoom {
            request_id: req.request_id.clone(),
            room_id: req.room_id.clone(),
            peer_id: req.peer_id.clone(),
            session_id: req.session_id.clone(),
            user_id: req.user_id.clone(),
        });
        set_context_metadata(&mut grpc_req, ctx);

        let resp = self
            .control_client()
            .await?
            .leave_room(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        Ok(MediaLeaveTransportResponse {
            left: inner.left,
            ext: Value::Null,
        })
    }

    async fn media_handle_sdp_offer(
        &self,
        ctx: &Ctx,
        req: &HandleSdpOfferRequest,
    ) -> Result<HandleSdpOfferResponse> {
        let mut grpc_req = Request::new(ProtoHandleOffer {
            request_id: req.request_id.clone(),
            room_id: req.room_id.clone(),
            peer_id: req.peer_id.clone(),
            sdp_offer: req.sdp_offer.clone(),
        });
        set_context_metadata(&mut grpc_req, ctx);

        let resp = self
            .control_client()
            .await?
            .handle_sdp_offer(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        Ok(HandleSdpOfferResponse {
            sdp_answer: inner.sdp_answer,
            ext: Value::Null,
        })
    }

    async fn media_handle_sdp_answer(
        &self,
        ctx: &Ctx,
        req: &HandleSdpAnswerRequest,
    ) -> Result<HandleSdpAnswerResponse> {
        let mut grpc_req = Request::new(ProtoHandleAnswer {
            request_id: req.request_id.clone(),
            room_id: req.room_id.clone(),
            peer_id: req.peer_id.clone(),
            sdp_answer: req.sdp_answer.clone(),
        });
        set_context_metadata(&mut grpc_req, ctx);

        let resp = self
            .control_client()
            .await?
            .handle_sdp_answer(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        Ok(HandleSdpAnswerResponse {
            accepted: inner.accepted,
            ext: Value::Null,
        })
    }

    async fn media_add_ice_candidate(
        &self,
        ctx: &Ctx,
        req: &AddIceCandidateRequest,
    ) -> Result<AddIceCandidateResponse> {
        let mut grpc_req = Request::new(ProtoAddIce {
            request_id: req.request_id.clone(),
            room_id: req.room_id.clone(),
            peer_id: req.peer_id.clone(),
            candidate_json: req.candidate_json.clone(),
        });
        set_context_metadata(&mut grpc_req, ctx);

        let resp = self
            .control_client()
            .await?
            .add_ice_candidate(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        Ok(AddIceCandidateResponse {
            accepted: inner.accepted,
            ext: Value::Null,
        })
    }

    async fn media_set_publisher_mute(
        &self,
        ctx: &Ctx,
        req: &MediaSetPublisherMuteRequest,
    ) -> Result<MediaSetPublisherMuteResponse> {
        let mut grpc_req = Request::new(ProtoSetPublisherMute {
            request_id: req.request_id.clone(),
            room_id: req.room_id.clone(),
            publisher_peer_id: req.publisher_peer_id.clone(),
            mute_audio: req.mute_audio,
            mute_video: req.mute_video,
        });
        set_context_metadata(&mut grpc_req, ctx);

        let resp = self
            .control_client()
            .await?
            .set_publisher_mute(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        Ok(MediaSetPublisherMuteResponse {
            applied: inner.applied,
            ext: Value::Null,
        })
    }

    async fn media_set_subscription(
        &self,
        ctx: &Ctx,
        req: &MediaSetSubscriptionRequest,
    ) -> Result<MediaSetSubscriptionResponse> {
        let mut grpc_req = Request::new(ProtoSetSubscription {
            request_id: req.request_id.clone(),
            room_id: req.room_id.clone(),
            subscriber_peer_id: req.subscriber_peer_id.clone(),
            track_id: req.track_id.clone(),
            enable: req.enable,
            media: Self::parse_media_kind(req.media.as_deref()) as i32,
            preferred_layer: Self::parse_simulcast_layer(req.preferred_layer.as_deref()) as i32,
            priority: req.priority,
        });
        set_context_metadata(&mut grpc_req, ctx);

        let resp = self
            .control_client()
            .await?
            .set_subscription(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();
        Ok(MediaSetSubscriptionResponse {
            applied: inner.applied,
            ext: Value::Null,
        })
    }

    async fn media_set_simulcast_layer(
        &self,
        ctx: &Ctx,
        req: &MediaSetSimulcastLayerRequest,
    ) -> Result<MediaSetSimulcastLayerResponse> {
        let mut grpc_req = Request::new(ProtoSetSimulcastLayer {
            request_id: req.request_id.clone(),
            room_id: req.room_id.clone(),
            subscriber_peer_id: req.subscriber_peer_id.clone(),
            track_id: req.track_id.clone(),
            layer: Self::parse_simulcast_layer(Some(req.layer.as_str())) as i32,
        });
        set_context_metadata(&mut grpc_req, ctx);

        let resp = self
            .control_client()
            .await?
            .set_simulcast_layer(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();
        Ok(MediaSetSimulcastLayerResponse {
            applied: inner.applied,
            ext: Value::Null,
        })
    }

    async fn media_get_network_quality(
        &self,
        ctx: &Ctx,
        req: &MediaGetNetworkQualityRequest,
    ) -> Result<MediaGetNetworkQualityResponse> {
        let mut grpc_req = Request::new(GetPeerNetworkQualityRequest {
            request_id: req.request_id.clone(),
            room_id: req.room_id.clone(),
            peer_id: req.peer_id.clone(),
        });
        set_context_metadata(&mut grpc_req, ctx);

        let resp = self
            .control_client()
            .await?
            .get_peer_network_quality(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();
        let quality = inner.quality;
        Ok(MediaGetNetworkQualityResponse {
            has_data: inner.has_data,
            upstream_score: quality.as_ref().map(|q| q.upstream_score).unwrap_or(0),
            downstream_score: quality.as_ref().map(|q| q.downstream_score).unwrap_or(0),
            rtt_ms: quality.as_ref().map(|q| q.rtt_ms).unwrap_or(0),
            packet_loss_ratio: quality.as_ref().map(|q| q.packet_loss_ratio).unwrap_or(0.0),
            ext: Value::Null,
        })
    }
}
