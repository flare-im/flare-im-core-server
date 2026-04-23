//! 通过 `flare.sfu.control.v1.SfuControl` 访问独立 **flare-strom-sfu** 控制面，实现 [`RtcCapability`]。

use std::collections::HashMap;

use async_trait::async_trait;
use flare_core_base::context::Ctx;
use flare_grpc_proto::sfu_control::sfu_control_client::SfuControlClient;
use flare_grpc_proto::sfu_control::{
    AcceptCallRequest as ProtoAcceptCallRequest, AddIceCandidateRequest as ProtoAddIce,
    CreateRoomRequest, GetJoinTokenRequest as ProtoJoin,
    HandleSdpAnswerRequest as ProtoHandleAnswer, HandleSdpOfferRequest as ProtoHandleOffer,
    HangupCallRequest as ProtoHangup, JoinRoomRequest as ProtoJoinRoom,
    LeaveRoomRequest as ProtoLeaveRoom,
};
use flare_server_core::client::set_context_metadata;
use serde::Deserialize;
use serde_json::{json, Value};
use tonic::transport::Channel;
use tonic::Request;
use uuid::Uuid;

use crate::domain::capability::{
    AcceptCallRequest, AcceptCallResponse, AddIceCandidateRequest, AddIceCandidateResponse,
    CapabilityError, CreateCallRequest, CreateCallResponse, GetJoinTokenRequest, GetJoinTokenResponse,
    HandleSdpAnswerRequest, HandleSdpAnswerResponse, HandleSdpOfferRequest, HandleSdpOfferResponse,
    HangupCallRequest, HangupCallResponse, ListParticipantsRequest, ListParticipantsResponse,
    RejectCallRequest, RejectCallResponse, Result, RtcCapability, SfuJoinRoomRequest,
    SfuJoinRoomResponse, SfuLeaveRoomRequest, SfuLeaveRoomResponse,
};

#[derive(Debug, Deserialize)]
struct CallRefPayload {
    call_id: String,
}

fn status_to_capability(s: tonic::Status) -> CapabilityError {
    CapabilityError::System(format!(
        "strom-sfu gRPC {}: {}",
        s.code(),
        s.message()
    ))
}

/// 独立 strom-sfu 进程的 gRPC 后端（与进程内 `flare-sfu` 二选一）。
pub struct StromSfuGrpcRtcCapability {
    client: SfuControlClient<Channel>,
}

impl StromSfuGrpcRtcCapability {
    /// 连接 `FLARE_STROM_SFU_GRPC_ENDPOINT` 形式的 URI（如 `http://127.0.0.1:50051`）。
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        let ep = endpoint.into();
        let channel = Channel::from_shared(ep.clone())
            .map_err(|e| anyhow::anyhow!("invalid strom SFU gRPC endpoint {ep}: {e}"))?
            .connect()
            .await
            .map_err(|e| anyhow::anyhow!("strom SFU gRPC dial {ep}: {e}"))?;
        Ok(Self {
            client: SfuControlClient::new(channel),
        })
    }

    /// 供 `ExtensionPlugin` 等旁路调用 `SfuControl`（与 [`RtcCapability`] 共用同一连接）。
    pub fn control_client(&self) -> SfuControlClient<Channel> {
        self.client.clone()
    }

    fn resolve_call_id(req_call_id: &str, ext: &Value) -> String {
        serde_json::from_value::<CallRefPayload>(ext.clone())
            .ok()
            .filter(|p| !p.call_id.is_empty())
            .map(|p| p.call_id)
            .unwrap_or_else(|| req_call_id.to_owned())
    }

    /// 为 strom-sfu 选择稳定的房间主键，避免同一通话重复创建时漂移到不同 room/call。
    ///
    /// 优先级：
    /// 1) `call_id` 可解析为 UUID（主路径，客户端通常传 UUID）
    /// 2) `conversation_id` 可解析为 UUID
    /// 3) 对 `call_id` 做 v5 派生（稳定）
    /// 4) 对 `conversation_id` 做 v5 派生（稳定）
    /// 5) 兜底随机 UUID（仅在两者都为空）
    fn resolve_room_id(call_id: &str, conversation_id: &str) -> String {
        let call_id = call_id.trim();
        if let Ok(u) = Uuid::parse_str(call_id) {
            return u.to_string();
        }

        let conversation_id = conversation_id.trim();
        if let Ok(u) = Uuid::parse_str(conversation_id) {
            return u.to_string();
        }

        if !call_id.is_empty() {
            return Uuid::new_v5(&Uuid::NAMESPACE_OID, call_id.as_bytes()).to_string();
        }
        if !conversation_id.is_empty() {
            return Uuid::new_v5(&Uuid::NAMESPACE_OID, conversation_id.as_bytes()).to_string();
        }
        Uuid::new_v4().to_string()
    }
}

#[async_trait]
impl RtcCapability for StromSfuGrpcRtcCapability {
    fn id(&self) -> &str {
        "strom-sfu.rtc"
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
            .client
            .clone()
            .create_room(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        Ok(CreateCallResponse {
            call_id: inner.call_id,
            room_id: inner.room_id,
            ext: json!({
                "created": inner.created,
                "room_id": room_id,
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
            .client
            .clone()
            .accept_call(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        Ok(AcceptCallResponse {
            call_id: inner.call_id,
            ext: json!({
                "room_id": inner.room_id,
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
            .client
            .clone()
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
            .client
            .clone()
            .get_join_token(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        Ok(GetJoinTokenResponse {
            token: inner.token,
            ttl_seconds: inner.ttl_seconds,
            ext: json!({
                "room_id": inner.room_id,
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
                "note": "strom-sfu gRPC: use GetRoomSummary / events for roster (placeholder)",
            }),
        })
    }

    async fn sfu_join_room(&self, ctx: &Ctx, req: &SfuJoinRoomRequest) -> Result<SfuJoinRoomResponse> {
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
            .client
            .clone()
            .join_room(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        Ok(SfuJoinRoomResponse {
            room_id: inner.room_id,
            peer_id: inner.peer_id,
            session_id: inner.session_id,
            call_id: inner.call_id,
            ext: Value::Null,
        })
    }

    async fn sfu_leave_room(
        &self,
        ctx: &Ctx,
        req: &SfuLeaveRoomRequest,
    ) -> Result<SfuLeaveRoomResponse> {
        let mut grpc_req = Request::new(ProtoLeaveRoom {
            request_id: req.request_id.clone(),
            room_id: req.room_id.clone(),
            peer_id: req.peer_id.clone(),
            session_id: req.session_id.clone(),
            user_id: req.user_id.clone(),
        });
        set_context_metadata(&mut grpc_req, ctx);

        let resp = self
            .client
            .clone()
            .leave_room(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        Ok(SfuLeaveRoomResponse {
            left: inner.left,
            ext: Value::Null,
        })
    }

    async fn sfu_handle_sdp_offer(
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
            .client
            .clone()
            .handle_sdp_offer(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        Ok(HandleSdpOfferResponse {
            sdp_answer: inner.sdp_answer,
            ext: Value::Null,
        })
    }

    async fn sfu_handle_sdp_answer(
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
            .client
            .clone()
            .handle_sdp_answer(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        Ok(HandleSdpAnswerResponse {
            accepted: inner.accepted,
            ext: Value::Null,
        })
    }

    async fn sfu_add_ice_candidate(
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
            .client
            .clone()
            .add_ice_candidate(grpc_req)
            .await
            .map_err(status_to_capability)?;
        let inner = resp.into_inner();

        Ok(AddIceCandidateResponse {
            accepted: inner.accepted,
            ext: Value::Null,
        })
    }
}
