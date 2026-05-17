//! 能力扩展领域端口：Guard / Resolver / RTC / 策略存储（由 infrastructure 实现）。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use flare_core_base::context::Ctx;

use super::context::{ConversationKind, PreSendEvaluateInput, ResolveTrigger};
use super::error::{CapabilityError, GuardDecision, Result as CapabilityResult};
use super::grant::UserCapabilityGrant;
use super::recipient::{RecipientResolveRequest, RecipientResolveResult};
use super::rtc::{
    AcceptCallRequest, AcceptCallResponse, AddIceCandidateRequest, AddIceCandidateResponse,
    CreateCallRequest, CreateCallResponse, GetJoinTokenRequest, GetJoinTokenResponse,
    HandleSdpAnswerRequest, HandleSdpAnswerResponse, HandleSdpOfferRequest, HandleSdpOfferResponse,
    HangupCallRequest, HangupCallResponse, ListParticipantsRequest, ListParticipantsResponse,
    MediaGetNetworkQualityRequest, MediaGetNetworkQualityResponse, MediaGetRoomStateRequest,
    MediaGetRoomStateResponse, MediaJoinTransportRequest,
    MediaJoinTransportResponse, MediaLeaveTransportRequest, MediaLeaveTransportResponse,
    MediaSetPublisherMuteRequest, MediaSetPublisherMuteResponse, MediaSetSimulcastLayerRequest,
    MediaSetSimulcastLayerResponse, MediaSetSubscriptionRequest, MediaSetSubscriptionResponse,
    RejectCallRequest, RejectCallResponse,
};

// ----------------------------------------------------------------------------- Guard / Resolver / RTC

/// 预发送 Guard（校验能力）
#[async_trait]
pub trait PreSendGuard: Send + Sync {
    fn id(&self) -> &str;
    async fn evaluate(
        &self,
        ctx: &Ctx,
        input: &PreSendEvaluateInput,
    ) -> CapabilityResult<GuardDecision>;
}

/// Guard 管道（便于测试替换 Runtime）
#[async_trait]
pub trait PreSendGuardPipeline: Send + Sync {
    async fn evaluate(
        &self,
        ctx: &Ctx,
        input: &PreSendEvaluateInput,
    ) -> CapabilityResult<GuardDecision>;
}

/// 接收者解析
#[async_trait]
pub trait RecipientResolver: Send + Sync {
    fn id(&self) -> &str;
    fn supports(&self, kind: &ConversationKind, trigger: ResolveTrigger) -> bool;
    async fn resolve(
        &self,
        ctx: &Ctx,
        req: &RecipientResolveRequest,
    ) -> CapabilityResult<RecipientResolveResult>;
}

/// RTC 动作能力
#[async_trait]
pub trait RtcCapability: Send + Sync {
    fn id(&self) -> &str;
    async fn create_call(
        &self,
        ctx: &Ctx,
        req: &CreateCallRequest,
    ) -> CapabilityResult<CreateCallResponse>;
    async fn accept_call(
        &self,
        ctx: &Ctx,
        req: &AcceptCallRequest,
    ) -> CapabilityResult<AcceptCallResponse>;
    async fn reject_call(
        &self,
        ctx: &Ctx,
        req: &RejectCallRequest,
    ) -> CapabilityResult<RejectCallResponse>;
    async fn hangup_call(
        &self,
        ctx: &Ctx,
        req: &HangupCallRequest,
    ) -> CapabilityResult<HangupCallResponse>;
    async fn get_join_token(
        &self,
        ctx: &Ctx,
        req: &GetJoinTokenRequest,
    ) -> CapabilityResult<GetJoinTokenResponse>;
    async fn list_participants(
        &self,
        ctx: &Ctx,
        req: &ListParticipantsRequest,
    ) -> CapabilityResult<ListParticipantsResponse>;

    /// 进入媒体传输房间（由具体 RTC 后端实现）。
    async fn media_join_transport(
        &self,
        _ctx: &Ctx,
        _req: &MediaJoinTransportRequest,
    ) -> CapabilityResult<MediaJoinTransportResponse> {
        Err(CapabilityError::NotSupported(
            "media_join_transport: not supported for this RTC backend".into(),
        ))
    }

    /// 离开媒体传输房间。
    async fn media_leave_transport(
        &self,
        _ctx: &Ctx,
        _req: &MediaLeaveTransportRequest,
    ) -> CapabilityResult<MediaLeaveTransportResponse> {
        Err(CapabilityError::NotSupported(
            "media_leave_transport: not supported for this RTC backend".into(),
        ))
    }

    /// 终端 offer → 媒体后端 answer。
    async fn media_handle_sdp_offer(
        &self,
        _ctx: &Ctx,
        _req: &HandleSdpOfferRequest,
    ) -> CapabilityResult<HandleSdpOfferResponse> {
        Err(CapabilityError::NotSupported(
            "media_handle_sdp_offer: not supported for this RTC backend".into(),
        ))
    }

    /// 处理 answer 型协商。
    async fn media_handle_sdp_answer(
        &self,
        _ctx: &Ctx,
        _req: &HandleSdpAnswerRequest,
    ) -> CapabilityResult<HandleSdpAnswerResponse> {
        Err(CapabilityError::NotSupported(
            "media_handle_sdp_answer: not supported for this RTC backend".into(),
        ))
    }

    /// Trickle ICE。
    async fn media_add_ice_candidate(
        &self,
        _ctx: &Ctx,
        _req: &AddIceCandidateRequest,
    ) -> CapabilityResult<AddIceCandidateResponse> {
        Err(CapabilityError::NotSupported(
            "media_add_ice_candidate: not supported for this RTC backend".into(),
        ))
    }

    /// 发布者软静音（摄像头/麦克风开关）。
    async fn media_set_publisher_mute(
        &self,
        _ctx: &Ctx,
        _req: &MediaSetPublisherMuteRequest,
    ) -> CapabilityResult<MediaSetPublisherMuteResponse> {
        Err(CapabilityError::NotSupported(
            "media_set_publisher_mute: not supported for this RTC backend".into(),
        ))
    }

    /// 订阅关系控制（SetSubscription）。
    async fn media_set_subscription(
        &self,
        _ctx: &Ctx,
        _req: &MediaSetSubscriptionRequest,
    ) -> CapabilityResult<MediaSetSubscriptionResponse> {
        Err(CapabilityError::NotSupported(
            "media_set_subscription: not supported for this RTC backend".into(),
        ))
    }

    /// Simulcast 层控制（SetSimulcastLayer）。
    async fn media_set_simulcast_layer(
        &self,
        _ctx: &Ctx,
        _req: &MediaSetSimulcastLayerRequest,
    ) -> CapabilityResult<MediaSetSimulcastLayerResponse> {
        Err(CapabilityError::NotSupported(
            "media_set_simulcast_layer: not supported for this RTC backend".into(),
        ))
    }

    /// 网络质量查询（GetPeerNetworkQuality）。
    async fn media_get_network_quality(
        &self,
        _ctx: &Ctx,
        _req: &MediaGetNetworkQualityRequest,
    ) -> CapabilityResult<MediaGetNetworkQualityResponse> {
        Err(CapabilityError::NotSupported(
            "media_get_network_quality: not supported for this RTC backend".into(),
        ))
    }

    /// 房间状态快照（peers + published tracks）。
    async fn media_get_room_state(
        &self,
        _ctx: &Ctx,
        _req: &MediaGetRoomStateRequest,
    ) -> CapabilityResult<MediaGetRoomStateResponse> {
        Err(CapabilityError::NotSupported(
            "media_get_room_state: not supported for this RTC backend".into(),
        ))
    }
}

// ----------------------------------------------------------------------------- 策略（授权 / 租户开关）

/// 能力策略后端（`CapabilityService`、分发命令共用；内存或 PostgreSQL 实现）
#[async_trait]
pub trait CapabilityPolicyBackend: Send + Sync {
    async fn ensure_dispatch_allowed(
        &self,
        tenant_id: &str,
        user_id: &str,
        capability_id: &str,
    ) -> CapabilityResult<()>;

    async fn list_user_grants(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> CapabilityResult<Vec<UserCapabilityGrant>>;

    async fn grant_user_capability(
        &self,
        tenant_id: &str,
        user_id: &str,
        capability_id: &str,
        expires_at: Option<DateTime<Utc>>,
        plan_code: Option<String>,
        source: Option<String>,
    ) -> CapabilityResult<()>;

    async fn revoke_user_capability(
        &self,
        tenant_id: &str,
        user_id: &str,
        capability_id: &str,
    ) -> CapabilityResult<()>;

    async fn set_tenant_capability(
        &self,
        tenant_id: &str,
        capability_id: &str,
        enabled: bool,
    ) -> CapabilityResult<()>;
}
