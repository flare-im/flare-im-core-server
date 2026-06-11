//! Capability extension ports.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use flare_core_base::context::Ctx;

use crate::context::{ConversationKind, PreSendEvaluateInput, ResolveTrigger};
use crate::error::{CapabilityError, GuardDecision, Result as CapabilityResult};
use crate::grant::UserCapabilityGrant;
use crate::recipient::{RecipientResolveRequest, RecipientResolveResult};
use crate::rtc::{
    AcceptCallRequest, AcceptCallResponse, AddIceCandidateRequest, AddIceCandidateResponse,
    CreateCallRequest, CreateCallResponse, GetJoinTokenRequest, GetJoinTokenResponse,
    HandleSdpAnswerRequest, HandleSdpAnswerResponse, HandleSdpOfferRequest, HandleSdpOfferResponse,
    HangupCallRequest, HangupCallResponse, ListParticipantsRequest, ListParticipantsResponse,
    MediaGetNetworkQualityRequest, MediaGetNetworkQualityResponse, MediaGetRoomStateRequest,
    MediaGetRoomStateResponse, MediaJoinTransportRequest, MediaJoinTransportResponse,
    MediaLeaveTransportRequest, MediaLeaveTransportResponse, MediaSetPublisherMuteRequest,
    MediaSetPublisherMuteResponse, MediaSetSimulcastLayerRequest, MediaSetSimulcastLayerResponse,
    MediaSetSubscriptionRequest, MediaSetSubscriptionResponse, RejectCallRequest,
    RejectCallResponse,
};

#[async_trait]
pub trait PreSendGuard: Send + Sync {
    fn id(&self) -> &str;

    async fn evaluate(
        &self,
        ctx: &Ctx,
        input: &PreSendEvaluateInput,
    ) -> CapabilityResult<GuardDecision>;
}

#[async_trait]
pub trait PreSendGuardPipeline: Send + Sync {
    async fn evaluate(
        &self,
        ctx: &Ctx,
        input: &PreSendEvaluateInput,
    ) -> CapabilityResult<GuardDecision>;
}

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

    async fn media_join_transport(
        &self,
        _ctx: &Ctx,
        _req: &MediaJoinTransportRequest,
    ) -> CapabilityResult<MediaJoinTransportResponse> {
        Err(CapabilityError::NotSupported(
            "media_join_transport: not supported for this RTC backend".into(),
        ))
    }

    async fn media_leave_transport(
        &self,
        _ctx: &Ctx,
        _req: &MediaLeaveTransportRequest,
    ) -> CapabilityResult<MediaLeaveTransportResponse> {
        Err(CapabilityError::NotSupported(
            "media_leave_transport: not supported for this RTC backend".into(),
        ))
    }

    async fn media_handle_sdp_offer(
        &self,
        _ctx: &Ctx,
        _req: &HandleSdpOfferRequest,
    ) -> CapabilityResult<HandleSdpOfferResponse> {
        Err(CapabilityError::NotSupported(
            "media_handle_sdp_offer: not supported for this RTC backend".into(),
        ))
    }

    async fn media_handle_sdp_answer(
        &self,
        _ctx: &Ctx,
        _req: &HandleSdpAnswerRequest,
    ) -> CapabilityResult<HandleSdpAnswerResponse> {
        Err(CapabilityError::NotSupported(
            "media_handle_sdp_answer: not supported for this RTC backend".into(),
        ))
    }

    async fn media_add_ice_candidate(
        &self,
        _ctx: &Ctx,
        _req: &AddIceCandidateRequest,
    ) -> CapabilityResult<AddIceCandidateResponse> {
        Err(CapabilityError::NotSupported(
            "media_add_ice_candidate: not supported for this RTC backend".into(),
        ))
    }

    async fn media_set_publisher_mute(
        &self,
        _ctx: &Ctx,
        _req: &MediaSetPublisherMuteRequest,
    ) -> CapabilityResult<MediaSetPublisherMuteResponse> {
        Err(CapabilityError::NotSupported(
            "media_set_publisher_mute: not supported for this RTC backend".into(),
        ))
    }

    async fn media_set_subscription(
        &self,
        _ctx: &Ctx,
        _req: &MediaSetSubscriptionRequest,
    ) -> CapabilityResult<MediaSetSubscriptionResponse> {
        Err(CapabilityError::NotSupported(
            "media_set_subscription: not supported for this RTC backend".into(),
        ))
    }

    async fn media_set_simulcast_layer(
        &self,
        _ctx: &Ctx,
        _req: &MediaSetSimulcastLayerRequest,
    ) -> CapabilityResult<MediaSetSimulcastLayerResponse> {
        Err(CapabilityError::NotSupported(
            "media_set_simulcast_layer: not supported for this RTC backend".into(),
        ))
    }

    async fn media_get_network_quality(
        &self,
        _ctx: &Ctx,
        _req: &MediaGetNetworkQualityRequest,
    ) -> CapabilityResult<MediaGetNetworkQualityResponse> {
        Err(CapabilityError::NotSupported(
            "media_get_network_quality: not supported for this RTC backend".into(),
        ))
    }

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
