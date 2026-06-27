//! Domain facade for RTC capability DTO contracts.

pub use flare_im_capability_core::{
    AcceptCallRequest, AcceptCallResponse, AddIceCandidateRequest, AddIceCandidateResponse,
    CreateCallRequest, CreateCallResponse, GetJoinTokenRequest, GetJoinTokenResponse,
    HandleSdpAnswerRequest, HandleSdpAnswerResponse, HandleSdpOfferRequest, HandleSdpOfferResponse,
    HangupCallRequest, HangupCallResponse, ListParticipantsRequest, ListParticipantsResponse,
    MediaGetNetworkQualityRequest, MediaGetNetworkQualityResponse, MediaGetRoomStateRequest,
    MediaGetRoomStateResponse, MediaJoinTransportRequest, MediaJoinTransportResponse,
    MediaLeaveTransportRequest, MediaLeaveTransportResponse, MediaSetPublisherMuteRequest,
    MediaSetPublisherMuteResponse, MediaSetSimulcastLayerRequest, MediaSetSimulcastLayerResponse,
    MediaSetSubscriptionRequest, MediaSetSubscriptionResponse, RejectCallRequest,
    RejectCallResponse, RtcParticipant,
};
