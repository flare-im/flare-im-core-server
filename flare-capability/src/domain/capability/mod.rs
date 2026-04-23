//! 能力扩展限界上下文：Guard / Resolver / RTC DTO、分发命令、策略端口、**Dispatch 领域服务**。
//!
//! Hook 引擎模型见 [`crate::domain::model`]。

pub mod command_dispatch_service;
pub mod context;
pub mod descriptor;
pub mod dispatch;
pub mod error;
pub mod grant;
pub mod ports;
pub mod recipient;
pub mod rtc;

pub use context::{
    CapabilityInvokeMeta, ConversationKind, PreSendEvaluateInput, ResolveTrigger,
};
pub use command_dispatch_service::{dispatch_rtc_by_capability_id, execute_capability_dispatch};
pub use descriptor::CapabilityDescriptor;
pub use dispatch::{CapabilityDispatchCommand, CapabilityDispatchResult};
pub use error::{CapabilityError, GuardDecision, GuardRejection, Result};
pub use grant::UserCapabilityGrant;
pub use ports::{
    CapabilityPolicyBackend, PreSendGuard, PreSendGuardPipeline, RecipientResolver, RtcCapability,
};
pub use recipient::{RecipientResolveRequest, RecipientResolveResult};
pub use rtc::{
    AcceptCallRequest, AcceptCallResponse, AddIceCandidateRequest, AddIceCandidateResponse,
    CreateCallRequest, CreateCallResponse, GetJoinTokenRequest, GetJoinTokenResponse,
    HandleSdpAnswerRequest, HandleSdpAnswerResponse, HandleSdpOfferRequest, HandleSdpOfferResponse,
    HangupCallRequest, HangupCallResponse, ListParticipantsRequest, ListParticipantsResponse,
    RejectCallRequest, RejectCallResponse, RtcParticipant, SfuJoinRoomRequest, SfuJoinRoomResponse,
    SfuLeaveRoomRequest, SfuLeaveRoomResponse,
};
