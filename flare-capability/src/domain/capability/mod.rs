//! 能力扩展限界上下文：Guard / Resolver / RTC DTO、分发命令、策略端口。
//!
//! 不含应用编排与基础设施实现；Hook 引擎模型见 [`crate::domain::model`]。

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
pub use descriptor::CapabilityDescriptor;
pub use dispatch::{CapabilityDispatchCommand, CapabilityDispatchResult};
pub use error::{CapabilityError, GuardDecision, GuardRejection, Result};
pub use grant::UserCapabilityGrant;
pub use ports::{
    CapabilityPolicyBackend, PreSendGuard, PreSendGuardPipeline, RecipientResolver, RtcCapability,
};
pub use recipient::{RecipientResolveRequest, RecipientResolveResult};
pub use rtc::{
    AcceptCallRequest, AcceptCallResponse, CreateCallRequest, CreateCallResponse,
    GetJoinTokenRequest, GetJoinTokenResponse, HangupCallRequest, HangupCallResponse,
    ListParticipantsRequest, ListParticipantsResponse, RejectCallRequest, RejectCallResponse,
    RtcParticipant,
};
