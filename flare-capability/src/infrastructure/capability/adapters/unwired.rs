//! 未接线的占位适配器（单一实现，避免按传输层重复一份空壳）。

use async_trait::async_trait;
use flare_core_base::context::Ctx;

use crate::domain::capability::{
    AcceptCallRequest, AcceptCallResponse, CapabilityError, ConversationKind, CreateCallRequest,
    CreateCallResponse, GetJoinTokenRequest, GetJoinTokenResponse, GuardDecision,
    HangupCallRequest, HangupCallResponse, ListParticipantsRequest, ListParticipantsResponse,
    PreSendEvaluateInput, PreSendGuard, RecipientResolveRequest, RecipientResolveResult,
    RecipientResolver, RejectCallRequest, RejectCallResponse, ResolveTrigger, Result,
    RtcCapability,
};

/// RTC：未注入媒体后端（本地或远端）时返回 `NotSupported`
pub struct UnwiredRtcCapability;

impl Default for UnwiredRtcCapability {
    fn default() -> Self {
        Self::new()
    }
}

impl UnwiredRtcCapability {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl RtcCapability for UnwiredRtcCapability {
    fn id(&self) -> &str {
        "unwired.rtc"
    }

    async fn create_call(
        &self,
        _ctx: &Ctx,
        _req: &CreateCallRequest,
    ) -> Result<CreateCallResponse> {
        Err(CapabilityError::NotSupported("UnwiredRtcCapability".into()))
    }

    async fn accept_call(
        &self,
        _ctx: &Ctx,
        _req: &AcceptCallRequest,
    ) -> Result<AcceptCallResponse> {
        Err(CapabilityError::NotSupported("UnwiredRtcCapability".into()))
    }

    async fn reject_call(
        &self,
        _ctx: &Ctx,
        _req: &RejectCallRequest,
    ) -> Result<RejectCallResponse> {
        Err(CapabilityError::NotSupported("UnwiredRtcCapability".into()))
    }

    async fn hangup_call(
        &self,
        _ctx: &Ctx,
        _req: &HangupCallRequest,
    ) -> Result<HangupCallResponse> {
        Err(CapabilityError::NotSupported("UnwiredRtcCapability".into()))
    }

    async fn get_join_token(
        &self,
        _ctx: &Ctx,
        _req: &GetJoinTokenRequest,
    ) -> Result<GetJoinTokenResponse> {
        Err(CapabilityError::NotSupported("UnwiredRtcCapability".into()))
    }

    async fn list_participants(
        &self,
        _ctx: &Ctx,
        _req: &ListParticipantsRequest,
    ) -> Result<ListParticipantsResponse> {
        Err(CapabilityError::NotSupported("UnwiredRtcCapability".into()))
    }
}

/// 接收者解析占位（未启用任何远端解析器）
pub struct UnwiredRecipientResolver;

impl Default for UnwiredRecipientResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl UnwiredRecipientResolver {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl RecipientResolver for UnwiredRecipientResolver {
    fn id(&self) -> &str {
        "unwired.recipient"
    }

    fn supports(&self, _kind: &ConversationKind, _trigger: ResolveTrigger) -> bool {
        false
    }

    async fn resolve(
        &self,
        _ctx: &Ctx,
        _req: &RecipientResolveRequest,
    ) -> Result<RecipientResolveResult> {
        Err(CapabilityError::NotSupported(
            "UnwiredRecipientResolver".into(),
        ))
    }
}

/// 静音检查占位（恒通过，待接入真实服务）
pub struct UnwiredMuteGuard;

impl Default for UnwiredMuteGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl UnwiredMuteGuard {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl PreSendGuard for UnwiredMuteGuard {
    fn id(&self) -> &str {
        "unwired.mute"
    }

    async fn evaluate(&self, _ctx: &Ctx, _input: &PreSendEvaluateInput) -> Result<GuardDecision> {
        Ok(GuardDecision::Allow)
    }
}

/// 好友关系检查占位（恒通过，待接入真实服务）
pub struct UnwiredFriendshipGuard;

impl Default for UnwiredFriendshipGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl UnwiredFriendshipGuard {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl PreSendGuard for UnwiredFriendshipGuard {
    fn id(&self) -> &str {
        "unwired.friendship"
    }

    async fn evaluate(&self, _ctx: &Ctx, _input: &PreSendEvaluateInput) -> Result<GuardDecision> {
        Ok(GuardDecision::Allow)
    }
}
