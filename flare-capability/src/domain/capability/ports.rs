//! 能力扩展领域端口：Guard / Resolver / RTC / 策略存储（由 infrastructure 实现）。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use flare_core_base::context::Ctx;

use super::context::{ConversationKind, PreSendEvaluateInput, ResolveTrigger};
use super::error::{GuardDecision, Result as CapabilityResult};
use super::grant::UserCapabilityGrant;
use super::recipient::{RecipientResolveRequest, RecipientResolveResult};
use super::rtc::{
    AcceptCallRequest, AcceptCallResponse, CreateCallRequest, CreateCallResponse,
    GetJoinTokenRequest, GetJoinTokenResponse, HangupCallRequest, HangupCallResponse,
    ListParticipantsRequest, ListParticipantsResponse, RejectCallRequest, RejectCallResponse,
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
