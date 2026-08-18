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

/// 计费/授权单位。
///
/// 决定「装了就能用」还是「还要逐人发放」。由插件在注册时声明 ——
/// 平台不替插件决定它该怎么卖。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeatModel {
    /// 装了全员可用：租户开关即安装状态。绝大多数插件属于这一类。
    Tenant,
    /// 还需逐人授权：留给有边际成本的（AI 按 token）与需合规隔离的（DLP）。
    PerUser,
    /// 未声明：**沿用旧语义**（要求用户授权）。
    ///
    /// 单列一个取值而不是默认成 PerUser，是因为两者要区别对待：
    /// 「明确声明按席位」是产品决策，「没声明」是迁移中间态 —— 前者稳定，
    /// 后者应当随插件逐个消失。混在一起就看不出还剩多少插件没迁。
    Unspecified,
}

impl SeatModel {
    /// 注册时上报的是字符串（协议里可选），这里做一次归一。
    ///
    /// 无法识别的取值按 `Unspecified` 处理而不是报错：注册契约的字段是可选的，
    /// 一个拼错的取值不该让插件注册失败 —— 它只会退回旧语义，是安全方向。
    pub fn parse(raw: &str) -> Self {
        match raw.trim() {
            "tenant" => Self::Tenant,
            "per_user" => Self::PerUser,
            _ => Self::Unspecified,
        }
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

    /// 租户级校验：只看「这个租户装没装这个能力」，不看用户授权。
    ///
    /// 供 `SeatModel::Tenant` 的插件使用。语义与按人那条的关键差别是
    /// **租户开关缺失即拒**（没装就是没装），而不是像旧语义那样「不设就放行」。
    ///
    /// 这里之所以能从第一天就严格：租户模型是新增语义，现存插件一个都没用它，
    /// 所以不存在「升级当天全员被拒」的反转风险。迁移由各插件自己声明触发。
    ///
    /// 默认实现返回未支持 —— 后端必须显式实现才能承接租户模型的插件。
    /// 默认返回 Ok 是危险的：那会让没实现的后端**静默放行所有租户级调用**。
    async fn ensure_tenant_capability_enabled(
        &self,
        _tenant_id: &str,
        _capability_id: &str,
    ) -> CapabilityResult<()> {
        Err(CapabilityError::NotSupported(
            "this policy backend does not implement tenant-scoped entitlement".into(),
        ))
    }

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

/// 能力分发路由：**把「哪些 capability_id 交给哪个后端」变成可注册的数据。**
///
/// 在此之前，应用层的分发器里写着 `if capability_id.starts_with("rtc.")` ——
/// 核心因此认识一个具体的插件种类，每加一类插件都要回来改分发器。这是插件系统
/// 最典型的老化方式：支持的插件越多，核心越臃肿。
///
/// 现在核心只做两件事：按注册顺序问每条路由「这个 id 归你吗」，没人认领就走
/// 远端插件路由表。核心代码里不该再出现任何具体插件的名字。
///
/// # 为什么 `matches` 不是返回前缀字符串
///
/// 前缀只是**今天**够用的匹配方式。把判定权交给路由自己，将来按 kind、按版本、
/// 按租户灰度来选路，都不需要改这个 trait —— 契约一旦有人依赖就改不动了，
/// 所以这里刻意留出判定自由度。
#[async_trait]
pub trait CapabilityDispatchRoute: Send + Sync {
    /// 路由标识，用于日志与冲突排查（例如 `rtc`）。
    fn route_id(&self) -> &str;

    /// 该 capability_id 是否由本路由接管。
    fn matches(&self, capability_id: &str) -> bool;

    /// 接管后的实际分发。
    async fn dispatch(
        &self,
        ctx: &Ctx,
        req: &crate::dispatch::CapabilityDispatchCommand,
    ) -> CapabilityResult<crate::dispatch::CapabilityDispatchResult>;
}

/// 插件健康探针：**把「用哪种协议探活」也变成可注册的数据。**
///
/// 通用健康检查器原本内嵌着 `if capability_id == "rtc.media.control"` 与
/// 一整段 SfuControl 客户端代码 —— 选择依据后来改成了插件声明的标签，
/// 但实现仍然留在通用路径里。加第二种需要特殊探活语义的插件时，
/// 还是只能回到那个文件里加分支。
///
/// 现在通用侧只做两件事：按插件声明的协议名找探针，找不到就用通用协议。
///
/// # 为什么探针拿的是 authority 而不是已建好的连接
///
/// 不同协议对连接的要求不同（超时、TLS、复用策略）。把建连交给探针自己，
/// 通用侧就不必知道任何一种协议的连接细节 —— 这是 kind 专有数据保持
/// 不透明的必要条件。
#[async_trait]
pub trait PluginHealthProbe: Send + Sync {
    /// 协议名，与插件注册时 `labels["health_protocol"]` 的取值对应。
    fn protocol(&self) -> &str;

    /// 探活。`Err` 的内容会被记进路由簿的 `last_error`，要能指明原因。
    async fn probe(&self, grpc_authority: &str, timeout: std::time::Duration)
    -> Result<(), String>;
}
