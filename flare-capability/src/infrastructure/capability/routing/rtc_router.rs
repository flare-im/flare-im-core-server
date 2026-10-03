//! RTC 路由器。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use flare_core_base::context::Ctx;
use tokio::sync::RwLock;

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
    RejectCallRequest, RejectCallResponse, Result, RtcCapability, trimmed_dispatch_tenant,
};

/// 选 RTC 后端时上下文缺租户的拒绝原因（`tenant required …` 口径，W12-T）。
pub const RTC_BACKEND_TENANT_REQUIRED: &str = "tenant required to select an RTC backend";

#[derive(Clone)]
pub struct RtcCapabilityRouter {
    backend: Arc<RwLock<Option<Arc<dyn RtcCapability>>>>,
    tenant_backends: Arc<RwLock<HashMap<String, Arc<dyn RtcCapability>>>>,
}

impl Default for RtcCapabilityRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl RtcCapabilityRouter {
    pub fn new() -> Self {
        Self {
            backend: Arc::new(RwLock::new(None)),
            tenant_backends: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn set_backend(&self, rtc: Option<Arc<dyn RtcCapability>>) {
        *self.backend.write().await = rtc;
    }

    pub async fn set_backend_for_tenant(
        &self,
        tenant_id: &str,
        rtc: Option<Arc<dyn RtcCapability>>,
    ) {
        let tenant = tenant_id.trim();
        if tenant.is_empty() {
            return;
        }
        let mut m = self.tenant_backends.write().await;
        match rtc {
            Some(v) => {
                m.insert(tenant.to_string(), v);
            }
            None => {
                m.remove(tenant);
            }
        }
    }

    /// 与 [`require`](Self::require) 一致：先查租户专属 backend，再回退全局 default。
    pub async fn has_backend_for_tenant(&self, tenant_id: &str) -> bool {
        let tenant = tenant_id.trim();
        if !tenant.is_empty() {
            let m = self.tenant_backends.read().await;
            if m.contains_key(tenant) {
                return true;
            }
        }
        self.backend.read().await.is_some()
    }

    /// 上下文缺租户（含空白）时以 `PolicyDenied` 拒绝（W13-D）：丢了租户的调用不能被全局默认
    /// 后端接走（该租户可能有专属后端），报错里也不再写租户 "0"。租户先去掉首尾空白再判空、再查
    /// 专属后端（与登记、[`has_backend_for_tenant`](Self::has_backend_for_tenant) 和调度路径的
    /// [`trimmed_dispatch_tenant`] 一致）；以前按原值查，带空白的租户查不到专属后端，落到默认后端。
    async fn require(&self, ctx: &Ctx) -> Result<Arc<dyn RtcCapability>> {
        let tenant = trimmed_dispatch_tenant(ctx.tenant_id())
            .ok_or_else(|| CapabilityError::PolicyDenied(RTC_BACKEND_TENANT_REQUIRED.into()))?;
        if let Some(b) = self.tenant_backends.read().await.get(tenant) {
            return Ok(Arc::clone(b));
        }
        self.backend.read().await.clone().ok_or_else(|| {
            CapabilityError::NotRegistered(format!("rtc capability backend (tenant={tenant})"))
        })
    }
}

#[async_trait]
impl RtcCapability for RtcCapabilityRouter {
    fn id(&self) -> &str {
        "rtc.router"
    }

    async fn create_call(&self, ctx: &Ctx, req: &CreateCallRequest) -> Result<CreateCallResponse> {
        let b = self.require(ctx).await?;
        b.create_call(ctx, req).await
    }

    async fn accept_call(&self, ctx: &Ctx, req: &AcceptCallRequest) -> Result<AcceptCallResponse> {
        let b = self.require(ctx).await?;
        b.accept_call(ctx, req).await
    }

    async fn reject_call(&self, ctx: &Ctx, req: &RejectCallRequest) -> Result<RejectCallResponse> {
        let b = self.require(ctx).await?;
        b.reject_call(ctx, req).await
    }

    async fn hangup_call(&self, ctx: &Ctx, req: &HangupCallRequest) -> Result<HangupCallResponse> {
        let b = self.require(ctx).await?;
        b.hangup_call(ctx, req).await
    }

    async fn get_join_token(
        &self,
        ctx: &Ctx,
        req: &GetJoinTokenRequest,
    ) -> Result<GetJoinTokenResponse> {
        let b = self.require(ctx).await?;
        b.get_join_token(ctx, req).await
    }

    async fn list_participants(
        &self,
        ctx: &Ctx,
        req: &ListParticipantsRequest,
    ) -> Result<ListParticipantsResponse> {
        let b = self.require(ctx).await?;
        b.list_participants(ctx, req).await
    }

    async fn media_join_transport(
        &self,
        ctx: &Ctx,
        req: &MediaJoinTransportRequest,
    ) -> Result<MediaJoinTransportResponse> {
        let b = self.require(ctx).await?;
        b.media_join_transport(ctx, req).await
    }

    async fn media_leave_transport(
        &self,
        ctx: &Ctx,
        req: &MediaLeaveTransportRequest,
    ) -> Result<MediaLeaveTransportResponse> {
        let b = self.require(ctx).await?;
        b.media_leave_transport(ctx, req).await
    }

    async fn media_handle_sdp_offer(
        &self,
        ctx: &Ctx,
        req: &HandleSdpOfferRequest,
    ) -> Result<HandleSdpOfferResponse> {
        let b = self.require(ctx).await?;
        b.media_handle_sdp_offer(ctx, req).await
    }

    async fn media_handle_sdp_answer(
        &self,
        ctx: &Ctx,
        req: &HandleSdpAnswerRequest,
    ) -> Result<HandleSdpAnswerResponse> {
        let b = self.require(ctx).await?;
        b.media_handle_sdp_answer(ctx, req).await
    }

    async fn media_add_ice_candidate(
        &self,
        ctx: &Ctx,
        req: &AddIceCandidateRequest,
    ) -> Result<AddIceCandidateResponse> {
        let b = self.require(ctx).await?;
        b.media_add_ice_candidate(ctx, req).await
    }

    async fn media_set_publisher_mute(
        &self,
        ctx: &Ctx,
        req: &MediaSetPublisherMuteRequest,
    ) -> Result<MediaSetPublisherMuteResponse> {
        let b = self.require(ctx).await?;
        b.media_set_publisher_mute(ctx, req).await
    }

    async fn media_set_subscription(
        &self,
        ctx: &Ctx,
        req: &MediaSetSubscriptionRequest,
    ) -> Result<MediaSetSubscriptionResponse> {
        let b = self.require(ctx).await?;
        b.media_set_subscription(ctx, req).await
    }

    async fn media_set_simulcast_layer(
        &self,
        ctx: &Ctx,
        req: &MediaSetSimulcastLayerRequest,
    ) -> Result<MediaSetSimulcastLayerResponse> {
        let b = self.require(ctx).await?;
        b.media_set_simulcast_layer(ctx, req).await
    }

    async fn media_get_network_quality(
        &self,
        ctx: &Ctx,
        req: &MediaGetNetworkQualityRequest,
    ) -> Result<MediaGetNetworkQualityResponse> {
        let b = self.require(ctx).await?;
        b.media_get_network_quality(ctx, req).await
    }

    async fn media_get_room_state(
        &self,
        ctx: &Ctx,
        req: &MediaGetRoomStateRequest,
    ) -> Result<MediaGetRoomStateResponse> {
        let b = self.require(ctx).await?;
        b.media_get_room_state(ctx, req).await
    }
}

#[cfg(test)]
mod tests {
    use super::{RTC_BACKEND_TENANT_REQUIRED, RtcCapabilityRouter};
    use crate::domain::capability::{
        AcceptCallRequest, AcceptCallResponse, CapabilityError, CreateCallRequest,
        CreateCallResponse, GetJoinTokenRequest, GetJoinTokenResponse, HangupCallRequest,
        HangupCallResponse, ListParticipantsRequest, ListParticipantsResponse,
        MediaJoinTransportRequest, MediaJoinTransportResponse, RejectCallRequest,
        RejectCallResponse, Result, RtcCapability,
    };
    use async_trait::async_trait;
    use flare_core_base::context::Ctx;
    use flare_server_core::Context;
    use serde_json::Value;
    use std::sync::Arc;

    struct StubRtc {
        id: &'static str,
    }

    #[async_trait]
    impl RtcCapability for StubRtc {
        fn id(&self) -> &str {
            self.id
        }

        async fn create_call(
            &self,
            _ctx: &Ctx,
            _req: &CreateCallRequest,
        ) -> Result<CreateCallResponse> {
            Ok(CreateCallResponse {
                call_id: self.id.to_string(),
                room_id: self.id.to_string(),
                ext: Value::Null,
            })
        }

        async fn accept_call(
            &self,
            _ctx: &Ctx,
            _req: &AcceptCallRequest,
        ) -> Result<AcceptCallResponse> {
            Err(CapabilityError::NotSupported(
                "accept_call test-only".into(),
            ))
        }

        async fn reject_call(
            &self,
            _ctx: &Ctx,
            _req: &RejectCallRequest,
        ) -> Result<RejectCallResponse> {
            Err(CapabilityError::NotSupported(
                "reject_call test-only".into(),
            ))
        }

        async fn hangup_call(
            &self,
            _ctx: &Ctx,
            _req: &HangupCallRequest,
        ) -> Result<HangupCallResponse> {
            Err(CapabilityError::NotSupported(
                "hangup_call test-only".into(),
            ))
        }

        async fn get_join_token(
            &self,
            _ctx: &Ctx,
            _req: &GetJoinTokenRequest,
        ) -> Result<GetJoinTokenResponse> {
            Err(CapabilityError::NotSupported(
                "get_join_token test-only".into(),
            ))
        }

        async fn list_participants(
            &self,
            _ctx: &Ctx,
            _req: &ListParticipantsRequest,
        ) -> Result<ListParticipantsResponse> {
            Err(CapabilityError::NotSupported(
                "list_participants test-only".into(),
            ))
        }

        async fn media_join_transport(
            &self,
            _ctx: &Ctx,
            req: &MediaJoinTransportRequest,
        ) -> Result<MediaJoinTransportResponse> {
            Ok(MediaJoinTransportResponse {
                room_id: req.room_id.clone(),
                peer_id: self.id.to_string(),
                session_id: format!("session-{}", self.id),
                call_id: req.call_id.clone(),
                ext: Value::Null,
            })
        }
    }

    fn ctx(tenant_id: &str) -> Ctx {
        Arc::new(
            Context::with_request_id("req-rtc-router-test")
                .with_user_id("u1")
                .with_tenant_id(tenant_id),
        )
    }

    #[tokio::test]
    async fn tenant_backend_overrides_default() {
        let router = RtcCapabilityRouter::new();
        router
            .set_backend(Some(Arc::new(StubRtc { id: "default" })))
            .await;
        router
            .set_backend_for_tenant("tenant-a", Some(Arc::new(StubRtc { id: "tenant-a" })))
            .await;

        let out_a = router
            .create_call(
                &ctx("tenant-a"),
                &CreateCallRequest {
                    tenant_id: "tenant-a".into(),
                    request_id: "r".into(),
                    conversation_id: "c".into(),
                    initiator_user_id: "u".into(),
                    media: None,
                    ext: Value::Null,
                },
            )
            .await
            .expect("tenant-a should use tenant backend");
        assert_eq!(out_a.call_id, "tenant-a");

        let out_b = router
            .create_call(
                &ctx("tenant-b"),
                &CreateCallRequest {
                    tenant_id: "tenant-b".into(),
                    request_id: "r".into(),
                    conversation_id: "c".into(),
                    initiator_user_id: "u".into(),
                    media: None,
                    ext: Value::Null,
                },
            )
            .await
            .expect("tenant-b should fallback to default backend");
        assert_eq!(out_b.call_id, "default");
    }

    #[tokio::test]
    async fn has_backend_for_tenant_matches_require_resolution() {
        let router = RtcCapabilityRouter::new();
        assert!(!router.has_backend_for_tenant("0").await);

        router
            .set_backend(Some(Arc::new(StubRtc { id: "default" })))
            .await;
        assert!(router.has_backend_for_tenant("0").await);
        assert!(router.has_backend_for_tenant("tenant-x").await);

        router
            .set_backend_for_tenant("tenant-a", Some(Arc::new(StubRtc { id: "tenant-a" })))
            .await;
        assert!(router.has_backend_for_tenant("tenant-a").await);
    }

    #[tokio::test]
    async fn tenant_backend_proxies_media_join_transport() {
        let router = RtcCapabilityRouter::new();
        router
            .set_backend_for_tenant("tenant-a", Some(Arc::new(StubRtc { id: "tenant-a" })))
            .await;

        let out = router
            .media_join_transport(
                &ctx("tenant-a"),
                &MediaJoinTransportRequest {
                    tenant_id: "tenant-a".into(),
                    request_id: "r".into(),
                    room_id: "room-1".into(),
                    call_id: "call-1".into(),
                    user_id: "u1".into(),
                    role: "caller".into(),
                    peer_id: None,
                },
            )
            .await
            .expect("media join should be proxied to tenant backend");

        assert_eq!(out.room_id, "room-1");
        assert_eq!(out.call_id, "call-1");
        assert_eq!(out.peer_id, "tenant-a");
    }

    /// 上下文缺租户（含空白）时拒绝，不落到全局默认后端，也不在报错里写租户 "0"（W13-D）。
    /// 租户专属后端存在时，丢了租户的调用以前会被默认后端接走。
    #[tokio::test]
    async fn a_context_without_a_tenant_is_denied_not_served_by_the_default_backend() {
        let router = RtcCapabilityRouter::new();
        router
            .set_backend(Some(Arc::new(StubRtc { id: "default" })))
            .await;
        router
            .set_backend_for_tenant("0", Some(Arc::new(StubRtc { id: "tenant-zero" })))
            .await;
        let no_tenant: Ctx = Arc::new(Context::with_request_id("req-rtc-router-test"));

        for ctx in [no_tenant, ctx(""), ctx("  ")] {
            let error = router
                .create_call(
                    &ctx,
                    &CreateCallRequest {
                        tenant_id: String::new(),
                        request_id: "r".into(),
                        conversation_id: "c".into(),
                        initiator_user_id: "u".into(),
                        media: None,
                        ext: Value::Null,
                    },
                )
                .await
                .map(|out| out.call_id)
                .expect_err("a call without a tenant must be denied");
            assert!(
                matches!(&error, CapabilityError::PolicyDenied(reason) if reason == RTC_BACKEND_TENANT_REQUIRED),
                "{:?}: {error:?}",
                ctx.tenant_id()
            );
        }
    }

    /// 上下文租户带首尾空白时按去掉空白后的租户选专属后端，与登记和 `has_backend_for_tenant`
    /// 一致；以前按原值查不到，落到全局默认后端。拒绝原因是 `tenant required …` 口径。
    #[tokio::test]
    async fn a_padded_context_tenant_selects_the_tenants_own_backend() {
        assert_eq!(
            RTC_BACKEND_TENANT_REQUIRED,
            "tenant required to select an RTC backend"
        );
        let router = RtcCapabilityRouter::new();
        router
            .set_backend(Some(Arc::new(StubRtc { id: "default" })))
            .await;
        router
            .set_backend_for_tenant(" tenant-a ", Some(Arc::new(StubRtc { id: "tenant-a" })))
            .await;
        assert!(router.has_backend_for_tenant(" tenant-a ").await);
        for tenant in ["tenant-a", " tenant-a ", "\ttenant-a"] {
            let out = router
                .create_call(
                    &ctx(tenant),
                    &CreateCallRequest {
                        tenant_id: String::new(),
                        request_id: "r".into(),
                        conversation_id: "c".into(),
                        initiator_user_id: "u".into(),
                        media: None,
                        ext: Value::Null,
                    },
                )
                .await
                .expect("a call with a tenant is served");
            assert_eq!(out.call_id, "tenant-a", "{tenant:?}");
        }
    }
}
