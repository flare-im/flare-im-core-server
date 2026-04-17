//! RTC 路由器。

use std::sync::Arc;

use async_trait::async_trait;
use flare_core_base::context::Ctx;
use tokio::sync::RwLock;

use crate::domain::capability::{
    AcceptCallRequest, AcceptCallResponse, CapabilityError, CreateCallRequest, CreateCallResponse,
    GetJoinTokenRequest, GetJoinTokenResponse, HangupCallRequest, HangupCallResponse,
    ListParticipantsRequest, ListParticipantsResponse, RejectCallRequest, RejectCallResponse,
    Result, RtcCapability,
};

#[derive(Clone)]
pub struct RtcCapabilityRouter {
    backend: Arc<RwLock<Option<Arc<dyn RtcCapability>>>>,
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
        }
    }

    pub async fn set_backend(&self, rtc: Option<Arc<dyn RtcCapability>>) {
        *self.backend.write().await = rtc;
    }

    async fn require(&self) -> Result<Arc<dyn RtcCapability>> {
        self.backend
            .read()
            .await
            .clone()
            .ok_or_else(|| CapabilityError::NotRegistered("rtc capability backend".into()))
    }
}

#[async_trait]
impl RtcCapability for RtcCapabilityRouter {
    fn id(&self) -> &str {
        "rtc.router"
    }

    async fn create_call(&self, ctx: &Ctx, req: &CreateCallRequest) -> Result<CreateCallResponse> {
        let b = self.require().await?;
        b.create_call(ctx, req).await
    }

    async fn accept_call(&self, ctx: &Ctx, req: &AcceptCallRequest) -> Result<AcceptCallResponse> {
        let b = self.require().await?;
        b.accept_call(ctx, req).await
    }

    async fn reject_call(&self, ctx: &Ctx, req: &RejectCallRequest) -> Result<RejectCallResponse> {
        let b = self.require().await?;
        b.reject_call(ctx, req).await
    }

    async fn hangup_call(&self, ctx: &Ctx, req: &HangupCallRequest) -> Result<HangupCallResponse> {
        let b = self.require().await?;
        b.hangup_call(ctx, req).await
    }

    async fn get_join_token(
        &self,
        ctx: &Ctx,
        req: &GetJoinTokenRequest,
    ) -> Result<GetJoinTokenResponse> {
        let b = self.require().await?;
        b.get_join_token(ctx, req).await
    }

    async fn list_participants(
        &self,
        ctx: &Ctx,
        req: &ListParticipantsRequest,
    ) -> Result<ListParticipantsResponse> {
        let b = self.require().await?;
        b.list_participants(ctx, req).await
    }
}
