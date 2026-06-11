//! RTC 信令桥：把 gateway 运行时接到 `flare-call` 生命周期命令与能力实例路由。

use std::sync::Arc;

use flare_call::application::call::{
    AcceptCallCommand, AcceptCallHandler, AcceptCallHandlerPort, CancelCallCommand,
    CancelCallHandler, CancelCallHandlerPort, HangupCallCommand, HangupCallHandler,
    HangupCallHandlerPort, RejectCallCommand, RejectCallHandler, RejectCallHandlerPort,
    StartCallCommand, StartCallHandler, StartCallHandlerPort,
};
use flare_call::domain::{CallSession, CallSessionRepository};
use flare_server_core::error::{FlareError, Result};
use uuid::Uuid;

use super::event::CallSignalRouteView;
use super::router::{CallSignalRouter, CapabilityRouteHint};

/// 网关侧桥接入口：保持薄层，生命周期状态迁移委托给 `flare-call`。
pub struct CallSignalBridge {
    router: Arc<CallSignalRouter>,
    repository: Arc<dyn CallSessionRepository>,
    start_call_handler: StartCallHandler,
    accept_call_handler: AcceptCallHandler,
    reject_call_handler: RejectCallHandler,
    cancel_call_handler: CancelCallHandler,
    hangup_call_handler: HangupCallHandler,
}

impl CallSignalBridge {
    pub fn new(router: Arc<CallSignalRouter>, repository: Arc<dyn CallSessionRepository>) -> Self {
        Self {
            router,
            repository: repository.clone(),
            start_call_handler: StartCallHandler::new(repository.clone()),
            accept_call_handler: AcceptCallHandler::new(repository.clone()),
            reject_call_handler: RejectCallHandler::new(repository.clone()),
            cancel_call_handler: CancelCallHandler::new(repository.clone()),
            hangup_call_handler: HangupCallHandler::new(repository),
        }
    }

    /// Client invite 进入 gateway 后创建业务中立的通话会话。
    pub async fn start_call(
        &self,
        tenant_id: impl Into<String>,
        conversation_id: impl Into<String>,
    ) -> Result<CallSession> {
        self.start_call_handler
            .handle(StartCallCommand {
                tenant_id: tenant_id.into(),
                conversation_id: conversation_id.into(),
            })
            .await
    }

    pub async fn accept_call(
        &self,
        session_id: Uuid,
        user_id: impl Into<String>,
    ) -> Result<CallSession> {
        self.accept_call_handler
            .handle(AcceptCallCommand {
                session_id,
                user_id: user_id.into(),
            })
            .await
    }

    pub async fn reject_call(
        &self,
        session_id: Uuid,
        user_id: impl Into<String>,
        reason: Option<String>,
    ) -> Result<CallSession> {
        self.reject_call_handler
            .handle(RejectCallCommand {
                session_id,
                user_id: user_id.into(),
                reason,
            })
            .await
    }

    pub async fn cancel_call(
        &self,
        session_id: Uuid,
        by_user_id: impl Into<String>,
    ) -> Result<CallSession> {
        self.cancel_call_handler
            .handle(CancelCallCommand {
                session_id,
                by_user_id: by_user_id.into(),
            })
            .await
    }

    pub async fn hangup_call(
        &self,
        session_id: Uuid,
        by_user_id: impl Into<String>,
    ) -> Result<CallSession> {
        self.hangup_call_handler
            .handle(HangupCallCommand {
                session_id,
                by_user_id: by_user_id.into(),
            })
            .await
    }

    /// RTC capability/plugin 完成房间分配后，把 opaque 路由键绑定回通话会话。
    pub async fn bind_capability_route(
        &self,
        session_id: Uuid,
        sfu_room_id: impl Into<String>,
        capability_instance_id: impl Into<String>,
    ) -> Result<CallSession> {
        let mut session = self
            .repository
            .find_by_id(&session_id)
            .await?
            .ok_or_else(|| FlareError::system("call session not found"))?;

        let _event = session.bind_room(sfu_room_id.into(), capability_instance_id.into());
        self.repository.save(&session).await?;
        Ok(session)
    }

    pub async fn on_uplink(
        &self,
        tenant_id: &str,
        signal: &CallSignalRouteView,
    ) -> flare_server_core::error::Result<Option<CapabilityRouteHint>> {
        self.router.route_uplink(tenant_id, signal).await
    }

    pub async fn on_downlink(
        &self,
        tenant_id: &str,
        signal: &CallSignalRouteView,
    ) -> flare_server_core::error::Result<Option<CapabilityRouteHint>> {
        self.router.route_downlink(tenant_id, signal).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::call_signal::event::{CallSignalRouteView, CallSignalType};
    use crate::call_signal::repository::InMemoryCallSessionRepository;
    use crate::call_signal::router::{CallBindingLookup, CallSignalRouter};
    use flare_call::domain::CallSessionState;

    fn bridge() -> CallSignalBridge {
        let repository = Arc::new(InMemoryCallSessionRepository::default());
        let lookup: Arc<dyn CallBindingLookup> = repository.clone();
        let call_repository: Arc<dyn CallSessionRepository> = repository;
        CallSignalBridge::new(Arc::new(CallSignalRouter::new(lookup)), call_repository)
    }

    #[tokio::test]
    async fn start_call_delegates_to_flare_call_handler() {
        let bridge = bridge();

        let session = bridge
            .start_call("tenant-a", "conversation-a")
            .await
            .expect("start call");

        assert_eq!(session.tenant_id, "tenant-a");
        assert_eq!(session.conversation_id, "conversation-a");
        assert_eq!(session.state, CallSessionState::Initiating);
    }

    #[tokio::test]
    async fn accept_call_transitions_the_flare_call_session() {
        let bridge = bridge();
        let session = bridge
            .start_call("tenant-a", "conversation-a")
            .await
            .expect("start call");

        let accepted = bridge
            .accept_call(session.id, "user-a")
            .await
            .expect("accept call");

        assert_eq!(accepted.state, CallSessionState::Active);
    }

    #[tokio::test]
    async fn capability_route_binding_feeds_signal_routing() {
        let bridge = bridge();
        let session = bridge
            .start_call("tenant-a", "conversation-a")
            .await
            .expect("start call");
        bridge
            .bind_capability_route(session.id, "room-a", "rtc-capability-a")
            .await
            .expect("bind route");

        let hint = bridge
            .on_uplink(
                "tenant-a",
                &CallSignalRouteView::new(CallSignalType::Other, None, Some("room-a".to_string())),
            )
            .await
            .expect("route")
            .expect("route hint");

        assert_eq!(hint.capability_instance_id, "rtc-capability-a");
        assert_eq!(hint.sfu_room_id.as_deref(), Some("room-a"));
    }
}
