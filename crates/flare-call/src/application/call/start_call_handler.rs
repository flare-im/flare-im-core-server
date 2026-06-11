//! Start-call command handler.

use std::sync::Arc;

use async_trait::async_trait;

use crate::domain::{CallSession, CallSessionRepository};

#[derive(Debug, Clone)]
pub struct StartCallCommand {
    pub tenant_id: String,
    pub conversation_id: String,
}

#[async_trait]
pub trait StartCallHandlerPort: Send + Sync {
    async fn handle(&self, cmd: StartCallCommand) -> flare_server_core::error::Result<CallSession>;
}

pub struct StartCallHandler {
    repo: Arc<dyn CallSessionRepository>,
}

impl StartCallHandler {
    pub fn new(repo: Arc<dyn CallSessionRepository>) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl StartCallHandlerPort for StartCallHandler {
    async fn handle(&self, cmd: StartCallCommand) -> flare_server_core::error::Result<CallSession> {
        let (session, _event) = CallSession::start(cmd.conversation_id, cmd.tenant_id);
        self.repo.save(&session).await?;
        Ok(session)
    }
}
