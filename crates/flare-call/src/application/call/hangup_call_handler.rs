//! Hangup-call command handler.

use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::{CallSession, CallSessionRepository};

#[derive(Debug, Clone)]
pub struct HangupCallCommand {
    pub session_id: Uuid,
    pub by_user_id: String,
}

#[async_trait]
pub trait HangupCallHandlerPort: Send + Sync {
    async fn handle(&self, cmd: HangupCallCommand)
    -> flare_server_core::error::Result<CallSession>;
}

pub struct HangupCallHandler {
    repo: Arc<dyn CallSessionRepository>,
}

impl HangupCallHandler {
    pub fn new(repo: Arc<dyn CallSessionRepository>) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl HangupCallHandlerPort for HangupCallHandler {
    async fn handle(
        &self,
        cmd: HangupCallCommand,
    ) -> flare_server_core::error::Result<CallSession> {
        let mut session = self
            .repo
            .find_by_id(&cmd.session_id)
            .await?
            .ok_or_else(|| {
                flare_server_core::error::FlareError::system("call session not found".to_string())
            })?;
        let _event = session.hangup(cmd.by_user_id)?;
        self.repo.save(&session).await?;
        Ok(session)
    }
}
