//! Reject-call command handler.

use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::{CallSession, CallSessionRepository};

#[derive(Debug, Clone)]
pub struct RejectCallCommand {
    pub session_id: Uuid,
    pub user_id: String,
    pub reason: Option<String>,
}

#[async_trait]
pub trait RejectCallHandlerPort: Send + Sync {
    async fn handle(&self, cmd: RejectCallCommand)
    -> flare_server_core::error::Result<CallSession>;
}

pub struct RejectCallHandler {
    repo: Arc<dyn CallSessionRepository>,
}

impl RejectCallHandler {
    pub fn new(repo: Arc<dyn CallSessionRepository>) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl RejectCallHandlerPort for RejectCallHandler {
    async fn handle(
        &self,
        cmd: RejectCallCommand,
    ) -> flare_server_core::error::Result<CallSession> {
        let mut session = self
            .repo
            .find_by_id(&cmd.session_id)
            .await?
            .ok_or_else(|| {
                flare_server_core::error::FlareError::system("call session not found".to_string())
            })?;
        let _event = session.reject(cmd.user_id, cmd.reason)?;
        self.repo.save(&session).await?;
        Ok(session)
    }
}
