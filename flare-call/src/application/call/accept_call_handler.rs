//! Accept-call command handler.

use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::{CallSession, CallSessionRepository};

#[derive(Debug, Clone)]
pub struct AcceptCallCommand {
    pub session_id: Uuid,
    pub user_id: String,
}

#[async_trait]
pub trait AcceptCallHandlerPort: Send + Sync {
    async fn handle(&self, cmd: AcceptCallCommand)
    -> flare_server_core::error::Result<CallSession>;
}

pub struct AcceptCallHandler {
    repo: Arc<dyn CallSessionRepository>,
}

impl AcceptCallHandler {
    pub fn new(repo: Arc<dyn CallSessionRepository>) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl AcceptCallHandlerPort for AcceptCallHandler {
    async fn handle(
        &self,
        cmd: AcceptCallCommand,
    ) -> flare_server_core::error::Result<CallSession> {
        let mut session = self
            .repo
            .find_by_id(&cmd.session_id)
            .await?
            .ok_or_else(|| {
                flare_server_core::error::FlareError::system("call session not found".to_string())
            })?;
        let _event = session.accept(cmd.user_id)?;
        self.repo.save(&session).await?;
        Ok(session)
    }
}
